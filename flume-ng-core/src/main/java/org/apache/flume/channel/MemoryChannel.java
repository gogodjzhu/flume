/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.channel;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.GuardedBy;

import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.annotations.Recyclable;
import org.apache.flume.instrumentation.ChannelCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * <p>
 * MemoryChannel is the recommended channel to use when speeds which
 * writing to disk is impractical is required or durability of data is not
 * required.
 * </p>
 * <p>
 * Additionally, MemoryChannel should be used when a channel is required for
 * unit testing purposes.
 * </p>
 *
 * 基于内存实现的Channel, 非常常用的一个Channel,
 * 它的优点:
 *  - 基于内存缓存Event, 速度快
 *  - 支持事务
 *  - 支持配置缓存大小
 *  - 支持配置事务大小
 * 缺点:
 *  - 崩溃导致数据丢失
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
@Recyclable
public class MemoryChannel extends BasicChannelSemantics {
  private static Logger LOGGER = LoggerFactory.getLogger(MemoryChannel.class);
  private static final Integer defaultCapacity = 100;
  private static final Integer defaultTransCapacity = 100;
  private static final double byteCapacitySlotSize = 100;
  private static final Long defaultByteCapacity = (long)(Runtime.getRuntime().maxMemory() * .80);
  private static final Integer defaultByteCapacityBufferPercentage = 20;

  private static final Integer defaultKeepAlive = 3;

  /**
   * MemoryChannel提供的事务实现, 在内存中实现事务操作. 同一个事务内可以同时执行读/写
   * 操作
   */
  private class MemoryTransaction extends BasicTransactionSemantics {
    private LinkedBlockingDeque<Event> takeList;
    private LinkedBlockingDeque<Event> putList;
    private final ChannelCounter channelCounter;
    private int putByteCounter = 0;
    private int takeByteCounter = 0;

    public MemoryTransaction(int transCapacity, ChannelCounter counter) {
      putList = new LinkedBlockingDeque<Event>(transCapacity);
      takeList = new LinkedBlockingDeque<Event>(transCapacity);

      channelCounter = counter;
    }

    @Override
    protected void doPut(Event event) throws InterruptedException {
      channelCounter.incrementEventPutAttemptCount();
      // byteCapacitySlotSize: 平均多少个字节占用一个Semaphore信号量
      // eventByteSize = eventSize(仅body) / byteCapacitySlotSize, event需要占用
      // 的信号量数量
      int eventByteSize = (int)Math.ceil(estimateEventSize(event)/byteCapacitySlotSize);


      // 使用event体积预估信号量(semaphore总的信号量即总的可用空间), 剩余不足时尝试等待
      if (bytesRemaining.tryAcquire(eventByteSize, keepAlive, TimeUnit.SECONDS)) {
        // putList用于保存写入到当前事务的未提交event(亦即source提交到channel的event)
        if(!putList.offer(event)) {
          throw new ChannelException("Put queue for MemoryTransaction of capacity " +
              putList.size() + " full, consider committing more frequently, " +
              "increasing capacity or increasing thread count");
        }
      } else {
        throw new ChannelException("Put queue for MemoryTransaction of byteCapacity " +
            (lastByteCapacity * (int)byteCapacitySlotSize) + " bytes cannot add an " +
            " event of size " + estimateEventSize(event) + " bytes because " +
             (bytesRemaining.availablePermits() * (int)byteCapacitySlotSize) + " bytes are already used." +
            " Try consider comitting more frequently, increasing byteCapacity or increasing thread count");
      }
      // takeByteCounter通过信号量来监控put情况
      putByteCounter += eventByteSize;
    }

    @Override
    protected Event doTake() throws InterruptedException {
      channelCounter.incrementEventTakeAttemptCount();

      // takeList用于保存从当前事务拉取的未commit的event(亦即sink从channel拉取的数据)
      if(takeList.remainingCapacity() == 0) {
        // takeList保存一个事务中未提交的数据, 它的容量填满时, 抛出异常. 应该适当提高
        // 事务提交的速度
        throw new ChannelException("Take list for MemoryTransaction, capacity " +
            takeList.size() + " full, consider committing more frequently, " +
            "increasing capacity, or increasing thread count");
      }
      // 通过queueStored : Semaphore 信号量来保存已经提交且可供拉取的event数量
      if(!queueStored.tryAcquire(keepAlive, TimeUnit.SECONDS)) {
        return null;
      }
      Event event;
      // 从queue中拉取一个event
      synchronized(queueLock) {
        event = queue.poll();
      }
      Preconditions.checkNotNull(event, "Queue.poll returned NULL despite semaphore " +
          "signalling existence of entry");
      // 将刚拉取的event放到takeList, 此list中保存的都是未commit的pull event
      takeList.put(event);

      // takeByteCounter通过信号量来监控take情况
      int eventByteSize = (int)Math.ceil(estimateEventSize(event)/byteCapacitySlotSize);
      takeByteCounter += eventByteSize;

      return event;
    }

    /**
     * commit事务, 注意读take/写put提交都通过本方法
     */
    @Override
    protected void doCommit() throws InterruptedException {
      // 等效于 queueRemaining 信号量尝试获取(putList.size() - takeList.size())
      // 目的在于保证queue有足够的空间保存事务commit的event
      // 注意这里的计算有点讲究: putList为待写入queue的event. takeList为从
      // queue拉取走, 但尚未commit的event, 此部分给queue腾出空间.
      // 当takeList - putList < 0时, 表示此次事务拉走的event数量多于新增, 此时commit
      // queue内空间不增反减, 所以不需要通过tryAcquire控制. 最后还需要release增加queue
      // 的剩余空间
      // 当takeList - putList > 0时, 即此次事务新增的event比拉走的多, 需要通过
      // tryAcquire控制, 消耗queue剩余空间
      int remainingChange = takeList.size() - putList.size();
      if(remainingChange < 0) {
        if(!queueRemaining.tryAcquire(-remainingChange, keepAlive, TimeUnit.SECONDS)) {
          throw new ChannelException("Space for commit to queue couldn't be acquired" +
              " Sinks are likely not keeping up with sources, or the buffer size is too tight");
        }
      }
      int puts = putList.size();
      int takes = takeList.size();
      // 将putList中的数据转移到queue
      synchronized(queueLock) {
        if(puts > 0 ) {
          while(!putList.isEmpty()) {
            if(!queue.offer(putList.removeFirst())) {
              throw new RuntimeException("Queue add failed, this shouldn't be able to happen");
            }
          }
        }
        putList.clear();
        takeList.clear();
      }
      // take commit释放queue空间, release表示增加剩余的可容纳字节, putByteCounter不
      // 做类似操作因为put commit只是将event从putList移到queue, 空间占用不变
      bytesRemaining.release(takeByteCounter);
      takeByteCounter = 0;
      putByteCounter = 0;

      // 新增queueStored信号量, 表示queue的容量新增
      queueStored.release(puts);
      if(remainingChange > 0) {
        // 此次事务新增的空间多于消耗的
        queueRemaining.release(remainingChange);
      }
      if (puts > 0) {
        channelCounter.addToEventPutSuccessCount(puts);
      }
      if (takes > 0) {
        channelCounter.addToEventTakeSuccessCount(takes);
      }

      // 更新channel内缓存的event数量
      channelCounter.setChannelSize(queue.size());
    }

    @Override
    protected void doRollback() {
      int takes = takeList.size();
      // rollback, putList直接清空, takeList还需放回queue
      synchronized(queueLock) {
        Preconditions.checkState(queue.remainingCapacity() >= takeList.size(), "Not enough space in memory channel " +
            "queue to rollback takes. This should never happen, please report");
        while(!takeList.isEmpty()) {
          queue.addFirst(takeList.removeLast());
        }
        putList.clear();
      }
      bytesRemaining.release(putByteCounter);
      putByteCounter = 0;
      takeByteCounter = 0;

      queueStored.release(takes);
      channelCounter.setChannelSize(queue.size());
    }

  }

  // lock to guard queue, mainly needed to keep it locked down during resizes
  // it should never be held through a blocking operation
  private Object queueLock = new Object();

  // 在实际缓存Event的队列, 注意此双向链表非线程安全, 所以通过上面的 queueLock 加锁操作
  @GuardedBy(value = "queueLock")
  private LinkedBlockingDeque<Event> queue;

  /**
   * 这里非常巧妙地利用三个Semaphore来控制queue的内存使用,
   * queueRemaining表示queue剩余的空间, 启动时为初始化capacity
   * queueStore 表示queue中保存的event数量, 启动时为0, commit和rollback的时候release,
   * take的时候tryAcquire
   * bytesRemaining为整个channel剩余可用的字节空间, byteCapacitySlotSize个字节映射一
   * 个信号量. 由于信号量为int类型, 当可用空间int溢出, 可以通过byteCapacitySlotSize
   * 调节
   */
  // invariant that tracks the amount of space remaining in the queue(with all uncommitted takeLists deducted)
  // we maintain the remaining permits = queue.remaining - takeList.size()
  // this allows local threads waiting for space in the queue to commit without denying access to the
  // shared lock to threads that would make more space on the queue
  private Semaphore queueRemaining;
  // used to make "reservations" to grab data from the queue.
  // by using this we can block for a while to get data without locking all other threads out
  // like we would if we tried to use a blocking call on queue
  private Semaphore queueStored;
  // maximum items in a transaction queue
  private volatile Integer transCapacity;
  private volatile int keepAlive;
  private volatile int byteCapacity;
  private volatile int lastByteCapacity;
  private volatile int byteCapacityBufferPercentage;
  private Semaphore bytesRemaining;
  private ChannelCounter channelCounter;


  public MemoryChannel() {
    super();
  }

  /**
   * Read parameters from context
   * <li>capacity = type long that defines the total number of events allowed at one time in the queue.
   * <li>transactionCapacity = type long that defines the total number of events allowed in one transaction.
   * <li>byteCapacity = type long that defines the max number of bytes used for events in the queue.
   * <li>byteCapacityBufferPercentage = type int that defines the percent of buffer between byteCapacity and the estimated event size.
   * <li>keep-alive = type int that defines the number of second to wait for a queue permit
   */
  @Override
  public void configure(Context context) {
    Integer capacity = null;
    try {
      capacity = context.getInteger("capacity", defaultCapacity);
    } catch(NumberFormatException e) {
      capacity = defaultCapacity;
      LOGGER.warn("Invalid capacity specified, initializing channel to "
          + "default capacity of {}", defaultCapacity);
    }

    if (capacity <= 0) {
      capacity = defaultCapacity;
      LOGGER.warn("Invalid capacity specified, initializing channel to "
          + "default capacity of {}", defaultCapacity);
    }
    try {
      transCapacity = context.getInteger("transactionCapacity", defaultTransCapacity);
    } catch(NumberFormatException e) {
      transCapacity = defaultTransCapacity;
      LOGGER.warn("Invalid transation capacity specified, initializing channel"
          + " to default capacity of {}", defaultTransCapacity);
    }

    if (transCapacity <= 0) {
      transCapacity = defaultTransCapacity;
      LOGGER.warn("Invalid transation capacity specified, initializing channel"
          + " to default capacity of {}", defaultTransCapacity);
    }
    Preconditions.checkState(transCapacity <= capacity,
        "Transaction Capacity of Memory Channel cannot be higher than " +
            "the capacity.");

    try {
      byteCapacityBufferPercentage = context.getInteger("byteCapacityBufferPercentage", defaultByteCapacityBufferPercentage);
    } catch(NumberFormatException e) {
      byteCapacityBufferPercentage = defaultByteCapacityBufferPercentage;
    }

    try {
      byteCapacity = (int)((context.getLong("byteCapacity", defaultByteCapacity).longValue() * (1 - byteCapacityBufferPercentage * .01 )) /byteCapacitySlotSize);
      if (byteCapacity < 1) {
        byteCapacity = Integer.MAX_VALUE;
      }
    } catch(NumberFormatException e) {
      byteCapacity = (int)((defaultByteCapacity * (1 - byteCapacityBufferPercentage * .01 )) /byteCapacitySlotSize);
    }

    try {
      keepAlive = context.getInteger("keep-alive", defaultKeepAlive);
    } catch(NumberFormatException e) {
      keepAlive = defaultKeepAlive;
    }

    if(queue != null) {
      try {
        resizeQueue(capacity);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    } else {
      synchronized(queueLock) {
        queue = new LinkedBlockingDeque<Event>(capacity);
        queueRemaining = new Semaphore(capacity);
        queueStored = new Semaphore(0);
      }
    }

    if (bytesRemaining == null) {
      bytesRemaining = new Semaphore(byteCapacity);
      lastByteCapacity = byteCapacity;
    } else {
      if (byteCapacity > lastByteCapacity) {
        bytesRemaining.release(byteCapacity - lastByteCapacity);
        lastByteCapacity = byteCapacity;
      } else {
        try {
          if(!bytesRemaining.tryAcquire(lastByteCapacity - byteCapacity, keepAlive, TimeUnit.SECONDS)) {
            LOGGER.warn("Couldn't acquire permits to downsize the byte capacity, resizing has been aborted");
          } else {
            lastByteCapacity = byteCapacity;
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }

    if (channelCounter == null) {
      channelCounter = new ChannelCounter(getName());
    }
  }

  private void resizeQueue(int capacity) throws InterruptedException {
    int oldCapacity;
    synchronized(queueLock) {
      oldCapacity = queue.size() + queue.remainingCapacity();
    }

    if(oldCapacity == capacity) {
      return;
    } else if (oldCapacity > capacity) {
      if(!queueRemaining.tryAcquire(oldCapacity - capacity, keepAlive, TimeUnit.SECONDS)) {
        LOGGER.warn("Couldn't acquire permits to downsize the queue, resizing has been aborted");
      } else {
        synchronized(queueLock) {
          LinkedBlockingDeque<Event> newQueue = new LinkedBlockingDeque<Event>(capacity);
          newQueue.addAll(queue);
          queue = newQueue;
        }
      }
    } else {
      synchronized(queueLock) {
        LinkedBlockingDeque<Event> newQueue = new LinkedBlockingDeque<Event>(capacity);
        newQueue.addAll(queue);
        queue = newQueue;
      }
      queueRemaining.release(capacity - oldCapacity);
    }
  }

  @Override
  public synchronized void start() {
    channelCounter.start();
    channelCounter.setChannelSize(queue.size());
    channelCounter.setChannelCapacity(Long.valueOf(
            queue.size() + queue.remainingCapacity()));
    super.start();
  }

  @Override
  public synchronized void stop() {
    channelCounter.setChannelSize(queue.size());
    channelCounter.stop();
    super.stop();
  }

  @Override
  protected BasicTransactionSemantics createTransaction() {
    return new MemoryTransaction(transCapacity, channelCounter);
  }

  private long estimateEventSize(Event event)
  {
    byte[] body = event.getBody();
    if(body != null && body.length != 0) {
      return body.length;
    }
    //Each event occupies at least 1 slot, so return 1.
    return 1;
  }
}
