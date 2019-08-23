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
package org.apache.flume.channel.file;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.flume.ChannelException;
import org.apache.flume.Event;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.channel.file.encryption.KeyProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.security.Key;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

/**
 * Stores FlumeEvents on disk and pointers to the events in a in memory queue.
 * Once a log object is created the replay method should be called to reconcile
 * the on disk write ahead log with the last checkpoint of the queue.
 *
 * Before calling any of commitPut/commitTake/get/put/rollback/take
 * Log.tryLockShared should be called and the above operations
 * should only be called if tryLockShared returns true. After
 * the operation and any additional modifications of the
 * FlumeEventQueue, the Log.unlockShared method should be called.
 *
 * 本类代表FileChannel所依赖的日志持久化工具.
 * 运行时整个channel内的Event信息都被维护在{@link this#queue}中. 但queue并不像
 * MemoryChannel 那样保存Event的实际内容, 它保存的只是Event在持久化文件中的指针.
 *
 * 持久化文件分两个部分:
 * - log文件{@link this#logDirs} 有n个目录构成, 所有的Event就持久化保存在这些文件当中,
 *   包括Put/Take/Commit/Rollback 4中类型, 具体格式参看:
 *    - {@link this#put(long, Event)}
 *    - {@link this#take(long, FlumeEventPointer)}
 *    - {@link this#commit(long, short)}
 *    - {@link this#rollback(long)}
 *   4种Event都包含所属的事务Id, 在保证Event完整且有序地写入log文件之后, 完全可以通过完
 *   整地重跑所有Event来恢复任意适合的状态(本质上就是恢复{@link this#queue}这个队列).
 *
 * - checkpoint文件 每次重跑所有Event来恢复显然太慢. {@link this#checkpointDir}
 *   保存了当前queue的快照信息(具体看{@link FlumeEventQueue#checkpoint(boolean)}),
 *   这样就可以直接解析checkpoint文件恢复queue, 而不需要replay 所有log文件. (但是当
 *   checkpoint文件损坏(比如未写完就关闭导致不完整), 或者升级导致版本不一致等灯光, 都会
 *   变成full replay重跑所有log文件, 效率非常低)
 *   在物理上我们会看到checkpoint包括3个部分:
 *     - checkpoint 记录每次checkpoint操作时整个queue的元数据, 包括当前最新的日志写入
 *       序号 和 已commit的消息列表, 以及checkpoint正常开始及结束标记.
 *     - inflightputs文件 保存已提交未commit的putEvent
 *     - inflighttakes文件 保存已提交未commit的takeEvent
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class Log {
  public static final String PREFIX = "log-";
  private static final Logger LOGGER = LoggerFactory.getLogger(Log.class);
  private static final int MIN_NUM_LOGS = 2;
  public static final String FILE_LOCK = "in_use.lock";
  // for reader
  private final Map<Integer, LogFile.RandomReader> idLogFileMap = Collections
      .synchronizedMap(new HashMap<Integer, LogFile.RandomReader>());
  private final AtomicInteger nextFileID = new AtomicInteger(0);
  private final File checkpointDir;
  private final File backupCheckpointDir;
  private final File[] logDirs;
  private final int queueCapacity;
  private final AtomicReferenceArray<LogFile.Writer> logFiles;

  private final ScheduledExecutorService workerExecutor;

  private volatile boolean open;
  private FlumeEventQueue queue;
  private long checkpointInterval;
  private long maxFileSize;
  private final boolean useFastReplay;
  // 最少空闲空间
  private final long minimumRequiredSpace;
  private final Map<String, FileLock> locks;
  private final ReentrantReadWriteLock checkpointLock =
      new ReentrantReadWriteLock(true);

  /**
   * Set of files that should be excluded from backup and restores.
   */
  public static final Set<String> EXCLUDES = Sets.newHashSet(FILE_LOCK);
  /**
   * Shared lock
   */
  private final ReadLock checkpointReadLock = checkpointLock.readLock();
  /**
   * Exclusive lock
   */
  private final WriteLock checkpointWriterLock = checkpointLock.writeLock();
  private int logWriteTimeout;
  private final String channelNameDescriptor;
  private int checkpointWriteTimeout;
  private boolean useLogReplayV1;
  private KeyProvider encryptionKeyProvider;
  private String encryptionCipherProvider;
  private String encryptionKeyAlias;
  private Key encryptionKey;
  private final long usableSpaceRefreshInterval;
  private boolean didFastReplay = false;
  private boolean didFullReplayDueToBadCheckpointException = false;
  private final boolean useDualCheckpoints;
  private volatile boolean backupRestored = false;

  private int readCount;
  private int putCount;
  private int takeCount;
  private int committedCount;
  private int rollbackCount;

  private final List<File> pendingDeletes = Lists.newArrayList();

  static class Builder {
    private long bCheckpointInterval;
    private long bMinimumRequiredSpace;
    private long bMaxFileSize;
    private int bQueueCapacity;
    private File bCheckpointDir;
    private File[] bLogDirs;
    private int bLogWriteTimeout =
        FileChannelConfiguration.DEFAULT_WRITE_TIMEOUT;
    private String bName;
    private int bCheckpointWriteTimeout =
        FileChannelConfiguration.DEFAULT_CHECKPOINT_WRITE_TIMEOUT;
    private boolean useLogReplayV1;
    private boolean useFastReplay;
    private KeyProvider bEncryptionKeyProvider;
    private String bEncryptionKeyAlias;
    private String bEncryptionCipherProvider;
    private long bUsableSpaceRefreshInterval = 15L * 1000L;
    private boolean bUseDualCheckpoints = false;
    private File bBackupCheckpointDir = null;

    Builder setUsableSpaceRefreshInterval(long usableSpaceRefreshInterval) {
      bUsableSpaceRefreshInterval = usableSpaceRefreshInterval;
      return this;
    }

    Builder setCheckpointInterval(long interval) {
      bCheckpointInterval = interval;
      return this;
    }

    Builder setMaxFileSize(long maxSize) {
      bMaxFileSize = maxSize;
      return this;
    }

    Builder setQueueSize(int capacity) {
      bQueueCapacity = capacity;
      return this;
    }

    Builder setCheckpointDir(File cpDir) {
      bCheckpointDir = cpDir;
      return this;
    }

    Builder setLogDirs(File[] dirs) {
      bLogDirs = dirs;
      return this;
    }

    Builder setLogWriteTimeout(int timeout) {
      bLogWriteTimeout = timeout;
      return this;
    }

    Builder setChannelName(String name) {
      bName = name;
      return this;
    }

    Builder setMinimumRequiredSpace(long minimumRequiredSpace) {
      bMinimumRequiredSpace = minimumRequiredSpace;
      return this;
    }

    Builder setCheckpointWriteTimeout(int checkpointTimeout){
      bCheckpointWriteTimeout = checkpointTimeout;
      return this;
    }

    Builder setUseLogReplayV1(boolean useLogReplayV1){
      this.useLogReplayV1 = useLogReplayV1;
      return this;
    }

    Builder setUseFastReplay(boolean useFastReplay){
      this.useFastReplay = useFastReplay;
      return this;
    }

    Builder setEncryptionKeyProvider(KeyProvider encryptionKeyProvider) {
      bEncryptionKeyProvider = encryptionKeyProvider;
      return this;
    }

    Builder setEncryptionKeyAlias(String encryptionKeyAlias) {
      bEncryptionKeyAlias = encryptionKeyAlias;
      return this;
    }

    Builder setEncryptionCipherProvider(String encryptionCipherProvider) {
      bEncryptionCipherProvider = encryptionCipherProvider;
      return this;
    }

    Builder setUseDualCheckpoints(boolean UseDualCheckpoints) {
      this.bUseDualCheckpoints = UseDualCheckpoints;
      return this;
    }

    Builder setBackupCheckpointDir(File backupCheckpointDir) {
      this.bBackupCheckpointDir = backupCheckpointDir;
      return this;
    }

    Log build() throws IOException {
      return new Log(bCheckpointInterval, bMaxFileSize, bQueueCapacity,
          bLogWriteTimeout, bCheckpointWriteTimeout, bUseDualCheckpoints,
          bCheckpointDir, bBackupCheckpointDir, bName,
          useLogReplayV1, useFastReplay, bMinimumRequiredSpace,
          bEncryptionKeyProvider, bEncryptionKeyAlias,
          bEncryptionCipherProvider, bUsableSpaceRefreshInterval,
          bLogDirs);
    }
  }

  private Log(long checkpointInterval, long maxFileSize, int queueCapacity,
      int logWriteTimeout, int checkpointWriteTimeout,
      boolean useDualCheckpoints, File checkpointDir, File backupCheckpointDir,
      String name, boolean useLogReplayV1, boolean useFastReplay,
      long minimumRequiredSpace, @Nullable KeyProvider encryptionKeyProvider,
      @Nullable String encryptionKeyAlias,
      @Nullable String encryptionCipherProvider,
      long usableSpaceRefreshInterval, File... logDirs)
          throws IOException {
    Preconditions.checkArgument(checkpointInterval > 0,
      "checkpointInterval <= 0");
    Preconditions.checkArgument(queueCapacity > 0, "queueCapacity <= 0");
    Preconditions.checkArgument(maxFileSize > 0, "maxFileSize <= 0");
    Preconditions.checkNotNull(checkpointDir, "checkpointDir");
    Preconditions.checkArgument(usableSpaceRefreshInterval > 0,
        "usableSpaceRefreshInterval <= 0");
    Preconditions.checkArgument(
      checkpointDir.isDirectory() || checkpointDir.mkdirs(), "CheckpointDir "
      + checkpointDir + " could not be created");
    if (useDualCheckpoints) {
      Preconditions.checkNotNull(backupCheckpointDir, "backupCheckpointDir is" +
        " null while dual checkpointing is enabled.");
      Preconditions.checkArgument(
        backupCheckpointDir.isDirectory() || backupCheckpointDir.mkdirs(),
        "Backup CheckpointDir " + backupCheckpointDir +
          " could not be created");
    }
    Preconditions.checkNotNull(logDirs, "logDirs");
    Preconditions.checkArgument(logDirs.length > 0, "logDirs empty");
    Preconditions.checkArgument(name != null && !name.trim().isEmpty(),
            "channel name should be specified");

    this.channelNameDescriptor = "[channel=" + name + "]";
    this.useLogReplayV1 = useLogReplayV1;
    this.useFastReplay = useFastReplay;
    this.minimumRequiredSpace = minimumRequiredSpace;
    this.usableSpaceRefreshInterval = usableSpaceRefreshInterval;
    for (File logDir : logDirs) {
      Preconditions.checkArgument(logDir.isDirectory() || logDir.mkdirs(),
          "LogDir " + logDir + " could not be created");
    }
    locks = Maps.newHashMap();
    try {
      // 锁checkPoint目录
      lock(checkpointDir);
      // 锁backupCheckpoint目录, 用于备份checkpointDir
      if(useDualCheckpoints) {
        lock(backupCheckpointDir);
      }
      // 锁log文件
      for (File logDir : logDirs) {
        lock(logDir);
      }
    } catch(IOException e) {
      unlock(checkpointDir);
      // TODO 这是bug么? 没有unlock(useDualCheckpoints), 当kill -9退出时岂不是无法
      // 删除lock文件?
      for (File logDir : logDirs) {
        unlock(logDir);
      }
      throw e;
    }
    if(encryptionKeyProvider != null && encryptionKeyAlias != null &&
        encryptionCipherProvider != null) {
      LOGGER.info("Encryption is enabled with encryptionKeyProvider = " +
          encryptionKeyProvider + ", encryptionKeyAlias = " + encryptionKeyAlias
          + ", encryptionCipherProvider = " + encryptionCipherProvider);
      this.encryptionKeyProvider = encryptionKeyProvider;
      this.encryptionKeyAlias = encryptionKeyAlias;
      this.encryptionCipherProvider = encryptionCipherProvider;
      this.encryptionKey = encryptionKeyProvider.getKey(encryptionKeyAlias);
    } else if (encryptionKeyProvider == null && encryptionKeyAlias == null &&
        encryptionCipherProvider == null) {
      LOGGER.info("Encryption is not enabled");
    } else {
      throw new IllegalArgumentException("Encryption configuration must all " +
          "null or all not null: encryptionKeyProvider = " +
          encryptionKeyProvider + ", encryptionKeyAlias = " +
          encryptionKeyAlias +  ", encryptionCipherProvider = " +
          encryptionCipherProvider);
    }
    open = false;
    this.checkpointInterval = Math.max(checkpointInterval, 1000);
    this.maxFileSize = maxFileSize;
    this.queueCapacity = queueCapacity;
    this.useDualCheckpoints = useDualCheckpoints;
    this.checkpointDir = checkpointDir;
    this.backupCheckpointDir = backupCheckpointDir;
    this.logDirs = logDirs;
    this.logWriteTimeout = logWriteTimeout;
    this.checkpointWriteTimeout = checkpointWriteTimeout;
    logFiles = new AtomicReferenceArray<LogFile.Writer>(this.logDirs.length);
    workerExecutor = Executors.newSingleThreadScheduledExecutor(new
      ThreadFactoryBuilder().setNameFormat("Log-BackgroundWorker-" + name)
        .build());
    workerExecutor.scheduleWithFixedDelay(new BackgroundWorker(this),
        this.checkpointInterval, this.checkpointInterval,
        TimeUnit.MILLISECONDS);
  }

  /**
   * Read checkpoint and data files from disk replaying them to the state
   * directly before the shutdown or crash.
   * 使用checkpoint恢复状态, 在启动的时候{@linkplain FileChannel#start()}调用此方法
   * @throws IOException
   */
  void replay() throws IOException {
    Preconditions.checkState(!open, "Cannot replay after Log has been opened");

    Preconditions.checkState(tryLockExclusive(), "Cannot obtain lock on "
        + channelNameDescriptor);

    try {
      /*
       * First we are going to look through the data directories
       * and find all log files. We will store the highest file id
       * (at the end of the filename) we find and use that when we
       * create additional log files.
       *
       * Also store up the list of files so we can replay them later.
       * 读取所有日志文件, 准备replay, 同时计算最大log文件id, 以供后续使用
       */
      LOGGER.info("Replay started");
      nextFileID.set(0);
      List<File> dataFiles = Lists.newArrayList();
      for (File logDir : logDirs) {
        for (File file : LogUtils.getLogs(logDir)) {
          int id = LogUtils.getIDForFile(file);
          dataFiles.add(file);
          nextFileID.set(Math.max(nextFileID.get(), id));
          // 初始化文件读取Reader, 当前支持两个版本的Reader对应两种格式的日志文件,
          // 通过文件元数据可以判断版本
          idLogFileMap.put(id, LogFileFactory.getRandomReader(new File(logDir,
              PREFIX + id), encryptionKeyProvider));
        }
      }
      LOGGER.info("Found NextFileID " + nextFileID +
          ", from " + dataFiles);

      /*
       * sort the data files by file id so we can replay them by file id
       * which should approximately give us sequential events
       * log文件排序, 序号小的创建时间早, 排序靠前
       */
      LogUtils.sort(dataFiles);

      boolean shouldFastReplay = this.useFastReplay;
      /*
       * Read the checkpoint (in memory queue) from one of two alternating
       * locations. We will read the last one written to disk.
       */
      File checkpointFile = new File(checkpointDir, "checkpoint");
      if(shouldFastReplay) {
        if(checkpointFile.exists()) {
          LOGGER.debug("Disabling fast full replay because checkpoint " +
              "exists: " + checkpointFile);
          shouldFastReplay = false;
        } else {
          LOGGER.debug("Not disabling fast full replay because checkpoint " +
              " does not exist: " + checkpointFile);
        }
      }
      // 未commit状态的take操作 checkpoint文件
      File inflightTakesFile = new File(checkpointDir, "inflighttakes");
      // 未commit状态的put操作 checkpoint文件
      File inflightPutsFile = new File(checkpointDir, "inflightputs");
      // 已经事务提交的event队列
      EventQueueBackingStore backingStore = null;


      try {
        // 根据checkpointFile恢复backingStore, 如果checkpoint文件损坏(比如在
        // checkpoint过程中关闭程序导致checkpoint不完整, checkpoint文件与capacity
        // 配置不符 等) 会抛出异常, 其中可以恢复的情况异常封装为BadCheckpointException,
        // 会在后续的catch中使用full play的方法恢复
        backingStore =
            EventQueueBackingStoreFactory.get(checkpointFile,
                backupCheckpointDir, queueCapacity, channelNameDescriptor,
                true, this.useDualCheckpoints);
        // FlumeEventQueue 是一个封装了完整功能的event队列, 负责对外支持事务操作
        queue = new FlumeEventQueue(backingStore, inflightTakesFile,
                inflightPutsFile);
        LOGGER.info("Last Checkpoint " + new Date(checkpointFile.lastModified())
                + ", queue depth = " + queue.getSize());

        /*
         * We now have everything we need to actually replay the log files
         * the queue, the timestamp the queue was written to disk, and
         * the list of data files.
         *
         * This will throw if and only if checkpoint file was fine,
         * but the inflights were not. If the checkpoint was bad, the backing
         * store factory would have thrown.
         *
         * 上面这段注释写清楚了, 到这里checkpoint恢复已经完成了.
         */
        doReplay(queue, dataFiles, encryptionKeyProvider, shouldFastReplay);
      }
      //
      catch (BadCheckpointException ex) {
        backupRestored = false;
        // 使用backupCheckpoint文件恢复
        if (useDualCheckpoints) {
          LOGGER.warn("Checkpoint may not have completed successfully. "
              + "Restoring checkpoint and starting up.", ex);
          if (EventQueueBackingStoreFile.backupExists(backupCheckpointDir)) {
            backupRestored = EventQueueBackingStoreFile.restoreBackup(
              checkpointDir, backupCheckpointDir);
          }
        }
        // 没有backupCheckpoint或者使用backupCheckpoint恢复也失败了, 那么就删除掉所
        // 有的checkpoint文件, 使用full relay
        if (!backupRestored) {
          LOGGER.warn("Checkpoint may not have completed successfully. "
              + "Forcing full replay, this may take a while.", ex);
          if (!Serialization.deleteAllFiles(checkpointDir, EXCLUDES)) {
            throw new IOException("Could not delete files in checkpoint " +
                "directory to recover from a corrupt or incomplete checkpoint");
          }
        }
        backingStore = EventQueueBackingStoreFactory.get(checkpointFile,
            backupCheckpointDir,
            queueCapacity, channelNameDescriptor, true, useDualCheckpoints);
        queue = new FlumeEventQueue(backingStore, inflightTakesFile,
                inflightPutsFile);
        // If the checkpoint was deleted due to BadCheckpointException, then
        // trigger fast replay if the channel is configured to.
        shouldFastReplay = this.useFastReplay;
        doReplay(queue, dataFiles, encryptionKeyProvider, shouldFastReplay);
        if(!shouldFastReplay) {
          didFullReplayDueToBadCheckpointException = true;
        }
      }


      for (int index = 0; index < logDirs.length; index++) {
        LOGGER.info("Rolling " + logDirs[index]);
        roll(index);
      }

      /*
       * Now that we have replayed, write the current queue to disk
       */
      writeCheckpoint(true);

      open = true;
    } catch (Exception ex) {
      LOGGER.error("Failed to initialize Log on " + channelNameDescriptor, ex);
      if (ex instanceof IOException) {
        throw (IOException) ex;
      }
      Throwables.propagate(ex);
    } finally {
      unlockExclusive();
    }
  }

  @SuppressWarnings("deprecation")
  private void doReplay(FlumeEventQueue queue, List<File> dataFiles,
                        KeyProvider encryptionKeyProvider,
                        boolean useFastReplay) throws Exception {
    CheckpointRebuilder rebuilder = new CheckpointRebuilder(dataFiles,
            queue);
    // fastReplay, 关闭或者fastReplay失败都会切回常规replay
    if (useFastReplay && rebuilder.rebuild()) {
      didFastReplay = true;
      LOGGER.info("Fast replay successful.");
    }
    // 常规replay
    else {
      ReplayHandler replayHandler = new ReplayHandler(queue,
              encryptionKeyProvider);
      if (useLogReplayV1) {
        LOGGER.info("Replaying logs with v1 replay logic");
        replayHandler.replayLogv1(dataFiles);
      } else {
        LOGGER.info("Replaying logs with v2 replay logic");
        replayHandler.replayLog(dataFiles);
      }
      readCount = replayHandler.getReadCount();
      putCount = replayHandler.getPutCount();
      takeCount = replayHandler.getTakeCount();
      rollbackCount = replayHandler.getRollbackCount();
      committedCount = replayHandler.getCommitCount();
    }
  }

  @VisibleForTesting
  boolean didFastReplay() {
    return didFastReplay;
  }
  @VisibleForTesting
  public int getReadCount() {
    return readCount;
  }
  @VisibleForTesting
  public int getPutCount() {
    return putCount;
  }

  @VisibleForTesting
  public int getTakeCount() {
    return takeCount;
  }
  @VisibleForTesting
  public int getCommittedCount() {
    return committedCount;
  }
  @VisibleForTesting
  public int getRollbackCount() {
    return rollbackCount;
  }

  /**
   * Was a checkpoint backup used to replay?
   * @return true if a checkpoint backup was used to replay.
   */
  @VisibleForTesting
  boolean backupRestored() {
    return backupRestored;
  }

  @VisibleForTesting
  boolean didFullReplayDueToBadCheckpointException() {
    return didFullReplayDueToBadCheckpointException;
  }

  int getNextFileID() {
    Preconditions.checkState(open, "Log is closed");
    return nextFileID.get();
  }

  FlumeEventQueue getFlumeEventQueue() {
    Preconditions.checkState(open, "Log is closed");
    return queue;
  }

  /**
   * Return the FlumeEvent for an event pointer. This method is
   * non-transactional. It is assumed the client has obtained this
   * FlumeEventPointer via FlumeEventQueue.
   *
   * @param pointer
   * @return FlumeEventPointer
   * @throws IOException
   * @throws InterruptedException
   */
  FlumeEvent get(FlumeEventPointer pointer) throws IOException,
    InterruptedException, NoopRecordException {
    Preconditions.checkState(open, "Log is closed");
    int id = pointer.getFileID();
    LogFile.RandomReader logFile = idLogFileMap.get(id);
    Preconditions.checkNotNull(logFile, "LogFile is null for id " + id);
    try {
      return logFile.get(pointer.getOffset());
    } catch (CorruptEventException ex) {
      open = false;
      throw new IOException("Corrupt event found. Please run File Channel " +
        "Integrity tool.", ex);
    }
  }

  /**
   * Log a put of an event
   *
   * Synchronization not required as this method is atomic
   *
   * 写入一个put event到日志
   *
   * 报文格式为:
   *
   * |----------------Put-------------------|
   * |  |---------FlumeEvent-------------|  |
   * |  |                                |  |
   * |  |   |--FlumeEventHeader * n--|   |  |
   * |  |   |------------------------|   |  |
   * |  |                                |  |
   * |  |   |-----bytes(body)--------|   |  |
   * |  |   |------------------------|   |  |
   * |  |                                |  |
   * |  |--------------------------------|  |
   * |                                      |
   * |  |-------sfixed64(checksum)-------|  |
   * |  |                                |  |
   * |  |--------------------------------|  |
   * |--------------------------------------|
   *
   * @param transactionID
   * @param event
   * @return
   * @throws IOException
   */
  FlumeEventPointer put(long transactionID, Event event)
      throws IOException {
    Preconditions.checkState(open, "Log is closed");
    FlumeEvent flumeEvent = new FlumeEvent(
        event.getHeaders(), event.getBody());
    // Put对象为一个Put操作的所有信息, 包含三个属性: 事务ID, 写日志文件ID, event实体
    Put put = new Put(transactionID, WriteOrderOracle.next(), flumeEvent);

    // 将Put对象编译为protobuf格式的ByteBuffer
    ByteBuffer buffer = TransactionEventRecord.toByteBuffer(put);

    // 事务号对文件数取余 得到 将要用于落地的日志文件编号
    int logFileIndex = nextLogWriter(transactionID);
    // 日志文件剩余可用空间
    long usableSpace = logFiles.get(logFileIndex).getUsableSpace();
    // 可用空间检查
    long requiredSpace = minimumRequiredSpace + buffer.limit();
    if(usableSpace <= requiredSpace) {
      throw new IOException("Usable space exhaused, only " + usableSpace +
          " bytes remaining, required " + requiredSpace + " bytes");
    }
    boolean error = true;
    try {
      try {
        /** 底层通过{@linkplain LogFile.Writer#write(ByteBuffer)} 将byteBuffer写
         * 入文件 */
        FlumeEventPointer ptr = logFiles.get(logFileIndex).put(buffer);
        error = false;
        return ptr;
      }
      // 可重试异常, 则重试一次
      catch (LogFileRetryableIOException e) {
        if(!open) {
          throw e;
        }
        roll(logFileIndex, buffer);
        FlumeEventPointer ptr = logFiles.get(logFileIndex).put(buffer);
        error = false;
        return ptr;
      }
    } finally {
      if(error && open) {
        roll(logFileIndex);
      }
    }
  }

  /**
   * Log a take of an event, pointer points at the corresponding put
   *
   * Synchronization not required as this method is atomic
   *
   * 写入一个take event到日志, 操作类似于{@linkplain this#put(long, Event)}
   *
   * 报文格式为:
   * |----------------Take------------------|
   * |  |-------sfixed64(fileID)---------|  |
   * |  |                                |  |
   * |  |--------------------------------|  |
   * |                                      |
   * |  |-------sfixed64(offset)---------|  |
   * |  |                                |  |
   * |  |--------------------------------|  |
   * |--------------------------------------|
   *
   * @param transactionID
   * @param pointer
   * @throws IOException
   */
  void take(long transactionID, FlumeEventPointer pointer)
      throws IOException {
    Preconditions.checkState(open, "Log is closed");
    Take take = new Take(transactionID, WriteOrderOracle.next(),
        pointer.getOffset(), pointer.getFileID());
    ByteBuffer buffer = TransactionEventRecord.toByteBuffer(take);
    int logFileIndex = nextLogWriter(transactionID);
    long usableSpace = logFiles.get(logFileIndex).getUsableSpace();
    long requiredSpace = minimumRequiredSpace + buffer.limit();
    if(usableSpace <= requiredSpace) {
      throw new IOException("Usable space exhaused, only " + usableSpace +
          " bytes remaining, required " + requiredSpace + " bytes");
    }
    boolean error = true;
    try {
      try {
        logFiles.get(logFileIndex).take(buffer);
        error = false;
      } catch (LogFileRetryableIOException e) {
        if(!open) {
          throw e;
        }
        roll(logFileIndex, buffer);
        logFiles.get(logFileIndex).take(buffer);
        error = false;
      }
    } finally {
      if(error && open) {
        roll(logFileIndex);
      }
    }
  }

  /**
   * Log a rollback of a transaction
   *
   * Synchronization not required as this method is atomic
   *
   * 写入一个rollback操作到日志文件, 操作类似于{@linkplain this#put(long, Event)}
   *
   * 报文格式为(为空):
   * |----------------RollBack--------------|
   * |                                      |
   * |--------------------------------------|
   *
   * @param transactionID
   * @throws IOException
   */
  void rollback(long transactionID) throws IOException {
    Preconditions.checkState(open, "Log is closed");

    if(LOGGER.isDebugEnabled()) {
      LOGGER.debug("Rolling back " + transactionID);
    }
    Rollback rollback = new Rollback(transactionID, WriteOrderOracle.next());
    ByteBuffer buffer = TransactionEventRecord.toByteBuffer(rollback);
    int logFileIndex = nextLogWriter(transactionID);
    long usableSpace = logFiles.get(logFileIndex).getUsableSpace();
    long requiredSpace = minimumRequiredSpace + buffer.limit();
    if(usableSpace <= requiredSpace) {
      throw new IOException("Usable space exhaused, only " + usableSpace +
          " bytes remaining, required " + requiredSpace + " bytes");
    }
    boolean error = true;
    try {
      try {
        logFiles.get(logFileIndex).rollback(buffer);
        error = false;
      } catch (LogFileRetryableIOException e) {
        if(!open) {
          throw e;
        }
        roll(logFileIndex, buffer);
        logFiles.get(logFileIndex).rollback(buffer);
        error = false;
      }
    } finally {
      if(error && open) {
        roll(logFileIndex);
      }
    }
  }

  /**
   * Log commit of put, we need to know which type of commit
   * so we know if the pointers corresponding to the events
   * should be added or removed from the flume queue. We
   * could infer but it's best to be explicit.
   *
   * Synchronization not required as this method is atomic
   *
   * 写入一个commit put操作到日志文件, 因为一个事务只能包含put 或者 take操作 二选一,
   * 将两者分开, 在事务回滚的时候更容易将对应的event从queue移除或者恢复
   * @param transactionID
   * @throws IOException
   * @throws InterruptedException
   */
  void commitPut(long transactionID) throws IOException,
  InterruptedException {
    Preconditions.checkState(open, "Log is closed");
    commit(transactionID, TransactionEventRecord.Type.PUT.get());
  }

  /**
   * Log commit of take, we need to know which type of commit
   * so we know if the pointers corresponding to the events
   * should be added or removed from the flume queue. We
   * could infer but it's best to be explicit.
   *
   * Synchronization not required as this method is atomic
   * 写入一个commit take操作到日志文件, 因为一个事务只能包含put 或者 take操作 二选一,
   * 将两者分开, 在事务回滚的时候更容易将对应的event从queue移除或者恢复
   * @param transactionID
   * @throws IOException
   * @throws InterruptedException
   */
  void commitTake(long transactionID) throws IOException,
  InterruptedException {
    Preconditions.checkState(open, "Log is closed");
    commit(transactionID, TransactionEventRecord.Type.TAKE.get());
  }


  private boolean tryLockExclusive() {
    try {
      return checkpointWriterLock.tryLock(checkpointWriteTimeout,
          TimeUnit.SECONDS);
    } catch (InterruptedException ex) {
      LOGGER.warn("Interrupted while waiting for log exclusive lock", ex);
      Thread.currentThread().interrupt();
    }
    return false;
  }
  private void unlockExclusive()  {
    checkpointWriterLock.unlock();
  }

  boolean tryLockShared() {
    try {
      return checkpointReadLock.tryLock(logWriteTimeout, TimeUnit.SECONDS);
    } catch (InterruptedException ex) {
      LOGGER.warn("Interrupted while waiting for log shared lock", ex);
      Thread.currentThread().interrupt();
    }
    return false;
  }

  void unlockShared()  {
    checkpointReadLock.unlock();
  }

  private void lockExclusive(){
    checkpointWriterLock.lock();
  }

  /**
   * Synchronization not required since this method gets the write lock,
   * so checkpoint and this method cannot run at the same time.
   */
  void close() throws IOException{
    lockExclusive();
    try {
      open = false;
      shutdownWorker();
      if (logFiles != null) {
        for (int index = 0; index < logFiles.length(); index++) {
          LogFile.Writer writer = logFiles.get(index);
          if(writer != null) {
            writer.close();
          }
        }
      }
      synchronized (idLogFileMap) {
        for (Integer logId : idLogFileMap.keySet()) {
          LogFile.RandomReader reader = idLogFileMap.get(logId);
          if (reader != null) {
            reader.close();
          }
        }
      }
      queue.close();
      try {
        unlock(checkpointDir);
      } catch (IOException ex) {
        LOGGER.warn("Error unlocking " + checkpointDir, ex);
      }
      if (useDualCheckpoints) {
        try {
          unlock(backupCheckpointDir);
        } catch (IOException ex) {
          LOGGER.warn("Error unlocking " + checkpointDir, ex);
        }
      }
      for (File logDir : logDirs) {
        try {
          unlock(logDir);
        } catch (IOException ex) {
          LOGGER.warn("Error unlocking " + logDir, ex);
        }
      }
    } finally {
      unlockExclusive();
    }
  }

  void shutdownWorker() {
    String msg = "Attempting to shutdown background worker.";
    System.out.println(msg);
    LOGGER.info(msg);
    workerExecutor.shutdown();
    try {
      workerExecutor.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOGGER.error("Interrupted while waiting for worker to die.");
    }
  }
  void setCheckpointInterval(long checkpointInterval) {
    this.checkpointInterval = checkpointInterval;
  }
  void setMaxFileSize(long maxFileSize) {
    this.maxFileSize = maxFileSize;
  }

  /**
   * Synchronization not required as this method is atomic
   *
   * 写入一个commit操作到日志文件, 操作类似于{@linkplain this#put(long, Event)}
   *
   * 报文格式为:
   * |----------------Commit----------------|
   * |  |-------sfixed64(type)-----------|  |
   * |  |                                |  |
   * |  |--------------------------------|  |
   * |--------------------------------------|
   * @param transactionID
   * @param type
   * @throws IOException
   */
  private void commit(long transactionID, short type) throws IOException {
    Preconditions.checkState(open, "Log is closed");
    Commit commit = new Commit(transactionID, WriteOrderOracle.next(), type);
    ByteBuffer buffer = TransactionEventRecord.toByteBuffer(commit);
    int logFileIndex = nextLogWriter(transactionID);
    long usableSpace = logFiles.get(logFileIndex).getUsableSpace();
    long requiredSpace = minimumRequiredSpace + buffer.limit();
    if(usableSpace <= requiredSpace) {
      throw new IOException("Usable space exhaused, only " + usableSpace +
          " bytes remaining, required " + requiredSpace + " bytes");
    }
    boolean error = true;
    try {
      try {
        LogFile.Writer logFileWriter = logFiles.get(logFileIndex);
        // If multiple transactions are committing at the same time,
        // this ensures that the number of actual fsyncs is small and a
        // number of them are grouped together into one.
        logFileWriter.commit(buffer);
        // 持久化到磁盘(FileChannel -> Disk)
        logFileWriter.sync();
        error = false;
      } catch (LogFileRetryableIOException e) {
        if(!open) {
          throw e;
        }
        roll(logFileIndex, buffer);
        LogFile.Writer logFileWriter = logFiles.get(logFileIndex);
        logFileWriter.commit(buffer);
        logFileWriter.sync();
        error = false;
      }
    } finally {
      if(error && open) {
        roll(logFileIndex);
      }
    }
  }


  /**
   * Atomic so not synchronization required.
   * @return
   */
  private int nextLogWriter(long transactionID) {
    return (int)Math.abs(transactionID % (long)logFiles.length());
  }
  /**
   * Unconditionally roll
   * Synchronization done internally
   *
   * @param index
   * @throws IOException
   */
  private void roll(int index) throws IOException {
    roll(index, null);
  }
  /**
   * Roll a log if needed. Roll always occurs if the log at the index
   * does not exist (typically on startup), or buffer is null. Otherwise
   * LogFile.Writer.isRollRequired is checked again to ensure we don't
   * have threads pile up on this log resulting in multiple successive
   * rolls
   *
   * Synchronization required since both synchronized and unsynchronized
   * methods call this method, and this method acquires only a
   * read lock. The synchronization guarantees that multiple threads don't
   * roll at the same time.
   *
   * @param index
   * @throws IOException
   */
    private synchronized void roll(int index, ByteBuffer buffer)
      throws IOException {
    if (!tryLockShared()) {
      throw new ChannelException("Failed to obtain lock for writing to the "
          + "log. Try increasing the log write timeout value. " +
          channelNameDescriptor);
    }

    try {
      LogFile.Writer oldLogFile = logFiles.get(index);
      // check to make sure a roll is actually required due to
      // the possibility of multiple writes waiting on lock
      if(oldLogFile == null || buffer == null ||
          oldLogFile.isRollRequired(buffer)) {
        try {
          LOGGER.info("Roll start " + logDirs[index]);
          int fileID = nextFileID.incrementAndGet();
          File file = new File(logDirs[index], PREFIX + fileID);
          LogFile.Writer writer = LogFileFactory.getWriter(file, fileID,
              maxFileSize, encryptionKey, encryptionKeyAlias,
              encryptionCipherProvider, usableSpaceRefreshInterval);
          idLogFileMap.put(fileID, LogFileFactory.getRandomReader(file,
              encryptionKeyProvider));
          // writer from this point on will get new reference
          logFiles.set(index, writer);
          // close out old log
          if (oldLogFile != null) {
            oldLogFile.close();
          }
        } finally {
          LOGGER.info("Roll end");
        }
      }
    } finally {
      unlockShared();
    }
  }

  private boolean writeCheckpoint() throws Exception {
    return writeCheckpoint(false);
  }

  /**
   * Write the current checkpoint object and then swap objects so that
   * the next checkpoint occurs on the other checkpoint directory.
   *
   * Synchronization is not required because this method acquires a
   * write lock. So this method gets exclusive access to all the
   * data structures this method accesses.
   *
   * 将当前queue状态写入快照相关文件.
   *
   * @param force  a flag to force the writing of checkpoint 是否非写不可, 在启动
   *               replay之后就会调用一次非写不可的checkpoint写入, 其他情况下是通过一
   *               个独立的线程来定时非强制写入checkpoint
   * @throws IOException if we are unable to write the checkpoint out to disk
   */
  private Boolean writeCheckpoint(Boolean force) throws Exception {
    boolean checkpointCompleted = false;
    long usableSpace = checkpointDir.getUsableSpace();
    if(usableSpace <= minimumRequiredSpace) {
      throw new IOException("Usable space exhaused, only " + usableSpace +
          " bytes remaining, required " + minimumRequiredSpace + " bytes");
    }
    // 独占锁, 保证线程安全
    boolean lockAcquired = tryLockExclusive();
    if(!lockAcquired) {
      return false;
    }
    SortedSet<Integer> logFileRefCountsAll = null, logFileRefCountsActive = null;
    try {
      // 先写入checkpoint文件
      if (queue.checkpoint(force)) {
        // 接下来遍历所有活跃的log文件, 更新他们的信息写入到对应meta文件
        long logWriteOrderID = queue.getLogWriteOrderID();

        //Since the active files might also be in the queue's fileIDs,
        //we need to either move each one to a new set or remove each one
        //as we do here. Otherwise we cannot make sure every element in
        //fileID set from the queue have been updated.
        //Since clone is smarter than insert, better to make
        //a copy of the set first so that we can use it later.
        logFileRefCountsAll = queue.getFileIDs();
        logFileRefCountsActive = new TreeSet<Integer>(logFileRefCountsAll);

        int numFiles = logFiles.length();
        for (int i = 0; i < numFiles; i++) {
          LogFile.Writer logWriter = logFiles.get(i);
          int logFileID = logWriter.getLogFileID();
          File logFile = logWriter.getFile();
          LogFile.MetaDataWriter writer =
              LogFileFactory.getMetaDataWriter(logFile, logFileID);
          try {
            writer.markCheckpoint(logWriter.position(), logWriteOrderID);
          } finally {
            writer.close();
          }
          logFileRefCountsAll.remove(logFileID);
          LOGGER.info("Updated checkpoint for file: " + logFile + " position: "
              + logWriter.position() + " logWriteOrderID: " + logWriteOrderID);
        }

        // Update any inactive data files as well
        Iterator<Integer> idIterator = logFileRefCountsAll.iterator();
        while (idIterator.hasNext()) {
          int id = idIterator.next();
          LogFile.RandomReader reader = idLogFileMap.remove(id);
          File file = reader.getFile();
          reader.close();
          LogFile.MetaDataWriter writer =
              LogFileFactory.getMetaDataWriter(file, id);
          try {
            writer.markCheckpoint(logWriteOrderID);
          } finally {
            writer.close();
          }
          reader = LogFileFactory.getRandomReader(file, encryptionKeyProvider);
          idLogFileMap.put(id, reader);
          LOGGER.debug("Updated checkpoint for file: " + file
              + "logWriteOrderID " + logWriteOrderID);
          idIterator.remove();
        }
        Preconditions.checkState(logFileRefCountsAll.size() == 0,
                "Could not update all data file timestamps: " + logFileRefCountsAll);
        //Add files from all log directories
        for (int index = 0; index < logDirs.length; index++) {
          logFileRefCountsActive.add(logFiles.get(index).getLogFileID());
        }
        checkpointCompleted = true;
      }
    } finally {
      unlockExclusive();
    }
    //Do the deletes outside the checkpointWriterLock
    //Delete logic is expensive.
    // 在checkpoint完成之后处理过期log文件的清理工作
    if (open && checkpointCompleted) {
      removeOldLogs(logFileRefCountsActive);
    }
    //Since the exception is not caught, this will not be returned if
    //an exception is thrown from the try.
    return true;
  }

  /**
   * 移除无用的旧log文件, 为了兼容backup checkpoint的模式, 清除工作选择了先标记, 下次
   * 清除的模式
   * @param fileIDs 所有仍在使用的日志文件id
   */
  private void removeOldLogs(SortedSet<Integer> fileIDs) {
    Preconditions.checkState(open, "Log is closed");
    // To maintain a single code path for deletes, if backup of checkpoint is
    // enabled or not, we will track the files which can be deleted after the
    // current checkpoint (since the one which just got backed up still needs
    // these files) and delete them only after the next (since the current
    // checkpoint will become the backup at that time,
    // and thus these files are no longer needed).
    for(File fileToDelete : pendingDeletes) {
      LOGGER.info("Removing old file: " + fileToDelete);
      // deleteQuietly方法会删除指定文件or目录, 成功或者失败均不会抛出异常, 静默删除
      FileUtils.deleteQuietly(fileToDelete);
    }
    pendingDeletes.clear();
    // we will find the smallest fileID currently in use and
    // won't delete any files with an id larger than the min
    int minFileID = fileIDs.first();
    LOGGER.debug("Files currently in use: " + fileIDs);
    for(File logDir : logDirs) {
      List<File> logs = LogUtils.getLogs(logDir);
      // sort oldset to newest
      LogUtils.sort(logs);
      // ensure we always keep two logs per dir
      int size = logs.size() - MIN_NUM_LOGS;
      for (int index = 0; index < size; index++) {
        File logFile = logs.get(index);
        int logFileID = LogUtils.getIDForFile(logFile);
        if(logFileID < minFileID) {
          LogFile.RandomReader reader = idLogFileMap.remove(logFileID);
          if(reader != null) {
            reader.close();
          }
          File metaDataFile = Serialization.getMetaDataFile(logFile);
          pendingDeletes.add(logFile);
          pendingDeletes.add(metaDataFile);
        }
      }
    }
  }
  /**
   * Lock storage to provide exclusive access.
   *
   * <p> Locking is not supported by all file systems.
   * E.g., NFS does not consistently support exclusive locks.
   *
   * <p> If locking is supported we guarantee exculsive access to the
   * storage directory. Otherwise, no guarantee is given.
   *
   * 使用文件锁锁住一个目录
   * 注意到这里 部分文件系统不支持排它锁, 所以做了一次secondLock排他性检测, 不支持的时
   * 候打出warn日志, 但是实际上还是当获取锁来处理, 这里比较危险
   *
   * @throws IOException if locking fails
   */
  private void lock(File dir) throws IOException {
    FileLock lock = tryLock(dir);
    if (lock == null) {
      String msg = "Cannot lock " + dir
          + ". The directory is already locked. "
          + channelNameDescriptor;
      LOGGER.info(msg);
      throw new IOException(msg);
    }
    // 二次检测排他性
    FileLock secondLock = tryLock(dir);
    if(secondLock != null) {
      LOGGER.warn("Directory "+dir+" does not support locking");
      secondLock.release();
      secondLock.channel().close();
    }
    locks.put(dir.getAbsolutePath(), lock);
  }

  /**
   * Attempts to acquire an exclusive lock on the directory.
   *
   * @return A lock object representing the newly-acquired lock or
   * <code>null</code> if directory is already locked.
   * @throws IOException if locking fails.
   */
  @SuppressWarnings("resource")
  private FileLock tryLock(File dir) throws IOException {
    File lockF = new File(dir, FILE_LOCK);
    lockF.deleteOnExit();
    RandomAccessFile file = new RandomAccessFile(lockF, "rws");
    FileLock res = null;
    try {
      // nio 提供的一个非常重要的基于文件的分布式锁
      res = file.getChannel().tryLock();
    } catch(OverlappingFileLockException oe) {
      file.close();
      return null;
    } catch(IOException e) {
      LOGGER.error("Cannot create lock on " + lockF, e);
      file.close();
      throw e;
    }
    return res;
  }

  /**
   * Unlock directory.
   *
   * @throws IOException
   */
  private void unlock(File dir) throws IOException {
    FileLock lock = locks.remove(dir.getAbsolutePath());
    if(lock == null) {
      return;
    }
    lock.release();
    lock.channel().close();
    lock = null;
  }
  static class BackgroundWorker implements Runnable {
    private static final Logger LOG = LoggerFactory
        .getLogger(BackgroundWorker.class);
    private final Log log;

    public BackgroundWorker(Log log) {
      this.log = log;
    }

    @Override
    public void run() {
      try {
        if (log.open) {
          log.writeCheckpoint();
        }
      } catch (IOException e) {
        LOG.error("Error doing checkpoint", e);
      } catch (Throwable e) {
        LOG.error("General error in checkpoint worker", e);
      }
    }
  }
}
