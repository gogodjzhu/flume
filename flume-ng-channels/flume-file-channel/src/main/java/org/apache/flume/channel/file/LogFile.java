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
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.flume.ChannelException;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.channel.file.encryption.CipherProvider;
import org.apache.flume.channel.file.encryption.KeyProvider;
import org.apache.flume.tools.DirectMemoryUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

@InterfaceAudience.Private
@InterfaceStability.Unstable
/**
 * 日志文件抽象, 代表磁盘中的一个log文件
 */
public abstract class LogFile {

  private static final Logger LOG = LoggerFactory
      .getLogger(LogFile.class);


  /**
   * This class preallocates the data files 1MB at time to avoid
   * the updating of the inode on each write and to avoid the disk
   * filling up during a write. It's also faster, so there.
   */
  private static final ByteBuffer FILL = DirectMemoryUtils.
      allocate(1024 * 1024); // preallocation, 1MB

  public static final byte OP_RECORD = Byte.MAX_VALUE;
  public static final byte OP_NOOP = (Byte.MAX_VALUE + Byte.MIN_VALUE)/2;
  public static final byte OP_EOF = Byte.MIN_VALUE;

  static {
    for (int i = 0; i < FILL.capacity(); i++) {
      FILL.put(OP_EOF);
    }
  }

  protected static void skipRecord(RandomAccessFile fileHandle,
    int offset) throws IOException {
    fileHandle.seek(offset);
    int length = fileHandle.readInt();
    fileHandle.skipBytes(length);
  }

  abstract static class MetaDataWriter {
    private final File file;
    private final int logFileID;
    private final RandomAccessFile writeFileHandle;

    private long lastCheckpointOffset;
    private long lastCheckpointWriteOrderID;

    protected MetaDataWriter(File file, int logFileID) throws IOException {
      this.file = file;
      this.logFileID = logFileID;
      writeFileHandle = new RandomAccessFile(file, "rw");

    }
    protected RandomAccessFile getFileHandle() {
      return writeFileHandle;
    }
    protected void setLastCheckpointOffset(long lastCheckpointOffset) {
      this.lastCheckpointOffset = lastCheckpointOffset;
    }
    protected void setLastCheckpointWriteOrderID(long lastCheckpointWriteOrderID) {
      this.lastCheckpointWriteOrderID = lastCheckpointWriteOrderID;
    }
    protected long getLastCheckpointOffset() {
      return lastCheckpointOffset;
    }
    protected long getLastCheckpointWriteOrderID() {
      return lastCheckpointWriteOrderID;
    }
    protected File getFile() {
      return file;
    }
    protected int getLogFileID() {
      return logFileID;
    }
    void markCheckpoint(long logWriteOrderID)
        throws IOException {
      markCheckpoint(lastCheckpointOffset, logWriteOrderID);
    }
    // 每个log文件对应有一个.meta文件保存元数据, 本方法就是将最新的元数据覆写到指定mera文件
    abstract void markCheckpoint(long currentPosition, long logWriteOrderID)
        throws IOException;

    abstract int getVersion();

    void close() {
      try {
        writeFileHandle.close();
      } catch (IOException e) {
        LOG.warn("Unable to close " + file, e);
      }
    }
  }

  @VisibleForTesting
  static class CachedFSUsableSpace {
    private final File fs;
    private final long interval;
    private final AtomicLong lastRefresh;
    private final AtomicLong value;

    CachedFSUsableSpace(File fs, long interval) {
      this.fs = fs;
      this.interval = interval;
      this.value = new AtomicLong(fs.getUsableSpace());
      this.lastRefresh = new AtomicLong(System.currentTimeMillis());
    }

    void decrement(long numBytes) {
      Preconditions.checkArgument(numBytes >= 0, "numBytes less than zero");
      value.addAndGet(-numBytes);
    }

    /**
     * 获取文件剩余可用空间(字节), 在单位时间interval(默认15秒)内多次调用会会用缓存结果
     * @return
     */
    long getUsableSpace() {
      long now = System.currentTimeMillis();
      if(now - interval > lastRefresh.get()) {
        value.set(fs.getUsableSpace());
        lastRefresh.set(now);
      }
      return Math.max(value.get(), 0L);
    }
  }

  static abstract class Writer {
    private final int logFileID;
    private final File file;
    private final long maxFileSize;
    private final RandomAccessFile writeFileHandle;
    private final FileChannel writeFileChannel;
    private final CipherProvider.Encryptor encryptor;
    private final CachedFSUsableSpace usableSpace;
    private volatile boolean open;
    private long lastCommitPosition;
    private long lastSyncPosition;

    // To ensure we can count the number of fsyncs.
    private long syncCount;


    Writer(File file, int logFileID, long maxFileSize,
        CipherProvider.Encryptor encryptor, long usableSpaceRefreshInterval)
        throws IOException {
      this.file = file;
      this.logFileID = logFileID;
      this.maxFileSize = Math.min(maxFileSize,
          FileChannelConfiguration.DEFAULT_MAX_FILE_SIZE);
      this.encryptor = encryptor;
      writeFileHandle = new RandomAccessFile(file, "rw");
      writeFileChannel = writeFileHandle.getChannel();
      usableSpace = new CachedFSUsableSpace(file, usableSpaceRefreshInterval);
      LOG.info("Opened " + file);
      open = true;
    }

    abstract int getVersion();

    protected CipherProvider.Encryptor getEncryptor() {
      return encryptor;
    }
    int getLogFileID() {
      return logFileID;
    }

    File getFile() {
      return file;
    }
    String getParent() {
      return file.getParent();
    }

    long getUsableSpace() {
      return usableSpace.getUsableSpace();
    }

    long getMaxSize() {
      return maxFileSize;
    }

    @VisibleForTesting
    long getLastCommitPosition(){
      return lastCommitPosition;
    }

    @VisibleForTesting
    long getLastSyncPosition() {
      return lastSyncPosition;
    }

    @VisibleForTesting
    long getSyncCount() {
      return syncCount;
    }
    synchronized long position() throws IOException {
      return getFileChannel().position();
    }

    // encrypt and write methods may not be thread safe in the following
    // methods, so all methods need to be synchronized.

    synchronized FlumeEventPointer put(ByteBuffer buffer) throws IOException {
      if(encryptor != null) {
        buffer = ByteBuffer.wrap(encryptor.encrypt(buffer.array()));
      }
      Pair<Integer, Integer> pair = write(buffer);
      return new FlumeEventPointer(pair.getLeft(), pair.getRight());
    }
    synchronized void take(ByteBuffer buffer) throws IOException {
      if(encryptor != null) {
        buffer = ByteBuffer.wrap(encryptor.encrypt(buffer.array()));
      }
      write(buffer);
    }
    synchronized void rollback(ByteBuffer buffer) throws IOException {
      if(encryptor != null) {
        buffer = ByteBuffer.wrap(encryptor.encrypt(buffer.array()));
      }
      write(buffer);
    }

    synchronized void commit(ByteBuffer buffer) throws IOException {
      if (encryptor != null) {
        buffer = ByteBuffer.wrap(encryptor.encrypt(buffer.array()));
      }
      write(buffer);
      lastCommitPosition = position();
    }

    /**
     * 将ByteBuffer写入文件, 使用的是{@linkplain FileChannel}提供方法
     * @return 返回Pair<日志文件ID, Buffer开始位置偏移量>
     */
    private Pair<Integer, Integer> write(ByteBuffer buffer)
      throws IOException {
      // 文件未打开, 可以重试再来一次
      if(!isOpen()) {
        throw new LogFileRetryableIOException("File closed " + file);
      }
      // FileChannel当前的position
      long length = position();
      // 检查文件可用容量是否足够
      long expectedLength = length + (long) buffer.limit();
      if(expectedLength > maxFileSize) {
        throw new LogFileRetryableIOException(expectedLength + " > " +
            maxFileSize);
      }
      int offset = (int)length;
      Preconditions.checkState(offset >= 0, String.valueOf(offset));
      // OP_RECORD(Byte.MAX_VALUE) + size + buffer
      int recordLength = 1 + (int)Serialization.SIZE_OF_INT + buffer.limit();
      // 减少可用空间
      usableSpace.decrement(recordLength);
      // 预分配空间
      preallocate(recordLength);
      // 构造buffer并写入FileChannel
      ByteBuffer toWrite = ByteBuffer.allocate(recordLength);
      toWrite.put(OP_RECORD);
      writeDelimitedBuffer(toWrite, buffer);
      toWrite.position(0);
      int wrote = getFileChannel().write(toWrite);
      Preconditions.checkState(wrote == toWrite.limit());
      return Pair.of(getLogFileID(), offset);
    }

    synchronized boolean isRollRequired(ByteBuffer buffer) throws IOException {
      return isOpen() && position() + (long) buffer.limit() > getMaxSize();
    }

    /**
     * Sync the underlying log file to disk. Expensive call,
     * should be used only on commits. If a sync has already happened after
     * the last commit, this method is a no-op
     * @throws IOException
     * @throws LogFileRetryableIOException - if this log file is closed.
     */
    synchronized void sync() throws IOException {
      if (!isOpen()) {
        throw new LogFileRetryableIOException("File closed " + file);
      }
      if (lastSyncPosition < lastCommitPosition) {
        getFileChannel().force(false);
        lastSyncPosition = position();
        syncCount++;
      }
    }


    protected boolean isOpen() {
      return open;
    }
    protected RandomAccessFile getFileHandle() {
      return writeFileHandle;
    }
    protected FileChannel getFileChannel() {
      return writeFileChannel;
    }
    synchronized void close() {
      if(open) {
        open = false;
        if(writeFileChannel.isOpen()) {
          LOG.info("Closing " + file);
          try {
            writeFileChannel.force(true);
          } catch (IOException e) {
            LOG.warn("Unable to flush to disk " + file, e);
          }
          try {
            writeFileHandle.close();
          } catch (IOException e) {
            LOG.warn("Unable to close " + file, e);
          }
        }
      }
    }

    /**
     * 为了防止每次write操作都做一次空间申请, 空间不足时会预分配1m的存储空间
     * @param size
     * @throws IOException
     */
    protected void preallocate(int size) throws IOException {
      long position = position();
      if(position + size > getFileChannel().size()) {
        LOG.debug("Preallocating at position " + position);
        synchronized (FILL) {
          FILL.position(0);
          getFileChannel().write(FILL, position);
        }
      }
    }
  }

  /**
   * This is an class meant to be an internal Flume API,
   * and can change at any time. Intended to be used only from File Channel  Integrity
   * test tool. Not to be used for any other purpose.
   */
  public static class OperationRecordUpdater {
    private final RandomAccessFile fileHandle;
    private final File file;

    public OperationRecordUpdater(File file) throws FileNotFoundException {
      Preconditions.checkState(file.exists(), "File to update, " +
        file.toString() + " does not exist.");
      this.file = file;
      fileHandle = new RandomAccessFile(file, "rw");
    }

    public void markRecordAsNoop(long offset) throws IOException {
      // First ensure that the offset actually is an OP_RECORD. There is a
      // small possibility that it still is OP_RECORD,
      // but is not actually the beginning of a record. Is there anything we
      // can do about it?
      fileHandle.seek(offset);
      byte byteRead = fileHandle.readByte();
      Preconditions.checkState(byteRead == OP_RECORD || byteRead == OP_NOOP,
        "Expected to read a record but the byte read indicates EOF");
      fileHandle.seek(offset);
      LOG.info("Marking event as " + OP_NOOP + " at " + offset + " for file " +
        file.toString());
      fileHandle.writeByte(OP_NOOP);
    }

    public void close() {
      try {
        fileHandle.getFD().sync();
        fileHandle.close();
      } catch (IOException e) {
        LOG.error("Could not close file handle to file " +
          fileHandle.toString(), e);
      }
    }
  }

  /**
   * log文件随机只读Reader, 抽象类
   * 当前版本支持v2和v3两个版本, 新的日志文件使用v3, 子类需要实现各自的
   * {@link this#doGet(RandomAccessFile)}方法以实现底层协议相关的解析处理
   *
   * RandomReader的特点是可以读取任意offset的数据, 底层通过
   * {@link RandomAccessFile#seek(long)}来定位数据
   */
  static abstract class RandomReader {
    private final File file;
    private final BlockingQueue<RandomAccessFile> readFileHandles =
        new ArrayBlockingQueue<RandomAccessFile>(50, true);
    private final KeyProvider encryptionKeyProvider;
    private volatile boolean open;
    public RandomReader(File file, @Nullable KeyProvider encryptionKeyProvider)
        throws IOException {
      this.file = file;
      this.encryptionKeyProvider = encryptionKeyProvider;
      // 预先创建一个文件读取实例
      readFileHandles.add(open());
      open = true;
    }

    /**
     * 留给不同版本的子类实现各自的get逻辑
     */
    protected abstract TransactionEventRecord doGet(RandomAccessFile fileHandle)
        throws IOException, CorruptEventException;

    abstract int getVersion();

    File getFile() {
      return file;
    }

    protected KeyProvider getKeyProvider() {
      return encryptionKeyProvider;
    }

    /**
     * 获取文件指定offset保存的Event
     * @param offset
     * @return
     * @throws IOException
     * @throws InterruptedException
     * @throws CorruptEventException
     * @throws NoopRecordException
     */
    FlumeEvent get(int offset) throws IOException, InterruptedException,
      CorruptEventException, NoopRecordException {
      Preconditions.checkState(open, "File closed");
      // 获取读取实例, 多线程同时读取可能会take阻塞等待
      RandomAccessFile fileHandle = checkOut();
      boolean error = true;
      try {
        // 定位读起始位
        fileHandle.seek(offset);
        byte operation = fileHandle.readByte();
        if(operation == OP_NOOP) {
          throw new NoopRecordException("No op record found. Corrupt record " +
            "may have been repaired by File Channel Integrity tool");
        }
        // record的开头标记符
        Preconditions.checkState(operation == OP_RECORD,
            Integer.toHexString(operation));
        // 调用子类实现的方法解析文件流为Envet对象
        TransactionEventRecord record = doGet(fileHandle);
        // 本类是只读的Reader, 所以只能获取Put操作, 别的操作不应该从这里获取?
        /** 本方法只会被{@link org.apache.flume.channel.file.FileChannel#doTake()},
         * 调用, 所以事实上也应该只获取到Put Event*/
        if(!(record instanceof Put)) {
          Preconditions.checkState(false, "Record is " +
              record.getClass().getSimpleName());
        }
        error = false;
        return ((Put)record).getEvent();
      } finally {
        if(error) {
          close(fileHandle, file);
        } else {
          checkIn(fileHandle);
        }
      }
    }

    synchronized void close() {
      if(open) {
        open = false;
        LOG.info("Closing RandomReader " + file);
        List<RandomAccessFile> fileHandles = Lists.newArrayList();
        while(readFileHandles.drainTo(fileHandles) > 0) {
          for(RandomAccessFile fileHandle : fileHandles) {
            synchronized (fileHandle) {
              try {
                fileHandle.close();
              } catch (IOException e) {
                LOG.warn("Unable to close fileHandle for " + file, e);
              }
            }
          }
          fileHandles.clear();
          try {
            Thread.sleep(5L);
          } catch (InterruptedException e) {
            // this is uninterruptable
          }
        }
      }
    }

    private RandomAccessFile open() throws IOException {
      return new RandomAccessFile(file, "r");
    }

    private void checkIn(RandomAccessFile fileHandle) {
      if(!readFileHandles.offer(fileHandle)) {
        close(fileHandle, file);
      }
    }

    /**
     * 获取一个指向目标文件{@link this#file}的读取实例
     * 同时存在实例数量上限由{@link this#readFileHandles}控制
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    private RandomAccessFile checkOut()
        throws IOException, InterruptedException {
      RandomAccessFile fileHandle = readFileHandles.poll();
      if(fileHandle != null) {
        return fileHandle;
      }
      int remaining = readFileHandles.remainingCapacity();
      if(remaining > 0) {
        LOG.info("Opening " + file + " for read, remaining capacity is "
            + remaining);
        return open();
      }
      return readFileHandles.take();
    }
    private static void close(RandomAccessFile fileHandle, File file) {
      if(fileHandle != null) {
        try {
          fileHandle.close();
        } catch (IOException e) {
          LOG.warn("Unable to close " + file, e);
        }
      }
    }
  }

  /**
   * 顺序读取Reader
   * 为保证顺序, 本类设计成一个实例只能只有一个fileHandle, 区别于RandomReader有多个
   * 使用上创建一个RandomReader可能多次使用(在FileChannel#doTake()方法里使用), 而
   * 本类应该创建一次使用一次
   */
  public static abstract class SequentialReader {

    private final RandomAccessFile fileHandle;
    private final FileChannel fileChannel;
    private final File file;
    private final KeyProvider encryptionKeyProvider;

    private int logFileID;
    private long lastCheckpointPosition;
    private long lastCheckpointWriteOrderID;
    private long backupCheckpointPosition;
    private long backupCheckpointWriteOrderID;

    /**
     * Construct a Sequential Log Reader object
     * @param file
     * @throws IOException if an I/O error occurs
     * @throws EOFException if the file is empty
     */
    SequentialReader(File file, @Nullable KeyProvider encryptionKeyProvider)
        throws IOException, EOFException {
      this.file = file;
      this.encryptionKeyProvider = encryptionKeyProvider;
      fileHandle = new RandomAccessFile(file, "r");
      fileChannel = fileHandle.getChannel();
    }
    abstract LogRecord doNext(int offset) throws IOException, CorruptEventException;

    abstract int getVersion();

    protected void setLastCheckpointPosition(long lastCheckpointPosition) {
      this.lastCheckpointPosition = lastCheckpointPosition;
    }
    protected void setLastCheckpointWriteOrderID(long lastCheckpointWriteOrderID) {
      this.lastCheckpointWriteOrderID = lastCheckpointWriteOrderID;
    }
    protected void setPreviousCheckpointPosition(
      long backupCheckpointPosition) {
      this.backupCheckpointPosition = backupCheckpointPosition;
    }
    protected void setPreviousCheckpointWriteOrderID(
      long backupCheckpointWriteOrderID) {
      this.backupCheckpointWriteOrderID = backupCheckpointWriteOrderID;
    }
    protected void setLogFileID(int logFileID) {
      this.logFileID = logFileID;
      Preconditions.checkArgument(logFileID >= 0, "LogFileID is not positive: "
          + Integer.toHexString(logFileID));

    }
    protected KeyProvider getKeyProvider() {
      return encryptionKeyProvider;
    }
    protected RandomAccessFile getFileHandle() {
      return fileHandle;
    }
    int getLogFileID() {
      return logFileID;
    }

    void skipToLastCheckpointPosition(long checkpointWriteOrderID)
      throws IOException {
      if (lastCheckpointPosition > 0L) {
        long position = 0;
        if (lastCheckpointWriteOrderID <= checkpointWriteOrderID) {
          position = lastCheckpointPosition;
        } else if (backupCheckpointWriteOrderID <= checkpointWriteOrderID
          && backupCheckpointPosition > 0) {
          position = backupCheckpointPosition;
        }
        fileChannel.position(position);
        LOG.info("fast-forward to checkpoint position: " + position);
      } else {
        LOG.info("Checkpoint for file(" + file.getAbsolutePath() + ") "
          + "is: " + lastCheckpointWriteOrderID + ", which is beyond the "
          + "requested checkpoint time: " + checkpointWriteOrderID
          + " and position " + lastCheckpointPosition);
      }
    }

    public LogRecord next() throws IOException, CorruptEventException {
      int offset = -1;
      try {
        long position = fileChannel.position();
        if (position > FileChannelConfiguration.DEFAULT_MAX_FILE_SIZE) {
          LOG.info("File position exceeds the threshold: "
                + FileChannelConfiguration.DEFAULT_MAX_FILE_SIZE
                + ", position: " + position);
        }
        offset = (int) position;
        Preconditions.checkState(offset >= 0);
        while (offset < fileHandle.length()) {
          byte operation = fileHandle.readByte();
          if (operation == OP_RECORD) {
            break;
          } else if (operation == OP_EOF) {
            LOG.info("Encountered EOF at " + offset + " in " + file);
            return null;
          } else if (operation == OP_NOOP) {
            LOG.info("No op event found in file: " + file.toString() +
              " at " + offset + ". Skipping event.");
            skipRecord(fileHandle, offset + 1);
            offset = (int) fileHandle.getFilePointer();
            continue;
          } else {
            LOG.error("Encountered non op-record at " + offset + " " +
              Integer.toHexString(operation) + " in " + file);
            return null;
          }
        }
        if(offset >= fileHandle.length()) {
          return null;
        }
        // 注意到, 这里的顺序Reader并不会像RandomReader抛弃非Put的Event, 因为顺序
        // Reader主要是为了给replay操作用的
        return doNext(offset);
      } catch(EOFException e) {
        return null;
      } catch (IOException e) {
        throw new IOException("Unable to read next Transaction from log file " +
            file.getCanonicalPath() + " at offset " + offset, e);
      }
    }

    public long getPosition() throws IOException {
      return fileChannel.position();
    }
    public void close() {
      if(fileHandle != null) {
        try {
          fileHandle.close();
        } catch (IOException e) {}
      }
    }
  }

  protected static void writeDelimitedBuffer(ByteBuffer output, ByteBuffer buffer)
      throws IOException {
    output.putInt(buffer.limit());
    output.put(buffer);
  }
  protected static byte[] readDelimitedBuffer(RandomAccessFile fileHandle)
      throws IOException {
    int length = fileHandle.readInt();
    Preconditions.checkState(length >= 0, Integer.toHexString(length));
    byte[] buffer = new byte[length];
    fileHandle.readFully(buffer);
    return buffer;
  }

  public static void main(String[] args) throws EOFException, IOException, CorruptEventException {
    File file = new File(args[0]);
    LogFile.SequentialReader reader = null;
    try {
      reader = LogFileFactory.getSequentialReader(file, null);
      LogRecord entry;
      FlumeEventPointer ptr;
      // for puts the fileId is the fileID of the file they exist in
      // for takes the fileId and offset are pointers to a put
      int fileId = reader.getLogFileID();
      int count = 0;
      int readCount = 0;
      int putCount = 0;
      int takeCount = 0;
      int rollbackCount = 0;
      int commitCount = 0;
      while ((entry = reader.next()) != null) {
        int offset = entry.getOffset();
        TransactionEventRecord record = entry.getEvent();
        short type = record.getRecordType();
        long trans = record.getTransactionID();
        long ts = record.getLogWriteOrderID();
        readCount++;
        ptr = null;
        if (type == TransactionEventRecord.Type.PUT.get()) {
          putCount++;
          ptr = new FlumeEventPointer(fileId, offset);
        } else if (type == TransactionEventRecord.Type.TAKE.get()) {
          takeCount++;
          Take take = (Take) record;
          ptr = new FlumeEventPointer(take.getFileID(), take.getOffset());
        } else if (type == TransactionEventRecord.Type.ROLLBACK.get()) {
          rollbackCount++;
        } else if (type == TransactionEventRecord.Type.COMMIT.get()) {
          commitCount++;
        } else {
          Preconditions.checkArgument(false, "Unknown record type: "
              + Integer.toHexString(type));
        }
        System.out.println(Joiner.on(", ").skipNulls().join(
            trans, ts, fileId, offset, TransactionEventRecord.getName(type), ptr));

      }
      System.out.println("Replayed " + count + " from " + file + " read: " + readCount
          + ", put: " + putCount + ", take: "
          + takeCount + ", rollback: " + rollbackCount + ", commit: "
          + commitCount);
    } catch (EOFException e) {
      System.out.println("Hit EOF on " + file);
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
  }
}
