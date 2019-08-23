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

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CheckpointRebuilder {

  private final List<File> logFiles;
  private final FlumeEventQueue queue;
  private final Set<ComparableFlumeEventPointer> committedPuts =
          Sets.newHashSet();
  private final Set<ComparableFlumeEventPointer> pendingTakes =
          Sets.newHashSet();
  private final SetMultimap<Long, ComparableFlumeEventPointer> uncommittedPuts =
          HashMultimap.create();
  private final SetMultimap<Long, ComparableFlumeEventPointer>
          uncommittedTakes = HashMultimap.create();

  private static Logger LOG =
          LoggerFactory.getLogger(CheckpointRebuilder.class);

  public CheckpointRebuilder(List<File> logFiles,
          FlumeEventQueue queue) throws IOException {
    this.logFiles = logFiles;
    this.queue = queue;
  }

  /**
   * fastReplay实际执行, 将log文件还原为queue队列
   * @return
   * @throws IOException
   * @throws Exception
   */
  public boolean rebuild() throws IOException, Exception {
    LOG.info("Attempting to fast replay the log files.");
    List<LogFile.SequentialReader> logReaders = Lists.newArrayList();
    for (File logFile : logFiles) {
      try {
        // 依赖文件实例化顺序Reader
        logReaders.add(LogFileFactory.getSequentialReader(logFile, null));
      } catch(EOFException e) {
        LOG.warn("Ignoring " + logFile + " due to EOF", e);
      }
    }
    // 全局的最新事务号和写入序号, 全局递增
    long transactionIDSeed = 0;
    long writeOrderIDSeed = 0;
    try {
      for (LogFile.SequentialReader log : logReaders) {
        LogRecord entry;
        int fileID = log.getLogFileID();
        // 顺序解析文件, 得到LogRecord, 再根据不同的Event类型在内存中重做整个过程. 最
        // 终得到已commit, 未take保留在channel中的put Event 的指针放回queue内存队列
        while ((entry = log.next()) != null) {
          int offset = entry.getOffset();
          TransactionEventRecord record = entry.getEvent();
          long trans = record.getTransactionID();
          long writeOrderID = record.getLogWriteOrderID();
            transactionIDSeed = Math.max(trans, transactionIDSeed);
            writeOrderIDSeed = Math.max(writeOrderID, writeOrderIDSeed);
          if (record.getRecordType() == TransactionEventRecord.Type.PUT.get()) {
            uncommittedPuts.put(record.getTransactionID(),
                    new ComparableFlumeEventPointer(
                    new FlumeEventPointer(fileID, offset),
                    record.getLogWriteOrderID()));
          } else if (record.getRecordType()
                  == TransactionEventRecord.Type.TAKE.get()) {
            Take take = (Take) record;
            uncommittedTakes.put(record.getTransactionID(),
                    new ComparableFlumeEventPointer(
                    new FlumeEventPointer(take.getFileID(), take.getOffset()),
                    record.getLogWriteOrderID()));
          } else if (record.getRecordType()
                  == TransactionEventRecord.Type.COMMIT.get()) {
            Commit commit = (Commit) record;
            if (commit.getType()
                    == TransactionEventRecord.Type.PUT.get()) {
              Set<ComparableFlumeEventPointer> puts =
                      uncommittedPuts.get(record.getTransactionID());
              if (puts != null) {
                for (ComparableFlumeEventPointer put : puts) {
                  // pending队列是已commit take的Event, 即还未落地commit文件就已经
                  // 被commit take消费
                  if (!pendingTakes.remove(put)) {
                    committedPuts.add(put);
                  }
                }
              }
            } else {
              Set<ComparableFlumeEventPointer> takes =
                      uncommittedTakes.get(record.getTransactionID());
              if (takes != null) {
                for (ComparableFlumeEventPointer take : takes) {
                  // commit Take Event, 将对应的Event从commitPuts中移除(此Event即被完整消费)
                  // 如果在commitPuts中没找到对应的event, 将其放入pendingTakes.
                  // TODO 可以take未commit的event? 难道put/take Event写入文件不一定严格有序
                  if (!committedPuts.remove(take)) {
                    pendingTakes.add(take);
                  }
                }
              }
            }
          } else if (record.getRecordType()
                  == TransactionEventRecord.Type.ROLLBACK.get()) {
            if (uncommittedPuts.containsKey(record.getTransactionID())) {
              uncommittedPuts.removeAll(record.getTransactionID());
            } else {
              uncommittedTakes.removeAll(record.getTransactionID());
            }
          }
        }
      }
    } catch (Exception e) {
      LOG.warn("Error while generating checkpoint "
              + "using fast generation logic", e);
      return false;
    } finally {
        TransactionIDOracle.setSeed(transactionIDSeed);
        WriteOrderOracle.setSeed(writeOrderIDSeed);
      for (LogFile.SequentialReader reader : logReaders) {
        reader.close();
      }
    }
    // 最后根据日志序号对全部剩余的commitPut做排序, 放回到queue, replay结束
    Set<ComparableFlumeEventPointer> sortedPuts =
            Sets.newTreeSet(committedPuts);
    int count = 0;
    for (ComparableFlumeEventPointer put : sortedPuts) {
      /** 注意queue保存的是 {@link FlumeEventPointer}, 它的属性只包含日志文件和event
       * 对应的offset 而不是整个event实体
       * */
      queue.addTail(put.pointer);
      count++;
    }
    LOG.info("Replayed {} events using fast replay logic.", count);
    return true;
  }

  private void writeCheckpoint() throws IOException {
    long checkpointLogOrderID = 0;
    List<LogFile.MetaDataWriter> metaDataWriters = Lists.newArrayList();
    for (File logFile : logFiles) {
        String name = logFile.getName();
        metaDataWriters.add(LogFileFactory.getMetaDataWriter(logFile,
            Integer.parseInt(name.substring(name.lastIndexOf('-') + 1))));
    }
    try {
      if (queue.checkpoint(true)) {
        checkpointLogOrderID = queue.getLogWriteOrderID();
        for (LogFile.MetaDataWriter metaDataWriter : metaDataWriters) {
          metaDataWriter.markCheckpoint(checkpointLogOrderID);
        }
      }
    } catch (Exception e) {
      LOG.warn("Error while generating checkpoint "
              + "using fast generation logic", e);
    } finally {
      for (LogFile.MetaDataWriter metaDataWriter : metaDataWriters) {
        metaDataWriter.close();
      }
    }
  }

  private final class ComparableFlumeEventPointer
          implements Comparable<ComparableFlumeEventPointer> {

    private final FlumeEventPointer pointer;
    private final long orderID;

    public ComparableFlumeEventPointer(FlumeEventPointer pointer, long orderID){
      Preconditions.checkNotNull(pointer, "FlumeEventPointer cannot be"
              + "null while creating a ComparableFlumeEventPointer");
      this.pointer = pointer;
      this.orderID = orderID;
    }

    @Override
    public int compareTo(ComparableFlumeEventPointer o) {
      if (orderID < o.orderID) {
        return -1;
      } else { //Unfortunately same log order id does not mean same event
        //for older logs.
        return 1;
      }
    }

    @Override
    public int hashCode(){
      return pointer.hashCode();
    }

    @Override
    public boolean equals(Object o){
      if(this == o){
        return true;
      }
      if(o == null){
        return false;
      }
      if(o.getClass() != this.getClass()){
        return false;
      }
      return pointer.equals(((ComparableFlumeEventPointer)o).pointer);
    }
  }

  public static void main(String[] args) throws Exception {
    Options options = new Options();
    Option opt = new Option("c", true, "checkpoint directory");
    opt.setRequired(true);
    options.addOption(opt);
    opt = new Option("l", true, "comma-separated list of log directories");
    opt.setRequired(true);
    options.addOption(opt);
    options.addOption(opt);
    opt = new Option("t", true, "capacity of the channel");
    opt.setRequired(true);
    options.addOption(opt);
    CommandLineParser parser = new GnuParser();
    CommandLine cli = parser.parse(options, args);
    File checkpointDir = new File(cli.getOptionValue("c"));
    String[] logDirs = cli.getOptionValue("l").split(",");
    List<File> logFiles = Lists.newArrayList();
    for (String logDir : logDirs) {
      logFiles.addAll(LogUtils.getLogs(new File(logDir)));
    }
    int capacity = Integer.parseInt(cli.getOptionValue("t"));
    File checkpointFile = new File(checkpointDir, "checkpoint");
    if(checkpointFile.exists()) {
      LOG.error("Cannot execute fast replay",
          new IllegalStateException("Checkpoint exists" + checkpointFile));
    } else {
      EventQueueBackingStore backingStore =
          EventQueueBackingStoreFactory.get(checkpointFile,
              capacity, "channel");
      FlumeEventQueue queue = new FlumeEventQueue(backingStore,
              new File(checkpointDir, "inflighttakes"),
              new File(checkpointDir, "inflightputs"));
      CheckpointRebuilder rebuilder = new CheckpointRebuilder(logFiles, queue);
      if(rebuilder.rebuild()) {
        rebuilder.writeCheckpoint();
      } else {
        LOG.error("Could not rebuild the checkpoint due to errors.");
      }
    }
  }
}
