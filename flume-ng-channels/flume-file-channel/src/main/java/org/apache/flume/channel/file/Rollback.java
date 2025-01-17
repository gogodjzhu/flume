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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.flume.channel.file.proto.ProtosFactory;

import com.google.common.base.Preconditions;

/**
 * Represents a Rollback on disk
 */
class Rollback extends TransactionEventRecord {
  Rollback(Long transactionID, Long logWriteOrderID) {
    super(transactionID, logWriteOrderID);
  }
  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
  }

  /**
   * 将本实例编译为protobuf字节流写入到输出流, 报文格式见:
   * {@linkplain Log#rollback(long)}
   */
  @Override
  void writeProtos(OutputStream out) throws IOException {
    ProtosFactory.Rollback.Builder rollbackBuilder =
        ProtosFactory.Rollback.newBuilder();
    rollbackBuilder.build().writeDelimitedTo(out);
  }
  @Override
  void readProtos(InputStream in) throws IOException {
    @SuppressWarnings("unused")
    ProtosFactory.Rollback rollback = Preconditions.checkNotNull(ProtosFactory.
        Rollback.parseDelimitedFrom(in), "Rollback cannot be null");
  }
  @Override
  short getRecordType() {
    return Type.ROLLBACK.get();
  }
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("Rollback [getLogWriteOrderID()=");
    builder.append(getLogWriteOrderID());
    builder.append(", getTransactionID()=");
    builder.append(getTransactionID());
    builder.append("]");
    return builder.toString();
  }
}
