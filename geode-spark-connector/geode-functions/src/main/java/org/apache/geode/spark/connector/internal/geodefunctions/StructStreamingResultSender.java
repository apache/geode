/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.spark.connector.internal.geodefunctions;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.cache.query.internal.types.ObjectTypeImpl;
import org.apache.geode.cache.query.internal.types.StructTypeImpl;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.cache.query.types.StructType;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.cache.CachedDeserializable;
import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;

/**
 * StructStreamingResultSender and StructStreamingResultCollector  are paired
 * to transfer result of list of `org.apache.geode.cache.query.Struct`
 * from GemFire server to Spark Connector (the client of GemFire server)
 * in streaming, i.e., while sender sending the result, the collector can
 * start processing the arrived result without waiting for full result to
 * become available.
 */
public class StructStreamingResultSender {

  public static final byte TYPE_CHUNK   = 0x30;
  public static final byte DATA_CHUNK   = 0x31;
  public static final byte ERROR_CHUNK  = 0x32;
  public static final byte SER_DATA     = 0x41;
  public static final byte UNSER_DATA   = 0x42;
  public static final byte BYTEARR_DATA = 0x43;

  private static ObjectTypeImpl ObjField = new ObjectTypeImpl(java.lang.Object.class);
  public static StructTypeImpl KeyValueType = new StructTypeImpl(new String[]{"key", "value"}, new ObjectType[]{ObjField, ObjField});

  private static final Logger logger = LogService.getLogger();
  private static final int CHUNK_SIZE = 4096;
  
  // Note: The type of ResultSender returned from GemFire FunctionContext is
  //  always ResultSender<Object>, so can't use ResultSender<byte[]> here
  private final ResultSender<Object> sender;
  private final StructType structType;
  private final Iterator<Object[]> rows;
  private String desc;
  private boolean closed = false;

  /**
   * the Constructor 
   * @param sender the base ResultSender that send data in byte array
   * @param type the StructType of result record
   * @param rows the iterator of the collection of results
   * @param desc description of this result (used for logging)           
   */
  public StructStreamingResultSender(
    ResultSender<Object> sender, StructType type, Iterator<Object[]> rows, String desc) {
    if (sender == null || rows == null)
      throw new NullPointerException("sender=" + sender + ", rows=" + rows);
    this.sender = sender;
    this.structType = type;
    this.rows = rows;
    this.desc = desc;
  }

  /** the Constructor with default `desc` */
  public StructStreamingResultSender(
          ResultSender<Object> sender, StructType type, Iterator<Object[]> rows) {
    this(sender, type, rows, "StructStreamingResultSender");
  }
  
  /**
   * Send the result in chunks. There are 3 types of chunk: TYPE, DATA, and ERROR.
   * TYPE chunk for sending struct type info, DATA chunk for sending data, and
   * ERROR chunk for sending exception. There are at most 1 TYPE chunk (omitted
   * for `KeyValueType`) and 1 ERROR chunk (if there's error), but usually
   * there are multiple DATA chunks. Each DATA chunk contains multiple rows
   * of data. The chunk size is determined by the const `CHUNK_SIZE`. If an
   * exception is thrown, it is serialized and sent as the last chunk  of the 
   * result (in the form of ERROR chunk).
   */
  public void send() {
    if (closed) throw new RuntimeException("sender is closed.");

    HeapDataOutputStream buf = new HeapDataOutputStream(CHUNK_SIZE + 2048, null);
    String dataType = null;
    int typeSize = 0;
    int rowCount = 0;
    int dataSize = 0;            
    try {
      if (rows.hasNext()) {
        // Note: only send type info if there's data with it
        typeSize = sendType(buf);
        buf.writeByte(DATA_CHUNK);
        int rowSize = structType == null ? 2 : structType.getFieldNames().length;
        while (rows.hasNext()) {
          rowCount ++;
          Object[] row = rows.next();          
          if (rowCount < 2) dataType = entryDataType(row);
          if (rowSize != row.length) 
            throw new IOException(rowToString("Expect "  + rowSize + " columns, but got ", row));
          serializeRowToBuffer(row, buf);
          if (buf.size() > CHUNK_SIZE) {
            dataSize += sendBufferredData(buf, false);
            buf.writeByte(DATA_CHUNK);
          }
        }
      }
      // send last piece of data or empty byte array
      dataSize += sendBufferredData(buf, true);
      logger.info(desc + ": " + rowCount + " rows, type=" + dataType + ", type.size=" +
                  typeSize + ", data.size=" + dataSize + ", row.avg.size=" +
                  (rowCount == 0 ? "NaN" : String.format("%.1f", ((float) dataSize)/rowCount)));
    } catch (IOException | RuntimeException e) {
      sendException(buf, e);
    } finally {
      closed = true;
    }
  }

  private String rowToString(String rowDesc, Object[] row) {
    StringBuilder buf = new StringBuilder();
    buf.append(rowDesc).append("(");
    for (int i = 0; i < row.length; i++) buf.append(i ==0 ? "" : " ,").append(row[i]);
    return buf.append(")") .toString();    
  }

  private String entryDataType(Object[] row) {
    StringBuilder buf = new StringBuilder();
    buf.append("(");
    for (int i = 0; i < row.length; i++) {
      if (i != 0) buf.append(", ");
      buf.append(row[i].getClass().getCanonicalName());
    }
    return buf.append(")").toString();
  }
  
  private void serializeRowToBuffer(Object[] row, HeapDataOutputStream buf) throws IOException {
    for (Object data : row) {
      if (data instanceof CachedDeserializable) {
        buf.writeByte(SER_DATA);
        DataSerializer.writeByteArray(((CachedDeserializable) data).getSerializedValue(), buf);
      } else if (data instanceof byte[]) {
        buf.writeByte(BYTEARR_DATA);
        DataSerializer.writeByteArray((byte[]) data, buf);
      } else {
        buf.writeByte(UNSER_DATA);
        DataSerializer.writeObject(data, buf);
      }
    }
  }
  
  /** return the size of type data */
  private int sendType(HeapDataOutputStream buf) throws IOException {
    // logger.info(desc + " struct type: " + structType);
    if (structType != null) {
      buf.writeByte(TYPE_CHUNK);
      DataSerializer.writeObject(structType, buf);
      return sendBufferredData(buf, false);      
    } else {
      return 0;  // default KeyValue type, no type info send
    }
  }
  
  private int sendBufferredData(HeapDataOutputStream buf, boolean isLast) throws IOException {
    if (isLast) sender.lastResult(buf.toByteArray());
    else sender.sendResult(buf.toByteArray());
    // logData(buf.toByteArray(), desc);
    int s = buf.size();
    buf.reset();
    return s;    
  }

  /** Send the exception as the last chunk of the result. */
  private void sendException(HeapDataOutputStream buf, Exception e) {
    // Note: if exception happens during the serialization, the `buf` may contain
    // partial serialized data, which may cause de-serialization hang or error.
    // Therefore, always empty the buffer before sending the exception
    if (buf.size() > 0) buf.reset();
    
    try {
      buf.writeByte(ERROR_CHUNK);
      DataSerializer.writeObject(e, buf);
    } catch (IOException ioe) {
      logger.error("StructStreamingResultSender failed to send the result:", e);
      logger.error("StructStreamingResultSender failed to serialize the exception:", ioe);
      buf.reset();
    }
    // Note: send empty chunk as the last result if serialization of exception 
    // failed, and the error is logged on the GemFire server side.
    sender.lastResult(buf.toByteArray());
    // logData(buf.toByteArray(), desc);
  }

//  private void logData(byte[] data, String desc) {
//    StringBuilder buf = new StringBuilder();
//    buf.append(desc);
//    for (byte b : data) {
//      buf.append(" ").append(b);
//    }
//    logger.info(buf.toString());
//  }

}
