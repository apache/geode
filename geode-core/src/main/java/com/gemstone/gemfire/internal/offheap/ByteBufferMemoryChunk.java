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
package com.gemstone.gemfire.internal.offheap;

import java.nio.ByteBuffer;

/**
 * This implementation may throw IndexOutOfBoundsException or IllegalArgumentException if the wrong offset is given to the read and write methods.
 * BufferUnderflowException will be thrown if an attempt to read more data than exists is made.
 * BufferOverflowException will be thrown if an attempt to write past the end of the chunk is made.
 * 
 * @author darrel
 * @since 9.0
 */
public class ByteBufferMemoryChunk implements MemoryChunk {

  private final ByteBuffer data;
  
  public ByteBufferMemoryChunk(ByteBuffer bb) {
    this.data = bb;
  }
  
  @Override
  public int getSize() {
    return this.data.capacity();
  }

  @Override
  public byte readByte(int offset) {
    return this.data.get(offset);
  }

  @Override
  public void writeByte(int offset, byte value) {
    this.data.put(offset, value);
  }

  @Override
  public void readBytes(int offset, byte[] bytes) {
    readBytes(offset, bytes, 0, bytes.length);
  }

  @Override
  public void writeBytes(int offset, byte[] bytes) {
    writeBytes(offset, bytes, 0, bytes.length);
  }

  @Override
  public void readBytes(int offset, byte[] bytes, int bytesOffset, int size) {
    // NOT THREAD SAFE
    this.data.position(offset);
    this.data.get(bytes, bytesOffset, size);
  }

  @Override
  public void writeBytes(int offset, byte[] bytes, int bytesOffset, int size) {
    // NOT THREAD SAFE
    this.data.position(offset);
    this.data.put(bytes, bytesOffset, size);
  }

  @Override
  public void release() {
  }

  @Override
  public void copyBytes(int src, int dst, int size) {
    // NOT THREAD SAFE
    this.data.position(src);
    ByteBuffer srcBuff = this.data.slice();
    srcBuff.limit(size);

    this.data.position(dst);
    this.data.put(srcBuff);
  }
}
