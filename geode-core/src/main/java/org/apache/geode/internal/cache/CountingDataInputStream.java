/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.cache;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.jetbrains.annotations.NotNull;

/**
 * A data input stream that counts the bytes it plans on reading.
 *
 *
 * @since GemFire prPersistSprint2
 */

public class CountingDataInputStream implements DataInput, AutoCloseable {
  private final long fileLength;
  private long count;
  private final DataInputStream dis;

  public CountingDataInputStream(InputStream is, long fileLength) {
    this.fileLength = fileLength;
    dis = new DataInputStream(is);
  }

  public long getCount() {
    return count;
  }

  public long getFileLength() {
    return fileLength;
  }

  public void decrementCount() {
    count--;
  }

  public boolean atEndOfFile() {
    return fileLength == count;
  }

  @Override
  public void readFully(byte @NotNull [] b) throws IOException {
    dis.readFully(b);
    count += b.length;
  }

  @Override
  public void readFully(byte @NotNull [] b, int off, int len) throws IOException {
    dis.readFully(b, off, len);
    count += len;
  }

  @Override
  public int skipBytes(int n) throws IOException {
    int result = dis.skipBytes(n);
    count += result;
    return result;
  }

  @Override
  public boolean readBoolean() throws IOException {
    boolean result = dis.readBoolean();
    count += 1;
    return result;
  }

  @Override
  public byte readByte() throws IOException {
    byte result = dis.readByte();
    count += 1;
    return result;
  }

  @Override
  public int readUnsignedByte() throws IOException {
    int result = dis.readUnsignedByte();
    count += 1;
    return result;
  }

  @Override
  public short readShort() throws IOException {
    short result = dis.readShort();
    count += 2;
    return result;
  }

  @Override
  public int readUnsignedShort() throws IOException {
    int result = dis.readUnsignedShort();
    count += 2;
    return result;
  }

  @Override
  public char readChar() throws IOException {
    char result = dis.readChar();
    count += 2;
    return result;
  }

  @Override
  public int readInt() throws IOException {
    int result = dis.readInt();
    count += 4;
    return result;
  }

  @Override
  public long readLong() throws IOException {
    long result = dis.readLong();
    count += 8;
    return result;
  }

  @Override
  public float readFloat() throws IOException {
    float result = dis.readFloat();
    count += 4;
    return result;
  }

  @Override
  public double readDouble() throws IOException {
    double result = dis.readDouble();
    count += 8;
    return result;
  }

  @Override
  public String readLine() throws IOException {
    throw new IllegalStateException("method not supported");
  }

  @Override
  public String readUTF() throws IOException {
    return DataInputStream.readUTF(this);
  }

  public void close() throws IOException {
    dis.close();
  }
}
