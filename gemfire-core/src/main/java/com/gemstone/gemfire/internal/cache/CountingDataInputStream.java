/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import java.io.*;

/**
 * A data input stream that counts the bytes it plans on reading.
 * 
 * @author Darrel Schneider
 * 
 * @since prPersistSprint2
 */

public class CountingDataInputStream implements DataInput {
  private final long fileLength;
  private long count;
  private final DataInputStream dis;

  public CountingDataInputStream(InputStream is, long fileLength) {
    this.fileLength = fileLength;
    this.dis = new DataInputStream(is);
  }

  public long getCount() {
    return this.count;
  }
  public long getFileLength() {
    return this.fileLength;
  }
  public void decrementCount() {
    this.count--;
  }
  public boolean atEndOfFile() {
    return this.fileLength == this.count;
  }

  public void readFully(byte b[]) throws IOException {
    this.dis.readFully(b);
    this.count += b.length;
  }
  public void readFully(byte b[], int off, int len) throws IOException {
    this.dis.readFully(b, off, len);
    this.count += len;
  }
  public int skipBytes(int n) throws IOException {
    int result = this.dis.skipBytes(n);
    this.count += result;
    return result;
  }
  public boolean readBoolean() throws IOException {
    boolean result = this.dis.readBoolean();
    this.count += 1;
    return result;
  }
  public byte readByte() throws IOException {
    byte result = this.dis.readByte();
    this.count += 1;
    return result;
  }
  public int readUnsignedByte() throws IOException {
    int result = this.dis.readUnsignedByte();
    this.count += 1;
    return result;
  }
  public short readShort() throws IOException {
    short result = this.dis.readShort();
    this.count += 2;
    return result;
  }
  public int readUnsignedShort() throws IOException {
    int result = this.dis.readUnsignedShort();
    this.count += 2;
    return result;
  }
  public char readChar() throws IOException {
    char result = this.dis.readChar();
    this.count += 2;
    return result;
  }
  public int readInt() throws IOException {
    int result = this.dis.readInt();
    this.count += 4;
    return result;
  }
  public long readLong() throws IOException {
    long result = this.dis.readLong();
    this.count += 8;
    return result;
  }
  public float readFloat() throws IOException {
    float result = this.dis.readFloat();
    this.count += 4;
    return result;
  }
  public double readDouble() throws IOException {
    double result = this.dis.readDouble();
    this.count += 8;
    return result;
  }
  public String readLine() throws IOException {
    throw new IllegalStateException("method not supported");
  }
  public String readUTF() throws IOException {
    return DataInputStream.readUTF(this);
  }

  public void close() throws IOException {
    this.dis.close();
  }
}
