/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.  
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.tcp;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;

/**
 * <p>
 * ByteBufferInputStream is an input stream for ByteBuffer objects. It's
 * incredible that the jdk doesn't have one of these already.
 * </p>
 * 
 * The methods in this class throw BufferUnderflowException, not EOFException,
 * if the end of the buffer is reached before we read the full amount. That
 * breaks the contract for InputStream and DataInput, but it works for our code.
 * 
 * @author Dan Smith
 * @author Bruce Schuchardt
 * @author Darrel Schneider
 * @since 3.0
 */

public class ByteBufferInputStream extends InputStream implements DataInput, java.io.Externalizable
{
  private ByteBuffer buffer;

  public ByteBufferInputStream(ByteBuffer buffer) {
    setBuffer(buffer);
  }
  
  public ByteBufferInputStream() {
  }
  
  protected ByteBufferInputStream(ByteBufferInputStream copy) {
    this.buffer = copy.buffer.duplicate();
  }

  public final void setBuffer(ByteBuffer buffer) {
    if(buffer == null) {
      throw new NullPointerException();
    }
    this.buffer = buffer;
  }

  /**
   * See the InputStream read method for javadocs.
   * Note that if an attempt
   * to read past the end of the wrapped ByteBuffer is done this method
   * throws BufferUnderflowException
   */
  @Override
  public final int read() {
    return (buffer.get() & 0xff);
  }
  

  /* this method is not thread safe
   * See the InputStream read method for javadocs.
   * Note that if an attempt
   * to read past the end of the wrapped ByteBuffer is done this method
   * throws BufferUnderflowException
   */
  @Override
  public final int read(byte b[], int off, int len) {
    buffer.get(b, off, len);
    return len;
  }

  @Override
  public int available() {
    return this.buffer.remaining();
  }
  
  public int position() {
    return this.buffer.position();
  }

  @Override
  public boolean markSupported() {
    return true;
  }

  @Override
  public void mark(int limit) {
    this.buffer.mark();
  }

  @Override
  public void reset() {
    this.buffer.reset();
  }

  @Override
  public long skip(long n) throws IOException {
    if (n <= Integer.MAX_VALUE) {
      return skipBytes((int) n);
    } else {
      return super.skip(n);
    }
  }

  public boolean readBoolean() {
    return this.buffer.get() != 0;
  }
  public boolean readBoolean(int pos) {
    return this.buffer.get(pos) != 0;
  }

  /* (non-Javadoc)
   * @see java.io.DataInput#readByte()
   */
  public byte readByte() {
    return this.buffer.get();
  }
  public byte readByte(int pos) {
    return this.buffer.get(pos);
  }

  /* (non-Javadoc)
   * @see java.io.DataInput#readChar()
   */
  public char readChar() {
    return this.buffer.getChar();
  }
  public char readChar(int pos) {
    return this.buffer.getChar(pos);
  }

  /* (non-Javadoc)
   * @see java.io.DataInput#readDouble()
   */
  public double readDouble() {
    return this.buffer.getDouble();
  }
  public double readDouble(int pos) {
    return this.buffer.getDouble(pos);
  }

  /* (non-Javadoc)
   * @see java.io.DataInput#readFloat()
   */
  public float readFloat() {
    return this.buffer.getFloat();
  }
  public float readFloat(int pos) {
    return this.buffer.getFloat(pos);
  }

  /* (non-Javadoc)
   * @see java.io.DataInput#readFully(byte[])
   */
  public void readFully(byte[] b) {
    this.buffer.get(b);
    
  }

  /* (non-Javadoc)
   * @see java.io.DataInput#readFully(byte[], int, int)
   */
  public void readFully(byte[] b, int off, int len) {
    this.buffer.get(b, off, len);
    
  }

  /* (non-Javadoc)
   * @see java.io.DataInput#readInt()
   */
  public int readInt() {
    return this.buffer.getInt();
  }
  public int readInt(int pos) {
    return this.buffer.getInt(pos);
  }

  /* (non-Javadoc)
   * @see java.io.DataInput#readLine()
   */
  public String readLine() {
    throw new UnsupportedOperationException();
  }

  /* (non-Javadoc)
   * @see java.io.DataInput#readLong()
   */
  public long readLong() {
    return this.buffer.getLong();
  }
  public long readLong(int pos) {
    return this.buffer.getLong(pos);
  }

  /* (non-Javadoc)
   * @see java.io.DataInput#readShort()
   */
  public short readShort() {
    return this.buffer.getShort();
  }
  public short readShort(int pos) {
    return this.buffer.getShort(pos);
  }

  /* (non-Javadoc)
   * @see java.io.DataInput#readUTF()
   */
  public String readUTF() throws IOException {
    return DataInputStream.readUTF(this);
  }

  /* (non-Javadoc)
   * @see java.io.DataInput#readUnsignedByte()
   */
  public int readUnsignedByte() {
    return this.buffer.get() & 0xff;
  }
  public int readUnsignedByte(int pos) {
    return this.buffer.get(pos) & 0xff;
  }

  /* (non-Javadoc)
   * @see java.io.DataInput#readUnsignedShort()
   */
  public int readUnsignedShort() {
    return this.buffer.getShort() & 0xffff;
  }
  public int readUnsignedShort(int pos) {
    return this.buffer.getShort(pos) & 0xffff;
  }

  /* (non-Javadoc)
   * @see java.io.DataInput#skipBytes(int)
   */
  public int skipBytes(int n) {
    int newPosition = this.buffer.position() + n;
    if(newPosition > this.buffer.limit()) {
      newPosition = this.buffer.limit();
      n = newPosition - this.buffer.position();
    }
    this.buffer.position(newPosition);
    return n;
  }

  public int size() {
    return this.buffer.limit();
  }

  public byte get(int idx) {
    return this.buffer.get(idx);
  }

  public short getShort(int idx) {
    return this.buffer.getShort(idx);
  }

  public int getInt(int idx) {
    return this.buffer.getInt(idx);
  }

  public void position(int absPos) {
//    if (absPos < 0) {
//      throw new IllegalArgumentException("position was less than zero " + absPos);
//    } else if (absPos > this.buffer.limit()) {
//      throw new IllegalArgumentException( "position " + absPos + " was greater than the limit " + this.buffer.limit());
//    }
    this.buffer.position(absPos);
  }

  public void sendTo(DataOutput out) throws IOException {
    byte[] bytes;
    int offset;
    int len = size();
    if (this.buffer.hasArray()) {
      bytes = this.buffer.array();
      offset = this.buffer.arrayOffset();
    } else {
      this.buffer.position(0);
      bytes = new byte[len];
      offset = 0;
      this.buffer.get(bytes);
    }
    out.write(bytes, offset, len);
  }
  
  public void sendTo(ByteBuffer out) {
    this.buffer.position(0);
    out.put(this.buffer);
 }

  public ByteBuffer slice() {
    return this.buffer.slice();
  }
  
  public ByteBuffer slice(int startOffset, int endOffset) {
    // We make a duplicate so we will have our own position, limit, and mark
    ByteBuffer bb = this.buffer.duplicate();
    bb.position(startOffset);
    bb.limit(endOffset);
    return bb.slice();
  }

  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeBoolean(this.buffer != null);
    if (this.buffer != null) {
      out.writeInt(this.buffer.capacity());
      out.writeInt(this.buffer.limit());
      out.writeInt(this.buffer.position());
      for (int i=0; i < this.buffer.capacity(); i++) {
        out.write(this.buffer.get(i));
      }
    }
  }

  public void readExternal(ObjectInput in) throws IOException,
      ClassNotFoundException {
    boolean hasBuffer = in.readBoolean();
    if (hasBuffer) {
      int capacity = in.readInt();
      int limit = in.readInt();
      int position = in.readInt();
      byte[] bytes = new byte[capacity];
      int bytesRead = in.read(bytes);
      if (bytesRead != capacity) {
        throw new IOException("Expected to read " + capacity + " bytes but only read " + bytesRead + " bytes.");
      }
      this.buffer = ByteBuffer.wrap(bytes, position, limit-position);
    } else {
      this.buffer = null;
    }
  }

  public ByteBuffer getBuffer() {
    return buffer;
  }
  
  
}
