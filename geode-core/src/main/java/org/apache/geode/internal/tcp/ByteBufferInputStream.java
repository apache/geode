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
package org.apache.geode.internal.tcp;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.geode.internal.ByteBufferWriter;
import org.apache.geode.internal.offheap.AddressableMemoryManager;
import org.apache.geode.internal.offheap.StoredObject;

/**
 * <p>
 * ByteBufferInputStream is an input stream for ByteBuffer objects. It's incredible that the jdk
 * doesn't have one of these already.
 * </p>
 *
 * The methods in this class throw BufferUnderflowException, not EOFException, if the end of the
 * buffer is reached before we read the full amount. That breaks the contract for InputStream and
 * DataInput, but it works for our code.
 *
 * @since GemFire 3.0
 */

public class ByteBufferInputStream extends InputStream
    implements DataInput, java.io.Externalizable {
  /**
   * This interface is used to wrap either a ByteBuffer or an offheap Chunk as the source of bytes
   * for a ByteBufferInputStream.
   *
   *
   */
  public interface ByteSource {
    int position();

    int limit();

    int capacity();

    int remaining();

    void position(int newPosition);

    void limit(int endOffset);

    void get(byte[] b);

    void get(byte[] b, int off, int len);

    byte get();

    byte get(int pos);

    short getShort();

    short getShort(int pos);

    char getChar();

    char getChar(int pos);

    int getInt();

    int getInt(int pos);

    long getLong();

    long getLong(int pos);

    float getFloat();

    float getFloat(int pos);

    double getDouble();

    double getDouble(int pos);

    boolean hasArray();

    byte[] array();

    int arrayOffset();

    ByteSource duplicate();

    ByteSource slice(int length);

    ByteSource slice(int pos, int limit);

    /**
     * Returns the ByteBuffer that this ByteSource wraps; null if no ByteBuffer
     */
    ByteBuffer getBackingByteBuffer();

    void sendTo(ByteBuffer out);

    void sendTo(DataOutput out) throws IOException;
  }

  public static class ByteSourceFactory {
    public static ByteSource wrap(byte[] bytes) {
      return new ByteBufferByteSource(ByteBuffer.wrap(bytes));
    }

    public static ByteSource create(ByteBuffer bb) {
      return new ByteBufferByteSource(bb);
    }

    public static ByteSource create(StoredObject so) {
      // Since I found a way to create a DirectByteBuffer (using reflection) from a StoredObject
      // we might not even need the ByteSource abstraction any more.
      // But it is possible that createByteBuffer will not work on a different jdk so keep it for
      // now.
      ByteBuffer bb = so.createDirectByteBuffer();
      if (bb != null) {
        return create(bb);
      } else {
        return new OffHeapByteSource(so);
      }
    }
  }

  public static class ByteBufferByteSource implements ByteSource {
    private final ByteBuffer bb;

    public ByteBufferByteSource(ByteBuffer bb) {
      this.bb = bb;
    }

    /**
     * Returns the current hash code of this byte source.
     *
     * <p>
     * The hash code of a byte source depends only upon its remaining elements; that is, upon the
     * elements from <tt>position()</tt> up to, and including, the element at
     * <tt>limit()</tt>&nbsp;-&nbsp;<tt>1</tt>.
     *
     * <p>
     * Because byte source hash codes are content-dependent, it is inadvisable to use byte sources
     * as keys in hash maps or similar data structures unless it is known that their contents will
     * not change.
     * </p>
     *
     * @return The current hash code of this byte source
     */
    @Override
    public int hashCode() {
      int h = 1;
      int p = position();
      for (int i = limit() - 1; i >= p; i--) {
        h = 31 * h + (int) get(i);
      }
      return h;
    }

    @Override
    public boolean equals(Object ob) {
      if (this == ob) {
        return true;
      }
      if (!(ob instanceof ByteSource)) {
        return false;
      }
      ByteSource that = (ByteSource) ob;
      if (this.remaining() != that.remaining()) {
        return false;
      }
      int p = this.position();
      for (int i = this.limit() - 1, j = that.limit() - 1; i >= p; i--, j--) {
        if (this.get(i) != that.get(j)) {
          return false;
        }
      }
      return true;
    }

    @Override
    public ByteSource duplicate() {
      return ByteSourceFactory.create(this.bb.duplicate());
    }

    @Override
    public byte get() {
      return this.bb.get();
    }

    @Override
    public void get(byte[] b, int off, int len) {
      this.bb.get(b, off, len);
    }

    @Override
    public int remaining() {
      return this.bb.remaining();
    }

    @Override
    public int position() {
      return this.bb.position();
    }

    @Override
    public byte get(int pos) {
      return this.bb.get(pos);
    }

    @Override
    public char getChar() {
      return this.bb.getChar();
    }

    @Override
    public char getChar(int pos) {
      return this.bb.getChar(pos);
    }

    @Override
    public double getDouble() {
      return this.bb.getDouble();
    }

    @Override
    public double getDouble(int pos) {
      return this.bb.getDouble(pos);
    }

    @Override
    public float getFloat() {
      return this.bb.getFloat();
    }

    @Override
    public float getFloat(int pos) {
      return this.bb.getFloat(pos);
    }

    @Override
    public void get(byte[] b) {
      this.bb.get(b);
    }

    @Override
    public int getInt() {
      return this.bb.getInt();
    }

    @Override
    public int getInt(int pos) {
      return this.bb.getInt(pos);
    }

    @Override
    public long getLong() {
      return this.bb.getLong();
    }

    @Override
    public long getLong(int pos) {
      return this.bb.getLong(pos);
    }

    @Override
    public short getShort() {
      return this.bb.getShort();
    }

    @Override
    public short getShort(int pos) {
      return this.bb.getShort(pos);
    }

    @Override
    public int limit() {
      return this.bb.limit();
    }

    @Override
    public void position(int newPosition) {
      this.bb.position(newPosition);
    }

    @Override
    public boolean hasArray() {
      return this.bb.hasArray();
    }

    @Override
    public byte[] array() {
      return this.bb.array();
    }

    @Override
    public int arrayOffset() {
      return this.bb.arrayOffset();
    }

    @Override
    public void limit(int endOffset) {
      this.bb.limit(endOffset);
    }

    @Override
    public ByteSource slice(int length) {
      if (length < 0) {
        throw new IllegalArgumentException();
      }
      ByteBuffer dup = this.bb.duplicate();
      dup.limit(dup.position() + length);
      return ByteSourceFactory.create(dup.slice());
    }

    @Override
    public ByteSource slice(int pos, int limit) {
      ByteBuffer dup = this.bb.duplicate();
      dup.limit(limit);
      dup.position(pos);
      return ByteSourceFactory.create(dup.slice());
    }

    @Override
    public int capacity() {
      return this.bb.capacity();
    }

    @Override
    public void sendTo(ByteBuffer out) {
      out.put(this.bb);
    }

    @Override
    public void sendTo(DataOutput out) throws IOException {
      int len = remaining();
      if (len == 0) {
        return;
      }
      if (out instanceof ByteBufferWriter) {
        ((ByteBufferWriter) out).write(this.bb);
        return;
      }
      if (this.bb.hasArray()) {
        byte[] bytes = this.bb.array();
        int offset = this.bb.arrayOffset() + this.bb.position();
        out.write(bytes, offset, len);
        this.bb.position(this.bb.limit());
      } else {
        while (len > 0) {
          out.writeByte(get());
          len--;
        }
      }
    }

    @Override
    public ByteBuffer getBackingByteBuffer() {
      return this.bb;
    }
  }

  public static class OffHeapByteSource implements ByteSource {
    private int position;
    private int limit;
    private final StoredObject chunk;

    public OffHeapByteSource(StoredObject so) {
      this.chunk = so;
      this.position = 0;
      this.limit = capacity();
    }

    private OffHeapByteSource(OffHeapByteSource other) {
      this.chunk = other.chunk;
      this.position = other.position;
      this.limit = other.limit;
    }

    /**
     * Returns the current hash code of this byte source.
     *
     * <p>
     * The hash code of a byte source depends only upon its remaining elements; that is, upon the
     * elements from <tt>position()</tt> up to, and including, the element at
     * <tt>limit()</tt>&nbsp;-&nbsp;<tt>1</tt>.
     *
     * <p>
     * Because byte source hash codes are content-dependent, it is inadvisable to use byte sources
     * as keys in hash maps or similar data structures unless it is known that their contents will
     * not change.
     * </p>
     *
     * @return The current hash code of this byte source
     */
    @Override
    public int hashCode() {
      int h = 1;
      int p = position();
      for (int i = limit() - 1; i >= p; i--) {
        h = 31 * h + (int) get(i);
      }
      return h;
    }

    @Override
    public boolean equals(Object ob) {
      if (this == ob) {
        return true;
      }
      if (!(ob instanceof ByteSource)) {
        return false;
      }
      ByteSource that = (ByteSource) ob;
      if (this.remaining() != that.remaining()) {
        return false;
      }
      int p = this.position();
      for (int i = this.limit() - 1, j = that.limit() - 1; i >= p; i--, j--) {
        if (this.get(i) != that.get(j)) {
          return false;
        }
      }
      return true;
    }

    @Override
    public int remaining() {
      return this.limit - this.position;
    }

    @Override
    public int position() {
      return this.position;
    }

    @Override
    public int limit() {
      return this.limit;
    }

    @Override
    public void position(int newPosition) {
      if ((newPosition > this.limit) || (newPosition < 0)) {
        throw new IllegalArgumentException();
      }
      this.position = newPosition;
    }

    @Override
    public void limit(int newLimit) {
      if ((newLimit > capacity()) || (newLimit < 0)) {
        throw new IllegalArgumentException();
      }
      this.limit = newLimit;
      if (this.position > this.limit) {
        this.position = this.limit;
      }
    }

    @Override
    public int capacity() {
      return this.chunk.getDataSize();
    }

    private int nextGetIndex() {
      int p = this.position;
      if (p >= this.limit) {
        throw new BufferUnderflowException();
      }
      this.position += 1;
      return p;
    }

    private int nextGetIndex(int nb) {
      int p = this.position;
      if (this.limit - p < nb) {
        throw new BufferUnderflowException();
      }
      this.position += nb;
      return p;
    }

    /**
     * Checks the given index against the limit, throwing an {@link IndexOutOfBoundsException} if it
     * is not smaller than the limit or is smaller than zero.
     */
    private void checkIndex(int i) {
      if ((i < 0) || (i >= this.limit)) {
        throw new IndexOutOfBoundsException();
      }
    }

    private void checkIndex(int i, int nb) {
      if ((i < 0) || (nb > this.limit - i)) {
        throw new IndexOutOfBoundsException();
      }
    }

    private static void checkBounds(int off, int len, int size) {
      if ((off | len | (off + len) | (size - (off + len))) < 0) {
        throw new IndexOutOfBoundsException();
      }
    }

    @Override
    public void get(byte[] b) {
      basicGet(b, 0, b.length);
    }

    @Override
    public void get(byte[] dst, int offset, int length) {
      checkBounds(offset, length, dst.length);
      basicGet(dst, offset, length);
    }

    private void basicGet(byte[] dst, int offset, int length) {
      if (length > remaining()) {
        throw new BufferUnderflowException();
      }
      int p = this.position;
      this.position += length;
      this.chunk.readDataBytes(p, dst, offset, length);
    }

    @Override
    public byte get() {
      return this.chunk.readDataByte(nextGetIndex());
    }

    @Override
    public byte get(int pos) {
      checkIndex(pos);
      return this.chunk.readDataByte(pos);
    }

    /**
     * Return true if the hardware supported unaligned reads from memory.
     */
    private static boolean determineUnaligned() {
      try {
        Class c = Class.forName("java.nio.Bits");
        Method m = c.getDeclaredMethod("unaligned");
        m.setAccessible(true);
        return (boolean) m.invoke(null);
      } catch (ClassNotFoundException | NoSuchMethodException | SecurityException
          | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
        return false;
        // throw new IllegalStateException("Could not invoke java.nio.Bits.unaligned()", e);
      }
    }

    private static final boolean unaligned = determineUnaligned();

    @Override
    public short getShort() {
      return basicGetShort(this.nextGetIndex(2));
    }

    @Override
    public short getShort(int pos) {
      this.checkIndex(pos, 2);
      return basicGetShort(pos);
    }

    private short basicGetShort(int pos) {
      long addr = this.chunk.getAddressForReadingData(pos, 2);
      if (unaligned) {
        short result = AddressableMemoryManager.readShort(addr);
        if (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN) {
          result = Short.reverseBytes(result);
        }
        return result;
      } else {
        int ch1 = AddressableMemoryManager.readByte(addr++);
        int ch2 = AddressableMemoryManager.readByte(addr);
        return (short) ((ch1 << 8) + (ch2 << 0));
      }
    }

    @Override
    public char getChar() {
      return basicGetChar(this.nextGetIndex(2));
    }

    @Override
    public char getChar(int pos) {
      this.checkIndex(pos, 2);
      return basicGetChar(pos);
    }

    private char basicGetChar(int pos) {
      long addr = this.chunk.getAddressForReadingData(pos, 2);
      if (unaligned) {
        char result = AddressableMemoryManager.readChar(addr);
        if (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN) {
          result = Character.reverseBytes(result);
        }
        return result;
      } else {
        int ch1 = AddressableMemoryManager.readByte(addr++);
        int ch2 = AddressableMemoryManager.readByte(addr);
        return (char) ((ch1 << 8) + (ch2 << 0));
      }
    }

    @Override
    public int getInt() {
      return basicGetInt(this.nextGetIndex(4));
    }

    @Override
    public int getInt(int pos) {
      this.checkIndex(pos, 4);
      return basicGetInt(pos);
    }

    private int basicGetInt(final int pos) {
      long addr = this.chunk.getAddressForReadingData(pos, 4);
      if (unaligned) {
        int result = AddressableMemoryManager.readInt(addr);
        if (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN) {
          result = Integer.reverseBytes(result);
        }
        return result;
      } else {
        byte b0 = AddressableMemoryManager.readByte(addr++);
        byte b1 = AddressableMemoryManager.readByte(addr++);
        byte b2 = AddressableMemoryManager.readByte(addr++);
        byte b3 = AddressableMemoryManager.readByte(addr);
        return (b0 << 24) + ((b1 & 255) << 16) + ((b2 & 255) << 8) + ((b3 & 255) << 0);
      }
    }

    @Override
    public long getLong() {
      return basicGetLong(this.nextGetIndex(8));
    }

    @Override
    public long getLong(int pos) {
      this.checkIndex(pos, 8);
      return basicGetLong(pos);
    }

    private long basicGetLong(final int pos) {
      long addr = this.chunk.getAddressForReadingData(pos, 8);
      if (unaligned) {
        long result = AddressableMemoryManager.readLong(addr);
        if (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN) {
          result = Long.reverseBytes(result);
        }
        return result;
      } else {
        byte b0 = AddressableMemoryManager.readByte(addr++);
        byte b1 = AddressableMemoryManager.readByte(addr++);
        byte b2 = AddressableMemoryManager.readByte(addr++);
        byte b3 = AddressableMemoryManager.readByte(addr++);
        byte b4 = AddressableMemoryManager.readByte(addr++);
        byte b5 = AddressableMemoryManager.readByte(addr++);
        byte b6 = AddressableMemoryManager.readByte(addr++);
        byte b7 = AddressableMemoryManager.readByte(addr);
        return (((long) b0 << 56) + ((long) (b1 & 255) << 48) + ((long) (b2 & 255) << 40)
            + ((long) (b3 & 255) << 32) + ((long) (b4 & 255) << 24) + ((b5 & 255) << 16)
            + ((b6 & 255) << 8) + ((b7 & 255) << 0));
      }
    }

    @Override
    public float getFloat() {
      return basicGetFloat(this.nextGetIndex(4));
    }

    @Override
    public float getFloat(int pos) {
      this.checkIndex(pos, 4);
      return basicGetFloat(pos);
    }

    private float basicGetFloat(int pos) {
      return Float.intBitsToFloat(basicGetInt(pos));
    }

    @Override
    public double getDouble() {
      return basicGetDouble(this.nextGetIndex(8));
    }

    @Override
    public double getDouble(int pos) {
      this.checkIndex(pos, 8);
      return basicGetDouble(pos);
    }

    private double basicGetDouble(int pos) {
      return Double.longBitsToDouble(basicGetLong(pos));
    }

    @Override
    public boolean hasArray() {
      return false;
    }

    @Override
    public byte[] array() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int arrayOffset() {
      throw new UnsupportedOperationException();
    }

    @Override
    public ByteSource duplicate() {
      return new OffHeapByteSource(this);
    }

    @Override
    public ByteSource slice(int length) {
      if (length < 0) {
        throw new IllegalArgumentException();
      }
      return slice(this.position, this.position + length);
    }

    @Override
    public ByteSource slice(int pos, int limit) {
      if ((limit > capacity()) || (limit < 0)) {
        throw new IllegalArgumentException();
      }
      if ((pos > limit) || (pos < 0)) {
        throw new IllegalArgumentException();
      }
      return new OffHeapByteSource(this.chunk.slice(pos, limit));
    }

    @Override
    public void sendTo(ByteBuffer out) {
      int len = remaining();
      while (len > 0) {
        out.put(get());
        len--;
      }
      // We will not even create an instance of this class if createByteBuffer works on this
      // platform.
      // if (len > 0) {
      // ByteBuffer bb = this.chunk.createByteBuffer();
      // bb.position(position());
      // bb.limit(limit());
      // out.put(bb);
      // position(limit());
      // }
    }

    @Override
    public void sendTo(DataOutput out) throws IOException {
      int len = remaining();
      while (len > 0) {
        out.writeByte(get());
        len--;
      }
    }

    @Override
    public ByteBuffer getBackingByteBuffer() {
      return null;
    }
  }

  private ByteSource buffer;

  public ByteBufferInputStream(ByteBuffer buffer) {
    setBuffer(buffer);
  }

  public ByteBufferInputStream() {}

  protected ByteBufferInputStream(ByteBufferInputStream copy) {
    this.buffer = copy.buffer.duplicate();
  }

  public ByteBufferInputStream(StoredObject blob) {
    this.buffer = ByteSourceFactory.create(blob);
  }

  public void setBuffer(ByteSource buffer) {
    if (buffer == null) {
      throw new NullPointerException();
    }
    this.buffer = buffer;
  }

  public void setBuffer(ByteBuffer bb) {
    if (bb == null) {
      throw new NullPointerException();
    }
    setBuffer(ByteSourceFactory.create(bb));
  }

  /**
   * See the InputStream read method for javadocs. Note that if an attempt to read past the end of
   * the wrapped ByteBuffer is done this method throws BufferUnderflowException
   */
  @Override
  public int read() {
    return (buffer.get() & 0xff);
  }


  /*
   * this method is not thread safe See the InputStream read method for javadocs. Note that if an
   * attempt to read past the end of the wrapped ByteBuffer is done this method throws
   * BufferUnderflowException
   */
  @Override
  public int read(byte b[], int off, int len) {
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

  // GemFire does not use mark or reset so I changed this class
  // to just inherit from InputStream which does not support mark/reset.
  // That way we do not need to add support for them to the new ByteSource class.

  // @Override
  // public boolean markSupported() {
  // return true;
  // }
  //
  // @Override
  // public void mark(int limit) {
  // this.buffer.mark();
  // }
  //
  // @Override
  // public void reset() {
  // this.buffer.reset();
  // }

  @Override
  public long skip(long n) throws IOException {
    if (n <= Integer.MAX_VALUE) {
      return skipBytes((int) n);
    } else {
      return super.skip(n);
    }
  }

  @Override
  public boolean readBoolean() {
    return this.buffer.get() != 0;
  }

  public boolean readBoolean(int pos) {
    return this.buffer.get(pos) != 0;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.io.DataInput#readByte()
   */
  @Override
  public byte readByte() {
    return this.buffer.get();
  }

  public byte readByte(int pos) {
    return this.buffer.get(pos);
  }

  /*
   * (non-Javadoc)
   *
   * @see java.io.DataInput#readChar()
   */
  @Override
  public char readChar() {
    return this.buffer.getChar();
  }

  public char readChar(int pos) {
    return this.buffer.getChar(pos);
  }

  /*
   * (non-Javadoc)
   *
   * @see java.io.DataInput#readDouble()
   */
  @Override
  public double readDouble() {
    return this.buffer.getDouble();
  }

  public double readDouble(int pos) {
    return this.buffer.getDouble(pos);
  }

  /*
   * (non-Javadoc)
   *
   * @see java.io.DataInput#readFloat()
   */
  @Override
  public float readFloat() {
    return this.buffer.getFloat();
  }

  public float readFloat(int pos) {
    return this.buffer.getFloat(pos);
  }

  /*
   * (non-Javadoc)
   *
   * @see java.io.DataInput#readFully(byte[])
   */
  @Override
  public void readFully(byte[] b) {
    this.buffer.get(b);

  }

  /*
   * (non-Javadoc)
   *
   * @see java.io.DataInput#readFully(byte[], int, int)
   */
  @Override
  public void readFully(byte[] b, int off, int len) {
    this.buffer.get(b, off, len);

  }

  /*
   * (non-Javadoc)
   *
   * @see java.io.DataInput#readInt()
   */
  @Override
  public int readInt() {
    return this.buffer.getInt();
  }

  public int readInt(int pos) {
    return this.buffer.getInt(pos);
  }

  /*
   * (non-Javadoc)
   *
   * @see java.io.DataInput#readLine()
   */
  @Override
  public String readLine() {
    throw new UnsupportedOperationException();
  }

  /*
   * (non-Javadoc)
   *
   * @see java.io.DataInput#readLong()
   */
  @Override
  public long readLong() {
    return this.buffer.getLong();
  }

  public long readLong(int pos) {
    return this.buffer.getLong(pos);
  }

  /*
   * (non-Javadoc)
   *
   * @see java.io.DataInput#readShort()
   */
  @Override
  public short readShort() {
    return this.buffer.getShort();
  }

  public short readShort(int pos) {
    return this.buffer.getShort(pos);
  }

  /*
   * (non-Javadoc)
   *
   * @see java.io.DataInput#readUTF()
   */
  @Override
  public String readUTF() throws IOException {
    return DataInputStream.readUTF(this);
  }

  /*
   * (non-Javadoc)
   *
   * @see java.io.DataInput#readUnsignedByte()
   */
  @Override
  public int readUnsignedByte() {
    return this.buffer.get() & 0xff;
  }

  public int readUnsignedByte(int pos) {
    return this.buffer.get(pos) & 0xff;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.io.DataInput#readUnsignedShort()
   */
  @Override
  public int readUnsignedShort() {
    return this.buffer.getShort() & 0xffff;
  }

  public int readUnsignedShort(int pos) {
    return this.buffer.getShort(pos) & 0xffff;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.io.DataInput#skipBytes(int)
   */
  @Override
  public int skipBytes(int n) {
    int newPosition = this.buffer.position() + n;
    if (newPosition > this.buffer.limit()) {
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
    // if (absPos < 0) {
    // throw new IllegalArgumentException("position was less than zero " + absPos);
    // } else if (absPos > this.buffer.limit()) {
    // throw new IllegalArgumentException( "position " + absPos + " was greater than the limit " +
    // this.buffer.limit());
    // }
    this.buffer.position(absPos);
  }

  public void sendTo(DataOutput out) throws IOException {
    this.buffer.position(0);
    this.buffer.sendTo(out);
  }

  public void sendTo(ByteBuffer out) {
    this.buffer.position(0);
    this.buffer.sendTo(out);
  }

  public ByteSource slice(int length) {
    return this.buffer.slice(length);
  }

  public ByteSource slice(int startOffset, int endOffset) {
    return this.buffer.slice(startOffset, endOffset);
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeBoolean(this.buffer != null);
    if (this.buffer != null) {
      out.writeInt(this.buffer.capacity());
      out.writeInt(this.buffer.limit());
      out.writeInt(this.buffer.position());
      for (int i = 0; i < this.buffer.capacity(); i++) {
        out.write(this.buffer.get(i));
      }
    }
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    boolean hasBuffer = in.readBoolean();
    if (hasBuffer) {
      int capacity = in.readInt();
      int limit = in.readInt();
      int position = in.readInt();
      byte[] bytes = new byte[capacity];
      int bytesRead = in.read(bytes);
      if (bytesRead != capacity) {
        throw new IOException(
            "Expected to read " + capacity + " bytes but only read " + bytesRead + " bytes.");
      }
      setBuffer(ByteBuffer.wrap(bytes, position, limit - position));
    } else {
      this.buffer = null;
    }
  }

  public ByteSource getBuffer() {
    return buffer;
  }
}
