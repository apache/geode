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
package org.apache.geode.internal;

import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UTFDataFormatException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.cache.BytesAndBitsForCompactor;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.tcp.ByteBufferInputStream.ByteSource;

/**
 * HeapDataOutputStream is an OutputStream that also implements DataOutput and stores all data
 * written to it in heap memory. It is always better to use this class instead
 * ByteArrayOutputStream.
 * <p>
 * This class is not thread safe
 * <p>
 * Added boolean flag that when turned on will throw an exception instead of allocating a new
 * buffer. The exception is a BufferOverflowException thrown from expand, and will restore the
 * position to the point at which the flag was set with the disallowExpansion method.
 *
 * Usage Model: boolean succeeded = true; stream.disallowExpansion(); try {
 * DataSerializer.writeObject(obj, stream); } catch (BufferOverflowException e) { succeeded = false;
 * }
 *
 * @since GemFire 5.0.2
 */
public class HeapDataOutputStream extends OutputStream
    implements ObjToByteArraySerializer, VersionedDataStream, ByteBufferWriter {
  private static final Logger logger = LogService.getLogger();

  ByteBuffer buffer;
  protected LinkedList<ByteBuffer> chunks = null;
  protected int size = 0;
  /**
   * True if this stream is currently setup for writing. Once it switches to reading then it must be
   * reset before it can be written again.
   */
  private boolean writeMode = true;
  private boolean ignoreWrites = false; // added for bug 39569
  private final int MIN_CHUNK_SIZE;
  private boolean disallowExpansion = false;
  private Error expansionException = null;
  private int memoPosition;
  private Version version;
  private boolean doNotCopy;

  private static final int INITIAL_CAPACITY = 1024;

  public HeapDataOutputStream(Version version) {
    this(INITIAL_CAPACITY, version);
  }

  /**
   * Create a HeapDataOutputStream optimized to contain just the specified string. The string will
   * be written to this stream encoded as utf.
   */
  public HeapDataOutputStream(String s) {
    int maxStrBytes;
    if (ASCII_STRINGS) {
      maxStrBytes = s.length();
    } else {
      maxStrBytes = s.length() * 3;
    }
    this.MIN_CHUNK_SIZE = INITIAL_CAPACITY;
    this.buffer = ByteBuffer.allocate(maxStrBytes);
    this.doNotCopy = false;
    writeUTFNoLength(s);
  }

  public HeapDataOutputStream(int allocSize, Version version) {
    this(allocSize, version, false);
  }

  /**
   * @param doNotCopy if true then byte arrays/buffers/sources will not be copied to this hdos but
   *        instead referenced.
   */
  public HeapDataOutputStream(int allocSize, Version version, boolean doNotCopy) {
    if (allocSize < 32) {
      this.MIN_CHUNK_SIZE = 32;
    } else {
      this.MIN_CHUNK_SIZE = allocSize;
    }
    this.buffer = ByteBuffer.allocate(allocSize);
    this.version = version;
    this.doNotCopy = doNotCopy;
  }

  /**
   * @param doNotCopy if true then byte arrays/buffers/sources will not be copied to this hdos but
   *        instead referenced.
   */
  public HeapDataOutputStream(ByteBuffer initialBuffer, Version version, boolean doNotCopy) {
    if (initialBuffer.position() != 0) {
      initialBuffer = initialBuffer.slice();
    }
    int allocSize = initialBuffer.capacity();
    if (allocSize < 32) {
      this.MIN_CHUNK_SIZE = 32;
    } else {
      this.MIN_CHUNK_SIZE = allocSize;
    }
    this.buffer = initialBuffer;
    this.version = version;
    this.doNotCopy = doNotCopy;
  }

  /**
   * Construct a HeapDataOutputStream which uses the byte array provided as its underlying
   * ByteBuffer
   *
   */
  public HeapDataOutputStream(byte[] bytes) {
    int len = bytes.length;
    if (len <= 0) {
      throw new IllegalArgumentException("The byte array must not be empty");
    }
    if (len > 32) {
      this.MIN_CHUNK_SIZE = len;
    } else {
      this.MIN_CHUNK_SIZE = 32;
    }
    this.buffer = ByteBuffer.wrap(bytes);
    this.doNotCopy = false;
  }

  /**
   * Returns true if this HDOS currently does not copy byte arrays/buffers written to it. Instead of
   * copying a reference is kept to the original array/buffer.
   */
  public boolean setDoNotCopy(boolean v) {
    boolean result = this.doNotCopy;
    if (result != v) {
      this.doNotCopy = v;
    }
    return result;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Version getVersion() {
    return this.version;
  }

  /*
   * throw an exception instead of allocating a new buffer. The exception is a
   * BufferOverflowException thrown from expand, and will restore the position to the point at which
   * the flag was set with the disallowExpansion method.
   *
   * @param ee the exception to throw if expansion is needed
   */
  public void disallowExpansion(Error ee) {
    this.disallowExpansion = true;
    this.expansionException = ee;
    this.memoPosition = this.buffer.position();
  }

  /** write the low-order 8 bits of the given int */
  @Override
  public void write(int b) {
    if (this.ignoreWrites)
      return;
    checkIfWritable();
    ensureCapacity(1);
    buffer.put((byte) b);
  }

  private void ensureCapacity(int amount) {
    int remainingSpace = this.buffer.capacity() - this.buffer.position();
    if (amount > remainingSpace) {
      expand(amount);
    }
  }

  private void expand(int amount) {
    if (this.disallowExpansion) {
      this.buffer.position(this.memoPosition);
      this.ignoreWrites = true;
      throw this.expansionException;
    }

    final ByteBuffer oldBuffer = this.buffer;
    if (this.chunks == null) {
      this.chunks = new LinkedList<ByteBuffer>();
    }
    oldBuffer.flip(); // now ready for reading
    this.size += oldBuffer.remaining();
    this.chunks.add(oldBuffer);
    if (amount < MIN_CHUNK_SIZE) {
      amount = MIN_CHUNK_SIZE;
    }
    this.buffer = ByteBuffer.allocate(amount);
  }

  private void checkIfWritable() {
    if (!this.writeMode) {
      throw new IllegalStateException(
          "not in write mode");
    }
  }

  /** override OutputStream's write() */
  @Override
  public void write(byte[] source, int offset, int len) {
    if (len == 0)
      return;
    if (this.ignoreWrites)
      return;
    checkIfWritable();
    if (this.doNotCopy && len > MIN_TO_COPY) {
      moveBufferToChunks();
      addToChunks(source, offset, len);
    } else {
      int remainingSpace = this.buffer.capacity() - this.buffer.position();
      if (remainingSpace < len) {
        this.buffer.put(source, offset, remainingSpace);
        offset += remainingSpace;
        len -= remainingSpace;
        ensureCapacity(len);
      }
      this.buffer.put(source, offset, len);
    }
  }

  private void addToChunks(byte[] source, int offset, int len) {
    ByteBuffer bb = ByteBuffer.wrap(source, offset, len).slice();
    bb = bb.slice();
    this.size += bb.remaining();
    this.chunks.add(bb);
  }

  private void addToChunks(ByteBuffer bb) {
    int remaining = bb.remaining();
    if (remaining > 0) {
      this.size += remaining;
      if (bb.position() != 0) {
        bb = bb.slice();
      }
      this.chunks.add(bb);
    }
  }

  public int getByteBufferCount() {
    int result = 0;
    if (this.chunks != null) {
      result += this.chunks.size();
    }
    if (this.buffer.remaining() > 0) {
      result++;
    }
    return result;
  }

  public void fillByteBufferArray(ByteBuffer[] bbArray, int offset) {
    if (this.chunks != null) {
      for (ByteBuffer bb : this.chunks) {
        bbArray[offset++] = bb;
      }
    }
    if (this.buffer.remaining() > 0) {
      bbArray[offset] = this.buffer;
    }
  }

  private void moveBufferToChunks() {
    final ByteBuffer oldBuffer = this.buffer;
    if (this.chunks == null) {
      this.chunks = new LinkedList<ByteBuffer>();
    }
    if (oldBuffer.position() == 0) {
      // if position is zero then nothing has been written (yet) to buffer so no need to move it to
      // chunks
      return;
    }
    oldBuffer.flip();
    this.size += oldBuffer.remaining();
    ByteBuffer bufToAdd = oldBuffer.slice();
    this.chunks.add(bufToAdd);
    int newPos = oldBuffer.limit();
    if ((oldBuffer.capacity() - newPos) <= 0) {
      this.buffer = ByteBuffer.allocate(MIN_CHUNK_SIZE);
    } else {
      oldBuffer.limit(oldBuffer.capacity());
      oldBuffer.position(newPos);
      this.buffer = oldBuffer.slice();
    }
  }

  public int size() {
    if (this.writeMode) {
      return this.size + this.buffer.position();
    } else {
      return this.size;
    }
  }

  /**
   * Free up any unused memory
   */
  public void trim() {
    finishWriting();
    if (this.buffer.limit() < this.buffer.capacity()) {
      // buffer is less than half full so allocate a new one and copy it in
      ByteBuffer bb = ByteBuffer.allocate(this.buffer.limit());
      bb.put(this.buffer);
      bb.flip(); // now ready for reading
      this.buffer = bb;
    }
  }

  private void consolidateChunks() {
    if (this.chunks != null) {
      final int size = size();
      ByteBuffer newBuffer = ByteBuffer.allocate(size);
      int newBufPos = 0;
      for (ByteBuffer bb : this.chunks) {
        newBuffer.put(bb);
        newBufPos += bb.position();
        newBuffer.position(newBufPos); // works around JRockit 1.4.2.04 bug
      }
      this.chunks = null;
      newBuffer.put(this.buffer);
      newBufPos += this.buffer.position();
      newBuffer.position(newBufPos); // works around JRockit 1.4.2.04 bug
      this.buffer = newBuffer;
      this.buffer.flip(); // now ready for reading
    }
  }

  /**
   * Prepare the contents for sending again
   */
  public void rewind() {
    finishWriting();
    this.size = 0;
    if (this.chunks != null) {
      for (ByteBuffer bb : this.chunks) {
        bb.rewind();
        size += bb.remaining();
      }
    }
    this.buffer.rewind();
    size += this.buffer.remaining();
  }

  public void reset() {
    this.size = 0;
    if (this.chunks != null) {
      this.chunks.clear();
      this.chunks = null;
    }
    this.buffer.clear();
    this.writeMode = true;
    this.ignoreWrites = false;
    this.disallowExpansion = false;
    this.expansionException = null;
  }

  @Override
  public void flush() {
    // noop
  }

  public void finishWriting() {
    if (this.writeMode) {
      this.ignoreWrites = false;
      this.writeMode = false;
      this.buffer.flip();
      this.size += this.buffer.remaining();
    }
  }

  /**
   * Returns a ByteBuffer of the unused buffer; returns null if the buffer was completely used.
   */
  public ByteBuffer finishWritingAndReturnUnusedBuffer() {
    finishWriting();
    ByteBuffer result = this.buffer.duplicate();
    if (result.remaining() == 0) {
      // buffer was never used.
      result.limit(result.capacity());
      return result;
    }
    int newPos = result.limit();
    if ((result.capacity() - newPos) > 0) {
      result.limit(result.capacity());
      result.position(newPos);
      return result.slice();
    } else {
      return null;
    }
  }

  @Override
  public void close() {
    reset();
  }

  /**
   * gets the contents of this stream as s ByteBuffer, ready for reading. The stream should not be
   * written to past this point until it has been reset.
   */
  public ByteBuffer toByteBuffer() {
    finishWriting();
    consolidateChunks();
    return this.buffer;
  }

  /**
   * gets the contents of this stream as a byte[]. The stream should not be written to past this
   * point until it has been reset.
   */
  public byte[] toByteArray() {
    ByteBuffer bb = toByteBuffer();
    if (bb.hasArray() && bb.arrayOffset() == 0 && bb.limit() == bb.capacity()) {
      return bb.array();
    } else {
      // create a new buffer of just the right size and copy the old buffer into it
      ByteBuffer tmp = ByteBuffer.allocate(bb.remaining());
      tmp.put(bb);
      tmp.flip();
      this.buffer = tmp;
      return this.buffer.array();
    }
  }


  /**
   * Writes this stream to the wrapper object of BytesAndBitsForCompactor type. The byte array
   * retrieved from the HeapDataOutputStream is set in the wrapper object. The byte array may be
   * partially filled. The valid length of data in the byte array is set in the wrapper. It is
   * assumed that the HeapDataOutputStream is appropriately seeded with a byte array from the
   * wrapper. However the filled byte array may or may not be the same as that used for seeding ,
   * depending upon whether the data got accommodated in the original byte buffer or not.
   *
   */
  // Asif
  public void sendTo(BytesAndBitsForCompactor wrapper, byte userBits) {
    ByteBuffer bb = toByteBuffer();
    if (bb.hasArray() && bb.arrayOffset() == 0) {
      wrapper.setData(bb.array(), userBits, bb.limit(), true /* is Reusable */);
    } else {
      // create a new buffer of just the right size and copy the old buffer into
      // it
      ByteBuffer tmp = ByteBuffer.allocate(bb.remaining());
      tmp.put(bb);
      tmp.flip();
      this.buffer = tmp;
      byte[] bytes = this.buffer.array();
      wrapper.setData(bytes, userBits, bytes.length, true /* is Reusable */);
    }
  }

  /**
   * Write this stream to the specified channel. Call multiple times until size returns zero to make
   * sure all bytes in the stream have been written.
   *
   * @return the number of bytes written, possibly zero.
   * @throws IOException if channel is closed, not yet connected, or some other I/O error occurs.
   */
  public int sendTo(SocketChannel chan) throws IOException {
    finishWriting();
    if (size() == 0) {
      return 0;
    }
    int result;
    if (this.chunks != null) {
      ByteBuffer[] bufs = new ByteBuffer[this.chunks.size() + 1];
      bufs = this.chunks.toArray(bufs);
      bufs[this.chunks.size()] = this.buffer;
      result = (int) chan.write(bufs);
    } else {
      result = chan.write(this.buffer);
    }
    this.size -= result;
    return result;
  }

  public void sendTo(SocketChannel chan, ByteBuffer out) throws IOException {
    finishWriting();
    if (size() == 0) {
      return;
    }
    out.clear();
    if (this.chunks != null) {
      for (ByteBuffer bb : this.chunks) {
        sendChunkTo(bb, chan, out);
      }
    }
    sendChunkTo(this.buffer, chan, out);
    flushBuffer(chan, out);
  }

  /**
   * sends the data from "in" by writing it to "sc" through "out" (out is used to chunk to data and
   * is probably a direct memory buffer).
   */
  private void sendChunkTo(ByteBuffer in, SocketChannel sc, ByteBuffer out) throws IOException {
    int bytesSent = in.remaining();
    if (in.isDirect()) {
      flushBuffer(sc, out);
      while (in.remaining() > 0) {
        sc.write(in);
      }
    } else {
      // copy in to out. If out fills flush it
      int OUT_MAX = out.remaining();
      if (bytesSent <= OUT_MAX) {
        out.put(in);
      } else {
        final byte[] bytes = in.array();
        int off = in.arrayOffset() + in.position();
        int len = bytesSent;
        while (len > 0) {
          int bytesThisTime = len;
          if (bytesThisTime > OUT_MAX) {
            bytesThisTime = OUT_MAX;
          }
          out.put(bytes, off, bytesThisTime);
          off += bytesThisTime;
          len -= bytesThisTime;
          flushBuffer(sc, out);
          OUT_MAX = out.remaining();
        }
        in.position(in.limit());
      }
    }
    this.size -= bytesSent;
  }

  private void flushBuffer(SocketChannel sc, ByteBuffer out) throws IOException {
    if (out.position() == 0)
      return;
    out.flip();
    while (out.remaining() > 0) {
      sc.write(out);
    }
    out.clear();
  }

  /**
   * Write the contents of this stream to the byte buffer.
   *
   * @throws BufferOverflowException if out is not large enough to contain all of our data.
   */
  public void sendTo(ByteBuffer out) {
    finishWriting();
    if (out.remaining() < size()) {
      throw new BufferOverflowException();
    }
    if (this.chunks != null) {
      for (ByteBuffer bb : this.chunks) {
        int bytesToWrite = bb.remaining();
        if (bytesToWrite > 0) {
          out.put(bb);
          this.size -= bytesToWrite;
        }
      }
    }
    {
      ByteBuffer bb = this.buffer;
      int bytesToWrite = bb.remaining();
      if (bytesToWrite > 0) {
        out.put(bb);
        this.size -= bytesToWrite;
      }
    }
  }

  /**
   * Write the contents of this stream to the specified stream using outBuf if a buffer is needed.
   */
  public void sendTo(OutputStream out, ByteBuffer outBuf) throws IOException {
    finishWriting();
    if (this.chunks != null) {
      for (ByteBuffer bb : this.chunks) {
        sendTo(out, outBuf, bb);
      }
    }
    sendTo(out, outBuf, this.buffer);
    flushStream(out, outBuf);
  }

  private void sendTo(OutputStream out, ByteBuffer outBuf, ByteBuffer inBuf) throws IOException {
    this.size -= writeByteBufferToStream(out, outBuf, inBuf);
  }

  /**
   * Returns the number of bytes written
   */
  public static int writeByteBufferToStream(OutputStream out, ByteBuffer outBuf, ByteBuffer inBuf)
      throws IOException {
    int bytesToWrite = inBuf.remaining();
    if (bytesToWrite > 0) {
      if (inBuf.hasArray()) {
        flushStream(out, outBuf);
        out.write(inBuf.array(), inBuf.arrayOffset() + inBuf.position(), bytesToWrite);
        inBuf.position(inBuf.limit());
      } else { // fix for bug 43007
        // copy direct inBuf to heap outBuf. If out fills flush it
        int bytesToWriteThisTime = bytesToWrite;
        int OUT_MAX = outBuf.remaining();
        while (bytesToWriteThisTime > OUT_MAX) {
          // copy only OUT_MAX bytes and flush outBuf
          int oldLimit = inBuf.limit();
          inBuf.limit(inBuf.position() + OUT_MAX);
          outBuf.put(inBuf);
          inBuf.limit(oldLimit);
          flushStream(out, outBuf);
          bytesToWriteThisTime -= OUT_MAX;
          OUT_MAX = outBuf.remaining();
        }
        outBuf.put(inBuf);
      }
    }
    return bytesToWrite;
  }

  public static void flushStream(OutputStream out, ByteBuffer outBuf) throws IOException {
    if (outBuf.position() == 0)
      return;
    assert outBuf.hasArray();
    outBuf.flip();
    out.write(outBuf.array(), outBuf.arrayOffset(), outBuf.remaining());
    outBuf.clear();
  }

  /**
   * Write the contents of this stream to the specified stream.
   */
  public void sendTo(ByteBufferWriter out) {
    finishWriting();
    if (this.chunks != null) {
      for (ByteBuffer bb : this.chunks) {
        basicSendTo(out, bb);
      }
    }
    basicSendTo(out, this.buffer);
  }

  private void basicSendTo(ByteBufferWriter out, ByteBuffer bb) {
    int bytesToWrite = bb.remaining();
    if (bytesToWrite > 0) {
      out.write(bb.duplicate());
      this.size -= bytesToWrite;
    }
  }

  /**
   * Returns an input stream that can be used to read the contents that where written to this output
   * stream.
   */
  public InputStream getInputStream() {
    return new HDInputStream();
  }

  private class HDInputStream extends InputStream {
    private Iterator<ByteBuffer> chunkIt;
    private ByteBuffer bb;

    public HDInputStream() {
      finishWriting();
      if (HeapDataOutputStream.this.chunks != null) {
        this.chunkIt = HeapDataOutputStream.this.chunks.iterator();
        nextChunk();
      } else {
        this.chunkIt = null;
        this.bb = HeapDataOutputStream.this.buffer;
      }
    }

    private void nextChunk() {
      if (this.chunkIt != null) {
        if (this.chunkIt.hasNext()) {
          this.bb = this.chunkIt.next();
        } else {
          this.chunkIt = null;
          this.bb = HeapDataOutputStream.this.buffer;
        }
      } else {
        this.bb = null; // EOF
      }
    }

    @Override
    public int available() {
      return size();
    }

    @Override
    public int read() {
      if (available() <= 0) {
        return -1;
      } else {
        int remaining = this.bb.limit() - this.bb.position();
        while (remaining == 0) {
          nextChunk();
          remaining = this.bb.limit() - this.bb.position();
        }
        consume(1);
        return this.bb.get() & 0xFF; // fix for bug 37068
      }
    }

    @Override
    public int read(byte[] dst, int off, int len) {
      if (available() <= 0) {
        return -1;
      } else {
        int readCount = 0;
        while (len > 0 && this.bb != null) {
          if (this.bb.limit() == this.bb.position()) {
            nextChunk();
          } else {
            int remaining = this.bb.limit() - this.bb.position();
            int bytesToRead = len;
            if (len > remaining) {
              bytesToRead = remaining;
            }
            this.bb.get(dst, off, bytesToRead);
            off += bytesToRead;
            len -= bytesToRead;
            readCount += bytesToRead;
          }
        }
        consume(readCount);
        return readCount;
      }
    }

    @Override
    public long skip(long n) {
      int remaining = size();
      if (remaining <= n) {
        // just skip over bytes remaining
        this.chunkIt = null;
        this.bb = null;
        consume(remaining);
        return remaining;
      } else {
        long skipped = 0;
        do {
          long skipsRemaining = n - skipped;
          skipped += chunkSkip(skipsRemaining);
        } while (skipped != n);
        return n;
      }
    }

    private long chunkSkip(long n) {
      int remaining = this.bb.limit() - this.bb.position();
      if (remaining <= n) {
        // skip this whole chunk
        this.bb.position(this.bb.limit());
        nextChunk();
        consume(remaining);
        return remaining;
      } else {
        // skip over just a part of this chunk
        this.bb.position(this.bb.position() + (int) n);
        consume((int) n);
        return n;
      }
    }

    private void consume(int c) {
      HeapDataOutputStream.this.size -= c;
    }

  }

  /**
   * Write the contents of this stream to the specified stream.
   * <p>
   * Note this implementation is exactly the same as writeTo(OutputStream) but they do not both
   * implement a common interface.
   */
  public void sendTo(DataOutput out) throws IOException {
    finishWriting();
    if (this.chunks != null) {
      for (ByteBuffer bb : this.chunks) {
        int bytesToWrite = bb.remaining();
        if (bytesToWrite > 0) {
          if (bb.hasArray()) {
            out.write(bb.array(), bb.arrayOffset() + bb.position(), bytesToWrite);
            bb.position(bb.limit());
          } else {
            byte[] bytes = new byte[bytesToWrite];
            bb.get(bytes);
            out.write(bytes);
          }
          this.size -= bytesToWrite;
        }
      }
    }
    {
      ByteBuffer bb = this.buffer;
      int bytesToWrite = bb.remaining();
      if (bytesToWrite > 0) {
        if (bb.hasArray()) {
          out.write(bb.array(), bb.arrayOffset() + bb.position(), bytesToWrite);
          bb.position(bb.limit());
        } else {
          byte[] bytes = new byte[bytesToWrite];
          bb.get(bytes);
          out.write(bytes);
        }
        this.size -= bytesToWrite;
      }
    }
  }

  // DataOutput methods
  /**
   * Writes a <code>boolean</code> value to this output stream. If the argument <code>v</code> is
   * <code>true</code>, the value <code>(byte)1</code> is written; if <code>v</code> is
   * <code>false</code>, the value <code>(byte)0</code> is written. The byte written by this method
   * may be read by the <code>readBoolean</code> method of interface <code>DataInput</code>, which
   * will then return a <code>boolean</code> equal to <code>v</code>.
   *
   * @param v the boolean to be written.
   */
  public void writeBoolean(boolean v) {
    write(v ? 1 : 0);
  }

  /**
   * Writes to the output stream the eight low- order bits of the argument <code>v</code>. The 24
   * high-order bits of <code>v</code> are ignored. (This means that <code>writeByte</code> does
   * exactly the same thing as <code>write</code> for an integer argument.) The byte written by this
   * method may be read by the <code>readByte</code> method of interface <code>DataInput</code>,
   * which will then return a <code>byte</code> equal to <code>(byte)v</code>.
   *
   * @param v the byte value to be written.
   */
  public void writeByte(int v) {
    write(v);
  }

  /**
   * Writes two bytes to the output stream to represent the value of the argument. The byte values
   * to be written, in the order shown, are:
   * <p>
   *
   * <pre>
   * <code>
   * (byte)(0xff &amp; (v &gt;&gt; 8))
   * (byte)(0xff &amp; v)
   * </code>
   * </pre>
   * <p>
   * The bytes written by this method may be read by the <code>readShort</code> method of interface
   * <code>DataInput</code> , which will then return a <code>short</code> equal to
   * <code>(short)v</code>.
   *
   * @param v the <code>short</code> value to be written.
   */
  public void writeShort(int v) {
    if (this.ignoreWrites)
      return;
    checkIfWritable();
    ensureCapacity(2);
    buffer.putShort((short) v);
  }

  /**
   * Writes a <code>char</code> value, wich is comprised of two bytes, to the output stream. The
   * byte values to be written, in the order shown, are:
   * <p>
   *
   * <pre>
   * <code>
   * (byte)(0xff &amp; (v &gt;&gt; 8))
   * (byte)(0xff &amp; v)
   * </code>
   * </pre>
   * <p>
   * The bytes written by this method may be read by the <code>readChar</code> method of interface
   * <code>DataInput</code> , which will then return a <code>char</code> equal to
   * <code>(char)v</code>.
   *
   * @param v the <code>char</code> value to be written.
   */
  public void writeChar(int v) {
    if (this.ignoreWrites)
      return;
    checkIfWritable();
    ensureCapacity(2);
    buffer.putChar((char) v);
  }

  /**
   * Writes an <code>int</code> value, which is comprised of four bytes, to the output stream. The
   * byte values to be written, in the order shown, are:
   * <p>
   *
   * <pre>
   * <code>
   * (byte)(0xff &amp; (v &gt;&gt; 24))
   * (byte)(0xff &amp; (v &gt;&gt; 16))
   * (byte)(0xff &amp; (v &gt;&gt; &#32; &#32;8))
   * (byte)(0xff &amp; v)
   * </code>
   * </pre>
   * <p>
   * The bytes written by this method may be read by the <code>readInt</code> method of interface
   * <code>DataInput</code> , which will then return an <code>int</code> equal to <code>v</code>.
   *
   * @param v the <code>int</code> value to be written.
   */
  public void writeInt(int v) {
    if (this.ignoreWrites)
      return;
    checkIfWritable();
    ensureCapacity(4);
    buffer.putInt(v);
  }

  /**
   * Writes a <code>long</code> value, which is comprised of eight bytes, to the output stream. The
   * byte values to be written, in the order shown, are:
   * <p>
   *
   * <pre>
   * <code>
   * (byte)(0xff &amp; (v &gt;&gt; 56))
   * (byte)(0xff &amp; (v &gt;&gt; 48))
   * (byte)(0xff &amp; (v &gt;&gt; 40))
   * (byte)(0xff &amp; (v &gt;&gt; 32))
   * (byte)(0xff &amp; (v &gt;&gt; 24))
   * (byte)(0xff &amp; (v &gt;&gt; 16))
   * (byte)(0xff &amp; (v &gt;&gt;  8))
   * (byte)(0xff &amp; v)
   * </code>
   * </pre>
   * <p>
   * The bytes written by this method may be read by the <code>readLong</code> method of interface
   * <code>DataInput</code> , which will then return a <code>long</code> equal to <code>v</code>.
   *
   * @param v the <code>long</code> value to be written.
   */
  public void writeLong(long v) {
    if (this.ignoreWrites)
      return;
    checkIfWritable();
    ensureCapacity(8);
    buffer.putLong(v);
  }

  /**
   * Reserves space in the output for a long and returns a LongUpdater than can be used to update
   * this particular long.
   *
   * @return the LongUpdater that allows the long to be updated
   */
  public LongUpdater reserveLong() {
    if (this.ignoreWrites)
      return null;
    checkIfWritable();
    ensureCapacity(8);
    LongUpdater result = new LongUpdater(this.buffer);
    buffer.putLong(0L);
    return result;
  }

  public static class LongUpdater {
    private final ByteBuffer bb;
    private final int pos;

    public LongUpdater(ByteBuffer bb) {
      this.bb = bb;
      this.pos = bb.position();
    }

    public void update(long v) {
      this.bb.putLong(this.pos, v);
    }
  }

  /**
   * Writes a <code>float</code> value, which is comprised of four bytes, to the output stream. It
   * does this as if it first converts this <code>float</code> value to an <code>int</code> in
   * exactly the manner of the <code>Float.floatToIntBits</code> method and then writes the
   * <code>int</code> value in exactly the manner of the <code>writeInt</code> method. The bytes
   * written by this method may be read by the <code>readFloat</code> method of interface
   * <code>DataInput</code>, which will then return a <code>float</code> equal to <code>v</code>.
   *
   * @param v the <code>float</code> value to be written.
   */
  public void writeFloat(float v) {
    if (this.ignoreWrites)
      return;
    checkIfWritable();
    ensureCapacity(4);
    buffer.putFloat(v);
  }

  /**
   * Writes a <code>double</code> value, which is comprised of eight bytes, to the output stream. It
   * does this as if it first converts this <code>double</code> value to a <code>long</code> in
   * exactly the manner of the <code>Double.doubleToLongBits</code> method and then writes the
   * <code>long</code> value in exactly the manner of the <code>writeLong</code> method. The bytes
   * written by this method may be read by the <code>readDouble</code> method of interface
   * <code>DataInput</code>, which will then return a <code>double</code> equal to <code>v</code>.
   *
   * @param v the <code>double</code> value to be written.
   */
  public void writeDouble(double v) {
    if (this.ignoreWrites)
      return;
    checkIfWritable();
    ensureCapacity(8);
    buffer.putDouble(v);
  }

  /**
   * Writes a string to the output stream. For every character in the string <code>s</code>, taken
   * in order, one byte is written to the output stream. If <code>s</code> is <code>null</code>, a
   * <code>NullPointerException</code> is thrown.
   * <p>
   * If <code>s.length</code> is zero, then no bytes are written. Otherwise, the character
   * <code>s[0]</code> is written first, then <code>s[1]</code>, and so on; the last character
   * written is <code>s[s.length-1]</code>. For each character, one byte is written, the low-order
   * byte, in exactly the manner of the <code>writeByte</code> method . The high-order eight bits of
   * each character in the string are ignored.
   *
   * @param str the string of bytes to be written.
   */
  public void writeBytes(String str) {
    if (this.ignoreWrites)
      return;
    checkIfWritable();
    int strlen = str.length();
    if (strlen > 0) {
      ensureCapacity(strlen);
      // I know this is a deprecated method but it is PERFECT for this impl.
      if (this.buffer.hasArray()) {
        // I know this is a deprecated method but it is PERFECT for this impl.
        int pos = this.buffer.position();
        str.getBytes(0, strlen, this.buffer.array(), this.buffer.arrayOffset() + pos);
        this.buffer.position(pos + strlen);
      } else {
        byte[] bytes = new byte[strlen];
        str.getBytes(0, strlen, bytes, 0);
        this.buffer.put(bytes);
      }
      // for (int i = 0 ; i < len ; i++) {
      // this.buffer.put((byte)s.charAt(i));
      // }
    }
  }

  /**
   * Writes every character in the string <code>s</code>, to the output stream, in order, two bytes
   * per character. If <code>s</code> is <code>null</code>, a <code>NullPointerException</code> is
   * thrown. If <code>s.length</code> is zero, then no characters are written. Otherwise, the
   * character <code>s[0]</code> is written first, then <code>s[1]</code>, and so on; the last
   * character written is <code>s[s.length-1]</code>. For each character, two bytes are actually
   * written, high-order byte first, in exactly the manner of the <code>writeChar</code> method.
   *
   * @param s the string value to be written.
   */
  public void writeChars(String s) {
    if (this.ignoreWrites)
      return;
    checkIfWritable();
    int len = s.length();
    if (len > 0) {
      ensureCapacity(len * 2);
      for (int i = 0; i < len; i++) {
        this.buffer.putChar(s.charAt(i));
      }
    }
  }

  /**
   * Use -Dgemfire.ASCII_STRINGS=true if all String instances contain ASCII characters. Setting this
   * to true gives a performance improvement.
   */
  private static final boolean ASCII_STRINGS =
      Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "ASCII_STRINGS");

  /**
   * Writes two bytes of length information to the output stream, followed by the Java modified UTF
   * representation of every character in the string <code>s</code>. If <code>s</code> is
   * <code>null</code>, a <code>NullPointerException</code> is thrown. Each character in the string
   * <code>s</code> is converted to a group of one, two, or three bytes, depending on the value of
   * the character.
   * <p>
   * If a character <code>c</code> is in the range <code>&#92;u0001</code> through
   * <code>&#92;u007f</code>, it is represented by one byte:
   * <p>
   *
   * <pre>
   * (byte) c
   * </pre>
   * <p>
   * If a character <code>c</code> is <code>&#92;u0000</code> or is in the range
   * <code>&#92;u0080</code> through <code>&#92;u07ff</code>, then it is represented by two bytes,
   * to be written in the order shown:
   * <p>
   *
   * <pre>
   * <code>
   * (byte)(0xc0 | (0x1f &amp; (c &gt;&gt; 6)))
   * (byte)(0x80 | (0x3f &amp; c))
   *  </code>
   * </pre>
   * <p>
   * If a character <code>c</code> is in the range <code>&#92;u0800</code> through
   * <code>uffff</code>, then it is represented by three bytes, to be written in the order shown:
   * <p>
   *
   * <pre>
   * <code>
   * (byte)(0xe0 | (0x0f &amp; (c &gt;&gt; 12)))
   * (byte)(0x80 | (0x3f &amp; (c &gt;&gt;  6)))
   * (byte)(0x80 | (0x3f &amp; c))
   *  </code>
   * </pre>
   * <p>
   * First, the total number of bytes needed to represent all the characters of <code>s</code> is
   * calculated. If this number is larger than <code>65535</code>, then a
   * <code>UTFDataFormatException</code> is thrown. Otherwise, this length is written to the output
   * stream in exactly the manner of the <code>writeShort</code> method; after this, the one-, two-,
   * or three-byte representation of each character in the string <code>s</code> is written.
   * <p>
   * The bytes written by this method may be read by the <code>readUTF</code> method of interface
   * <code>DataInput</code> , which will then return a <code>String</code> equal to <code>s</code>.
   *
   * @param str the string value to be written.
   */
  public void writeUTF(String str) throws UTFDataFormatException {
    if (this.ignoreWrites)
      return;
    checkIfWritable();
    if (ASCII_STRINGS) {
      writeAsciiUTF(str, true);
    } else {
      writeFullUTF(str, true);
    }
  }

  private void writeAsciiUTF(String str, boolean encodeLength) throws UTFDataFormatException {
    int strlen = str.length();
    if (encodeLength && strlen > 65535) {
      throw new UTFDataFormatException();
    }

    int maxLen = strlen;
    if (encodeLength) {
      maxLen += 2;
    }
    ensureCapacity(maxLen);

    if (encodeLength) {
      this.buffer.putShort((short) strlen);
    }
    if (this.buffer.hasArray()) {
      // I know this is a deprecated method but it is PERFECT for this impl.
      int pos = this.buffer.position();
      str.getBytes(0, strlen, this.buffer.array(), this.buffer.arrayOffset() + pos);
      this.buffer.position(pos + strlen);
    } else {
      for (int i = 0; i < strlen; i++) {
        this.buffer.put((byte) str.charAt(i));
      }
      // byte[] bytes = new byte[strlen];
      // str.getBytes(0, strlen, bytes, 0);
      // this.buffer.put(bytes);
    }
  }

  /**
   * The logic used here is based on java's DataOutputStream.writeUTF() from the version 1.6.0_10.
   * The reader code should use the logic similar to DataOutputStream.readUTF() from the version
   * 1.6.0_10 to decode this properly.
   */
  private void writeFullUTF(String str, boolean encodeLength) throws UTFDataFormatException {
    int strlen = str.length();
    if (encodeLength && strlen > 65535) {
      throw new UTFDataFormatException();
    }
    // make room for worst case space 3 bytes for each char and 2 for len
    {
      int maxLen = (strlen * 3);
      if (encodeLength) {
        maxLen += 2;
      }
      ensureCapacity(maxLen);
    }
    int utfSizeIdx = this.buffer.position();
    if (encodeLength) {
      // skip bytes reserved for length
      this.buffer.position(utfSizeIdx + 2);
    }
    for (int i = 0; i < strlen; i++) {
      int c = str.charAt(i);
      if ((c >= 0x0001) && (c <= 0x007F)) {
        this.buffer.put((byte) c);
      } else if (c > 0x07FF) {
        this.buffer.put((byte) (0xE0 | ((c >> 12) & 0x0F)));
        this.buffer.put((byte) (0x80 | ((c >> 6) & 0x3F)));
        this.buffer.put((byte) (0x80 | ((c >> 0) & 0x3F)));
      } else {
        this.buffer.put((byte) (0xC0 | ((c >> 6) & 0x1F)));
        this.buffer.put((byte) (0x80 | ((c >> 0) & 0x3F)));
      }
    }
    int utflen = this.buffer.position() - utfSizeIdx;
    if (encodeLength) {
      utflen -= 2;
      if (utflen > 65535) {
        // act as if we wrote nothing to this buffer
        this.buffer.position(utfSizeIdx);
        throw new UTFDataFormatException();
      }
      this.buffer.putShort(utfSizeIdx, (short) utflen);
    }
  }

  /**
   * Same as {@link #writeUTF} but it does not encode the length in the first two bytes and allows
   * strings longer than 65k to be encoded.
   */
  public void writeUTFNoLength(String str) {
    if (this.ignoreWrites)
      return;
    checkIfWritable();
    try {
      if (ASCII_STRINGS) {
        writeAsciiUTF(str, false);
      } else {
        writeFullUTF(str, false);
      }
    } catch (UTFDataFormatException ex) {
      // this shouldn't happen since we did not encode the length
      throw new IllegalStateException(
          String.format("unexpected %s", ex));
    }
  }

  /**
   * Writes the given object to this stream as a byte array. The byte array is produced by
   * serializing v. The serialization is done by calling DataSerializer.writeObject.
   */
  public void writeAsSerializedByteArray(Object v) throws IOException {
    if (this.ignoreWrites)
      return;
    checkIfWritable();
    ensureCapacity(5);
    if (v instanceof HeapDataOutputStream) {
      HeapDataOutputStream other = (HeapDataOutputStream) v;
      other.finishWriting();
      InternalDataSerializer.writeArrayLength(other.size(), this);
      if (this.doNotCopy) {
        if (other.chunks != null) {
          for (ByteBuffer bb : other.chunks) {
            write(bb);
          }
        }
        write(other.buffer);
      } else {
        other.sendTo((ByteBufferWriter) this);
        other.rewind();
      }
    } else {
      ByteBuffer sizeBuf = this.buffer;
      int sizePos = sizeBuf.position();
      sizeBuf.position(sizePos + 5);
      final int preArraySize = size();
      DataSerializer.writeObject(v, this);
      int arraySize = size() - preArraySize;
      sizeBuf.put(sizePos, InternalDataSerializer.INT_ARRAY_LEN);
      sizeBuf.putInt(sizePos + 1, arraySize);
    }
  }

  /**
   * We set "doNotCopy" to prevent wasting time by copying bytes. But to do this we create either a
   * HeapByteBuffer to DirectByteBuffer to reference the byte array or off-heap memory. The
   * ByteBuffer instance itself uses up memory that needs to be initialized and eventually gc'd so
   * for smaller sizes it is better to just copy it. Public for unit test access.
   */
  public static final int MIN_TO_COPY = 128;

  /**
   * Write a byte buffer to this HeapDataOutputStream,
   *
   * the contents of the buffer between the position and the limit are copied to the output stream.
   */
  @Override
  public void write(ByteBuffer bb) {
    if (this.ignoreWrites)
      return;
    checkIfWritable();
    int remaining = bb.remaining();
    if (remaining == 0)
      return;
    if (this.doNotCopy && remaining > MIN_TO_COPY) {
      moveBufferToChunks();
      addToChunks(bb);
    } else {
      int remainingSpace = this.buffer.remaining();
      if (remainingSpace < remaining) {
        int oldLimit = bb.limit();
        bb.limit(bb.position() + remainingSpace);
        this.buffer.put(bb);
        bb.limit(oldLimit);
        ensureCapacity(bb.remaining());
      }
      this.buffer.put(bb);
    }
  }

  /**
   * Write a byte source to this HeapDataOutputStream,
   *
   * the contents of the buffer between the position and the limit are copied to the output stream.
   */
  public void write(ByteSource source) {
    ByteBuffer bb = source.getBackingByteBuffer();
    if (bb != null) {
      write(bb);
      return;
    }
    if (this.ignoreWrites)
      return;
    checkIfWritable();
    int remainingSpace = this.buffer.limit() - this.buffer.position();
    if (remainingSpace < source.remaining()) {
      int oldLimit = source.limit();
      source.limit(source.position() + remainingSpace);
      source.sendTo(this.buffer);
      source.limit(oldLimit);
      ensureCapacity(source.remaining());
    }
    source.sendTo(this.buffer);
  }
}
