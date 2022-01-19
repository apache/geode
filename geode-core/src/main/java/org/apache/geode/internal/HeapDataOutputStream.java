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
import java.io.OutputStream;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.apache.geode.DataSerializer;
import org.apache.geode.internal.cache.BytesAndBitsForCompactor;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.StaticSerialization;
import org.apache.geode.internal.tcp.ByteBufferInputStream;

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
public class HeapDataOutputStream extends
    org.apache.geode.internal.serialization.BufferDataOutputStream
    implements ObjToByteArraySerializer, ByteBufferWriter {

  public HeapDataOutputStream(KnownVersion version) {
    this(INITIAL_CAPACITY, version);
  }

  /**
   * Create a HeapDataOutputStream optimized to contain just the specified string. The string will
   * be written to this stream encoded as utf.
   */
  public HeapDataOutputStream(String s) {
    super(s);
  }

  public HeapDataOutputStream(int allocSize, KnownVersion version) {
    this(allocSize, version, false);
  }

  public HeapDataOutputStream(int allocSize) {
    this(allocSize, null, false);
  }

  /**
   * @param doNotCopy if true then byte arrays/buffers/sources will not be copied to this hdos but
   *        instead referenced.
   */
  public HeapDataOutputStream(int allocSize, KnownVersion version, boolean doNotCopy) {
    super(allocSize, version, doNotCopy);
  }

  /**
   * @param doNotCopy if true then byte arrays/buffers/sources will not be copied to this hdos but
   *        instead referenced.
   */
  public HeapDataOutputStream(ByteBuffer initialBuffer, KnownVersion version, boolean doNotCopy) {
    super(initialBuffer, version, doNotCopy);
  }

  /**
   * Construct a HeapDataOutputStream which uses the byte array provided as its underlying
   * ByteBuffer
   *
   */
  public HeapDataOutputStream(byte[] bytes) {
    super(bytes);
  }

  /**
   * Free up any unused memory
   */
  public void trim() {
    finishWriting();
    if (buffer.limit() < buffer.capacity()) {
      // buffer is less than half full so allocate a new one and copy it in
      ByteBuffer bb = ByteBuffer.allocate(buffer.limit());
      bb.put(buffer);
      bb.flip(); // now ready for reading
      buffer = bb;
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
      buffer = tmp;
      byte[] bytes = buffer.array();
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
    if (chunks != null) {
      ByteBuffer[] bufs = new ByteBuffer[chunks.size() + 1];
      bufs = chunks.toArray(bufs);
      bufs[chunks.size()] = buffer;
      result = (int) chan.write(bufs);
    } else {
      result = chan.write(buffer);
    }
    size -= result;
    return result;
  }

  public void sendTo(SocketChannel chan, ByteBuffer out) throws IOException {
    finishWriting();
    if (size() == 0) {
      return;
    }
    out.clear();
    if (chunks != null) {
      for (ByteBuffer bb : chunks) {
        sendChunkTo(bb, chan, out);
      }
    }
    sendChunkTo(buffer, chan, out);
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
    size -= bytesSent;
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
    if (chunks != null) {
      for (ByteBuffer bb : chunks) {
        int bytesToWrite = bb.remaining();
        if (bytesToWrite > 0) {
          out.put(bb);
          size -= bytesToWrite;
        }
      }
    }
    {
      ByteBuffer bb = buffer;
      int bytesToWrite = bb.remaining();
      if (bytesToWrite > 0) {
        out.put(bb);
        size -= bytesToWrite;
      }
    }
  }

  /**
   * Write the contents of this stream to the specified stream using outBuf if a buffer is needed.
   */
  public void sendTo(OutputStream out, ByteBuffer outBuf) throws IOException {
    finishWriting();
    if (chunks != null) {
      for (ByteBuffer bb : chunks) {
        sendTo(out, outBuf, bb);
      }
    }
    sendTo(out, outBuf, buffer);
    flushStream(out, outBuf);
  }

  private void sendTo(OutputStream out, ByteBuffer outBuf, ByteBuffer inBuf) throws IOException {
    size -= writeByteBufferToStream(out, outBuf, inBuf);
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

  /**
   * Write the contents of this stream to the specified stream.
   */
  public void sendTo(ByteBufferWriter out) {
    finishWriting();
    if (chunks != null) {
      for (ByteBuffer bb : chunks) {
        basicSendTo(out, bb);
      }
    }
    basicSendTo(out, buffer);
  }

  private void basicSendTo(ByteBufferWriter out, ByteBuffer bb) {
    int bytesToWrite = bb.remaining();
    if (bytesToWrite > 0) {
      out.write(bb.duplicate());
      size -= bytesToWrite;
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
    if (chunks != null) {
      for (ByteBuffer bb : chunks) {
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
          size -= bytesToWrite;
        }
      }
    }
    {
      ByteBuffer bb = buffer;
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
        size -= bytesToWrite;
      }
    }
  }

  // DataOutput methods

  /**
   * Writes the given object to this stream as a byte array. The byte array is produced by
   * serializing v. The serialization is done by calling DataSerializer.writeObject.
   */
  @Override
  public void writeAsSerializedByteArray(Object v) throws IOException {
    if (ignoreWrites) {
      return;
    }
    checkIfWritable();
    ensureCapacity(5);
    if (v instanceof HeapDataOutputStream) {
      HeapDataOutputStream other = (HeapDataOutputStream) v;
      other.finishWriting();
      InternalDataSerializer.writeArrayLength(other.size(), this);
      if (doNotCopy) {
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
      ByteBuffer sizeBuf = buffer;
      int sizePos = sizeBuf.position();
      sizeBuf.position(sizePos + 5);
      final int preArraySize = size();
      DataSerializer.writeObject(v, this);
      int arraySize = size() - preArraySize;
      sizeBuf.put(sizePos, StaticSerialization.INT_ARRAY_LEN);
      sizeBuf.putInt(sizePos + 1, arraySize);
    }
  }

  /**
   * Write a byte source to this HeapDataOutputStream,
   *
   * the contents of the buffer between the position and the limit are copied to the output stream.
   */
  public void write(ByteBufferInputStream.ByteSource source) {
    ByteBuffer bb = source.getBackingByteBuffer();
    if (bb != null) {
      write(bb);
      return;
    }
    if (ignoreWrites) {
      return;
    }
    checkIfWritable();
    int remainingSpace = buffer.limit() - buffer.position();
    if (remainingSpace < source.remaining()) {
      int oldLimit = source.limit();
      source.limit(source.position() + remainingSpace);
      source.sendTo(buffer);
      source.limit(oldLimit);
      ensureCapacity(source.remaining());
    }
    source.sendTo(buffer);
  }


}
