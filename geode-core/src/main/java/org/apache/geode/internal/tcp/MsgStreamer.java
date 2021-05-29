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

import java.io.IOException;
import java.io.OutputStream;
import java.io.UTFDataFormatException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import org.apache.geode.DataSerializer;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.distributed.internal.DMStats;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.ByteBufferWriter;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.ObjToByteArraySerializer;
import org.apache.geode.internal.net.BufferPool;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.StaticSerialization;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * <p>
 * MsgStreamer supports streaming a message to a tcp Connection in chunks. This allows us to send a
 * message without needing to pre-serialize it completely in memory thus saving buffer memory.
 *
 * @since GemFire 5.0.2
 *
 */
public class MsgStreamer extends OutputStream
    implements ObjToByteArraySerializer, BaseMsgStreamer, ByteBufferWriter {

  /**
   * List of connections to send this msg to.
   */
  private final @NotNull List<Connection> connections;

  private final BufferPool bufferPool;

  /**
   * Any exceptions that happen during sends
   */
  private @Nullable ConnectExceptions connectExceptions;

  /**
   * The byte buffer we used for preparing a chunk of the message. Currently this buffer is obtained
   * from the connection.
   */
  private final ByteBuffer buffer;

  private int flushedBytes = 0;

  /**
   * the message this streamer is to send
   */
  private final DistributionMessage msg;

  /**
   * True if this message went out as a normal one (it fit it one chunk) False if this message
   * needed to be chunked.
   */
  private boolean normalMsg = false;

  /**
   * Set to true when we have started serializing a message. If this is true and doneWritingMsg is
   * false and we think we have finished writing the msg then we have a problem.
   */
  private boolean startedSerializingMsg = false;

  /**
   * Set to true after last byte of message has been written to this stream.
   */
  private boolean doneWritingMsg = false;

  private final DMStats stats;
  private short msgId;
  private long serStartTime;
  private final boolean directReply;

  /**
   * Called to free up resources used by this streamer after the streamer has produced its message.
   */
  protected void release() {
    MsgIdGenerator.release(msgId);
    buffer.clear();
    overflowBuf = null;
    bufferPool.releaseSenderBuffer(buffer);
  }

  @Override
  public @Nullable ConnectExceptions getConnectExceptions() {
    return connectExceptions;
  }

  @Override
  public @NotNull List<@NotNull Connection> getSentConnections() {
    return connections;
  }

  /**
   * Create a msg streamer that will send the given msg to the given cons.
   *
   * Note: This is no longer supposed to be called directly rather the {@link #create} method should
   * now be used.
   */
  MsgStreamer(@NotNull List<Connection> connections, DistributionMessage msg, boolean directReply,
      DMStats stats,
      int sendBufferSize, BufferPool bufferPool) {
    this.stats = stats;
    this.msg = msg;
    this.connections = connections;
    int bufferSize = Math.min(sendBufferSize, Connection.MAX_MSG_SIZE);
    buffer = bufferPool.acquireDirectSenderBuffer(bufferSize);
    buffer.clear();
    buffer.position(Connection.MSG_HEADER_BYTES);
    msgId = MsgIdGenerator.NO_MSG_ID;
    this.directReply = directReply;
    this.bufferPool = bufferPool;
    startSerialization();
  }

  /**
   * Create message streamers splitting into versioned streamers, if required, for given list of
   * connections to remote nodes. This method can either return a single MsgStreamer object or a
   * List of MsgStreamer objects.
   */
  public static BaseMsgStreamer create(List<Connection> cons, final DistributionMessage msg,
      final boolean directReply, final DMStats stats, BufferPool bufferPool) {
    final Connection firstCon = cons.get(0);

    // split into different versions if required
    final int numCons = cons.size();
    if (numCons > 1) {
      Object2ObjectOpenHashMap<KnownVersion, List<Connection>> versionToConnMap = null;
      int numVersioned = 0;
      for (final Connection connection : cons) {
        final KnownVersion version = connection.getRemoteVersion();
        if (version != null
            && KnownVersion.CURRENT_ORDINAL > version.ordinal()) {
          if (versionToConnMap == null) {
            versionToConnMap = new Object2ObjectOpenHashMap<>();
          }
          versionToConnMap.computeIfAbsent(version, k -> new ArrayList<>(numCons)).add(connection);
          numVersioned++;
        }
      }

      if (versionToConnMap == null) {
        return new MsgStreamer(cons, msg, directReply, stats, firstCon.getSendBufferSize(),
            bufferPool);
      } else {
        // if there is a versioned stream created, then split remaining
        // connections to unversioned stream
        final List<MsgStreamer> streamers = new ArrayList<>(versionToConnMap.size() + 1);
        final int sendBufferSize = firstCon.getSendBufferSize();
        if (numCons > numVersioned) {
          // allocating list of numCons size so that as the result of
          // getSentConnections it may not need to be reallocated later
          final List<Connection> currentVersionConnections = new ArrayList<>(numCons);
          for (Connection connection : cons) {
            final KnownVersion version = connection.getRemoteVersion();
            if (version == null || version.ordinal() >= KnownVersion.CURRENT_ORDINAL) {
              currentVersionConnections.add(connection);
            }
          }
          streamers.add(
              new MsgStreamer(currentVersionConnections, msg, directReply, stats, sendBufferSize,
                  bufferPool));
        }
        for (ObjectIterator<Object2ObjectMap.Entry<KnownVersion, List<Connection>>> itr =
            versionToConnMap.object2ObjectEntrySet().fastIterator(); itr.hasNext();) {
          Object2ObjectMap.Entry<KnownVersion, List<Connection>> entry = itr.next();
          streamers.add(new VersionedMsgStreamer(entry.getValue(), msg, directReply, stats,
              bufferPool, sendBufferSize, entry.getKey()));
        }
        return new MsgStreamerList(streamers);
      }
    } else {
      final KnownVersion version = firstCon.getRemoteVersion();
      if (null == version) {
        return new MsgStreamer(cons, msg, directReply, stats, firstCon.getSendBufferSize(),
            bufferPool);
      } else {
        return new VersionedMsgStreamer(cons, msg, directReply, stats, bufferPool,
            firstCon.getSendBufferSize(), version);
      }
    }
  }

  @Override
  public void reserveConnections(long startTime, long ackTimeout, long ackSDTimeout) {
    for (final Connection connection : connections) {
      connection.setInUse(true, startTime, ackTimeout, ackSDTimeout, connections);
      if (ackTimeout > 0) {
        connection.scheduleAckTimeouts();
      }
    }
  }

  private void startSerialization() {
    serStartTime = stats.startMsgSerialization();
  }

  @Override
  public int writeMessage() throws IOException {
    try {
      startedSerializingMsg = true;
      InternalDataSerializer.writeDSFID(msg, this);
      doneWritingMsg = true;
      if (flushedBytes == 0) {
        // message fit in one chunk
        normalMsg = true;
      }
      realFlush(true);
      return flushedBytes;
    } finally {
      release();
    }
  }

  @Override
  public void write(int b) {
    ensureCapacity(1);
    if (overflowBuf != null) {
      overflowBuf.write(b);
      return;
    }
    buffer.put((byte) (b & 0xff));
  }

  private void ensureCapacity(int amount) {
    if (overflowBuf != null) {
      return;
    }
    int remainingSpace = buffer.capacity() - buffer.position();
    if (amount > remainingSpace) {
      realFlush(false);
    }
  }

  @Override
  public void flush() {
    // this is a noop so that when ObjectOutputStream calls us
    // for each chunk from it we will not send data early to our connection.
  }

  private int overflowMode = 0;
  private HeapDataOutputStream overflowBuf = null;

  private boolean isOverflowMode() {
    return overflowMode > 0;
  }

  private void enableOverflowMode() {
    overflowMode++;
  }

  private void disableOverflowMode() {
    overflowMode--;
    if (!isOverflowMode()) {
      overflowBuf = null;
    }
  }

  public void realFlush(boolean lastFlushForMessage) {
    if (isOverflowMode()) {
      if (overflowBuf == null) {
        overflowBuf = new HeapDataOutputStream(
            buffer.capacity() - Connection.MSG_HEADER_BYTES, KnownVersion.CURRENT);
      }
      return;
    }
    buffer.flip();
    setMessageHeader();
    final int serializedBytes = buffer.limit();
    flushedBytes += serializedBytes;
    DistributionMessage conflationMsg = null;
    if (normalMsg) {
      // we can't conflate chunked messages; this fixes bug 36633
      conflationMsg = msg;
    }
    stats.endMsgSerialization(serStartTime);
    for (final Iterator<Connection> it = connections.iterator(); it.hasNext();) {
      final Connection connection = it.next();
      try {
        connection.sendPreserialized(buffer,
            lastFlushForMessage && msg.containsRegionContentChange(), conflationMsg);
      } catch (IOException ex) {
        it.remove();
        if (connectExceptions == null) {
          connectExceptions = new ConnectExceptions();
        }
        connectExceptions.addFailure(connection.getRemoteAddress(), ex);
        connection.closeForReconnect(
            String.format("closing due to %s", "IOException"));
      } catch (ConnectionException ex) {
        it.remove();
        if (connectExceptions == null) {
          connectExceptions = new ConnectExceptions();
        }
        connectExceptions.addFailure(connection.getRemoteAddress(), ex);
        connection.closeForReconnect(
            String.format("closing due to %s", "ConnectionException"));
      }
      buffer.rewind();
    }
    startSerialization();
    buffer.clear();
    buffer.position(Connection.MSG_HEADER_BYTES);
  }

  @VisibleForTesting
  protected ByteBuffer getBuffer() {
    return buffer;
  }

  @Override
  public void close() throws IOException {
    try {
      if (startedSerializingMsg && !doneWritingMsg) {
        // if we wrote any bytes on the connections then we need to close them
        // since they have been corrupted by a partial serialization.
        if (flushedBytes > 0) {
          for (final Connection connection : connections) {
            connection.closeForReconnect("Message serialization could not complete");
          }
        }
      }
    } finally {
      super.close();
    }
  }

  /** override OutputStream's write() */
  @Override
  public void write(byte @NotNull [] source, int offset, int len) {
    if (overflowBuf != null) {
      overflowBuf.write(source, offset, len);
      return;
    }
    while (len > 0) {
      int remainingSpace = buffer.capacity() - buffer.position();
      if (remainingSpace == 0) {
        realFlush(false);
        if (overflowBuf != null) {
          overflowBuf.write(source, offset, len);
          return;
        }
      } else {
        int chunkSize = remainingSpace;
        if (len < chunkSize) {
          chunkSize = len;
        }
        buffer.put(source, offset, chunkSize);
        offset += chunkSize;
        len -= chunkSize;
      }
    }
  }

  @Override
  public void write(ByteBuffer bb) {
    if (overflowBuf != null) {
      overflowBuf.write(bb);
      return;
    }
    int len = bb.remaining();
    while (len > 0) {
      int remainingSpace = buffer.capacity() - buffer.position();
      if (remainingSpace == 0) {
        realFlush(false);
        if (overflowBuf != null) {
          overflowBuf.write(bb);
          return;
        }
      } else {
        int chunkSize = remainingSpace;
        if (len < chunkSize) {
          chunkSize = len;
        }
        int oldLimit = bb.limit();
        bb.limit(bb.position() + chunkSize);
        buffer.put(bb);
        bb.limit(oldLimit);
        len -= chunkSize;
      }
    }
  }

  /**
   * write the header after the message has been written to the stream
   */
  private void setMessageHeader() {
    Assert.assertTrue(overflowBuf == null);
    Assert.assertTrue(!isOverflowMode());
    // int processorType = this.msg.getProcessorType();
    int msgType;
    if (doneWritingMsg) {
      if (normalMsg) {
        msgType = Connection.NORMAL_MSG_TYPE;
      } else {
        msgType = Connection.END_CHUNKED_MSG_TYPE;
      }
      if (directReply) {
        msgType |= Connection.DIRECT_ACK_BIT;
      }
    } else {
      msgType = Connection.CHUNKED_MSG_TYPE;
    }
    if (!normalMsg) {
      if (msgId == MsgIdGenerator.NO_MSG_ID) {
        msgId = MsgIdGenerator.obtain();
      }
    }

    buffer.putInt(Connection.MSG_HEADER_SIZE_OFFSET,
        Connection.calcHdrSize(buffer.limit() - Connection.MSG_HEADER_BYTES));
    buffer.put(Connection.MSG_HEADER_TYPE_OFFSET, (byte) (msgType & 0xff));
    buffer.putShort(Connection.MSG_HEADER_ID_OFFSET, msgId);
    buffer.position(0);
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
  @Override
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
  @Override
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
  @Override
  public void writeShort(int v) {
    // if (logger.isTraceEnabled()) logger.trace(" short={}", v);

    ensureCapacity(2);
    if (overflowBuf != null) {
      overflowBuf.writeShort(v);
      return;
    }
    buffer.putShort((short) (v & 0xffff));
  }

  /**
   * Writes a <code>char</code> value, which is comprised of two bytes, to the output stream. The
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
  @Override
  public void writeChar(int v) {
    // if (logger.isTraceEnabled()) logger.trace(" char={}", v);

    ensureCapacity(2);
    if (overflowBuf != null) {
      overflowBuf.writeChar(v);
      return;
    }
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
  @Override
  public void writeInt(int v) {
    // if (logger.isTraceEnabled()) logger.trace(" int={}", v);

    ensureCapacity(4);
    if (overflowBuf != null) {
      overflowBuf.writeInt(v);
      return;
    }
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
  @Override
  public void writeLong(long v) {
    // if (logger.isTraceEnabled()) logger.trace(" long={}", v);

    ensureCapacity(8);
    if (overflowBuf != null) {
      overflowBuf.writeLong(v);
      return;
    }
    buffer.putLong(v);
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
  @Override
  public void writeFloat(float v) {
    // if (logger.isTraceEnabled()) logger.trace(" float={}", v);

    ensureCapacity(4);
    if (overflowBuf != null) {
      overflowBuf.writeFloat(v);
      return;
    }
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
  @Override
  public void writeDouble(double v) {
    // if (logger.isTraceEnabled()) logger.trace(" double={}", v);

    ensureCapacity(8);
    if (overflowBuf != null) {
      overflowBuf.writeDouble(v);
      return;
    }
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
  @Override
  public void writeBytes(final @NotNull String str) {
    // if (logger.isTraceEnabled()) logger.trace(" bytes={}", str);

    if (overflowBuf != null) {
      overflowBuf.writeBytes(str);
      return;
    }
    int strlen = str.length();
    if (strlen > 0) {
      for (int i = 0; i < strlen; i++) {
        writeByte((byte) str.charAt(i));
      }
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
  @Override
  public void writeChars(final @NotNull String s) {
    // if (logger.isTraceEnabled()) logger.trace(" chars={}", s);

    if (overflowBuf != null) {
      overflowBuf.writeChars(s);
      return;
    }
    int len = s.length();
    int offset = 0;
    while (len > 0) {
      int remainingCharSpace = (buffer.capacity() - buffer.position()) / 2;
      if (remainingCharSpace == 0) {
        realFlush(false);
        if (overflowBuf != null) {
          overflowBuf.writeChars(s.substring(offset));
          return;
        }
      } else {
        int chunkSize = remainingCharSpace;
        if (len < chunkSize) {
          chunkSize = len;
        }
        for (int i = 0; i < chunkSize; i++) {
          buffer.putChar(s.charAt(offset + i));
        }
        offset += chunkSize;
        len -= chunkSize;
      }
    }
  }

  /**
   * Use -Dgemfire.ASCII_STRINGS=true if all String instances contain ASCII characters. Setting this
   * to true gives a performance improvement.
   */
  private static final boolean ASCII_STRINGS =
      Boolean.getBoolean(GeodeGlossary.GEMFIRE_PREFIX + "ASCII_STRINGS");

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
   * @exception IOException if an I/O error occurs.
   */
  @Override
  public void writeUTF(@NotNull String str) throws IOException {
    // if (logger.isTraceEnabled()) logger.trace(" utf={}", str);

    if (overflowBuf != null) {
      overflowBuf.writeUTF(str);
      return;
    }
    if (ASCII_STRINGS) {
      writeAsciiUTF(str);
    } else {
      writeFullUTF(str);
    }
  }

  private void writeAsciiUTF(String str) throws IOException {
    int len = str.length();
    if (len > 65535) {
      throw new UTFDataFormatException();
    }
    writeShort(len);
    int offset = 0;
    while (len > 0) {
      int remainingSpace = buffer.capacity() - buffer.position();
      if (remainingSpace == 0) {
        realFlush(false);
        if (overflowBuf != null) {
          overflowBuf.write(str.substring(offset).getBytes());
          return;
        }
      } else {
        int chunkSize = remainingSpace;
        if (len < chunkSize) {
          chunkSize = len;
        }
        for (int i = 0; i < chunkSize; i++) {
          buffer.put((byte) str.charAt(offset + i));
        }
        offset += chunkSize;
        len -= chunkSize;
      }
    }
  }

  private void writeFullUTF(String str) throws IOException {
    int strlen = str.length();
    if (strlen > 65535) {
      throw new UTFDataFormatException();
    }
    {
      int remainingSpace = buffer.capacity() - buffer.position();
      if (remainingSpace >= ((strlen * 3) + 2)) {
        // we have plenty of room to do this with one pass directly into the buffer
        writeQuickFullUTF(str, strlen);
        return;
      }
    }
    int utfSize = 0;
    for (int i = 0; i < strlen; i++) {
      int c = str.charAt(i);
      if ((c >= 0x0001) && (c <= 0x007F)) {
        utfSize += 1;
      } else if (c > 0x07FF) {
        utfSize += 3;
      } else {
        utfSize += 2;
      }
    }
    if (utfSize > 65535) {
      throw new UTFDataFormatException();
    }
    writeShort(utfSize);
    for (int i = 0; i < strlen; i++) {
      int c = str.charAt(i);
      if ((c >= 0x0001) && (c <= 0x007F)) {
        writeByte((byte) c);
      } else if (c > 0x07FF) {
        writeByte((byte) (0xE0 | ((c >> 12) & 0x0F)));
        writeByte((byte) (0x80 | ((c >> 6) & 0x3F)));
        writeByte((byte) (0x80 | ((c) & 0x3F)));
      } else {
        writeByte((byte) (0xC0 | ((c >> 6) & 0x1F)));
        writeByte((byte) (0x80 | ((c) & 0x3F)));
      }
    }
  }

  /**
   * Used when we know the max size will fit in the current buffer.
   */
  private void writeQuickFullUTF(String str, int strlen) throws IOException {
    int utfSizeIdx = buffer.position();
    // skip bytes reserved for length
    buffer.position(utfSizeIdx + 2);
    for (int i = 0; i < strlen; i++) {
      int c = str.charAt(i);
      if ((c >= 0x0001) && (c <= 0x007F)) {
        buffer.put((byte) c);
      } else if (c > 0x07FF) {
        buffer.put((byte) (0xE0 | ((c >> 12) & 0x0F)));
        buffer.put((byte) (0x80 | ((c >> 6) & 0x3F)));
        buffer.put((byte) (0x80 | ((c) & 0x3F)));
      } else {
        buffer.put((byte) (0xC0 | ((c >> 6) & 0x1F)));
        buffer.put((byte) (0x80 | ((c) & 0x3F)));
      }
    }
    int utflen = buffer.position() - (utfSizeIdx + 2);
    if (utflen > 65535) {
      // act as if we wrote nothing to this buffer
      buffer.position(utfSizeIdx);
      throw new UTFDataFormatException();
    }
    buffer.putShort(utfSizeIdx, (short) utflen);
  }

  /**
   * Attempt to fit v into the current buffer as a serialized byte array. This is done by reserving
   * 5 bytes for the length and then starting the serialization. If all the bytes fit then the
   * length is fixed up and we are done. If it doesn't fit then we need to serialize the remainder
   * to a temporary HeapDataOutputStream and then fix the length flush the first chunk and then send
   * the contents of the HeapDataOutputStream to this streamer. All of this is done to prevent an
   * extra copy when the serialized form will all fit into our current buffer.
   */
  @Override
  public void writeAsSerializedByteArray(Object v) throws IOException {
    if (v instanceof HeapDataOutputStream) {
      HeapDataOutputStream other = (HeapDataOutputStream) v;
      InternalDataSerializer.writeArrayLength(other.size(), this);
      other.sendTo((ByteBufferWriter) this);
      other.rewind();
      return;
    }
    if (overflowBuf != null) {
      overflowBuf.writeAsSerializedByteArray(v);
      return;
    }
    if (isOverflowMode()) {
      // we must have recursed
      int remainingSpace = buffer.capacity() - buffer.position();
      if (remainingSpace < 5) {
        // we don't even have room to write the length field so just create the overflowBuf
        overflowBuf = new HeapDataOutputStream(
            buffer.capacity() - Connection.MSG_HEADER_BYTES, KnownVersion.CURRENT);
        overflowBuf.writeAsSerializedByteArray(v);
        return;
      }
    } else {
      // need 5 bytes for length plus enough room for an 'average' small object. I pulled 1024 as
      // the average out of thin air.
      ensureCapacity(5 + 1024);
    }
    int lengthPos = buffer.position();
    buffer.position(lengthPos + 5);
    enableOverflowMode();
    boolean finished = false;
    try {
      try {
        DataSerializer.writeObject(v, this);
      } catch (IOException e) {
        throw new IllegalArgumentException("An Exception was thrown while serializing.", e);
      }
      int baLength = buffer.position() - (lengthPos + 5);
      HeapDataOutputStream overBuf = overflowBuf;
      if (overBuf != null) {
        baLength += overBuf.size();
      }
      buffer.put(lengthPos, StaticSerialization.INT_ARRAY_LEN);
      buffer.putInt(lengthPos + 1, baLength);
      disableOverflowMode();
      finished = true;
      if (overBuf != null && !isOverflowMode()) {
        overBuf.sendTo((ByteBufferWriter) this);
      }
    } finally {
      if (!finished) {
        // reset buffer and act as if we did nothing
        buffer.position(lengthPos);
        disableOverflowMode();
      }
    }
  }


}
