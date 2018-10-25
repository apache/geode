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
import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.internal.DMStats;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.ByteBufferWriter;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.ObjToByteArraySerializer;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.logging.LogService;

/**
 * <p>
 * MsgStreamer supports streaming a message to a tcp Connection in chunks. This allows us to send a
 * message without needing to perserialize it completely in memory thus saving buffer memory.
 *
 * @since GemFire 5.0.2
 *
 */

public class MsgStreamer extends OutputStream
    implements ObjToByteArraySerializer, BaseMsgStreamer, ByteBufferWriter {

  private static final Logger logger = LogService.getLogger();

  /**
   * List of connections to send this msg to.
   */
  private final List<?> cons;

  /**
   * Any exceptions that happen during sends
   */
  private ConnectExceptions ce;
  /**
   * The byte buffer we used for preparing a chunk of the message. Currently this buffer is obtained
   * from the connection.
   */
  private final ByteBuffer buffer;
  private int flushedBytes = 0;
  // the message this streamer is to send
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
    MsgIdGenerator.release(this.msgId);
    this.buffer.clear();
    this.overflowBuf = null;
    Buffers.releaseSenderBuffer(this.buffer, this.stats);
  }

  /**
   * Returns an exception the describes which cons the message was not sent to. Call this after
   * {@link #writeMessage}.
   */
  public ConnectExceptions getConnectExceptions() {
    return this.ce;
  }

  /**
   * Returns a list of the Connections that the message was sent to. Call this after
   * {@link #writeMessage}.
   */
  public List<?> getSentConnections() {
    return this.cons;
  }

  /**
   * Create a msg streamer that will send the given msg to the given cons.
   *
   * Note: This is no longer supposed to be called directly rather the {@link #create} method should
   * now be used.
   */
  MsgStreamer(List<?> cons, DistributionMessage msg, boolean directReply, DMStats stats,
      int sendBufferSize) {
    this.stats = stats;
    this.msg = msg;
    this.cons = cons;
    this.buffer = Buffers.acquireSenderBuffer(sendBufferSize, stats);
    this.buffer.clear();
    this.buffer.position(Connection.MSG_HEADER_BYTES);
    this.msgId = MsgIdGenerator.NO_MSG_ID;
    this.directReply = directReply;
    startSerialization();
  }

  /**
   * Create message streamers splitting into versioned streamers, if required, for given list of
   * connections to remote nodes. This method can either return a single MsgStreamer object or a
   * List of MsgStreamer objects.
   */
  public static BaseMsgStreamer create(List<?> cons, final DistributionMessage msg,
      final boolean directReply, final DMStats stats) {
    final Connection firstCon = (Connection) cons.get(0);
    // split into different versions if required
    Version version;
    final int numCons = cons.size();
    if (numCons > 1) {
      Connection con;
      Object2ObjectOpenHashMap versionToConnMap = null;
      int numVersioned = 0;
      for (Object c : cons) {
        con = (Connection) c;
        if ((version = con.getRemoteVersion()) != null) {
          if (versionToConnMap == null) {
            versionToConnMap = new Object2ObjectOpenHashMap();
          }
          @SuppressWarnings("unchecked")
          ArrayList<Object> vcons = (ArrayList<Object>) versionToConnMap.get(version);
          if (vcons == null) {
            vcons = new ArrayList<Object>(numCons);
            versionToConnMap.put(version, vcons);
          }
          vcons.add(con);
          numVersioned++;
        }
      }
      if (versionToConnMap == null) {
        return new MsgStreamer(cons, msg, directReply, stats, firstCon.getSendBufferSize());
      } else {
        // if there is a versioned stream created, then split remaining
        // connections to unversioned stream
        final ArrayList<MsgStreamer> streamers =
            new ArrayList<MsgStreamer>(versionToConnMap.size() + 1);
        final int sendBufferSize = firstCon.getSendBufferSize();
        if (numCons > numVersioned) {
          // allocating list of numCons size so that as the result of
          // getSentConnections it may not need to be reallocted later
          final ArrayList<Object> unversionedCons = new ArrayList<Object>(numCons);
          for (Object c : cons) {
            con = (Connection) c;
            if ((version = con.getRemoteVersion()) == null) {
              unversionedCons.add(con);
            }
          }
          streamers.add(new MsgStreamer(unversionedCons, msg, directReply, stats, sendBufferSize));
        }
        for (ObjectIterator<Object2ObjectMap.Entry> itr =
            versionToConnMap.object2ObjectEntrySet().fastIterator(); itr.hasNext();) {
          Object2ObjectMap.Entry entry = itr.next();
          Object ver = entry.getKey();
          Object l = entry.getValue();
          streamers.add(new VersionedMsgStreamer((List<?>) l, msg, directReply, stats,
              sendBufferSize, (Version) ver));
        }
        return new MsgStreamerList(streamers);
      }
    } else if ((version = firstCon.getRemoteVersion()) == null) {
      return new MsgStreamer(cons, msg, directReply, stats, firstCon.getSendBufferSize());
    } else {
      // create a single VersionedMsgStreamer
      return new VersionedMsgStreamer(cons, msg, directReply, stats, firstCon.getSendBufferSize(),
          version);
    }
  }

  /**
   * set connections to be "in use" and schedule alert tasks
   *
   */
  public void reserveConnections(long startTime, long ackTimeout, long ackSDTimeout) {
    for (Iterator it = cons.iterator(); it.hasNext();) {
      Connection con = (Connection) it.next();
      con.setInUse(true, startTime, ackTimeout, ackSDTimeout, cons);
      if (ackTimeout > 0) {
        con.scheduleAckTimeouts();
      }
    }
  }

  private void startSerialization() {
    this.serStartTime = stats.startMsgSerialization();
  }

  /**
   * @throws IOException if serialization failure
   */
  public int writeMessage() throws IOException {
    // if (logger.isTraceEnabled()) logger.trace(this.msg);

    try {
      this.startedSerializingMsg = true;
      InternalDataSerializer.writeDSFID(this.msg, this);
      this.doneWritingMsg = true;
      if (this.flushedBytes == 0) {
        // message fit in one chunk
        this.normalMsg = true;
      }
      realFlush(true);
      return this.flushedBytes;
    } finally {
      release();
    }
  }

  /** write the low-order 8 bits of the given int */
  @Override
  public void write(int b) {
    // if (logger.isTraceEnabled()) logger.trace(" byte={}", b);

    ensureCapacity(1);
    if (this.overflowBuf != null) {
      this.overflowBuf.write(b);
      return;
    }
    this.buffer.put((byte) b);
  }

  private void ensureCapacity(int amount) {
    if (this.overflowBuf != null) {
      return;
    }
    int remainingSpace = this.buffer.capacity() - this.buffer.position();
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
    return this.overflowMode > 0;
  }

  private void enableOverflowMode() {
    this.overflowMode++;
  }

  private void disableOverflowMode() {
    this.overflowMode--;
    if (!isOverflowMode()) {
      this.overflowBuf = null;
    }
  }

  public void realFlush(boolean lastFlushForMessage) {
    if (isOverflowMode()) {
      if (this.overflowBuf == null) {
        this.overflowBuf = new HeapDataOutputStream(
            this.buffer.capacity() - Connection.MSG_HEADER_BYTES, Version.CURRENT);
      }
      return;
    }
    this.buffer.flip();
    setMessageHeader();
    final int serializedBytes = this.buffer.limit();
    this.flushedBytes += serializedBytes;
    DistributionMessage conflationMsg = null;
    if (this.normalMsg) {
      // we can't conflate chunked messages; this fixes bug 36633
      conflationMsg = this.msg;
    }
    this.stats.endMsgSerialization(this.serStartTime);
    for (Iterator it = this.cons.iterator(); it.hasNext();) {
      Connection con = (Connection) it.next();
      try {
        con.sendPreserialized(this.buffer,
            lastFlushForMessage && this.msg.containsRegionContentChange(), conflationMsg);
      } catch (IOException ex) {
        it.remove();
        if (this.ce == null)
          this.ce = new ConnectExceptions();
        this.ce.addFailure(con.getRemoteAddress(), ex);
        con.closeForReconnect(
            String.format("closing due to %s", "IOException"));
      } catch (ConnectionException ex) {
        it.remove();
        if (this.ce == null)
          this.ce = new ConnectExceptions();
        this.ce.addFailure(con.getRemoteAddress(), ex);
        con.closeForReconnect(
            String.format("closing due to %s", "ConnectionException"));
      }
      this.buffer.rewind();
    }
    startSerialization();
    this.buffer.clear();
    this.buffer.position(Connection.MSG_HEADER_BYTES);
  }

  @Override
  public void close() throws IOException {
    try {
      if (this.startedSerializingMsg && !this.doneWritingMsg) {
        // if we wrote any bytes on the cnxs then we need to close them
        // since they have been corrupted by a partial serialization.
        if (this.flushedBytes > 0) {
          for (Iterator it = this.cons.iterator(); it.hasNext();) {
            Connection con = (Connection) it.next();
            con.closeForReconnect("Message serialization could not complete");
          }
        }
      }
    } finally {
      super.close();
    }
  }

  /** override OutputStream's write() */
  @Override
  public void write(byte[] source, int offset, int len) {
    // if (logger.isTraceEnabled()) {
    // logger.trace(" bytes={} offset={} len={}", source, offset, len);
    // }
    if (this.overflowBuf != null) {
      this.overflowBuf.write(source, offset, len);
      return;
    }
    while (len > 0) {
      int remainingSpace = this.buffer.capacity() - this.buffer.position();
      if (remainingSpace == 0) {
        realFlush(false);
        if (this.overflowBuf != null) {
          this.overflowBuf.write(source, offset, len);
          return;
        }
      } else {
        int chunkSize = remainingSpace;
        if (len < chunkSize) {
          chunkSize = len;
        }
        this.buffer.put(source, offset, chunkSize);
        offset += chunkSize;
        len -= chunkSize;
      }
    }
  }

  @Override
  public void write(ByteBuffer bb) {
    // if (logger.isTraceEnabled()) {
    // logger.trace(" bytes={} offset={} len={}", source, offset, len);
    // }
    if (this.overflowBuf != null) {
      this.overflowBuf.write(bb);
      return;
    }
    int len = bb.remaining();
    while (len > 0) {
      int remainingSpace = this.buffer.capacity() - this.buffer.position();
      if (remainingSpace == 0) {
        realFlush(false);
        if (this.overflowBuf != null) {
          this.overflowBuf.write(bb);
          return;
        }
      } else {
        int chunkSize = remainingSpace;
        if (len < chunkSize) {
          chunkSize = len;
        }
        int oldLimit = bb.limit();
        bb.limit(bb.position() + chunkSize);
        this.buffer.put(bb);
        bb.limit(oldLimit);
        len -= chunkSize;
      }
    }
  }

  /**
   * write the header after the message has been written to the stream
   */
  private void setMessageHeader() {
    Assert.assertTrue(this.overflowBuf == null);
    Assert.assertTrue(!isOverflowMode());
    // int processorType = this.msg.getProcessorType();
    int msgType;
    if (this.doneWritingMsg) {
      if (this.normalMsg) {
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
    if (!this.normalMsg) {
      if (this.msgId == MsgIdGenerator.NO_MSG_ID) {
        this.msgId = MsgIdGenerator.obtain();
      }
    }

    this.buffer.putInt(Connection.MSG_HEADER_SIZE_OFFSET,
        Connection.calcHdrSize(this.buffer.limit() - Connection.MSG_HEADER_BYTES));
    this.buffer.put(Connection.MSG_HEADER_TYPE_OFFSET, (byte) (msgType & 0xff));
    this.buffer.putShort(Connection.MSG_HEADER_ID_OFFSET, this.msgId);
    this.buffer.position(0);
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
    // if (logger.isTraceEnabled()) logger.trace(" short={}", v);

    ensureCapacity(2);
    if (this.overflowBuf != null) {
      this.overflowBuf.writeShort(v);
      return;
    }
    this.buffer.putShort((short) v);
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
    // if (logger.isTraceEnabled()) logger.trace(" char={}", v);

    ensureCapacity(2);
    if (this.overflowBuf != null) {
      this.overflowBuf.writeChar(v);
      return;
    }
    this.buffer.putChar((char) v);
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
    // if (logger.isTraceEnabled()) logger.trace(" int={}", v);

    ensureCapacity(4);
    if (this.overflowBuf != null) {
      this.overflowBuf.writeInt(v);
      return;
    }
    this.buffer.putInt(v);
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
    // if (logger.isTraceEnabled()) logger.trace(" long={}", v);

    ensureCapacity(8);
    if (this.overflowBuf != null) {
      this.overflowBuf.writeLong(v);
      return;
    }
    this.buffer.putLong(v);
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
    // if (logger.isTraceEnabled()) logger.trace(" float={}", v);

    ensureCapacity(4);
    if (this.overflowBuf != null) {
      this.overflowBuf.writeFloat(v);
      return;
    }
    this.buffer.putFloat(v);
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
    // if (logger.isTraceEnabled()) logger.trace(" double={}", v);

    ensureCapacity(8);
    if (this.overflowBuf != null) {
      this.overflowBuf.writeDouble(v);
      return;
    }
    this.buffer.putDouble(v);
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
    // if (logger.isTraceEnabled()) logger.trace(" bytes={}", str);

    if (this.overflowBuf != null) {
      this.overflowBuf.writeBytes(str);
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
  public void writeChars(String s) {
    // if (logger.isTraceEnabled()) logger.trace(" chars={}", s);

    if (this.overflowBuf != null) {
      this.overflowBuf.writeChars(s);
      return;
    }
    int len = s.length();
    int offset = 0;
    while (len > 0) {
      int remainingCharSpace = (this.buffer.capacity() - this.buffer.position()) / 2;
      if (remainingCharSpace == 0) {
        realFlush(false);
        if (this.overflowBuf != null) {
          this.overflowBuf.writeChars(s.substring(offset));
          return;
        }
      } else {
        int chunkSize = remainingCharSpace;
        if (len < chunkSize) {
          chunkSize = len;
        }
        for (int i = 0; i < chunkSize; i++) {
          this.buffer.putChar(s.charAt(offset + i));
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
   * @exception IOException if an I/O error occurs.
   */
  public void writeUTF(String str) throws IOException {
    // if (logger.isTraceEnabled()) logger.trace(" utf={}", str);

    if (this.overflowBuf != null) {
      this.overflowBuf.writeUTF(str);
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
      int remainingSpace = this.buffer.capacity() - this.buffer.position();
      if (remainingSpace == 0) {
        realFlush(false);
        if (this.overflowBuf != null) {
          this.overflowBuf.write(str.substring(offset).getBytes());
          return;
        }
      } else {
        int chunkSize = remainingSpace;
        if (len < chunkSize) {
          chunkSize = len;
        }
        for (int i = 0; i < chunkSize; i++) {
          this.buffer.put((byte) str.charAt(offset + i));
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
      int remainingSpace = this.buffer.capacity() - this.buffer.position();
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
        writeByte((byte) (0x80 | ((c >> 0) & 0x3F)));
      } else {
        writeByte((byte) (0xC0 | ((c >> 6) & 0x1F)));
        writeByte((byte) (0x80 | ((c >> 0) & 0x3F)));
      }
    }
  }

  /**
   * Used when we know the max size will fit in the current buffer.
   */
  private void writeQuickFullUTF(String str, int strlen) throws IOException {
    int utfSizeIdx = this.buffer.position();
    // skip bytes reserved for length
    this.buffer.position(utfSizeIdx + 2);
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
    int utflen = this.buffer.position() - (utfSizeIdx + 2);
    if (utflen > 65535) {
      // act as if we wrote nothing to this buffer
      this.buffer.position(utfSizeIdx);
      throw new UTFDataFormatException();
    }
    this.buffer.putShort(utfSizeIdx, (short) utflen);
  }

  /**
   * Attempt to fit v into the current buffer as a serialized byte array. This is done by reserving
   * 5 bytes for the length and then starting the serialization. If all the bytes fit then the
   * length is fixed up and we are done. If it doesn't fit then we need to serialize the remainder
   * to a temporary HeapDataOutputStream and then fix the length flush the first chunk and then send
   * the contents of the HeapDataOutputStream to this streamer. All of this is done to prevent an
   * extra copy when the serialized form will all fit into our current buffer.
   */
  public void writeAsSerializedByteArray(Object v) throws IOException {
    if (v instanceof HeapDataOutputStream) {
      HeapDataOutputStream other = (HeapDataOutputStream) v;
      InternalDataSerializer.writeArrayLength(other.size(), this);
      other.sendTo((ByteBufferWriter) this);
      other.rewind();
      return;
    }
    if (this.overflowBuf != null) {
      this.overflowBuf.writeAsSerializedByteArray(v);
      return;
    }
    if (isOverflowMode()) {
      // we must have recursed which is now allowed to fix bug 38194
      int remainingSpace = this.buffer.capacity() - this.buffer.position();
      if (remainingSpace < 5) {
        // we don't even have room to write the length field so just create
        // the overflowBuf
        this.overflowBuf = new HeapDataOutputStream(
            this.buffer.capacity() - Connection.MSG_HEADER_BYTES, Version.CURRENT);
        this.overflowBuf.writeAsSerializedByteArray(v);
        return;
      }
    } else {
      ensureCapacity(5 + 1024); /*
                                 * need 5 bytes for length plus enough room for an 'average' small
                                 * object. I pulled 1024 as the average out of thin air.
                                 */
    }
    int lengthPos = this.buffer.position();
    this.buffer.position(lengthPos + 5);
    enableOverflowMode();
    boolean finished = false;
    try {
      try {
        DataSerializer.writeObject(v, this);
      } catch (IOException e) {
        RuntimeException e2 = new IllegalArgumentException(
            "An Exception was thrown while serializing.");
        e2.initCause(e);
        throw e2;
      }
      int baLength = this.buffer.position() - (lengthPos + 5);
      HeapDataOutputStream overBuf = this.overflowBuf;
      if (overBuf != null) {
        baLength += overBuf.size();
      }
      this.buffer.put(lengthPos, InternalDataSerializer.INT_ARRAY_LEN);
      this.buffer.putInt(lengthPos + 1, baLength);
      disableOverflowMode();
      finished = true;
      if (overBuf != null && !isOverflowMode()) {
        overBuf.sendTo((ByteBufferWriter) this);
      }
    } finally {
      if (!finished) {
        // reset buffer and act as if we did nothing
        this.buffer.position(lengthPos);
        disableOverflowMode();
      }
    }
  }


}
