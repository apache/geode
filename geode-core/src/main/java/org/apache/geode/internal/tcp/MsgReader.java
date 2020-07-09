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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.ByteBuffer;

import org.apache.geode.distributed.internal.DMStats;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.net.BufferPool;
import org.apache.geode.internal.net.SocketUtils;
import org.apache.geode.internal.serialization.KnownVersion;

/**
 * This class is currently used for reading direct ack responses It should probably be used for all
 * of the reading done in Connection.
 *
 */
public class MsgReader {
  protected final ClusterConnection conn;
  private final BufferPool bufferPool;
  protected final Header header = new Header();
  private ByteBuffer peerNetData;
  private final InputStream inputStream;
  private final ByteBufferInputStream byteBufferInputStream;
  private int lastProcessedPosition;
  private int lastReadPosition;


  MsgReader(ClusterConnection conn, BufferPool bufferPool, InputStream inputStream,
      KnownVersion version) {
    this.bufferPool = bufferPool;
    this.conn = conn;
    this.inputStream = inputStream;
    this.byteBufferInputStream =
        version == null ? new ByteBufferInputStream() : new VersionedByteBufferInputStream(version);
  }

  Header readHeader() throws IOException {
    ByteBuffer buffer = readAtLeast(ClusterConnection.MSG_HEADER_BYTES);

    Assert.assertTrue(buffer.remaining() >= ClusterConnection.MSG_HEADER_BYTES);

    int messageLength = buffer.getInt();
    /* nioMessageVersion = */
    ClusterConnection.calcHdrVersion(messageLength);
    messageLength = ClusterConnection.calcMsgByteSize(messageLength);
    byte messageType = buffer.get();
    short messageId = buffer.getShort();

    boolean directAck = (messageType & ClusterConnection.DIRECT_ACK_BIT) != 0;
    if (directAck) {
      messageType &= ~ClusterConnection.DIRECT_ACK_BIT; // clear the ack bit
    }

    header.setFields(messageLength, messageType, messageId);

    return header;
  }

  /**
   * Block until you can read a message. Returns null if the message was a message chunk.
   *
   * @return the message, or null if we only received a chunk of the message
   */
  DistributionMessage readMessage(Header header)
      throws IOException, ClassNotFoundException {
    ByteBuffer inputBuffer = readAtLeast(header.messageLength);
    Assert.assertTrue(inputBuffer.remaining() >= header.messageLength);
    this.getStats().incMessagesBeingReceived(true, header.messageLength);
    long startSer = this.getStats().startMsgDeserialization();
    try {
      byteBufferInputStream.setBuffer(inputBuffer);
      ReplyProcessor21.initMessageRPId();
      return (DistributionMessage) InternalDataSerializer.readDSFID(byteBufferInputStream);
    } finally {
      this.getStats().endMsgDeserialization(startSer);
      this.getStats().decMessagesBeingReceived(header.messageLength);
      conn.doneReading(inputBuffer);
    }
  }

  void readChunk(Header header, MsgDestreamer md)
      throws IOException {
    ByteBuffer buffer = readAtLeast(header.messageLength);
    this.getStats().incMessagesBeingReceived(md.size() == 0, header.messageLength);
    md.addChunk(buffer, header.messageLength);
    // show that the bytes have been consumed by adjusting the buffer's position
    buffer.position(buffer.position() + header.messageLength);
  }



  private ByteBuffer readAtLeast(int bytes) throws IOException {
    peerNetData = ensureCapacity(bytes, peerNetData,
        BufferPool.BufferType.TRACKED_RECEIVER);
    return readAtLeast(bytes, peerNetData, conn.getSocket());
  }

  public void close() {
    if (peerNetData != null) {
      conn.getBufferPool().releaseReceiveBuffer(peerNetData);
    }
  }

  private ByteBuffer readAtLeast(int bytes, ByteBuffer buffer, Socket socket)
      throws IOException {

    Assert.assertTrue(buffer.capacity() - lastProcessedPosition >= bytes);

    // read into the buffer starting at the end of valid data
    buffer.limit(buffer.capacity());
    buffer.position(lastReadPosition);

    while (buffer.position() < (lastProcessedPosition + bytes)) {
      int amountRead = SocketUtils.readFromSocket(socket, buffer, inputStream);
      if (amountRead < 0) {
        throw new EOFException();
      }
    }

    // keep track of how much of the buffer contains valid data with lastReadPosition
    lastReadPosition = buffer.position();

    // set up the buffer for reading and keep track of how much has been consumed with
    // lastProcessedPosition
    buffer.limit(lastProcessedPosition + bytes);
    buffer.position(lastProcessedPosition);
    lastProcessedPosition += bytes;

    return buffer;
  }

  private ByteBuffer ensureCapacity(int amount, ByteBuffer buffer,
      BufferPool.BufferType bufferType) {
    if (buffer == null) {
      buffer = conn.getConduit().useDirectReceiveBuffers()
          ? bufferPool.acquireDirectBuffer(bufferType, amount)
          : bufferPool.acquireNonDirectBuffer(bufferType, amount);
      buffer.clear();
      lastProcessedPosition = 0;
      lastReadPosition = 0;
    } else if (buffer.capacity() > amount) {
      // we already have a buffer that's big enough
      if (buffer.capacity() - lastProcessedPosition < amount) {
        buffer.limit(lastReadPosition);
        buffer.position(lastProcessedPosition);
        buffer.compact();
        lastReadPosition = buffer.position();
        lastProcessedPosition = 0;
      }
    } else {
      ByteBuffer oldBuffer = buffer;
      oldBuffer.limit(lastReadPosition);
      oldBuffer.position(lastProcessedPosition);
      buffer = conn.getConduit().useDirectReceiveBuffers()
          ? bufferPool.acquireDirectBuffer(bufferType, amount)
          : bufferPool.acquireNonDirectBuffer(bufferType, amount);
      buffer.clear();
      buffer.put(oldBuffer);
      bufferPool.releaseBuffer(bufferType, oldBuffer);
      lastReadPosition = buffer.position();
      lastProcessedPosition = 0;
    }
    return buffer;
  }



  private DMStats getStats() {
    return conn.getConduit().getStats();
  }

  public static class Header {

    private int messageLength;
    private byte messageType;
    private short messageId;

    public void setFields(int nioMessageLength, byte nioMessageType, short nioMsgId) {
      messageLength = nioMessageLength;
      messageType = nioMessageType;
      messageId = nioMsgId;
    }

    int getMessageLength() {
      return messageLength;
    }

    byte getMessageType() {
      return messageType;
    }

    short getMessageId() {
      return messageId;
    }
  }

}
