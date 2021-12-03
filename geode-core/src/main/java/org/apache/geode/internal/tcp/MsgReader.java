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
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

import org.apache.logging.log4j.Logger;

import org.apache.geode.distributed.internal.DMStats;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.net.BufferPoolImpl;
import org.apache.geode.internal.net.ByteBufferSharing;
import org.apache.geode.internal.net.NioFilter;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * This class is currently used for reading direct ack responses It should probably be used for all
 * of the reading done in Connection.
 *
 */
public class MsgReader {
  private static final Logger logger = LogService.getLogger();

  protected final Connection conn;
  protected final Header header = new Header();
  private final NioFilter ioFilter;
  private ByteBuffer peerNetData;
  private final ByteBufferInputStream byteBufferInputStream;



  MsgReader(Connection conn, NioFilter nioFilter, KnownVersion version) {
    this.conn = conn;
    this.ioFilter = nioFilter;
    this.byteBufferInputStream =
        version == null ? new ByteBufferInputStream() : new VersionedByteBufferInputStream(version);
  }

  Header readHeader() throws IOException {
    try (final ByteBufferSharing sharedBuffer = readAtLeast(Connection.MSG_HEADER_BYTES)) {
      ByteBuffer unwrappedBuffer = sharedBuffer.getBuffer();

      Assert.assertTrue(unwrappedBuffer.remaining() >= Connection.MSG_HEADER_BYTES);

      try {
        int nioMessageLength = unwrappedBuffer.getInt();
        /* nioMessageVersion = */
        Connection.calcHdrVersion(nioMessageLength);
        nioMessageLength = Connection.calcMsgByteSize(nioMessageLength);
        byte nioMessageType = unwrappedBuffer.get();
        short nioMsgId = unwrappedBuffer.getShort();

        boolean directAck = (nioMessageType & Connection.DIRECT_ACK_BIT) != 0;
        if (directAck) {
          nioMessageType &= ~Connection.DIRECT_ACK_BIT; // clear the ack bit
        }

        header.setFields(nioMessageLength, nioMessageType, nioMsgId);

        return header;
      } catch (BufferUnderflowException e) {
        throw e;
      }
    }

  }

  /**
   * Block until you can read a message. Returns null if the message was a message chunk.
   *
   * @return the message, or null if we only received a chunk of the message
   */
  DistributionMessage readMessage(Header header)
      throws IOException, ClassNotFoundException {
    try (final ByteBufferSharing sharedBuffer = readAtLeast(header.messageLength)) {
      ByteBuffer nioInputBuffer = sharedBuffer.getBuffer();
      Assert.assertTrue(nioInputBuffer.remaining() >= header.messageLength);
      this.getStats().incMessagesBeingReceived(true, header.messageLength);
      long startSer = this.getStats().startMsgDeserialization();
      try {
        byteBufferInputStream.setBuffer(nioInputBuffer);
        ReplyProcessor21.initMessageRPId();
        return (DistributionMessage) InternalDataSerializer.readDSFID(byteBufferInputStream);
      } catch (RuntimeException e) {
        throw e;
      } catch (IOException e) {
        throw e;
      } finally {
        this.getStats().endMsgDeserialization(startSer);
        this.getStats().decMessagesBeingReceived(header.messageLength);
        ioFilter.doneReadingDirectAck(nioInputBuffer);
      }
    }
  }

  void readChunk(Header header, MsgDestreamer md)
      throws IOException {
    try (final ByteBufferSharing sharedBuffer = readAtLeast(header.messageLength)) {
      ByteBuffer unwrappedBuffer = sharedBuffer.getBuffer();
      this.getStats().incMessagesBeingReceived(md.size() == 0, header.messageLength);
      md.addChunk(unwrappedBuffer, header.messageLength);
      // show that the bytes have been consumed by adjusting the buffer's position
      unwrappedBuffer.position(unwrappedBuffer.position() + header.messageLength);
    }
  }



  private ByteBufferSharing readAtLeast(int bytes) throws IOException {
    peerNetData = ioFilter.ensureWrappedCapacity(bytes, peerNetData,
        BufferPoolImpl.BufferType.TRACKED_RECEIVER);
    return ioFilter.readAtLeast(conn.getSocket().getChannel(), bytes, peerNetData);
  }

  public void close() {
    if (peerNetData != null) {
      conn.getBufferPool().releaseReceiveBuffer(peerNetData);
    }
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
