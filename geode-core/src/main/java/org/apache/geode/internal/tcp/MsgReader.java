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
import java.nio.ByteBuffer;

import org.apache.logging.log4j.Logger;

import org.apache.geode.distributed.internal.DMStats;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.net.Buffers;
import org.apache.geode.internal.net.NioFilter;

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
  private final ByteBuffer peerNetData;
  private final ByteBufferInputStream bbis;

  private int lastReadPosition;
  private int lastProcessedPosition;

  public MsgReader(Connection conn, NioFilter nioFilter, ByteBuffer peerNetData, Version version) {
    this.conn = conn;
    this.ioFilter = nioFilter;
    this.peerNetData = peerNetData;
    ByteBuffer buffer = ioFilter.getUnwrappedBuffer(peerNetData);
    buffer.position(0).limit(0);
    this.bbis =
        version == null ? new ByteBufferInputStream() : new VersionedByteBufferInputStream(version);
  }

  public Header readHeader() throws IOException {
    ByteBuffer nioInputBuffer = readAtLeast(Connection.MSG_HEADER_BYTES);

    int nioMessageLength = nioInputBuffer.getInt();
    /* nioMessageVersion = */ Connection.calcHdrVersion(nioMessageLength);
    nioMessageLength = Connection.calcMsgByteSize(nioMessageLength);
    byte nioMessageType = nioInputBuffer.get();
    short nioMsgId = nioInputBuffer.getShort();

    nioInputBuffer.position(nioInputBuffer.limit());

    boolean directAck = (nioMessageType & Connection.DIRECT_ACK_BIT) != 0;
    if (directAck) {
      // logger.info("DEBUG: msg from " + getRemoteAddress() + " is direct ack" );
      nioMessageType &= ~Connection.DIRECT_ACK_BIT; // clear the ack bit
    }

    header.nioMessageLength = nioMessageLength;
    header.nioMessageType = nioMessageType;
    header.nioMsgId = nioMsgId;
    return header;
  }

  /**
   * Block until you can read a message. Returns null if the message was a message chunk.
   *
   * @return the message, or null if we only received a chunk of the message
   */
  public DistributionMessage readMessage(Header header)
      throws IOException, ClassNotFoundException, InterruptedException {
    ByteBuffer nioInputBuffer = readAtLeast(header.nioMessageLength);
    this.getStats().incMessagesBeingReceived(true, header.nioMessageLength);
    long startSer = this.getStats().startMsgDeserialization();
    try {
      bbis.setBuffer(nioInputBuffer);
      DistributionMessage msg = null;
      ReplyProcessor21.initMessageRPId();
      // add serialization stats
      msg = (DistributionMessage) InternalDataSerializer.readDSFID(bbis);
      return msg;
    } finally {
      this.getStats().endMsgDeserialization(startSer);
      this.getStats().decMessagesBeingReceived(header.nioMessageLength);
      ioFilter.doneReading(nioInputBuffer);
    }
  }

  public void readChunk(Header header, MsgDestreamer md)
      throws IOException, ClassNotFoundException, InterruptedException {
    ByteBuffer nioInputBuffer = readAtLeast(header.nioMessageLength);
    this.getStats().incMessagesBeingReceived(md.size() == 0, header.nioMessageLength);
    md.addChunk(nioInputBuffer, header.nioMessageLength);
  }

  public ByteBuffer readAtLeast(int bytes) throws IOException {
    ioFilter.ensureUnwrappedCapacity(bytes, peerNetData, Buffers.BufferType.UNTRACKED,
        getStats());

    ByteBuffer unwrappedBuffer = ioFilter.getUnwrappedBuffer(peerNetData);

    while ((lastReadPosition - lastProcessedPosition) < bytes) {
      unwrappedBuffer.limit(unwrappedBuffer.capacity());
      unwrappedBuffer.position(lastReadPosition);

      int amountRead = conn.getSocket().getChannel().read(peerNetData);
      if (amountRead < 0) {
        throw new EOFException();
      }
      if (amountRead > 0) {
        peerNetData.flip();
        unwrappedBuffer = ioFilter.unwrap(peerNetData);
        lastReadPosition = unwrappedBuffer.position();
      }
    }
    unwrappedBuffer.limit(lastProcessedPosition + bytes);
    unwrappedBuffer.position(lastProcessedPosition);
    lastProcessedPosition = unwrappedBuffer.limit();

    return unwrappedBuffer;
  }

  protected DMStats getStats() {
    return conn.getConduit().getStats();
  }

  public static class Header {

    int nioMessageLength;
    byte nioMessageType;
    short nioMsgId;

    public Header() {}

    public int getNioMessageLength() {
      return nioMessageLength;
    }

    public byte getNioMessageType() {
      return nioMessageType;
    }

    public short getNioMessageId() {
      return nioMsgId;
    }


  }

  public void close() {}


}
