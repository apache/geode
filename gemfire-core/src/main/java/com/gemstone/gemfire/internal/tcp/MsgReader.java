/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.tcp;

import com.gemstone.gemfire.distributed.internal.DMStats;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.Version;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * This class is currently used for reading direct ack responses
 * It should probably be used for all of the reading done
 * in Connection.
 * @author dsmith
 *
 */
public abstract class MsgReader {
  protected final Connection conn;
  protected final Header header = new Header();
  private final ByteBufferInputStream bbis;

  public MsgReader(Connection conn, Version version) {
    this.conn = conn;
    this.bbis = version == null ? new ByteBufferInputStream()
        : new VersionedByteBufferInputStream(version);
  }

  public Header readHeader() throws IOException {
    ByteBuffer nioInputBuffer = readAtLeast(Connection.MSG_HEADER_BYTES);
    int nioMessageLength = nioInputBuffer.getInt();
    /* nioMessageVersion = */ Connection.calcHdrVersion(nioMessageLength);
    nioMessageLength = Connection.calcMsgByteSize(nioMessageLength);
    byte nioMessageType = nioInputBuffer.get();
    short nioMsgId = nioInputBuffer.getShort();
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
   * Block until you can read a message. Returns null if the message
   * was a message chunk.
   * @return the message, or null if we only received a chunk of the message
   * @throws ClassNotFoundException 
   * @throws IOException 
   * @throws InterruptedException 
   */
  public DistributionMessage readMessage(Header header) throws IOException, ClassNotFoundException, InterruptedException {
    ByteBuffer nioInputBuffer = readAtLeast(header.nioMessageLength);
    this.getStats().incMessagesBeingReceived(true, header.nioMessageLength);
    long startSer = this.getStats().startMsgDeserialization();
    try {
        bbis.setBuffer(nioInputBuffer);
        DistributionMessage msg = null;
        ReplyProcessor21.initMessageRPId();
        // add serialization stats
        msg = (DistributionMessage)InternalDataSerializer.readDSFID(bbis);
        return msg;
    } finally {
      this.getStats().endMsgDeserialization(startSer);
      this.getStats().decMessagesBeingReceived(header.nioMessageLength);
    }
  }
    
  public void readChunk(Header header, MsgDestreamer md) throws IOException, ClassNotFoundException, InterruptedException {
    ByteBuffer nioInputBuffer = readAtLeast(header.nioMessageLength);
    this.getStats().incMessagesBeingReceived(md.size() == 0, header.nioMessageLength);
    md.addChunk(nioInputBuffer, header.nioMessageLength);
  }

  public abstract ByteBuffer readAtLeast(int bytes) throws IOException;
  
  protected DMStats getStats() {
    return conn.owner.getConduit().stats;
  }

  public static class Header {

    int nioMessageLength;
    byte nioMessageType;
    short nioMsgId;

    public Header() {
    }

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

  public void close() {
  }

}
