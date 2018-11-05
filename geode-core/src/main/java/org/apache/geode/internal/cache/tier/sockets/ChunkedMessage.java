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

package org.apache.geode.internal.cache.tier.sockets;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.logging.log4j.Logger;

import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.logging.LogService;

/**
 * Class <code>ChunkedMessage</code> is used to send messages from a server to a client divided into
 * chunks.
 *
 * This class encapsulates the wire protocol. It provides accessors to encode and decode a message
 * and serialize it out to the wire.
 *
 * <PRE>
 *
 * messageType - int - 4 bytes type of message, types enumerated below
 *
 * numberOfParts - int - 4 bytes number of elements (LEN-BYTE* pairs) contained
 * in the payload. Message can be a multi-part message
 *
 * transId - int - 4 bytes filled in by the requestor, copied back into the
 * response len1 part1 . . . lenn partn
 *
 * </PRE>
 *
 * We read the fixed length 15 bytes into a byte[] and populate a bytebuffer We read the fixed
 * length header tokens from the header parse the header and use information contained in there to
 * read the payload.
 *
 * <P>
 *
 * See also <a href="package-summary.html#messages">package description </a>.
 *
 * @see org.apache.geode.internal.cache.tier.MessageType
 *
 *
 * @since GemFire 4.2
 */
public class ChunkedMessage extends Message {
  private static final Logger logger = LogService.getLogger();

  /**
   * The chunk header length. The chunk header contains a 5-byte int chunk length (4 bytes for the
   * chunk length and 1 byte for the last chunk boolean)
   */
  private static final int CHUNK_HEADER_LENGTH = 5;
  /**
   * The main header length. The main header contains 3 4-byte ints
   */
  private static final int CHUNK_MSG_HEADER_LENGTH = 12;


  /**
   * The chunk's payload length
   */
  protected int chunkLength;

  /**
   * Whether this is the last chunk
   */
  protected byte lastChunk;

  // /**
  // * The main header length. The main header contains 3 4-byte ints
  // */
  // private static final int HEADER_LENGTH = 12;

  /**
   * Initially false; set to true once the message header is sent; set back to false when last chunk
   * is sent.
   */
  private transient boolean headerSent = false;

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();

    sb.append(super.toString());
    sb.append("; chunkLength= " + chunkLength);
    sb.append("; lastChunk=" + lastChunk);
    return sb.toString();
  }

  /**
   * Creates a new message with the given number of parts
   *
   * @param numberOfParts The number of parts to create
   */
  public ChunkedMessage(int numberOfParts, Version version) {
    super(numberOfParts, version);
  }

  /**
   * Returns the header length.
   *
   * @return the header length
   */
  @Override
  public int getHeaderLength() {
    return CHUNK_MSG_HEADER_LENGTH;
  }

  /**
   * Sets whether this is the last chunk.
   *
   * @param lastChunk Whether this is the last chunk
   */
  public void setLastChunk(boolean lastChunk) {
    // TODO:hitesh now it should send security header(connectionID)
    if (lastChunk) {
      this.lastChunk = 0X01;
      setFESpecialCase();
    } else {
      this.lastChunk = 0X00;
    }
  }

  private void setFESpecialCase() {
    byte b = ServerConnection.isExecuteFunctionOnLocalNodeOnly();
    if ((b & 1) == 1) {
      // we are in special function execution case, where filter key is one only
      // now checking whether this function executed locally or not.
      // if not then inform client so that it will refresh pr-meta-data
      if (((b & 2) == 2)) {

        this.lastChunk |= 0x04;// setting third bit, we are okay
      }
    }
  }

  public void setLastChunkAndNumParts(boolean lastChunk, int numParts) {
    setLastChunk(lastChunk);
    if (this.serverConnection != null
        && this.serverConnection.getClientVersion().compareTo(Version.GFE_65) >= 0) {
      // we us e three bits for number of parts in last chunk byte
      // we us e three bits for number of parts in last chunk byte
      byte localLastChunk = (byte) (numParts << 5);
      this.lastChunk |= localLastChunk;
    }
  }

  public void setServerConnection(ServerConnection servConn) {
    if (this.serverConnection != servConn)
      throw new IllegalStateException("this.sc was not correctly set");
  }

  /**
   * Answers whether this is the last chunk.
   *
   * @return whether this is the last chunk
   */
  public boolean isLastChunk() {
    if ((this.lastChunk & 0X01) == 0X01) {
      return true;
    }

    return false;
  }

  /**
   * Returns the chunk length.
   *
   * @return the chunk length
   */
  public int getChunkLength() {
    return this.chunkLength;
  }

  /**
   * Populates the header with information received via socket
   */
  public void readHeader() throws IOException {
    if (this.socket != null) {
      final ByteBuffer cb = getCommBuffer();
      synchronized (cb) {
        fetchHeader();
        final int type = cb.getInt();
        final int numParts = cb.getInt();
        final int txid = cb.getInt();
        cb.clear();
        if (!MessageType.validate(type)) {
          throw new IOException(
              String.format("Invalid message type %s while reading header",
                  Integer.valueOf(type)));
        }

        // Set the header and payload fields only after receiving all the
        // socket data, providing better message consistency in the face
        // of exceptional conditions (e.g. IO problems, timeouts etc.)
        this.messageType = type;
        this.numberOfParts = numParts; // Already set in setPayloadFields via setNumberOfParts
        this.transactionId = txid;
      }
    } else {
      throw new IOException("Dead Connection");
    }
  }

  /**
   * Reads a chunk of this message.
   */
  public void receiveChunk() throws IOException {
    if (this.socket != null) {
      synchronized (getCommBuffer()) {
        readChunk();
      }
    } else {
      throw new IOException("Dead Connection");
    }
  }

  /**
   * Reads a chunk of this message.
   */
  private void readChunk() throws IOException {
    final ByteBuffer cb = getCommBuffer();
    clearParts();
    cb.clear();
    int totalBytesRead = 0;
    do {
      int bytesRead = 0;
      bytesRead =
          inputStream.read(cb.array(), totalBytesRead, CHUNK_HEADER_LENGTH - totalBytesRead);
      if (bytesRead == -1) {
        throw new EOFException(
            "Chunk read error (connection reset)");
      }
      totalBytesRead += bytesRead;
      if (this.messageStats != null) {
        this.messageStats.incReceivedBytes(bytesRead);
      }
    } while (totalBytesRead < CHUNK_HEADER_LENGTH);

    cb.rewind();

    // Set chunk length and last chunk
    this.chunkLength = cb.getInt();
    // setLastChunk(cb.get() == 0x01);
    byte lastChunk = cb.get();
    setLastChunk((lastChunk & 0x01) == 0x01);
    if ((lastChunk & 0x02) == 0x02) {
      this.securePart = new Part();
      if (logger.isDebugEnabled()) {
        logger.debug("ChunkedMessage.readChunk() securePart present");
      }
    }
    cb.clear();
    if ((lastChunk & 0x01) == 0x01) {
      int numParts = lastChunk >> 5;
      if (numParts > 0) {
        this.numberOfParts = numParts;
      }
    }
    readPayloadFields(this.numberOfParts, this.chunkLength);
  }

  /**
   * Sends the header of this message.
   */
  public void sendHeader() throws IOException {
    if (this.socket != null) {
      synchronized (getCommBuffer()) {
        getDSCODEsForWrite();
        flushBuffer();
        // Darrel says: I see no need for the following os.flush() call
        // so I've deadcoded it for performance.
        // this.os.flush();
      }
      this.currentPart = 0;
      this.headerSent = true;
    } else {
      throw new IOException("Dead Connection");
    }
  }

  /**
   * Return true if the header for this message has already been sent.
   */
  public boolean headerHasBeenSent() {
    return this.headerSent;
  }

  /**
   * Sends a chunk of this message.
   */
  public void sendChunk() throws IOException {
    if (isLastChunk()) {
      this.headerSent = false;
    }
    sendBytes(true);
  }

  /**
   * Sends a chunk of this message.
   */
  public void sendChunk(ServerConnection servConn) throws IOException {
    if (this.serverConnection != servConn)
      throw new IllegalStateException("this.sc was not correctly set");
    sendChunk();
  }

  @Override
  protected Part getSecurityPart() {
    if (this.isLastChunk())
      return super.getSecurityPart();
    else
      return null;
  }

  @Override
  protected int checkAndSetSecurityPart() {
    return (this.securePart != null) ? 1 : 0;
  }

  @Override
  protected void packHeaderInfoForSending(int msgLen, boolean isSecurityHeader) {
    final ByteBuffer cb = getCommBuffer();
    cb.putInt(msgLen);
    byte isLastChunk = 0x00;
    if (isLastChunk()) {
      // isLastChunk = (byte) 0x01 ;
      isLastChunk = this.lastChunk;
      if (isSecurityHeader) {
        isLastChunk |= 0x02;
      }
    }
    // cb.put(isLastChunk() ? (byte) 0x01 : (byte) 0x00);
    cb.put(isLastChunk);
  }

  /**
   * Converts the header of this message into a <code>byte</code> array using a {@link ByteBuffer}.
   */
  protected void getDSCODEsForWrite() {
    final ByteBuffer cb = getCommBuffer();
    cb.clear();
    cb.putInt(this.messageType);
    cb.putInt(this.numberOfParts);

    cb.putInt(this.transactionId);
  }
}
