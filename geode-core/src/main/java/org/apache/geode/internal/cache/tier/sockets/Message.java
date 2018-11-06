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
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Logger;

import org.apache.geode.SerializationException;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.offheap.StoredObject;
import org.apache.geode.internal.offheap.annotations.Unretained;
import org.apache.geode.internal.util.BlobHelper;

/**
 * This class encapsulates the wire protocol. It provides accessors to encode and decode a message
 * and serialize it out to the wire.
 *
 * <PRE>
 * messageType       - int   - 4 bytes type of message, types enumerated below
 *
 * msgLength     - int - 4 bytes   total length of variable length payload
 *
 * numberOfParts - int - 4 bytes   number of elements (LEN-BYTE* pairs)
 *                     contained in the payload. Message can
 *                       be a multi-part message
 *
 * transId       - int - 4 bytes  filled in by the requester, copied back into
 *                    the response
 *
 * flags         - byte- 1 byte   filled in by the requester
 * len1
 * part1
 * .
 * .
 * .
 * lenn
 * partn
 * </PRE>
 *
 * We read the fixed length 16 bytes into a byte[] and populate a bytebuffer We read the fixed
 * length header tokens from the header parse the header and use information contained in there to
 * read the payload.
 *
 * <P>
 *
 * See also <a href="package-summary.html#messages">package description</a>.
 *
 * @see MessageType
 */
public class Message {

  // Tentative workaround to avoid OOM stated in #46754.
  public static final ThreadLocal<Integer> MESSAGE_TYPE = new ThreadLocal<>();

  public static final String MAX_MESSAGE_SIZE_PROPERTY =
      DistributionConfig.GEMFIRE_PREFIX + "client.max-message-size";

  static final int DEFAULT_MAX_MESSAGE_SIZE = 1073741824;

  private static final Logger logger = LogService.getLogger();

  private static final int PART_HEADER_SIZE = 5; // 4 bytes for length, 1 byte for isObject

  private static final int FIXED_LENGTH = 17;

  private static final ThreadLocal<ByteBuffer> tlCommBuffer = new ThreadLocal<>();

  // These two statics are fields shoved into the flags byte for transmission.
  // The MESSAGE_IS_RETRY bit is stripped out during deserialization but the other
  // is left in place
  private static final byte MESSAGE_HAS_SECURE_PART = (byte) 0x02;
  private static final byte MESSAGE_IS_RETRY = (byte) 0x04;

  private static final byte MESSAGE_IS_RETRY_MASK = (byte) 0xFB;

  private static final int DEFAULT_CHUNK_SIZE = 1024;

  private static final byte[] TRUE = defineTrue();
  private static final byte[] FALSE = defineFalse();

  private static final int NO_HEADER_READ_TIMEOUT = 0;

  private static byte[] defineTrue() {
    try (HeapDataOutputStream hdos = new HeapDataOutputStream(10, null)) {
      BlobHelper.serializeTo(Boolean.TRUE, hdos);
      return hdos.toByteArray();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  private static byte[] defineFalse() {
    try (HeapDataOutputStream hdos = new HeapDataOutputStream(10, null)) {
      BlobHelper.serializeTo(Boolean.FALSE, hdos);
      return hdos.toByteArray();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * maximum size of an outgoing message. See GEODE-478
   */
  private final int maxMessageSize;

  protected int messageType;
  private int payloadLength = 0;
  int numberOfParts = 0;
  protected int transactionId = TXManagerImpl.NOTX;
  int currentPart = 0;
  private Part[] partsList = null;
  private ByteBuffer cachedCommBuffer;
  protected Socket socket = null;
  private SocketChannel socketChannel = null;
  private OutputStream outputStream = null;
  protected InputStream inputStream = null;
  private boolean messageModified = true;

  /** is this message a retry of a previously sent message? */
  private boolean isRetry;

  private byte flags = 0x00;
  MessageStats messageStats = null;
  protected ServerConnection serverConnection = null;
  private int maxIncomingMessageLength = -1;
  private Semaphore dataLimiter = null;
  private Semaphore messageLimiter = null;
  private boolean readHeader = false;
  private int chunkSize = DEFAULT_CHUNK_SIZE;

  Part securePart = null;
  private boolean isMetaRegion = false;

  private Version version;

  /**
   * Creates a new message with the given number of parts
   */
  public Message(int numberOfParts, Version destVersion) {
    this.maxMessageSize = Integer.getInteger(MAX_MESSAGE_SIZE_PROPERTY, DEFAULT_MAX_MESSAGE_SIZE);
    this.version = destVersion;
    Assert.assertTrue(destVersion != null, "Attempt to create an unversioned message");
    this.partsList = new Part[numberOfParts];
    this.numberOfParts = numberOfParts;
    int partsListLength = this.partsList.length;
    for (int i = 0; i < partsListLength; i++) {
      this.partsList[i] = new Part();
    }
  }

  public boolean isSecureMode() {
    return this.securePart != null;
  }

  public byte[] getSecureBytes() throws IOException, ClassNotFoundException {
    return (byte[]) this.securePart.getObject();
  }

  public void setMessageType(int msgType) {
    this.messageModified = true;
    if (!MessageType.validate(msgType)) {
      throw new IllegalArgumentException(
          "Invalid MessageType");
    }
    this.messageType = msgType;
  }

  public void setVersion(Version clientVersion) {
    this.version = clientVersion;
  }

  public void setMessageHasSecurePartFlag() {
    this.flags |= MESSAGE_HAS_SECURE_PART;
  }

  public void clearMessageHasSecurePartFlag() {
    this.flags &= MESSAGE_HAS_SECURE_PART;
  }

  /**
   * Sets and builds the {@link Part}s that are sent in the payload of the Message
   */
  public void setNumberOfParts(int numberOfParts) {
    // hitesh: need to add security header here from server
    // need to insure it is not chunked message
    // should we look message type to avoid internal message like ping
    this.messageModified = true;
    this.currentPart = 0;
    this.numberOfParts = numberOfParts;
    if (numberOfParts > this.partsList.length) {
      Part[] newPartsList = new Part[numberOfParts];
      for (int i = 0; i < numberOfParts; i++) {
        if (i < this.partsList.length) {
          newPartsList[i] = this.partsList[i];
        } else {
          newPartsList[i] = new Part();
        }
      }
      this.partsList = newPartsList;
    }
  }

  /**
   * For boundary testing we may need to inject mock parts. For testing only.
   */
  void setParts(Part[] parts) {
    this.partsList = parts;
  }

  public void setTransactionId(int transactionId) {
    this.messageModified = true;
    this.transactionId = transactionId;
  }

  public void setIsRetry() {
    this.isRetry = true;
  }

  /**
   * This returns true if the message has been marked as having been previously transmitted to a
   * different server.
   */
  public boolean isRetry() {
    return this.isRetry;
  }

  /* Sets size for HDOS chunk. */
  public void setChunkSize(int chunkSize) {
    this.chunkSize = chunkSize;
  }

  /**
   * When building a Message this will return the number of the next Part to be added to the message
   */
  int getNextPartNumber() {
    return this.currentPart;
  }

  public void addStringPart(String str) {
    addStringPart(str, false);
  }

  private static final Map<String, byte[]> CACHED_STRINGS = new ConcurrentHashMap<>();

  public void addStringPart(String str, boolean enableCaching) {
    if (str == null) {
      addRawPart(null, false);
      return;
    }

    Part part = this.partsList[this.currentPart];
    if (enableCaching) {
      byte[] bytes = CACHED_STRINGS.get(str);
      if (bytes == null) {
        try (HeapDataOutputStream hdos = new HeapDataOutputStream(str)) {
          bytes = hdos.toByteArray();
          CACHED_STRINGS.put(str, bytes);
        }
      }
      part.setPartState(bytes, false);

    } else {
      // do NOT close the HeapDataOutputStream
      this.messageModified = true;
      part.setPartState(new HeapDataOutputStream(str), false);
    }
    this.currentPart++;
  }

  /*
   * Adds a new part to this message that contains a {@code byte} array (as opposed to a serialized
   * object).
   *
   * @see #addPart(byte[], boolean)
   */
  public void addBytesPart(byte[] newPart) {
    addRawPart(newPart, false);
  }

  public void addStringOrObjPart(Object o) {
    if (o instanceof String || o == null) {
      addStringPart((String) o);
    } else {
      // Note even if o is a byte[] we need to serialize it.
      // This could be cleaned up but it would require C client code to change.
      serializeAndAddPart(o, false);
    }
  }

  public void addObjPart(Object o) {
    addObjPart(o, false);
  }

  /**
   * Like addObjPart(Object) but also prefers to reference objects in the part instead of copying
   * them into a byte buffer.
   */
  public void addObjPartNoCopying(Object o) {
    if (o == null || o instanceof byte[]) {
      addRawPart((byte[]) o, false);
    } else {
      serializeAndAddPartNoCopying(o);
    }
  }

  public void addObjPart(Object o, boolean zipValues) {
    if (o == null || o instanceof byte[]) {
      addRawPart((byte[]) o, false);
    } else if (o instanceof Boolean) {
      addRawPart((Boolean) o ? TRUE : FALSE, true);
    } else {
      serializeAndAddPart(o, zipValues);
    }
  }

  /**
   * Object o is always null
   */
  public void addPartInAnyForm(@Unretained Object o, boolean isObject) {
    if (o == null) {
      addRawPart((byte[]) o, false);
    } else if (o instanceof byte[]) {
      addRawPart((byte[]) o, isObject);
    } else if (o instanceof StoredObject) {
      // It is possible it is an off-heap StoredObject that contains a simple non-object byte[].
      this.messageModified = true;
      Part part = this.partsList[this.currentPart];
      part.setPartState((StoredObject) o, isObject);
      this.currentPart++;
    } else {
      serializeAndAddPart(o, false);
    }
  }

  private void serializeAndAddPartNoCopying(Object o) {
    Version v = this.version;
    if (this.version.equals(Version.CURRENT)) {
      v = null;
    }

    // Create the HDOS with a flag telling it that it can keep any byte[] or ByteBuffers/ByteSources
    // passed to it. Do NOT close the HeapDataOutputStream!
    HeapDataOutputStream hdos = new HeapDataOutputStream(this.chunkSize, v, true);
    try {
      BlobHelper.serializeTo(o, hdos);
    } catch (IOException ex) {
      throw new SerializationException("failed serializing object", ex);
    }
    this.messageModified = true;
    Part part = this.partsList[this.currentPart];
    part.setPartState(hdos, true);
    this.currentPart++;
  }

  private void serializeAndAddPart(Object o, boolean zipValues) {
    if (zipValues) {
      throw new UnsupportedOperationException("zipValues no longer supported");
    }

    Version v = this.version;
    if (this.version.equals(Version.CURRENT)) {
      v = null;
    }

    // do NOT close the HeapDataOutputStream
    HeapDataOutputStream hdos = new HeapDataOutputStream(this.chunkSize, v);
    try {
      BlobHelper.serializeTo(o, hdos);
    } catch (IOException ex) {
      throw new SerializationException("failed serializing object", ex);
    }
    this.messageModified = true;
    Part part = this.partsList[this.currentPart];
    part.setPartState(hdos, true);
    this.currentPart++;
  }

  public void addIntPart(int v) {
    this.messageModified = true;
    Part part = this.partsList[this.currentPart];
    part.setInt(v);
    this.currentPart++;
  }

  public void addLongPart(long v) {
    this.messageModified = true;
    Part part = this.partsList[this.currentPart];
    part.setLong(v);
    this.currentPart++;
  }

  /**
   * Adds a new part to this message that may contain a serialized object.
   */
  public void addRawPart(byte[] newPart, boolean isObject) {
    this.messageModified = true;
    Part part = this.partsList[this.currentPart];
    part.setPartState(newPart, isObject);
    this.currentPart++;
  }

  public int getMessageType() {
    return this.messageType;
  }

  public int getPayloadLength() {
    return this.payloadLength;
  }

  public int getHeaderLength() {
    return FIXED_LENGTH;
  }

  public int getNumberOfParts() {
    return this.numberOfParts;
  }

  public int getTransactionId() {
    return this.transactionId;
  }

  public Part getPart(int index) {
    if (index < this.numberOfParts) {
      Part p = this.partsList[index];
      if (this.version != null) {
        p.setVersion(this.version);
      }
      return p;
    }
    return null;
  }

  public static ByteBuffer setTLCommBuffer(ByteBuffer bb) {
    ByteBuffer result = tlCommBuffer.get();
    tlCommBuffer.set(bb);
    return result;
  }

  public ByteBuffer getCommBuffer() {
    if (this.cachedCommBuffer != null) {
      return this.cachedCommBuffer;
    } else {
      return tlCommBuffer.get();
    }
  }

  public void clear() {
    this.isRetry = false;
    int len = this.payloadLength;
    if (len != 0) {
      this.payloadLength = 0;
    }
    if (this.readHeader) {
      if (this.messageStats != null) {
        this.messageStats.decMessagesBeingReceived(len);
      }
    }
    ByteBuffer buffer = getCommBuffer();
    if (buffer != null) {
      buffer.clear();
    }
    clearParts();
    if (len != 0 && this.dataLimiter != null) {
      this.dataLimiter.release(len);
      this.dataLimiter = null;
      this.maxIncomingMessageLength = 0;
    }
    if (this.readHeader) {
      if (this.messageLimiter != null) {
        this.messageLimiter.release(1);
        this.messageLimiter = null;
      }
      this.readHeader = false;
    }
    this.flags = 0;
  }

  protected void packHeaderInfoForSending(int msgLen, boolean isSecurityHeader) {
    // setting second bit of flags byte for client this is not require but this makes all changes
    // easily at client side right now just see this bit and process security header
    byte flagsByte = this.flags;
    if (isSecurityHeader) {
      flagsByte |= MESSAGE_HAS_SECURE_PART;
    }
    if (this.isRetry) {
      flagsByte |= MESSAGE_IS_RETRY;
    }
    getCommBuffer().putInt(this.messageType).putInt(msgLen).putInt(this.numberOfParts)
        .putInt(this.transactionId).put(flagsByte);
  }

  protected Part getSecurityPart() {
    if (this.serverConnection != null) {
      // look types right put get etc
      return this.serverConnection.updateAndGetSecurityPart();
    }
    return null;
  }

  public void setSecurePart(byte[] bytes) {
    this.securePart = new Part();
    this.securePart.setPartState(bytes, false);
  }

  public void setMetaRegion(boolean isMetaRegion) {
    this.isMetaRegion = isMetaRegion;
  }

  boolean getAndResetIsMetaRegion() {
    boolean isMetaRegion = this.isMetaRegion;
    this.isMetaRegion = false;
    return isMetaRegion;
  }

  /**
   * Sends this message out on its socket.
   */
  void sendBytes(boolean clearMessage) throws IOException {
    if (this.serverConnection != null) {
      // Keep track of the fact that we are making progress.
      this.serverConnection.updateProcessingMessage();
    }
    if (this.socket == null) {
      throw new IOException("Dead Connection");
    }
    try {
      final ByteBuffer commBuffer = getCommBuffer();
      if (commBuffer == null) {
        throw new IOException("No buffer");
      }
      synchronized (commBuffer) {
        long totalPartLen = 0;
        long headerLen = 0;
        int partsToTransmit = this.numberOfParts;

        for (int i = 0; i < this.numberOfParts; i++) {
          Part part = this.partsList[i];
          headerLen += PART_HEADER_SIZE;
          totalPartLen += part.getLength();
        }

        Part securityPart = this.getSecurityPart();
        if (securityPart == null) {
          securityPart = this.securePart;
        }
        if (securityPart != null) {
          headerLen += PART_HEADER_SIZE;
          totalPartLen += securityPart.getLength();
          partsToTransmit++;
        }

        if (headerLen + totalPartLen > Integer.MAX_VALUE) {
          throw new MessageTooLargeException(
              "Message size (" + (headerLen + totalPartLen) + ") exceeds maximum integer value");
        }

        int msgLen = (int) (headerLen + totalPartLen);

        if (msgLen > this.maxMessageSize) {
          throw new MessageTooLargeException("Message size (" + msgLen
              + ") exceeds gemfire.client.max-message-size setting (" + this.maxMessageSize + ")");
        }

        commBuffer.clear();
        packHeaderInfoForSending(msgLen, securityPart != null);
        for (int i = 0; i < partsToTransmit; i++) {
          Part part = i == this.numberOfParts ? securityPart : this.partsList[i];

          if (commBuffer.remaining() < PART_HEADER_SIZE) {
            flushBuffer();
          }

          int partLen = part.getLength();
          commBuffer.putInt(partLen);
          commBuffer.put(part.getTypeCode());
          if (partLen <= commBuffer.remaining()) {
            part.writeTo(commBuffer);
          } else {
            flushBuffer();
            if (this.socketChannel != null) {
              part.writeTo(this.socketChannel, commBuffer);
            } else {
              part.writeTo(this.outputStream, commBuffer);
            }
            if (this.messageStats != null) {
              this.messageStats.incSentBytes(partLen);
            }
          }
        }
        if (commBuffer.position() != 0) {
          flushBuffer();
        }
        this.messageModified = false;
        if (this.socketChannel == null) {
          this.outputStream.flush();
        }
      }
    } finally {
      if (clearMessage) {
        clearParts();
      }
    }
  }

  void flushBuffer() throws IOException {
    final ByteBuffer cb = getCommBuffer();
    if (this.socketChannel != null) {
      cb.flip();
      do {
        this.socketChannel.write(cb);
      } while (cb.remaining() > 0);
    } else {
      this.outputStream.write(cb.array(), 0, cb.position());
    }
    if (this.messageStats != null) {
      this.messageStats.incSentBytes(cb.position());
    }
    cb.clear();
  }

  private void readHeaderAndBody(int headerReadTimeoutMillis) throws IOException {
    clearParts();
    // TODO: for server changes make sure sc is not null as this class also used by client

    int timeout = socket.getSoTimeout();
    try {
      socket.setSoTimeout(headerReadTimeoutMillis);
      fetchHeader();
    } finally {
      socket.setSoTimeout(timeout);
    }

    final ByteBuffer cb = getCommBuffer();
    final int type = cb.getInt();
    final int len = cb.getInt();
    final int numParts = cb.getInt();
    final int txid = cb.getInt();
    byte bits = cb.get();
    cb.clear();

    if (!MessageType.validate(type)) {
      throw new IOException(String.format("Invalid message type %s while reading header",
          type));
    }

    int timeToWait = 0;
    if (this.serverConnection != null) {
      // Keep track of the fact that a message is being processed.
      this.serverConnection.setProcessingMessage();
      timeToWait = this.serverConnection.getClientReadTimeout();
    }
    this.readHeader = true;

    if (this.messageLimiter != null) {
      for (;;) {
        this.serverConnection.getCachedRegionHelper().checkCancelInProgress(null);
        boolean interrupted = Thread.interrupted();
        try {
          if (timeToWait == 0) {
            this.messageLimiter.acquire(1);
          } else {
            if (!this.messageLimiter.tryAcquire(1, timeToWait, TimeUnit.MILLISECONDS)) {
              if (this.messageStats instanceof CacheServerStats) {
                ((CacheServerStats) this.messageStats).incConnectionsTimedOut();
              }
              throw new IOException(
                  String.format(
                      "Operation timed out on server waiting on concurrent message limiter after waiting %s milliseconds",
                      timeToWait));
            }
          }
          break;
        } catch (InterruptedException ignore) {
          interrupted = true;
        } finally {
          if (interrupted) {
            Thread.currentThread().interrupt();
          }
        }
      } // for
    }

    if (len > 0) {
      if (this.maxIncomingMessageLength > 0 && len > this.maxIncomingMessageLength) {
        throw new IOException(String.format("Message size %s exceeded max limit of %s",
            new Object[] {len, this.maxIncomingMessageLength}));
      }

      if (this.dataLimiter != null) {
        for (;;) {
          if (this.serverConnection != null) {
            this.serverConnection.getCachedRegionHelper().checkCancelInProgress(null);
          }
          boolean interrupted = Thread.interrupted();
          try {
            if (timeToWait == 0) {
              this.dataLimiter.acquire(len);
            } else {
              int newTimeToWait = timeToWait;
              if (this.messageLimiter != null) {
                // may have waited for msg limit so recalc time to wait
                newTimeToWait -= (int) this.serverConnection.getCurrentMessageProcessingTime();
              }
              if (newTimeToWait <= 0
                  || !this.messageLimiter.tryAcquire(1, newTimeToWait, TimeUnit.MILLISECONDS)) {
                throw new IOException(
                    String.format(
                        "Operation timed out on server waiting on concurrent data limiter after waiting %s milliseconds",
                        timeToWait));
              }
            }
            // makes sure payloadLength gets set now so we will release the semaphore
            this.payloadLength = len;
            break; // success
          } catch (InterruptedException ignore) {
            interrupted = true;
          } finally {
            if (interrupted) {
              Thread.currentThread().interrupt();
            }
          }
        }
      }
    }
    if (this.messageStats != null) {
      this.messageStats.incMessagesBeingReceived(len);
      this.payloadLength = len; // makes sure payloadLength gets set now so we will dec on clear
    }

    this.isRetry = (bits & MESSAGE_IS_RETRY) != 0;
    bits &= MESSAGE_IS_RETRY_MASK;
    this.flags = bits;
    this.messageType = type;

    readPayloadFields(numParts, len);

    // Set the header and payload fields only after receiving all the
    // socket data, providing better message consistency in the face
    // of exceptional conditions (e.g. IO problems, timeouts etc.)
    this.payloadLength = len;
    // this.numberOfParts = numParts; Already set in setPayloadFields via setNumberOfParts
    this.transactionId = txid;
    this.flags = bits;
    if (this.serverConnection != null) {
      // Keep track of the fact that a message is being processed.
      this.serverConnection.updateProcessingMessage();
    }
  }

  /**
   * Read the actual bytes of the header off the socket
   */
  void fetchHeader() throws IOException {
    final ByteBuffer cb = getCommBuffer();
    cb.clear();

    // messageType is invalidated here and can be used as an indicator
    // of problems reading the message
    this.messageType = MessageType.INVALID;

    final int headerLength = getHeaderLength();
    if (this.socketChannel != null) {
      cb.limit(headerLength);
      do {
        int bytesRead = this.socketChannel.read(cb);
        if (bytesRead == -1) {
          throw new EOFException(
              "The connection has been reset while reading the header");
        }
        if (this.messageStats != null) {
          this.messageStats.incReceivedBytes(bytesRead);
        }
      } while (cb.remaining() > 0);
      cb.flip();

    } else {
      int hdr = 0;
      do {
        int bytesRead = this.inputStream.read(cb.array(), hdr, headerLength - hdr);
        if (bytesRead == -1) {
          throw new EOFException(
              "The connection has been reset while reading the header");
        }
        hdr += bytesRead;
        if (this.messageStats != null) {
          this.messageStats.incReceivedBytes(bytesRead);
        }
      } while (hdr < headerLength);

      // now setup the commBuffer for the caller to parse it
      cb.rewind();
    }
  }

  /**
   * TODO: refactor overly long method readPayloadFields
   */
  void readPayloadFields(final int numParts, final int len) throws IOException {
    if (len > 0 && numParts <= 0 || len <= 0 && numParts > 0) {
      throw new IOException(
          String.format("Part length ( %s ) and number of parts ( %s ) inconsistent",
              new Object[] {len, numParts}));
    }

    Integer msgType = MESSAGE_TYPE.get();
    if (msgType != null && msgType == MessageType.PING) {
      // set it to null right away.
      MESSAGE_TYPE.set(null);
      // Some number which will not throw OOM but still be acceptable for a ping operation.
      int pingParts = 10;
      if (numParts > pingParts) {
        throw new IOException("Part length ( " + numParts + " ) is  inconsistent for "
            + MessageType.getString(msgType) + " operation.");
      }
    }

    setNumberOfParts(numParts);
    if (numParts <= 0) {
      return;
    }

    if (len < 0) {
      logger.info("rpl: neg len: {}", len);
      throw new IOException("Dead Connection");
    }

    final ByteBuffer cb = getCommBuffer();
    cb.clear();
    cb.flip();

    int readSecurePart = checkAndSetSecurityPart();

    int bytesRemaining = len;
    for (int i = 0; i < numParts + readSecurePart
        || readSecurePart == 1 && cb.remaining() > 0; i++) {
      int bytesReadThisTime = readPartChunk(bytesRemaining);
      bytesRemaining -= bytesReadThisTime;

      Part part;

      if (i < numParts) {
        part = this.partsList[i];
      } else {
        part = this.securePart;
      }

      int partLen = cb.getInt();
      byte partType = cb.get();
      byte[] partBytes = null;

      if (partLen > 0) {
        partBytes = new byte[partLen];
        int alreadyReadBytes = cb.remaining();
        if (alreadyReadBytes > 0) {
          if (partLen < alreadyReadBytes) {
            alreadyReadBytes = partLen;
          }
          cb.get(partBytes, 0, alreadyReadBytes);
        }

        // now we need to read partLen - alreadyReadBytes off the wire
        int off = alreadyReadBytes;
        int remaining = partLen - off;
        while (remaining > 0) {
          if (this.socketChannel != null) {
            int bytesThisTime = remaining;
            cb.clear();
            if (bytesThisTime > cb.capacity()) {
              bytesThisTime = cb.capacity();
            }
            cb.limit(bytesThisTime);
            int res = this.socketChannel.read(cb);
            if (res != -1) {
              cb.flip();
              bytesRemaining -= res;
              remaining -= res;
              cb.get(partBytes, off, res);
              off += res;
              if (this.messageStats != null) {
                this.messageStats.incReceivedBytes(res);
              }
            } else {
              throw new EOFException(
                  "The connection has been reset while reading a part");
            }
          } else {
            int res = this.inputStream.read(partBytes, off, remaining);
            if (res != -1) {
              bytesRemaining -= res;
              remaining -= res;
              off += res;
              if (this.messageStats != null) {
                this.messageStats.incReceivedBytes(res);
              }
            } else {
              throw new EOFException(
                  "The connection has been reset while reading a part");
            }
          }
        }
      }
      part.init(partBytes, partType);
    }
  }

  protected int checkAndSetSecurityPart() {
    if ((this.flags | MESSAGE_HAS_SECURE_PART) == this.flags) {
      this.securePart = new Part();
      return 1;
    } else {
      this.securePart = null;
      return 0;
    }
  }

  /**
   * @param bytesRemaining the most bytes we can read
   * @return the number of bytes read into commBuffer
   */
  private int readPartChunk(int bytesRemaining) throws IOException {
    final ByteBuffer commBuffer = getCommBuffer();
    if (commBuffer.remaining() >= PART_HEADER_SIZE) {
      // we already have the next part header in commBuffer so just return
      return 0;
    }

    if (commBuffer.position() != 0) {
      commBuffer.compact();
    } else {
      commBuffer.position(commBuffer.limit());
      commBuffer.limit(commBuffer.capacity());
    }

    if (this.serverConnection != null) {
      // Keep track of the fact that we are making progress
      this.serverConnection.updateProcessingMessage();
    }
    int bytesRead = 0;

    if (this.socketChannel != null) {
      int remaining = commBuffer.remaining();
      if (remaining > bytesRemaining) {
        remaining = bytesRemaining;
        commBuffer.limit(commBuffer.position() + bytesRemaining);
      }
      while (remaining > 0) {
        int res = this.socketChannel.read(commBuffer);
        if (res != -1) {
          remaining -= res;
          bytesRead += res;
          if (this.messageStats != null) {
            this.messageStats.incReceivedBytes(res);
          }
        } else {
          throw new EOFException(
              "The connection has been reset while reading the payload");
        }
      }

    } else {
      int bytesToRead = commBuffer.capacity() - commBuffer.position();
      if (bytesRemaining < bytesToRead) {
        bytesToRead = bytesRemaining;
      }
      int pos = commBuffer.position();

      while (bytesToRead > 0) {
        int res = this.inputStream.read(commBuffer.array(), pos, bytesToRead);
        if (res != -1) {
          bytesToRead -= res;
          pos += res;
          bytesRead += res;
          if (this.messageStats != null) {
            this.messageStats.incReceivedBytes(res);
          }
        } else {
          throw new EOFException(
              "The connection has been reset while reading the payload");
        }
      }

      commBuffer.position(pos);
    }
    commBuffer.flip();
    return bytesRead;
  }

  /**
   * Gets rid of all the parts that have been added to this message.
   */
  public void clearParts() {
    for (Part part : this.partsList) {
      part.clear();
    }
    this.currentPart = 0;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("type=").append(MessageType.getString(this.messageType));
    sb.append("; payloadLength=").append(this.payloadLength);
    sb.append("; numberOfParts=").append(this.numberOfParts);
    sb.append("; hasSecurePart=").append(isSecureMode());
    sb.append("; transactionId=").append(this.transactionId);
    sb.append("; currentPart=").append(this.currentPart);
    sb.append("; messageModified=").append(this.messageModified);
    sb.append("; flags=").append(Integer.toHexString(this.flags));
    for (int i = 0; i < this.numberOfParts; i++) {
      sb.append("; part[").append(i).append("]={");
      sb.append(this.partsList[i]);
      sb.append("}");
    }
    return sb.toString();
  }

  // Set up a message on the server side.
  void setComms(ServerConnection sc, Socket socket, ByteBuffer bb, MessageStats msgStats)
      throws IOException {
    this.serverConnection = sc;
    setComms(socket, bb, msgStats);
  }

  // Set up a message on the client side.
  void setComms(Socket socket, ByteBuffer bb, MessageStats msgStats) throws IOException {
    this.socketChannel = socket.getChannel();
    if (this.socketChannel == null) {
      setComms(socket, socket.getInputStream(), socket.getOutputStream(), bb, msgStats);
    } else {
      setComms(socket, null, null, bb, msgStats);
    }
  }

  // Set up a message on the client side.
  public void setComms(Socket socket, InputStream is, OutputStream os, ByteBuffer bb,
      MessageStats msgStats) {
    Assert.assertTrue(socket != null);
    this.socket = socket;
    this.socketChannel = socket.getChannel();
    this.inputStream = is;
    this.outputStream = os;
    this.cachedCommBuffer = bb;
    this.messageStats = msgStats;
  }

  /**
   * Undo any state changes done by setComms.
   *
   * @since GemFire 5.7
   */
  public void unsetComms() {
    this.socket = null;
    this.socketChannel = null;
    this.inputStream = null;
    this.outputStream = null;
    this.cachedCommBuffer = null;
    this.messageStats = null;
  }

  /**
   * Sends this message to its receiver over its setOutputStream?? output stream.
   */
  public void send() throws IOException {
    send(true);
  }

  public void send(ServerConnection servConn) throws IOException {
    if (this.serverConnection != servConn)
      throw new IllegalStateException("this.sc was not correctly set");
    send(true);
  }

  /**
   * Sends this message to its receiver over its setOutputStream?? output stream.
   */
  public void send(boolean clearMessage) throws IOException {
    sendBytes(clearMessage);
  }

  /**
   * Read a message, populating the state of this {@code Message} with information received via its
   * socket
   *
   * @param timeoutMillis timeout setting for reading the header (0 = no timeout)
   */
  public void receiveWithHeaderReadTimeout(int timeoutMillis) throws IOException {
    if (this.socket != null) {
      synchronized (getCommBuffer()) {
        readHeaderAndBody(timeoutMillis);
      }
    } else {
      throw new IOException("Dead Connection");
    }
  }

  /**
   * Populates the state of this {@code Message} with information received via its socket
   */
  public void receive() throws IOException {
    receiveWithHeaderReadTimeout(NO_HEADER_READ_TIMEOUT);
  }

  public void receive(ServerConnection sc, int maxMessageLength, Semaphore dataLimiter,
      Semaphore msgLimiter) throws IOException {
    this.serverConnection = sc;
    this.maxIncomingMessageLength = maxMessageLength;
    this.dataLimiter = dataLimiter;
    this.messageLimiter = msgLimiter;
    receive();
  }

}
