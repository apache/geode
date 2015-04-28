/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.tier.sockets;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.SerializationException;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.HeapDataOutputStream;
import com.gemstone.gemfire.internal.SocketUtils;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.cache.TXManagerImpl;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.internal.util.BlobHelper;

/**
 * This class encapsulates the wire protocol. It provides accessors to
 * encode and decode a message and  serialize it out to the wire.
 *
 * <PRE>
 * msgType       - int   - 4 bytes type of message, types enumerated below
 *
 * msgLength     - int - 4 bytes   total length of variable length payload
 *
 * numberOfParts - int - 4 bytes   number of elements (LEN-BYTE* pairs)
 *                     contained in the payload. Message can
 *                       be a multi-part message
 *
 * transId       - int - 4 bytes  filled in by the requestor, copied back into
 *                    the response
 *
 * earlyAck      - byte- 1 byte   filled in by the requestor
 * len1
 * part1
 * .
 * .
 * .
 * lenn
 * partn
 * </PRE>
 *
 * We read the fixed length 16 bytes into a byte[] and populate a bytebuffer
 * We read the fixed length header tokens from the header
 * parse the header and use information contained in there to read the payload.
 *
 * <P>
 *
 * See also <a href="package-summary.html#messages">package description</a>.
 *
 * @see com.gemstone.gemfire.internal.cache.tier.MessageType
 *
 * @author Sudhir Menon
 * @since 2.0.2
 */
public class Message  {

  private static final Logger logger = LogService.getLogger();
  
  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append("type=" + MessageType.getString(msgType));
    sb.append("; payloadLength=" + payloadLength);
    sb.append("; numberOfParts=" + numberOfParts);
    sb.append("; transactionId=" + transactionId);
    //sb.append("; bufferLength=" + bufferLength);
    sb.append("; currentPart=" + currentPart);
    sb.append("; messageModified=" + messageModified);
    sb.append("; earlyAck=" + earlyAck);
    for (int i = 0; i < numberOfParts; i ++) {
      sb.append("; part[" + i + "]={");
      sb.append(this.partsList[i].toString());
      sb.append("}");
    }
    return sb.toString();
  }

  protected final static int FIXED_LENGTH = 17;
  protected int msgType;
  protected int payloadLength=0;
  protected int numberOfParts =0;
  protected int transactionId = TXManagerImpl.NOTX;
  protected int currentPart = 0;
  protected Part[] partsList = null;
  protected ByteBuffer cachedCommBuffer;
  protected Socket socket = null;
  protected SocketChannel sockCh = null;
  protected OutputStream os = null;
  protected InputStream is = null;
  protected boolean messageModified = true;
  /** is this message a retry of a previously sent message? */
  protected boolean isRetry;
  private byte earlyAck = 0x00;
  protected MessageStats msgStats = null;
  protected ServerConnection sc = null;
  private int MAX_DATA = -1;
  private Semaphore dataLimiter = null;
//  private int MAX_MSGS = -1;
  private Semaphore msgLimiter = null;
  private boolean hdrRead = false;  
  private int chunkSize = 1024;//Default Chunk Size.

  protected Part securePart = null;

  // These two statics are fields shoved into the earlyAck byte for transmission.
  // The MESSAGE_IS_RETRY bit is stripped out during deserialization but the other
  // is left in place
  public static final byte MESSAGE_HAS_SECURE_PART = (byte)0x02;
  public static final byte MESSAGE_IS_RETRY = (byte)0x04;
  
  public static final byte MESSAGE_IS_RETRY_MASK = (byte)0xFB;

  // Tentative workaround to avoid OOM stated in #46754.
  public static final ThreadLocal<Integer> messageType = new ThreadLocal<Integer>();
  
  Version destVersion;
  
  /**
   * Creates a new message with the given number of parts
   */
  public Message(int numberOfParts, Version destVersion) {
    this.destVersion = destVersion;
    Assert.assertTrue(destVersion != null, "Attempt to create an unversioned message");
    partsList = new Part[numberOfParts];
    this.numberOfParts = numberOfParts;
    for (int i=0;i<partsList.length;i++) {
      partsList[i] = new Part();
    }
  }

  public boolean isSecureMode() {    
    return securePart != null;
  }
  
  public byte[] getSecureBytes()
    throws IOException, ClassNotFoundException {
    return (byte[])this.securePart.getObject();
  }
  
  public void setMessageType(int msgType) {
    this.messageModified = true;
    if (!MessageType.validate(msgType)) {
      throw new IllegalArgumentException(LocalizedStrings.Message_INVALID_MESSAGETYPE.toLocalizedString());
    }
    this.msgType = msgType;
  }
  
  public void setVersion(Version clientVersion) {
    this.destVersion = clientVersion;
  }

  /**
   * Sets whether this message is early-ack
   * @param earlyAck whether this message is early-ack
   */
  public void setEarlyAck(boolean earlyAck) {
    if (earlyAck) {
      this.earlyAck = 0x01;
    } else {
      this.earlyAck = 0x00;
    }
  }

  // TODO (ashetkar) To be removed later.
  public void setEarlyAck(byte earlyAck) {
    // Check that the passed in value is within the acceptable range.
    if (0x00 <= earlyAck && earlyAck <= 0x02) {
      this.earlyAck |= earlyAck;
    }
  }

  /*
   * public void setPayloadLength(int payloadLength) {
     this.payloadLength = payloadLength;
  }*/

  /**
   *  Sets and builds the {@link Part}s that are sent
   *  in the payload of the Message
   * @param numberOfParts
   */
  public void setNumberOfParts(int numberOfParts) {
    //TODO:hitesh need to add security header here from server
    //need to insure it is not chunked message
    //should we look message type to avoid internal message like ping
    this.messageModified = true;
    this.currentPart=0;
    this.numberOfParts = numberOfParts;
    if (numberOfParts > this.partsList.length) {
      Part[] newPartsList = new Part[numberOfParts];
      for (int i=0;i<numberOfParts;i++) {
        if (i < this.partsList.length) {
          newPartsList[i] = this.partsList[i];
        } else {
          newPartsList[i] = new Part();
        }
      }
      this.partsList = newPartsList;
    }
  }

  public void setTransactionId(int transactionId) {
    this.messageModified = true;
    this.transactionId = transactionId;
  }
  
  public void setIsRetry() {
    this.isRetry = true;
  }
  
  /**
   * This returns true if the message has been marked as having been previously
   * transmitted to a different server.
   */
  public boolean isRetry() {
    return this.isRetry;
  }

  /*Sets size for HDOS chunk.*/
  public void setChunkSize(int chunkSize) {
    this.chunkSize = chunkSize;
  }

  public void addStringPart(String str) {
    if (str==null) {
      addRawPart((byte[])null, false);
    }
    else {
      HeapDataOutputStream hdos = new HeapDataOutputStream(str);
      this.messageModified = true;
      Part part = partsList[this.currentPart];
      part.setPartState(hdos, false);
      this.currentPart++;
    }
  }

  /**
   * Sets whether or not a
   * <code>DataOutputStream</code>/<code>DataOutputStream</code>
   * should be used to send/receive data.
      public void setUseDataStream (boolean useDataStream) {
        this.useDataStream = useDataStream;
    }
   */

  /*
   * Adds a new part to this message that contains a <code>byte</code>
   * array (as opposed to a serialized object).
   *
   * @see #addPart(byte[], boolean)
   */
  public void addBytesPart(byte[] newPart) {
    addRawPart(newPart, false);
  }

  public void addStringOrObjPart(Object o) {
    if (o instanceof String || o == null) {
      addStringPart((String)o);
    } else {
      // Note even if o is a byte[] we need to serialize it.
      // This could be cleaned up but it would require C client code to change.
      serializeAndAddPart(o, false);
    }
  }

  public void addDeltaPart(HeapDataOutputStream hdos) { // TODO: Amogh- Should it be just DataOutput?
    this.messageModified = true;
    Part part = partsList[this.currentPart];
    part.setPartState(hdos, false);
    this.currentPart++;
  }

  public void addObjPart(Object o) {
    addObjPart(o, false);
  }
  public void addObjPart(Object o, boolean zipValues) {
    if (o == null || o instanceof byte[]) {
      addRawPart((byte[])o, false);
    } else {
      serializeAndAddPart(o, zipValues);
    }
  }

  private void serializeAndAddPart(Object o, boolean zipValues) {
    if (zipValues) {
      throw new UnsupportedOperationException("zipValues no longer supported");    
      
//       byte[] b = CacheServerHelper.serialize(o, zipValues);
//       addRawPart(b, true);
    } else {
      HeapDataOutputStream hdos;
      Version v = destVersion;
      if (destVersion.equals(Version.CURRENT)){
        v = null;
      }
      hdos = new HeapDataOutputStream(chunkSize, v);
      try {
//        logger.fine("hitesh before serializatino: " );
//        
//        if (o != null ){
//          logger.fine("hitesh before serializatino: " + o.toString());
//          logger.fine("hitesh before serializatino: " + o.getClass().getName());
//        }
        BlobHelper.serializeTo(o, hdos);
      } catch (IOException ex) {
        throw new SerializationException("failed serializing object", ex);
      }
      this.messageModified = true;
      Part part = partsList[this.currentPart];
      part.setPartState(hdos, true);
      this.currentPart++;
    }
  }

  public void addIntPart(int v) {
    this.messageModified = true;
    Part part = partsList[this.currentPart];
    part.setInt(v);
    this.currentPart++;
  }
  
  public void addLongPart(long v) {
    this.messageModified = true;
    Part part = partsList[this.currentPart];
    part.setLong(v);
    this.currentPart++;
  }
  
  /**
   * Adds a new part to this message that may contain a serialized
   * object.
   */
  public void addRawPart(byte[] newPart,boolean isObject) {
    this.messageModified = true;
    Part part = partsList[this.currentPart];
    part.setPartState(newPart, isObject);
    this.currentPart++;
  }

  public int getMessageType() {
    return this.msgType;
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
    if (index < this.numberOfParts)
      return partsList[index];
    return null;
  }

  public boolean getEarlyAck() {
    return this.earlyAck == 0x01 ? true: false;
  }

  // TODO (ashetkar) To be removed
  public byte getEarlyAckByte() {
    return this.earlyAck;
  }

  private static ThreadLocal tlCommBuffer = new ThreadLocal();

  public static ByteBuffer setTLCommBuffer(ByteBuffer bb) {
    ByteBuffer result = (ByteBuffer)tlCommBuffer.get();
    tlCommBuffer.set(bb);
    return result;
  }

  public ByteBuffer getCommBuffer() {
    if (this.cachedCommBuffer != null) {
      return this.cachedCommBuffer;
    }
    else {
      return (ByteBuffer)tlCommBuffer.get();
    }
  }

  public void clear() {
    this.isRetry = false;
    int len = this.payloadLength;
    if (len != 0) {
      this.payloadLength = 0;
    }
    if (this.hdrRead) {
      if (this.msgStats != null) {
        this.msgStats.decMessagesBeingReceived(len);
      }
    }
    if (this.socket != null) {
      getCommBuffer().clear();
    }
    flush();
    if (len != 0 && this.dataLimiter != null) {
      this.dataLimiter.release(len);
      this.dataLimiter = null;
      this.MAX_DATA = 0;
    }
    if (this.hdrRead) {
      if (this.msgLimiter != null) {
        this.msgLimiter.release(1);
        this.msgLimiter = null;
      }
      this.hdrRead = false;
    }
  }

  protected void packHeaderInfoForSending(int msgLen, boolean isSecurityHeader) {
    //TODO:hitesh setting second bit of early ack for client 
    //this is not require but this makes all changes easily at client side right now
    //just see this bit and process security header
    byte eAck = this.earlyAck;
    if (isSecurityHeader) {
      eAck |= MESSAGE_HAS_SECURE_PART;
    }
    if (this.isRetry) {
      eAck |= MESSAGE_IS_RETRY;
    }
    getCommBuffer()
      .putInt(this.msgType)
      .putInt(msgLen)
      .putInt(this.numberOfParts)
      .putInt(this.transactionId)
      .put(eAck);
  }

  private static final int PART_HEADER_SIZE = 5; // 4 bytes for length, 1 byte for isObject
  
  protected Part getSecurityPart() {
    if (this.sc != null ) {
      //look types right put get etc
     return this.sc.updateAndGetSecurityPart(); 
    }
    return null;
  }

  public void setSecurePart(byte[] bytes) {
    this.securePart = new Part();
    this.securePart.setPartState(bytes, false);
  }

  private boolean m_isMetaRegion = false;

  public void setMetaRegion(boolean isMetaRegion) {
    this.m_isMetaRegion = isMetaRegion;
  }

  public boolean getAndResetIsMetaRegion() {
    boolean isMetaRegion = this.m_isMetaRegion;
    this.m_isMetaRegion = false;
    return isMetaRegion;
  }

  /**
   * Sends this message out on its socket.
   */
  protected void sendBytes(boolean clearMessage) throws IOException {
    if (this.sc != null) {
      // Keep track of the fact that we are making progress.
      this.sc.updateProcessingMessage();
    }
    if (this.socket != null) {
      final ByteBuffer cb = getCommBuffer();
      if (cb == null) {
        throw new IOException("No buffer");
      }
      synchronized(cb) {
        int numOfSecureParts = 0;
        Part securityPart = this.getSecurityPart();
        boolean isSecurityHeader = false;
        
        if (securityPart != null) {
          isSecurityHeader = true;
          numOfSecureParts = 1;
        }
        else if (this.securePart != null) {
          // This is a client sending this message.
          securityPart = this.securePart;
          isSecurityHeader = true;
          numOfSecureParts = 1;          
        }

        //this.logger.fine("hitesh sendbytes forServer_SecurityPart " + numOfSecureParts);
        int totalPartLen = 0;
        for (int i=0;i<this.numberOfParts;i++){
          Part part = this.partsList[i];
          totalPartLen += part.getLength();
        }

        if(numOfSecureParts == 1) {
          totalPartLen += securityPart.getLength();
        }
        int msgLen = (PART_HEADER_SIZE * (this.numberOfParts + numOfSecureParts)) + totalPartLen;
        cb.clear();
        packHeaderInfoForSending(msgLen, isSecurityHeader);
        for (int i=0;i<this.numberOfParts + numOfSecureParts;i++) {
          Part part = null;
          if(i == this.numberOfParts) {
            part = securityPart;
          }
          else {
            part = partsList[i];
          }
          if (cb.remaining() < PART_HEADER_SIZE) {
            flushBuffer();
          }
          int partLen = part.getLength();
          cb.putInt(partLen);
          cb.put(part.getTypeCode());
          if (partLen <= cb.remaining()) {
            part.sendTo(cb);
          } else {
            flushBuffer();
            // send partBytes
            if (this.sockCh != null) {
              part.sendTo(this.sockCh, cb);
            } else {
              part.sendTo(this.os);
            }
            if (this.msgStats != null) {
              this.msgStats.incSentBytes(partLen);
            }
          }
        }
        if (cb.position() != 0) {
          flushBuffer();
        }
        this.messageModified = false;
        if (this.sockCh == null) {
          this.os.flush();
        }
      }
      if(clearMessage) {
        flush();
      }
    }
    else {
      throw new IOException(LocalizedStrings.Message_DEAD_CONNECTION.toLocalizedString());
    }
  }

  protected void flushBuffer() throws IOException {
    final ByteBuffer cb = getCommBuffer();
    if (this.sockCh != null) {
      cb.flip();
      do {
        this.sockCh.write(cb);
      } while (cb.remaining() > 0);
    } else {
      this.os.write(cb.array(), 0, cb.position());
    }
    if (this.msgStats != null) {
      this.msgStats.incSentBytes(cb.position());
    }
    cb.clear();
  }

  private void read()
  throws IOException {
    flush();
    //TODO:Hitesh ??? for server changes make sure sc is not null as this class also used by client :(
    readHeaderAndPayload();
  }

  /**
   * Read the actual bytes of the header off the socket
   */
  protected final void fetchHeader() throws IOException {
    final ByteBuffer cb = getCommBuffer();
    cb.clear();
    // msgType is invalidated here and can be used as an indicator
    // of problems reading the message
    this.msgType = MessageType.INVALID;

    int hdr = 0;

    final int headerLength = getHeaderLength();
    if (this.sockCh != null) {
      cb.limit(headerLength);
      do {
        int bytesRead = this.sockCh.read(cb);
        //System.out.println("DEBUG: fetchHeader read " + bytesRead + " bytes commBuffer=" + cb);
        if (bytesRead == -1) {
          throw new EOFException(LocalizedStrings.Message_THE_CONNECTION_HAS_BEEN_RESET_WHILE_READING_THE_HEADER.toLocalizedString());
        }
        if (this.msgStats != null) {
          this.msgStats.incReceivedBytes(bytesRead);
        }
      } while (cb.remaining() > 0);
      cb.flip();
    } else {
      do {
        int bytesRead = -1;
        try {
          bytesRead = this.is.read(cb.array(),hdr, headerLength-hdr);
        }
        catch (SocketTimeoutException e) {
//          bytesRead = 0;
          // TODO add a cancellation check
          throw e;
        }
        if (bytesRead == -1) {
          throw new EOFException(LocalizedStrings.Message_THE_CONNECTION_HAS_BEEN_RESET_WHILE_READING_THE_HEADER.toLocalizedString());
        }
        hdr += bytesRead;
        if (this.msgStats != null) {
          this.msgStats.incReceivedBytes(bytesRead);
        }
      } while (hdr < headerLength);

      // now setup the commBuffer for the caller to parse it
      cb.rewind();
    }
  }

  private void readHeaderAndPayload()
  throws IOException {
    //TODO:Hitesh ???
    fetchHeader();
    final ByteBuffer cb = getCommBuffer();
    final int type = cb.getInt();
    final int len = cb.getInt();
    final int numParts = cb.getInt();
    final int txid = cb.getInt();
    byte early = cb.get();
    cb.clear();

    if (!MessageType.validate(type)) {
      throw new IOException(LocalizedStrings.Message_INVALID_MESSAGE_TYPE_0_WHILE_READING_HEADER.toLocalizedString(Integer.valueOf(type)));
    }
    int timeToWait = 0;
    if (this.sc != null) {
      // Keep track of the fact that a message is being processed.
      this.sc.setProcessingMessage();
      timeToWait = sc.getClientReadTimeout();
    }
    this.hdrRead = true;
    if (this.msgLimiter != null) {
        for (;;) {
          this.sc.getCachedRegionHelper().checkCancelInProgress(null);
          boolean interrupted = Thread.interrupted();
          try {
            if (timeToWait == 0) {
              this.msgLimiter.acquire(1);
            } 
            else {
              if (!this.msgLimiter.tryAcquire(1, timeToWait, TimeUnit.MILLISECONDS)) {
                if (this.msgStats != null
                    && this.msgStats instanceof CacheServerStats) {
                  ((CacheServerStats)this.msgStats).incConnectionsTimedOut();
                }
                throw new IOException(LocalizedStrings.Message_OPERATION_TIMED_OUT_ON_SERVER_WAITING_ON_CONCURRENT_MESSAGE_LIMITER_AFTER_WAITING_0_MILLISECONDS.toLocalizedString(Integer.valueOf(timeToWait)));
              }
            }
            break;
          }
          catch (InterruptedException e) {
            interrupted = true;
          }
          finally {
            if (interrupted) {
              Thread.currentThread().interrupt();
            }
          }
        } // for
    }
    if (len > 0) {
      if (this.MAX_DATA > 0 && len > this.MAX_DATA) {
        throw new IOException(LocalizedStrings.Message_MESSAGE_SIZE_0_EXCEEDED_MAX_LIMIT_OF_1.toLocalizedString(new Object[] {Integer.valueOf(len), Integer.valueOf(this.MAX_DATA)}));
      }
      if (this.dataLimiter != null) {
        for (;;) {
          if (sc != null) {
            this.sc.getCachedRegionHelper().checkCancelInProgress(null);
          }
          boolean interrupted = Thread.interrupted();
          try {
            if (timeToWait == 0) {
              this.dataLimiter.acquire(len);
            } 
            else {
              int newTimeToWait = timeToWait;
              if (this.msgLimiter != null) {
                // may have waited for msg limit so recalc time to wait
                newTimeToWait -= (int)sc.getCurrentMessageProcessingTime();
              }
              if (newTimeToWait <= 0 || !this.msgLimiter.tryAcquire(1, newTimeToWait, TimeUnit.MILLISECONDS)) {
                throw new IOException(LocalizedStrings.Message_OPERATION_TIMED_OUT_ON_SERVER_WAITING_ON_CONCURRENT_DATA_LIMITER_AFTER_WAITING_0_MILLISECONDS.toLocalizedString(timeToWait));
              }
            }
            this.payloadLength = len; // makes sure payloadLength gets set now so we will release the semaphore
            break; // success
          }
          catch (InterruptedException e) {
            interrupted = true;
          }
          finally {
            if (interrupted) {
              Thread.currentThread().interrupt();
            }
          }
        }
      }
    }
    if (this.msgStats != null) {
      this.msgStats.incMessagesBeingReceived(len);
      this.payloadLength = len; // makes sure payloadLength gets set now so we will dec on clear
    }
    
    this.isRetry = (early & MESSAGE_IS_RETRY) != 0;
    early = (byte)(early & MESSAGE_IS_RETRY_MASK);

    //TODO:hitesh it was below ??
    this.earlyAck = early;
    this.msgType = type;
    //this.logger.fine("Before reading message parts, earlyAck already read as " + this.earlyAck);
    readPayloadFields(numParts, len);

    // Set the header and payload fields only after receiving all the
    // socket data, providing better message consistency in the face
    // of exceptional conditions (e.g. IO problems, timeouts etc.)
    this.msgType = type;
    this.payloadLength = len;
    // this.numberOfParts = numParts;  Already set in setPayloadFields via setNumberOfParts
    this.transactionId = txid;
    this.earlyAck = early;
    if (this.sc != null) {
      // Keep track of the fact that a message is being processed.
      this.sc.updateProcessingMessage();
    }
  }

//   static final int MAX_PART_BUFFERS = 2;
//   static final int MIN_PART_BUFFER_SIZE = 999;
//   static final int MAX_PART_BUFFER_SIZE = 1024*1024*11;
//   static ArrayList partBuffers = new ArrayList(2);
//   static int partBufferIdx = 0;
//   static {
//     for (int i=0; i < MAX_PART_BUFFERS; i++) {
//       partBuffers.add(i, null);
//     }
//   }

//   private static synchronized byte[] getPartBuffer(int size) {
//     byte[] result;
//     synchronized (partBuffers) {
//       result = (byte[])partBuffers.get(partBufferIdx);
//       if (result == null) {
//         result = new byte[size];
//         partBuffers.add(partBufferIdx, result);
//       } else if (result.length != size) {
//         // can't use a cached one
//         return null;
//       }
//       partBufferIdx++;
//       if (partBufferIdx >= MAX_PART_BUFFERS) {
//         partBufferIdx = 0;
//       }
//     }
//     return result;
//   }

  protected void readPayloadFields(final int numParts, final int len)
  throws IOException {
    //TODO:Hitesh
    if (len > 0 && numParts <= 0 ||
        len <= 0 && numParts > 0) {
      throw new IOException(LocalizedStrings.Message_PART_LENGTH_0_AND_NUMBER_OF_PARTS_1_INCONSISTENT.toLocalizedString(
            new Object[] {Integer.valueOf(len), Integer.valueOf(numParts)}));
    }

    Integer msgType = messageType.get();
    if (msgType != null && msgType == MessageType.PING) {
      messageType.set(null); // set it to null right away.
      int pingParts = 10; // Some number which will not throw OOM but still be acceptable for a ping operation.
      if (numParts > pingParts) {
        throw new IOException("Part length ( " + numParts
            + " ) is  inconsistent for " + MessageType.getString(msgType)
            + " operation.");
      }
    }
    setNumberOfParts(numParts);
    if (numParts <= 0)
      return;
  
    if (len < 0) {
      logger.info(LocalizedMessage.create(LocalizedStrings.Message_RPL_NEG_LEN__0, len));
      throw new IOException(LocalizedStrings.Message_DEAD_CONNECTION.toLocalizedString());
    }    
    
    final ByteBuffer cb = getCommBuffer();
    cb.clear();
    cb.flip();

    int readSecurePart = 0;
    //TODO:Hitesh look if securePart can be cached here
    //this.logger.fine("readPayloadFields() early ack = " + this.earlyAck);
    readSecurePart = checkAndSetSecurityPart();
    
    int bytesRemaining = len;
    //this.logger.fine("readPayloadFields() : numParts=" + numParts + " len=" + len);
    for (int i = 0; ((i < numParts + readSecurePart) || ((readSecurePart == 1) && (cb
        .remaining() > 0))); i++) {
      int bytesReadThisTime = readPartChunk(bytesRemaining);
      bytesRemaining -= bytesReadThisTime;

      Part part;
      
      if(i < numParts) {
        part = this.partsList[i];
      }
      else {
        part = this.securePart;
      }
      
      int partLen = cb.getInt();
      byte partType = cb.get();
      byte[] partBytes = null;
//      this.logger.fine("readPayloadFields(): partLen=" + partLen + " partType=" + partType);
      if (partLen > 0) {
//         if (partLen >= MIN_PART_BUFFER_SIZE && partLen <= MAX_PART_BUFFER_SIZE) {
//           partBytes = getPartBuffer(partLen);
//         }
//         if (partBytes == null) {
          partBytes = new byte[partLen];
//         }
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
          if (this.sockCh != null) {
            int bytesThisTime = remaining;
            cb.clear();
            if (bytesThisTime > cb.capacity()) {
              bytesThisTime = cb.capacity();
            }
            cb.limit(bytesThisTime);
            int res = this.sockCh.read(cb);
            //System.out.println("DEBUG: part read " + res + " bytes commBuffer=" + cb);
            if (res != -1) {
              cb.flip();
              bytesRemaining -= res;
              remaining -= res;
              cb.get(partBytes, off, res);
              off += res;
              if (this.msgStats != null) {
                this.msgStats.incReceivedBytes(res);
              }
            } else {
              throw new EOFException(LocalizedStrings.Message_THE_CONNECTION_HAS_BEEN_RESET_WHILE_READING_A_PART.toLocalizedString());
            }
          } else {
            int res = 0;
            try {
              res = this.is.read(partBytes, off, remaining);
            }
            catch (SocketTimeoutException e) {
//              res = 0;
              // TODO: add cancellation check
              throw e;
            }
            if (res != -1) {
              bytesRemaining -= res;
              remaining -= res;
              off += res;
              if (this.msgStats != null) {
                this.msgStats.incReceivedBytes(res);
              }
            } else {
              throw new EOFException(LocalizedStrings.Message_THE_CONNECTION_HAS_BEEN_RESET_WHILE_READING_A_PART.toLocalizedString());
            }
          }
        }
      }
      part.init(partBytes, partType);
    }
  }

  protected int checkAndSetSecurityPart() {
    if ((this.earlyAck | MESSAGE_HAS_SECURE_PART) == this.earlyAck) {
      this.securePart = new Part();
      return 1;
    }
    else {
      this.securePart = null;
      return 0;
    }
  }

  /**
   * @param bytesRemaining the most bytes we can read
   * @return the number of bytes read into commBuffer
   */
  private int readPartChunk(int bytesRemaining) throws IOException {
    final ByteBuffer cb = getCommBuffer();
    //this.logger.info("DEBUG: commBuffer.remaining=" + cb.remaining());
    if (cb.remaining() >= PART_HEADER_SIZE) {
      // we already have the next part header in commBuffer so just return
      return 0;
    }
    if (cb.position() != 0) {
      cb.compact();
    } else {
      cb.position(cb.limit());
      cb.limit(cb.capacity());
    }
    int bytesRead = 0;
    if (this.sc != null) {
      // Keep track of the fact that we are making progress
      this.sc.updateProcessingMessage();
    }
    if (this.sockCh != null) {
      int remaining = cb.remaining();
      if (remaining > bytesRemaining) {
        remaining = bytesRemaining;
        cb.limit(cb.position()+bytesRemaining);
      }
      while (remaining > 0) {
        int res = this.sockCh.read(cb);
        //System.out.println("DEBUG: partChunk read " + res + " bytes commBuffer=" + cb);
        if (res != -1) {
          remaining -= res;
          bytesRead += res;
          if (this.msgStats != null) {
            this.msgStats.incReceivedBytes(res);
          }
        } else {
          throw new EOFException(LocalizedStrings.Message_THE_CONNECTION_HAS_BEEN_RESET_WHILE_READING_THE_PAYLOAD.toLocalizedString());
        }
      }

    } else {
      int bufSpace = cb.capacity() - cb.position();
      int bytesToRead = bufSpace;
      if (bytesRemaining < bytesToRead) {
        bytesToRead = bytesRemaining;
      }
      int pos = cb.position();
      while (bytesToRead > 0) {
        int res = 0;
        try {
          res = this.is.read(cb.array(), pos, bytesToRead);
        }
        catch (SocketTimeoutException e) {
//          res = 0;
          // TODO add a cancellation check
          throw e;
        }
        if (res != -1) {
          bytesToRead -= res;
          pos += res;
          bytesRead += res;
          if (this.msgStats != null) {
            this.msgStats.incReceivedBytes(res);
          }
        } else {
          throw new EOFException(LocalizedStrings.Message_THE_CONNECTION_HAS_BEEN_RESET_WHILE_READING_THE_PAYLOAD.toLocalizedString());
        }
      }
      cb.position(pos);
    }
    cb.flip();
    return bytesRead;
  }

  /**
   * Flushes any previously unwritten information and resets this
   * <code>Message</code> object for reuse.
   */
  public void flush() {
    for (int i=0; i< partsList.length; i++){
      partsList[i].clear();
    }
    this.currentPart=0;
  }

  public void setComms(Socket socket, ByteBuffer bb, MessageStats msgStats) throws IOException {
    this.sockCh = socket.getChannel();
    if (this.sockCh == null) {
      setComms(socket, SocketUtils.getInputStream(socket), SocketUtils.getOutputStream(socket), bb, msgStats);
    } else {
      setComms(socket, null, null,  bb, msgStats);
    }
  }
  
  public void setComms(Socket socket, InputStream is, OutputStream os, ByteBuffer bb, MessageStats msgStats)
    throws IOException
  {
    Assert.assertTrue(socket != null);
    this.socket = socket;
    this.sockCh = socket.getChannel();
    this.is = is;
    this.os = os;
    this.cachedCommBuffer = bb;
    this.msgStats = msgStats;
  }
  /**
   * Undo any state changes done by setComms.
   * @since 5.7
   */
  public void unsetComms() {
    this.socket = null;
    this.sockCh = null;
    this.is = null;
    this.os = null;
    this.cachedCommBuffer = null;
    this.msgStats = null;
  }

  /**
   * Sends this message to its receiver over its
   * setOutputStream?? output stream.
   */
  public void send()
  throws IOException {
    send(true);
  }
  
  public void send(ServerConnection servConn)
  throws IOException {
    this.sc = servConn;
    send(true);
  }
  
  /**
   * Sends this message to its receiver over its
   * setOutputStream?? output stream.
   */
  public void send(boolean clearMessage)
  throws IOException {
    sendBytes(clearMessage);
  }

  public void send(ServerConnection servConn, int transactionId) throws IOException {
    this.sc = servConn;
    sendBytes(true);
  }

  public void send(int transactionId)
    throws IOException {
    sendBytes(true);
  }

  /**
   *  Populates the stats of this <code>Message</code> with information
   *  received via its socket
   */
  public void recv()
  throws IOException {
    if (this.socket != null) {
      synchronized(getCommBuffer()) {
        read();
      }
    }
    else {
      throw new IOException(LocalizedStrings.Message_DEAD_CONNECTION.toLocalizedString());
    }
  }
  public void recv(ServerConnection sc, int MAX_DATA, Semaphore dataLimiter, int MAX_MSGS, Semaphore msgLimiter)
  throws IOException {
    this.sc = sc;
    this.MAX_DATA = MAX_DATA;
    this.dataLimiter = dataLimiter;
//    this.MAX_MSGS = MAX_MSGS;
    this.msgLimiter = msgLimiter;
    recv();
  }

  public boolean canStartRemoteTransaction() {
    return true;
  }
}
