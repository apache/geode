/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.tcp;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SocketChannel;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.distributed.DistributedSystemDisconnectedException;
import com.gemstone.gemfire.distributed.internal.ConflationKey;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DMStats;
import com.gemstone.gemfire.distributed.internal.DirectReplyProcessor;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DistributionConfigImpl;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.DistributionStats;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.distributed.internal.ReplyMessage;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.distributed.internal.ReplySender;
import com.gemstone.gemfire.distributed.internal.direct.DirectChannel;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.MembershipManager;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.ByteArrayDataInput;
import com.gemstone.gemfire.internal.DSFIDFactory;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.SocketCloser;
import com.gemstone.gemfire.internal.SocketCreator;
import com.gemstone.gemfire.internal.SocketUtils;
import com.gemstone.gemfire.internal.SystemTimer;
import com.gemstone.gemfire.internal.SystemTimer.SystemTimerTask;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.LoggingThreadGroup;
import com.gemstone.gemfire.internal.logging.log4j.AlertAppender;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.internal.tcp.MsgReader.Header;
import com.gemstone.gemfire.internal.util.concurrent.ReentrantSemaphore;
import com.gemstone.org.jgroups.util.StringId;

/** <p>Connection is a socket holder that sends and receives serialized
    message objects.  A Connection may be closed to preserve system
    resources and will automatically be reopened when it's needed.</p>

    @author Bruce Schuchardt
    @since 2.0

*/

public class Connection implements Runnable {
  private static final Logger logger = LogService.getLogger();

  private static final int INITIAL_CAPACITY = Integer.getInteger("p2p.readerBufferSize", 32768).intValue();
  private static int P2P_CONNECT_TIMEOUT;
  private static boolean IS_P2P_CONNECT_TIMEOUT_INITIALIZED = false; 
  
  public final static int NORMAL_MSG_TYPE = 0x4c;
  public final static int CHUNKED_MSG_TYPE = 0x4d; // a chunk of one logical msg
  public final static int END_CHUNKED_MSG_TYPE = 0x4e; // last in a series of chunks
  public final static int DIRECT_ACK_BIT = 0x20;
  //We no longer support early ack
  //public final static int EARLY_ACK_BIT = 0x10;

  public static final int MSG_HEADER_SIZE_OFFSET = 0;
  public static final int MSG_HEADER_TYPE_OFFSET = 4;
  public static final int MSG_HEADER_ID_OFFSET = 5;
  public static final int MSG_HEADER_BYTES = 7;

  /**
   * Small buffer used for send socket buffer on receiver connections
   * and receive buffer on sender connections.
   */
  public final static int SMALL_BUFFER_SIZE = Integer.getInteger("gemfire.SMALL_BUFFER_SIZE",4096).intValue();

  /** counter to give connections a unique id */
  private static AtomicLong idCounter = new AtomicLong(1);

  /** the table holding this connection */
  final ConnectionTable owner;
  
  /** Set to false once run() is terminating. Using this instead of Thread.isAlive  
    * as the reader thread may be a pooled thread.
    */ 
  private volatile boolean isRunning = false; 

  /** true if connection is a shared resource that can be used by more than one thread */
  private boolean sharedResource;

  public final boolean isSharedResource() {
    return this.sharedResource;
  }
  
  /** The idle timeout timer task for this connection */
  private SystemTimerTask idleTask;
  
  /**
   * Returns the depth of unshared reader threads from this thread to
   * the original non-reader-thread.
   * E.g., ServerConnection -> reader(domino=1) -> reader(domino=2) -> reader(domino=3)
   */
  public static int getDominoCount() {
    return dominoCount.get().intValue();
  }

  private final static ThreadLocal isReaderThread = new ThreadLocal();
  public final static void makeReaderThread() {
    // mark this thread as a reader thread
    makeReaderThread(true);
  }
  private final static void makeReaderThread(boolean v) {
    isReaderThread.set(v);
  }
  // return true if this thread is a reader thread
  public final static boolean isReaderThread() {
    Object o = isReaderThread.get();
    if (o == null) {
      return false;
    } else {
      return ((Boolean)o).booleanValue();
    }
  }

  private int getP2PConnectTimeout() {
    if(IS_P2P_CONNECT_TIMEOUT_INITIALIZED)
      return P2P_CONNECT_TIMEOUT;
    String connectTimeoutStr = System.getProperty("p2p.connectTimeout");
    if(connectTimeoutStr != null) {
      P2P_CONNECT_TIMEOUT =  Integer.parseInt(connectTimeoutStr);
    }
    else {      
      P2P_CONNECT_TIMEOUT = 6*this.owner.owner.getDM().getConfig().getMemberTimeout();
    }
    IS_P2P_CONNECT_TIMEOUT_INITIALIZED = true;
    return P2P_CONNECT_TIMEOUT;     
  }
  /**
   * If true then readers for thread owned sockets will send all messages on thread owned senders.
   * Even normally unordered msgs get send on TO socks.
   */
  private static final boolean DOMINO_THREAD_OWNED_SOCKETS = Boolean.getBoolean("p2p.ENABLE_DOMINO_THREAD_OWNED_SOCKETS");
  private final static ThreadLocal isDominoThread = new ThreadLocal();
  // return true if this thread is a reader thread
  public final static boolean tipDomino() {
    if (DOMINO_THREAD_OWNED_SOCKETS) {
      // mark this thread as one who wants to send ALL on TO sockets
      ConnectionTable.threadWantsOwnResources();
      isDominoThread.set(Boolean.TRUE);
      return true;
    } else {
      return false;
    }
  }
  public final static boolean isDominoThread() {
    Object o = isDominoThread.get();
    if (o == null) {
      return false;
    } else {
      return ((Boolean)o).booleanValue();
    }
  }

  /** the socket entrusted to this connection */
  private final Socket socket;

  /** the non-NIO output stream */
  OutputStream output;

  /** output stream/channel lock */
  private final Object outLock = new Object();

  /** the ID string of the conduit (for logging) */
  String conduitIdStr;

  /** remoteId identifies the remote conduit's listener.  It does NOT
     identify the "port" that this connection's socket is attached
     to, which is a different thing altogether */
  Stub remoteId;

  /** Identifies the java group member on the other side of the connection. */
  InternalDistributedMember remoteAddr;

  /**
   * Identifies the version of the member on the other side of the connection.
   */
  Version remoteVersion;

  /**
   * True if this connection was accepted by a listening socket.
   * This makes it a receiver.
   * False if this connection was explicitly created by a connect call.
   * This makes it a sender.
   */
  private final boolean isReceiver;
  
  /**
   * count of how many unshared p2p-readers removed from the original action this
   * thread is.  For instance, server-connection -> owned p2p reader (count 0)
   * -> owned p2p reader (count 1) -> owned p2p reader (count 2).  This shows
   * up in thread names as "DOM #x" (domino #x)
   */
  private static ThreadLocal<Integer> dominoCount = new ThreadLocal<Integer>() {
    @Override
    protected Integer initialValue() {
      return 0;
    }};

//  /**
//   * name of sender thread thread.  Useful in finding out why a reader
//   * thread was created.  Add sending of the name in handshakes and
//   * add it to the name of the reader thread (the code is there but commented out)
//   */
//  private String senderName = null;
  
  // If we are a sender then we want to know if the receiver on the
  // other end is willing to have its messages queued. The following
  // four "async" inst vars come from his handshake response.
  /**
   * How long to wait if receiver will not accept a message before we
   * go into queue mode.
   * @since 4.2.2
   */
  private int asyncDistributionTimeout = 0;
  /**
   * How long to wait,
   * with the receiver not accepting any messages,
   * before kicking the receiver out of the distributed system.
   * Ignored if asyncDistributionTimeout is zero.
   * @since 4.2.2
   */
  private int asyncQueueTimeout = 0;
  /**
   * How much queued data we can have,
   * with the receiver not accepting any messages,
   * before kicking the receiver out of the distributed system.
   * Ignored if asyncDistributionTimeout is zero.
   * Canonicalized to bytes (property file has it as megabytes
   * @since 4.2.2
   */
  private long asyncMaxQueueSize = 0;
  /**
   * True if an async queue is already being filled.
   */
  private volatile boolean asyncQueuingInProgress = false;

  /**
   * Maps ConflatedKey instances to ConflatedKey instance.
   * Note that even though the key and value for an entry is the map
   * will always be "equal" they will not always be "==".
   */
  private final Map conflatedKeys = new HashMap();

  //private final Queue outgoingQueue = new LinkedBlockingQueue();


  // NOTE: LinkedBlockingQueue has a bug in which removes from the queue
  // cause future offer to increase the size without adding anything to the queue.
  // So I've changed from this backport class to a java.util.LinkedList
  private final LinkedList outgoingQueue = new LinkedList();

  /**
   * Number of bytes in the outgoingQueue.
   * Used to control capacity.
   */
  private long queuedBytes = 0;

  /** used for async writes */
  Thread pusherThread;

  /**
   * The maximum number of concurrent senders sending a message to a single recipient.
   */
  private final static int MAX_SENDERS = Integer.getInteger("p2p.maxConnectionSenders", DirectChannel.DEFAULT_CONCURRENCY_LEVEL).intValue();
  /**
   * This semaphore is used to throttle how many threads will try to do sends on
   * this connection concurrently. A thread must acquire this semaphore before it
   * is allowed to start serializing its message.
   */
  private final Semaphore senderSem = new ReentrantSemaphore(MAX_SENDERS);

  /** Set to true once the handshake has been read */
  volatile boolean handshakeRead = false;
  volatile boolean handshakeCancelled = false;

  private volatile int replyCode = 0;

  private static final byte REPLY_CODE_OK = (byte)69;
  private static final byte REPLY_CODE_OK_WITH_ASYNC_INFO = (byte)70;

  private final Object handshakeSync = new Object();

  /** message reader thread */
  private volatile Thread readerThread;

//  /**
//   * When a thread owns the outLock and is writing to the socket, it must
//   * be placed in this variable so that it can be interrupted should the
//   * socket need to be closed.
//   */
//  private volatile Thread writerThread;

  /** whether the reader thread is, or should be, running */
  volatile boolean stopped = true;

  /** set to true once a close begins */
  private final AtomicBoolean closing = new AtomicBoolean(false);

  volatile boolean readerShuttingDown = false;

  /** whether the socket is connected */
  volatile boolean connected = false;

  /**
   * Set to true once a connection finishes its constructor
   */
  volatile boolean finishedConnecting = false;

  volatile boolean accessed = true;
  volatile boolean socketInUse = false;
  volatile boolean timedOut = false;
  
  /**
   * task for detecting ack timeouts and issuing alerts
   */
  private SystemTimer.SystemTimerTask ackTimeoutTask;

  // State for ackTimeoutTask: transmissionStartTime, ackWaitTimeout, ackSATimeout, ackConnectionGroup, ackThreadName
  
  /** millisecond clock at the time message transmission started, if doing
   *  forced-disconnect processing */
  long transmissionStartTime;
  
  /** ack wait timeout - if socketInUse, use this to trigger SUSPECT processing */
  private long ackWaitTimeout;

  /** ack severe alert timeout - if socketInUse, use this to send alert */
  private long ackSATimeout;
  
  /**
   * other connections participating in the current transmission.  we notify
   * them if ackSATimeout expires to keep all members from generating alerts
   * when only one is slow 
   */
  List ackConnectionGroup;
  
  /** name of thread that we're currently performing an operation in (may be null) */
  String ackThreadName;
  

  /** the buffer used for NIO message receipt */
  ByteBuffer nioInputBuffer;

  /** the position of the next message's content */
//  int nioMessageStart;

  /** the length of the next message to be dispatched */
  int nioMessageLength;
//  byte nioMessageVersion;

  /** the type of message being received */
  byte nioMessageType;

  /** used to lock access to destreamer data */
  private final Object destreamerLock = new Object();
  /** caches a msg destreamer that is currently not being used */
  MsgDestreamer idleMsgDestreamer;
  /** used to map a msgId to a MsgDestreamer which are
    * used for destreaming chunked messages using nio */
  HashMap destreamerMap;

  boolean directAck;

  short nioMsgId;

  /** whether the length of the next message has been established */
  boolean nioLengthSet = false;

  /** is this connection used for serial message delivery? */
  boolean preserveOrder = false;

  /** number of messages sent on this connection */
  private long messagesSent;

  /** number of messages received on this connection */
  private long messagesReceived;

  /** unique ID of this connection (remote if isReceiver==true) */
  private volatile long uniqueId;

  private int sendBufferSize = -1;
  private int recvBufferSize = -1;
  
  private ReplySender replySender;

  private void setSendBufferSize(Socket sock) {
    setSendBufferSize(sock, this.owner.getConduit().tcpBufferSize);
  }
  private void setReceiveBufferSize(Socket sock) {
    setReceiveBufferSize(sock, this.owner.getConduit().tcpBufferSize);
  }
  private void setSendBufferSize(Socket sock, int requestedSize) {
    setSocketBufferSize(sock, true, requestedSize);
  }
  private void setReceiveBufferSize(Socket sock, int requestedSize) {
    setSocketBufferSize(sock, false, requestedSize);
  }
  public int getReceiveBufferSize() {
    return recvBufferSize;
  }
  private void setSocketBufferSize(Socket sock, boolean send, int requestedSize) {
    setSocketBufferSize(sock, send, requestedSize, false);
  }
  private void setSocketBufferSize(Socket sock, boolean send, int requestedSize, boolean alreadySetInSocket) {
    if (requestedSize > 0) {
      try {
        int currentSize = send
          ? sock.getSendBufferSize()
          : sock.getReceiveBufferSize();
        if (currentSize == requestedSize) {
          if (send) {
            this.sendBufferSize = currentSize;
          }
          return;
        }
        if (!alreadySetInSocket) {
          if (send) {
            sock.setSendBufferSize(requestedSize);
          } else {
            sock.setReceiveBufferSize(requestedSize);
          }
        } else {
        }
      } catch (SocketException ignore) {
      }
      try {
        int actualSize = send
          ? sock.getSendBufferSize()
          : sock.getReceiveBufferSize();
        if (send) {
          this.sendBufferSize = actualSize;
        } else {
          this.recvBufferSize = actualSize;
        }
        if (actualSize < requestedSize) {
          logger.info(LocalizedMessage.create(LocalizedStrings.Connection_SOCKET_0_IS_1_INSTEAD_OF_THE_REQUESTED_2,
              new Object[] {(send ? "send buffer size" : "receive buffer size"), Integer.valueOf(actualSize), Integer.valueOf(requestedSize)}));
        } else if (actualSize > requestedSize) {
          if (logger.isTraceEnabled()) {
            logger.trace("Socket {} buffer size is {} instead of the requested {}",
                (send ? "send" : "receive"), actualSize, requestedSize);
          }
          // Remember the request size which is smaller.
          // This remembered value is used for allocating direct mem buffers.
          if (send) {
            this.sendBufferSize = requestedSize;
          } else {
            this.recvBufferSize = requestedSize;
          }
        }
      } catch (SocketException ignore) {
        if (send) {
          this.sendBufferSize = requestedSize;
        } else {
          this.recvBufferSize = requestedSize;
        }
      }
    }
  }
  /**
   * Returns the size of the send buffer on this connection's socket.
   */
  public int getSendBufferSize() {
    int result = this.sendBufferSize;
    if (result != -1) {
      return result;
    }
    try {
      result = getSocket().getSendBufferSize();
    } catch (SocketException ignore) {
      // just return a default
      result = this.owner.getConduit().tcpBufferSize;
    }
    this.sendBufferSize = result;
    return result;
  }

  /** creates a connection that we accepted (it was initiated by
   * an explicit connect being done on the other side).
   * We will only receive data on this socket; never send.
   */
  protected static Connection createReceiver(ConnectionTable t, Socket s)
    throws IOException, ConnectionException
  {
    Connection c = new Connection(t, s);
    boolean readerStarted = false;
    try {
      c.startReader(t);
      readerStarted = true;
    } finally {
      if (!readerStarted) {
        c.closeForReconnect(LocalizedStrings.Connection_COULD_NOT_START_READER_THREAD.toLocalizedString());
      }
    }
    c.waitForHandshake();
    //sendHandshakeReplyOK();
    c.finishedConnecting = true;
    return c;
  }
  
  /** creates a connection that we accepted (it was initiated by
   * an explicit connect being done on the other side).
   */
  private Connection(ConnectionTable t, Socket s)
    throws IOException, ConnectionException
  {
    if (t == null) {
      throw new IllegalArgumentException(LocalizedStrings.Connection_NULL_CONNECTIONTABLE.toLocalizedString());
    }
    this.isReceiver = true;
    this.owner = t;
    this.socket = s;
    this.conduitIdStr = owner.getConduit().getId().toString();
    this.handshakeRead = false;
    this.handshakeCancelled = false;
    this.connected = true;

    try {
      s.setTcpNoDelay(true);
      s.setKeepAlive(true);
//      s.setSoLinger(true, (Integer.valueOf(System.getProperty("p2p.lingerTime", "5000"))).intValue());
      setSendBufferSize(s, SMALL_BUFFER_SIZE);
      setReceiveBufferSize(s);
    }
    catch (SocketException e) {
      // unable to get the settings we want.  Don't log an error because it will
      // likely happen a lot
    }
    if (!useNIO()) {
      try {
        //this.output = new BufferedOutputStream(s.getOutputStream(), SMALL_BUFFER_SIZE);
        this.output = s.getOutputStream();
      }
      catch (IOException io) {
        logger.fatal(LocalizedMessage.create(LocalizedStrings.Connection_UNABLE_TO_GET_P2P_CONNECTION_STREAMS), io);
        t.getSocketCloser().asyncClose(s, this.remoteAddr.toString(), null);
        throw io;
      }
    }
  }
  
  void setIdleTimeoutTask(SystemTimerTask task) {
    this.idleTask = task;
  }


  /**
   * Returns true if an idle connection was detected.
   */
  public boolean checkForIdleTimeout() {
    if (isSocketClosed()) {
      return true;
    }
    if (isSocketInUse()) {
      return false;
    }
    boolean isIdle = !this.accessed;
    this.accessed = false;
    if (isIdle) {
      this.timedOut = true;
      this.owner.getConduit().stats.incLostLease();
      if (logger.isDebugEnabled()) {
        logger.debug("Closing idle connection {} shared={} ordered={}", this, this.sharedResource, this.preserveOrder);
      }
      try {
        // Instead of calling requestClose
        // we call closeForReconnect.
        // We don't want this timeout close to close
        // any other connections. The problem with
        // requestClose has removeEndpoint set to true
        // which will close an receivers we have if this
        // connection is a shared one.
        closeForReconnect(LocalizedStrings.Connection_IDLE_CONNECTION_TIMED_OUT.toLocalizedString());
      } catch (Exception ignore) {}
    }
    return isIdle;
  }

  static private byte[] okHandshakeBytes;
  static private ByteBuffer okHandshakeBuf;
  static {
    int msglen = 1; // one byte for reply code
    byte[] bytes = new byte[MSG_HEADER_BYTES + msglen];
    msglen = calcHdrSize(msglen);
    bytes[MSG_HEADER_SIZE_OFFSET] = (byte)((msglen/0x1000000) & 0xff);
    bytes[MSG_HEADER_SIZE_OFFSET+1] = (byte)((msglen/0x10000) & 0xff);
    bytes[MSG_HEADER_SIZE_OFFSET+2] = (byte)((msglen/0x100) & 0xff);
    bytes[MSG_HEADER_SIZE_OFFSET+3] = (byte)(msglen & 0xff);
    bytes[MSG_HEADER_TYPE_OFFSET] = (byte)NORMAL_MSG_TYPE; // message type
    bytes[MSG_HEADER_ID_OFFSET] = (byte)((MsgIdGenerator.NO_MSG_ID/0x100) & 0xff);
    bytes[MSG_HEADER_ID_OFFSET+1] = (byte)(MsgIdGenerator.NO_MSG_ID & 0xff);
    bytes[MSG_HEADER_BYTES] = REPLY_CODE_OK;
    int allocSize = bytes.length;
    ByteBuffer bb;
    if (TCPConduit.useDirectBuffers) {
      bb = ByteBuffer.allocateDirect(allocSize);
    } else {
      bb = ByteBuffer.allocate(allocSize);
    }
    bb.put(bytes);
    okHandshakeBuf = bb;
    okHandshakeBytes = bytes;
  }

  /**
   * maximum message buffer size
   */
  public static final int MAX_MSG_SIZE = 0x00ffffff;
  public static int calcHdrSize(int byteSize) {
    if (byteSize > MAX_MSG_SIZE) {
      throw new IllegalStateException(LocalizedStrings.Connection_TCP_MESSAGE_EXCEEDED_MAX_SIZE_OF_0.toLocalizedString(Integer.valueOf(MAX_MSG_SIZE)));
    }
    int hdrSize = byteSize;
    hdrSize |= (HANDSHAKE_VERSION << 24);
    return hdrSize;
  }
  public static int calcMsgByteSize(int hdrSize) {
    return hdrSize & MAX_MSG_SIZE;
  }
  public static byte calcHdrVersion(int hdrSize) throws IOException {
    byte ver = (byte)(hdrSize >> 24);
    if (ver != HANDSHAKE_VERSION) {
      throw new IOException(LocalizedStrings.Connection_DETECTED_WRONG_VERSION_OF_GEMFIRE_PRODUCT_DURING_HANDSHAKE_EXPECTED_0_BUT_FOUND_1.toLocalizedString(new Object[] {new Byte(HANDSHAKE_VERSION), new Byte(ver)}));
    }
    return ver;
  }
  private void sendOKHandshakeReply() throws IOException, ConnectionException {
    byte[] my_okHandshakeBytes = null;
    ByteBuffer my_okHandshakeBuf = null;
    if (this.isReceiver) {
      DistributionConfig cfg = owner.getConduit().config;
      ByteBuffer bb;
      if (useNIO() && TCPConduit.useDirectBuffers) {
        bb = ByteBuffer.allocateDirect(128);
      } else {
        bb = ByteBuffer.allocate(128);
      }
      bb.putInt(0); // reserve first 4 bytes for packet length
      bb.put((byte)NORMAL_MSG_TYPE);
      bb.putShort(MsgIdGenerator.NO_MSG_ID);
      bb.put(REPLY_CODE_OK_WITH_ASYNC_INFO);
      bb.putInt(cfg.getAsyncDistributionTimeout());
      bb.putInt(cfg.getAsyncQueueTimeout());
      bb.putInt(cfg.getAsyncMaxQueueSize());
      // write own product version
      Version.writeOrdinal(bb, Version.CURRENT.ordinal(), true);
      // now set the msg length into position 0
      bb.putInt(0, calcHdrSize(bb.position()-MSG_HEADER_BYTES));
      if (useNIO()) {
        my_okHandshakeBuf = bb;
        bb.flip();
      } else {
        my_okHandshakeBytes = new byte[bb.position()];
        bb.flip();
        bb.get(my_okHandshakeBytes);
      }
    } else {
      my_okHandshakeBuf = okHandshakeBuf;
      my_okHandshakeBytes = okHandshakeBytes;
    }
    if (useNIO()) {
      synchronized (my_okHandshakeBuf) {
        my_okHandshakeBuf.position(0);
        nioWriteFully(getSocket().getChannel(), my_okHandshakeBuf, false, null);
      }
    } else {
      synchronized(outLock) {
        try {
//          this.writerThread = Thread.currentThread();
          this.output.write(my_okHandshakeBytes, 0, my_okHandshakeBytes.length);
          this.output.flush();
        }
        finally {
//          this.writerThread = null;
        }
      }
    }
  }

  private static final int HANDSHAKE_TIMEOUT_MS = Integer.getInteger("p2p.handshakeTimeoutMs", 59000).intValue();
  //private static final byte HANDSHAKE_VERSION = 1; // 501
  //public static final byte HANDSHAKE_VERSION = 2; // cbb5x_PerfScale
  //public static final byte HANDSHAKE_VERSION = 3; // durable_client
  //public static final byte HANDSHAKE_VERSION = 4; // dataSerialMay19
//  public static final byte HANDSHAKE_VERSION = 5; // split-brain bits
  //public static final byte HANDSHAKE_VERSION = 6; // direct ack changes
  // NOTICE: handshake_version should not be changed anymore.  Use the gemfire
  //         version transmitted with the handshake bits and handle old handshakes
  //         based on that
  public static final byte HANDSHAKE_VERSION = 7; // product version exchange during handshake

  /**
   * @throws ConnectionException if the conduit has stopped
   */
  private void waitForHandshake() throws ConnectionException {
    boolean needToClose = false;
    String reason = null;
    try {
    synchronized (this.handshakeSync) {
      if (!this.handshakeRead && !this.handshakeCancelled) {
        boolean success = false;
        reason = LocalizedStrings.Connection_UNKNOWN.toLocalizedString();
        boolean interrupted = Thread.interrupted();
        try {
          final long endTime = System.currentTimeMillis() + HANDSHAKE_TIMEOUT_MS;
          long msToWait = HANDSHAKE_TIMEOUT_MS;
          while (!this.handshakeRead && !this.handshakeCancelled && msToWait > 0) {
            this.handshakeSync.wait(msToWait); // spurious wakeup ok
            if (!this.handshakeRead && !this.handshakeCancelled) {
              msToWait = endTime - System.currentTimeMillis();
            }
          }
          if (!this.handshakeRead && !this.handshakeCancelled) {
            reason = LocalizedStrings.Connection_HANDSHAKE_TIMED_OUT.toLocalizedString();
            String peerName;
            if (this.remoteAddr != null) {
              peerName = this.remoteAddr.toString();
            }
            else {
              peerName = "socket " + this.socket.getRemoteSocketAddress().toString()
                  + ":" + this.socket.getPort();
            }
            throw new ConnectionException(LocalizedStrings.Connection_CONNECTION_HANDSHAKE_WITH_0_TIMED_OUT_AFTER_WAITING_1_MILLISECONDS.toLocalizedString(
                  new Object[] {peerName, Integer.valueOf(HANDSHAKE_TIMEOUT_MS)}));
          } else {
            success = this.handshakeRead;
          }
        } catch (InterruptedException ex) {
          interrupted = true;
          this.owner.getConduit().getCancelCriterion().checkCancelInProgress(ex);
          reason = LocalizedStrings.Connection_INTERRUPTED.toLocalizedString();
        } finally {
          if (interrupted) {
            Thread.currentThread().interrupt();
          }
          if (success) {
            if (this.isReceiver) {
              needToClose = !owner.getConduit().getMembershipManager().addSurpriseMember(this.remoteAddr, this.remoteId);
              if (needToClose) {
                reason = "this member is shunned";
              }
            }
          }
          else {
            needToClose = true; // for bug 42159
          }
        }
      } // !handshakeRead
    } // synchronized

    } finally {
      if (needToClose) {
        // moved this call outside of the sync for bug 42159
        try {
          requestClose(reason); // fix for bug 31546
        } 
        catch (Exception ignore) {
        }
      }
    }
  }

  private void notifyHandshakeWaiter(boolean success) {
    synchronized (this.handshakeSync) {
      if (success) {
        this.handshakeRead = true;
      } else {
        this.handshakeCancelled = true;
      }
      this.handshakeSync.notify();
    }
  }
  
  private final AtomicBoolean asyncCloseCalled = new AtomicBoolean();
  
  /**
   * asynchronously close this connection
   * 
   * @param beingSick
   */
  private void asyncClose(boolean beingSick) {
    // note: remoteId may be null if this is a receiver that hasn't finished its handshake
    
    // we do the close in a background thread because the operation may hang if 
    // there is a problem with the network.  See bug #46659

    // if simulating sickness, sockets must be closed in-line so that tests know
    // that the vm is sick when the beSick operation completes
    if (beingSick) {
      prepareForAsyncClose();
    }
    else {
      if (this.asyncCloseCalled.compareAndSet(false, true)) {
        Socket s = this.socket;
        if (s != null && !s.isClosed()) {
          prepareForAsyncClose();
          this.owner.getSocketCloser().asyncClose(s, String.valueOf(this.remoteAddr), null);
        }
      }
    }
  }
  
  private void prepareForAsyncClose() {
    synchronized(stateLock) {
      if (readerThread != null && isRunning && !readerShuttingDown
          && (connectionState == STATE_READING || connectionState == STATE_READING_ACK)) {
        readerThread.interrupt();
      }
    }
  }

  private static final int CONNECT_HANDSHAKE_SIZE = 4096;

  private void handshakeNio() throws IOException {
    // We jump through some extra hoops to use a MsgOutputStream
    // This keeps us from allocating an extra DirectByteBuffer.
    InternalDistributedMember myAddr = this.owner.getConduit().getLocalAddress();
    synchronized (myAddr) {
      while (myAddr.getIpAddress() == null) {
        try {
          myAddr.wait(); // spurious wakeup ok
        }
        catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          this.owner.getConduit().getCancelCriterion().checkCancelInProgress(ie);
        }
      }
    }

    Assert.assertTrue(myAddr.getDirectChannelPort() == this.owner.getConduit().getPort());

    final MsgOutputStream connectHandshake
      = new MsgOutputStream(CONNECT_HANDSHAKE_SIZE);
    //connectHandshake.reset();
    /**
     * Note a byte of zero is always written because old products
     * serialized a member id with always sends an ip address.
     * My reading of the ip-address specs indicated that the first byte
     * of a valid address would never be 0.
     */
    connectHandshake.writeByte(0);
    connectHandshake.writeByte(HANDSHAKE_VERSION);
    // NOTE: if you add or remove code in this section bump HANDSHAKE_VERSION
    InternalDataSerializer.invokeToData(myAddr, connectHandshake);
    connectHandshake.writeBoolean(this.sharedResource);
    connectHandshake.writeBoolean(this.preserveOrder);
    connectHandshake.writeLong(this.uniqueId);
    // write the product version ordinal
    Version.CURRENT.writeOrdinal(connectHandshake, true);
    connectHandshake.writeInt(dominoCount.get()+1);
// this writes the sending member + thread name that is stored in senderName
// on the receiver to show the cause of reader thread creation
//    if (dominoCount.get() > 0) {
//      connectHandshake.writeUTF(Thread.currentThread().getName());
//    } else {
//      String name = owner.getDM().getConfig().getName();
//      if (name == null) {
//        name = "pid="+OSProcess.getId();
//      }
//      connectHandshake.writeUTF("["+name+"] "+Thread.currentThread().getName());
//    }
    connectHandshake.setMessageHeader(NORMAL_MSG_TYPE, DistributionManager.STANDARD_EXECUTOR, MsgIdGenerator.NO_MSG_ID);
    nioWriteFully(getSocket().getChannel(), connectHandshake.getContentBuffer(), false, null);
  }

  private void handshakeStream() throws IOException {
    //this.output = new BufferedOutputStream(getSocket().getOutputStream(), owner.getConduit().bufferSize);
    this.output = getSocket().getOutputStream();
    ByteArrayOutputStream baos = new ByteArrayOutputStream(CONNECT_HANDSHAKE_SIZE);
    DataOutputStream os = new DataOutputStream(baos);
    InternalDistributedMember myAddr = owner.getConduit().getLocalAddress();
    os.writeByte(0);
    os.writeByte(HANDSHAKE_VERSION);
    // NOTE: if you add or remove code in this section bump HANDSHAKE_VERSION
    InternalDataSerializer.invokeToData(myAddr, os);
    os.writeBoolean(this.sharedResource);
    os.writeBoolean(this.preserveOrder);
    os.writeLong(this.uniqueId);
    Version.CURRENT.writeOrdinal(os, true);
    os.writeInt(dominoCount.get()+1);
 // this writes the sending member + thread name that is stored in senderName
 // on the receiver to show the cause of reader thread creation
//    if (dominoCount.get() > 0) {
//      os.writeUTF(Thread.currentThread().getName());
//    } else {
//      String name = owner.getDM().getConfig().getName();
//      if (name == null) {
//        name = "pid="+OSProcess.getId();
//      }
//      os.writeUTF("["+name+"] "+Thread.currentThread().getName());
//    }
    os.flush();

    byte[] msg = baos.toByteArray();
    int len = calcHdrSize(msg.length);
    byte[] lenbytes = new byte[MSG_HEADER_BYTES];
    lenbytes[MSG_HEADER_SIZE_OFFSET] = (byte)((len/0x1000000) & 0xff);
    lenbytes[MSG_HEADER_SIZE_OFFSET+1] = (byte)((len/0x10000) & 0xff);
    lenbytes[MSG_HEADER_SIZE_OFFSET+2] = (byte)((len/0x100) & 0xff);
    lenbytes[MSG_HEADER_SIZE_OFFSET+3] = (byte)(len & 0xff);
    lenbytes[MSG_HEADER_TYPE_OFFSET] = (byte)NORMAL_MSG_TYPE;
    lenbytes[MSG_HEADER_ID_OFFSET] = (byte)((MsgIdGenerator.NO_MSG_ID/0x100) & 0xff);
    lenbytes[MSG_HEADER_ID_OFFSET+1] = (byte)(MsgIdGenerator.NO_MSG_ID & 0xff);
    synchronized(outLock) {
      try {
//        this.writerThread = Thread.currentThread();
        this.output.write(lenbytes, 0, lenbytes.length);
        this.output.write(msg, 0, msg.length);
        this.output.flush();
      }
      finally {
//        this.writerThread = null;
      }
    }
  }

  /**
   *
   * @throws IOException if handshake fails
   */
  private void attemptHandshake(ConnectionTable connTable) throws IOException {
    // send HANDSHAKE
    // send this server's port.  It's expected on the other side
    if (useNIO()) {
      handshakeNio();
    }
    else {
      handshakeStream();
    }

    startReader(connTable); // this reader only reads the handshake and then exits
    waitForHandshake(); // waiting for reply
  }

  /** time between connection attempts*/
  private static final int RECONNECT_WAIT_TIME
    = Integer.getInteger("gemfire.RECONNECT_WAIT_TIME", 2000).intValue();

  /** creates a new connection to a remote server.
   *  We are initiating this connection; the other side must accept us
   *  We will almost always send messages; small acks are received.
   */
  protected static Connection createSender(final MembershipManager mgr,
                                           final ConnectionTable t,
                                           final boolean preserveOrder,
                                           final Stub key,
                                           final InternalDistributedMember remoteAddr,
                                           final boolean sharedResource,
                                           final long startTime,
                                           final long ackTimeout,
                                           final long ackSATimeout)
    throws IOException, DistributedSystemDisconnectedException
  {
    boolean warningPrinted = false;
    boolean success = false;
    boolean firstTime = true;
    Connection conn = null;
    // keep trying.  Note that this may be executing during the shutdown window
    // where a cancel criterion has not been established, but threads are being
    // interrupted.  In this case we must allow the connection to succeed even
    // though subsequent messaging using the socket may fail
    boolean interrupted = Thread.interrupted();
    boolean severeAlertIssued = false;
    boolean suspected = false;
    long reconnectWaitTime = RECONNECT_WAIT_TIME;
    boolean connectionErrorLogged = false;
    try {
      while (!success) { // keep trying
        // Quit if DM has stopped distribution
        t.getConduit().getCancelCriterion().checkCancelInProgress(null);
        long now = System.currentTimeMillis();
        if (!severeAlertIssued && ackSATimeout > 0  &&  startTime + ackTimeout < now) {
          if (startTime + ackTimeout + ackSATimeout < now) {
            if (remoteAddr != null) {
              logger.fatal(LocalizedMessage.create(
                  LocalizedStrings.Connection_UNABLE_TO_FORM_A_TCPIP_CONNECTION_TO_0_IN_OVER_1_SECONDS, new Object[] { remoteAddr,
                  (ackSATimeout + ackTimeout) / 1000 }));
            }
            severeAlertIssued = true;
          }
          else if (!suspected) {
            if (remoteAddr != null) {
              logger.warn(LocalizedMessage.create(
                  LocalizedStrings.Connection_UNABLE_TO_FORM_A_TCPIP_CONNECTION_TO_0_IN_OVER_1_SECONDS,
                  new Object[] { remoteAddr, (ackTimeout)/1000 }));
            }
            mgr.suspectMember(remoteAddr, LocalizedStrings.Connection_UNABLE_TO_FORM_A_TCPIP_CONNECTION_IN_A_REASONABLE_AMOUNT_OF_TIME.toLocalizedString());
            suspected = true;
          }
          reconnectWaitTime = Math.min(RECONNECT_WAIT_TIME,
              ackSATimeout - (now - startTime - ackTimeout));
          if (reconnectWaitTime <= 0) {
            reconnectWaitTime = RECONNECT_WAIT_TIME;
          }
        } else if (!suspected && (startTime > 0) && (ackTimeout > 0)
            && (startTime + ackTimeout < now)) {
          mgr.suspectMember(remoteAddr, LocalizedStrings.Connection_UNABLE_TO_FORM_A_TCPIP_CONNECTION_IN_A_REASONABLE_AMOUNT_OF_TIME.toLocalizedString());
          suspected = true;
        }
        if (firstTime) {
          firstTime = false;
          InternalDistributedMember m = mgr.getMemberForStub(key, true);
          if (m == null) {
            throw new IOException("Member for stub " + key + " left the group");
          }
        }
        else {
          // if we're sending an alert and can't connect, bail out.  A sick
          // alert listener should not prevent cache operations from continuing
          if (AlertAppender.isThreadAlerting()) {
            // do not change the text of this exception - it is looked for in exception handlers
            throw new IOException("Cannot form connection to alert listener " + key);
          }
            
          // Wait briefly...
          interrupted = Thread.interrupted() || interrupted;
          try {
            Thread.sleep(reconnectWaitTime);
          }
          catch (InterruptedException ie) {
            interrupted = true;
            t.getConduit().getCancelCriterion().checkCancelInProgress(ie);
          }
          t.getConduit().getCancelCriterion().checkCancelInProgress(null);
          InternalDistributedMember m = mgr.getMemberForStub(key, true);
          if (m == null) {
            throw new IOException(LocalizedStrings.Connection_MEMBER_FOR_STUB_0_LEFT_THE_GROUP.toLocalizedString(key));
          }
          if (!warningPrinted) {
            warningPrinted = true;
            logger.warn(LocalizedMessage.create(LocalizedStrings.Connection_CONNECTION_ATTEMPTING_RECONNECT_TO_PEER__0, m));
          }          
          t.getConduit().stats.incReconnectAttempts();
        }
        //create connection
        try {
          conn = null;
          conn = new Connection(mgr, t, preserveOrder, key, remoteAddr, sharedResource);
        }
        catch (javax.net.ssl.SSLHandshakeException se) {
          // no need to retry if certificates were rejected
          throw se;
        }
        catch (IOException ioe) {
          // Only give up if the member leaves the view.
          InternalDistributedMember m = mgr.getMemberForStub(key, true);
          if (m == null) {
            throw ioe;
          }
          t.getConduit().getCancelCriterion().checkCancelInProgress(null);
          if ("Too many open files".equals(ioe.getMessage())) {
            t.fileDescriptorsExhausted();
          }
          else if (!connectionErrorLogged) {
            connectionErrorLogged = true; // otherwise change to use 100ms intervals causes a lot of these
            logger.info(LocalizedMessage.create(
                LocalizedStrings.Connection_CONNECTION_FAILED_TO_CONNECT_TO_PEER_0_BECAUSE_1,
                new Object[] {sharedResource, preserveOrder, m, ioe}));
          }
        } // IOException
        finally {
          if (conn == null) {
            t.getConduit().stats.incFailedConnect();
          }
        }
        if (conn != null) {
          // handshake
          try {
            conn.attemptHandshake(t);
            if (conn.isSocketClosed()) {
              // something went wrong while reading the handshake
              // and the socket was closed or this guy sent us a
              // ShutdownMessage
              InternalDistributedMember m = mgr.getMemberForStub(key, true);
              if (m == null) {
                throw new IOException(LocalizedStrings.Connection_MEMBER_FOR_STUB_0_LEFT_THE_GROUP.toLocalizedString(key));
              }
              t.getConduit().getCancelCriterion().checkCancelInProgress(null);
              // no success but no need to log; just retry
            }
            else {
              success = true;
            }
          }
          catch (DistributedSystemDisconnectedException e) {
            throw e;
          }
          catch (ConnectionException e) {
            InternalDistributedMember m = mgr.getMemberForStub(key, true);
            if (m == null) {
              IOException ioe = new IOException(LocalizedStrings.Connection_HANDSHAKE_FAILED.toLocalizedString());
              ioe.initCause(e);
              throw ioe;
            }
            t.getConduit().getCancelCriterion().checkCancelInProgress(null);
            logger.info(LocalizedMessage.create(
                LocalizedStrings.Connection_CONNECTION_HANDSHAKE_FAILED_TO_CONNECT_TO_PEER_0_BECAUSE_1,
                new Object[] {sharedResource, preserveOrder, m,e}));
          }
          catch (IOException e) {
            InternalDistributedMember m = mgr.getMemberForStub(key, true);
            if (m == null) {
              throw e;
            }
            t.getConduit().getCancelCriterion().checkCancelInProgress(null);
            logger.info(LocalizedMessage.create(
                LocalizedStrings.Connection_CONNECTION_HANDSHAKE_FAILED_TO_CONNECT_TO_PEER_0_BECAUSE_1,
                new Object[] {sharedResource, preserveOrder, m,e}));
            if (!sharedResource && "Too many open files".equals(e.getMessage())) {
              t.fileDescriptorsExhausted();
            }
          }
          finally {
            if (!success) {
              try {
                conn.requestClose(LocalizedStrings.Connection_FAILED_HANDSHAKE.toLocalizedString()); 
              } catch (Exception ignore) {}
              conn = null;
            }
          }
        }
      } // while
      if (warningPrinted) {
        logger.info(LocalizedMessage.create(
            LocalizedStrings.Connection_0_SUCCESSFULLY_REESTABLISHED_CONNECTION_TO_PEER_1,
            new Object[] {mgr.getLocalMember(), remoteAddr}));
      }
    }
    finally {
      try {
        if (!success) {
          if (conn != null) {
            conn.requestClose(LocalizedStrings.Connection_FAILED_CONSTRUCTION.toLocalizedString());
            conn = null;
          }
        }
      }
      finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    }
    //Assert.assertTrue(conn != null);
    if (conn == null) {
      throw new ConnectionException(
        LocalizedStrings.Connection_CONNECTION_FAILED_CONSTRUCTION_FOR_PEER_0
          .toLocalizedString(mgr.getMemberForStub(key, true)));
    }
    if (preserveOrder && BATCH_SENDS) {
      conn.createBatchSendBuffer();
    }
    conn.finishedConnecting = true;
    return conn;
  }

  private void setRemoteAddr(InternalDistributedMember m, Stub stub) {
    this.remoteAddr = this.owner.getDM().getCanonicalId(m);
    this.remoteId = stub;
    MembershipManager mgr = this.owner.owner.getMembershipManager();
    mgr.addSurpriseMember(m, stub);
  }
  
  /** creates a new connection to a remote server.
   *  We are initiating this connection; the other side must accept us
   *  We will almost always send messages; small acks are received.
   */
  private Connection(MembershipManager mgr,
                     ConnectionTable t,
                     boolean preserveOrder,
                     Stub key,
                     InternalDistributedMember remoteAddr,
                     boolean sharedResource)
    throws IOException, DistributedSystemDisconnectedException
  {    
    if (t == null) {
      throw new IllegalArgumentException(LocalizedStrings.Connection_CONNECTIONTABLE_IS_NULL.toLocalizedString());
    }
    this.isReceiver = false;
    this.owner = t;
    this.sharedResource = sharedResource;
    this.preserveOrder = preserveOrder;
    setRemoteAddr(remoteAddr, key);
    this.conduitIdStr = this.owner.getConduit().getId().toString();
    this.handshakeRead = false;
    this.handshakeCancelled = false;
    this.connected = true;

    this.uniqueId = idCounter.getAndIncrement();

    // connect to listening socket

    InetSocketAddress addr = new InetSocketAddress(remoteId.getInetAddress(), remoteId.getPort());
    if (useNIO()) {
      SocketChannel channel = SocketChannel.open();
      this.owner.addConnectingSocket(channel.socket(), addr.getAddress());
      try {
        channel.socket().setTcpNoDelay(true);

        channel.socket().setKeepAlive(SocketCreator.ENABLE_TCP_KEEP_ALIVE);
  
        /** If conserve-sockets is false, the socket can be used for receiving responses,
         * so set the receive buffer accordingly.
         */
        if(!sharedResource) {
          setReceiveBufferSize(channel.socket(), this.owner.getConduit().tcpBufferSize);
        } 
        else {
          setReceiveBufferSize(channel.socket(), SMALL_BUFFER_SIZE); // make small since only receive ack messages
        }
        setSendBufferSize(channel.socket());
        channel.configureBlocking(true);

        int connectTime = getP2PConnectTimeout();; 

        try {
          SocketUtils.connect(channel.socket(), addr, connectTime);
        } catch (NullPointerException e) {
          // bug #45044 - jdk 1.7 sometimes throws an NPE here
          ConnectException c = new ConnectException("Encountered bug #45044 - retrying");
          c.initCause(e);
          // prevent a hot loop by sleeping a little bit
          try {
            Thread.sleep(1000);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
          }
          throw c;
        } catch (CancelledKeyException e) {
          // bug #44469: for some reason NIO throws this runtime exception
          //             instead of an IOException on timeouts
          ConnectException c = new ConnectException(LocalizedStrings.Connection_ATTEMPT_TO_CONNECT_TIMED_OUT_AFTER_0_MILLISECONDS
              .toLocalizedString(new Object[]{connectTime}));
          c.initCause(e);
          throw c;
        } catch (ClosedSelectorException e) {
          // bug #44808: for some reason JRockit NIO thorws this runtime exception
          //             instead of an IOException on timeouts
          ConnectException c = new ConnectException(LocalizedStrings.Connection_ATTEMPT_TO_CONNECT_TIMED_OUT_AFTER_0_MILLISECONDS
              .toLocalizedString(new Object[]{connectTime}));
          c.initCause(e);
          throw c;
        }
      }
      finally {
        this.owner.removeConnectingSocket(channel.socket());
      }
      this.socket = channel.socket();
    }
    else {
      if (TCPConduit.useSSL) {
        // socket = javax.net.ssl.SSLSocketFactory.getDefault()
        //  .createSocket(remoteId.getInetAddress(), remoteId.getPort());
        int socketBufferSize = sharedResource ? SMALL_BUFFER_SIZE : this.owner.getConduit().tcpBufferSize;
        this.socket = SocketCreator.getDefaultInstance().connectForServer( remoteId.getInetAddress(), remoteId.getPort(), socketBufferSize );
        // Set the receive buffer size local fields. It has already been set in the socket.
        setSocketBufferSize(this.socket, false, socketBufferSize, true);
        setSendBufferSize(this.socket);
      }
      else {
        //socket = new Socket(remoteId.getInetAddress(), remoteId.getPort());
        Socket s = new Socket();
        this.socket = s;
        s.setTcpNoDelay(true);
        s.setKeepAlive(SocketCreator.ENABLE_TCP_KEEP_ALIVE);
        setReceiveBufferSize(s, SMALL_BUFFER_SIZE);
        setSendBufferSize(s);
        SocketUtils.connect(s, addr, 0);
      }
    }
    if (logger.isDebugEnabled()) {
      logger.debug("Connection: connected to {} with stub {}", remoteAddr, addr);
    }
    try { getSocket().setTcpNoDelay(true); } catch (SocketException e) {  }
  }

  /**
   * Batch sends currently should not be turned on because:
   *  1. They will be used for all sends (instead of just no-ack)
   *     and thus will break messages that wait for a response (or kill perf).
   *  2. The buffer is not properly flushed and closed on shutdown.
   *     The code attempts to do this but must not be doing it  correctly.
   */
  private static final boolean BATCH_SENDS = Boolean.getBoolean("p2p.batchSends");
  protected static final int BATCH_BUFFER_SIZE = Integer.getInteger("p2p.batchBufferSize", 1024*1024).intValue();
  protected static final int BATCH_FLUSH_MS = Integer.getInteger("p2p.batchFlushTime", 50).intValue();
  protected Object batchLock;
  protected ByteBuffer fillBatchBuffer;
  protected ByteBuffer sendBatchBuffer;
  private BatchBufferFlusher batchFlusher;

  private void createBatchSendBuffer() {
    // batch send buffer isn't needed if old-io is being used
    if (!this.useNIO) {
      return;
    }
    this.batchLock = new Object();
    if (TCPConduit.useDirectBuffers) {
      this.fillBatchBuffer = ByteBuffer.allocateDirect(BATCH_BUFFER_SIZE);
      this.sendBatchBuffer = ByteBuffer.allocateDirect(BATCH_BUFFER_SIZE);
    } else {
      this.fillBatchBuffer = ByteBuffer.allocate(BATCH_BUFFER_SIZE);
      this.sendBatchBuffer = ByteBuffer.allocate(BATCH_BUFFER_SIZE);
    }
    this.batchFlusher = new BatchBufferFlusher();
    this.batchFlusher.start();
  }

  private class BatchBufferFlusher extends Thread  {
    private volatile boolean flushNeeded = false;
    private volatile boolean timeToStop = false;
    private DMStats stats;


    public BatchBufferFlusher() {
      setDaemon(true);
      this.stats = owner.getConduit().stats;
    }
    /**
     * Called when a message writer needs the current fillBatchBuffer flushed
     */
    public void flushBuffer(ByteBuffer bb) {
      final long start = DistributionStats.getStatTime();
      try {
        synchronized (this) {
          synchronized (batchLock) {
            if (bb != fillBatchBuffer) {
              // it must have already been flushed. So just return
              // and use the new fillBatchBuffer
              return;
            }
          }
          this.flushNeeded = true;
          this.notify();
        }
        synchronized (batchLock) {
          // Wait for the flusher thread
          while (bb == fillBatchBuffer) {
            Connection.this.owner.getConduit().getCancelCriterion().checkCancelInProgress(null);
            boolean interrupted = Thread.interrupted();
            try {
              batchLock.wait();  // spurious wakeup ok
            } 
            catch (InterruptedException ex) {
              interrupted = true;
            }
            finally {
              if (interrupted) {
                Thread.currentThread().interrupt();
              }
            }
          } // while
        }
      } finally {
        owner.getConduit().stats.incBatchWaitTime(start);
      }
    }

    public void close() {
      synchronized (this) {
        this.timeToStop = true;
        this.flushNeeded = true;
        this.notify();
      }
    }

    @Override
    public void run() {
      try {
        synchronized (this) {
          while (!timeToStop) {
            if (!this.flushNeeded && fillBatchBuffer.position() <= (BATCH_BUFFER_SIZE/2)) {
              wait(BATCH_FLUSH_MS); // spurious wakeup ok
            }
            if (this.flushNeeded || fillBatchBuffer.position() > (BATCH_BUFFER_SIZE/2)) {
              final long start = DistributionStats.getStatTime();
              synchronized (batchLock) {
                // This is the only block of code that will swap
                // the buffer references
                this.flushNeeded = false;
                ByteBuffer tmp = fillBatchBuffer;
                fillBatchBuffer = sendBatchBuffer;
                sendBatchBuffer = tmp;
                batchLock.notifyAll();
              }
              // We now own the sendBatchBuffer
              if (sendBatchBuffer.position() > 0) {
                final boolean origSocketInUse = socketInUse;
                socketInUse = true;
                try {
                  sendBatchBuffer.flip();
                  SocketChannel channel = getSocket().getChannel();
                  nioWriteFully(channel, sendBatchBuffer, false, null);
                  sendBatchBuffer.clear();
                } catch (IOException ex) {
                  logger.fatal(LocalizedMessage.create(LocalizedStrings.Connection_EXCEPTION_FLUSHING_BATCH_SEND_BUFFER_0,ex));
                  readerShuttingDown = true;
                  requestClose(LocalizedStrings.Connection_EXCEPTION_FLUSHING_BATCH_SEND_BUFFER_0.toLocalizedString(ex));
                } catch (ConnectionException ex) {
                  logger.fatal(LocalizedMessage.create(LocalizedStrings.Connection_EXCEPTION_FLUSHING_BATCH_SEND_BUFFER_0,ex));
                  readerShuttingDown = true;
                  requestClose(LocalizedStrings.Connection_EXCEPTION_FLUSHING_BATCH_SEND_BUFFER_0.toLocalizedString(ex));
                } finally {
                  accessed();
                  socketInUse = origSocketInUse;
                }
              }
              this.stats.incBatchFlushTime(start);
            }
          }
        }
      } catch (InterruptedException ex) {
        // time for this thread to shutdown
//        Thread.currentThread().interrupt();
      }
    }
  }

  private void closeBatchBuffer() {
    if (this.batchFlusher != null) {
      this.batchFlusher.close();
    }
  }

  /** use to test message prep overhead (no socket write).
   * WARNING: turning this on completely disables distribution of batched sends
   */
  private static final boolean SOCKET_WRITE_DISABLED = Boolean.getBoolean("p2p.disableSocketWrite");

  private void batchSend(ByteBuffer src) throws IOException {
    if (SOCKET_WRITE_DISABLED) {
      return;
    }
    final long start = DistributionStats.getStatTime();
    try {
      ByteBuffer dst = null;
      Assert.assertTrue(src.remaining() <= BATCH_BUFFER_SIZE , "Message size(" + src.remaining() + ") exceeded BATCH_BUFFER_SIZE(" + BATCH_BUFFER_SIZE + ")");
      do {
        synchronized (this.batchLock) {
          dst = this.fillBatchBuffer;
          if (src.remaining() <= dst.remaining()) {
            final long copyStart = DistributionStats.getStatTime();
            dst.put(src);
            this.owner.getConduit().stats.incBatchCopyTime(copyStart);
            return;
          }
        }
        // If we got this far then we do not have room in the current
        // buffer and need the flusher thread to flush before we can fill it
        this.batchFlusher.flushBuffer(dst);
      } while (true);
    } finally {
      this.owner.getConduit().stats.incBatchSendTime(start);
    }
  }

  /**
   * Request that the manager close this connection, or close it
   * forcibly if there is no manager.  Invoking this method ensures
   * that the proper synchronization is done.
   */
  void requestClose(String reason) {
    close(reason, true, true, false, false);
  }

  boolean isClosing() {
    return this.closing.get();
  }

  /**
   * Used to close a connection that has not yet been registered
   * with the distribution manager.
   */
  void closePartialConnect(String reason) {
    close(reason, false, false, false, false);
  }
  
  void closePartialConnect(String reason, boolean beingSick) {
    close(reason, false, false, beingSick, false);
  }

  void closeForReconnect(String reason) {
    close(reason, true, false, false, false);
  }

  void closeOldConnection(String reason) {
    close(reason, true, true, false, true);
  }

  /**
   * Closes the connection.
   *
   * @see #requestClose
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="TLW_TWO_LOCK_WAIT")
  private void close(String reason, boolean cleanupEndpoint, 
      boolean p_removeEndpoint, boolean beingSick, boolean forceRemoval) {
    boolean removeEndpoint = p_removeEndpoint;
    // use getAndSet outside sync on this to fix 42330
    boolean onlyCleanup = this.closing.getAndSet(true);
    if (onlyCleanup && !forceRemoval) {
      return;
    }
    if (!onlyCleanup) {
    synchronized (this) {
      this.stopped = true;
      if (this.connected) {
        if (this.asyncQueuingInProgress
            && this.pusherThread != Thread.currentThread()) {
          // We don't need to do this if we are the pusher thread
          // and we have determined that we need to close the connection.
          // See bug 37601.
          synchronized (this.outgoingQueue) {
            // wait for the flusher to complete (it may timeout)
            while (this.asyncQueuingInProgress) {
              // Don't do this: causes closes to not get done in the event
              // of an orderly shutdown:
  //            this.owner.getConduit().getCancelCriterion().checkCancelInProgress(null);
              boolean interrupted = Thread.interrupted();
              try {
                this.outgoingQueue.wait(); // spurious wakeup ok
              } catch (InterruptedException ie) {
                interrupted = true;
  //                this.owner.getConduit().getCancelCriterion().checkCancelInProgress(ie);
              }
              finally {
                if (interrupted) Thread.currentThread().interrupt();
              }
            } // while
          } // synchronized
        }
        this.connected = false;
        closeSenderSem();
        {
          final DMStats stats = this.owner.getConduit().stats;
          if (this.finishedConnecting) {
            if (this.isReceiver) {
              stats.decReceivers();
            } else {
              stats.decSenders(this.sharedResource, this.preserveOrder);
            }
          }
        }
        if (logger.isDebugEnabled()) {
          logger.debug("Closing socket for {}", this);
        }
      }
        else if (!forceRemoval) {
        removeEndpoint = false;
      }
      // make sure our socket is closed
      asyncClose(false);
      nioLengthSet = false;
    } // synchronized
    
      // moved the call to notifyHandshakeWaiter out of the above
      // synchronized block to fix bug #42159
      // Make sure anyone waiting for a handshake stops waiting
      notifyHandshakeWaiter(false);
    // wait a bit for the our reader thread to exit
    // don't wait if we are the reader thread
    boolean isIBM = false;
    // if network partition detection is enabled or this is an admin vm
    // we can't wait for the reader thread when running in an IBM JRE.  See
    // bug 41889
    if (this.owner.owner.config.getEnableNetworkPartitionDetection() ||
        this.owner.owner.getLocalId().getVmKind() == DistributionManager.ADMIN_ONLY_DM_TYPE ||
        this.owner.owner.getLocalId().getVmKind() == DistributionManager.LOCATOR_DM_TYPE) {
      isIBM = "IBM Corporation".equals(System.getProperty("java.vm.vendor"));
    }
    {
      // Now that readerThread is returned to a pool after we close
      // we need to be more careful not to join on a thread that belongs
      // to someone else.
      Thread readerThreadSnapshot = this.readerThread;
      if (!beingSick && readerThreadSnapshot != null && !isIBM
          && this.isRunning && !this.readerShuttingDown
          && readerThreadSnapshot != Thread.currentThread()) {
        try {
          readerThreadSnapshot.join(500);
          readerThreadSnapshot = this.readerThread;
          if (this.isRunning && !this.readerShuttingDown
              && readerThreadSnapshot != null
              && owner.getDM().getRootCause() == null) { // don't wait twice if there's a system failure
            readerThreadSnapshot.join(1500);
            if (this.isRunning) {
              logger.info(LocalizedMessage.create(LocalizedStrings.Connection_TIMED_OUT_WAITING_FOR_READERTHREAD_ON_0_TO_FINISH, this));
            }
          }
        }
        catch (IllegalThreadStateException ignore) {
          // ignored - thread already stopped
        }
        catch (InterruptedException ignore) {
          Thread.currentThread().interrupt();
          // but keep going, we're trying to close.
        }
      }
    }

    closeBatchBuffer();
    closeAllMsgDestreamers();
    }
    if (cleanupEndpoint) {
      if (this.isReceiver) {
        this.owner.removeReceiver(this);
      }
      if (removeEndpoint) {
        if (this.sharedResource) {
          if (!this.preserveOrder) {
            // only remove endpoint when shared unordered connection
            // is closed. This is part of the fix for bug 32980.
            if (!this.isReceiver) {
              // Only remove endpoint if sender.
              if (this.finishedConnecting) {
                // only remove endpoint if our constructor finished
                this.owner.removeEndpoint(this.remoteId, reason);
              }
            }
          }
          else {
            this.owner.removeSharedConnection(reason, this.remoteId, this.preserveOrder, this);
          }
        }
        else if (!this.isReceiver) {
          this.owner.removeThreadConnection(this.remoteId, this);
        }
      }
      else {
        // This code is ok to do even if the ConnectionTable
        // has never added this Connection to its maps since
        // the calls in this block use our identity to do the removes.
        if (this.sharedResource) {
          this.owner.removeSharedConnection(reason, this.remoteId, this.preserveOrder, this);
        }
        else if (!this.isReceiver) {
          this.owner.removeThreadConnection(this.remoteId, this);
        }
      }
    }
    
    //This cancels the idle timer task, but it also removes the tasks
    //reference to this connection, freeing up the connection (and it's buffers
    //for GC sooner.
    if(idleTask != null) {
      idleTask.cancel();
    }
    
    if(ackTimeoutTask!=null){
        ackTimeoutTask.cancel();
    }

  }

  /** starts a reader thread */
  private void startReader(ConnectionTable connTable) { 
    Assert.assertTrue(!this.isRunning); 
    stopped = false; 
    this.isRunning = true; 
    connTable.executeCommand(this);  
  } 


  /** in order to read non-NIO socket-based messages we need to have a thread
      actively trying to grab bytes out of the sockets input queue.
      This is that thread. */
  public void run() {
    this.readerThread = Thread.currentThread();
    this.readerThread.setName(p2pReaderName());
    ConnectionTable.threadWantsSharedResources();
    makeReaderThread(this.isReceiver);
    try {
      if (useNIO()) {
        runNioReader();
      } else {
        runOioReader();
      }
    } finally {
      // bug36060: do the socket close within a finally block
      if (logger.isDebugEnabled()) {
        logger.debug("Stopping {} for {}", p2pReaderName(), remoteId);
      }
      if (this.isReceiver) {
        if (!this.sharedResource) {
          this.owner.owner.stats.incThreadOwnedReceivers(-1L, dominoCount.get());
        }
        asyncClose(false);
        this.owner.removeAndCloseThreadOwnedSockets();
      }
      ByteBuffer tmp = this.nioInputBuffer;
      if(tmp != null) {
        this.nioInputBuffer = null;
        final DMStats stats = this.owner.getConduit().stats;
        Buffers.releaseReceiveBuffer(tmp, stats);
      }
      // make sure that if the reader thread exits we notify a thread waiting
      // for the handshake.
      // see bug 37524 for an example of listeners hung in waitForHandshake
      notifyHandshakeWaiter(false);
      this.readerThread.setName("unused p2p reader");
      synchronized (this.stateLock) {
        this.isRunning = false;
        this.readerThread = null;
      }
    } // finally
  }

  private String p2pReaderName() {
    StringBuffer sb = new StringBuffer(64);
    if (this.isReceiver) {
      sb.append("P2P message reader@");
    } else {
      sb.append("P2P handshake reader@");
    }
    sb.append(Integer.toHexString(System.identityHashCode(this)));
    if (!this.isReceiver) {
      sb.append('-')
        .append(getUniqueId());
    }
    return sb.toString();
  }

  private void runNioReader() {
    // take a snapshot of uniqueId to detect reconnect attempts; see bug 37592
    SocketChannel channel = null;
    try {
      channel = getSocket().getChannel();
      channel.configureBlocking(true);
    } catch (ClosedChannelException e) {
      // bug 37693: the channel was asynchronously closed.  Our work
      // is done.
      try { 
        requestClose(LocalizedStrings.Connection_RUNNIOREADER_CAUGHT_CLOSED_CHANNEL.toLocalizedString()); 
      } catch (Exception ignore) {}      
      return; // exit loop and thread
    } catch (IOException ex) {
      if (stopped || owner.getConduit().getCancelCriterion().cancelInProgress() != null) {
        try { 
          requestClose(LocalizedStrings.Connection_RUNNIOREADER_CAUGHT_SHUTDOWN.toLocalizedString());
        } catch (Exception ignore) {}
        return; // bug37520: exit loop (and thread)
      }
      logger.fatal(LocalizedMessage.create(LocalizedStrings.Connection_FAILED_SETTING_CHANNEL_TO_BLOCKING_MODE_0, ex));
      this.readerShuttingDown = true;
      try { requestClose(LocalizedStrings.Connection_FAILED_SETTING_CHANNEL_TO_BLOCKING_MODE_0.toLocalizedString(ex)); } catch (Exception ignore) {}
      return;
    }

    if (!stopped) {
//      Assert.assertTrue(owner != null, "How did owner become null");
      if (logger.isDebugEnabled()) {
        logger.debug("Starting {}", p2pReaderName());
      }
    }
    // we should not change the state of the connection if we are a handshake reader thread
    // as there is a race between this thread and the application thread doing direct ack
    // fix for #40869
    boolean isHandShakeReader = false;
    try {
      for (;;) {
        if (stopped) {
          break;
        }
        if (SystemFailure.getFailure() != null) {
          // Allocate no objects here!
          Socket s = this.socket;
          if (s != null) {
            try {
              s.close();
            }
            catch (IOException e) {
              // don't care
            }
          }
          SystemFailure.checkFailure(); // throws
        }
        if (this.owner.getConduit().getCancelCriterion().cancelInProgress() != null) {
          break;
        }

        try {
          ByteBuffer buff = getNIOBuffer();
          synchronized(stateLock) {
            connectionState = STATE_READING;
          }
          int amt = channel.read(buff);
          synchronized(stateLock) {
            connectionState = STATE_IDLE;
          }
          if (amt == 0) {
            continue;
          }
          if (amt < 0) {
            this.readerShuttingDown = true;
            try {
              requestClose("SocketChannel.read returned EOF");
              requestClose(LocalizedStrings.Connection_SOCKETCHANNEL_READ_RETURNED_EOF.toLocalizedString());
            } catch (Exception e) {
              // ignore - shutting down
            }
            return;
          }

          processNIOBuffer();
          if (!this.isReceiver
              && (this.handshakeRead || this.handshakeCancelled)) {
            if (logger.isDebugEnabled()) {
              if (this.handshakeRead) {
                logger.debug("{} handshake has been read {}", p2pReaderName(), this);
              } else {
                logger.debug("{} handshake has been cancelled {}", p2pReaderName(), this);
              }
            }
            isHandShakeReader = true;
            // Once we have read the handshake the reader can go away
            break;
          }
        }
        catch (CancelException e) {
          if (logger.isDebugEnabled()) {
            logger.debug("{} Terminated <{}> due to cancellation", p2pReaderName(), this, e);
          }
          this.readerShuttingDown = true;
          try { 
            requestClose(LocalizedStrings.Connection_CACHECLOSED_IN_CHANNEL_READ_0.toLocalizedString(e));
          } catch (Exception ex) {}
          return;
        }
        catch (ClosedChannelException e) {
          this.readerShuttingDown = true;
          try { 
            requestClose(LocalizedStrings.Connection_CLOSEDCHANNELEXCEPTION_IN_CHANNEL_READ_0.toLocalizedString(e));
          } catch (Exception ex) {}
          return;
        }
        catch (IOException e) {
          if (! isSocketClosed()
                && !"Socket closed".equalsIgnoreCase(e.getMessage()) // needed for Solaris jdk 1.4.2_08
                ) {
            if (logger.isDebugEnabled() && !isIgnorableIOException(e)) {
              logger.debug("{} io exception for {}", p2pReaderName(), this, e);
            }
            if(e.getMessage().contains("interrupted by a call to WSACancelBlockingCall")) {
              if (logger.isDebugEnabled()) {
                logger.debug("{} received unexpected WSACancelBlockingCall exception, which may result in a hang", p2pReaderName()); 
              }
            }
          }
          this.readerShuttingDown = true;
          try { 
            requestClose(LocalizedStrings.Connection_IOEXCEPTION_IN_CHANNEL_READ_0.toLocalizedString(e));
          } catch (Exception ex) {}
          return;

        } catch (Exception e) {
          this.owner.getConduit().getCancelCriterion().checkCancelInProgress(null); // bug 37101
          if (!stopped && ! isSocketClosed() ) {
            logger.fatal(LocalizedMessage.create(LocalizedStrings.Connection_0_EXCEPTION_IN_CHANNEL_READ, p2pReaderName()), e);
          }
          this.readerShuttingDown = true;
          try { 
            requestClose(LocalizedStrings.Connection_0_EXCEPTION_IN_CHANNEL_READ.toLocalizedString(e)); 
          } catch (Exception ex) {}
          return;
        }
      } // for
    }
    finally {
      if (!isHandShakeReader) {
        synchronized(stateLock) {
          connectionState = STATE_IDLE;
        }
      }
      if (logger.isDebugEnabled()) {
        logger.debug("{} runNioReader terminated id={} from {}", p2pReaderName(), conduitIdStr, remoteAddr);
      }
    }
  }

  /**
   * checks to see if an exception should not be logged: i.e., "forcibly closed",
   * "reset by peer", or "connection reset"
   * */
  public static final boolean isIgnorableIOException(Exception e) {
    if (e instanceof ClosedChannelException) {
      return true;
    }

    String msg = e.getMessage();
    if (msg == null) {
      msg = e.toString();
    }

    msg = msg.toLowerCase();
    return (msg.indexOf("forcibly closed") >= 0)
      ||   (msg.indexOf("reset by peer")   >= 0)
      ||   (msg.indexOf("connection reset")   >= 0);
  }

  private static boolean validMsgType(int msgType) {
    return msgType == NORMAL_MSG_TYPE
      || msgType == CHUNKED_MSG_TYPE
      || msgType == END_CHUNKED_MSG_TYPE;
  }

  private void closeAllMsgDestreamers() {
    synchronized (this.destreamerLock) {
      if (this.idleMsgDestreamer != null) {
        this.idleMsgDestreamer.close();
        this.idleMsgDestreamer = null;
      }
      if (this.destreamerMap != null) {
        Iterator it = this.destreamerMap.values().iterator();
        while (it.hasNext()) {
          MsgDestreamer md = (MsgDestreamer)it.next();
          md.close();
        }
        this.destreamerMap = null;
      }
    }
  }

  MsgDestreamer obtainMsgDestreamer(short msgId, final Version v) {
    synchronized (this.destreamerLock) {
      if (this.destreamerMap == null) {
        this.destreamerMap = new HashMap();
      }
      Short key = new Short(msgId);
      MsgDestreamer result = (MsgDestreamer)this.destreamerMap.get(key);
      if (result == null) {
        result = this.idleMsgDestreamer;
        if (result != null) {
          this.idleMsgDestreamer = null;
        } else {
          result = new MsgDestreamer(this.owner.getConduit().stats, 
              this.owner.owner.getCancelCriterion(), v);
        }
        result.setName(p2pReaderName() + " msgId=" + msgId);
        this.destreamerMap.put(key, result);
      }
      return result;
    }
  }
  void releaseMsgDestreamer(short msgId, MsgDestreamer md) {
    Short key = new Short(msgId);
    synchronized (this.destreamerLock) {
      this.destreamerMap.remove(key);
      if (this.idleMsgDestreamer == null) {
        md.reset();
        this.idleMsgDestreamer = md;
      } else {
        md.close();
      }
    }
  }

  private void sendFailureReply(int rpId, String exMsg, Throwable ex, boolean directAck) {
    ReplySender dm = null;
    if(directAck) {
      dm = new DirectReplySender(this);
    } else if(rpId != 0) {
      dm = this.owner.getDM();
    }
    if (dm != null) {
      ReplyMessage.send(getRemoteAddress(), rpId, new ReplyException(exMsg, ex), dm);
    }
  }
  private void runOioReader() {
    InputStream input = null;
    try {
      if (logger.isDebugEnabled()) {
        logger.debug("Socket is of type: {}", getSocket().getClass() );
      }
      input = new BufferedInputStream(getSocket().getInputStream(), INITIAL_CAPACITY);
    }
    catch (IOException io) {
      if (stopped || owner.getConduit().getCancelCriterion().cancelInProgress() != null) {
        return; // bug 37520: exit run loop (and thread)
      }
      logger.fatal(LocalizedMessage.create(LocalizedStrings.Connection_UNABLE_TO_GET_INPUT_STREAM), io);
      stopped = true;
    }

    if (!stopped) {
      Assert.assertTrue(owner != null, LocalizedStrings.Connection_OWNER_SHOULD_NOT_BE_NULL.toLocalizedString());
      if (logger.isDebugEnabled()) {
        logger.debug("Starting {}", p2pReaderName());
      }
    }

    byte[] lenbytes = new byte[MSG_HEADER_BYTES];

    final ByteArrayDataInput dis = new ByteArrayDataInput();
    while (!stopped) {
      try {
        if (SystemFailure.getFailure() != null) {
          // Allocate no objects here!
          Socket s = this.socket;
          if (s != null) {
            try {
              s.close();
            }
            catch (IOException e) {
              // don't care
            }
          }
          SystemFailure.checkFailure(); // throws
        }
        if (this.owner.getConduit().getCancelCriterion().cancelInProgress() != null) {
          break;
        }
        int len = 0;
        if (readFully(input, lenbytes, lenbytes.length) < 0) {
          stopped = true;
          continue;
        }
//        long recvNanos = DistributionStats.getStatTime();
        len = ((lenbytes[MSG_HEADER_SIZE_OFFSET]&0xff) * 0x1000000) +
          ((lenbytes[MSG_HEADER_SIZE_OFFSET+1]&0xff) * 0x10000) +
          ((lenbytes[MSG_HEADER_SIZE_OFFSET+2]&0xff) * 0x100) +
          (lenbytes[MSG_HEADER_SIZE_OFFSET+3]&0xff);
        /*byte msgHdrVersion =*/ calcHdrVersion(len);
        len = calcMsgByteSize(len);
        int msgType = lenbytes[MSG_HEADER_TYPE_OFFSET];
        short msgId = (short)((lenbytes[MSG_HEADER_ID_OFFSET]&0xff * 0x100)
                              + (lenbytes[MSG_HEADER_ID_OFFSET+1]&0xff));
        boolean myDirectAck = (msgType & DIRECT_ACK_BIT) != 0;
        if (myDirectAck) {
          msgType &= ~DIRECT_ACK_BIT; // clear the bit
        }
        // Following validation fixes bug 31145
        if (!validMsgType(msgType)) {
          logger.fatal(LocalizedMessage.create(LocalizedStrings.Connection_UNKNOWN_P2P_MESSAGE_TYPE_0, Integer.valueOf(msgType)));
          this.readerShuttingDown = true;
          requestClose(LocalizedStrings.Connection_UNKNOWN_P2P_MESSAGE_TYPE_0.toLocalizedString(Integer.valueOf(msgType)));
          break;
        }
        if (logger.isTraceEnabled())
          logger.trace("{} reading {} bytes", conduitIdStr, len);
        byte[] bytes = new byte[len];
        if (readFully(input, bytes, len) < 0) {
          stopped = true;
          continue;
        }
        boolean interrupted = Thread.interrupted();
        try {
          if (this.handshakeRead) {
            if (msgType == NORMAL_MSG_TYPE) {
              //DMStats stats = this.owner.getConduit().stats;
              //long start = DistributionStats.getStatTime();
              this.owner.getConduit().stats.incMessagesBeingReceived(true, len);
              dis.initialize(bytes, this.remoteVersion);
              DistributionMessage msg = null;
              try {
                ReplyProcessor21.initMessageRPId();
                long startSer = this.owner.getConduit().stats.startMsgDeserialization();
                msg = (DistributionMessage)InternalDataSerializer.readDSFID(dis);
                this.owner.getConduit().stats.endMsgDeserialization(startSer);
                if (dis.available() != 0) {
                  logger.warn(LocalizedMessage.create(
                      LocalizedStrings.Connection_MESSAGE_DESERIALIZATION_OF_0_DID_NOT_READ_1_BYTES,
                      new Object[] { msg, Integer.valueOf(dis.available())}));
                }
                //stats.incBatchCopyTime(start);
                try {
                  //start = DistributionStats.getStatTime();
                  if (!dispatchMessage(msg, len, myDirectAck)) {
                    continue;
                  }
                  //stats.incBatchSendTime(start);
                }
                catch (MemberShunnedException e) {
                  continue;
                }
                catch (Exception de) {
                  this.owner.getConduit().getCancelCriterion().checkCancelInProgress(de); // bug 37101
                  logger.fatal(LocalizedMessage.create(LocalizedStrings.Connection_ERROR_DISPATCHING_MESSAGE), de);
                }
              }
              catch (VirtualMachineError err) {
                SystemFailure.initiateFailure(err);
                // If this ever returns, rethrow the error.  We're poisoned
                // now, so don't let this thread continue.
                throw err;
              }
              catch (Throwable e) {
                // Whenever you catch Error or Throwable, you must also
                // catch VirtualMachineError (see above).  However, there is
                // _still_ a possibility that you are dealing with a cascading
                // error condition, so you also need to check to see if the JVM
                // is still usable:
                SystemFailure.checkFailure();
                // In particular I want OutOfMem to be caught here
                if (!myDirectAck) {
                  String reason = LocalizedStrings.Connection_ERROR_DESERIALIZING_MESSAGE.toLocalizedString();
                  sendFailureReply(ReplyProcessor21.getMessageRPId(), reason, e, myDirectAck);
                }
                if(e instanceof CancelException) {
                  if (!(e instanceof CacheClosedException)) {
                    // Just log a message if we had trouble deserializing due to CacheClosedException; see bug 43543
                    throw (CancelException) e;
                  }
                }
                logger.fatal(LocalizedMessage.create(LocalizedStrings.Connection_ERROR_DESERIALIZING_MESSAGE), e);
                //requestClose();
                //return;
              } finally {
                ReplyProcessor21.clearMessageRPId();
              }
            } else if (msgType == CHUNKED_MSG_TYPE) {
              MsgDestreamer md = obtainMsgDestreamer(msgId, remoteVersion);
              this.owner.getConduit().stats.incMessagesBeingReceived(md.size() == 0, len);
              try {
                md.addChunk(bytes);
              } catch (IOException ex) {
                logger.fatal(LocalizedMessage.create(LocalizedStrings.Connection_FAILED_HANDLING_CHUNK_MESSAGE), ex);
              }
            } else /* (msgType == END_CHUNKED_MSG_TYPE) */ {
              MsgDestreamer md = obtainMsgDestreamer(msgId, remoteVersion);
              this.owner.getConduit().stats.incMessagesBeingReceived(md.size() == 0, len);
              try {
                md.addChunk(bytes);
              } catch (IOException ex) {
                logger.fatal(LocalizedMessage.create(LocalizedStrings.Connection_FAILED_HANDLING_END_CHUNK_MESSAGE), ex);
              }
              DistributionMessage msg = null;
              int msgLength = 0;
              String failureMsg = null;
              Throwable failureEx = null;
              int rpId = 0;
              try {
                msg = md.getMessage();
              } catch (ClassNotFoundException ex) {
                this.owner.getConduit().stats.decMessagesBeingReceived(md.size());
                failureEx = ex;
                rpId = md.getRPid();
                logger.warn(LocalizedMessage.create(LocalizedStrings.Connection_CLASSNOTFOUND_DESERIALIZING_MESSAGE_0, ex));
              } catch (IOException ex) {
                this.owner.getConduit().stats.decMessagesBeingReceived(md.size());
                failureMsg = LocalizedStrings.Connection_IOEXCEPTION_DESERIALIZING_MESSAGE.toLocalizedString();
                failureEx = ex;
                rpId = md.getRPid();
                logger.fatal(LocalizedMessage.create(LocalizedStrings.Connection_IOEXCEPTION_DESERIALIZING_MESSAGE), failureEx);
              } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw ex; // caught by outer try
              } 
              catch (VirtualMachineError err) {
                SystemFailure.initiateFailure(err);
                // If this ever returns, rethrow the error.  We're poisoned
                // now, so don't let this thread continue.
                throw err;
              }
              catch (Throwable ex) {
                // Whenever you catch Error or Throwable, you must also
                // catch VirtualMachineError (see above).  However, there is
                // _still_ a possibility that you are dealing with a cascading
                // error condition, so you also need to check to see if the JVM
                // is still usable:
                SystemFailure.checkFailure();
                this.owner.getConduit().stats.decMessagesBeingReceived(md.size());
                failureMsg = LocalizedStrings.Connection_UNEXPECTED_FAILURE_DESERIALIZING_MESSAGE.toLocalizedString();
                failureEx = ex;
                rpId = md.getRPid();
                logger.fatal(LocalizedMessage.create(LocalizedStrings.Connection_UNEXPECTED_FAILURE_DESERIALIZING_MESSAGE), failureEx);
              } finally {
                msgLength = md.size();
                releaseMsgDestreamer(msgId, md);
              }
              if (msg != null) {
                try {
                  if (!dispatchMessage(msg, msgLength, myDirectAck)) {
                    continue;
                  }
                }
                catch (MemberShunnedException e) {
                  continue;
                }
                catch (Exception de) {
                  this.owner.getConduit().getCancelCriterion().checkCancelInProgress(de);
                  logger.fatal(LocalizedMessage.create(LocalizedStrings.Connection_ERROR_DISPATCHING_MESSAGE), de);
                }
                catch (ThreadDeath td) {
                  throw td;
                } 
                catch (VirtualMachineError err) {
                  SystemFailure.initiateFailure(err);
                  // If this ever returns, rethrow the error.  We're poisoned
                  // now, so don't let this thread continue.
                  throw err;
                }
                catch (Throwable t) {
                  // Whenever you catch Error or Throwable, you must also
                  // catch VirtualMachineError (see above).  However, there is
                  // _still_ a possibility that you are dealing with a cascading
                  // error condition, so you also need to check to see if the JVM
                  // is still usable:
                  SystemFailure.checkFailure();
                  logger.fatal(LocalizedMessage.create(LocalizedStrings.Connection_THROWABLE_DISPATCHING_MESSAGE), t);
                }
              } else if (failureEx != null) {
                sendFailureReply(rpId, failureMsg, failureEx, myDirectAck);
              }
            }
          } else {
            dis.initialize(bytes, null);
            if (!this.isReceiver) {
              this.replyCode = dis.readUnsignedByte();
              if (this.replyCode != REPLY_CODE_OK && this.replyCode != REPLY_CODE_OK_WITH_ASYNC_INFO) {
                Integer replyCodeInteger = Integer.valueOf(this.replyCode);
                String err = LocalizedStrings.Connection_UNKNOWN_HANDSHAKE_REPLY_CODE_0.toLocalizedString(replyCodeInteger);
                
                if (this.replyCode == 0) { // bug 37113
                  if (logger.isDebugEnabled()) {
                    logger.debug("{} (peer probably departed ungracefully)", err);
                  }
                }
                else {
                  logger.fatal(LocalizedMessage.create(LocalizedStrings.Connection_UNKNOWN_HANDSHAKE_REPLY_CODE_0, replyCodeInteger));
                }
                this.readerShuttingDown = true;
                requestClose(err);
                  break;
              }
              if (this.replyCode == REPLY_CODE_OK_WITH_ASYNC_INFO) {
                this.asyncDistributionTimeout = dis.readInt();
                this.asyncQueueTimeout = dis.readInt();
                this.asyncMaxQueueSize = (long)dis.readInt() * (1024*1024);
                if (this.asyncDistributionTimeout != 0) {
                  logger.info(LocalizedMessage.create(LocalizedStrings.
                    Connection_0_ASYNC_CONFIGURATION_RECEIVED_1,
                    new Object[] {p2pReaderName(),
                      " asyncDistributionTimeout=" 
                      + this.asyncDistributionTimeout
                      + " asyncQueueTimeout=" + this.asyncQueueTimeout
                      + " asyncMaxQueueSize=" 
                      + (this.asyncMaxQueueSize / (1024*1024))}));
                }
                // read the product version ordinal for on-the-fly serialization
                // transformations (for rolling upgrades)
                this.remoteVersion = Version.readVersion(dis, true);
              }
              notifyHandshakeWaiter(true);
            } else {
              byte b = dis.readByte();
              if (b != 0) {
                throw new IllegalStateException(LocalizedStrings.Connection_DETECTED_OLD_VERSION_PRE_5_0_1_OF_GEMFIRE_OR_NONGEMFIRE_DURING_HANDSHAKE_DUE_TO_INITIAL_BYTE_BEING_0.toLocalizedString(new Byte(b)));
              }
              byte handShakeByte = dis.readByte();
              if (handShakeByte != HANDSHAKE_VERSION) {
                throw new IllegalStateException(LocalizedStrings.Connection_DETECTED_WRONG_VERSION_OF_GEMFIRE_PRODUCT_DURING_HANDSHAKE_EXPECTED_0_BUT_FOUND_1
                    .toLocalizedString(new Object[]{new Byte(HANDSHAKE_VERSION), new Byte(handShakeByte)}));
              }
              InternalDistributedMember remote = DSFIDFactory.readInternalDistributedMember(dis);
              Stub stub = new Stub(remote.getIpAddress()/*fix for bug 33615*/, remote.getDirectChannelPort(), remote.getVmViewId());
              setRemoteAddr(remote, stub);
              Thread.currentThread().setName(LocalizedStrings.Connection_P2P_MESSAGE_READER_FOR_0.toLocalizedString(this.remoteAddr, this.socket.getPort()));
              this.sharedResource = dis.readBoolean();
              this.preserveOrder = dis.readBoolean();
              this.uniqueId = dis.readLong();
              // read the product version ordinal for on-the-fly serialization
              // transformations (for rolling upgrades)
              this.remoteVersion = Version.readVersion(dis, true);
              int dominoNumber = 0;
              if (this.remoteVersion == null ||
                  (this.remoteVersion.compareTo(Version.GFE_80) >= 0) ) {
                dominoNumber = dis.readInt();
                if (this.sharedResource) {
                  dominoNumber = 0;
                }
                dominoCount.set(dominoNumber);
//                this.senderName = dis.readUTF();
              }

              if (!this.sharedResource) {
                if (tipDomino()) {
                  logger.info(LocalizedMessage.create(
                    LocalizedStrings.Connection_THREAD_OWNED_RECEIVER_FORCING_ITSELF_TO_SEND_ON_THREAD_OWNED_SOCKETS));
// bug #49565 - if domino count is >= 2 use shared resources.
// Also see DistributedCacheOperation#supportsDirectAck
                } else { // if (dominoNumber < 2){
                  ConnectionTable.threadWantsOwnResources();
                  if (logger.isDebugEnabled()) {
                    logger.debug("thread-owned receiver with domino count of {} will prefer sending on thread-owned sockets", dominoNumber);
                  }
//                } else {
//                  ConnectionTable.threadWantsSharedResources();
//                  logger.fine("thread-owned receiver with domino count of " + dominoNumber + " will prefer shared sockets");
                }
                this.owner.owner.stats.incThreadOwnedReceivers(1L, dominoNumber);
              }
              
              if (logger.isDebugEnabled()) {
                logger.debug("{} remoteId is {} {}", p2pReaderName(), this.remoteId,
                    (this.remoteVersion != null ? " (" + this.remoteVersion + ')' : ""));
              }

              String authInit = System
                  .getProperty(DistributionConfigImpl.SECURITY_SYSTEM_PREFIX
                      + DistributionConfig.SECURITY_PEER_AUTH_INIT_NAME);
              boolean isSecure = authInit != null && authInit.length() != 0;

              if (isSecure) {
                // ARB: wait till member authentication has been confirmed?
                if (owner.getConduit().waitForMembershipCheck(this.remoteAddr)) {
                  sendOKHandshakeReply(); // fix for bug 33224
                  notifyHandshakeWaiter(true);
                }
                else {
                  // ARB: throw exception??
                  notifyHandshakeWaiter(false);
                  logger.warn(LocalizedMessage.create(
                    LocalizedStrings.Connection_0_TIMED_OUT_DURING_A_MEMBERSHIP_CHECK, p2pReaderName()));
                }
              }
              else {
                sendOKHandshakeReply(); // fix for bug 33224
                notifyHandshakeWaiter(true);
              }
            }
            if (!this.isReceiver
                && (this.handshakeRead || this.handshakeCancelled)) {
              if (logger.isDebugEnabled()) {
                 if (this.handshakeRead) {
                  logger.debug("{} handshake has been read {}", p2pReaderName(), this);
                } else {
                  logger.debug("{} handshake has been cancelled {}", p2pReaderName(), this);
                }
              }
              // Once we have read the handshake the reader can go away
              break;
            }
            continue;
          }
        }
        catch (InterruptedException e) {
          interrupted = true;
          this.owner.getConduit().getCancelCriterion().checkCancelInProgress(e);
          logger.fatal(LocalizedMessage.create(LocalizedStrings.Connection_0_STRAY_INTERRUPT_READING_MESSAGE, p2pReaderName()), e);
          continue;
        }
        catch (Exception ioe) {
          this.owner.getConduit().getCancelCriterion().checkCancelInProgress(ioe); // bug 37101
          if (!stopped) {
            logger.fatal(LocalizedMessage.create(LocalizedStrings.Connection_0_ERROR_READING_MESSAGE, p2pReaderName()), ioe);
          }
          continue;
        }
        finally {
          if (interrupted) {
            Thread.currentThread().interrupt();
          }
        }
      }
      catch (CancelException e) {
        if (logger.isDebugEnabled()) {
          String ccMsg = p2pReaderName() + " Cancelled: " + this;
          if (e.getMessage() != null) {
            ccMsg += ": " + e.getMessage();
          }
          logger.debug(ccMsg);
        }
        this.readerShuttingDown = true;
        try { 
          requestClose(LocalizedStrings.Connection_CACHECLOSED_IN_CHANNEL_READ_0.toLocalizedString(e));
        } catch (Exception ex) {}
        this.stopped = true;
      }
      catch (IOException io) {
        boolean closed = isSocketClosed()
                || "Socket closed".equalsIgnoreCase(io.getMessage()); // needed for Solaris jdk 1.4.2_08
        if (!closed) {
          if (logger.isDebugEnabled() && !isIgnorableIOException(io)) {
            logger.debug("{} io exception for {}", p2pReaderName(), this, io);
          }
        }
        this.readerShuttingDown = true;
        try { 
          requestClose(LocalizedStrings.Connection_IOEXCEPTION_RECEIVED_0.toLocalizedString(io));
        } catch (Exception ex) {}

        if (closed) {
          stopped = true;
        }
        else {
          // sleep a bit to avoid a hot error loop
          try { Thread.sleep(1000); } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            if (this.owner.getConduit().getCancelCriterion().cancelInProgress() != null) {
              return;
            }
            break;
          }
        }
      } // IOException
      catch (Exception e) {
        if (this.owner.getConduit().getCancelCriterion().cancelInProgress() != null) {
          return; // bug 37101
        }
        if (!stopped && !(e instanceof InterruptedException) ) {
          logger.fatal(LocalizedMessage.create(LocalizedStrings.Connection_0_EXCEPTION_RECEIVED, p2pReaderName()), e);
        }
        if (isSocketClosed()) {
          stopped = true;
        }
        else {
          this.readerShuttingDown = true;
          try { requestClose(LocalizedStrings.Connection_0_EXCEPTION_RECEIVED.toLocalizedString(e)); } catch (Exception ex) {}

          // sleep a bit to avoid a hot error loop
          try { Thread.sleep(1000); } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            break;
          }
        }
      }
    }
  }
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="DE_MIGHT_IGNORE")
  final int readFully(InputStream input, byte[] buffer, int len) throws IOException {
    int bytesSoFar = 0;
    while (bytesSoFar < len) {
      this.owner.getConduit().getCancelCriterion().checkCancelInProgress(null);
      try {
        synchronized(stateLock) {
          connectionState = STATE_READING;
        }
        int bytesThisTime = input.read(buffer, bytesSoFar, len-bytesSoFar);
        if (bytesThisTime < 0) {
          this.readerShuttingDown = true;
          try {  
            requestClose(LocalizedStrings.Connection_STREAM_READ_RETURNED_NONPOSITIVE_LENGTH.toLocalizedString());
          } catch (Exception ex) {  }
          return -1;
        }
        bytesSoFar += bytesThisTime;
      }
      catch (InterruptedIOException io) {
//      try { Thread.sleep(10); }
//      catch (InterruptedException ie) {
//        Thread.currentThread().interrupt();
//      }

        // Current thread has been interrupted.  Regard it similar to an EOF
        this.readerShuttingDown = true;
        try {
          requestClose(LocalizedStrings.Connection_CURRENT_THREAD_INTERRUPTED.toLocalizedString());
        } catch (Exception ex) { }
        Thread.currentThread().interrupt();
        this.owner.getConduit().getCancelCriterion().checkCancelInProgress(null);
      }
      finally {
        synchronized(stateLock) {
          connectionState = STATE_IDLE;
        }
      }
    } // while
    return len;
  }

  /**
   * sends a serialized message to the other end of this connection.  This
      is used by the DirectChannel in GemFire when the message is going to
      be sent to multiple recipients.
   * @throws ConnectionException if the conduit has stopped
   */
  public void sendPreserialized(ByteBuffer buffer,
      boolean cacheContentChanges, DistributionMessage msg)
    throws IOException, ConnectionException
  {
    if (!connected) {
      throw new ConnectionException(LocalizedStrings.Connection_NOT_CONNECTED_TO_0.toLocalizedString(this.remoteId));
    }
    if (this.batchFlusher != null) {
      batchSend(buffer);
      return;
    }
    final boolean origSocketInUse = this.socketInUse;
    byte originalState = -1;
    synchronized (stateLock) {
      originalState = this.connectionState;;
      this.connectionState = STATE_SENDING;
    }
    this.socketInUse = true;
    try {
      if (useNIO()) {
        SocketChannel channel = getSocket().getChannel();
        nioWriteFully(channel, buffer, false, msg);
      } else {
        if (buffer.hasArray()) {
          this.output.write(buffer.array(), buffer.arrayOffset(),
                            buffer.limit() - buffer.position());
        } else {
          byte[] bytesToWrite = getBytesToWrite(buffer);
          synchronized(outLock) {
            try {
//              this.writerThread = Thread.currentThread();
              this.output.write(bytesToWrite);
              this.output.flush();
            }
            finally {
//              this.writerThread = null;
            }
          }
        }
      }
      if (cacheContentChanges) {
        messagesSent++;
      }
    } finally {
      accessed();
      this.socketInUse = origSocketInUse;
      synchronized (stateLock) {
        this.connectionState = originalState;
      }
    }
  }
  /**
   * If <code>use</code> is true then "claim" the connection for our use.
   * If <code>use</code> is false then "release" the connection.
   * Fixes bug 37657.
   * @return true if connection was already in use at time of call;
   *         false if not.
   */
  public boolean setInUse(boolean use, long startTime, long ackWaitThreshold, long ackSAThreshold, List connectionGroup) {
    // just do the following; EVEN if the connection has been closed
    final boolean origSocketInUse = this.socketInUse;
    synchronized(this) {
      if (use && (ackWaitThreshold > 0 || ackSAThreshold > 0)) {
        // set times that events should be triggered
        this.transmissionStartTime = startTime;
        this.ackWaitTimeout = ackWaitThreshold;
        this.ackSATimeout = ackSAThreshold;
        this.ackConnectionGroup = connectionGroup;
        this.ackThreadName = Thread.currentThread().getName();
      }
      else {
        this.ackWaitTimeout = 0;
        this.ackSATimeout = 0;
        this.ackConnectionGroup = null;
        this.ackThreadName = null;
      }
      synchronized (this.stateLock) {
        this.connectionState = STATE_IDLE;
      }
      this.socketInUse = use;
    }
    if (!use) {
      accessed();
    }
    return origSocketInUse;
  }
  

  /** ensure that a task is running to monitor transmission and reading of acks */
  public synchronized void scheduleAckTimeouts() {
    if (ackTimeoutTask == null) {
      final long msAW = this.owner.getDM().getConfig().getAckWaitThreshold() * 1000;
      final long msSA = this.owner.getDM().getConfig().getAckSevereAlertThreshold() * 1000;
      ackTimeoutTask = new SystemTimer.SystemTimerTask() {
        @Override
        public void run2() {
          if (owner.isClosed()) {
            return;
          }
          byte connState = -1;
          synchronized (stateLock) {
            connState = connectionState;
          }
          boolean sentAlert = false;
          synchronized(Connection.this) {
            if (socketInUse) {
              switch (connState) {
                case Connection.STATE_IDLE:
                  break;
                case Connection.STATE_SENDING:
                  sentAlert = doSevereAlertProcessing();
                  break;
                case Connection.STATE_POST_SENDING:
                  break;
                case Connection.STATE_READING_ACK:
                  sentAlert = doSevereAlertProcessing();
                  break;
                case Connection.STATE_RECEIVED_ACK:
                  break;
                default:
              }
            }
          }
          if (sentAlert) {
            // since transmission and ack-receipt are performed serially, we don't
            // want to complain about all receivers out just because one was slow.  We therefore reset
            // the time stamps and give others more time
            for (Iterator it=ackConnectionGroup.iterator(); it.hasNext(); ) {
              Connection con = (Connection)it.next();
              if (con != Connection.this) {
                con.transmissionStartTime += con.ackSATimeout;
              }
            }
          }
        }
      };
      
      synchronized(owner) {
        SystemTimer timer = owner.getIdleConnTimer();
        if (timer != null) {
          if (msSA > 0) {
            timer.scheduleAtFixedRate(ackTimeoutTask, msAW, Math.min(msAW, msSA));
          }
          else {
            timer.schedule(ackTimeoutTask, msAW);
          }
        }
      }
    }
  }

  /** ack-wait-threshold and ack-severe-alert-threshold processing */
  protected boolean doSevereAlertProcessing() {
    long now = System.currentTimeMillis();
    if (ackSATimeout > 0
        && (transmissionStartTime + ackWaitTimeout + ackSATimeout) <= now) {
      logger.fatal(LocalizedMessage.create(
          LocalizedStrings.Connection_0_SECONDS_HAVE_ELAPSED_WAITING_FOR_A_RESPONSE_FROM_1_FOR_THREAD_2,
          new Object[] {Long.valueOf((ackWaitTimeout+ackSATimeout)/1000), getRemoteAddress(), ackThreadName}));
      // turn off subsequent checks by setting the timeout to zero, then boot the member
      ackSATimeout = 0;
      return true;
    }
    else if (!ackTimedOut
        && (0 < ackWaitTimeout) 
        && (transmissionStartTime + ackWaitTimeout) <= now) {
      logger.warn(LocalizedMessage.create(
          LocalizedStrings.Connection_0_SECONDS_HAVE_ELAPSED_WAITING_FOR_A_RESPONSE_FROM_1_FOR_THREAD_2,
          new Object[] {Long.valueOf(ackWaitTimeout/1000), getRemoteAddress(), ackThreadName}));
      ackTimedOut = true;
      
      final StringId state = (connectionState == Connection.STATE_SENDING)?
          LocalizedStrings.Connection_TRANSMIT_ACKWAITTHRESHOLD : LocalizedStrings.Connection_RECEIVE_ACKWAITTHRESHOLD;
      if (ackSATimeout > 0) {
        this.owner.getDM().getMembershipManager()
        .suspectMembers(Collections.singleton(getRemoteAddress()), state.toLocalizedString());
      }
    }
    return false;
  }

  static private byte[] getBytesToWrite(ByteBuffer buffer) {
    byte[] bytesToWrite = new byte[buffer.limit()];
    buffer.get(bytesToWrite);
    return bytesToWrite;
  }

//  private String socketInfo() {
//    return (" socket: " + getSocket().getLocalAddress() + ":" + getSocket().getLocalPort() + " -> " +
//            getSocket().getInetAddress() + ":" + getSocket().getPort() + " connection = " + System.identityHashCode(this));
//
//  }

  private final boolean addToQueue(ByteBuffer buffer, DistributionMessage msg,
                                   boolean force) throws ConnectionException {
    final DMStats stats = this.owner.getConduit().stats;
    long start = DistributionStats.getStatTime();
    try {
      ConflationKey ck = null;
      if (msg != null) {
        ck = msg.getConflationKey();
      }
      Object objToQueue = null;
      // if we can conflate delay the copy to see if we can reuse
      // an already allocated buffer.
      final int newBytes = buffer.remaining();
      final int origBufferPos = buffer.position(); // to fix bug 34832
      if (ck == null || !ck.allowsConflation()) {
        // do this outside of sync for multi thread perf
        ByteBuffer newbb = ByteBuffer.allocate(newBytes);
        newbb.put(buffer);
        newbb.flip();
        objToQueue = newbb;
      }
      synchronized (this.outgoingQueue) {
        if (this.disconnectRequested) {
          buffer.position(origBufferPos);
          // we have given up so just drop this message.
          throw new ConnectionException(LocalizedStrings.Connection_FORCED_DISCONNECT_SENT_TO_0.toLocalizedString(this.remoteId));
        }
        if (!force && !this.asyncQueuingInProgress) {
          // reset buffer since we will be sending it. This fixes bug 34832
          buffer.position(origBufferPos);
          // the pusher emptied the queue so don't add since we are not forced to.
          return false;
        }
        boolean didConflation = false;
        if (ck != null) {
          if (ck.allowsConflation()) {
            objToQueue = ck;
            Object oldValue = this.conflatedKeys.put(ck, ck);
            if (oldValue != null) {
              ConflationKey oldck = (ConflationKey)oldValue;
              ByteBuffer oldBuffer = oldck.getBuffer();
              // need to always do this to allow old buffer to be gc'd
              oldck.setBuffer(null);
              // remove the conflated key from current spot in queue
              // Note we no longer remove from the queue because the search
              // can be expensive on large queues. Instead we just wait for
              // the queue removal code to find the oldck and ignore it since
              // its buffer is null
              // We do a quick check of the last thing in the queue
              // and if it has the same identity of our last thing then
              // remove it
              if (this.outgoingQueue.getLast() == oldck) {
                this.outgoingQueue.removeLast();
              }
              int oldBytes = oldBuffer.remaining();
              this.queuedBytes -= oldBytes;
              stats.incAsyncQueueSize(-oldBytes);
              stats.incAsyncConflatedMsgs();
              didConflation = true;
              if (oldBuffer.capacity() >= newBytes) {
                // copy new buffer into oldBuffer
                oldBuffer.clear();
                oldBuffer.put(buffer);
                oldBuffer.flip();
                ck.setBuffer(oldBuffer);
              } else {
                // old buffer was not large enough
                oldBuffer = null;
                ByteBuffer newbb = ByteBuffer.allocate(newBytes);
                newbb.put(buffer);
                newbb.flip();
                ck.setBuffer(newbb);
              }
            } else {
              // no old buffer so need to allocate one
              ByteBuffer newbb = ByteBuffer.allocate(newBytes);
              newbb.put(buffer);
              newbb.flip();
              ck.setBuffer(newbb);
            }
          } else {
            // just forget about having a conflatable operation
            /*Object removedVal =*/ this.conflatedKeys.remove(ck);
          }
        }
        {
          long newQueueSize = newBytes + this.queuedBytes;
          if (newQueueSize > this.asyncMaxQueueSize) {
            logger.warn(LocalizedMessage.create(
                LocalizedStrings.Connection_QUEUED_BYTES_0_EXCEEDS_MAX_OF_1_ASKING_SLOW_RECEIVER_2_TO_DISCONNECT,
                new Object[] {newQueueSize, this.asyncMaxQueueSize, this.remoteAddr }));
            stats.incAsyncQueueSizeExceeded(1);
            disconnectSlowReceiver();
            // reset buffer since we will be sending it
            buffer.position(origBufferPos);
            return false;
          }
        }
        this.outgoingQueue.addLast(objToQueue);
        this.queuedBytes += newBytes;
        stats.incAsyncQueueSize(newBytes);
        if (!didConflation) {
          stats.incAsyncQueuedMsgs();
        }
        return true;
      }
    } finally {
      if (DistributionStats.enableClockStats) {
        stats.incAsyncQueueAddTime(DistributionStats.getStatTime() - start);
      }
    }
  }
  /**
   * Return true if it was able to handle a block write of the given buffer.
   * Return false if it is still the caller is still responsible for writing it.
   * @throws ConnectionException if the conduit has stopped
   */
  private final boolean handleBlockedWrite(ByteBuffer buffer,
      DistributionMessage msg) throws ConnectionException
  {
    if (!addToQueue(buffer, msg, true)) {
      return false;
    } else {
      startNioPusher();
      return true;
    }
  }

  private final Object nioPusherSync = new Object();

  private void startNioPusher() {
      synchronized (this.nioPusherSync) {
        while (this.pusherThread != null) {
          // wait for previous pusher thread to exit
          boolean interrupted = Thread.interrupted();
          try {
            this.nioPusherSync.wait(); // spurious wakeup ok
          } catch (InterruptedException ex) {
            interrupted = true;
            this.owner.getConduit().getCancelCriterion().checkCancelInProgress(ex);
          }
          finally {
            if (interrupted) {
              Thread.currentThread().interrupt();
            }
          }
        }
        this.asyncQueuingInProgress = true;
        ThreadGroup group =
          LoggingThreadGroup.createThreadGroup("P2P Writer Threads", logger);
        this.pusherThread = new Thread(group, new Runnable() {
            public void run() {
              Connection.this.runNioPusher();
            }
          }, "P2P async pusher to " + this.remoteAddr);
        this.pusherThread.setDaemon(true);
      } // synchronized
      this.pusherThread.start();
  }

  private final ByteBuffer takeFromOutgoingQueue() throws InterruptedException {
    ByteBuffer result = null;
    final DMStats stats = this.owner.getConduit().stats;
    long start = DistributionStats.getStatTime();
    try {
      synchronized (this.outgoingQueue) {
        if (this.disconnectRequested) {
          // don't bother with anymore work since we are done
          this.asyncQueuingInProgress = false;
          this.outgoingQueue.notifyAll();
          return null;
        }
        //Object o = this.outgoingQueue.poll();
        do {
          if (this.outgoingQueue.isEmpty()) {
            break;
          }
          Object o = this.outgoingQueue.removeFirst();
          if (o == null) {
            break;
          }
          if (o instanceof ConflationKey) {
            result = ((ConflationKey)o).getBuffer();
            if (result != null) {
              this.conflatedKeys.remove(o);
            } else {
              // if result is null then this same key will be found later in the
              // queue so we just need to skip this entry
              continue;
            }
          } else {
            result = (ByteBuffer)o;
          }
          int newBytes = result.remaining();
          this.queuedBytes -= newBytes;
          stats.incAsyncQueueSize(-newBytes);
          stats.incAsyncDequeuedMsgs();
        } while (result == null);
        if (result == null) {
          this.asyncQueuingInProgress = false;
          this.outgoingQueue.notifyAll();
        }
      }
      return result;
    } finally {
      if (DistributionStats.enableClockStats) {
        stats.incAsyncQueueRemoveTime(DistributionStats.getStatTime() - start);
      }
    }
  }

  private boolean disconnectRequested = false;


  /**
   * @since 4.2.2
   */
  private void disconnectSlowReceiver() {
    synchronized (this.outgoingQueue) {
      if (this.disconnectRequested) {
        // only ask once
        return;
      }
      this.disconnectRequested = true;
    }
    DM dm = this.owner.getDM();
    if (dm == null) {
      this.owner.removeEndpoint(this.remoteId, LocalizedStrings.Connection_NO_DISTRIBUTION_MANAGER.toLocalizedString());
      return;
    }
    dm.getMembershipManager().requestMemberRemoval(this.remoteAddr, 
                                                   LocalizedStrings.Connection_DISCONNECTED_AS_A_SLOWRECEIVER.toLocalizedString());
    // Ok, we sent the message, the coordinator should kick the member out
    // immediately and inform this process with a new view.
    // Let's wait
    // for that to happen and if it doesn't in X seconds
    // then remove the endpoint.
    final int FORCE_TIMEOUT = 3000;
    while (dm.getOtherDistributionManagerIds().contains(this.remoteAddr)) {
      try {
        Thread.sleep(50);
      }
      catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        this.owner.getConduit().getCancelCriterion().checkCancelInProgress(ie);
        return;
      }
    }
    this.owner.removeEndpoint(this.remoteId, 
                              LocalizedStrings.Connection_FORCE_DISCONNECT_TIMED_OUT.toLocalizedString());
    if (dm.getOtherDistributionManagerIds().contains(this.remoteAddr)) {
      if (logger.isDebugEnabled()) {
        logger.debug("Force disconnect timed out after waiting {} seconds", (FORCE_TIMEOUT/1000));
      }
      return;
    }
  }

  /**
   * have the pusher thread check for queue overflow
   * and for idle time exceeded
   */
  protected void runNioPusher() {
    try {
    final DMStats stats = this.owner.getConduit().stats;
    final long threadStart = stats.startAsyncThread();
    try {
      stats.incAsyncQueues(1);
      stats.incAsyncThreads(1);

      try {
        int flushId = 0;
        while (this.asyncQueuingInProgress && this.connected) {
          if (SystemFailure.getFailure() != null) {
            // Allocate no objects here!
            Socket s = this.socket;
            if (s != null) {
              try {
                s.close();
              }
              catch (IOException e) {
                // don't care
              }
            }
            SystemFailure.checkFailure(); // throws
          }
          if (this.owner.getConduit().getCancelCriterion().cancelInProgress() != null) {
            break;
          }
          flushId++;
          long flushStart = stats.startAsyncQueueFlush();
          try {
            long curQueuedBytes = this.queuedBytes;
            if (curQueuedBytes > this.asyncMaxQueueSize) {
              logger.warn(LocalizedMessage.create(
                  LocalizedStrings.Connection_QUEUED_BYTES_0_EXCEEDS_MAX_OF_1_ASKING_SLOW_RECEIVER_2_TO_DISCONNECT,
                  new Object[]{ curQueuedBytes, this.asyncMaxQueueSize, this.remoteAddr}));
              stats.incAsyncQueueSizeExceeded(1);
              disconnectSlowReceiver();
              return;
            }
            SocketChannel channel = getSocket().getChannel();
            ByteBuffer bb = takeFromOutgoingQueue();
            if (bb == null) {
              if (logger.isDebugEnabled() && flushId == 1) {
                logger.debug("P2P pusher found empty queue");
              }
              return;
            }
            nioWriteFully(channel, bb, true, null);
            // We should not add messagesSent here according to Bruce.
            // The counts are increased elsewhere.
            // messagesSent++;
            accessed();
          } finally {
            stats.endAsyncQueueFlush(flushStart);
          }
        } // while
      } finally {
        // need to force this to false before doing the requestClose calls
        synchronized (this.outgoingQueue) {
          this.asyncQueuingInProgress = false;
          this.outgoingQueue.notifyAll();
        }
      }
    } catch (InterruptedException ex) {
      // someone wants us to stop.
      // No need to set interrupt bit, we're quitting.
      // No need to throw an error, we're quitting.
    } catch (IOException ex) {
      final String err = LocalizedStrings.Connection_P2P_PUSHER_IO_EXCEPTION_FOR_0.toLocalizedString(this);
      if (! isSocketClosed() ) {
        if (logger.isDebugEnabled() && !isIgnorableIOException(ex)) {
          logger.debug(err, ex);
        }
      }
      try { requestClose(err + ": " + ex); } catch (Exception ignore) {}
    } 
    catch (CancelException ex) { // bug 37367
      final String err = LocalizedStrings.Connection_P2P_PUSHER_0_CAUGHT_CACHECLOSEDEXCEPTION_1.toLocalizedString(new Object[] {this, ex});
      logger.debug(err);
      try { requestClose(err); } catch (Exception ignore) {}
      return;
    } 
    catch (Exception ex) {
      this.owner.getConduit().getCancelCriterion().checkCancelInProgress(ex); // bug 37101
      if (! isSocketClosed() ) {
        logger.fatal(LocalizedMessage.create(LocalizedStrings.Connection_P2P_PUSHER_EXCEPTION_0, ex), ex);
      }
      try { requestClose(LocalizedStrings.Connection_P2P_PUSHER_EXCEPTION_0.toLocalizedString(ex)); } catch (Exception ignore) {}
    } finally {
      stats.incAsyncQueueSize(-this.queuedBytes);
      this.queuedBytes = 0;
      stats.endAsyncThread(threadStart);
      stats.incAsyncThreads(-1);
      stats.incAsyncQueues(-1);
      if (logger.isDebugEnabled()) {
        logger.debug("runNioPusher terminated id={} from {}/{}", conduitIdStr, remoteId, remoteAddr);
      }
    }
    } finally {
      synchronized (this.nioPusherSync) {
        this.pusherThread = null;
        this.nioPusherSync.notify();
      }
    }
  }

  /**
   * Return false if socket writes to be done async/nonblocking
   * Return true if socket writes to be done sync/blocking
   */
  private final boolean useSyncWrites(boolean forceAsync) {
    if (forceAsync) {
      return false;
    }
    // only use sync writes if:
    // we are already queuing
    if (this.asyncQueuingInProgress) {
      // it will just tack this msg onto the outgoing queue
      return true;
    }
    // or we are a receiver
    if (this.isReceiver) {
      return true;
    }
    // or we are an unordered connection
    if (!this.preserveOrder) {
      return true;
    }
    // or the receiver does not allow queuing
    if (this.asyncDistributionTimeout == 0) {
      return true;
    }
    // OTHERWISE return false and let caller send async
    return false;
  }

  /**
   * If true then act as if the socket buffer is full and start async queuing
   */
  public static volatile boolean FORCE_ASYNC_QUEUE = false;

  static private final int MAX_WAIT_TIME = (1<<5); // ms (must be a power of 2)

  private final void writeAsync(SocketChannel channel,
      ByteBuffer buffer, boolean forceAsync, DistributionMessage p_msg,
      final DMStats stats) throws IOException {
    DistributionMessage msg = p_msg;
//  async/non-blocking
    boolean socketWriteStarted = false;
    long startSocketWrite = 0;
    int retries = 0;
    int totalAmtWritten = 0;
    try {
      synchronized (this.outLock) {
        if (!forceAsync) {
          // check one more time while holding outLock in case a pusher was created
          if (this.asyncQueuingInProgress) {
            if (addToQueue(buffer, msg, false)) {
              return;
            }
            // fall through
          }
        }
        socketWriteStarted = true;
        startSocketWrite = stats.startSocketWrite(false);
        long now = System.currentTimeMillis();
        int waitTime = 1;
        long distributionTimeoutTarget = 0;
        // if asyncDistributionTimeout == 1 then we want to start queuing
        // as soon as we do a non blocking socket write that returns 0
        if (this.asyncDistributionTimeout != 1) {
          distributionTimeoutTarget = now + this.asyncDistributionTimeout;
        }
        long queueTimeoutTarget = now + this.asyncQueueTimeout;
        channel.configureBlocking(false);
        try {
          do {
            this.owner.getConduit().getCancelCriterion().checkCancelInProgress(null);
            retries++;
            int amtWritten;
            if (FORCE_ASYNC_QUEUE) {
              amtWritten = 0;
            } else {
              amtWritten = channel.write(buffer);
            }
            if (amtWritten == 0) {
              now = System.currentTimeMillis();
              long timeoutTarget;
              if (!forceAsync) {
                if (now > distributionTimeoutTarget) {
                  if (logger.isDebugEnabled()) {
                    if (distributionTimeoutTarget == 0) {
                      logger.debug("Starting async pusher to handle async queue because distribution-timeout is 1 and the last socket write would have blocked.");
                    } else {
                      long blockedMs = now - distributionTimeoutTarget;
                      blockedMs += this.asyncDistributionTimeout;
                      logger.debug("Blocked for {}ms which is longer than the max of {}ms so starting async pusher to handle async queue.",
                          blockedMs, this.asyncDistributionTimeout);
                    }
                  }
                  stats.incAsyncDistributionTimeoutExceeded();
                  if (totalAmtWritten > 0) {
                    // we have written part of the msg to the socket buffer
                    // and we are going to queue the remainder.
                    // We set msg to null so that will not make
                    // the partial msg a candidate for conflation.
                    msg = null;
                  }
                  if (handleBlockedWrite(buffer, msg)) {
                    return;
                  }
                }
                timeoutTarget = distributionTimeoutTarget;
              } else {
                boolean disconnectNeeded = false;
                long curQueuedBytes = this.queuedBytes;
                if (curQueuedBytes > this.asyncMaxQueueSize) {
                  logger.warn(LocalizedMessage.create(
                      LocalizedStrings.Connection_QUEUED_BYTES_0_EXCEEDS_MAX_OF_1_ASKING_SLOW_RECEIVER_2_TO_DISCONNECT,
                      new Object[]{ Long.valueOf(curQueuedBytes), Long.valueOf(this.asyncMaxQueueSize), this.remoteAddr})); 
                  stats.incAsyncQueueSizeExceeded(1);
                  disconnectNeeded = true;
                }
                if (now > queueTimeoutTarget) {
                  // we have waited long enough
                  // the pusher has been idle too long!
                  long blockedMs = now - queueTimeoutTarget;
                  blockedMs += this.asyncQueueTimeout;
                  logger.warn(LocalizedMessage.create(
                      LocalizedStrings.Connection_BLOCKED_FOR_0_MS_WHICH_IS_LONGER_THAN_THE_MAX_OF_1_MS_ASKING_SLOW_RECEIVER_2_TO_DISCONNECT,
                      new Object[] {Long.valueOf(blockedMs), Integer.valueOf(this.asyncQueueTimeout), this.remoteAddr}));
                  stats.incAsyncQueueTimeouts(1);
                  disconnectNeeded = true;
                }
                if (disconnectNeeded) {
                  disconnectSlowReceiver();
                  synchronized (this.outgoingQueue) {
                    this.asyncQueuingInProgress = false;
                    this.outgoingQueue.notifyAll(); // for bug 42330
                  }
                  return;
                }
                timeoutTarget = queueTimeoutTarget;
              }
              {
                long msToWait = waitTime;
                long msRemaining = timeoutTarget - now;
                if (msRemaining > 0) {
                  msRemaining /= 2;
                }
                if (msRemaining < msToWait) {
                  msToWait = msRemaining;
                }
                if (msToWait <= 0) {
                  Thread.yield();
                } else {
                  boolean interrupted = Thread.interrupted();;
                  try {
                    Thread.sleep(msToWait);
                  } catch (InterruptedException ex) {
                    interrupted = true;
                    this.owner.getConduit().getCancelCriterion().checkCancelInProgress(ex);
                  }
                  finally {
                    if (interrupted) {
                      Thread.currentThread().interrupt();
                    }
                  }
                }
              }
              if (waitTime < MAX_WAIT_TIME) {
                // double it since it is not yet the max
                waitTime <<= 1;
              }
            } // amtWritten == 0
            else {
              totalAmtWritten += amtWritten;
              // reset queueTimeoutTarget since we made some progress
              queueTimeoutTarget = System.currentTimeMillis() + this.asyncQueueTimeout;
              waitTime = 1;
            }
          } while (buffer.remaining() > 0);
        } finally {
          channel.configureBlocking(true);
        }
      }
    } finally {
      if (socketWriteStarted) {
        if (retries > 0) {
          retries--;
        }
        stats.endSocketWrite(false, startSocketWrite, totalAmtWritten, retries);
      }
    }
  }

  /** nioWriteFully implements a blocking write on a channel that is in
   *  non-blocking mode.
   * @param forceAsync true if we need to force a blocking async write.
   * @throws ConnectionException if the conduit has stopped
   */
  protected final void nioWriteFully(SocketChannel channel,
                                     ByteBuffer buffer,
                                     boolean forceAsync,
                                     DistributionMessage msg)
    throws IOException, ConnectionException
  {
    final DMStats stats = this.owner.getConduit().stats;
    if (!this.sharedResource) {
      stats.incTOSentMsg();
    }
    if (useSyncWrites(forceAsync)) {
      if (this.asyncQueuingInProgress) {
        if (addToQueue(buffer, msg, false)) {
          return;
        }
        // fall through
      }
      long startLock = stats.startSocketLock();
      synchronized (this.outLock) {
        stats.endSocketLock(startLock);
        if (this.asyncQueuingInProgress) {
          if (addToQueue(buffer, msg, false)) {
            return;
          }
          // fall through
        }
        do {
          int amtWritten = 0;
          long start = stats.startSocketWrite(true);
          try {
//            this.writerThread = Thread.currentThread();
            amtWritten = channel.write(buffer);
          }
          finally {
            stats.endSocketWrite(true, start, amtWritten, 0);
//            this.writerThread = null;
          }
        } while (buffer.remaining() > 0);
      } // synchronized
    }
    else {
      writeAsync(channel, buffer, forceAsync, msg, stats);
    }
  }

  /** gets the buffer for receiving message length bytes */
  protected ByteBuffer getNIOBuffer() {
    final DMStats stats = this.owner.getConduit().stats;
    if (nioInputBuffer == null) {
      int allocSize = this.recvBufferSize;
      if (allocSize == -1) {
        allocSize = this.owner.getConduit().tcpBufferSize;
      }
      nioInputBuffer = Buffers.acquireReceiveBuffer(allocSize, stats);
    }
    return nioInputBuffer;
  }

  /**
   * stateLock is used to synchronize state changes.
   */
  protected Object stateLock = new Object();
  
  /** for timeout processing, this is the current state of the connection */
  protected byte connectionState = STATE_IDLE;
  
  /*~~~~~~~~~~~~~ connection states ~~~~~~~~~~~~~~~*/
  /** the connection is idle, but may be in use */
  protected static final byte STATE_IDLE = 0;
  /** the connection is in use and is transmitting data */
  protected static final byte STATE_SENDING = 1;
  /** the connection is in use and is done transmitting */
  protected static final byte STATE_POST_SENDING = 2;
  /** the connection is in use and is reading a direct-ack */
  protected static final byte STATE_READING_ACK = 3;
  /** the connection is in use and has finished reading a direct-ack */
  protected static final byte STATE_RECEIVED_ACK = 4;
  /** the connection is in use and is reading a message */
  protected static final byte STATE_READING = 5;
  
  protected static final String[] STATE_NAMES = new String[] {
    "idle", "sending", "post_sending", "reading_ack", "received_ack", "reading" };
  /*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
  
  /** set to true if we exceeded the ack-wait-threshold waiting for a response */
  protected volatile boolean ackTimedOut;

  private static int ACK_SIZE = 1;
  private static byte ACK_BYTE = 37;

  /**
   * @param msToWait number of milliseconds to wait for an ack.
   *                 If 0 then wait forever.
   * @param msInterval interval between checks
   * @throws SocketTimeoutException if msToWait expires.
   * @throws ConnectionException if ack is not received (fixes bug 34312)
   */
  public void readAck(final int msToWait, final long msInterval, final DirectReplyProcessor processor) throws SocketTimeoutException,
      ConnectionException {
    if (isSocketClosed()) {
      throw new ConnectionException(LocalizedStrings.Connection_CONNECTION_IS_CLOSED.toLocalizedString());
    }
    synchronized (this.stateLock) {
      this.connectionState = STATE_READING_ACK;
    }
    
    
    boolean origSocketInUse = this.socketInUse;
    this.socketInUse = true;
    MsgReader msgReader = null;
    DMStats stats = owner.getConduit().stats;
    final Version version = getRemoteVersion();
    try {
      if(useNIO()) {
        msgReader = new NIOMsgReader(this, version);
      } else {
        msgReader = new OioMsgReader(this, version);
      }
      
      Header header = msgReader.readHeader();

      ReplyMessage msg;
      int len;
      if(header.getNioMessageType() == NORMAL_MSG_TYPE) {
        msg = (ReplyMessage) msgReader.readMessage(header);
        len = header.getNioMessageLength();
      } else {
        //TODO - really no need to go to shared map here, we could probably just cache an idle one.
        MsgDestreamer destreamer = obtainMsgDestreamer(
            header.getNioMessageId(), version);
        while (header.getNioMessageType() == CHUNKED_MSG_TYPE) {
          msgReader.readChunk(header, destreamer);
          header = msgReader.readHeader();
        }
        msgReader.readChunk(header, destreamer);
        msg = (ReplyMessage) destreamer.getMessage();
        releaseMsgDestreamer(header.getNioMessageId(), destreamer);
        len = destreamer.size();
      }
      //I'd really just like to call dispatchMessage here. However,
      //that call goes through a bunch of checks that knock about
      //10% of the performance. Since this direct-ack stuff is all
      //about performance, we'll skip those checks. Skipping them
      //should be legit, because we just sent a message so we know
      //the member is already in our view, etc.
      DistributionManager dm = (DistributionManager) owner.getDM();
      msg.setBytesRead(len);
      msg.setSender(remoteAddr);
      stats.incReceivedMessages(1L);
      stats.incReceivedBytes(msg.getBytesRead());
      stats.incMessageChannelTime(msg.resetTimestamp());
      msg.process(dm, processor);
//      dispatchMessage(msg, len, false);
    }
    catch (MemberShunnedException e) {
      //do nothing
    }
    catch (SocketTimeoutException timeout) {
      throw timeout;
    }
    catch (IOException e) {
      final String err = LocalizedStrings.Connection_ACK_READ_IO_EXCEPTION_FOR_0.toLocalizedString(this);
      if (! isSocketClosed() ) {
        if (logger.isDebugEnabled() && !isIgnorableIOException(e)) {
          logger.debug(err, e);
        }
      }
      try { requestClose(err + ": " + e); } catch (Exception ex) {}
      throw new ConnectionException(LocalizedStrings.Connection_UNABLE_TO_READ_DIRECT_ACK_BECAUSE_0.toLocalizedString(e));
    }
    catch(ConnectionException e) {
      this.owner.getConduit().getCancelCriterion().checkCancelInProgress(e);
      throw e;
    }
    catch (Exception e) {
      this.owner.getConduit().getCancelCriterion().checkCancelInProgress(e);
      if (! isSocketClosed() ) {
        logger.fatal(LocalizedMessage.create(LocalizedStrings.Connection_ACK_READ_EXCEPTION), e);
      }
      try { requestClose(LocalizedStrings.Connection_ACK_READ_EXCEPTION_0.toLocalizedString(e)); } catch (Exception ex) {}
      throw new ConnectionException(LocalizedStrings.Connection_UNABLE_TO_READ_DIRECT_ACK_BECAUSE_0.toLocalizedString(e));
    }
    finally {
      stats.incProcessedMessages(1L);
      accessed();
      this.socketInUse = origSocketInUse;
      if (this.ackTimedOut) {
        logger.info(LocalizedMessage.create(
            LocalizedStrings.Connection_FINISHED_WAITING_FOR_REPLY_FROM_0,
            new Object[] {getRemoteAddress()}));
        this.ackTimedOut = false;
      }
      if(msgReader != null) {
        msgReader.close();
      }
    }
    synchronized (stateLock) {
      this.connectionState = STATE_RECEIVED_ACK;
    }
  }


  /** processes the current NIO buffer.  If there are complete messages
      in the buffer, they are deserialized and passed to TCPConduit for
      further processing */
  private void processNIOBuffer() throws ConnectionException, IOException {
    if (nioInputBuffer != null) {
      nioInputBuffer.flip();
    }
    boolean done = false;

    while (!done && connected) {
      this.owner.getConduit().getCancelCriterion().checkCancelInProgress(null);
//      long startTime = DistributionStats.getStatTime();
      int remaining = nioInputBuffer.remaining();
      if (nioLengthSet || remaining >= MSG_HEADER_BYTES) {
        if (!nioLengthSet) {
          int headerStartPos = nioInputBuffer.position();
          nioMessageLength = nioInputBuffer.getInt();
          /* nioMessageVersion = */ calcHdrVersion(nioMessageLength);
          nioMessageLength = calcMsgByteSize(nioMessageLength);
          nioMessageType = nioInputBuffer.get();
          nioMsgId = nioInputBuffer.getShort();
          directAck = (nioMessageType & DIRECT_ACK_BIT) != 0;
          if (directAck) {
            nioMessageType &= ~DIRECT_ACK_BIT; // clear the ack bit
          }
          // Following validation fixes bug 31145
          if (!validMsgType(nioMessageType)) {
            Integer nioMessageTypeInteger = Integer.valueOf(nioMessageType);
            logger.fatal(LocalizedMessage.create(LocalizedStrings.Connection_UNKNOWN_P2P_MESSAGE_TYPE_0, nioMessageTypeInteger));
            this.readerShuttingDown = true;
            requestClose(LocalizedStrings.Connection_UNKNOWN_P2P_MESSAGE_TYPE_0.toLocalizedString(nioMessageTypeInteger));
            break;
          }
          nioLengthSet = true;
          // keep the header "in" the buffer until we have read the entire msg.
          // Trust me: this will reduce copying on large messages.
          nioInputBuffer.position(headerStartPos);
        }
        if (remaining >= nioMessageLength+MSG_HEADER_BYTES) {
          nioLengthSet = false;
          nioInputBuffer.position(nioInputBuffer.position()+MSG_HEADER_BYTES);
          // don't trust the message deserialization to leave the position in
          // the correct spot.  Some of the serialization uses buffered
          // streams that can leave the position at the wrong spot
          int startPos = nioInputBuffer.position();
          int oldLimit = nioInputBuffer.limit();
          nioInputBuffer.limit(startPos+nioMessageLength);
          if (this.handshakeRead) {
            if (nioMessageType == NORMAL_MSG_TYPE) {
              this.owner.getConduit().stats.incMessagesBeingReceived(true, nioMessageLength);
              ByteBufferInputStream bbis = remoteVersion == null
                  ? new ByteBufferInputStream(nioInputBuffer)
                  : new VersionedByteBufferInputStream(nioInputBuffer,
                      remoteVersion);
              DistributionMessage msg = null;
              try {
                ReplyProcessor21.initMessageRPId();
                // add serialization stats
                long startSer = this.owner.getConduit().stats.startMsgDeserialization();
                msg = (DistributionMessage)InternalDataSerializer.readDSFID(bbis);
                this.owner.getConduit().stats.endMsgDeserialization(startSer);
                if (bbis.available() != 0) {
                  logger.warn(LocalizedMessage.create(
                      LocalizedStrings.Connection_MESSAGE_DESERIALIZATION_OF_0_DID_NOT_READ_1_BYTES,
                      new Object[] { msg, Integer.valueOf(bbis.available())}));
                }
                try {
                  if (!dispatchMessage(msg, nioMessageLength, directAck)) {
                    directAck = false;
                  }
                }
                catch (MemberShunnedException e) {
                  directAck = false; // don't respond (bug39117)
                }
                catch (Exception de) {
                  this.owner.getConduit().getCancelCriterion().checkCancelInProgress(de);
                  logger.fatal(LocalizedMessage.create(LocalizedStrings.Connection_ERROR_DISPATCHING_MESSAGE), de);
                }
                catch (ThreadDeath td) {
                  throw td;
                }
                catch (VirtualMachineError err) {
                  SystemFailure.initiateFailure(err);
                  // If this ever returns, rethrow the error.  We're poisoned
                  // now, so don't let this thread continue.
                  throw err;
                }
                catch (Throwable t) {
                  // Whenever you catch Error or Throwable, you must also
                  // catch VirtualMachineError (see above).  However, there is
                  // _still_ a possibility that you are dealing with a cascading
                  // error condition, so you also need to check to see if the JVM
                  // is still usable:
                  SystemFailure.checkFailure();
                  logger.fatal(LocalizedMessage.create(LocalizedStrings.Connection_THROWABLE_DISPATCHING_MESSAGE), t);
                }
              }
              catch (VirtualMachineError err) {
                SystemFailure.initiateFailure(err);
                // If this ever returns, rethrow the error.  We're poisoned
                // now, so don't let this thread continue.
                throw err;
              }
              catch (Throwable t) {
                // Whenever you catch Error or Throwable, you must also
                // catch VirtualMachineError (see above).  However, there is
                // _still_ a possibility that you are dealing with a cascading
                // error condition, so you also need to check to see if the JVM
                // is still usable:
                SystemFailure.checkFailure();
                  sendFailureReply(ReplyProcessor21.getMessageRPId(), LocalizedStrings.Connection_ERROR_DESERIALIZING_MESSAGE.toLocalizedString(), t, directAck);
                if (t instanceof ThreadDeath) {
                  throw (ThreadDeath)t;
                }
                if(t instanceof CancelException) {
                  if (!(t instanceof CacheClosedException)) {
                    // Just log a message if we had trouble deserializing due to CacheClosedException; see bug 43543
                    throw (CancelException) t;
                  }
                }
                logger.fatal(LocalizedMessage.create(LocalizedStrings.Connection_ERROR_DESERIALIZING_MESSAGE), t);
              }
              finally {
                ReplyProcessor21.clearMessageRPId();
              }
            }
            else if (nioMessageType == CHUNKED_MSG_TYPE) {
              MsgDestreamer md = obtainMsgDestreamer(nioMsgId, remoteVersion);
              this.owner.getConduit().stats.incMessagesBeingReceived(md.size() == 0, nioMessageLength);
              try {
                md.addChunk(nioInputBuffer, nioMessageLength);
              }
              catch (IOException ex) {
                logger.fatal(LocalizedMessage.create(LocalizedStrings.Connection_FAILED_HANDLING_CHUNK_MESSAGE), ex);
              }
            }
            else /* (nioMessageType == END_CHUNKED_MSG_TYPE) */ {
              //logger.info("END_CHUNK msgId="+nioMsgId);
              MsgDestreamer md = obtainMsgDestreamer(nioMsgId, remoteVersion);
              this.owner.getConduit().stats.incMessagesBeingReceived(md.size() == 0, nioMessageLength);
              try {
                md.addChunk(nioInputBuffer, nioMessageLength);
              }
              catch (IOException ex) {
                logger.fatal(LocalizedMessage.create(LocalizedStrings.Connection_FAILED_HANDLING_END_CHUNK_MESSAGE), ex);
              }
              DistributionMessage msg = null;
              int msgLength = 0;
              String failureMsg = null;
              Throwable failureEx = null;
              int rpId = 0;
              boolean interrupted = false;
              try {
                msg = md.getMessage();
              }
              catch (ClassNotFoundException ex) {
                this.owner.getConduit().stats.decMessagesBeingReceived(md.size());
                failureMsg = LocalizedStrings.Connection_CLASSNOTFOUND_DESERIALIZING_MESSAGE.toLocalizedString();
                failureEx = ex;
                rpId = md.getRPid();
                logger.fatal(LocalizedMessage.create(LocalizedStrings.Connection_CLASSNOTFOUND_DESERIALIZING_MESSAGE_0, ex));
              }
              catch (IOException ex) {
                this.owner.getConduit().stats.decMessagesBeingReceived(md.size());
                failureMsg = LocalizedStrings.Connection_IOEXCEPTION_DESERIALIZING_MESSAGE.toLocalizedString();
                failureEx = ex;
                rpId = md.getRPid();
                logger.fatal(LocalizedMessage.create(LocalizedStrings.Connection_IOEXCEPTION_DESERIALIZING_MESSAGE), failureEx);
              }
              catch (InterruptedException ex) {
                interrupted = true;
                this.owner.getConduit().getCancelCriterion().checkCancelInProgress(ex);
              } 
              catch (VirtualMachineError err) {
                SystemFailure.initiateFailure(err);
                // If this ever returns, rethrow the error.  We're poisoned
                // now, so don't let this thread continue.
                throw err;
              }
              catch (Throwable ex) {
                // Whenever you catch Error or Throwable, you must also
                // catch VirtualMachineError (see above).  However, there is
                // _still_ a possibility that you are dealing with a cascading
                // error condition, so you also need to check to see if the JVM
                // is still usable:
                SystemFailure.checkFailure();
                this.owner.getConduit().getCancelCriterion().checkCancelInProgress(ex);
                this.owner.getConduit().stats.decMessagesBeingReceived(md.size());
                failureMsg = LocalizedStrings.Connection_UNEXPECTED_FAILURE_DESERIALIZING_MESSAGE.toLocalizedString();
                failureEx = ex;
                rpId = md.getRPid();
                logger.fatal(LocalizedMessage.create(LocalizedStrings.Connection_UNEXPECTED_FAILURE_DESERIALIZING_MESSAGE), failureEx);
              }
              finally {
                msgLength = md.size();
                releaseMsgDestreamer(nioMsgId, md);
                if (interrupted) {
                  Thread.currentThread().interrupt();
                }
              }
              if (msg != null) {
                try {
                  if (!dispatchMessage(msg, msgLength, directAck)) {
                    directAck = false;
                  }
                }
                catch (MemberShunnedException e) {
                  // not a member anymore - don't reply
                  directAck = false;
                }
                catch (Exception de) {
                  this.owner.getConduit().getCancelCriterion().checkCancelInProgress(de);
                  logger.fatal(LocalizedMessage.create(LocalizedStrings.Connection_ERROR_DISPATCHING_MESSAGE), de);
                }
                catch (ThreadDeath td) {
                  throw td;
                } 
                catch (VirtualMachineError err) {
                  SystemFailure.initiateFailure(err);
                  // If this ever returns, rethrow the error.  We're poisoned
                  // now, so don't let this thread continue.
                  throw err;
                }
                catch (Throwable t) {
                  // Whenever you catch Error or Throwable, you must also
                  // catch VirtualMachineError (see above).  However, there is
                  // _still_ a possibility that you are dealing with a cascading
                  // error condition, so you also need to check to see if the JVM
                  // is still usable:
                  SystemFailure.checkFailure();
                  logger.fatal(LocalizedMessage.create(LocalizedStrings.Connection_THROWABLE_DISPATCHING_MESSAGE), t);
                }
              }
              else if (failureEx != null) {
                sendFailureReply(rpId, failureMsg, failureEx, directAck);
              }
            }
          }
          else {
            // read HANDSHAKE
            ByteBufferInputStream bbis =
              new ByteBufferInputStream(nioInputBuffer);
            DataInputStream dis = new DataInputStream(bbis);
            if (!this.isReceiver) {
              try {
                this.replyCode = dis.readUnsignedByte();
                if (this.replyCode == REPLY_CODE_OK_WITH_ASYNC_INFO) {
                  this.asyncDistributionTimeout = dis.readInt();
                  this.asyncQueueTimeout = dis.readInt();
                  this.asyncMaxQueueSize = (long)dis.readInt() * (1024*1024);
                  if (this.asyncDistributionTimeout != 0) {
                    logger.info(LocalizedMessage.create(
                        LocalizedStrings.Connection_0_ASYNC_CONFIGURATION_RECEIVED_1,
                        new Object[] {p2pReaderName(),
                          " asyncDistributionTimeout="
                          + this.asyncDistributionTimeout
                          + " asyncQueueTimeout=" + this.asyncQueueTimeout
                          + " asyncMaxQueueSize="  
                          + (this.asyncMaxQueueSize / (1024*1024))}));  
                  }
                  // read the product version ordinal for on-the-fly serialization
                  // transformations (for rolling upgrades)
                  this.remoteVersion = Version.readVersion(dis, true);
                }
              }
              catch (Exception e) {
                this.owner.getConduit().getCancelCriterion().checkCancelInProgress(e);
                logger.fatal(LocalizedMessage.create(LocalizedStrings.Connection_ERROR_DESERIALIZING_P2P_HANDSHAKE_REPLY), e);
                this.readerShuttingDown = true;
                requestClose(LocalizedStrings.Connection_ERROR_DESERIALIZING_P2P_HANDSHAKE_REPLY.toLocalizedString());
                return;
              }
              catch (ThreadDeath td) {
                throw td;
              } 
              catch (VirtualMachineError err) {
                SystemFailure.initiateFailure(err);
                // If this ever returns, rethrow the error.  We're poisoned
                // now, so don't let this thread continue.
                throw err;
              }
              catch (Throwable t) {
                // Whenever you catch Error or Throwable, you must also
                // catch VirtualMachineError (see above).  However, there is
                // _still_ a possibility that you are dealing with a cascading
                // error condition, so you also need to check to see if the JVM
                // is still usable:
                SystemFailure.checkFailure();
                logger.fatal(LocalizedMessage.create(LocalizedStrings.Connection_THROWABLE_DESERIALIZING_P2P_HANDSHAKE_REPLY), t);
                this.readerShuttingDown = true;
                requestClose(LocalizedStrings.Connection_THROWABLE_DESERIALIZING_P2P_HANDSHAKE_REPLY.toLocalizedString());
                return;
              }
              if (this.replyCode != REPLY_CODE_OK && this.replyCode != REPLY_CODE_OK_WITH_ASYNC_INFO) {
                 StringId err = LocalizedStrings.Connection_UNKNOWN_HANDSHAKE_REPLY_CODE_0_NIOMESSAGELENGTH_1_PROCESSORTYPE_2;
                 Object[] errArgs = new Object[] {Integer.valueOf(this.replyCode), Integer.valueOf(nioMessageLength)}; 
                if (replyCode == 0 && logger.isDebugEnabled()) { // bug 37113
                  logger.debug(err.toLocalizedString(errArgs) + " (peer probably departed ungracefully)");
                }
                else {
                  logger.fatal(LocalizedMessage.create(err, errArgs));
                }
                this.readerShuttingDown = true;
                requestClose(err.toLocalizedString(errArgs));
                return;
              }
              notifyHandshakeWaiter(true);
            }
            else {
              try {
                byte b = dis.readByte();
                if (b != 0) {
                  throw new IllegalStateException(LocalizedStrings.Connection_DETECTED_OLD_VERSION_PRE_501_OF_GEMFIRE_OR_NONGEMFIRE_DURING_HANDSHAKE_DUE_TO_INITIAL_BYTE_BEING_0.toLocalizedString(new Byte(b)));
                }
                byte handShakeByte = dis.readByte();
                if (handShakeByte != HANDSHAKE_VERSION) {
                  throw new IllegalStateException(LocalizedStrings.Connection_DETECTED_WRONG_VERSION_OF_GEMFIRE_PRODUCT_DURING_HANDSHAKE_EXPECTED_0_BUT_FOUND_1.toLocalizedString(new Object[] {new Byte(HANDSHAKE_VERSION), new Byte(handShakeByte)}));
                }
                InternalDistributedMember remote = DSFIDFactory.readInternalDistributedMember(dis);
                Stub stub = new Stub(remote.getIpAddress()/*fix for bug 33615*/, remote.getDirectChannelPort(), remote.getVmViewId());
                setRemoteAddr(remote, stub);
                this.sharedResource = dis.readBoolean();
                this.preserveOrder = dis.readBoolean();
                this.uniqueId = dis.readLong();
                // read the product version ordinal for on-the-fly serialization
                // transformations (for rolling upgrades)
                this.remoteVersion = Version.readVersion(dis, true);
                int dominoNumber = 0;
                if (this.remoteVersion == null || 
                    (this.remoteVersion.compareTo(Version.GFE_80) >= 0)) {
                  dominoNumber = dis.readInt();
                  if (this.sharedResource) {
                    dominoNumber = 0;
                  }
                  dominoCount.set(dominoNumber);
//                  this.senderName = dis.readUTF();
                }
                if (!this.sharedResource) {
                  if (tipDomino()) {
                    logger.info(LocalizedMessage.create(
                      LocalizedStrings.Connection_THREAD_OWNED_RECEIVER_FORCING_ITSELF_TO_SEND_ON_THREAD_OWNED_SOCKETS));
// bug #49565 - if domino count is >= 2 use shared resources.
// Also see DistributedCacheOperation#supportsDirectAck
                  } else { //if (dominoNumber < 2) {
                    ConnectionTable.threadWantsOwnResources();
                    if (logger.isDebugEnabled()) {
                      logger.debug("thread-owned receiver with domino count of {} will prefer sending on thread-owned sockets", dominoNumber);
                    }
//                  } else {
//                    ConnectionTable.threadWantsSharedResources();
                  }
                  this.owner.owner.stats.incThreadOwnedReceivers(1L, dominoNumber);
                  //Because this thread is not shared resource, it will be used for direct
                  //ack. Direct ack messages can be large. This call will resize the send
                  //buffer.
                  setSendBufferSize(this.socket);
                }
//                String name = owner.getDM().getConfig().getName();
//                if (name == null) {
//                  name = "pid="+OSProcess.getId();
//                }
                Thread.currentThread().setName(
//                    (!this.sharedResource && this.senderName != null? ("<"+this.senderName+"> -> ") : "") +
//                     "[" + name + "] "+
                                     "P2P message reader for " + this.remoteAddr
                                               + " " + (this.sharedResource?"":"un") + "shared"
                                               + " " + (this.preserveOrder?"":"un") + "ordered"
                                               + " uid=" + this.uniqueId
                                               + (dominoNumber>0? (" dom #" + dominoNumber) : "")
                                               + " port=" + this.socket.getPort());
              }
              catch (Exception e) {
                this.owner.getConduit().getCancelCriterion().checkCancelInProgress(e); // bug 37101
                logger.fatal(LocalizedMessage.create(LocalizedStrings.Connection_ERROR_DESERIALIZING_P2P_HANDSHAKE_MESSAGE), e);
                this.readerShuttingDown = true;
                requestClose(LocalizedStrings.Connection_ERROR_DESERIALIZING_P2P_HANDSHAKE_MESSAGE.toLocalizedString());
                return;
              }
              if (logger.isDebugEnabled()) {
                logger.debug("P2P handshake remoteId is {}{}", this.remoteId,
                    (this.remoteVersion != null ? " (" + this.remoteVersion + ')' : ""));
              }
              try {
                String authInit = System
                    .getProperty(DistributionConfigImpl.SECURITY_SYSTEM_PREFIX
                        + DistributionConfig.SECURITY_PEER_AUTH_INIT_NAME);
                boolean isSecure = authInit!= null && authInit.length() != 0 ;

                if (isSecure) {
                  if (owner.getConduit()
                      .waitForMembershipCheck(this.remoteAddr)) {
                    sendOKHandshakeReply(); // fix for bug 33224
                    notifyHandshakeWaiter(true);
                  }
                  else {
                    // ARB: check if we need notifyHandshakeWaiter() call.
                    notifyHandshakeWaiter(false);
                    logger.warn(LocalizedMessage.create(
                      LocalizedStrings.Connection_0_TIMED_OUT_DURING_A_MEMBERSHIP_CHECK, p2pReaderName()));
                    return;
                  }
                }
                else {
                  sendOKHandshakeReply(); // fix for bug 33224
                  try {
                    notifyHandshakeWaiter(true);
                  }
                  catch (Exception e) {
                    logger.fatal(LocalizedMessage.create(LocalizedStrings.Connection_UNCAUGHT_EXCEPTION_FROM_LISTENER), e);
                  }
                }
              }
              catch (IOException ex) {
                final String err = LocalizedStrings.Connection_FAILED_SENDING_HANDSHAKE_REPLY.toLocalizedString();
                if (logger.isDebugEnabled()) {
                  logger.debug(err, ex);
                }
                this.readerShuttingDown = true;
                requestClose(err + ": " + ex);
                return;
              }
            }
          }
          if (!connected) {
            continue;
          }
          accessed();
          nioInputBuffer.limit(oldLimit);
          nioInputBuffer.position(startPos + nioMessageLength);
        }
        else {
          done = true;
          compactOrResizeBuffer(nioMessageLength);
        }
      }
      else {
        done = true;
        if (nioInputBuffer.position() != 0) {
          nioInputBuffer.compact();
        }
        else {
          nioInputBuffer.position(nioInputBuffer.limit());
          nioInputBuffer.limit(nioInputBuffer.capacity());
        }
      }
    }
  }
  private void compactOrResizeBuffer(int messageLength) {
    final int oldBufferSize = nioInputBuffer.capacity();
    final DMStats stats = this.owner.getConduit().stats;
    int allocSize = messageLength+MSG_HEADER_BYTES;
    if (oldBufferSize < allocSize) {
      // need a bigger buffer
      logger.info(LocalizedMessage.create(
        LocalizedStrings.Connection_ALLOCATING_LARGER_NETWORK_READ_BUFFER_NEW_SIZE_IS_0_OLD_SIZE_WAS_1,
        new Object[] { Integer.valueOf(allocSize), Integer.valueOf(oldBufferSize)}));
      ByteBuffer oldBuffer = nioInputBuffer;
      nioInputBuffer = Buffers.acquireReceiveBuffer(allocSize,  stats);

      if(oldBuffer != null) {
        int oldByteCount = oldBuffer.remaining(); // needed to workaround JRockit 1.4.2.04 bug
        nioInputBuffer.put(oldBuffer);
        nioInputBuffer.position(oldByteCount); // workaround JRockit 1.4.2.04 bug
      Buffers.releaseReceiveBuffer(oldBuffer,  stats);
      }
    }
    else {
      if (nioInputBuffer.position() != 0) {
        nioInputBuffer.compact();
      }
      else {
        nioInputBuffer.position(nioInputBuffer.limit());
        nioInputBuffer.limit(nioInputBuffer.capacity());
      }
    }
  }
  private boolean dispatchMessage(DistributionMessage msg, int bytesRead, boolean directAck) {
    try {
      msg.setDoDecMessagesBeingReceived(true);
      if(directAck) {
        Assert.assertTrue(!isSharedResource(), "We were asked to send a direct reply on a shared socket");
        //TODO dirack we should resize the send buffer if we know this socket is used for direct ack.
        msg.setReplySender(new DirectReplySender(this));
      }
      this.owner.getConduit().messageReceived(this, msg, bytesRead);
      return true;
    } finally {
      if (msg.containsRegionContentChange()) {
        messagesReceived++;
      }
    }
  }

  protected Socket getSocket() throws SocketException {
    // fix for bug 37286
    Socket result = this.socket;
    if (result == null) {
      throw new SocketException(LocalizedStrings.Connection_SOCKET_HAS_BEEN_CLOSED.toLocalizedString());
    }
    return result;
  }
  public boolean isSocketClosed() {
    return this.socket.isClosed() || !this.socket.isConnected();
  }
  private boolean isSocketInUse() {
    return this.socketInUse;
  }


  protected final void accessed() {
    this.accessed = true;
  }

  /** returns the ConnectionKey stub representing the other side of
      this connection (host:port) */
  public final Stub getRemoteId() {
    return remoteId;
  }

  /** return the DM id of the guy on the other side of this connection.
   */
  public final InternalDistributedMember getRemoteAddress() {
    return this.remoteAddr;
  }

  /**
   * Return the version of the guy on the other side of this connection.
   */
  public final Version getRemoteVersion() {
    return this.remoteVersion;
  }

  @Override
  public String toString() {
    return String.valueOf(remoteAddr) + '@' + this.uniqueId
        + (this.remoteVersion != null ? ('(' + this.remoteVersion.toString()
            + ')') : "") /*DEBUG + " accepted=" + this.isReceiver + " connected=" + this.connected + " hash=" + System.identityHashCode(this) + " preserveOrder=" + this.preserveOrder +
    " closing=" + isClosing() + ">"*/;
  }

  /**
   * answers whether this connection was initiated in this vm
   * @return true if the connection was initiated here
   * @since 5.1
   */
  protected boolean getOriginatedHere() {
    return !this.isReceiver;
  }

  /**
   * answers whether this connection is used for ordered message delivery
   */
  protected boolean getPreserveOrder() {
    return preserveOrder;
  }

  /**
   * answers the unique ID of this connection in the originating VM
   */
  protected long getUniqueId() {
    return this.uniqueId;
  }

  /**
   * answers the number of messages received by this connection
   */
  protected long getMessagesReceived() {
    return messagesReceived;
  }

  /**
   * answers the number of messages sent on this connection
   */
  protected long getMessagesSent() {
    return messagesSent;
  }
  public void acquireSendPermission() throws ConnectionException {
    if (!this.connected) {
      throw new ConnectionException(LocalizedStrings.Connection_CONNECTION_IS_CLOSED.toLocalizedString());
    }
    if (isReaderThread()) {
      // reader threads send replies and we always want to permit those without waiting
      return;
    }
    // @todo darrel: add some stats
    boolean interrupted = false;
    try {
      for (;;) {
        this.owner.getConduit().getCancelCriterion().checkCancelInProgress(null);
        try {
          this.senderSem.acquire();
          break;
        } catch (InterruptedException ex) {
          interrupted = true;
        }
      } // for
    }
    finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
    if (!this.connected) {
      this.senderSem.release();
      this.owner.getConduit().getCancelCriterion().checkCancelInProgress(null); // bug 37101
      throw new ConnectionException(LocalizedStrings.Connection_CONNECTION_IS_CLOSED.toLocalizedString());
    }
  }
  public void releaseSendPermission() {
    if (isReaderThread()) {
      return;
    }
    this.senderSem.release();
  }
  private void closeSenderSem() {
    // All we need to do is increase the number of permits by one
    // just in case 1 or more guys are currently waiting to acquire.
    // One of them will get it and then find out the connection is closed
    // and then he will release it until all guys currently waiting to acquire
    // will complete by throwing a ConnectionException.
    releaseSendPermission();
  }
  
  boolean nioChecked;
  boolean useNIO;
  
  private final boolean useNIO() {
    if (TCPConduit.useSSL) {
      return false;
    }
    if (this.nioChecked) {
      return this.useNIO;
    }
    this.nioChecked = true;
    this.useNIO = this.owner.getConduit().useNIO();
    if (!this.useNIO) {
      return false;
    }
    // JDK bug 6230761 - NIO can't be used with IPv6 on Windows
    if (this.socket != null && (this.socket.getInetAddress() instanceof Inet6Address)) {
      String os = System.getProperty("os.name");
      if (os != null) {
        if (os.indexOf("Windows") != -1) {
          this.useNIO = false;
        }
      }
    }
    return this.useNIO;
  }
}
