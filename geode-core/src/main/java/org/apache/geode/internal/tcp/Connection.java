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

import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_PEER_AUTH_INIT;
import static org.apache.geode.internal.net.SSLConfigurationFactory.getSSLConfigForComponent;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.ConnectException;
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

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.SerializationException;
import org.apache.geode.SystemFailure;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.annotations.internal.MutableForTesting;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.ConflationKey;
import org.apache.geode.distributed.internal.DMStats;
import org.apache.geode.distributed.internal.DirectReplyProcessor;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.ReplySender;
import org.apache.geode.distributed.internal.direct.DirectChannel;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.MembershipManager;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.DSFIDFactory;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.SystemTimer;
import org.apache.geode.internal.SystemTimer.SystemTimerTask;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.alerting.AlertingAction;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.LoggingThread;
import org.apache.geode.internal.net.BufferPool;
import org.apache.geode.internal.net.NioFilter;
import org.apache.geode.internal.net.NioPlainEngine;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.internal.tcp.MsgReader.Header;
import org.apache.geode.internal.util.concurrent.ReentrantSemaphore;

/**
 * Connection is a socket holder that sends and receives serialized message objects. A Connection
 * may be closed to preserve system resources and will automatically be reopened when it's needed.
 *
 * @since GemFire 2.0
 */
public class Connection implements Runnable {
  private static final Logger logger = LogService.getLogger();
  public static final String THREAD_KIND_IDENTIFIER = "P2P message reader";

  @MakeNotStatic
  private static int P2P_CONNECT_TIMEOUT;
  @MakeNotStatic
  private static boolean IS_P2P_CONNECT_TIMEOUT_INITIALIZED = false;

  static final int NORMAL_MSG_TYPE = 0x4c;
  static final int CHUNKED_MSG_TYPE = 0x4d; // a chunk of one logical msg
  static final int END_CHUNKED_MSG_TYPE = 0x4e; // last in a series of chunks
  static final int DIRECT_ACK_BIT = 0x20;

  static final int MSG_HEADER_SIZE_OFFSET = 0;
  static final int MSG_HEADER_TYPE_OFFSET = 4;
  static final int MSG_HEADER_ID_OFFSET = 5;
  static final int MSG_HEADER_BYTES = 7;

  /**
   * Small buffer used for send socket buffer on receiver connections and receive buffer on sender
   * connections.
   */
  public static final int SMALL_BUFFER_SIZE =
      Integer.getInteger(DistributionConfig.GEMFIRE_PREFIX + "SMALL_BUFFER_SIZE", 4096);

  /** counter to give connections a unique id */
  @MakeNotStatic
  private static final AtomicLong idCounter = new AtomicLong(1);

  /** string used as the reason for initiating suspect processing */
  public static final String INITIATING_SUSPECT_PROCESSING =
      "member unexpectedly shut down shared, unordered connection";

  /** the table holding this connection */
  private final ConnectionTable owner;

  private final TCPConduit conduit;
  private NioFilter ioFilter;

  /**
   * Set to false once run() is terminating. Using this instead of Thread.isAlive as the reader
   * thread may be a pooled thread.
   */
  private volatile boolean isRunning = false;

  /** true if connection is a shared resource that can be used by more than one thread */
  private boolean sharedResource;

  public boolean isSharedResource() {
    return this.sharedResource;
  }

  /** The idle timeout timer task for this connection */
  private SystemTimerTask idleTask;

  private static final ThreadLocal<Boolean> isReaderThread = new ThreadLocal<Boolean>() {
    @Override
    public Boolean initialValue() {
      return Boolean.FALSE;
    }
  };

  public static void makeReaderThread() {
    // mark this thread as a reader thread
    makeReaderThread(true);
  }

  private static void makeReaderThread(boolean v) {
    isReaderThread.set(v);
  }

  // return true if this thread is a reader thread
  private static boolean isReaderThread() {
    return isReaderThread.get();
  }

  private int getP2PConnectTimeout() {
    if (IS_P2P_CONNECT_TIMEOUT_INITIALIZED)
      return P2P_CONNECT_TIMEOUT;
    String connectTimeoutStr = System.getProperty("p2p.connectTimeout");
    if (connectTimeoutStr != null) {
      P2P_CONNECT_TIMEOUT = Integer.parseInt(connectTimeoutStr);
    } else {
      P2P_CONNECT_TIMEOUT = 6 * this.conduit.getDM().getConfig().getMemberTimeout();
    }
    IS_P2P_CONNECT_TIMEOUT_INITIALIZED = true;
    return P2P_CONNECT_TIMEOUT;
  }

  /**
   * If true then readers for thread owned sockets will send all messages on thread owned senders.
   * Even normally unordered msgs get send on TO socks.
   */
  private static final boolean DOMINO_THREAD_OWNED_SOCKETS =
      Boolean.getBoolean("p2p.ENABLE_DOMINO_THREAD_OWNED_SOCKETS");

  private static final ThreadLocal<Boolean> isDominoThread = new ThreadLocal<Boolean>() {
    @Override
    public Boolean initialValue() {
      return Boolean.FALSE;
    }
  };

  // return true if this thread is a reader thread
  private static boolean tipDomino() {
    if (DOMINO_THREAD_OWNED_SOCKETS) {
      // mark this thread as one who wants to send ALL on TO sockets
      ConnectionTable.threadWantsOwnResources();
      isDominoThread.set(Boolean.TRUE);
      return true;
    } else {
      return false;
    }
  }

  public static boolean isDominoThread() {
    return isDominoThread.get();
  }

  /** the socket entrusted to this connection */
  private final Socket socket;

  /** output stream/channel lock */
  private final Object outLock = new Object();

  /** the ID string of the conduit (for logging) */
  private String conduitIdStr;

  /** Identifies the java group member on the other side of the connection. */
  InternalDistributedMember remoteAddr;

  /**
   * Identifies the version of the member on the other side of the connection.
   */
  private Version remoteVersion;

  /**
   * True if this connection was accepted by a listening socket. This makes it a receiver. False if
   * this connection was explicitly created by a connect call. This makes it a sender.
   */
  private final boolean isReceiver;

  /**
   * count of how many unshared p2p-readers removed from the original action this thread is. For
   * instance, server-connection -> owned p2p reader (count 0) -> owned p2p reader (count 1) ->
   * owned p2p reader (count 2). This shows up in thread names as "DOM #x" (domino #x)
   */
  private static final ThreadLocal<Integer> dominoCount = new ThreadLocal<Integer>() {
    @Override
    protected Integer initialValue() {
      return 0;
    }
  };

  /**
   * How long to wait if receiver will not accept a message before we go into queue mode.
   *
   * @since GemFire 4.2.2
   */
  private int asyncDistributionTimeout = 0;

  /**
   * How long to wait, with the receiver not accepting any messages, before kicking the receiver out
   * of the distributed system. Ignored if asyncDistributionTimeout is zero.
   *
   * @since GemFire 4.2.2
   */
  private int asyncQueueTimeout = 0;

  /**
   * How much queued data we can have, with the receiver not accepting any messages, before kicking
   * the receiver out of the distributed system. Ignored if asyncDistributionTimeout is zero.
   * Canonicalized to bytes (property file has it as megabytes
   *
   * @since GemFire 4.2.2
   */
  private long asyncMaxQueueSize = 0;

  /**
   * True if an async queue is already being filled.
   */
  private volatile boolean asyncQueuingInProgress = false;

  /**
   * Maps ConflatedKey instances to ConflatedKey instance. Note that even though the key and value
   * for an entry is the map will always be "equal" they will not always be "==".
   */
  private final Map conflatedKeys = new HashMap();

  // NOTE: LinkedBlockingQueue has a bug in which removes from the queue
  // cause future offer to increase the size without adding anything to the queue.
  // So I've changed from this backport class to a java.util.LinkedList
  private final LinkedList outgoingQueue = new LinkedList();

  /**
   * Number of bytes in the outgoingQueue. Used to control capacity.
   */
  private long queuedBytes;

  /** used for async writes */
  private Thread pusherThread;

  /**
   * The maximum number of concurrent senders sending a message to a single recipient.
   */
  private static final int MAX_SENDERS = Integer
      .getInteger("p2p.maxConnectionSenders", DirectChannel.DEFAULT_CONCURRENCY_LEVEL);
  /**
   * This semaphore is used to throttle how many threads will try to do sends on this connection
   * concurrently. A thread must acquire this semaphore before it is allowed to start serializing
   * its message.
   */
  private final Semaphore senderSem = new ReentrantSemaphore(MAX_SENDERS);

  /** Set to true once the handshake has been read */
  private volatile boolean handshakeRead;
  private volatile boolean handshakeCancelled;

  private volatile int replyCode;

  private static final byte REPLY_CODE_OK = (byte) 69;
  private static final byte REPLY_CODE_OK_WITH_ASYNC_INFO = (byte) 70;

  private final Object handshakeSync = new Object();

  /** message reader thread */
  private volatile Thread readerThread;

  /** whether the reader thread is, or should be, running */
  volatile boolean stopped = true;

  /** set to true once a close begins */
  private final AtomicBoolean closing = new AtomicBoolean(false);

  private volatile boolean readerShuttingDown = false;

  /** whether the socket is connected */
  volatile boolean connected = false;

  /**
   * Set to true once a connection finishes its constructor
   */
  private volatile boolean finishedConnecting = false;

  private volatile boolean accessed = true;
  private volatile boolean socketInUse = false;
  volatile boolean timedOut = false;

  /**
   * task for detecting ack timeouts and issuing alerts
   */
  private SystemTimer.SystemTimerTask ackTimeoutTask;

  /**
   * millisecond clock at the time message transmission started, if doing forced-disconnect
   * processing
   */
  private long transmissionStartTime;

  /** ack wait timeout - if socketInUse, use this to trigger SUSPECT processing */
  private long ackWaitTimeout;

  /** ack severe alert timeout - if socketInUse, use this to send alert */
  private long ackSATimeout;

  /**
   * other connections participating in the current transmission. we notify them if ackSATimeout
   * expires to keep all members from generating alerts when only one is slow
   */
  private List ackConnectionGroup;

  /** name of thread that we're currently performing an operation in (may be null) */
  private String ackThreadName;

  /** the buffer used for message receipt */
  private ByteBuffer inputBuffer;

  /** the length of the next message to be dispatched */
  private int messageLength;

  /** the type of message being received */
  private byte messageType;

  /**
   * when messages are chunked by a MsgStreamer we track the destreamers on
   * the receiving side using a message identifier
   */
  private short messageId;

  /** whether the length of the next message has been established */
  private boolean lengthSet = false;

  /** used to lock access to destreamer data */
  private final Object destreamerLock = new Object();

  /** caches a msg destreamer that is currently not being used */
  private MsgDestreamer idleMsgDestreamer;

  /**
   * used to map a msgId to a MsgDestreamer which are used for destreaming chunked messages
   */
  private HashMap destreamerMap;

  private boolean directAck;


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

  private void setSocketBufferSize(Socket sock, boolean send, int requestedSize) {
    if (requestedSize > 0) {
      try {
        int currentSize = send ? sock.getSendBufferSize() : sock.getReceiveBufferSize();
        if (currentSize == requestedSize) {
          if (send) {
            this.sendBufferSize = currentSize;
          }
          return;
        }
        if (send) {
          sock.setSendBufferSize(requestedSize);
        } else {
          sock.setReceiveBufferSize(requestedSize);
        }
      } catch (SocketException ignore) {
      }
      try {
        int actualSize = send ? sock.getSendBufferSize() : sock.getReceiveBufferSize();
        if (send) {
          this.sendBufferSize = actualSize;
        } else {
          this.recvBufferSize = actualSize;
        }
        if (actualSize < requestedSize) {
          logger.info("Socket {} is {} instead of the requested {}.",
              (send ? "send buffer size" : "receive buffer size"),
              actualSize, requestedSize);
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
  int getSendBufferSize() {
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

  /**
   * creates a "reader" connection that we accepted (it was initiated by an explicit connect being
   * done on
   * the other side).
   */
  protected Connection(ConnectionTable t, Socket socket)
      throws ConnectionException {
    if (t == null) {
      throw new IllegalArgumentException(
          "Null ConnectionTable");
    }
    this.conduit = t.getConduit();
    this.isReceiver = true;
    this.owner = t;
    this.socket = socket;
    this.conduitIdStr = owner.getConduit().getSocketId().toString();
    this.handshakeRead = false;
    this.handshakeCancelled = false;
    this.connected = true;

    try {
      socket.setTcpNoDelay(true);
      socket.setKeepAlive(true);
      setSendBufferSize(socket, SMALL_BUFFER_SIZE);
      setReceiveBufferSize(socket);
    } catch (SocketException e) {
      // unable to get the settings we want. Don't log an error because it will
      // likely happen a lot
    }
  }

  void initReceiver() {
    this.startReader(owner);
  }

  void setIdleTimeoutTask(SystemTimerTask task) {
    this.idleTask = task;
  }


  /**
   * Returns true if an idle connection was detected.
   */
  boolean checkForIdleTimeout() {
    if (isSocketClosed()) {
      return true;
    }
    if (isSocketInUse() || (this.sharedResource && !this.preserveOrder)) { // shared/unordered
                                                                           // connections are used
                                                                           // for failure-detection
                                                                           // and are not subject to
                                                                           // idle-timeout
      return false;
    }
    boolean isIdle = !this.accessed;
    this.accessed = false;
    if (isIdle) {
      this.timedOut = true;
      this.owner.getConduit().getStats().incLostLease();
      if (logger.isDebugEnabled()) {
        logger.debug("Closing idle connection {} shared={} ordered={}", this, this.sharedResource,
            this.preserveOrder);
      }
      try {
        // Instead of calling requestClose
        // we call closeForReconnect.
        // We don't want this timeout close to close
        // any other connections. The problem with
        // requestClose has removeEndpoint set to true
        // which will close an receivers we have if this
        // connection is a shared one.
        closeForReconnect(
            "idle connection timed out");
      } catch (Exception ignore) {
      }
    }
    return isIdle;
  }

  @MakeNotStatic
  private static final ByteBuffer okHandshakeBuf;
  static {
    int msglen = 1; // one byte for reply code
    byte[] bytes = new byte[MSG_HEADER_BYTES + msglen];
    msglen = calcHdrSize(msglen);
    bytes[MSG_HEADER_SIZE_OFFSET] = (byte) ((msglen / 0x1000000) & 0xff);
    bytes[MSG_HEADER_SIZE_OFFSET + 1] = (byte) ((msglen / 0x10000) & 0xff);
    bytes[MSG_HEADER_SIZE_OFFSET + 2] = (byte) ((msglen / 0x100) & 0xff);
    bytes[MSG_HEADER_SIZE_OFFSET + 3] = (byte) (msglen & 0xff);
    bytes[MSG_HEADER_TYPE_OFFSET] = (byte) NORMAL_MSG_TYPE; // message type
    bytes[MSG_HEADER_ID_OFFSET] = (byte) ((MsgIdGenerator.NO_MSG_ID >> 8) & 0xff);
    bytes[MSG_HEADER_ID_OFFSET + 1] = (byte) (MsgIdGenerator.NO_MSG_ID & 0xff);
    bytes[MSG_HEADER_BYTES] = REPLY_CODE_OK;
    int allocSize = bytes.length;
    ByteBuffer bb;
    if (BufferPool.useDirectBuffers) {
      bb = ByteBuffer.allocateDirect(allocSize);
    } else {
      bb = ByteBuffer.allocate(allocSize);
    }
    bb.put(bytes);
    okHandshakeBuf = bb;
  }

  /**
   * maximum message buffer size
   */
  public static final int MAX_MSG_SIZE = 0x00ffffff;

  static int calcHdrSize(int byteSize) {
    if (byteSize > MAX_MSG_SIZE) {
      throw new IllegalStateException(String.format("tcp message exceeded max size of %s",
          MAX_MSG_SIZE));
    }
    int hdrSize = byteSize;
    hdrSize |= (HANDSHAKE_VERSION << 24);
    return hdrSize;
  }

  static int calcMsgByteSize(int hdrSize) {
    return hdrSize & MAX_MSG_SIZE;
  }

  static byte calcHdrVersion(int hdrSize) throws IOException {
    byte ver = (byte) (hdrSize >> 24);
    if (ver != HANDSHAKE_VERSION) {
      throw new IOException(
          String.format(
              "Detected wrong version of GemFire product during handshake. Expected %s but found %s",
              HANDSHAKE_VERSION, ver));
    }
    return ver;
  }

  private void sendOKHandshakeReply() throws IOException, ConnectionException {
    ByteBuffer my_okHandshakeBuf;
    if (this.isReceiver) {
      DistributionConfig cfg = owner.getConduit().config;
      ByteBuffer bb;
      if (BufferPool.useDirectBuffers) {
        bb = ByteBuffer.allocateDirect(128);
      } else {
        bb = ByteBuffer.allocate(128);
      }
      bb.putInt(0); // reserve first 4 bytes for packet length
      bb.put((byte) NORMAL_MSG_TYPE);
      bb.putShort(MsgIdGenerator.NO_MSG_ID);
      bb.put(REPLY_CODE_OK_WITH_ASYNC_INFO);
      bb.putInt(cfg.getAsyncDistributionTimeout());
      bb.putInt(cfg.getAsyncQueueTimeout());
      bb.putInt(cfg.getAsyncMaxQueueSize());
      // write own product version
      Version.writeOrdinal(bb, Version.CURRENT.ordinal(), true);
      // now set the msg length into position 0
      bb.putInt(0, calcHdrSize(bb.position() - MSG_HEADER_BYTES));
      my_okHandshakeBuf = bb;
      bb.flip();
    } else {
      my_okHandshakeBuf = okHandshakeBuf;
    }
    my_okHandshakeBuf.position(0);
    writeFully(getSocket().getChannel(), my_okHandshakeBuf, false, null);
  }

  private static final int HANDSHAKE_TIMEOUT_MS =
      Integer.getInteger("p2p.handshakeTimeoutMs", 59000);
  // private static final byte HANDSHAKE_VERSION = 1; // 501
  // public static final byte HANDSHAKE_VERSION = 2; // cbb5x_PerfScale
  // public static final byte HANDSHAKE_VERSION = 3; // durable_client
  // public static final byte HANDSHAKE_VERSION = 4; // dataSerialMay19
  // public static final byte HANDSHAKE_VERSION = 5; // split-brain bits
  // public static final byte HANDSHAKE_VERSION = 6; // direct ack changes
  // NOTICE: handshake_version should not be changed anymore. Use the gemfire
  // version transmitted with the handshake bits and handle old handshakes
  // based on that
  private static final byte HANDSHAKE_VERSION = 7; // product version exchange during handshake

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
          reason = "unknown";
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
              reason = "handshake timed out";
              String peerName;
              if (this.remoteAddr != null) {
                peerName = this.remoteAddr.toString();
                // late in the life of jdk 1.7 we started seeing connections accepted
                // when accept() was not even being called. This started causing timeouts
                // to occur in the handshake threads instead of causing failures in
                // connection-formation. So, we need to initiate suspect processing here
                owner.getDM().getMembershipManager().suspectMember(this.remoteAddr,
                    String.format(
                        "Connection handshake with %s timed out after waiting %s milliseconds.",

                        peerName, HANDSHAKE_TIMEOUT_MS));
              } else {
                peerName = "socket " + this.socket.getRemoteSocketAddress().toString() + ":"
                    + this.socket.getPort();
              }
              throw new ConnectionException(
                  String.format(
                      "Connection handshake with %s timed out after waiting %s milliseconds.",
                      peerName, HANDSHAKE_TIMEOUT_MS));
            } else {
              success = this.handshakeRead;
            }
          } catch (InterruptedException ex) {
            interrupted = true;
            this.owner.getConduit().getCancelCriterion().checkCancelInProgress(ex);
            reason = "interrupted";
          } finally {
            if (interrupted) {
              Thread.currentThread().interrupt();
            }
            if (success) {
              if (this.isReceiver) {
                needToClose =
                    !owner.getConduit().getMembershipManager().addSurpriseMember(this.remoteAddr);
                if (needToClose) {
                  reason = "this member is shunned";
                }
              }
            } else {
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
        } catch (Exception ignore) {
        }
      }
    }
  }

  private void notifyHandshakeWaiter(boolean success) {
    if (getConduit().useSSL() && ioFilter != null) {
      // clear out any remaining handshake bytes
      ByteBuffer buffer = ioFilter.getUnwrappedBuffer(inputBuffer);
      buffer.position(0).limit(0);
    }
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
   * @param beingSickForTests test hook to simulate sickness in communications & membership
   */
  private void asyncClose(boolean beingSickForTests) {
    // note: remoteAddr may be null if this is a receiver that hasn't finished its handshake

    // we do the close in a background thread because the operation may hang if
    // there is a problem with the network. See bug #46659

    // if simulating sickness, sockets must be closed in-line so that tests know
    // that the vm is sick when the beSick operation completes
    if (beingSickForTests) {
      prepareForAsyncClose();
    } else {
      if (this.asyncCloseCalled.compareAndSet(false, true)) {
        Socket s = this.socket;
        if (s != null && !s.isClosed()) {
          prepareForAsyncClose();
          this.owner.getSocketCloser().asyncClose(s, String.valueOf(this.remoteAddr),
              () -> ioFilter.close(s.getChannel()));
        }
      }
    }
  }

  private void prepareForAsyncClose() {
    synchronized (stateLock) {
      if (readerThread != null && isRunning && !readerShuttingDown
          && (connectionState == STATE_READING || connectionState == STATE_READING_ACK)) {
        readerThread.interrupt();
      }
    }
  }

  private static final int CONNECT_HANDSHAKE_SIZE = 4096;

  /**
   * waits until we've joined the distributed system before returning
   */
  private void waitForAddressCompletion() {
    InternalDistributedMember myAddr = this.owner.getConduit().getMemberId();
    synchronized (myAddr) {
      while ((!owner.getConduit().getCancelCriterion().isCancelInProgress())
          && myAddr.getInetAddress() == null && myAddr.getVmViewId() < 0) {
        try {
          myAddr.wait(100); // spurious wakeup ok
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          this.owner.getConduit().getCancelCriterion().checkCancelInProgress(ie);
        }
      }
      Assert.assertTrue(myAddr.getDirectChannelPort() == this.owner.getConduit().getPort());
    }
  }

  private void handshakeFromNewSender() throws IOException {
    waitForAddressCompletion();

    InternalDistributedMember myAddr = this.owner.getConduit().getMemberId();
    final MsgOutputStream connectHandshake = new MsgOutputStream(CONNECT_HANDSHAKE_SIZE);
    /*
     * Note a byte of zero is always written because old products serialized a member id with always
     * sends an ip address. My reading of the ip-address specs indicated that the first byte of a
     * valid address would never be 0.
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
    connectHandshake.writeInt(dominoCount.get() + 1);
    // this writes the sending member + thread name that is stored in senderName
    // on the receiver to show the cause of reader thread creation
    // if (dominoCount.get() > 0) {
    // connectHandshake.writeUTF(Thread.currentThread().getName());
    // } else {
    // String name = owner.getDM().getConfig().getName();
    // if (name == null) {
    // name = "pid="+OSProcess.getId();
    // }
    // connectHandshake.writeUTF("["+name+"] "+Thread.currentThread().getName());
    // }
    connectHandshake.setMessageHeader(NORMAL_MSG_TYPE, ClusterDistributionManager.STANDARD_EXECUTOR,
        MsgIdGenerator.NO_MSG_ID);
    writeFully(getSocket().getChannel(), connectHandshake.getContentBuffer(), false, null);
  }

  /**
   *
   * @throws IOException if handshake fails
   */
  private void attemptHandshake(ConnectionTable connTable) throws IOException {
    // send HANDSHAKE
    // send this member's information. It's expected on the other side
    if (logger.isDebugEnabled()) {
      logger.debug("starting peer-to-peer handshake on socket {}", socket);
    }
    handshakeFromNewSender();
    startReader(connTable); // this reader only reads the handshake and then exits
    waitForHandshake(); // waiting for reply
  }

  /** time between connection attempts */
  private static final int RECONNECT_WAIT_TIME = Integer
      .getInteger(DistributionConfig.GEMFIRE_PREFIX + "RECONNECT_WAIT_TIME", 2000);

  /**
   * creates a new connection to a remote server. We are initiating this connection; the other side
   * must accept us We will almost always send messages; small acks are received.
   */
  protected static Connection createSender(final MembershipManager mgr, final ConnectionTable t,
      final boolean preserveOrder, final DistributedMember remoteAddr, final boolean sharedResource,
      final long startTime, final long ackTimeout, final long ackSATimeout)
      throws IOException, DistributedSystemDisconnectedException {
    boolean warningPrinted = false;
    boolean success = false;
    boolean firstTime = true;
    Connection conn = null;
    // keep trying. Note that this may be executing during the shutdown window
    // where a cancel criterion has not been established, but threads are being
    // interrupted. In this case we must allow the connection to succeed even
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
        if (!severeAlertIssued && ackSATimeout > 0 && startTime + ackTimeout < now) {
          if (startTime + ackTimeout + ackSATimeout < now) {
            if (remoteAddr != null) {
              logger.fatal("Unable to form a TCP/IP connection to {} in over {} seconds",
                  remoteAddr, (ackSATimeout + ackTimeout) / 1000);
            }
            severeAlertIssued = true;
          } else if (!suspected) {
            if (remoteAddr != null) {
              logger.warn("Unable to form a TCP/IP connection to {} in over {} seconds",
                  remoteAddr, (ackTimeout) / 1000);
            }
            mgr.suspectMember((InternalDistributedMember) remoteAddr,
                "Unable to form a TCP/IP connection in a reasonable amount of time");
            suspected = true;
          }
          reconnectWaitTime =
              Math.min(RECONNECT_WAIT_TIME, ackSATimeout - (now - startTime - ackTimeout));
          if (reconnectWaitTime <= 0) {
            reconnectWaitTime = RECONNECT_WAIT_TIME;
          }
        } else if (!suspected && (startTime > 0) && (ackTimeout > 0)
            && (startTime + ackTimeout < now)) {
          mgr.suspectMember((InternalDistributedMember) remoteAddr,
              "Unable to form a TCP/IP connection in a reasonable amount of time");
          suspected = true;
        }
        if (firstTime) {
          firstTime = false;
          if (!mgr.memberExists(remoteAddr) || mgr.isShunned(remoteAddr)
              || mgr.shutdownInProgress()) {
            throw new IOException("Member " + remoteAddr + " left the system");
          }
        } else {
          // if we're sending an alert and can't connect, bail out. A sick
          // alert listener should not prevent cache operations from continuing
          if (AlertingAction.isThreadAlerting()) {
            // do not change the text of this exception - it is looked for in exception handlers
            throw new IOException("Cannot form connection to alert listener " + remoteAddr);
          }

          // Wait briefly...
          interrupted = Thread.interrupted() || interrupted;
          try {
            Thread.sleep(reconnectWaitTime);
          } catch (InterruptedException ie) {
            interrupted = true;
            t.getConduit().getCancelCriterion().checkCancelInProgress(ie);
          }
          t.getConduit().getCancelCriterion().checkCancelInProgress(null);
          if (giveUpOnMember(mgr, remoteAddr)) {
            throw new IOException(
                String.format("Member %s left the group", remoteAddr));
          }
          if (!warningPrinted) {
            warningPrinted = true;
            logger.warn("Connection: Attempting reconnect to peer {}",
                remoteAddr);
          }
          t.getConduit().getStats().incReconnectAttempts();
        }
        // create connection
        try {
          conn = null;
          conn = new Connection(t, preserveOrder, remoteAddr, sharedResource);
        } catch (javax.net.ssl.SSLHandshakeException se) {
          // no need to retry if certificates were rejected
          throw se;
        } catch (IOException ioe) {
          // Only give up if the member leaves the view.
          if (giveUpOnMember(mgr, remoteAddr)) {
            throw ioe;
          }
          t.getConduit().getCancelCriterion().checkCancelInProgress(null);
          if ("Too many open files".equals(ioe.getMessage())) {
            t.fileDescriptorsExhausted();
          } else if (!connectionErrorLogged) {
            connectionErrorLogged = true; // otherwise change to use 100ms intervals causes a lot of
                                          // these
            logger.info("Connection: shared={} ordered={} failed to connect to peer {} because: {}",
                sharedResource, preserveOrder, remoteAddr,
                ioe.getCause() != null ? ioe.getCause() : ioe);
          }
        } // IOException
        finally {
          if (conn == null) {
            t.getConduit().getStats().incFailedConnect();
          }
        }
        if (conn != null) {
          // handshake
          try {
            conn.attemptHandshake(t);
            if (conn.isSocketClosed()) {
              // something went wrong while reading the handshake
              // and the socket was closed or we were sent
              // ShutdownMessage
              if (giveUpOnMember(mgr, remoteAddr)) {
                throw new IOException(String.format("Member %s left the group",
                    remoteAddr));
              }
              t.getConduit().getCancelCriterion().checkCancelInProgress(null);
              // no success but no need to log; just retry
            } else {
              success = true;
            }
          } catch (ConnectionException e) {
            if (giveUpOnMember(mgr, remoteAddr)) {
              throw new IOException("Handshake failed", e);
            }
            t.getConduit().getCancelCriterion().checkCancelInProgress(null);
            logger.info(
                "Connection: shared={} ordered={} handshake failed to connect to peer {} because: {}",
                sharedResource, preserveOrder, remoteAddr, e);
          } catch (IOException e) {
            if (giveUpOnMember(mgr, remoteAddr)) {
              throw e;
            }
            t.getConduit().getCancelCriterion().checkCancelInProgress(null);
            logger.info(
                "Connection: shared={} ordered={} handshake failed to connect to peer {} because: {}",
                sharedResource, preserveOrder, remoteAddr, e);
            if (!sharedResource && "Too many open files".equals(e.getMessage())) {
              t.fileDescriptorsExhausted();
            }
          } finally {
            if (!success) {
              try {
                conn.requestClose("failed handshake");
              } catch (Exception ignore) {
              }
              conn = null;
            }
          }
        }
      } // while
      if (warningPrinted) {
        logger.info("{}: Successfully reestablished connection to peer {}",
            mgr.getLocalMember(), remoteAddr);
      }
    } finally {
      try {
        if (!success) {
          if (conn != null) {
            conn.requestClose("failed construction");
            conn = null;
          }
        }
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    }
    // Assert.assertTrue(conn != null);
    if (conn == null) {
      throw new ConnectionException(
          String.format("Connection: failed construction for peer %s",
              remoteAddr));
    }
    if (preserveOrder && BATCH_SENDS) {
      conn.createBatchSendBuffer();
    }
    conn.finishedConnecting = true;
    return conn;
  }

  private static boolean giveUpOnMember(MembershipManager mgr, DistributedMember remoteAddr) {
    return !mgr.memberExists(remoteAddr) || mgr.isShunned(remoteAddr) || mgr.shutdownInProgress();
  }

  private void setRemoteAddr(DistributedMember m) {
    this.remoteAddr = this.owner.getDM().getCanonicalId(m);
    MembershipManager mgr = this.conduit.getMembershipManager();
    mgr.addSurpriseMember(m);
  }

  /**
   * creates a new connection to a remote server. We are initiating this connection; the other side
   * must accept us We will almost always send messages; small acks are received.
   */
  private Connection(ConnectionTable t, boolean preserveOrder, DistributedMember remoteID,
      boolean sharedResource) throws IOException, DistributedSystemDisconnectedException {

    // initialize a socket upfront. So that the
    InternalDistributedMember remoteAddr = (InternalDistributedMember) remoteID;
    if (t == null) {
      throw new IllegalArgumentException(
          "ConnectionTable is null.");
    }
    this.conduit = t.getConduit();
    this.isReceiver = false;
    this.owner = t;
    this.sharedResource = sharedResource;
    this.preserveOrder = preserveOrder;
    setRemoteAddr(remoteAddr);
    this.conduitIdStr = this.owner.getConduit().getSocketId().toString();
    this.handshakeRead = false;
    this.handshakeCancelled = false;
    this.connected = true;

    this.uniqueId = idCounter.getAndIncrement();

    // connect to listening socket

    InetSocketAddress addr =
        new InetSocketAddress(remoteAddr.getInetAddress(), remoteAddr.getDirectChannelPort());
    SocketChannel channel = SocketChannel.open();
    this.owner.addConnectingSocket(channel.socket(), addr.getAddress());

    try {
      channel.socket().setTcpNoDelay(true);
      channel.socket().setKeepAlive(SocketCreator.ENABLE_TCP_KEEP_ALIVE);

      /*
       * If conserve-sockets is false, the socket can be used for receiving responses, so set the
       * receive buffer accordingly.
       */
      if (!sharedResource) {
        setReceiveBufferSize(channel.socket(), this.owner.getConduit().tcpBufferSize);
      } else {
        setReceiveBufferSize(channel.socket(), SMALL_BUFFER_SIZE); // make small since only
        // receive ack messages
      }
      setSendBufferSize(channel.socket());
      channel.configureBlocking(true);

      int connectTime = getP2PConnectTimeout();

      try {

        channel.socket().connect(addr, connectTime);

        createIoFilter(channel, true);

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
      } catch (SSLException e) {
        ConnectException c = new ConnectException("Problem connecting to peer " + addr);
        c.initCause(e);
        throw c;
      } catch (CancelledKeyException | ClosedSelectorException e) {
        // bug #44469: for some reason NIO throws this runtime exception
        // instead of an IOException on timeouts
        ConnectException c = new ConnectException(
            String.format("Attempt timed out after %s milliseconds",
                connectTime));
        c.initCause(e);
        throw c;
      }
    } finally {
      this.owner.removeConnectingSocket(channel.socket());
    }
    this.socket = channel.socket();

    if (logger.isDebugEnabled()) {
      logger.debug("Connection: connected to {} with IP address {}", remoteAddr, addr);
    }
    try {
      getSocket().setTcpNoDelay(true);
    } catch (SocketException ignored) {
    }
  }

  /**
   * Batch sends currently should not be turned on because: 1. They will be used for all sends
   * (instead of just no-ack) and thus will break messages that wait for a response (or kill perf).
   * 2. The buffer is not properly flushed and closed on shutdown. The code attempts to do this but
   * must not be doing it correctly.
   */
  private static final boolean BATCH_SENDS = Boolean.getBoolean("p2p.batchSends");
  private static final int BATCH_BUFFER_SIZE =
      Integer.getInteger("p2p.batchBufferSize", 1024 * 1024);
  private static final int BATCH_FLUSH_MS = Integer.getInteger("p2p.batchFlushTime", 50);
  private final Object batchLock = new Object();
  private ByteBuffer fillBatchBuffer;
  private ByteBuffer sendBatchBuffer;
  private BatchBufferFlusher batchFlusher;

  private void createBatchSendBuffer() {
    if (BufferPool.useDirectBuffers) {
      this.fillBatchBuffer = ByteBuffer.allocateDirect(BATCH_BUFFER_SIZE);
      this.sendBatchBuffer = ByteBuffer.allocateDirect(BATCH_BUFFER_SIZE);
    } else {
      this.fillBatchBuffer = ByteBuffer.allocate(BATCH_BUFFER_SIZE);
      this.sendBatchBuffer = ByteBuffer.allocate(BATCH_BUFFER_SIZE);
    }
    this.batchFlusher = new BatchBufferFlusher();
    this.batchFlusher.start();
  }

  void cleanUpOnIdleTaskCancel() {
    // Make sure receivers are removed from the connection table, this should always be a noop, but
    // is done here as a failsafe.
    if (isReceiver) {
      owner.removeReceiver(this);
    }
  }

  public void setInputBuffer(ByteBuffer buffer) {
    this.inputBuffer = buffer;
  }

  private class BatchBufferFlusher extends Thread {
    private volatile boolean flushNeeded = false;
    private volatile boolean timeToStop = false;
    private DMStats stats;


    BatchBufferFlusher() {
      setDaemon(true);
      this.stats = owner.getConduit().getStats();
    }

    /**
     * Called when a message writer needs the current fillBatchBuffer flushed
     */
    void flushBuffer(ByteBuffer bb) {
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
              batchLock.wait(); // spurious wakeup ok
            } catch (InterruptedException ex) {
              interrupted = true;
            } finally {
              if (interrupted) {
                Thread.currentThread().interrupt();
              }
            }
          } // while
        }
      } finally {
        owner.getConduit().getStats().incBatchWaitTime(start);
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
            if (!this.flushNeeded && fillBatchBuffer.position() <= (BATCH_BUFFER_SIZE / 2)) {
              wait(BATCH_FLUSH_MS); // spurious wakeup ok
            }
            if (this.flushNeeded || fillBatchBuffer.position() > (BATCH_BUFFER_SIZE / 2)) {
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
                  writeFully(channel, sendBatchBuffer, false, null);
                  sendBatchBuffer.clear();
                } catch (IOException | ConnectionException ex) {
                  logger.fatal("Exception flushing batch send buffer: %s", ex);
                  readerShuttingDown = true;
                  requestClose(String.format("Exception flushing batch send buffer: %s",
                      ex));
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
        // Thread.currentThread().interrupt();
      }
    }
  }

  private void closeBatchBuffer() {
    if (this.batchFlusher != null) {
      this.batchFlusher.close();
    }
  }

  /**
   * use to test message prep overhead (no socket write). WARNING: turning this on completely
   * disables distribution of batched sends
   */
  private static final boolean SOCKET_WRITE_DISABLED = Boolean.getBoolean("p2p.disableSocketWrite");

  private void batchSend(ByteBuffer src) throws IOException {
    if (SOCKET_WRITE_DISABLED) {
      return;
    }
    final long start = DistributionStats.getStatTime();
    try {
      ByteBuffer dst = null;
      Assert.assertTrue(src.remaining() <= BATCH_BUFFER_SIZE, "Message size(" + src.remaining()
          + ") exceeded BATCH_BUFFER_SIZE(" + BATCH_BUFFER_SIZE + ")");
      do {
        synchronized (this.batchLock) {
          dst = this.fillBatchBuffer;
          if (src.remaining() <= dst.remaining()) {
            final long copyStart = DistributionStats.getStatTime();
            dst.put(src);
            this.owner.getConduit().getStats().incBatchCopyTime(copyStart);
            return;
          }
        }
        // If we got this far then we do not have room in the current
        // buffer and need the flusher thread to flush before we can fill it
        this.batchFlusher.flushBuffer(dst);
      } while (true);
    } finally {
      this.owner.getConduit().getStats().incBatchSendTime(start);
    }
  }

  /**
   * Request that the manager close this connection, or close it forcibly if there is no manager.
   * Invoking this method ensures that the proper synchronization is done.
   */
  void requestClose(String reason) {
    close(reason, true, true, false, false);
  }

  boolean isClosing() {
    return this.closing.get();
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
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "TLW_TWO_LOCK_WAIT")
  private void close(String reason, boolean cleanupEndpoint, boolean p_removeEndpoint,
      boolean beingSick, boolean forceRemoval) {
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
          if (this.asyncQueuingInProgress && this.pusherThread != Thread.currentThread()) {
            // We don't need to do this if we are the pusher thread
            // and we have determined that we need to close the connection.
            // See bug 37601.
            synchronized (this.outgoingQueue) {
              // wait for the flusher to complete (it may timeout)
              while (this.asyncQueuingInProgress) {
                // Don't do this: causes closes to not get done in the event
                // of an orderly shutdown:
                // this.owner.getConduit().getCancelCriterion().checkCancelInProgress(null);
                boolean interrupted = Thread.interrupted();
                try {
                  this.outgoingQueue.wait(); // spurious wakeup ok
                } catch (InterruptedException ie) {
                  interrupted = true;
                  // this.owner.getConduit().getCancelCriterion().checkCancelInProgress(ie);
                } finally {
                  if (interrupted)
                    Thread.currentThread().interrupt();
                }
              } // while
            } // synchronized
          }
          this.connected = false;
          closeSenderSem();
          {
            final DMStats stats = this.owner.getConduit().getStats();
            if (this.finishedConnecting) {
              if (this.isReceiver) {
                stats.decReceivers();
              } else {
                stats.decSenders(this.sharedResource, this.preserveOrder);
              }
            }
          }
        } else if (!forceRemoval) {
          removeEndpoint = false;
        }
        // make sure our socket is closed
        asyncClose(false);
        if (!this.isReceiver) {
          // receivers release the input buffer when exiting run(). Senders use the
          // inputBuffer for reading direct-reply responses
          releaseInputBuffer();
        }
        lengthSet = false;
      } // synchronized

      // moved the call to notifyHandshakeWaiter out of the above
      // synchronized block to fix bug #42159
      // Make sure anyone waiting for a handshake stops waiting
      notifyHandshakeWaiter(false);
      // wait a bit for the our reader thread to exit
      // don't wait if we are the reader thread
      boolean isIBM = false;
      // if network partition detection is enabled or this is an admin vm
      // we can't wait for the reader thread when running in an IBM JRE. See
      // bug 41889
      if (this.conduit.config.getEnableNetworkPartitionDetection()
          || this.conduit.getMemberId().getVmKind() == ClusterDistributionManager.ADMIN_ONLY_DM_TYPE
          || this.conduit.getMemberId().getVmKind() == ClusterDistributionManager.LOCATOR_DM_TYPE) {
        isIBM = "IBM Corporation".equals(System.getProperty("java.vm.vendor"));
      }
      {
        // Now that readerThread is returned to a pool after we close
        // we need to be more careful not to join on a thread that belongs
        // to someone else.
        Thread readerThreadSnapshot = this.readerThread;
        if (!beingSick && readerThreadSnapshot != null && !isIBM && this.isRunning
            && !this.readerShuttingDown && readerThreadSnapshot != Thread.currentThread()) {
          try {
            readerThreadSnapshot.join(500);
            readerThreadSnapshot = this.readerThread;
            if (this.isRunning && !this.readerShuttingDown && readerThreadSnapshot != null
                && owner.getDM().getRootCause() == null) { // don't wait twice if there's a system
                                                           // failure
              readerThreadSnapshot.join(1500);
              if (this.isRunning) {
                logger.info("Timed out waiting for readerThread on {} to finish.",
                    this);
              }
            }
          } catch (IllegalThreadStateException ignore) {
            // ignored - thread already stopped
          } catch (InterruptedException ignore) {
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
                this.owner.removeEndpoint(this.remoteAddr, reason);
              }
            }
          } else {
            // noinspection ConstantConditions
            this.owner.removeSharedConnection(reason, this.remoteAddr, this.preserveOrder, this);
          }
        } else if (!this.isReceiver) {
          this.owner.removeThreadConnection(this.remoteAddr, this);
        }
      } else {
        // This code is ok to do even if the ConnectionTable
        // has never added this Connection to its maps since
        // the calls in this block use our identity to do the removes.
        if (this.sharedResource) {
          this.owner.removeSharedConnection(reason, this.remoteAddr, this.preserveOrder, this);
        } else if (!this.isReceiver) {
          this.owner.removeThreadConnection(this.remoteAddr, this);
        }
      }
    }

    // This cancels the idle timer task, but it also removes the tasks
    // reference to this connection, freeing up the connection (and it's buffers
    // for GC sooner.
    if (idleTask != null) {
      idleTask.cancel();
    }

    if (ackTimeoutTask != null) {
      ackTimeoutTask.cancel();
    }

  }

  /** starts a reader thread */
  private void startReader(ConnectionTable connTable) {
    if (logger.isDebugEnabled()) {
      logger.debug("Starting thread for " + p2pReaderName());
    }
    Assert.assertTrue(!this.isRunning);
    stopped = false;
    this.isRunning = true;
    connTable.executeCommand(this);
  }


  /**
   * in order to read non-NIO socket-based messages we need to have a thread actively trying to grab
   * bytes out of the sockets input queue. This is that thread.
   */
  @Override
  public void run() {
    this.readerThread = Thread.currentThread();
    this.readerThread.setName(p2pReaderName());
    ConnectionTable.threadWantsSharedResources();
    makeReaderThread(this.isReceiver);
    try {
      readMessages();
    } finally {
      // bug36060: do the socket close within a finally block
      if (logger.isDebugEnabled()) {
        logger.debug("Stopping {} for {}", p2pReaderName(), remoteAddr);
      }
      if (this.isReceiver) {
        try {
          initiateSuspicionIfSharedUnordered();
        } catch (CancelException e) {
          // shutting down
        }
        if (!this.sharedResource) {
          this.conduit.getStats().incThreadOwnedReceivers(-1L, dominoCount.get());
        }
        asyncClose(false);
        this.owner.removeAndCloseThreadOwnedSockets();
      }
      releaseInputBuffer();

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

  private void releaseInputBuffer() {
    ByteBuffer tmp = this.inputBuffer;
    if (tmp != null) {
      this.inputBuffer = null;
      final DMStats stats = this.owner.getConduit().getStats();
      getBufferPool().releaseReceiveBuffer(tmp);
    }
  }

  BufferPool getBufferPool() {
    return owner.getBufferPool();
  }

  private String p2pReaderName() {
    StringBuilder sb = new StringBuilder(64);
    if (this.isReceiver) {
      sb.append(THREAD_KIND_IDENTIFIER + "@");
    } else if (this.handshakeRead) {
      sb.append("P2P message sender@");
    } else {
      sb.append("P2P handshake reader@");
    }
    sb.append(Integer.toHexString(System.identityHashCode(this)));
    if (!this.isReceiver) {
      sb.append('-').append(getUniqueId());
    }
    return sb.toString();
  }

  private void readMessages() {
    // take a snapshot of uniqueId to detect reconnect attempts; see bug 37592
    SocketChannel channel;
    try {
      channel = getSocket().getChannel();
      socket.setSoTimeout(0);
      socket.setTcpNoDelay(true);
      if (ioFilter == null) {
        createIoFilter(channel, false);
      }
      channel.configureBlocking(true);
    } catch (ClosedChannelException e) {
      // bug 37693: the channel was asynchronously closed. Our work
      // is done.
      try {
        requestClose(
            "readMessages caught closed channel");
      } catch (Exception ignore) {
      }
      return; // exit loop and thread
    } catch (IOException ex) {
      if (stopped || owner.getConduit().getCancelCriterion().isCancelInProgress()) {
        try {
          requestClose(
              "readMessages caught shutdown");
        } catch (Exception ignore) {
        }
        return; // bug37520: exit loop (and thread)
      }
      logger.info("Failed initializing socket for message {}: {}",
          (this.isReceiver ? "receiver" : "sender"), ex.getMessage());
      this.readerShuttingDown = true;
      try {
        requestClose(String.format("Failed initializing socket %s",
            ex));
      } catch (Exception ignore) {
      }
      return;
    }

    if (!stopped) {
      // Assert.assertTrue(owner != null, "How did owner become null");
      if (logger.isDebugEnabled()) {
        logger.debug("Starting {} on {}", p2pReaderName(), socket);
      }
    }
    // we should not change the state of the connection if we are a handshake reader thread
    // as there is a race between this thread and the application thread doing direct ack
    // fix for #40869
    boolean isHandShakeReader = false;
    // if we're using SSL/TLS the input buffer may already have data to process
    boolean skipInitialRead = getInputBuffer().position() > 0;
    boolean isInitialRead = true;
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
              ioFilter.close(s.getChannel());
              s.close();
            } catch (IOException e) {
              // don't care
            }
          }
          SystemFailure.checkFailure(); // throws
        }
        if (this.owner.getConduit().getCancelCriterion().isCancelInProgress()) {
          break;
        }

        try {
          ByteBuffer buff = getInputBuffer();
          synchronized (stateLock) {
            connectionState = STATE_READING;
          }
          int amountRead;
          if (!isInitialRead) {
            amountRead = channel.read(buff);
          } else {
            isInitialRead = false;
            if (!skipInitialRead) {
              amountRead = channel.read(buff);
            } else {
              amountRead = buff.position();
            }
          }
          synchronized (stateLock) {
            connectionState = STATE_IDLE;
          }
          if (amountRead == 0) {
            continue;
          }
          if (amountRead < 0) {
            this.readerShuttingDown = true;
            try {
              requestClose("SocketChannel.read returned EOF");
            } catch (Exception e) {
              // ignore - shutting down
            }
            return;
          }

          processInputBuffer();

          if (!this.isReceiver && (this.handshakeRead || this.handshakeCancelled)) {
            if (logger.isDebugEnabled()) {
              if (this.handshakeRead) {
                logger.debug("handshake has been read {}", this);
              } else {
                logger.debug("handshake has been cancelled {}", this);
              }
            }
            isHandShakeReader = true;
            // Once we have read the handshake the reader can go away
            break;
          }
        } catch (CancelException e) {
          if (logger.isDebugEnabled()) {
            logger.debug("{} Terminated <{}> due to cancellation", p2pReaderName(), this, e);
          }
          this.readerShuttingDown = true;
          try {
            requestClose(
                String.format("CacheClosed in channel read: %s", e));
          } catch (Exception ignored) {
          }
          return;
        } catch (ClosedChannelException e) {
          this.readerShuttingDown = true;
          try {
            requestClose(String.format("ClosedChannelException in channel read: %s",
                e));
          } catch (Exception ignored) {
          }
          return;
        } catch (IOException e) {
          if (!isSocketClosed() && !"Socket closed".equalsIgnoreCase(e.getMessage()) // needed for
                                                                                     // Solaris jdk
                                                                                     // 1.4.2_08
          ) {
            if (logger.isDebugEnabled() && !isIgnorableIOException(e)) {
              logger.debug("{} io exception for {}", p2pReaderName(), this, e);
            }
            if (e.getMessage().contains("interrupted by a call to WSACancelBlockingCall")) {
              if (logger.isDebugEnabled()) {
                logger.debug(
                    "{} received unexpected WSACancelBlockingCall exception, which may result in a hang",
                    p2pReaderName());
              }
            }
          }
          this.readerShuttingDown = true;
          try {
            requestClose(
                String.format("IOException in channel read: %s", e));
          } catch (Exception ignored) {
          }
          return;

        } catch (Exception e) {
          this.owner.getConduit().getCancelCriterion().checkCancelInProgress(null); // bug 37101
          if (!stopped && !isSocketClosed()) {
            logger.fatal(String.format("%s exception in channel read", p2pReaderName()), e);
          }
          this.readerShuttingDown = true;
          try {
            requestClose(
                String.format("%s exception in channel read", e));
          } catch (Exception ignored) {
          }
          return;
        }
      } // for
    } finally {
      if (!isHandShakeReader) {
        synchronized (stateLock) {
          connectionState = STATE_IDLE;
        }
      }
      if (logger.isDebugEnabled()) {
        logger.debug("readMessages terminated id={} from {} isHandshakeReader={}", conduitIdStr,
            remoteAddr, isHandShakeReader);
      }
    }
  }

  private void createIoFilter(SocketChannel channel, boolean clientSocket) throws IOException {
    if (getConduit().useSSL() && channel != null) {
      InetSocketAddress address = (InetSocketAddress) channel.getRemoteAddress();
      SSLEngine engine =
          getConduit().getSocketCreator().createSSLEngine(address.getHostName(), address.getPort());

      if (!clientSocket) {
        engine.setNeedClientAuth(getSSLConfigForComponent(getConduit().config,
            SecurableCommunicationChannel.CLUSTER).isRequireAuth());
      }

      int packetBufferSize = engine.getSession().getPacketBufferSize();
      if (inputBuffer == null
          || (inputBuffer.capacity() < packetBufferSize)) {
        // TLS has a minimum input buffer size constraint
        if (inputBuffer != null) {
          getBufferPool().releaseReceiveBuffer(inputBuffer);
        }
        inputBuffer = getBufferPool().acquireReceiveBuffer(packetBufferSize);
      }
      if (channel.socket().getReceiveBufferSize() < packetBufferSize) {
        channel.socket().setReceiveBufferSize(packetBufferSize);
      }
      if (channel.socket().getSendBufferSize() < packetBufferSize) {
        channel.socket().setSendBufferSize(packetBufferSize);
      }
      ioFilter = getConduit().getSocketCreator().handshakeSSLSocketChannel(channel, engine,
          getConduit().idleConnectionTimeout, clientSocket, inputBuffer,
          getBufferPool());
    } else {
      ioFilter = new NioPlainEngine(getBufferPool());
    }
  }

  /**
   * initiate suspect processing if a shared/ordered connection is lost and we're not shutting down
   */
  private void initiateSuspicionIfSharedUnordered() {
    if (this.isReceiver && this.handshakeRead && !this.preserveOrder && this.sharedResource) {
      if (!this.owner.getConduit().getCancelCriterion().isCancelInProgress()) {
        this.owner.getDM().getMembershipManager().suspectMember(this.getRemoteAddress(),
            INITIATING_SUSPECT_PROCESSING);
      }
    }
  }

  /**
   * checks to see if an exception should not be logged: i.e., "forcibly closed", "reset by peer",
   * or "connection reset"
   */
  private static boolean isIgnorableIOException(Exception e) {
    if (e instanceof ClosedChannelException) {
      return true;
    }

    String msg = e.getMessage();
    if (msg == null) {
      msg = e.toString();
    }

    msg = msg.toLowerCase();
    return (msg.contains("forcibly closed")) || (msg.contains("reset by peer"))
        || (msg.contains("connection reset"));
  }

  private static boolean validMsgType(int msgType) {
    return msgType == NORMAL_MSG_TYPE || msgType == CHUNKED_MSG_TYPE
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
          MsgDestreamer md = (MsgDestreamer) it.next();
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
      MsgDestreamer result = (MsgDestreamer) this.destreamerMap.get(key);
      if (result == null) {
        result = this.idleMsgDestreamer;
        if (result != null) {
          this.idleMsgDestreamer = null;
        } else {
          result = new MsgDestreamer(this.owner.getConduit().getStats(),
              this.conduit.getCancelCriterion(), v);
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
    if (directAck) {
      dm = new DirectReplySender(this);
    } else if (rpId != 0) {
      dm = this.owner.getDM();
    }
    if (dm != null) {
      ReplyMessage.send(getRemoteAddress(), rpId, new ReplyException(exMsg, ex), dm);
    }
  }


  /**
   * sends a serialized message to the other end of this connection. This is used by the
   * DirectChannel in GemFire when the message is going to be sent to multiple recipients.
   *
   * @throws ConnectionException if the conduit has stopped
   */
  void sendPreserialized(ByteBuffer buffer, boolean cacheContentChanges,
      DistributionMessage msg) throws IOException, ConnectionException {
    if (!connected) {
      throw new ConnectionException(
          String.format("Not connected to %s", this.remoteAddr));
    }
    if (this.batchFlusher != null) {
      batchSend(buffer);
      return;
    }
    final boolean origSocketInUse = this.socketInUse;
    byte originalState;
    synchronized (stateLock) {
      originalState = this.connectionState;
      this.connectionState = STATE_SENDING;
    }
    this.socketInUse = true;
    try {
      SocketChannel channel = getSocket().getChannel();
      writeFully(channel, buffer, false, msg);
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
   * If <code>use</code> is true then "claim" the connection for our use. If <code>use</code> is
   * false then "release" the connection. Fixes bug 37657.
   *
   * @return true if connection was already in use at time of call; false if not.
   */
  public boolean setInUse(boolean use, long startTime, long ackWaitThreshold, long ackSAThreshold,
      List connectionGroup) {
    // just do the following; EVEN if the connection has been closed
    final boolean origSocketInUse = this.socketInUse;
    synchronized (this) {
      if (use && (ackWaitThreshold > 0 || ackSAThreshold > 0)) {
        // set times that events should be triggered
        this.transmissionStartTime = startTime;
        this.ackWaitTimeout = ackWaitThreshold;
        this.ackSATimeout = ackSAThreshold;
        this.ackConnectionGroup = connectionGroup;
        this.ackThreadName = Thread.currentThread().getName();
      } else {
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

  /**
   * For testing we want to configure the connection without having to read a handshake
   */
  void setSharedUnorderedForTest() {
    this.preserveOrder = false;
    this.sharedResource = true;
    this.handshakeRead = true;
  }


  /** ensure that a task is running to monitor transmission and reading of acks */
  synchronized void scheduleAckTimeouts() {
    if (ackTimeoutTask == null) {
      final long msAW = this.owner.getDM().getConfig().getAckWaitThreshold() * 1000L;
      final long msSA = this.owner.getDM().getConfig().getAckSevereAlertThreshold() * 1000L;
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
          synchronized (Connection.this) {
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
          List group = ackConnectionGroup;
          if (sentAlert && group != null) {
            // since transmission and ack-receipt are performed serially, we don't
            // want to complain about all receivers out just because one was slow. We therefore
            // reset
            // the time stamps and give others more time
            for (Iterator it = group.iterator(); it.hasNext();) {
              Connection con = (Connection) it.next();
              if (con != Connection.this) {
                con.transmissionStartTime += con.ackSATimeout;
              }
            }
          }
        }
      };

      synchronized (owner) {
        SystemTimer timer = owner.getIdleConnTimer();
        if (timer != null) {
          if (msSA > 0) {
            timer.scheduleAtFixedRate(ackTimeoutTask, msAW, Math.min(msAW, msSA));
          } else {
            timer.schedule(ackTimeoutTask, msAW);
          }
        }
      }
    }
  }

  /** ack-wait-threshold and ack-severe-alert-threshold processing */
  private boolean doSevereAlertProcessing() {
    long now = System.currentTimeMillis();
    if (ackSATimeout > 0 && (transmissionStartTime + ackWaitTimeout + ackSATimeout) <= now) {
      logger.fatal("{} seconds have elapsed waiting for a response from {} for thread {}",
          (ackWaitTimeout + ackSATimeout) / 1000L,
          getRemoteAddress(),
          ackThreadName);
      // turn off subsequent checks by setting the timeout to zero, then boot the member
      ackSATimeout = 0;
      return true;
    } else if (!ackTimedOut && (0 < ackWaitTimeout)
        && (transmissionStartTime + ackWaitTimeout) <= now) {
      logger.warn("{} seconds have elapsed waiting for a response from {} for thread {}",
          ackWaitTimeout / 1000L, getRemoteAddress(), ackThreadName);
      ackTimedOut = true;

      final String state = (connectionState == Connection.STATE_SENDING)
          ? "Sender has been unable to transmit a message within ack-wait-threshold seconds"
          : "Sender has been unable to receive a response to a message within ack-wait-threshold seconds";
      if (ackSATimeout > 0) {
        this.owner.getDM().getMembershipManager()
            .suspectMembers(Collections.singleton(getRemoteAddress()), state);
      }
    }
    return false;
  }

  private boolean addToQueue(ByteBuffer buffer, DistributionMessage msg, boolean force)
      throws ConnectionException {
    final DMStats stats = this.owner.getConduit().getStats();
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
          throw new ConnectionException(String.format("Forced disconnect sent to %s",
              this.remoteAddr));
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
              ConflationKey oldck = (ConflationKey) oldValue;
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
            /* Object removedVal = */ this.conflatedKeys.remove(ck);
          }
        }
        {
          long newQueueSize = newBytes + this.queuedBytes;
          if (newQueueSize > this.asyncMaxQueueSize) {
            logger.warn("Queued bytes {} exceeds max of {}, asking slow receiver {} to disconnect.",
                newQueueSize, this.asyncMaxQueueSize, this.remoteAddr);
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
   * Return true if it was able to handle a block write of the given buffer. Return false if it is
   * still the caller is still responsible for writing it.
   *
   * @throws ConnectionException if the conduit has stopped
   */
  private boolean handleBlockedWrite(ByteBuffer buffer, DistributionMessage msg)
      throws ConnectionException {
    if (!addToQueue(buffer, msg, true)) {
      return false;
    } else {
      startMessagePusher();
      return true;
    }
  }

  private final Object pusherSync = new Object();

  private void startMessagePusher() {
    synchronized (this.pusherSync) {
      while (this.pusherThread != null) {
        // wait for previous pusher thread to exit
        boolean interrupted = Thread.interrupted();
        try {
          this.pusherSync.wait(); // spurious wakeup ok
        } catch (InterruptedException ex) {
          interrupted = true;
          this.owner.getConduit().getCancelCriterion().checkCancelInProgress(ex);
        } finally {
          if (interrupted) {
            Thread.currentThread().interrupt();
          }
        }
      }
      this.asyncQueuingInProgress = true;
      this.pusherThread =
          new LoggingThread("P2P async pusher to " + this.remoteAddr, this::runMessagePusher);
    } // synchronized
    this.pusherThread.start();
  }

  private ByteBuffer takeFromOutgoingQueue() throws InterruptedException {
    ByteBuffer result = null;
    final DMStats stats = this.owner.getConduit().getStats();
    long start = DistributionStats.getStatTime();
    try {
      synchronized (this.outgoingQueue) {
        if (this.disconnectRequested) {
          // don't bother with anymore work since we are done
          this.asyncQueuingInProgress = false;
          this.outgoingQueue.notifyAll();
          return null;
        }
        // Object o = this.outgoingQueue.poll();
        do {
          if (this.outgoingQueue.isEmpty()) {
            break;
          }
          Object o = this.outgoingQueue.removeFirst();
          if (o == null) {
            break;
          }
          if (o instanceof ConflationKey) {
            result = ((ConflationKey) o).getBuffer();
            if (result != null) {
              this.conflatedKeys.remove(o);
            } else {
              // if result is null then this same key will be found later in the
              // queue so we just need to skip this entry
              continue;
            }
          } else {
            result = (ByteBuffer) o;
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
   * @since GemFire 4.2.2
   */
  private void disconnectSlowReceiver() {
    synchronized (this.outgoingQueue) {
      if (this.disconnectRequested) {
        // only ask once
        return;
      }
      this.disconnectRequested = true;
    }
    DistributionManager dm = this.owner.getDM();
    if (dm == null) {
      this.owner.removeEndpoint(this.remoteAddr,
          "no distribution manager");
      return;
    }
    dm.getMembershipManager().requestMemberRemoval(this.remoteAddr,
        "Disconnected as a slow-receiver");
    // Ok, we sent the message, the coordinator should kick the member out
    // immediately and inform this process with a new view.
    // Let's wait
    // for that to happen and if it doesn't in X seconds
    // then remove the endpoint.
    final int FORCE_TIMEOUT = 3000;
    while (dm.getOtherDistributionManagerIds().contains(this.remoteAddr)) {
      try {
        Thread.sleep(50);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        this.owner.getConduit().getCancelCriterion().checkCancelInProgress(ie);
        return;
      }
    }
    this.owner.removeEndpoint(this.remoteAddr,
        "Force disconnect timed out");
    if (dm.getOtherDistributionManagerIds().contains(this.remoteAddr)) {
      if (logger.isDebugEnabled()) {
        logger.debug("Force disconnect timed out after waiting {} seconds", (FORCE_TIMEOUT / 1000));
      }
      return;
    }
  }

  /**
   * have the pusher thread check for queue overflow and for idle time exceeded
   */
  private void runMessagePusher() {
    try {
      final DMStats stats = this.owner.getConduit().getStats();
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
                  logger.debug("closing socket", new Exception("closing socket"));
                  ioFilter.close(s.getChannel());
                  s.close();
                } catch (IOException e) {
                  // don't care
                }
              }
              SystemFailure.checkFailure(); // throws
            }
            if (this.owner.getConduit().getCancelCriterion().isCancelInProgress()) {
              break;
            }
            flushId++;
            long flushStart = stats.startAsyncQueueFlush();
            try {
              long curQueuedBytes = this.queuedBytes;
              if (curQueuedBytes > this.asyncMaxQueueSize) {
                logger.warn(
                    "Queued bytes {} exceeds max of {}, asking slow receiver {} to disconnect.",
                    curQueuedBytes, this.asyncMaxQueueSize, this.remoteAddr);
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
              writeFully(channel, bb, true, null);
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
        final String err =
            String.format("P2P pusher io exception for %s", this);
        if (!isSocketClosed()) {
          if (logger.isDebugEnabled() && !isIgnorableIOException(ex)) {
            logger.debug(err, ex);
          }
        }
        try {
          requestClose(err + ": " + ex);
        } catch (Exception ignore) {
        }
      } catch (CancelException ex) { // bug 37367
        final String err = String.format("P2P pusher %s caught CacheClosedException: %s",
            this, ex);
        logger.debug(err);
        try {
          requestClose(err);
        } catch (Exception ignore) {
        }
        return;
      } catch (Exception ex) {
        this.owner.getConduit().getCancelCriterion().checkCancelInProgress(ex); // bug 37101
        if (!isSocketClosed()) {
          logger.fatal(String.format("P2P pusher exception: %s", ex), ex);
        }
        try {
          requestClose(String.format("P2P pusher exception: %s", ex));
        } catch (Exception ignore) {
        }
      } finally {
        stats.incAsyncQueueSize(-this.queuedBytes);
        this.queuedBytes = 0;
        stats.endAsyncThread(threadStart);
        stats.incAsyncThreads(-1);
        stats.incAsyncQueues(-1);
        if (logger.isDebugEnabled()) {
          logger.debug("runMessagePusher terminated id={} from {}/{}", conduitIdStr, remoteAddr,
              remoteAddr);
        }
      }
    } finally {
      synchronized (this.pusherSync) {
        this.pusherThread = null;
        this.pusherSync.notify();
      }
    }
  }

  /**
   * Return false if socket writes to be done async/nonblocking Return true if socket writes to be
   * done sync/blocking
   */
  private boolean useSyncWrites(boolean forceAsync) {
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
  @MutableForTesting
  public static volatile boolean FORCE_ASYNC_QUEUE = false;

  private static final int MAX_WAIT_TIME = (1 << 5); // ms (must be a power of 2)

  private void writeAsync(SocketChannel channel, ByteBuffer buffer, boolean forceAsync,
      DistributionMessage p_msg, final DMStats stats) throws IOException {
    DistributionMessage msg = p_msg;
    // async/non-blocking
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
          ByteBuffer wrappedBuffer = ioFilter.wrap(buffer);
          do {
            this.owner.getConduit().getCancelCriterion().checkCancelInProgress(null);
            retries++;
            int amtWritten;
            if (FORCE_ASYNC_QUEUE) {
              amtWritten = 0;
            } else {
              amtWritten = channel.write(wrappedBuffer);
            }
            if (amtWritten == 0) {
              now = System.currentTimeMillis();
              long timeoutTarget;
              if (!forceAsync) {
                if (now > distributionTimeoutTarget) {
                  if (logger.isDebugEnabled()) {
                    if (distributionTimeoutTarget == 0) {
                      logger.debug(
                          "Starting async pusher to handle async queue because distribution-timeout is 1 and the last socket write would have blocked.");
                    } else {
                      long blockedMs = now - distributionTimeoutTarget;
                      blockedMs += this.asyncDistributionTimeout;
                      logger.debug(
                          "Blocked for {}ms which is longer than the max of {}ms so starting async pusher to handle async queue.",
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
                  if (handleBlockedWrite(wrappedBuffer, msg)) {
                    return;
                  }
                }
                timeoutTarget = distributionTimeoutTarget;
              } else {
                boolean disconnectNeeded = false;
                long curQueuedBytes = this.queuedBytes;
                if (curQueuedBytes > this.asyncMaxQueueSize) {
                  logger.warn(
                      "Queued bytes {} exceeds max of {}, asking slow receiver {} to disconnect.",
                      curQueuedBytes,
                      this.asyncMaxQueueSize, this.remoteAddr);
                  stats.incAsyncQueueSizeExceeded(1);
                  disconnectNeeded = true;
                }
                if (now > queueTimeoutTarget) {
                  // we have waited long enough
                  // the pusher has been idle too long!
                  long blockedMs = now - queueTimeoutTarget;
                  blockedMs += this.asyncQueueTimeout;
                  logger.warn(
                      "Blocked for {}ms which is longer than the max of {}ms, asking slow receiver {} to disconnect.",
                      blockedMs,
                      this.asyncQueueTimeout, this.remoteAddr);
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
                  boolean interrupted = Thread.interrupted();
                  try {
                    Thread.sleep(msToWait);
                  } catch (InterruptedException ex) {
                    interrupted = true;
                    this.owner.getConduit().getCancelCriterion().checkCancelInProgress(ex);
                  } finally {
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
          } while (wrappedBuffer.remaining() > 0);
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

  /**
   * writeFully implements a blocking write on a channel that is in non-blocking mode.
   *
   * @param forceAsync true if we need to force a blocking async write.
   * @throws ConnectionException if the conduit has stopped
   */
  void writeFully(SocketChannel channel, ByteBuffer buffer, boolean forceAsync,
      DistributionMessage msg) throws IOException, ConnectionException {
    final DMStats stats = this.owner.getConduit().getStats();
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
        ByteBuffer wrappedBuffer = ioFilter.wrap(buffer);
        while (wrappedBuffer.remaining() > 0) {
          int amtWritten = 0;
          long start = stats.startSocketWrite(true);
          try {
            amtWritten = channel.write(wrappedBuffer);
          } finally {
            stats.endSocketWrite(true, start, amtWritten, 0);
          }
        }

      } // synchronized
    } else {
      writeAsync(channel, buffer, forceAsync, msg, stats);
    }
  }

  /** gets the buffer for receiving message length bytes */
  private ByteBuffer getInputBuffer() {
    if (inputBuffer == null) {
      int allocSize = this.recvBufferSize;
      if (allocSize == -1) {
        allocSize = this.owner.getConduit().tcpBufferSize;
      }
      inputBuffer = getBufferPool().acquireReceiveBuffer(allocSize);
    }
    return inputBuffer;
  }

  /**
   * stateLock is used to synchronize state changes.
   */
  private final Object stateLock = new Object();

  /** for timeout processing, this is the current state of the connection */
  private byte connectionState = STATE_IDLE;

  /* ~~~~~~~~~~~~~ connection states ~~~~~~~~~~~~~~~ */
  /** the connection is idle, but may be in use */
  private static final byte STATE_IDLE = 0;
  /** the connection is in use and is transmitting data */
  private static final byte STATE_SENDING = 1;
  /** the connection is in use and is done transmitting */
  private static final byte STATE_POST_SENDING = 2;
  /** the connection is in use and is reading a direct-ack */
  private static final byte STATE_READING_ACK = 3;
  /** the connection is in use and has finished reading a direct-ack */
  private static final byte STATE_RECEIVED_ACK = 4;
  /** the connection is in use and is reading a message */
  private static final byte STATE_READING = 5;
  /* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ */

  /** set to true if we exceeded the ack-wait-threshold waiting for a response */
  private volatile boolean ackTimedOut;

  /**
   * @throws SocketTimeoutException if wait expires.
   * @throws ConnectionException if ack is not received
   */
  public void readAck(final DirectReplyProcessor processor)
      throws SocketTimeoutException, ConnectionException {
    if (isSocketClosed()) {
      throw new ConnectionException(
          "connection is closed");
    }
    synchronized (this.stateLock) {
      this.connectionState = STATE_READING_ACK;
    }

    boolean origSocketInUse = this.socketInUse;
    this.socketInUse = true;
    MsgReader msgReader = null;
    DMStats stats = owner.getConduit().getStats();
    final Version version = getRemoteVersion();
    try {
      msgReader = new MsgReader(this, ioFilter, version);

      Header header = msgReader.readHeader();

      ReplyMessage msg = null;
      int len;
      if (header.getMessageType() == NORMAL_MSG_TYPE) {
        msg = (ReplyMessage) msgReader.readMessage(header);
        len = header.getMessageLength();
      } else {
        MsgDestreamer destreamer = obtainMsgDestreamer(header.getMessageId(), version);
        while (header.getMessageType() == CHUNKED_MSG_TYPE) {
          msgReader.readChunk(header, destreamer);
          header = msgReader.readHeader();
        }
        msgReader.readChunk(header, destreamer);
        msg = (ReplyMessage) destreamer.getMessage();
        releaseMsgDestreamer(header.getMessageId(), destreamer);
        len = destreamer.size();
      }
      // I'd really just like to call dispatchMessage here. However,
      // that call goes through a bunch of checks that knock about
      // 10% of the performance. Since this direct-ack stuff is all
      // about performance, we'll skip those checks. Skipping them
      // should be legit, because we just sent a message so we know
      // the member is already in our view, etc.
      ClusterDistributionManager dm = (ClusterDistributionManager) owner.getDM();
      msg.setBytesRead(len);
      msg.setSender(remoteAddr);
      stats.incReceivedMessages(1L);
      stats.incReceivedBytes(msg.getBytesRead());
      stats.incMessageChannelTime(msg.resetTimestamp());
      msg.process(dm, processor);
      // dispatchMessage(msg, len, false);
    } catch (MemberShunnedException e) {
      // do nothing
    } catch (SocketTimeoutException timeout) {
      throw timeout;
    } catch (IOException e) {
      final String err =
          String.format("ack read io exception for %s", this);
      if (!isSocketClosed()) {
        if (logger.isDebugEnabled() && !isIgnorableIOException(e)) {
          logger.debug(err, e);
        }
      }
      try {
        requestClose(err + ": " + e);
      } catch (Exception ex) {
      }
      throw new ConnectionException(
          String.format("Unable to read direct ack because: %s", e));
    } catch (ConnectionException e) {
      this.owner.getConduit().getCancelCriterion().checkCancelInProgress(e);
      throw e;
    } catch (Exception e) {
      this.owner.getConduit().getCancelCriterion().checkCancelInProgress(e);
      if (!isSocketClosed()) {
        logger.fatal("ack read exception", e);
      }
      try {
        requestClose(String.format("ack read exception: %s", e));
      } catch (Exception ex) {
      }
      throw new ConnectionException(
          String.format("Unable to read direct ack because: %s", e));
    } finally {
      stats.incProcessedMessages(1L);
      accessed();
      this.socketInUse = origSocketInUse;
      if (this.ackTimedOut) {
        logger.info("Finished waiting for reply from {}",
            getRemoteAddress());
        this.ackTimedOut = false;
      }
      if (msgReader != null) {
        msgReader.close();
      }
    }
    synchronized (stateLock) {
      this.connectionState = STATE_RECEIVED_ACK;
    }
  }

  /**
   * processes the current NIO buffer. If there are complete messages in the buffer, they are
   * deserialized and passed to TCPConduit for further processing
   */
  private void processInputBuffer() throws ConnectionException, IOException {

    inputBuffer.flip();

    ByteBuffer peerDataBuffer = ioFilter.unwrap(inputBuffer);
    peerDataBuffer.flip();

    boolean done = false;

    while (!done && connected) {
      this.owner.getConduit().getCancelCriterion().checkCancelInProgress(null);
      int remaining = peerDataBuffer.remaining();
      if (lengthSet || remaining >= MSG_HEADER_BYTES) {
        if (!lengthSet) {
          if (readMessageHeader(peerDataBuffer)) {
            break;
          }
        }
        if (remaining >= messageLength + MSG_HEADER_BYTES) {
          lengthSet = false;
          peerDataBuffer.position(peerDataBuffer.position() + MSG_HEADER_BYTES);
          // don't trust the message deserialization to leave the position in
          // the correct spot. Some of the serialization uses buffered
          // streams that can leave the position at the wrong spot
          int startPos = peerDataBuffer.position();
          int oldLimit = peerDataBuffer.limit();
          peerDataBuffer.limit(startPos + messageLength);

          if (this.handshakeRead) {
            try {
              readMessage(peerDataBuffer);
            } catch (SerializationException e) {
              logger.info("input buffer startPos {} oldLimit {}", startPos, oldLimit);
              throw e;
            }
          } else {
            ByteBufferInputStream bbis = new ByteBufferInputStream(peerDataBuffer);
            DataInputStream dis = new DataInputStream(bbis);
            if (!this.isReceiver) {
              // we read the handshake and then stop processing since we don't want
              // to process the input buffer anymore in a handshake thread
              readHandshakeForSender(dis, peerDataBuffer);
              return;
            } else {
              if (readHandshakeForReceiver(dis)) {
                ioFilter.doneReading(peerDataBuffer);
                return;
              }
            }
          }
          if (!connected) {
            continue;
          }
          accessed();
          peerDataBuffer.limit(oldLimit);
          peerDataBuffer.position(startPos + messageLength);
        } else {
          done = true;
          if (getConduit().useSSL()) {
            ioFilter.doneReading(peerDataBuffer);
          } else {
            compactOrResizeBuffer(messageLength);
          }
        }
      } else {
        ioFilter.doneReading(peerDataBuffer);
        done = true;
      }
    }
  }

  private boolean readHandshakeForReceiver(DataInputStream dis) {
    try {
      byte b = dis.readByte();
      if (b != 0) {
        throw new IllegalStateException(
            String.format(
                "Detected old version (pre 5.0.1) of GemFire or non-GemFire during handshake due to initial byte being %s",
                b));
      }
      byte handshakeByte = dis.readByte();
      if (handshakeByte != HANDSHAKE_VERSION) {
        throw new IllegalStateException(
            String.format(
                "Detected wrong version of GemFire product during handshake. Expected %s but found %s",
                HANDSHAKE_VERSION, handshakeByte));
      }
      InternalDistributedMember remote = DSFIDFactory.readInternalDistributedMember(dis);
      setRemoteAddr(remote);
      this.sharedResource = dis.readBoolean();
      this.preserveOrder = dis.readBoolean();
      this.uniqueId = dis.readLong();
      // read the product version ordinal for on-the-fly serialization
      // transformations (for rolling upgrades)
      this.remoteVersion = Version.readVersion(dis, true);
      int dominoNumber = 0;
      if (this.remoteVersion == null
          || (this.remoteVersion.compareTo(Version.GFE_80) >= 0)) {
        dominoNumber = dis.readInt();
        if (this.sharedResource) {
          dominoNumber = 0;
        }
        dominoCount.set(dominoNumber);
        // this.senderName = dis.readUTF();
      }
      if (!this.sharedResource) {
        if (tipDomino()) {
          logger.info(
              "thread owned receiver forcing itself to send on thread owned sockets");
          // bug #49565 - if domino count is >= 2 use shared resources.
          // Also see DistributedCacheOperation#supportsDirectAck
        } else { // if (dominoNumber < 2) {
          ConnectionTable.threadWantsOwnResources();
          if (logger.isDebugEnabled()) {
            logger.debug(
                "thread-owned receiver with domino count of {} will prefer sending on thread-owned sockets",
                dominoNumber);
          }
          // } else {
          // ConnectionTable.threadWantsSharedResources();
        }
        this.conduit.getStats().incThreadOwnedReceivers(1L, dominoNumber);
        // Because this thread is not shared resource, it will be used for direct
        // ack. Direct ack messages can be large. This call will resize the send
        // buffer.
        setSendBufferSize(this.socket);
      }
      // String name = owner.getDM().getConfig().getName();
      // if (name == null) {
      // name = "pid="+OSProcess.getId();
      // }
      setThreadName(dominoNumber);
    } catch (Exception e) {
      this.owner.getConduit().getCancelCriterion().checkCancelInProgress(e); // bug 37101
      logger.fatal("Error deserializing P2P handshake message", e);
      this.readerShuttingDown = true;
      requestClose("Error deserializing P2P handshake message");
      return true;
    }
    if (logger.isDebugEnabled()) {
      logger.debug("P2P handshake remoteAddr is {}{}", this.remoteAddr,
          (this.remoteVersion != null ? " (" + this.remoteVersion + ')' : ""));
    }
    try {
      String authInit = System.getProperty(
          DistributionConfigImpl.SECURITY_SYSTEM_PREFIX + SECURITY_PEER_AUTH_INIT);
      boolean isSecure = authInit != null && authInit.length() != 0;

      if (isSecure) {
        if (owner.getConduit().waitForMembershipCheck(this.remoteAddr)) {
          sendOKHandshakeReply(); // fix for bug 33224
          notifyHandshakeWaiter(true);
        } else {
          // ARB: check if we need notifyHandshakeWaiter() call.
          notifyHandshakeWaiter(false);
          logger.warn("{} timed out during a membership check.",
              p2pReaderName());
          return true;
        }
      } else {
        sendOKHandshakeReply(); // fix for bug 33224
        try {
          notifyHandshakeWaiter(true);
        } catch (Exception e) {
          logger.fatal("Uncaught exception from listener", e);
        }
      }
      this.finishedConnecting = true;
    } catch (IOException ex) {
      final String err = "Failed sending handshake reply";
      if (logger.isDebugEnabled()) {
        logger.debug(err, ex);
      }
      this.readerShuttingDown = true;
      requestClose(err + ": " + ex);
      return true;
    }
    return false;
  }

  private boolean readMessageHeader(ByteBuffer peerDataBuffer) throws IOException {
    int headerStartPos = peerDataBuffer.position();
    messageLength = peerDataBuffer.getInt();
    /* nioMessageVersion = */
    calcHdrVersion(messageLength);
    messageLength = calcMsgByteSize(messageLength);
    messageType = peerDataBuffer.get();
    messageId = peerDataBuffer.getShort();
    directAck = (messageType & DIRECT_ACK_BIT) != 0;
    if (directAck) {
      messageType &= ~DIRECT_ACK_BIT; // clear the ack bit
    }
    // Following validation fixes bug 31145
    if (!validMsgType(messageType)) {
      Integer nioMessageTypeInteger = (int) messageType;
      logger.fatal("Unknown P2P message type: {}", nioMessageTypeInteger);
      this.readerShuttingDown = true;
      requestClose(String.format("Unknown P2P message type: %s",
          nioMessageTypeInteger));
      return true;
    }
    lengthSet = true;
    // keep the header "in" the buffer until we have read the entire msg.
    // Trust me: this will reduce copying on large messages.
    peerDataBuffer.position(headerStartPos);
    return false;
  }

  private void readMessage(ByteBuffer peerDataBuffer) {
    if (messageType == NORMAL_MSG_TYPE) {
      this.owner.getConduit().getStats().incMessagesBeingReceived(true, messageLength);
      ByteBufferInputStream bbis =
          remoteVersion == null ? new ByteBufferInputStream(peerDataBuffer)
              : new VersionedByteBufferInputStream(peerDataBuffer, remoteVersion);
      DistributionMessage msg;
      try {
        ReplyProcessor21.initMessageRPId();
        // add serialization stats
        long startSer = this.owner.getConduit().getStats().startMsgDeserialization();
        int startingPosition = peerDataBuffer.position();
        try {
          msg = (DistributionMessage) InternalDataSerializer.readDSFID(bbis);
        } catch (SerializationException e) {
          logger.info("input buffer starting position {} "
              + " current position {} limit {} capacity {} message length {}",
              startingPosition, peerDataBuffer.position(), peerDataBuffer.limit(),
              peerDataBuffer.capacity(), messageLength);
          throw e;
        }
        this.owner.getConduit().getStats().endMsgDeserialization(startSer);
        if (bbis.available() != 0) {
          logger.warn("Message deserialization of {} did not read {} bytes.",
              msg, bbis.available());
        }
        try {
          if (!dispatchMessage(msg, messageLength, directAck)) {
            directAck = false;
          }
        } catch (MemberShunnedException e) {
          directAck = false; // don't respond (bug39117)
        } catch (Exception de) {
          this.owner.getConduit().getCancelCriterion().checkCancelInProgress(de);
          logger.fatal("Error dispatching message", de);
        } catch (ThreadDeath td) {
          throw td;
        } catch (VirtualMachineError err) {
          SystemFailure.initiateFailure(err);
          // If this ever returns, rethrow the error. We're poisoned
          // now, so don't let this thread continue.
          throw err;
        } catch (Throwable t) {
          // Whenever you catch Error or Throwable, you must also
          // catch VirtualMachineError (see above). However, there is
          // _still_ a possibility that you are dealing with a cascading
          // error condition, so you also need to check to see if the JVM
          // is still usable:
          SystemFailure.checkFailure();
          logger.fatal("Throwable dispatching message", t);
        }
      } catch (VirtualMachineError err) {
        SystemFailure.initiateFailure(err);
        // If this ever returns, rethrow the error. We're poisoned
        // now, so don't let this thread continue.
        throw err;
      } catch (Throwable t) {
        // Whenever you catch Error or Throwable, you must also
        // catch VirtualMachineError (see above). However, there is
        // _still_ a possibility that you are dealing with a cascading
        // error condition, so you also need to check to see if the JVM
        // is still usable:
        SystemFailure.checkFailure();
        sendFailureReply(ReplyProcessor21.getMessageRPId(),
            "Error deserializing message", t,
            directAck);
        if (t instanceof ThreadDeath) {
          throw (ThreadDeath) t;
        }
        if (t instanceof CancelException) {
          if (!(t instanceof CacheClosedException)) {
            // Just log a message if we had trouble deserializing due to
            // CacheClosedException; see bug 43543
            throw (CancelException) t;
          }
        }
        logger.fatal("Error deserializing message", t);
      } finally {
        ReplyProcessor21.clearMessageRPId();
      }
    } else if (messageType == CHUNKED_MSG_TYPE) {
      MsgDestreamer md = obtainMsgDestreamer(messageId, remoteVersion);
      this.owner.getConduit().getStats().incMessagesBeingReceived(md.size() == 0,
          messageLength);
      try {
        md.addChunk(peerDataBuffer, messageLength);
      } catch (IOException ex) {
      }
    } else /* (nioMessageType == END_CHUNKED_MSG_TYPE) */ {
      MsgDestreamer md = obtainMsgDestreamer(messageId, remoteVersion);
      this.owner.getConduit().getStats().incMessagesBeingReceived(md.size() == 0,
          messageLength);
      try {
        md.addChunk(peerDataBuffer, messageLength);
      } catch (IOException ex) {
        logger.fatal("Failed handling end chunk message", ex);
      }
      DistributionMessage msg = null;
      int msgLength;
      String failureMsg = null;
      Throwable failureEx = null;
      int rpId = 0;
      boolean interrupted = false;
      try {
        msg = md.getMessage();
      } catch (ClassNotFoundException ex) {
        this.owner.getConduit().getStats().decMessagesBeingReceived(md.size());
        failureMsg = "ClassNotFound deserializing message";
        failureEx = ex;
        rpId = md.getRPid();
        logger.fatal("ClassNotFound deserializing message: {}", ex.toString());
      } catch (IOException ex) {
        this.owner.getConduit().getStats().decMessagesBeingReceived(md.size());
        failureMsg = "IOException deserializing message";
        failureEx = ex;
        rpId = md.getRPid();
        logger.fatal("IOException deserializing message", failureEx);
      } catch (InterruptedException ex) {
        interrupted = true;
        this.owner.getConduit().getCancelCriterion().checkCancelInProgress(ex);
      } catch (VirtualMachineError err) {
        SystemFailure.initiateFailure(err);
        // If this ever returns, rethrow the error. We're poisoned
        // now, so don't let this thread continue.
        throw err;
      } catch (Throwable ex) {
        // Whenever you catch Error or Throwable, you must also
        // catch VirtualMachineError (see above). However, there is
        // _still_ a possibility that you are dealing with a cascading
        // error condition, so you also need to check to see if the JVM
        // is still usable:
        SystemFailure.checkFailure();
        this.owner.getConduit().getCancelCriterion().checkCancelInProgress(ex);
        this.owner.getConduit().getStats().decMessagesBeingReceived(md.size());
        failureMsg = "Unexpected failure deserializing message";
        failureEx = ex;
        rpId = md.getRPid();
        logger.fatal("Unexpected failure deserializing message",
            failureEx);
      } finally {
        msgLength = md.size();
        releaseMsgDestreamer(messageId, md);
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
      if (msg != null) {
        try {
          if (!dispatchMessage(msg, msgLength, directAck)) {
            directAck = false;
          }
        } catch (MemberShunnedException e) {
          // not a member anymore - don't reply
          directAck = false;
        } catch (Exception de) {
          this.owner.getConduit().getCancelCriterion().checkCancelInProgress(de);
          logger.fatal("Error dispatching message", de);
        } catch (ThreadDeath td) {
          throw td;
        } catch (VirtualMachineError err) {
          SystemFailure.initiateFailure(err);
          // If this ever returns, rethrow the error. We're poisoned
          // now, so don't let this thread continue.
          throw err;
        } catch (Throwable t) {
          // Whenever you catch Error or Throwable, you must also
          // catch VirtualMachineError (see above). However, there is
          // _still_ a possibility that you are dealing with a cascading
          // error condition, so you also need to check to see if the JVM
          // is still usable:
          SystemFailure.checkFailure();
          logger.fatal("Throwable dispatching message", t);
        }
      } else if (failureEx != null) {
        sendFailureReply(rpId, failureMsg, failureEx, directAck);
      }
    }
  }

  private void readHandshakeForSender(DataInputStream dis, ByteBuffer peerDataBuffer) {
    try {
      this.replyCode = dis.readUnsignedByte();
      switch (replyCode) {
        case REPLY_CODE_OK:
          ioFilter.doneReading(peerDataBuffer);
          notifyHandshakeWaiter(true);
          return;
        case REPLY_CODE_OK_WITH_ASYNC_INFO:
          this.asyncDistributionTimeout = dis.readInt();
          this.asyncQueueTimeout = dis.readInt();
          this.asyncMaxQueueSize = (long) dis.readInt() * (1024 * 1024);
          if (this.asyncDistributionTimeout != 0) {
            logger.info("{} async configuration received {}.",
                p2pReaderName(),
                " asyncDistributionTimeout=" + this.asyncDistributionTimeout
                    + " asyncQueueTimeout=" + this.asyncQueueTimeout
                    + " asyncMaxQueueSize="
                    + (this.asyncMaxQueueSize / (1024 * 1024)));
          }
          // read the product version ordinal for on-the-fly serialization
          // transformations (for rolling upgrades)
          this.remoteVersion = Version.readVersion(dis, true);
          ioFilter.doneReading(peerDataBuffer);
          notifyHandshakeWaiter(true);
          return;
        default:
          String err =
              "Unknown handshake reply code: %s nioMessageLength: %s";
          Object[] errArgs = new Object[] {this.replyCode,
              messageLength};
          if (replyCode == 0 && logger.isDebugEnabled()) { // bug 37113
            logger.debug(
                String.format(err, errArgs) + " (peer probably departed ungracefully)");
          } else {
            logger.fatal(err, errArgs);
          }
          this.readerShuttingDown = true;
          requestClose(String.format(err, errArgs));
          return;
      }
    } catch (Exception e) {
      this.owner.getConduit().getCancelCriterion().checkCancelInProgress(e);
      logger.fatal("Error deserializing P2P handshake reply", e);
      this.readerShuttingDown = true;
      requestClose("Error deserializing P2P handshake reply");
      return;
    } catch (ThreadDeath td) {
      throw td;
    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error. We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (Throwable t) {
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above). However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      logger.fatal("Throwable deserializing P2P handshake reply",
          t);
      this.readerShuttingDown = true;
      requestClose("Throwable deserializing P2P handshake reply");
      return;
    }
  }

  private void setThreadName(int dominoNumber) {
    Thread.currentThread().setName(THREAD_KIND_IDENTIFIER + " for " + this.remoteAddr + " "
        + (this.sharedResource ? "" : "un") + "shared" + " " + (this.preserveOrder ? "" : "un")
        + "ordered" + " uid=" + this.uniqueId + (dominoNumber > 0 ? (" dom #" + dominoNumber) : "")
        + " port=" + this.socket.getPort());
  }

  private void compactOrResizeBuffer(int messageLength) {
    final int oldBufferSize = inputBuffer.capacity();
    final DMStats stats = this.owner.getConduit().getStats();
    int allocSize = messageLength + MSG_HEADER_BYTES;
    if (oldBufferSize < allocSize) {
      // need a bigger buffer
      logger.info("Allocating larger network read buffer, new size is {} old size was {}.",
          allocSize, oldBufferSize);
      ByteBuffer oldBuffer = inputBuffer;
      inputBuffer = getBufferPool().acquireReceiveBuffer(allocSize);

      if (oldBuffer != null) {
        int oldByteCount = oldBuffer.remaining();
        inputBuffer.put(oldBuffer);
        inputBuffer.position(oldByteCount);
        getBufferPool().releaseReceiveBuffer(oldBuffer);
      }
    } else {
      if (inputBuffer.position() != 0) {
        inputBuffer.compact();
      } else {
        inputBuffer.position(inputBuffer.limit());
        inputBuffer.limit(inputBuffer.capacity());
      }
    }
  }

  private boolean dispatchMessage(DistributionMessage msg, int bytesRead, boolean directAck) {
    try {
      msg.setDoDecMessagesBeingReceived(true);
      if (directAck) {
        Assert.assertTrue(!isSharedResource(),
            "We were asked to send a direct reply on a shared socket");
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

  protected TCPConduit getConduit() {
    return this.conduit;
  }

  protected Socket getSocket() throws SocketException {
    // fix for bug 37286
    Socket result = this.socket;
    if (result == null) {
      throw new SocketException(
          "socket has been closed");
    }
    return result;
  }

  boolean isSocketClosed() {
    return this.socket.isClosed() || !this.socket.isConnected();
  }

  boolean isReceiverStopped() {
    return this.stopped;
  }

  private boolean isSocketInUse() {
    return this.socketInUse;
  }


  protected void accessed() {
    this.accessed = true;
  }

  /**
   * return the DM id of the member on the other side of this connection.
   */
  public InternalDistributedMember getRemoteAddress() {
    return this.remoteAddr;
  }

  /**
   * Return the version of the member on the other side of this connection.
   */
  Version getRemoteVersion() {
    return this.remoteVersion;
  }

  @Override
  public String toString() {
    return String.valueOf(remoteAddr) + '@' + this.uniqueId
        + (this.remoteVersion != null ? ('(' + this.remoteVersion.toString() + ')')
            : "") /*
                   * DEBUG + " accepted=" + this.isReceiver + " connected=" + this.connected +
                   * " hash=" + System.identityHashCode(this) + " preserveOrder=" +
                   * this.preserveOrder + " closing=" + isClosing() + ">"
                   */;
  }

  /**
   * answers whether this connection was initiated in this vm
   *
   * @return true if the connection was initiated here
   * @since GemFire 5.1
   */
  boolean getOriginatedHere() {
    return !this.isReceiver;
  }

  /**
   * answers whether this connection is used for ordered message delivery
   */
  boolean getPreserveOrder() {
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
  long getMessagesReceived() {
    return messagesReceived;
  }

  /**
   * answers the number of messages sent on this connection
   */
  long getMessagesSent() {
    return messagesSent;
  }

  public void acquireSendPermission() throws ConnectionException {
    if (!this.connected) {
      throw new ConnectionException(
          "connection is closed");
    }
    if (isReaderThread()) {
      // reader threads send replies and we always want to permit those without waiting
      return;
    }
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
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
    if (!this.connected) {
      this.senderSem.release();
      this.owner.getConduit().getCancelCriterion().checkCancelInProgress(null); // bug 37101
      throw new ConnectionException(
          "connection is closed");
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
    // just in case 1 or more connections are currently waiting to acquire.
    // One of them will get the permit, then find out the connection is closed
    // and release the permit until all the connections currently waiting to acquire
    // will complete by throwing a ConnectionException.
    releaseSendPermission();
  }

}
