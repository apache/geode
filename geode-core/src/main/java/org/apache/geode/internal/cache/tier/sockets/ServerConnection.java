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

import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_ACCESSOR;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_ACCESSOR_PP;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_AUTHENTICATOR;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.security.Principal;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.lang.mutable.MutableInt;
import org.apache.logging.log4j.Logger;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.ThreadState;

import org.apache.geode.CancelException;
import org.apache.geode.DataSerializer;
import org.apache.geode.SystemFailure;
import org.apache.geode.cache.UnsupportedVersionException;
import org.apache.geode.cache.client.internal.AbstractOp;
import org.apache.geode.cache.client.internal.Connection;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.tier.Acceptor;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.CommunicationMode;
import org.apache.geode.internal.cache.tier.InternalClientMembership;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.ServerSideHandshake;
import org.apache.geode.internal.cache.tier.sockets.command.Default;
import org.apache.geode.internal.logging.InternalLogWriter;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.security.AuthorizeRequest;
import org.apache.geode.internal.security.AuthorizeRequestPP;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.util.Breadcrumbs;
import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.security.AuthenticationRequiredException;
import org.apache.geode.security.GemFireSecurityException;

/**
 * Provides an implementation for the server socket end of the hierarchical cache connection. Each
 * server connection runs in its own thread to maximize concurrency and improve response times to
 * edge requests
 *
 * @since GemFire 2.0.2
 */
public abstract class ServerConnection implements Runnable {

  protected static final Logger logger = LogService.getLogger();

  /**
   * This is a buffer that we add to client readTimeout value before we cleanup the connection. This
   * buffer time helps prevent EOF in the client instead of SocketTimeout
   */
  private static final int TIMEOUT_BUFFER_FOR_CONNECTION_CLEANUP_MS = 5000;

  private static final String DISALLOW_INTERNAL_MESSAGES_WITHOUT_CREDENTIALS_NAME =
      "geode.disallow-internal-messages-without-credentials";

  /**
   * When true requires some formerly credential-less messages to carry credentials. See GEODE-3249
   * and ServerConnection.isInternalMessage()
   */
  public static boolean allowInternalMessagesWithoutCredentials =
      !(Boolean.getBoolean(DISALLOW_INTERNAL_MESSAGES_WITHOUT_CREDENTIALS_NAME));

  private Map commands;

  protected final SecurityService securityService;

  protected final CacheServerStats stats;

  private final ServerSideHandshakeFactory handshakeFactory = new ServerSideHandshakeFactory();

  // The key is the size of each ByteBuffer. The value is a queue of byte buffers all of that size.
  private static final ConcurrentHashMap<Integer, LinkedBlockingQueue<ByteBuffer>> commBufferMap =
      new ConcurrentHashMap<>(4, 0.75f, 1);
  private ServerConnectionCollection serverConnectionCollection;

  public static ByteBuffer allocateCommBuffer(int size, Socket sock) {
    // I expect that size will almost always be the same value
    if (sock.getChannel() == null) {
      // The socket this commBuffer will be used for is old IO (it has no channel).
      // So the commBuffer should be heap based.
      return ByteBuffer.allocate(size);
    }
    LinkedBlockingQueue<ByteBuffer> q = commBufferMap.get(size);
    ByteBuffer result = null;
    if (q != null) {
      result = q.poll();
    }
    if (result == null) {
      result = ByteBuffer.allocateDirect(size);
    } else {
      result.position(0);
      result.limit(result.capacity());
    }
    return result;
  }

  public static void releaseCommBuffer(ByteBuffer bb) {
    if (bb != null && bb.isDirect()) {
      LinkedBlockingQueue<ByteBuffer> q = commBufferMap.get(bb.capacity());
      if (q == null) {
        q = new LinkedBlockingQueue<>();
        LinkedBlockingQueue<ByteBuffer> oldQ = commBufferMap.putIfAbsent(bb.capacity(), q);
        if (oldQ != null) {
          q = oldQ;
        }
      }
      q.offer(bb);
    }
  }

  public static void emptyCommBufferPool() {
    for (LinkedBlockingQueue<ByteBuffer> q : commBufferMap.values()) {
      q.clear();
    }
  }

  Socket theSocket;
  private ByteBuffer commBuffer;
  protected final CachedRegionHelper crHelper;
  protected String name;

  // IMPORTANT: if new messages are added change setHandshake to initialize them
  // to the correct Version for serializing to the client
  private Message requestMessage = new Message(2, Version.CURRENT);
  private Message replyMessage = new Message(1, Version.CURRENT);
  private Message responseMessage = new Message(1, Version.CURRENT);
  private Message errorMessage = new Message(1, Version.CURRENT);

  // IMPORTANT: if new messages are added change setHandshake to initialize them
  // to the correct Version for serializing to the client
  private ChunkedMessage queryResponseMessage = new ChunkedMessage(2, Version.CURRENT);
  private ChunkedMessage chunkedResponseMessage = new ChunkedMessage(1, Version.CURRENT);
  private ChunkedMessage executeFunctionResponseMessage = new ChunkedMessage(1, Version.CURRENT);
  private ChunkedMessage registerInterestResponseMessage = new ChunkedMessage(1, Version.CURRENT);
  private ChunkedMessage keySetResponseMessage = new ChunkedMessage(1, Version.CURRENT);

  private final InternalLogWriter logWriter;
  private final InternalLogWriter securityLogWriter;
  final AcceptorImpl acceptor;
  private Thread owner;

  /**
   * Handshake reference uniquely identifying a client
   */
  protected ServerSideHandshake handshake;
  private int handshakeTimeout;
  private final Object handshakeMonitor = new Object();

  /*
   * This timeout is request specific which come with message itself Otherwise, timeout which comes
   * during handshake is used.
   */
  private volatile int requestSpecificTimeout = -1;

  /**
   * Tracks the id of the most recent batch to which a reply has been sent
   */
  private int latestBatchIdReplied = -1;

  /**
   * Client identity from handshake
   */
  ClientProxyMembershipID proxyId;

  private byte[] memberIdByteArray;

  /**
   * Authorize client requests using this object. This is set when each operation on this connection
   * is authorized in post-operation phase.
   */
  private AuthorizeRequestPP postAuthzRequest;

  /**
   * The communication mode for this <code>ServerConnection</code>. Valid types include
   * 'client-server', 'gateway-gateway' and 'monitor-server'.
   */
  protected final CommunicationMode communicationMode;

  private long processingMessageStartTime = -1;
  private Object processingMessageLock = new Object();

  private static ConcurrentHashMap<ClientProxyMembershipID, ClientUserAuths> proxyIdVsClientUserAuths =
      new ConcurrentHashMap<>();


  private ClientUserAuths clientUserAuths;

  // this is constant(server and client) for first user request, after that it is random
  // this also need to send in handshake
  private long connectionId = Connection.DEFAULT_CONNECTION_ID;

  private Random randomConnectionIdGen;

  private Part securePart = null;

  protected Principal principal;

  private MessageIdExtractor messageIdExtractor = new MessageIdExtractor();

  /**
   * A debug flag used for testing Backward compatibility
   */
  private static boolean TEST_VERSION_AFTER_HANDSHAKE_FLAG = false;

  /**
   * Creates a new <code>ServerConnection</code> that processes messages received from an edge
   * client over a given <code>Socket</code>.
   */
  public ServerConnection(Socket socket, InternalCache internalCache, CachedRegionHelper helper,
      CacheServerStats stats, int hsTimeout, int socketBufferSize, String communicationModeStr,
      byte communicationMode, Acceptor acceptor, SecurityService securityService) {

    StringBuilder buffer = new StringBuilder(100);
    if (((AcceptorImpl) acceptor).isGatewayReceiver()) {
      buffer.append("GatewayReceiver connection from [");
    } else {
      buffer.append("Server connection from [");
    }
    buffer.append(communicationModeStr).append(" host address=")
        .append(socket.getInetAddress().getHostAddress()).append("; ").append(communicationModeStr)
        .append(" port=").append(socket.getPort()).append("]");
    this.name = buffer.toString();

    this.stats = stats;
    this.acceptor = (AcceptorImpl) acceptor;
    this.crHelper = helper;
    this.logWriter = (InternalLogWriter) internalCache.getLogger();
    this.securityLogWriter = (InternalLogWriter) internalCache.getSecurityLoggerI18n();
    this.communicationMode = CommunicationMode.fromModeNumber(communicationMode);
    this.principal = null;
    this.postAuthzRequest = null;
    this.randomConnectionIdGen = new Random(this.hashCode());

    this.securityService = securityService;

    final boolean isDebugEnabled = logger.isDebugEnabled();
    try {
      theSocket = socket;
      theSocket.setSendBufferSize(socketBufferSize);
      theSocket.setReceiveBufferSize(socketBufferSize);

      if (isDebugEnabled) {
        logger.debug(
            "{}: Accepted client connection from {}[client host name={}; client host address={}; client port={}]",
            getName(), communicationModeStr, socket.getInetAddress().getCanonicalHostName(),
            socket.getInetAddress().getHostAddress(), socket.getPort());
      }
      this.handshakeTimeout = hsTimeout;
    } catch (Exception e) {
      if (isDebugEnabled) {
        logger.debug("While creating server connection", e);
      }
    }
  }

  public AcceptorImpl getAcceptor() {
    return this.acceptor;
  }

  private static final ThreadLocal<Byte> executeFunctionOnLocalNodeOnly =
      ThreadLocal.withInitial(() -> (byte) 0x00);

  public static void executeFunctionOnLocalNodeOnly(Byte value) {
    byte b = value;
    executeFunctionOnLocalNodeOnly.set(b);
  }

  public static Byte isExecuteFunctionOnLocalNodeOnly() {
    return executeFunctionOnLocalNodeOnly.get();
  }

  private boolean verifyClientConnection() {
    synchronized (this.handshakeMonitor) {
      if (this.handshake == null) {
        ServerSideHandshake readHandshake;
        try {

          readHandshake = handshakeFactory.readHandshake(getSocket(), getHandShakeTimeout(),
              getCommunicationMode(), getDistributedSystem(), getSecurityService());

        } catch (SocketTimeoutException timeout) {
          logger.warn("{}: Handshake reply code timeout, not received with in {} ms",
              new Object[] {getName(), handshakeTimeout});
          failConnectionAttempt();
          return false;
        } catch (EOFException | SocketException e) {
          // no need to warn client just gave up on this server before we could
          // handshake
          logger.info("{} {}", getName(), e);
          failConnectionAttempt();
          return false;
        } catch (IOException e) {
          logger.warn(getName() + ": Received no handshake reply code",
              e);
          failConnectionAttempt();
          return false;
        } catch (AuthenticationRequiredException | AuthenticationFailedException ex) {
          handleHandshakeAuthenticationException(ex);
          return false;
        } catch (UnsupportedVersionException uve) {
          // Server logging
          logger.warn("{} {}", getName(), uve.getMessage(), uve);
          handleHandshakeException(uve);
          return false;
        } catch (Exception ex) {
          logger.warn("{} {}", getName(), ex.getLocalizedMessage());
          handleHandshakeException(ex);
          return false;
        }

        setHandshake(readHandshake);
        setProxyId(readHandshake.getMembershipId());
        if (readHandshake.getVersion().compareTo(Version.GFE_65) < 0
            || getCommunicationMode().isWAN()) {
          try {
            setAuthAttributes();

          } catch (AuthenticationRequiredException | AuthenticationFailedException ex) {
            handleHandshakeAuthenticationException(ex);
            return false;
          } catch (Exception ex) {
            logger.warn("{} {}", getName(), ex.getLocalizedMessage());
            handleHandshakeException(ex);
            return false;
          }
        }

        // readHandshake will establish a handshake object in this ServerConnection
        if (this.handshake.isOK()) {
          try {
            return processHandShake();
          } catch (CancelException e) {
            if (!crHelper.isShutdown()) {
              logger.warn(getName() + ": Unexpected cancellation: ", e);
            }
            cleanup();
            return false;
          }
        } else {
          // is this branch ever taken?
          this.crHelper.checkCancelInProgress(null); // bug 37113?
          logger.warn("Received Unknown handshake reply code.");
          refuseHandshake("Received Unknown handshake reply code.", AcceptorImpl.REPLY_INVALID);
          return false;
        }
      }
    }
    return true;
  }

  private void failConnectionAttempt() {
    stats.incFailedConnectionAttempts();
    cleanup();
  }

  private void handleHandshakeException(Exception ex) {
    refuseHandshake(ex.getMessage(), AcceptorImpl.REPLY_REFUSED);
    failConnectionAttempt();
  }

  private void handleHandshakeAuthenticationException(Exception ex) {
    if (ex instanceof AuthenticationRequiredException) {
      AuthenticationRequiredException noauth = (AuthenticationRequiredException) ex;
      String exStr = noauth.getLocalizedMessage();
      if (noauth.getCause() != null) {
        exStr += " : " + noauth.getCause().getLocalizedMessage();
      }
      if (securityLogWriter.warningEnabled()) {
        securityLogWriter.warning(String.format("%s",
            getName() + ": Security exception: " + exStr));
      }
      refuseHandshake(noauth.getMessage(), Handshake.REPLY_EXCEPTION_AUTHENTICATION_REQUIRED);
      failConnectionAttempt();
    } else if (ex instanceof AuthenticationFailedException) {
      AuthenticationFailedException failed = (AuthenticationFailedException) ex;
      String exStr = failed.getLocalizedMessage();
      if (failed.getCause() != null) {
        exStr += " : " + failed.getCause().getLocalizedMessage();
      }
      if (securityLogWriter.warningEnabled()) {
        securityLogWriter.warning(String.format("%s",
            getName() + ": Security exception: " + exStr));
      }
      refuseHandshake(failed.getMessage(), Handshake.REPLY_EXCEPTION_AUTHENTICATION_FAILED);
      failConnectionAttempt();
    } else {
      logger.warn(
          "Unexpected exception type in ServerConnection handleHandshakeAuthenticationException");
      throw new RuntimeException(
          "Invalid exception type, must be either AuthenticationRequiredException or AuthenticationFailedException",
          ex);
    }
  }

  protected Map getCommands() {
    return this.commands;
  }

  protected Socket getSocket() {
    return this.theSocket;
  }

  private int getHandShakeTimeout() {
    return this.handshakeTimeout;
  }

  protected DistributedSystem getDistributedSystem() {
    return getCache().getDistributedSystem();
  }

  public InternalCache getCache() {
    return this.crHelper.getCache();
  }

  public ServerSideHandshake getHandshake() {
    return this.handshake;
  }

  public void setHandshake(ServerSideHandshake handshake) {
    this.handshake = handshake;
    Version v = handshake.getVersion();

    this.replyMessage.setVersion(v);
    this.requestMessage.setVersion(v);
    this.responseMessage.setVersion(v);
    this.errorMessage.setVersion(v);

    this.queryResponseMessage.setVersion(v);
    this.chunkedResponseMessage.setVersion(v);
    this.executeFunctionResponseMessage.setVersion(v);
    this.registerInterestResponseMessage.setVersion(v);
    this.keySetResponseMessage.setVersion(v);
  }

  void setRequestMessage(Message requestMessage) {
    this.requestMessage = requestMessage;
  }

  public Version getClientVersion() {
    return this.handshake.getVersion();
  }

  protected void setProxyId(ClientProxyMembershipID proxyId) {
    this.proxyId = proxyId;
    this.memberIdByteArray = EventID.getMembershipId(proxyId);
    this.name = "Server connection from [" + proxyId + "; port=" + this.theSocket.getPort() + "]";
  }

  protected void setPrincipal(Principal principal) {
    this.principal = principal;
  }

  // hitesh:this is for backward compability
  private long setUserAuthorizeAndPostAuthorizeRequest(AuthorizeRequest authzRequest,
      AuthorizeRequestPP postAuthzRequest) throws IOException {
    UserAuthAttributes userAuthAttr = new UserAuthAttributes(authzRequest, postAuthzRequest);
    if (this.clientUserAuths == null) {
      this.initializeClientUserAuths();
    }
    try {
      return this.clientUserAuths.putUserAuth(userAuthAttr);
    } catch (NullPointerException exception) {
      if (this.isTerminated()) {
        // Bug #52023.
        throw new IOException("Server connection is terminated.");
      }
      throw exception;
    }
  }

  public InternalLogWriter getSecurityLogWriter() {
    return this.securityLogWriter;
  }

  private SecurityService getSecurityService() {
    return this.securityService;
  }

  private boolean incedCleanupTableRef = false;
  private boolean incedCleanupProxyIdTableRef = false;

  private final Object chmLock = new Object();
  private boolean chmRegistered = false;

  private Map<ServerSideHandshake, MutableInt> getCleanupTable() {
    return acceptor.getClientHealthMonitor().getCleanupTable();
  }

  private Map<ClientProxyMembershipID, MutableInt> getCleanupProxyIdTable() {
    return acceptor.getClientHealthMonitor().getCleanupProxyIdTable();
  }

  boolean processHandShake() {
    boolean result = false;
    boolean clientJoined = false;
    boolean registerClient = false;

    final boolean isDebugEnabled = logger.isDebugEnabled();
    try {
      synchronized (getCleanupTable()) {
        MutableInt numRefs = getCleanupTable().get(this.handshake);
        byte endpointType = (byte) 0;
        int queueSize = 0;

        if (this.proxyId.isDurable()) {
          if (isDebugEnabled) {
            logger.debug("looking if the Proxy existed for this durable client or not :{}",
                this.proxyId);
          }
          CacheClientProxy proxy =
              getAcceptor().getCacheClientNotifier().getClientProxy(this.proxyId);
          if (proxy != null && proxy.waitRemoval()) {
            proxy = getAcceptor().getCacheClientNotifier().getClientProxy(this.proxyId);
          }
          if (proxy != null) {
            if (isDebugEnabled) {
              logger.debug("Proxy existed for this durable client :{} and proxy : {}", this.proxyId,
                  proxy);
            }
            if (proxy.isPrimary()) {
              endpointType = (byte) 2;
              queueSize = proxy.getQueueSize();
            } else {
              endpointType = (byte) 1;
              queueSize = proxy.getQueueSize();
            }
          }
          // Bug Fix for 37986
          if (numRefs == null) {
            // Check whether this is a durable client first. A durable client with
            // the same id is not allowed. In this case, reject the client.
            if (proxy != null && !proxy.isPaused()) {
              // The handshake refusal message must be smaller than 127 bytes.
              String handshakeRefusalMessage =
                  String.format("Duplicate durable clientId (%s)",
                      proxyId.getDurableId());
              logger.warn("{} : {}",
                  new Object[] {this.name, handshakeRefusalMessage});
              refuseHandshake(handshakeRefusalMessage,
                  Handshake.REPLY_EXCEPTION_DUPLICATE_DURABLE_CLIENT);
              return result;
            }
          }
        }
        if (numRefs != null) {
          if (acceptHandShake(endpointType, queueSize)) {
            numRefs.increment();
            this.incedCleanupTableRef = true;
            result = true;
          }
          return result;
        } else {
          if (acceptHandShake(endpointType, queueSize)) {
            clientJoined = true;
            getCleanupTable().put(this.handshake, new MutableInt(1));
            this.incedCleanupTableRef = true;
            this.stats.incCurrentClients();
            result = true;
          }
          return result;
        }
      }
    } finally {
      if (isTerminated() || !result) {
        return false;
      }
      synchronized (getCleanupProxyIdTable()) {
        MutableInt numRefs = getCleanupProxyIdTable().get(this.proxyId);
        if (numRefs != null) {
          numRefs.increment();
        } else {
          registerClient = true;
          getCleanupProxyIdTable().put(this.proxyId, new MutableInt(1));
        }
        this.incedCleanupProxyIdTableRef = true;
      }

      if (isDebugEnabled) {
        logger.debug("{}registering client {}", (registerClient ? "" : "not "), proxyId);
      }
      this.crHelper.checkCancelInProgress(null);
      if (clientJoined && isFiringMembershipEvents()) {
        // This is a new client. Notify bridge membership and heartbeat monitor.
        InternalClientMembership.notifyClientJoined(this.proxyId.getDistributedMember());
      }

      ClientHealthMonitor chm = this.acceptor.getClientHealthMonitor();
      synchronized (this.chmLock) {
        this.chmRegistered = true;
      }
      if (registerClient) {
        // hitesh: it will add client
        chm.registerClient(this.proxyId);
      }
      // hitesh:it will add client connection in set
      serverConnectionCollection = chm.addConnection(this.proxyId, this);
      this.acceptor.getConnectionListener().connectionOpened(registerClient, communicationMode);
      // Hitesh: add user creds in map for single user case.
    }
  }

  private boolean isFiringMembershipEvents() {
    return this.acceptor.isRunning()
        && !(this.acceptor.getCachedRegionHelper().getCache()).isClosed()
        && !acceptor.getCachedRegionHelper().getCache().getCancelCriterion().isCancelInProgress();
  }

  private void refuseHandshake(String message, byte exception) {
    try {
      acceptor.refuseHandshake(this.theSocket.getOutputStream(), message, exception);
    } catch (IOException ignore) {
    } finally {
      this.stats.incFailedConnectionAttempts();
      cleanup();
    }
  }

  private boolean acceptHandShake(byte endpointType, int queueSize) {
    return doHandShake(endpointType, queueSize) && handshakeAccepted();
  }

  protected abstract boolean doHandShake(byte epType, int qSize);

  private boolean handshakeAccepted() {
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Accepted handshake", this.name);
    }

    if (this.communicationMode == CommunicationMode.ClientToServerForQueue) {
      this.stats.incCurrentQueueConnections();
    } else {
      this.stats.incCurrentClientConnections();
    }
    return true;
  }

  public void setCq(String cqName, boolean isDurable) throws Exception {
    final boolean isDebugEnabled = logger.isDebugEnabled();
    if (this.requestMessage.isSecureMode()) {
      if (isDebugEnabled) {
        logger.debug("setCq() security header found registering CQname = {}", cqName);
      }
      try {
        byte[] secureBytes = this.requestMessage.getSecureBytes();

        secureBytes = this.handshake.getEncryptor().decryptBytes(secureBytes);
        AuthIds aIds = new AuthIds(secureBytes);

        long uniqueId = aIds.getUniqueId();

        CacheClientProxy proxy =
            getAcceptor().getCacheClientNotifier().getClientProxy(this.proxyId);

        if (proxy != null) {
          proxy.setCQVsUserAuth(cqName, uniqueId, isDurable);
        }
      } catch (Exception ex) {
        if (isDebugEnabled) {
          logger.debug("While setting cq got exception ", ex);
        }
        throw ex;
      }
    } else {
      if (isDebugEnabled) {
        logger.debug("setCq() security header is not found ");
      }
    }
  }

  public void removeCq(String cqName, boolean isDurable) {
    final boolean isDebugEnabled = logger.isDebugEnabled();
    if (this.requestMessage.isSecureMode()) {
      if (isDebugEnabled) {
        logger.debug("removeCq() security header found registering CQname = {}", cqName);
      }
      try {
        this.clientUserAuths.removeUserAuthAttributesForCq(cqName, isDurable);
      } catch (Exception ex) {
        if (isDebugEnabled) {
          logger.debug("While setting cq got exception ", ex);
        }
      }
    } else {
      if (isDebugEnabled) {
        logger.debug("removeCq() security header is not found");
      }
    }
  }

  /**
   * @return whether this is a connection to a client, regardless of protocol.
   */
  public boolean isClientServerConnection() {
    return communicationMode.isClientToServerOrSubscriptionFeed();
  }

  private boolean clientDisconnectedCleanly = false;
  private Throwable clientDisconnectedException;
  private int failureCount = 0;
  boolean processMessages = true;

  protected void doHandshake() {
    // hitesh:to create new connection handshake
    if (verifyClientConnection()) {
      // Initialize the commands after the handshake so that the version
      // can be used.
      initializeCommands();
      // its initialized in verifyClientConnection call
      if (!getCommunicationMode().isWAN()) {
        initializeClientUserAuths();
      }
    }
    if (TEST_VERSION_AFTER_HANDSHAKE_FLAG) {
      short testVersionAfterHandshake = 4;
      Assert.assertTrue((this.handshake.getVersion().ordinal() == testVersionAfterHandshake),
          "Found different version after handshake");
      TEST_VERSION_AFTER_HANDSHAKE_FLAG = false;
    }
  }

  void doNormalMessage() {
    if (serverConnectionCollection == null) {
      // return here if we haven't successfully completed handshake
      logger.warn("Continued processing ServerConnection after handshake failed");
      this.processMessages = false;
      return;
    }
    Message message;
    message = BaseCommand.readRequest(this);
    synchronized (serverConnectionCollection) {
      if (serverConnectionCollection.isTerminating) {
        // Client is being disconnected, don't try to process message.
        this.processMessages = false;
        return;
      }
      serverConnectionCollection.connectionsProcessing.incrementAndGet();
    }
    ThreadState threadState = null;
    try {
      if (message != null) {
        // Since this thread is not interrupted when the cache server is shutdown, test again after
        // a message has been read. This is a bit of a hack. I think this thread should be
        // interrupted, but currently AcceptorImpl doesn't keep track of the threads that it
        // launches.
        if (!this.processMessages || (crHelper.isShutdown())) {
          if (logger.isDebugEnabled()) {
            logger.debug("{} ignoring message of type {} from client {} due to shutdown.",
                getName(), MessageType.getString(message.getMessageType()), this.proxyId);
          }
          return;
        }

        if (message.getMessageType() != MessageType.PING) {
          // check for invalid number of message parts
          if (message.getNumberOfParts() <= 0) {
            failureCount++;
            if (failureCount > 3) {
              this.processMessages = false;
              return;
            } else {
              return;
            }
          }
        }

        if (logger.isTraceEnabled()) {
          logger.trace("{} received {} with txid {}", getName(),
              MessageType.getString(message.getMessageType()), message.getTransactionId());
          if (message.getTransactionId() < -1) { // TODO: why is this happening?
            message.setTransactionId(-1);
          }
        }

        if (message.getMessageType() != MessageType.PING) {
          // we have a real message (non-ping),
          // so let's call receivedPing to let the CHM know client is busy
          acceptor.getClientHealthMonitor().receivedPing(this.proxyId);
        }
        Command command = getCommand(message.getMessageType());
        if (command == null) {
          command = Default.getCommand();
        }

        // if a subject exists for this uniqueId, binds the subject to this thread so that we can do
        // authorization later
        if (securityService.isIntegratedSecurity()
            && !isInternalMessage(this.requestMessage, allowInternalMessagesWithoutCredentials)
            && !this.communicationMode.isWAN()) {
          long uniqueId = getUniqueId();
          String messageType = MessageType.getString(this.requestMessage.getMessageType());
          Subject subject = this.clientUserAuths.getSubject(uniqueId);
          if (subject != null) {
            threadState = securityService.bindSubject(subject);
            logger.debug("Bound {} with uniqueId {} for message {} with {}", subject.getPrincipal(),
                uniqueId, messageType, this.getName());
          } else if (uniqueId == 0) {
            logger.debug("No unique ID yet. {}, {}", messageType, this.getName());
          } else {
            logger.error("Failed to bind the subject of uniqueId {} for message {} with {}",
                uniqueId, messageType, this.getName());
            throw new AuthenticationRequiredException("Failed to find the authenticated user.");
          }
        }

        command.execute(message, this, this.securityService);
      }
    } finally {
      // Keep track of the fact that a message is no longer being
      // processed.
      serverConnectionCollection.connectionsProcessing.decrementAndGet();
      setNotProcessingMessage();
      clearRequestMessage();
      if (threadState != null) {
        threadState.clear();
      }
    }
  }

  private final Object terminationLock = new Object();
  private boolean terminated = false;

  public boolean isTerminated() {
    synchronized (this.terminationLock) {
      return this.terminated;
    }
  }

  private void cleanClientAuths() {
    if (this.clientUserAuths != null) {
      this.clientUserAuths.cleanup(false);
    }
  }

  // package access allowed so AcceptorImpl can call
  void handleTermination() {
    if (this.crHelper.isShutdown()) {
      setClientDisconnectCleanly();
    }
    handleTermination(false);
  }

  void handleTermination(boolean timedOut) {
    boolean cleanupStats = false;
    synchronized (this.terminationLock) {
      if (this.terminated) {
        return;
      }
      this.terminated = true;
    }
    boolean clientDeparted = false;
    boolean unregisterClient = false;
    setNotProcessingMessage();
    synchronized (getCleanupTable()) {
      if (this.incedCleanupTableRef) {
        this.incedCleanupTableRef = false;
        cleanupStats = true;
        MutableInt numRefs = getCleanupTable().get(this.handshake);
        if (numRefs != null) {
          numRefs.decrement();
          if (numRefs.toInteger() <= 0) {
            clientDeparted = true;
            getCleanupTable().remove(this.handshake);
            this.stats.decCurrentClients();
          }
        }
        if (this.communicationMode == CommunicationMode.ClientToServerForQueue) {
          this.stats.decCurrentQueueConnections();
        } else {
          this.stats.decCurrentClientConnections();
        }
      }
    }

    synchronized (getCleanupProxyIdTable()) {
      if (this.incedCleanupProxyIdTableRef) {
        this.incedCleanupProxyIdTableRef = false;
        MutableInt numRefs = getCleanupProxyIdTable().get(this.proxyId);
        if (numRefs != null) {
          numRefs.decrement();
          if (numRefs.toInteger() <= 0) {
            unregisterClient = true;
            getCleanupProxyIdTable().remove(this.proxyId);
            // here we can remove entry multiuser map for client
            proxyIdVsClientUserAuths.remove(this.proxyId);
          }
        }
      }
    }
    cleanup(timedOut);
    if (getAcceptor().isRunning()) {
      // If the client has departed notify bridge membership and unregister it from
      // the heartbeat monitor; other wise just remove the connection.
      if (clientDeparted && isFiringMembershipEvents()) {
        if (this.clientDisconnectedCleanly && !forceClientCrashEvent) {
          InternalClientMembership.notifyClientLeft(proxyId.getDistributedMember());
        } else {
          InternalClientMembership.notifyClientCrashed(this.proxyId.getDistributedMember());
        }
        // The client has departed. Remove this last connection and unregister it.
      }
    }

    // moved out of above if to fix bug 36751
    boolean needsUnregister = false;
    synchronized (this.chmLock) {
      if (this.chmRegistered) {
        needsUnregister = true;
        this.chmRegistered = false;
      }
    }
    if (unregisterClient)// last serverconnection call all close on auth objects
    {
      cleanClientAuths();
    }
    this.clientUserAuths = null;
    if (needsUnregister) {
      this.acceptor.getClientHealthMonitor().removeConnection(this.proxyId, this);
      if (unregisterClient) {
        this.acceptor.getClientHealthMonitor().unregisterClient(this.proxyId, getAcceptor(),
            this.clientDisconnectedCleanly, this.clientDisconnectedException);
      }
    }

    if (cleanupStats) {
      this.acceptor.getConnectionListener().connectionClosed(clientDeparted, communicationMode);
    }
  }

  protected abstract void doOneMessage();

  private void initializeClientUserAuths() {
    this.clientUserAuths = getClientUserAuths(this.proxyId);
  }

  static ClientUserAuths getClientUserAuths(ClientProxyMembershipID proxyId) {
    ClientUserAuths clientUserAuths = new ClientUserAuths(proxyId.hashCode());
    ClientUserAuths returnedClientUserAuths =
        proxyIdVsClientUserAuths.putIfAbsent(proxyId, clientUserAuths);

    if (returnedClientUserAuths == null) {
      return clientUserAuths;
    }
    return returnedClientUserAuths;
  }

  void initializeCommands() {
    // The commands are cached here, but are just referencing the ones
    // stored in the CommandInitializer
    this.commands = CommandInitializer.getCommands(this);
  }

  private Command getCommand(Integer messageType) {
    return (Command) this.commands.get(messageType);
  }

  public void removeUserAuth(Message message, boolean keepAlive) {
    try {
      byte[] secureBytes = message.getSecureBytes();

      secureBytes = this.handshake.getEncryptor().decryptBytes(secureBytes);

      // need to decrypt it first then get connectionid
      AuthIds aIds = new AuthIds(secureBytes);

      long connId = aIds.getConnectionId();

      if (connId != this.connectionId) {
        throw new AuthenticationFailedException("Authentication failed");
      }

      try {
        // first try integrated security
        boolean removed = this.clientUserAuths.removeSubject(aIds.getUniqueId());

        // if not successful, try the old way
        if (!removed) {
          this.clientUserAuths.removeUserId(aIds.getUniqueId(), keepAlive);
        }
      } catch (NullPointerException exception) {
        // Bug #52023.
        logger.debug("Exception {}", exception);
      }
    } catch (Exception exception) {
      throw new AuthenticationFailedException("Authentication failed", exception);
    }
  }

  public byte[] setCredentials(Message message) {

    try {
      // need to get connection id from secure part of message, before that need to insure
      // encryption of id
      // need to check here, whether it matches with serverConnection id or not
      // need to decrypt bytes if its in DH mode
      // need to get properties of credentials(need to remove extra stuff if something is there from
      // client)
      // need to generate unique-id for client
      // need to send back in response with encryption
      if (!AcceptorImpl.isAuthenticationRequired() && message.isSecureMode()) {
        // TODO (ashetkar)
        /*
         * This means that client and server VMs have different security settings. The server does
         * not have any security settings specified while client has.
         *
         * Here, should we just ignore this and send the dummy security part (connectionId, userId)
         * in the response (in this case, client needs to know that it is not expected to read any
         * security part in any of the server response messages) or just throw an exception
         * indicating bad configuration?
         */
        // This is a CREDENTIALS_NORMAL case.;
        return new byte[0];
      }
      if (!message.isSecureMode()) {
        throw new AuthenticationFailedException("Authentication failed");
      }

      byte[] secureBytes = message.getSecureBytes();

      secureBytes = this.handshake.getEncryptor().decryptBytes(secureBytes);

      // need to decrypt it first then get connectionid
      AuthIds aIds = new AuthIds(secureBytes);

      long connId = aIds.getConnectionId();

      if (connId != this.connectionId) {
        throw new AuthenticationFailedException("Authentication failed");
      }

      byte[] credBytes = message.getPart(0).getSerializedForm();

      credBytes = this.handshake.getEncryptor().decryptBytes(credBytes);

      ByteArrayInputStream bis = new ByteArrayInputStream(credBytes);
      DataInputStream dinp = new DataInputStream(bis);
      Properties credentials = DataSerializer.readProperties(dinp);

      // When here, security is enforced on server, if login returns a subject, then it's the newly
      // integrated security, otherwise, do it the old way.
      long uniqueId;

      DistributedSystem system = this.getDistributedSystem();
      String methodName = system.getProperties().getProperty(SECURITY_CLIENT_AUTHENTICATOR);

      Object principal = Handshake.verifyCredentials(methodName, credentials,
          system.getSecurityProperties(), (InternalLogWriter) system.getLogWriter(),
          (InternalLogWriter) system.getSecurityLogWriter(), this.proxyId.getDistributedMember(),
          this.securityService);
      if (principal instanceof Subject) {
        Subject subject = (Subject) principal;
        uniqueId = this.clientUserAuths.putSubject(subject);
      } else {
        // this sets principal in map as well....
        uniqueId = getUniqueId((Principal) principal);
      }

      // create secure part which will be send in response
      return encryptId(uniqueId);
    } catch (AuthenticationFailedException | AuthenticationRequiredException exception) {
      throw exception;
    } catch (Exception exception) {
      throw new AuthenticationFailedException("REPLY_REFUSED", exception);
    }
  }

  private void setSecurityPart() {
    try {
      this.connectionId = randomConnectionIdGen.nextLong();
      this.securePart = new Part();
      byte[] id = encryptId(this.connectionId);
      this.securePart.setPartState(id, false);
    } catch (Exception ex) {
      logger.warn("Server failed to encrypt data " + ex);
      throw new GemFireSecurityException("Server failed to encrypt response message.");
    }
  }

  /**
   * MessageType of the messages (typically internal commands) which do not need to participate in
   * security should be added in the following if block.
   *
   * @see AbstractOp#processSecureBytes(Connection, Message)
   * @see AbstractOp#needsUserId()
   * @see AbstractOp#sendMessage(Connection)
   */
  public Part updateAndGetSecurityPart() {
    // need to take care all message types here
    if (AcceptorImpl.isAuthenticationRequired()
        && this.handshake.getVersion().compareTo(Version.GFE_65) >= 0
        && !this.communicationMode.isWAN() && !this.requestMessage.getAndResetIsMetaRegion()
        && !isInternalMessage(this.requestMessage, allowInternalMessagesWithoutCredentials)) {
      setSecurityPart();
      return this.securePart;
    } else {
      if (AcceptorImpl.isAuthenticationRequired() && logger.isDebugEnabled()) {
        logger.debug(
            "ServerConnection.updateAndGetSecurityPart() not adding security part for message type {}",
            MessageType.getString(this.requestMessage.messageType));
      }
    }
    return null;
  }

  public boolean isInternalMessage(Message message, boolean allowOldInternalMessages) {
    int messageType = message.getMessageType();
    boolean isInternalMessage = messageType == MessageType.PING
        || messageType == MessageType.REQUEST_EVENT_VALUE || messageType == MessageType.MAKE_PRIMARY
        || messageType == MessageType.REMOVE_USER_AUTH || messageType == MessageType.CLIENT_READY
        || messageType == MessageType.SIZE || messageType == MessageType.TX_FAILOVER
        || messageType == MessageType.TX_SYNCHRONIZATION || messageType == MessageType.COMMIT
        || messageType == MessageType.ROLLBACK || messageType == MessageType.CLOSE_CONNECTION
        || messageType == MessageType.INVALID || messageType == MessageType.PERIODIC_ACK
        || messageType == MessageType.GET_CLIENT_PR_METADATA
        || messageType == MessageType.GET_CLIENT_PARTITION_ATTRIBUTES;

    // we allow older clients to not send credentials for a handful of messages
    // if and only if a system property is set. This allows a rolling upgrade
    // to be performed.
    if (!isInternalMessage && allowOldInternalMessages) {
      isInternalMessage = messageType == MessageType.GETCQSTATS_MSG_TYPE
          || messageType == MessageType.MONITORCQ_MSG_TYPE
          || messageType == MessageType.REGISTER_DATASERIALIZERS
          || messageType == MessageType.REGISTER_INSTANTIATORS
          || messageType == MessageType.ADD_PDX_TYPE
          || messageType == MessageType.GET_PDX_ID_FOR_TYPE
          || messageType == MessageType.GET_PDX_TYPE_BY_ID
          || messageType == MessageType.GET_FUNCTION_ATTRIBUTES
          || messageType == MessageType.ADD_PDX_ENUM
          || messageType == MessageType.GET_PDX_ID_FOR_ENUM
          || messageType == MessageType.GET_PDX_ENUM_BY_ID
          || messageType == MessageType.GET_PDX_TYPES || messageType == MessageType.GET_PDX_ENUMS;
    }
    return isInternalMessage;
  }

  public void run() {
    setOwner();

    if (getAcceptor().isSelector()) {
      boolean finishedMessage = false;
      try {
        this.stats.decThreadQueueSize();
        if (!isTerminated()) {
          getAcceptor().setTLCommBuffer();
          doOneMessage();
          if (this.processMessages && !(this.crHelper.isShutdown())) {
            registerWithSelector(); // finished message so reregister
            finishedMessage = true;
          }
        }
      } catch (java.nio.channels.ClosedChannelException | CancelException ignore) {
        // ok shutting down
      } catch (IOException ex) {
        logger.warn(ex.toString() + " : Unexpected Exception");
        setClientDisconnectedException(ex);
      } finally {
        getAcceptor().releaseTLCommBuffer();
        // DistributedSystem.releaseThreadsSockets();
        unsetOwner();
        setNotProcessingMessage();
        // unset request specific timeout
        this.unsetRequestSpecificTimeout();
        if (!finishedMessage) {
          try {
            handleTermination();
          } catch (CancelException e) {
            // ignore
          }
        }
      }
    } else {
      try {
        while (this.processMessages && !(this.crHelper.isShutdown())) {
          try {
            doOneMessage();
          } catch (CancelException e) {
            // allow finally block to handle termination
          } finally {
            this.unsetRequestSpecificTimeout();
            Breadcrumbs.clearBreadcrumb();
          }
        }
      } finally {
        try {
          this.unsetRequestSpecificTimeout();
          handleTermination();
          DistributedSystem.releaseThreadsSockets();
        } catch (CancelException e) {
          // ignore
        }
      }
    }
  }

  /**
   * Register this connection with the given selector for read events. Note that switch the channel
   * to non-blocking so it can be in a selector.
   */
  void registerWithSelector() throws IOException {
    getSelectableChannel().configureBlocking(false);
    getAcceptor().registerSC(this);
  }

  SelectableChannel getSelectableChannel() {
    return this.theSocket.getChannel();
  }

  void registerWithSelector2(Selector s) throws IOException {
    getSelectableChannel().register(s, SelectionKey.OP_READ, this);
  }

  /**
   * Switch this connection to blocking mode so we can use oldIO to read and write messages.
   */
  void makeBlocking() throws IOException {
    SelectableChannel c = this.theSocket.getChannel();
    c.configureBlocking(true);
  }

  private static boolean forceClientCrashEvent = false;

  public static void setForceClientCrashEvent(boolean value) {
    forceClientCrashEvent = value;
  }

  /**
   * @return String representing the DistributedSystemMembership of the Client VM
   */
  public String getMembershipID() {
    return this.proxyId.getDSMembership();
  }

  public int getSocketPort() {
    return theSocket.getPort();
  }

  public String getSocketHost() {
    return theSocket.getInetAddress().getHostAddress();
  }

  protected CommunicationMode getCommunicationMode() {
    return this.communicationMode;
  }

  InetAddress getSocketAddress() {
    return theSocket.getInetAddress();
  }

  public void setRequestSpecificTimeout(int requestSpecificTimeout) {
    this.requestSpecificTimeout = requestSpecificTimeout;
  }

  private void unsetRequestSpecificTimeout() {
    this.requestSpecificTimeout = -1;
  }

  /**
   * Returns the client's read-timeout setting. This is used in the client health monitor to timeout
   * connections that have taken too long & the client will have already given up waiting for a
   * response. Certain messages also may include an override to the normal read-timeout, such as a
   * query or a putAll.
   */
  protected int getClientReadTimeout() {
    if (this.requestSpecificTimeout == -1) {
      return this.handshake.getClientReadTimeout();
    } else {
      return this.requestSpecificTimeout;
    }
  }

  void setProcessingMessage() {
    synchronized (this.processingMessageLock) {
      // go ahead and reset it if it is already set
      this.processingMessageStartTime = System.currentTimeMillis();
    }
  }

  void updateProcessingMessage() {
    synchronized (this.processingMessageLock) {
      // only update it if it was already set by setProcessingMessage
      if (this.processingMessageStartTime != -1) {
        this.processingMessageStartTime = System.currentTimeMillis();
      }
    }
  }

  private void setNotProcessingMessage() {
    synchronized (this.processingMessageLock) {
      this.processingMessageStartTime = -1;
    }
  }

  long getCurrentMessageProcessingTime() {
    long result;
    synchronized (this.processingMessageLock) {
      result = this.processingMessageStartTime;
    }
    if (result != -1) {
      result = System.currentTimeMillis() - result;
    }
    return result;
  }

  boolean hasBeenTimedOutOnClient() {
    int timeout = getClientReadTimeout();
    if (timeout > 0) { // 0 means no timeout
      timeout = timeout + TIMEOUT_BUFFER_FOR_CONNECTION_CLEANUP_MS;
      /*
       * This is a buffer that we add to client readTimeout value before we cleanup the connection.
       * This buffer time helps prevent EOF in the client instead of SocketTimeout
       */
      synchronized (this.processingMessageLock) {
        // If a message is currently being processed and it has been
        // being processed for more than the client read timeout,
        // then return true
        if (getCurrentMessageProcessingTime() > timeout) {
          return true;
        }
      }
    }
    return false;
  }

  public String getSocketString() {
    try {
      return String.valueOf(theSocket.getInetAddress()) + ':' +
          theSocket.getPort() + " timeout: " + theSocket.getSoTimeout();
    } catch (Exception e) {
      return String.format("Error in getSocketString: %s",
          e.getLocalizedMessage());
    }
  }

  private void clearRequestMessage() {
    requestMessage.clear();
  }

  public void incrementLatestBatchIdReplied(int justProcessed) {
    // not synchronized because it only has a single caller
    if (justProcessed - this.latestBatchIdReplied != 1) {
      this.stats.incOutOfOrderBatchIds();
      logger.warn("Batch IDs are out of order. Setting latestBatchId to: {}. It was: {}",
          new Object[] {justProcessed,
              this.latestBatchIdReplied});
    }
    this.latestBatchIdReplied = justProcessed;
  }

  public int getLatestBatchIdReplied() {
    return this.latestBatchIdReplied;
  }

  private final Object ownerLock = new Object();

  private void setOwner() {
    synchronized (this.ownerLock) {
      this.owner = Thread.currentThread();
    }
  }

  private void unsetOwner() {
    synchronized (this.ownerLock) {
      this.owner = null;
      // clear the interrupted bit since our thread is in a thread pool
      Thread.interrupted();
    }
  }

  void initStreams(Socket s, int socketBufferSize, MessageStats messageStats) {
    try {
      if (getAcceptor().isSelector()) {
        // set it on the message to null. This causes Message
        // to fetch it from a thread local. That way we only need
        // one per thread in our selector thread pool instead of
        // one per connection.
        commBuffer = null;
      } else {
        commBuffer = allocateCommBuffer(socketBufferSize, s);
      }
      requestMessage.setComms(this, theSocket, commBuffer, messageStats);
      replyMessage.setComms(this, theSocket, commBuffer, messageStats);
      responseMessage.setComms(this, theSocket, commBuffer, messageStats);
      errorMessage.setComms(this, theSocket, commBuffer, messageStats);

      chunkedResponseMessage.setComms(this, theSocket, commBuffer, messageStats);
      queryResponseMessage.setComms(this, theSocket, commBuffer, messageStats);
      executeFunctionResponseMessage.setComms(this, theSocket, commBuffer, messageStats);
      registerInterestResponseMessage.setComms(this, theSocket, commBuffer, messageStats);
      keySetResponseMessage.setComms(this, theSocket, commBuffer, messageStats);
    } catch (RuntimeException re) {
      throw re;
    } catch (Exception e) {
      logger.fatal(e.getMessage(), e);
    }
  }

  public boolean isOpen() {
    return !isClosed();
  }

  public boolean isClosed() {
    return this.theSocket == null || !this.theSocket.isConnected() || this.theSocket.isClosed();
  }

  public void cleanup(boolean timedOut) {
    if (cleanup() && timedOut) {
      this.stats.incConnectionsTimedOut();
    }
  }

  public boolean cleanup() {
    if (isClosed()) {
      return false;
    }
    if (this.communicationMode.isWAN()
        || this.communicationMode.isCountedAsClientServerConnection()) {
      getAcceptor().decClientServerCnxCount();
    }

    try {
      theSocket.close();
    } catch (Exception ignored) {
    }

    try {
      if (this.postAuthzRequest != null) {
        this.postAuthzRequest.close();
        this.postAuthzRequest = null;
      }
    } catch (Exception ex) {
      if (securityLogWriter.warningEnabled()) {
        securityLogWriter.warning(
            String.format(
                "%s: An exception was thrown while closing client post-process authorization callback. %s",
                new Object[] {this.name, ex}));
      }
    }

    getAcceptor().unregisterSC(this);
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Closed connection", this.name);
    }
    releaseCommBuffer();
    processMessages = false;
    return true;
  }

  private void releaseCommBuffer() {
    ByteBuffer byteBuffer = this.commBuffer;
    if (byteBuffer != null) {
      this.commBuffer = null;
      ServerConnection.releaseCommBuffer(byteBuffer);
    }
  }

  /**
   * Just ensure that this class gets loaded.
   *
   * @see SystemFailure#loadEmergencyClasses()
   */
  public static void loadEmergencyClasses() {
    // nothing needed, just make sure this class gets loaded.
  }

  /**
   * @see SystemFailure#emergencyClose()
   */
  public void emergencyClose() {
    this.terminated = true;
    Socket s = this.theSocket;
    if (s != null) {
      try {
        s.close();
      } catch (IOException e) {
        // ignore
      }
    }
  }

  @Override
  public String toString() {
    return this.name;
  }

  /**
   * returns the name of this connection
   */
  public String getName() {
    return this.name;
  }

  /**
   * @return The ClientProxyMembershipID associated with the ServerConnection
   */
  public ClientProxyMembershipID getProxyID() {
    return this.proxyId;
  }

  /**
   * @return The ClientProxyMembershipID associated with the ServerConnection
   */
  public CachedRegionHelper getCachedRegionHelper() {
    return this.crHelper;
  }

  /**
   * @return The CacheServerStats associated with the ServerConnection
   */
  public CacheServerStats getCacheServerStats() {
    return this.stats;
  }

  /**
   * @return The ReplyMessage associated with the ServerConnection
   */
  public Message getReplyMessage() {
    return this.replyMessage;
  }

  /**
   * @return The ChunkedResponseMessage associated with the ServerConnection
   */
  public ChunkedMessage getChunkedResponseMessage() {
    return this.chunkedResponseMessage;
  }

  /**
   * @return The ErrorResponseMessage associated with the ServerConnection
   */
  public Message getErrorResponseMessage() {
    return this.errorMessage;
  }

  /**
   * @return The ResponseMessage associated with the ServerConnection
   */
  public Message getResponseMessage() {
    return this.responseMessage;
  }

  /**
   * @return The Request Message associated with the ServerConnection
   */
  Message getRequestMessage() {
    return this.requestMessage;
  }

  /**
   * @return The QueryResponseMessage associated with the ServerConnection
   */
  ChunkedMessage getQueryResponseMessage() {
    return this.queryResponseMessage;
  }

  public ChunkedMessage getFunctionResponseMessage() {
    return this.executeFunctionResponseMessage;
  }

  ChunkedMessage getKeySetResponseMessage() {
    return this.keySetResponseMessage;
  }

  public ChunkedMessage getRegisterInterestResponseMessage() {
    return this.registerInterestResponseMessage;
  }

  /*
   * The four boolean fields and the String & Object field below are the transient data We have made
   * it fields just because we know that they will be operated by a single thread only & hence in
   * effect behave as local variables.
   */
  private boolean requiresResponse;
  private boolean requiresChunkedResponse;
  private boolean potentialModification;
  private boolean responded;
  private Object modKey = null;
  private String modRegion = null;

  void resetTransientData() {
    this.potentialModification = false;
    this.requiresResponse = false;
    this.responded = false;
    this.requiresChunkedResponse = false;
    this.modKey = null;
    this.modRegion = null;

    queryResponseMessage.setNumberOfParts(2);
    chunkedResponseMessage.setNumberOfParts(1);
    executeFunctionResponseMessage.setNumberOfParts(1);
    registerInterestResponseMessage.setNumberOfParts(1);
    keySetResponseMessage.setNumberOfParts(1);
  }

  String getModRegion() {
    return this.modRegion;
  }

  Object getModKey() {
    return this.modKey;
  }

  boolean getPotentialModification() {
    return this.potentialModification;
  }

  public void setModificationInfo(boolean potentialModification, String modRegion, Object modKey) {
    this.potentialModification = potentialModification;
    this.modRegion = modRegion;
    this.modKey = modKey;
  }

  public void setAsTrue(int boolID) {
    switch (boolID) {
      case Command.RESPONDED:
        this.responded = true;
        break;
      case Command.REQUIRES_RESPONSE:
        this.requiresResponse = true;
        break;
      case Command.REQUIRES_CHUNKED_RESPONSE:
        this.requiresChunkedResponse = true;
        break;
      default:
        throw new IllegalArgumentException(
            String.format("The ID passed is %s which does not correspond with any transient data",
                boolID));
    }
  }

  public boolean getTransientFlag(int boolID) {
    boolean retVal;
    switch (boolID) {
      case Command.RESPONDED:
        retVal = this.responded;
        break;
      case Command.REQUIRES_RESPONSE:
        retVal = this.requiresResponse;
        break;
      case Command.REQUIRES_CHUNKED_RESPONSE:
        retVal = this.requiresChunkedResponse;
        break;
      default:
        throw new IllegalArgumentException(
            String.format("The ID passed is %s which does not correspond with any transient data",
                boolID));
    }
    return retVal;
  }

  public void setFlagProcessMessagesAsFalse() {
    this.processMessages = false;
  }

  public InternalLogWriter getLogWriter() {
    return this.logWriter; // TODO:LOG:CONVERT: remove getLogWriter after callers are converted
  }

  // this is for old client before(<6.5), from 6.5 userAuthId comes in user request
  private long userAuthId;

  // this is for old client before(<6.5), from 6.5 userAuthId comes in user request
  void setUserAuthId(long uniqueId) {
    this.userAuthId = uniqueId;
  }

  private byte[] encryptId(long id) throws Exception {
    // deserialize this using handshake keys
    try (HeapDataOutputStream heapDataOutputStream = new HeapDataOutputStream(Version.CURRENT)) {

      heapDataOutputStream.writeLong(id);

      return this.handshake.getEncryptor().encryptBytes(heapDataOutputStream.toByteArray());
    }
  }

  public long getUniqueId() {
    long uniqueId;

    if (this.handshake.getVersion().isPre65() || communicationMode.isWAN()) {
      uniqueId = this.userAuthId;
    } else if (this.requestMessage.isSecureMode()) {
      uniqueId = messageIdExtractor.getUniqueIdFromMessage(this.requestMessage,
          this.handshake.getEncryptor(), this.connectionId);
    } else {
      throw new AuthenticationRequiredException(
          "No security credentials are provided");
    }
    return uniqueId;
  }

  public AuthorizeRequest getAuthzRequest() throws AuthenticationRequiredException, IOException {
    // look client version and return authzrequest
    // for backward client it will be store in member variable userAuthId
    // for other look "requestMessage" here and get unique-id from this to get the authzrequest

    if (!AcceptorImpl.isAuthenticationRequired()) {
      return null;
    }

    if (securityService.isIntegratedSecurity()) {
      return null;
    }

    long uniqueId = getUniqueId();

    UserAuthAttributes uaa = null;
    try {
      uaa = this.clientUserAuths.getUserAuthAttributes(uniqueId);
    } catch (NullPointerException npe) {
      if (this.isTerminated()) {
        // Bug #52023.
        throw new IOException("Server connection is terminated.");
      } else {
        logger.debug("Unexpected exception {}", npe);
      }
    }
    if (uaa == null) {
      throw new AuthenticationRequiredException("User authorization attributes not found.");
    }
    AuthorizeRequest authReq = uaa.getAuthzRequest();
    if (logger.isDebugEnabled()) {
      logger.debug("getAuthzRequest() authrequest: {}",
          ((authReq == null) ? "NULL (only authentication is required)" : "not null"));
    }
    return authReq;
  }

  public AuthorizeRequestPP getPostAuthzRequest()
      throws AuthenticationRequiredException, IOException {
    if (!AcceptorImpl.isAuthenticationRequired()) {
      return null;
    }

    if (securityService.isIntegratedSecurity()) {
      return null;
    }

    // look client version and return authzrequest
    // for backward client it will be store in member variable userAuthId
    // for other look "requestMessage" here and get unique-id from this to get the authzrequest
    long uniqueId = getUniqueId();

    UserAuthAttributes uaa = null;
    try {
      uaa = this.clientUserAuths.getUserAuthAttributes(uniqueId);
    } catch (NullPointerException npe) {
      if (this.isTerminated()) {
        // Bug #52023.
        throw new IOException("Server connection is terminated.");
      } else {
        logger.debug("Unexpected exception {}", npe);
      }
    }
    if (uaa == null) {
      throw new AuthenticationRequiredException("User authorization attributes not found.");
    }

    return uaa.getPostAuthzRequest();
  }

  /**
   * returns the member ID byte array to be used for creating EventID objects
   */
  public byte[] getEventMemberIDByteArray() {
    return this.memberIdByteArray;
  }

  public void setClientDisconnectCleanly() {
    this.clientDisconnectedCleanly = true;
  }

  public void setClientDisconnectedException(Throwable e) {
    this.clientDisconnectedException = e;
  }

  void setMessageIdExtractor(MessageIdExtractor messageIdExtractor) {
    this.messageIdExtractor = messageIdExtractor;
  }

  private void setAuthAttributes() throws Exception {
    logger.debug("setAttributes()");
    Object principal = getHandshake().verifyCredentials();

    long uniqueId;
    if (principal instanceof Subject) {
      uniqueId = getClientUserAuths(getProxyID()).putSubject((Subject) principal);
    } else {
      // this sets principal in map as well....
      uniqueId = getUniqueId((Principal) principal);
      setPrincipal((Principal) principal);
    }
    setUserAuthId(uniqueId);
  }

  /**
   * For legacy auth?
   */
  private long getUniqueId(Principal principal) throws Exception {
    InternalLogWriter securityLogWriter = getSecurityLogWriter();
    DistributedSystem system = getDistributedSystem();
    Properties systemProperties = system.getProperties();
    String authzFactoryName = systemProperties.getProperty(SECURITY_CLIENT_ACCESSOR);
    String postAuthzFactoryName = systemProperties.getProperty(SECURITY_CLIENT_ACCESSOR_PP);
    AuthorizeRequest authzRequest = null;
    AuthorizeRequestPP postAuthzRequest = null;

    if (authzFactoryName != null && authzFactoryName.length() > 0) {
      if (securityLogWriter.fineEnabled())
        securityLogWriter.fine(
            getName() + ": Setting pre-process authorization callback to: " + authzFactoryName);
      if (principal == null) {
        if (securityLogWriter.warningEnabled()) {
          securityLogWriter.warning(
              String.format(
                  "%s: Authorization enabled but authentication callback (%s)  returned with null credentials for proxyID: %s",
                  new Object[] {getName(), SECURITY_CLIENT_AUTHENTICATOR, getProxyID()}));
        }
      }
      authzRequest = new AuthorizeRequest(authzFactoryName, getProxyID(), principal, getCache());
    }
    if (postAuthzFactoryName != null && postAuthzFactoryName.length() > 0) {
      if (securityLogWriter.fineEnabled())
        securityLogWriter.fine(getName() + ": Setting post-process authorization callback to: "
            + postAuthzFactoryName);
      if (principal == null) {
        if (securityLogWriter.warningEnabled()) {
          securityLogWriter.warning(
              String.format(
                  "%s: Post-process authorization enabled, but no authentication callback (%s) is configured",
                  new Object[] {getName(), SECURITY_CLIENT_AUTHENTICATOR}));
        }
      }
      postAuthzRequest =
          new AuthorizeRequestPP(postAuthzFactoryName, getProxyID(), principal, getCache());
    }
    return setUserAuthorizeAndPostAuthorizeRequest(authzRequest, postAuthzRequest);
  }
}
