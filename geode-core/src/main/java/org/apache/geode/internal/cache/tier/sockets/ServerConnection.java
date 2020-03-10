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

import java.io.EOFException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.security.Principal;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.logging.log4j.Logger;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.ThreadState;

import org.apache.geode.CancelException;
import org.apache.geode.DataSerializer;
import org.apache.geode.SystemFailure;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.annotations.internal.MutableForTesting;
import org.apache.geode.cache.UnsupportedVersionException;
import org.apache.geode.cache.client.internal.Connection;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.HeapDataOutputStream;
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
import org.apache.geode.internal.security.AuthorizeRequest;
import org.apache.geode.internal.security.AuthorizeRequestPP;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.serialization.ByteArrayDataInput;
import org.apache.geode.internal.serialization.Version;
import org.apache.geode.internal.util.Breadcrumbs;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.security.AuthenticationRequiredException;
import org.apache.geode.security.GemFireSecurityException;
import org.apache.geode.security.NotAuthorizedException;

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
  @MutableForTesting
  public static boolean allowInternalMessagesWithoutCredentials =
      !Boolean.getBoolean(DISALLOW_INTERNAL_MESSAGES_WITHOUT_CREDENTIALS_NAME);

  private Map commands;

  protected final SecurityService securityService;

  protected final CacheServerStats stats;

  private final ServerSideHandshakeFactory handshakeFactory = new ServerSideHandshakeFactory();

  // The key is the size of each ByteBuffer. The value is a queue of byte buffers all of that size.
  @MakeNotStatic
  private static final ConcurrentHashMap<Integer, LinkedBlockingQueue<ByteBuffer>> commBufferMap =
      new ConcurrentHashMap<>(4, 0.75f, 1);

  private ServerConnectionCollection serverConnectionCollection;

  private final ProcessingMessageTimer processingMessageTimer = new ProcessingMessageTimer();

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
  private final Message replyMessage = new Message(1, Version.CURRENT);
  private final Message responseMessage = new Message(1, Version.CURRENT);
  private final Message errorMessage = new Message(1, Version.CURRENT);

  // IMPORTANT: if new messages are added change setHandshake to initialize them
  // to the correct Version for serializing to the client
  private final ChunkedMessage queryResponseMessage = new ChunkedMessage(2, Version.CURRENT);
  private final ChunkedMessage chunkedResponseMessage = new ChunkedMessage(1, Version.CURRENT);
  private final ChunkedMessage executeFunctionResponseMessage =
      new ChunkedMessage(1, Version.CURRENT);
  private final ChunkedMessage registerInterestResponseMessage =
      new ChunkedMessage(1, Version.CURRENT);
  private final ChunkedMessage keySetResponseMessage = new ChunkedMessage(1, Version.CURRENT);

  @Deprecated
  private final InternalLogWriter logWriter;
  @Deprecated
  private final InternalLogWriter securityLogWriter;

  final Acceptor acceptor;

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
  private ClientProxyMembershipID proxyId;

  private byte[] memberIdByteArray;

  /**
   * Authorize client requests using this object. This is set when each operation on this connection
   * is authorized in post-operation phase.
   */
  private AuthorizeRequestPP postAuthzRequest;

  /**
   * The communication mode for this {@code ServerConnection}. Valid types include
   * 'client-server', 'gateway-gateway' and 'monitor-server'.
   */
  protected final CommunicationMode communicationMode;

  @MakeNotStatic
  private static final ConcurrentHashMap<ClientProxyMembershipID, ClientUserAuths> proxyIdVsClientUserAuths =
      new ConcurrentHashMap<>();


  private ClientUserAuths clientUserAuths;

  // this is constant(server and client) for first user request, after that it is random
  // this also need to send in handshake
  private long connectionId = Connection.DEFAULT_CONNECTION_ID;

  private final Random randomConnectionIdGen;

  private Part securePart;

  protected Principal principal;

  private MessageIdExtractor messageIdExtractor = new MessageIdExtractor();

  /**
   * A debug flag used for testing Backward compatibility
   */
  @MutableForTesting
  private static boolean TEST_VERSION_AFTER_HANDSHAKE_FLAG;

  /**
   * Creates a new {@code ServerConnection} that processes messages received from an edge
   * client over a given {@code Socket}.
   */
  public ServerConnection(final Socket socket, final InternalCache internalCache,
      final CachedRegionHelper cachedRegionHelper, final CacheServerStats stats,
      final int hsTimeout, final int socketBufferSize, final String communicationModeStr,
      final byte communicationMode, final Acceptor acceptor,
      final SecurityService securityService) {
    StringBuilder buffer = new StringBuilder(100);
    if (acceptor.isGatewayReceiver()) {
      buffer.append("GatewayReceiver connection from [");
    } else {
      buffer.append("Server connection from [");
    }
    buffer.append(communicationModeStr).append(" host address=")
        .append(socket.getInetAddress().getHostAddress()).append("; ").append(communicationModeStr)
        .append(" port=").append(socket.getPort()).append("]");
    name = buffer.toString();

    this.stats = stats;
    this.acceptor = acceptor;
    crHelper = cachedRegionHelper;
    logWriter = (InternalLogWriter) internalCache.getLogger();
    securityLogWriter = (InternalLogWriter) internalCache.getSecurityLoggerI18n();
    this.communicationMode = CommunicationMode.fromModeNumber(communicationMode);
    principal = null;
    postAuthzRequest = null;
    randomConnectionIdGen = new Random(hashCode());

    this.securityService = securityService;

    final boolean isDebugEnabled = logger.isDebugEnabled();
    try {
      theSocket = socket;
      theSocket.setSendBufferSize(socketBufferSize);
      theSocket.setReceiveBufferSize(socketBufferSize);

      if (isDebugEnabled) {
        logger.debug(
            "{}: Accepted client connection from {}[client host={}; client port={}]",
            getName(), communicationModeStr, socket.getInetAddress(),
            socket.getPort());
      }
      handshakeTimeout = hsTimeout;
    } catch (Exception e) {
      if (isDebugEnabled) {
        logger.debug("While creating server connection", e);
      }
    }
  }

  public Acceptor getAcceptor() {
    return acceptor;
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
    synchronized (handshakeMonitor) {
      if (handshake == null) {
        ServerSideHandshake readHandshake;
        try {
          readHandshake = handshakeFactory.readHandshake(getSocket(), getHandShakeTimeout(),
              getCommunicationMode(), getDistributedSystem(), getSecurityService());

        } catch (SocketTimeoutException timeout) {
          logger.warn("{}: Handshake reply code timeout, not received with in {} ms",
              getName(), handshakeTimeout);
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
        if (handshake.isOK()) {
          try {
            return processHandShake();
          } catch (CancelException e) {
            if (!crHelper.isShutdown()) {
              logger.warn(getName() + ": Unexpected cancellation: ", e);
            }
            cleanup();
            return false;
          }
        }
        // is this branch ever taken?
        crHelper.checkCancelInProgress(null);
        logger.warn("Received Unknown handshake reply code.");
        refuseHandshake("Received Unknown handshake reply code.", Handshake.REPLY_INVALID);
        return false;
      }
    }
    return true;
  }

  private void failConnectionAttempt() {
    stats.incFailedConnectionAttempts();
    cleanup();
  }

  private void handleHandshakeException(Exception ex) {
    refuseHandshake(ex.getMessage(), Handshake.REPLY_REFUSED);
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
    return commands;
  }

  protected Socket getSocket() {
    return theSocket;
  }

  private int getHandShakeTimeout() {
    return handshakeTimeout;
  }

  protected DistributedSystem getDistributedSystem() {
    return getCache().getDistributedSystem();
  }

  public InternalCache getCache() {
    return crHelper.getCache();
  }

  public ServerSideHandshake getHandshake() {
    return handshake;
  }

  public void setHandshake(ServerSideHandshake handshake) {
    this.handshake = handshake;
    Version v = handshake.getVersion();

    replyMessage.setVersion(v);
    requestMessage.setVersion(v);
    responseMessage.setVersion(v);
    errorMessage.setVersion(v);

    queryResponseMessage.setVersion(v);
    chunkedResponseMessage.setVersion(v);
    executeFunctionResponseMessage.setVersion(v);
    registerInterestResponseMessage.setVersion(v);
    keySetResponseMessage.setVersion(v);
  }

  void setRequestMessage(Message requestMessage) {
    this.requestMessage = requestMessage;
  }

  public Version getClientVersion() {
    return handshake.getVersion();
  }

  protected void setProxyId(ClientProxyMembershipID proxyId) {
    this.proxyId = proxyId;
    memberIdByteArray = EventID.getMembershipId(proxyId);
    name = "Server connection from [" + proxyId + "; port=" + theSocket.getPort() + "]";
  }

  protected void setPrincipal(Principal principal) {
    this.principal = principal;
  }

  private long setUserAuthorizeAndPostAuthorizeRequest(AuthorizeRequest authzRequest,
      AuthorizeRequestPP postAuthzRequest) throws IOException {
    UserAuthAttributes userAuthAttr = new UserAuthAttributes(authzRequest, postAuthzRequest);
    if (clientUserAuths == null) {
      initializeClientUserAuths();
    }
    try {
      return clientUserAuths.putUserAuth(userAuthAttr);
    } catch (NullPointerException exception) {
      if (isTerminated()) {
        throw new IOException("Server connection is terminated.");
      }
      throw exception;
    }
  }

  @Deprecated
  public InternalLogWriter getSecurityLogWriter() {
    return securityLogWriter;
  }

  private SecurityService getSecurityService() {
    return securityService;
  }

  private boolean incedCleanupTableRef;
  private boolean incedCleanupProxyIdTableRef;

  private final Object chmLock = new Object();
  private boolean chmRegistered;

  private Map<ServerSideHandshake, MutableInt> getCleanupTable() {
    return acceptor.getClientHealthMonitor().getCleanupTable();
  }

  private Map<ClientProxyMembershipID, MutableInt> getCleanupProxyIdTable() {
    return acceptor.getClientHealthMonitor().getCleanupProxyIdTable();
  }

  boolean processHandShake() {
    boolean result = false;
    boolean clientJoined = false;

    final boolean isDebugEnabled = logger.isDebugEnabled();
    try {
      synchronized (getCleanupTable()) {
        MutableInt numRefs = getCleanupTable().get(handshake);
        byte endpointType = (byte) 0;
        int queueSize = 0;

        if (proxyId.isDurable()) {
          if (isDebugEnabled) {
            logger.debug("looking if the Proxy existed for this durable client or not :{}",
                proxyId);
          }
          CacheClientProxy proxy =
              getAcceptor().getCacheClientNotifier().getClientProxy(proxyId);
          if (proxy != null && proxy.waitRemoval()) {
            proxy = getAcceptor().getCacheClientNotifier().getClientProxy(proxyId);
          }
          if (proxy != null) {
            if (isDebugEnabled) {
              logger.debug("Proxy existed for this durable client :{} and proxy : {}", proxyId,
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
          if (numRefs == null) {
            // Check whether this is a durable client first. A durable client with
            // the same id is not allowed. In this case, reject the client.
            if (proxy != null && !proxy.isPaused()) {
              // The handshake refusal message must be smaller than 127 bytes.
              String handshakeRefusalMessage =
                  String.format("Duplicate durable clientId (%s)", proxyId.getDurableId());
              logger.warn("{} : {}", name, handshakeRefusalMessage);
              refuseHandshake(handshakeRefusalMessage,
                  Handshake.REPLY_EXCEPTION_DUPLICATE_DURABLE_CLIENT);
              return result;
            }
          }
        }
        if (numRefs != null) {
          if (acceptHandShake(endpointType, queueSize)) {
            numRefs.increment();
            incedCleanupTableRef = true;
            result = true;
          }
          return result;
        }
        if (acceptHandShake(endpointType, queueSize)) {
          clientJoined = true;
          getCleanupTable().put(handshake, new MutableInt(1));
          incedCleanupTableRef = true;
          stats.incCurrentClients();
          result = true;
        }
        return result;
      }
    } finally {
      if (isTerminated() || !result) {
        return false;
      }
      boolean registerClient = false;
      synchronized (getCleanupProxyIdTable()) {
        MutableInt numRefs = getCleanupProxyIdTable().get(proxyId);
        if (numRefs != null) {
          numRefs.increment();
        } else {
          registerClient = true;
          getCleanupProxyIdTable().put(proxyId, new MutableInt(1));
        }
        incedCleanupProxyIdTableRef = true;
      }

      if (isDebugEnabled) {
        logger.debug("{}registering client {}", registerClient ? "" : "not ", proxyId);
      }
      crHelper.checkCancelInProgress(null);
      if (clientJoined && isFiringMembershipEvents()) {
        InternalClientMembership.notifyClientJoined(proxyId.getDistributedMember());
      }

      ClientHealthMonitor chm = acceptor.getClientHealthMonitor();
      synchronized (chmLock) {
        chmRegistered = true;
      }
      if (registerClient) {
        chm.registerClient(proxyId);
      }
      serverConnectionCollection = chm.addConnection(proxyId, this);
      acceptor.getConnectionListener().connectionOpened(registerClient, communicationMode);
    }
  }

  private boolean isFiringMembershipEvents() {
    return acceptor.isRunning()
        && !acceptor.getCachedRegionHelper().getCache().isClosed()
        && !acceptor.getCachedRegionHelper().getCache().getCancelCriterion().isCancelInProgress();
  }

  private void refuseHandshake(String message, byte exception) {
    try {
      acceptor.refuseHandshake(theSocket.getOutputStream(), message, exception);
    } catch (IOException ignore) {
    } finally {
      stats.incFailedConnectionAttempts();
      cleanup();
    }
  }

  private boolean acceptHandShake(byte endpointType, int queueSize) {
    return doHandShake(endpointType, queueSize) && handshakeAccepted();
  }

  protected abstract boolean doHandShake(byte epType, int qSize);

  private boolean handshakeAccepted() {
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Accepted handshake", name);
    }

    if (communicationMode == CommunicationMode.ClientToServerForQueue) {
      stats.incCurrentQueueConnections();
    } else {
      stats.incCurrentClientConnections();
    }
    return true;
  }

  public void setCq(String cqName, boolean isDurable) throws Exception {
    final boolean isDebugEnabled = logger.isDebugEnabled();
    if (requestMessage.isSecureMode()) {
      if (isDebugEnabled) {
        logger.debug("setCq() security header found registering CQname = {}", cqName);
      }
      try {
        byte[] secureBytes = requestMessage.getSecureBytes();

        secureBytes = handshake.getEncryptor().decryptBytes(secureBytes);
        AuthIds aIds = new AuthIds(secureBytes);

        long uniqueId = aIds.getUniqueId();

        CacheClientProxy proxy =
            getAcceptor().getCacheClientNotifier().getClientProxy(proxyId);

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
    if (requestMessage.isSecureMode()) {
      if (isDebugEnabled) {
        logger.debug("removeCq() security header found registering CQname = {}", cqName);
      }
      try {
        clientUserAuths.removeUserAuthAttributesForCq(cqName, isDurable);
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

  private boolean clientDisconnectedCleanly;
  private Throwable clientDisconnectedException;
  private int failureCount;
  private volatile boolean processMessages = true;

  public boolean getProcessMessages() {
    return processMessages;
  }

  @VisibleForTesting
  void setProcessMessages(boolean processMessages) {
    this.processMessages = processMessages;
  }

  protected void doHandshake() {
    if (verifyClientConnection()) {
      initializeCommands();
      if (!getCommunicationMode().isWAN()) {
        initializeClientUserAuths();
      }
    }
    if (TEST_VERSION_AFTER_HANDSHAKE_FLAG) {
      short testVersionAfterHandshake = 4;
      Assert.assertTrue(handshake.getVersion().ordinal() == testVersionAfterHandshake,
          "Found different version after handshake");
      TEST_VERSION_AFTER_HANDSHAKE_FLAG = false;
    }
  }

  void doNormalMessage() {
    if (serverConnectionCollection == null) {
      logger.warn("Continued processing ServerConnection after handshake failed");
      processMessages = false;
      return;
    }
    Message message = BaseCommand.readRequest(this);
    if (!serverConnectionCollection.incrementConnectionsProcessing()) {
      // Client is being disconnected, don't try to process message.
      processMessages = false;
      return;
    }

    ThreadState threadState = null;
    try {
      if (message != null) {
        // Since this thread is not interrupted when the cache server is shutdown, test again after
        // a message has been read. This is a bit of a hack. I think this thread should be
        // interrupted, but currently AcceptorImpl doesn't keep track of the threads that it
        // launches.
        if (!processMessages || crHelper.isShutdown()) {
          if (logger.isDebugEnabled()) {
            logger.debug("{} ignoring message of type {} from client {} due to shutdown.",
                getName(), MessageType.getString(message.getMessageType()), proxyId);
          }
          return;
        }

        if (message.getMessageType() != MessageType.PING) {
          // check for invalid number of message parts
          if (message.getNumberOfParts() <= 0) {
            failureCount++;
            if (failureCount > 3) {
              processMessages = false;
              return;
            }
            return;
          }
        }

        if (logger.isTraceEnabled()) {
          logger.trace("{} received {} with txid {}", getName(),
              MessageType.getString(message.getMessageType()), message.getTransactionId());
          if (message.getTransactionId() < -1) {
            message.setTransactionId(-1);
          }
        }

        if (message.getMessageType() != MessageType.PING) {
          // we have a real message (non-ping),
          // so let's call receivedPing to let the CHM know client is busy
          acceptor.getClientHealthMonitor().receivedPing(proxyId);
        }
        Command command = getCommand(message.getMessageType());
        if (command == null) {
          command = Default.getCommand();
        }

        // if a subject exists for this uniqueId, binds the subject to this thread so that we can do
        // authorization later
        if (securityService.isIntegratedSecurity()
            && !isInternalMessage(requestMessage, allowInternalMessagesWithoutCredentials)
            && !communicationMode.isWAN()) {
          long uniqueId = getUniqueId();
          String messageType = MessageType.getString(requestMessage.getMessageType());
          Subject subject = clientUserAuths.getSubject(uniqueId);
          if (subject != null) {
            threadState = securityService.bindSubject(subject);
            logger.debug("Bound {} with uniqueId {} for message {} with {}", subject.getPrincipal(),
                uniqueId, messageType, getName());
          } else if (uniqueId == 0) {
            logger.debug("No unique ID yet. {}, {}", messageType, getName());
          } else {
            logger.warn(
                "Failed to bind the subject of uniqueId {} for message {} with {} : Possible re-authentication required",
                uniqueId, messageType, this.getName());
            throw new AuthenticationRequiredException("Failed to find the authenticated user.");
          }
        }

        command.execute(message, this, securityService);
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
  private boolean terminated;

  public boolean isTerminated() {
    synchronized (terminationLock) {
      return terminated;
    }
  }

  private void cleanClientAuths() {
    if (clientUserAuths != null) {
      clientUserAuths.cleanup(false);
    }
  }

  // package access allowed so AcceptorImpl can call
  void handleTermination() {
    if (crHelper.isShutdown()) {
      setClientDisconnectCleanly();
    }
    handleTermination(false);
  }

  void handleTermination(boolean timedOut) {
    synchronized (terminationLock) {
      if (terminated) {
        return;
      }
      terminated = true;
    }
    setNotProcessingMessage();
    boolean clientDeparted = false;
    boolean cleanupStats = false;
    synchronized (getCleanupTable()) {
      if (incedCleanupTableRef) {
        incedCleanupTableRef = false;
        cleanupStats = true;
        MutableInt numRefs = getCleanupTable().get(handshake);
        if (numRefs != null) {
          numRefs.decrement();
          if (numRefs.intValue() <= 0) {
            clientDeparted = true;
            getCleanupTable().remove(handshake);
            stats.decCurrentClients();
          }
        }
        if (communicationMode == CommunicationMode.ClientToServerForQueue) {
          stats.decCurrentQueueConnections();
        } else {
          stats.decCurrentClientConnections();
        }
      }
    }

    boolean unregisterClient = false;
    synchronized (getCleanupProxyIdTable()) {
      if (incedCleanupProxyIdTableRef) {
        incedCleanupProxyIdTableRef = false;
        MutableInt numRefs = getCleanupProxyIdTable().get(proxyId);
        if (numRefs != null) {
          numRefs.decrement();
          if (numRefs.intValue() <= 0) {
            unregisterClient = true;
            getCleanupProxyIdTable().remove(proxyId);
            // here we can remove entry multiuser map for client
            proxyIdVsClientUserAuths.remove(proxyId);
          }
        }
      }
    }
    cleanup(timedOut);
    if (getAcceptor().isRunning()) {
      // If the client has departed notify bridge membership and unregister it from
      // the heartbeat monitor; other wise just remove the connection.
      if (clientDeparted && isFiringMembershipEvents()) {
        if (clientDisconnectedCleanly && !forceClientCrashEvent) {
          InternalClientMembership.notifyClientLeft(proxyId.getDistributedMember());
        } else {
          InternalClientMembership.notifyClientCrashed(proxyId.getDistributedMember());
        }
        // The client has departed. Remove this last connection and unregister it.
      }
    }

    boolean needsUnregister = false;
    synchronized (chmLock) {
      if (chmRegistered) {
        needsUnregister = true;
        chmRegistered = false;
      }
    }
    if (unregisterClient) {
      // last serverconnection call all close on auth objects
      cleanClientAuths();
    }
    clientUserAuths = null;
    if (needsUnregister) {
      acceptor.getClientHealthMonitor().removeConnection(proxyId, this);
      if (unregisterClient) {
        acceptor.getClientHealthMonitor().unregisterClient(proxyId, getAcceptor(),
            clientDisconnectedCleanly, clientDisconnectedException);
      }
    }

    if (cleanupStats) {
      acceptor.getConnectionListener().connectionClosed(clientDeparted, communicationMode);
    }
  }

  protected abstract void doOneMessage();

  private void initializeClientUserAuths() {
    clientUserAuths = getClientUserAuths(proxyId);
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
    commands = CommandInitializer.getCommands(this);
  }

  private Command getCommand(Integer messageType) {
    return (Command) commands.get(messageType);
  }

  public void removeUserAuth(Message message, boolean keepAlive) {
    try {
      byte[] secureBytes = message.getSecureBytes();

      secureBytes = handshake.getEncryptor().decryptBytes(secureBytes);

      // need to decrypt it first then get connectionid
      AuthIds aIds = new AuthIds(secureBytes);

      long connId = aIds.getConnectionId();

      if (connId != connectionId) {
        throw new AuthenticationFailedException("Authentication failed");
      }

      try {
        // first try integrated security
        boolean removed = clientUserAuths.removeSubject(aIds.getUniqueId());

        // if not successful, try the old way
        if (!removed) {
          clientUserAuths.removeUserId(aIds.getUniqueId(), keepAlive);
        }
      } catch (NullPointerException exception) {
        logger.debug("Exception", exception);
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

      secureBytes = handshake.getEncryptor().decryptBytes(secureBytes);

      // need to decrypt it first then get connectionid
      AuthIds aIds = new AuthIds(secureBytes);

      long connId = aIds.getConnectionId();

      if (connId != connectionId) {
        throw new AuthenticationFailedException("Authentication failed");
      }

      byte[] credBytes = message.getPart(0).getSerializedForm();

      credBytes = handshake.getEncryptor().decryptBytes(credBytes);

      ByteArrayDataInput dinp = new ByteArrayDataInput(credBytes);
      Properties credentials = DataSerializer.readProperties(dinp);

      // When here, security is enforced on server, if login returns a subject, then it's the newly
      // integrated security, otherwise, do it the old way.
      long uniqueId;

      DistributedSystem system = getDistributedSystem();
      String methodName = system.getProperties().getProperty(SECURITY_CLIENT_AUTHENTICATOR);

      Object principal = Handshake.verifyCredentials(methodName, credentials,
          system.getSecurityProperties(), (InternalLogWriter) system.getLogWriter(),
          (InternalLogWriter) system.getSecurityLogWriter(), proxyId.getDistributedMember(),
          securityService);
      if (principal instanceof Subject) {
        Subject subject = (Subject) principal;
        uniqueId = clientUserAuths.putSubject(subject);
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

  @VisibleForTesting
  protected ClientUserAuths getClientUserAuths() {
    return clientUserAuths;
  }

  private void setSecurityPart() {
    try {
      connectionId = randomConnectionIdGen.nextLong();
      securePart = new Part();
      byte[] id = encryptId(connectionId);
      securePart.setPartState(id, false);
    } catch (Exception ex) {
      logger.warn("Server failed to encrypt data " + ex);
      throw new GemFireSecurityException("Server failed to encrypt response message.");
    }
  }

  /**
   * MessageType of the messages (typically internal commands) which do not need to participate in
   * security should be added in the following if block.
   */
  public Part updateAndGetSecurityPart() {
    // need to take care all message types here
    if (AcceptorImpl.isAuthenticationRequired()
        && handshake.getVersion().compareTo(Version.GFE_65) >= 0
        && !communicationMode.isWAN() && !requestMessage.getAndResetIsMetaRegion()
        && !isInternalMessage(requestMessage, allowInternalMessagesWithoutCredentials)) {
      setSecurityPart();
      return securePart;
    }
    if (AcceptorImpl.isAuthenticationRequired() && logger.isDebugEnabled()) {
      logger.debug(
          "ServerConnection.updateAndGetSecurityPart() not adding security part for message type {}",
          MessageType.getString(requestMessage.messageType));
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

    // we allow older clients to not send credentials for a handful of messages if and only if a
    // system property is set. This allows a rolling upgrade to be performed.
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

  @Override
  public void run() {
    if (getAcceptor().isSelector()) {
      boolean finishedMessage = false;
      try {
        stats.decThreadQueueSize();
        if (!isTerminated()) {
          getAcceptor().setTLCommBuffer();
          doOneMessage();
          if (processMessages && !crHelper.isShutdown()) {
            // finished message so reregister
            registerWithSelector();
            finishedMessage = true;
          }
        }
      } catch (ClosedChannelException | CancelException ignore) {
        // ok shutting down
      } catch (IOException ex) {
        logger.warn("Unexpected Exception", ex);
        setClientDisconnectedException(ex);
      } catch (AuthenticationRequiredException ex) {
        logger.warn("Unexpected Exception", ex);
      } finally {
        getAcceptor().releaseTLCommBuffer();
        setNotProcessingMessage();
        // unset request specific timeout
        unsetRequestSpecificTimeout();
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
        while (processMessages && !crHelper.isShutdown()) {
          try {
            doOneMessage();
          } catch (CancelException e) {
            // allow finally block to handle termination
          } finally {
            unsetRequestSpecificTimeout();
            Breadcrumbs.clearBreadcrumb();
          }
        }
      } finally {
        try {
          unsetRequestSpecificTimeout();
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
    getAcceptor().registerServerConnection(this);
  }

  SelectableChannel getSelectableChannel() {
    return theSocket.getChannel();
  }

  void registerWithSelector2(Selector s) throws ClosedChannelException {
    getSelectableChannel().register(s, SelectionKey.OP_READ, this);
  }

  /**
   * Switch this connection to blocking mode so we can use oldIO to read and write messages.
   */
  void makeBlocking() throws IOException {
    SelectableChannel c = theSocket.getChannel();
    c.configureBlocking(true);
  }

  @MutableForTesting
  private static boolean forceClientCrashEvent;

  public static void setForceClientCrashEvent(boolean value) {
    forceClientCrashEvent = value;
  }

  /**
   * @return String representing the DistributedSystemMembership of the Client VM
   */
  public String getMembershipID() {
    return proxyId.getDSMembership();
  }

  public int getSocketPort() {
    return theSocket.getPort();
  }

  public String getSocketHost() {
    return theSocket.getInetAddress().getHostAddress();
  }

  protected CommunicationMode getCommunicationMode() {
    return communicationMode;
  }

  InetAddress getSocketAddress() {
    return theSocket.getInetAddress();
  }

  public void setRequestSpecificTimeout(int requestSpecificTimeout) {
    this.requestSpecificTimeout = requestSpecificTimeout;
  }

  private void unsetRequestSpecificTimeout() {
    requestSpecificTimeout = -1;
  }

  /**
   * Returns the client's read-timeout setting. This is used in the client health monitor to timeout
   * connections that have taken too long & the client will have already given up waiting for a
   * response. Certain messages also may include an override to the normal read-timeout, such as a
   * query or a putAll.
   */
  protected int getClientReadTimeout() {
    if (requestSpecificTimeout == -1) {
      return handshake.getClientReadTimeout();
    }
    return requestSpecificTimeout;
  }

  void setProcessingMessage() {
    processingMessageTimer.setProcessingMessage();
  }

  void updateProcessingMessage() {
    processingMessageTimer.updateProcessingMessage();
  }

  private void setNotProcessingMessage() {
    processingMessageTimer.setNotProcessingMessage();
  }

  long getCurrentMessageProcessingTime() {
    return processingMessageTimer.getCurrentMessageProcessingTime();
  }

  boolean hasBeenTimedOutOnClient() {
    int timeout = getClientReadTimeout();
    // 0 means no timeout
    if (timeout > 0) {
      timeout = timeout + TIMEOUT_BUFFER_FOR_CONNECTION_CLEANUP_MS;
      /*
       * This is a buffer that we add to client readTimeout value before we cleanup the connection.
       * This buffer time helps prevent EOF in the client instead of SocketTimeout
       */
      return getCurrentMessageProcessingTime() > timeout;
    }
    return false;
  }

  public String getSocketString() {
    try {
      return String.valueOf(theSocket.getInetAddress()) + ':' +
          theSocket.getPort() + " timeout: " + theSocket.getSoTimeout();
    } catch (Exception e) {
      return String.format("Error in getSocketString: %s", e.getLocalizedMessage());
    }
  }

  private void clearRequestMessage() {
    requestMessage.clear();
  }

  public void incrementLatestBatchIdReplied(int justProcessed) {
    // not synchronized because it only has a single caller
    if (justProcessed - latestBatchIdReplied != 1) {
      stats.incOutOfOrderBatchIds();
      logger.warn("Batch IDs are out of order. Setting latestBatchId to: {}. It was: {}",
          justProcessed, latestBatchIdReplied);
    }
    latestBatchIdReplied = justProcessed;
  }

  public int getLatestBatchIdReplied() {
    return latestBatchIdReplied;
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
    return theSocket == null || !theSocket.isConnected() || theSocket.isClosed();
  }

  public void cleanup(boolean timedOut) {
    if (cleanup() && timedOut) {
      stats.incConnectionsTimedOut();
    }
  }

  public boolean cleanup() {
    if (isClosed()) {
      return false;
    }
    if (communicationMode.isWAN() || communicationMode.isCountedAsClientServerConnection()) {
      getAcceptor().decClientServerConnectionCount();
    }

    try {
      theSocket.close();
    } catch (Exception ignored) {
    }

    try {
      if (postAuthzRequest != null) {
        postAuthzRequest.close();
        postAuthzRequest = null;
      }
    } catch (Exception ex) {
      if (securityLogWriter.warningEnabled()) {
        securityLogWriter.warning(
            String.format(
                "%s: An exception was thrown while closing client post-process authorization callback. %s",
                name, ex));
      }
    }

    getAcceptor().unregisterServerConnection(this);
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Closed connection", name);
    }
    releaseCommBuffer();
    processMessages = false;
    return true;
  }

  private void releaseCommBuffer() {
    ByteBuffer byteBuffer = commBuffer;
    if (byteBuffer != null) {
      commBuffer = null;
      releaseCommBuffer(byteBuffer);
    }
  }

  /**
   * @see SystemFailure#emergencyClose()
   */
  public void emergencyClose() {
    terminated = true;
    Socket s = theSocket;
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
    return name;
  }

  /**
   * returns the name of this connection
   */
  public String getName() {
    return name;
  }

  /**
   * @return The ClientProxyMembershipID associated with the ServerConnection
   */
  public ClientProxyMembershipID getProxyID() {
    return proxyId;
  }

  /**
   * @return The ClientProxyMembershipID associated with the ServerConnection
   */
  public CachedRegionHelper getCachedRegionHelper() {
    return crHelper;
  }

  /**
   * @return The CacheServerStats associated with the ServerConnection
   */
  public CacheServerStats getCacheServerStats() {
    return stats;
  }

  /**
   * @return The ReplyMessage associated with the ServerConnection
   */
  public Message getReplyMessage() {
    return replyMessage;
  }

  /**
   * @return The ChunkedResponseMessage associated with the ServerConnection
   */
  public ChunkedMessage getChunkedResponseMessage() {
    return chunkedResponseMessage;
  }

  /**
   * @return The ErrorResponseMessage associated with the ServerConnection
   */
  public Message getErrorResponseMessage() {
    return errorMessage;
  }

  /**
   * @return The ResponseMessage associated with the ServerConnection
   */
  public Message getResponseMessage() {
    return responseMessage;
  }

  /**
   * @return The Request Message associated with the ServerConnection
   */
  Message getRequestMessage() {
    return requestMessage;
  }

  /**
   * @return The QueryResponseMessage associated with the ServerConnection
   */
  ChunkedMessage getQueryResponseMessage() {
    return queryResponseMessage;
  }

  public ChunkedMessage getFunctionResponseMessage() {
    return executeFunctionResponseMessage;
  }

  ChunkedMessage getKeySetResponseMessage() {
    return keySetResponseMessage;
  }

  public ChunkedMessage getRegisterInterestResponseMessage() {
    return registerInterestResponseMessage;
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
  private Object modKey;
  private String modRegion;

  void resetTransientData() {
    potentialModification = false;
    requiresResponse = false;
    responded = false;
    requiresChunkedResponse = false;
    modKey = null;
    modRegion = null;

    queryResponseMessage.setNumberOfParts(2);
    chunkedResponseMessage.setNumberOfParts(1);
    executeFunctionResponseMessage.setNumberOfParts(1);
    registerInterestResponseMessage.setNumberOfParts(1);
    keySetResponseMessage.setNumberOfParts(1);
  }

  String getModRegion() {
    return modRegion;
  }

  Object getModKey() {
    return modKey;
  }

  boolean getPotentialModification() {
    return potentialModification;
  }

  public void setModificationInfo(boolean potentialModification, String modRegion, Object modKey) {
    this.potentialModification = potentialModification;
    this.modRegion = modRegion;
    this.modKey = modKey;
  }

  public void setAsTrue(int boolID) {
    switch (boolID) {
      case Command.RESPONDED:
        responded = true;
        break;
      case Command.REQUIRES_RESPONSE:
        requiresResponse = true;
        break;
      case Command.REQUIRES_CHUNKED_RESPONSE:
        requiresChunkedResponse = true;
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
        retVal = responded;
        break;
      case Command.REQUIRES_RESPONSE:
        retVal = requiresResponse;
        break;
      case Command.REQUIRES_CHUNKED_RESPONSE:
        retVal = requiresChunkedResponse;
        break;
      default:
        throw new IllegalArgumentException(
            String.format("The ID passed is %s which does not correspond with any transient data",
                boolID));
    }
    return retVal;
  }

  public void setFlagProcessMessagesAsFalse() {
    processMessages = false;
  }

  @Deprecated
  public InternalLogWriter getLogWriter() {
    return logWriter;
  }

  // this is for old client before(<6.5), from 6.5 userAuthId comes in user request
  private long userAuthId;

  // this is for old client before(<6.5), from 6.5 userAuthId comes in user request
  void setUserAuthId(long uniqueId) {
    userAuthId = uniqueId;
  }

  private byte[] encryptId(long id) throws Exception {
    // deserialize this using handshake keys
    try (HeapDataOutputStream heapDataOutputStream = new HeapDataOutputStream(Version.CURRENT)) {

      heapDataOutputStream.writeLong(id);

      return handshake.getEncryptor().encryptBytes(heapDataOutputStream.toByteArray());
    }
  }

  public long getUniqueId() {
    long uniqueId;

    if (handshake.getVersion().isPre65() || communicationMode.isWAN()) {
      uniqueId = userAuthId;
    } else if (requestMessage.isSecureMode()) {
      uniqueId = messageIdExtractor.getUniqueIdFromMessage(requestMessage,
          handshake.getEncryptor(), connectionId);
    } else {
      throw new AuthenticationRequiredException("No security credentials are provided");
    }
    return uniqueId;
  }

  private UserAuthAttributes getUserAuthAttributes() throws IOException {
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
      uaa = clientUserAuths.getUserAuthAttributes(uniqueId);
    } catch (NullPointerException npe) {
      if (isTerminated()) {
        throw new IOException("Server connection is terminated.");
      }
      logger.debug("Unexpected exception {}", npe);
    }
    if (uaa == null) {
      throw new AuthenticationRequiredException("User authorization attributes not found.");
    }
    return uaa;
  }

  public AuthorizeRequest getAuthzRequest() throws AuthenticationRequiredException, IOException {
    UserAuthAttributes uaa = getUserAuthAttributes();
    if (uaa == null) {
      return null;
    }

    AuthorizeRequest authReq = uaa.getAuthzRequest();
    if (logger.isDebugEnabled()) {
      logger.debug("getAuthzRequest() authrequest: {}",
          authReq == null ? "NULL (only authentication is required)" : "not null");
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
      uaa = clientUserAuths.getUserAuthAttributes(uniqueId);
    } catch (NullPointerException npe) {
      if (isTerminated()) {
        throw new IOException("Server connection is terminated.");
      }
      logger.debug("Unexpected exception", npe);
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
    return memberIdByteArray;
  }

  public void setClientDisconnectCleanly() {
    clientDisconnectedCleanly = true;
  }

  public void setClientDisconnectedException(Throwable e) {
    clientDisconnectedException = e;
  }

  void setMessageIdExtractor(MessageIdExtractor messageIdExtractor) {
    this.messageIdExtractor = messageIdExtractor;
  }

  private void setAuthAttributes()
      throws AuthenticationRequiredException, AuthenticationFailedException, ClassNotFoundException,
      NoSuchMethodException, InvocationTargetException, IOException, IllegalAccessException {
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
  private long getUniqueId(Principal principal)
      throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException,
      InvocationTargetException, NotAuthorizedException, IOException {
    InternalLogWriter securityLogWriter = getSecurityLogWriter();
    DistributedSystem system = getDistributedSystem();
    Properties systemProperties = system.getProperties();
    String authzFactoryName = systemProperties.getProperty(SECURITY_CLIENT_ACCESSOR);
    String postAuthzFactoryName = systemProperties.getProperty(SECURITY_CLIENT_ACCESSOR_PP);
    AuthorizeRequest authzRequest = null;

    if (authzFactoryName != null && !authzFactoryName.isEmpty()) {
      if (securityLogWriter.fineEnabled())
        securityLogWriter.fine(
            getName() + ": Setting pre-process authorization callback to: " + authzFactoryName);
      if (principal == null) {
        if (securityLogWriter.warningEnabled()) {
          securityLogWriter.warning(
              String.format(
                  "%s: Authorization enabled but authentication callback (%s)  returned with null credentials for proxyID: %s",
                  getName(), SECURITY_CLIENT_AUTHENTICATOR, getProxyID()));
        }
      }
      authzRequest = new AuthorizeRequest(authzFactoryName, getProxyID(), principal, getCache());
    }
    AuthorizeRequestPP postAuthzRequest = null;
    if (postAuthzFactoryName != null && !postAuthzFactoryName.isEmpty()) {
      if (securityLogWriter.fineEnabled())
        securityLogWriter.fine(getName() + ": Setting post-process authorization callback to: "
            + postAuthzFactoryName);
      if (principal == null) {
        if (securityLogWriter.warningEnabled()) {
          securityLogWriter.warning(
              String.format(
                  "%s: Post-process authorization enabled, but no authentication callback (%s) is configured",
                  getName(), SECURITY_CLIENT_AUTHENTICATOR));
        }
      }
      postAuthzRequest =
          new AuthorizeRequestPP(postAuthzFactoryName, getProxyID(), principal, getCache());
    }
    return setUserAuthorizeAndPostAuthorizeRequest(authzRequest, postAuthzRequest);
  }

  @VisibleForTesting
  static class ProcessingMessageTimer {

    @VisibleForTesting
    static final long NOT_PROCESSING = -1L;

    @VisibleForTesting
    final AtomicLong processingMessageStartTime = new AtomicLong(NOT_PROCESSING);

    /**
     * Set or resets time regardless if already set.
     */
    void setProcessingMessage() {
      processingMessageStartTime.set(System.currentTimeMillis());
    }

    /**
     * Updates time if previously set.
     */
    void updateProcessingMessage() {
      final long current = processingMessageStartTime.get();
      if (NOT_PROCESSING != current) {
        final long now = System.currentTimeMillis();
        if (now > current) {
          // if another thread sets to -1 or updates the time we don't need to update the time.
          processingMessageStartTime.compareAndSet(current, now);
        }
      }
    }

    void setNotProcessingMessage() {
      processingMessageStartTime.set(NOT_PROCESSING);
    }

    long getCurrentMessageProcessingTime() {
      long result = processingMessageStartTime.get();
      if (result != NOT_PROCESSING) {
        result = System.currentTimeMillis() - result;
      }
      return result;
    }
  }
}
