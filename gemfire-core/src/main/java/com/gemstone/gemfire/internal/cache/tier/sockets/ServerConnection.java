/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache.tier.sockets;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.security.Principal;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.client.internal.AbstractOp;
import com.gemstone.gemfire.cache.client.internal.Connection;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.HeapDataOutputStream;
import com.gemstone.gemfire.internal.SocketUtils;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.tier.Acceptor;
import com.gemstone.gemfire.internal.cache.tier.CachedRegionHelper;
import com.gemstone.gemfire.internal.cache.tier.ClientHandShake;
import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.InternalClientMembership;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.command.Default;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.InternalLogWriter;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.internal.security.AuthorizeRequest;
import com.gemstone.gemfire.internal.security.AuthorizeRequestPP;
import com.gemstone.gemfire.internal.util.Breadcrumbs;
import com.gemstone.gemfire.security.AuthenticationFailedException;
import com.gemstone.gemfire.security.AuthenticationRequiredException;
import com.gemstone.gemfire.security.GemFireSecurityException;

/**
 * Provides an implementation for the server socket end of the hierarchical
 * cache connection. Each server connection runs in its own thread to maximize
 * concurrency and improve response times to edge requests
 *
 * @author Sudhir Menon
 * @since 2.0.2
 */
public class ServerConnection implements Runnable {

  private static final Logger logger = LogService.getLogger();
  
  /**
   * This is a buffer that we add to client readTimeout value before we cleanup the connection. 
   * This buffer time helps prevent EOF in the client instead of SocketTimeout
   */
  private static final int TIMEOUT_BUFFER_FOR_CONNECTION_CLEANUP_MS = 5000;
  
  //TODO:Asif: Do we need a  Concurrent Map as we know it will be only get operations?
 // private static final CM commands = CFactory.createCM();
  private Map commands;


  final protected CacheServerStats stats;

  // private static boolean useDataStream =
  // System.getProperty("hct.useDataStream", "false").equals("true");

  // The key is the size of each ByteBuffer. The value is a queue of byte buffers all of that size.
  private static final ConcurrentHashMap<Integer, LinkedBlockingQueue<ByteBuffer>> commBufferMap = new ConcurrentHashMap<>(4, 0.75f, 1);

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
    for (LinkedBlockingQueue<ByteBuffer> q: commBufferMap.values()) {
      q.clear();
    }
  }

  private Socket theSocket;
  //private InputStream in = null;
  //private OutputStream out = null;
  private ByteBuffer commBuffer;
  private final CachedRegionHelper crHelper;
  private String name = null;

  // IMPORTANT: if new messages are added change setHandshake to initialize them
  // to the correct Version for serializing to the client
  private Message requestMsg = new Message(2, Version.CURRENT);
  private Message replyMsg = new Message(1, Version.CURRENT);
  private Message responseMsg = new Message(1, Version.CURRENT);
  private Message errorMsg = new Message(1, Version.CURRENT);

  // IMPORTANT: if new messages are added change setHandshake to initialize them
  // to the correct Version for serializing to the client
  private ChunkedMessage queryResponseMsg = new ChunkedMessage(2, Version.CURRENT);
  private ChunkedMessage chunkedResponseMsg = new ChunkedMessage(1, Version.CURRENT);
  private ChunkedMessage executeFunctionResponseMsg = new ChunkedMessage(1, Version.CURRENT);
  private ChunkedMessage registerInterestResponseMsg = new ChunkedMessage(1, Version.CURRENT);
  private ChunkedMessage keySetResponseMsg = new ChunkedMessage(1, Version.CURRENT);

  private final InternalLogWriter logWriter;
  private final InternalLogWriter securityLogWriter;
  final private AcceptorImpl acceptor;
  private Thread owner;

  /**
   * Handshake reference uniquely identifying a client
   */
  private ClientHandShake handshake;
  private int handShakeTimeout;
  private final Object handShakeMonitor = new Object();
  
  /*
   *  This timeout is request specific which come with message itself
   *  Otherwise, timeout which comes during handshake is used.  
   */
  private volatile int requestSpecificTimeout = -1;

  /** Tracks the id of the most recent batch to which a reply has been sent */
  private int latestBatchIdReplied = -1;

  /*
   * Uniquely identifying the client's Distributed System
   *

   private String membershipId;


    * Uniquely identifying the client's ConnectionProxy object
    *
    *
   private String proxyID ;
   */
  ClientProxyMembershipID proxyId;

  byte [] memberIdByteArray;

  /**
   * Authorize client requests using this object. This is set when each
   * operation on this connection is authorized in pre-operation phase.
   */
  private AuthorizeRequest authzRequest;

  /**
   * Authorize client requests using this object. This is set when each
   * operation on this connection is authorized in post-operation phase.
   */
  private AuthorizeRequestPP postAuthzRequest;

  /**
   * The communication mode for this <code>ServerConnection</code>.
   * Valid types include 'client-server', 'gateway-gateway' and
   * 'monitor-server'.
   */
  private final byte communicationMode;
  private final String communicationModeStr;

  private long processingMessageStartTime = -1;
  private Object processingMessageLock = new Object();
  
  private static ConcurrentHashMap<ClientProxyMembershipID, ClientUserAuths> proxyIdVsClientUserAuths = new ConcurrentHashMap<ClientProxyMembershipID, ClientUserAuths>();
 
  
  private ClientUserAuths clientUserAuths;

  //this is constant(server and client) for first user request, after that it is random
  //this also need to send in handshake
  private long connectionId = Connection.DEFAULT_CONNECTION_ID;
  
  private Random randomConnectionIdGen = null;
  
  private Part securePart = null;

  private Principal principal;

  /**
  * A debug flag used for testing Backward compatibility
  */
  public static boolean TEST_VERSION_AFTER_HANDSHAKE_FLAG = false;
  
  public static short testVersionAfterHandshake = 4;
  
  /**
   * Creates a new <code>ServerConnection</code> that processes messages
   * received from an edge client over a given <code>Socket</code>.
   */
  public ServerConnection(
    Socket s,
    Cache c,
    CachedRegionHelper helper,
    CacheServerStats stats,
    int hsTimeout,
    int socketBufferSize,
  String communicationModeStr,
  byte communicationMode,
  Acceptor acceptor)
  {
    StringBuffer buffer = new StringBuffer(100);
    if(((AcceptorImpl)acceptor).isGatewayReceiver()) {
      buffer
        .append("GatewayReceiver connection from [");
    }
    else{
      buffer
        .append("Server connection from [");
    }
    buffer
        .append(communicationModeStr)
        .append(" host address=")
        .append(s.getInetAddress().getHostAddress())
        .append("; ")
        .append(communicationModeStr)
        .append(" port=")
        .append(s.getPort())
        .append("]");
    this.name = buffer.toString();

    this.stats = stats;
    this.acceptor = (AcceptorImpl)acceptor;
    this.crHelper = helper;
    this.logWriter = (InternalLogWriter)c.getLoggerI18n();
    this.securityLogWriter = (InternalLogWriter)c.getSecurityLoggerI18n();
    this.communicationModeStr = communicationModeStr;
    this.communicationMode = communicationMode;
    this.principal = null;
    this.authzRequest = null;
    this.postAuthzRequest = null;
    this.randomConnectionIdGen = new Random(this.hashCode());
    
    final boolean isDebugEnabled = logger.isDebugEnabled();
    try {
      // requestMsg.setUseDataStream(useDataStream);
      // replyMsg.setUseDataStream(useDataStream);
      // responseMsg.setUseDataStream(useDataStream);
      // errorMsg.setUseDataStream(useDataStream);

      initStreams(s, socketBufferSize, stats);

      if (isDebugEnabled) {
        logger.debug("{}: Accepted client connection from {}[client host name={}; client host address={}; client port={}]",
            getName(), s.getInetAddress().getCanonicalHostName(), s.getInetAddress().getHostAddress(), s.getPort());
      }
      this.handShakeTimeout = hsTimeout;
    }
    catch (Exception e) {
      if (isDebugEnabled) {
        logger.debug("While creating server connection", e);
      }
    }
  }

  public AcceptorImpl getAcceptor() {
    return this.acceptor;
  }
  
  static private final ThreadLocal<Byte> executeFunctionOnLocalNodeOnly = new ThreadLocal<Byte>() {
    @Override
    protected Byte initialValue() {
      return 0x00;
    }
  };

  static public void executeFunctionOnLocalNodeOnly(Byte value) {
    byte b = value.byteValue();
    executeFunctionOnLocalNodeOnly.set(b);
  }

  static public Byte isExecuteFunctionOnLocalNodeOnly() {
    return executeFunctionOnLocalNodeOnly.get();
  }

  private boolean verifyClientConnection() {
    synchronized(this.handShakeMonitor) {
      if (this.handshake == null) {
//        synchronized (getCleanupTable()) {
        boolean readHandShake = ServerHandShakeProcessor.readHandShake(this);
        if (readHandShake) {
          if (this.handshake.isOK()) {
            try {
              return processHandShake();
            }
            catch (CancelException e) {
              if (!crHelper.isShutdown()) {
                logger.warn(LocalizedMessage.create(LocalizedStrings.ServerConnection_0_UNEXPECTED_CANCELLATION, getName()), e);
              }
              cleanup();
              return false;
            }
          }
          else {
            this.crHelper.checkCancelInProgress(null); // bug 37113?
            logger.warn(LocalizedMessage.create(LocalizedStrings.ServerConnection_0_RECEIVED_UNKNOWN_HANDSHAKE_REPLY_CODE_1, new Object[] { this.name, new Byte(this.handshake.getCode()) }));
            refuseHandshake(LocalizedStrings.ServerConnection_RECEIVED_UNKNOWN_HANDSHAKE_REPLY_CODE.toLocalizedString(), ServerHandShakeProcessor.REPLY_INVALID);
            return false;
          }
        } else {
          this.stats.incFailedConnectionAttempts();
          cleanup();
          return false;
        }
//        }
      }
    }
    return true;
  }
  
  protected Map getCommands() {
    return this.commands;
  }
  
  protected Socket getSocket() {
    return this.theSocket;
  }

  protected int getHandShakeTimeout() {
    return this.handShakeTimeout;
  }

  protected DistributedSystem getDistributedSystem() {
    return getCache().getDistributedSystem();
  }

  public Cache getCache() {
    return this.crHelper.getCache();
  }

  public ClientHandShake getHandshake() {
    return this.handshake;
  }

  public void setHandshake(ClientHandShake handshake) {
    this.handshake = handshake;
    Version v = handshake.getVersion();
    
    this.replyMsg.setVersion(v);
    this.requestMsg.setVersion(v);
    this.responseMsg.setVersion(v);
    this.errorMsg.setVersion(v);
    
    this.queryResponseMsg.setVersion(v);
    this.chunkedResponseMsg.setVersion(v);
    this.executeFunctionResponseMsg.setVersion(v);
    this.registerInterestResponseMsg.setVersion(v);
    this.keySetResponseMsg.setVersion(v);
  }

  public Version getClientVersion() {
    return this.handshake.getVersion();
  }
  
  protected void setProxyId(ClientProxyMembershipID proxyId) {
    this.proxyId = proxyId;
    this.memberIdByteArray = EventID.getMembershipId(proxyId);
//    LogWriterI18n log = InternalDistributedSystem.getLoggerI18n();
//    byte[] oldIdArray = proxyId.getMembershipByteArray();
//    log.warning(LocalizedStrings.DEBUG, "Size comparison for " + proxyId.getDistributedMember()
//        + "   old=" + oldIdArray.length + "   new=" + memberIdByteArray.length
//        + "   diff=" + (oldIdArray.length - memberIdByteArray.length));
    this.name = "Server connection from [" + proxyId + "; port="
        + this.theSocket.getPort() + "]";
  }

 protected void setPrincipal(Principal principal) {
    this.principal = principal;
  }

  protected void setAuthorizeRequest(AuthorizeRequest authzRequest) {
    this.authzRequest = authzRequest;
  }

  protected void setPostAuthorizeRequest(AuthorizeRequestPP postAuthzRequest) {
    this.postAuthzRequest = postAuthzRequest;
  }
  
  //hitesh:this is for backward compability
  public long setUserAuthorizeAndPostAuthorizeRequest(AuthorizeRequest authzRequest, AuthorizeRequestPP postAuthzRequest)
  {
    UserAuthAttributes userAuthAttr = new UserAuthAttributes(authzRequest, postAuthzRequest);
    if (this.clientUserAuths == null )
      this.initializeClientUserAuths();
    return this.clientUserAuths.putUserAuth(userAuthAttr);
  }
  //this is backward compability only, if any race condition happens.
  //where server is unregistering the client and client is creating new connection.
  private void resetUserAuthorizeAndPostAuthorizeRequest()
  {
    if (AcceptorImpl.isAuthenticationRequired()
        && (this.handshake.getVersion().compareTo(Version.GFE_65) < 0
            || this.getCommunicationMode() == Acceptor.GATEWAY_TO_GATEWAY))
    {
      ClientUserAuths cua = proxyIdVsClientUserAuths.get(this.proxyId);
      if (cua != this.clientUserAuths)
      {
        UserAuthAttributes uaa = this.clientUserAuths.getUserAuthAttributes(this.userAuthId);
        initializeClientUserAuths();
        this.userAuthId = this.clientUserAuths.putUserAuth(uaa);
      }
    }
  }

  public InternalLogWriter getSecurityLogWriter() {
    return this.securityLogWriter;
  }

  private boolean incedCleanupTableRef = false;
  private boolean incedCleanupProxyIdTableRef = false;

  private final Object chmLock = new Object();
  private boolean chmRegistered = false;
  
  private Map getCleanupTable() {
    return acceptor.getClientHealthMonitor().getCleanupTable();
  }
  
  private Map getCleanupProxyIdTable() {
    return acceptor.getClientHealthMonitor().getCleanupProxyIdTable();
  }
  
  private ClientHealthMonitor getClientHealthMonitor() {
    return acceptor.getClientHealthMonitor();
  }
  
  private boolean processHandShake() {
    boolean result = false;
    boolean clientJoined = false;
    boolean registerClient = false;

    final boolean isDebugEnabled = logger.isDebugEnabled();
    try {
    synchronized (getCleanupTable()) {
      Counter numRefs = (Counter)getCleanupTable().get(this.handshake);
      byte epType = (byte)0 ;
      int qSize = 0 ;
      
      if (this.proxyId.isDurable()) {
        if (isDebugEnabled) {
          logger.debug("looking if the Proxy existed for this durable client or not :{}", this.proxyId);
        }
        CacheClientProxy proxy = getAcceptor().getCacheClientNotifier().getClientProxy(this.proxyId);
        if (proxy != null && proxy.waitRemoval()) {
            proxy = getAcceptor().getCacheClientNotifier().getClientProxy(
                this.proxyId);
        }
        if (proxy != null) {
          if (isDebugEnabled) {
            logger.debug("Proxy existed for this durable client :{} and proxy : {}", this.proxyId, proxy);
          }
          if(proxy.isPrimary()){
            epType = (byte)2 ; 
            qSize = proxy.getQueueSize();
          }else {
            epType = (byte)1;
           qSize = proxy.getQueueSize();
          }
        }    
        // Bug Fix for 37986
        if(numRefs == null){
          // Check whether this is a durable client first. A durable client with
          // the same id is not allowed. In this case, reject the client.
          if (proxy != null && !proxy.isPaused()) {
            // The handshake refusal message must be smaller than 127 bytes.
            String handshakeRefusalMessage = LocalizedStrings.ServerConnection_DUPLICATE_DURABLE_CLIENTID_0
              .toLocalizedString(proxyId.getDurableId());
            logger.warn(LocalizedMessage.create(LocalizedStrings.TWO_ARG_COLON,
                new Object[] {this.name, handshakeRefusalMessage}));
            refuseHandshake(handshakeRefusalMessage, HandShake.REPLY_EXCEPTION_DUPLICATE_DURABLE_CLIENT);
            return result;
          }      
        }
      }
      if (numRefs != null) {
        if (acceptHandShake(epType, qSize)) {
          numRefs.incr();
          this.incedCleanupTableRef = true;
          result = true;
        }
        return result;
      }
      else {
        if (acceptHandShake(epType, qSize)) {
          clientJoined = true;
          numRefs = new Counter();
          getCleanupTable().put(this.handshake, numRefs);
          numRefs.incr();
          this.incedCleanupTableRef = true;
          this.stats.incCurrentClients();
          result = true;
        }
        return result;
      }
    } // sync
    } // try
    finally {
      if (isTerminated() || result == false) {
        return false;
      }
      synchronized(getCleanupProxyIdTable()) {
        Counter numRefs = (Counter) getCleanupProxyIdTable().get(this.proxyId);
        if (numRefs != null) {
          numRefs.incr();
        } else {
          registerClient = true;
          numRefs = new Counter();
          numRefs.incr();
          getCleanupProxyIdTable().put(this.proxyId, numRefs);
          InternalDistributedMember idm = (InternalDistributedMember)this.proxyId.getDistributedMember();
        }
        this.incedCleanupProxyIdTableRef = true;
      }

      if (isDebugEnabled) {
        logger.debug("{} registering client {}", (registerClient? "" : "not "), proxyId);
      }
      this.crHelper.checkCancelInProgress(null);
      if (clientJoined && isFiringMembershipEvents()) {
        // This is a new client. Notify bridge membership and heartbeat monitor.
        InternalClientMembership.notifyJoined(this.proxyId.getDistributedMember(),
            true);
        }

      ClientHealthMonitor chm = this.acceptor.getClientHealthMonitor();
      synchronized (this.chmLock) {
        this.chmRegistered = true;
      }
      if (registerClient) {
        //hitesh: it will add client 
        chm.registerClient(this.proxyId);
      }
      //hitesh:it will add client connection in set
      chm.addConnection(this.proxyId, this);
      this.acceptor.getConnectionListener().connectionOpened(registerClient, communicationMode);
      //Hitesh: add user creds in map for single user case. 
    } // finally
  }
  
  private boolean isFiringMembershipEvents() {
    return this.acceptor.isRunning() 
      && !((GemFireCacheImpl)this.acceptor.getCachedRegionHelper().getCache()).isClosed()
      && this.acceptor.getCachedRegionHelper().getCache().getCancelCriterion().cancelInProgress() == null;
  }

  protected void refuseHandshake(String msg, byte exception)
  {
    try {
      ServerHandShakeProcessor.refuse(this.theSocket.getOutputStream(), msg, exception);
    }
    catch (IOException ignore) {
    }
    finally {
      this.stats.incFailedConnectionAttempts();
      cleanup();
    }
  }

  private boolean acceptHandShake(byte epType, int qSize)
  {
    try {
      this.handshake.accept(SocketUtils.getOutputStream(theSocket), SocketUtils.getInputStream(this.theSocket)//this.theSocket
          , epType, qSize, this.communicationMode,
          this.principal);
    }
    catch (IOException ioe) {
      if (!crHelper.isShutdown() && !isTerminated()) {
        logger.warn(LocalizedMessage.create(LocalizedStrings.
          ServerConnection_0_HANDSHAKE_ACCEPT_FAILED_ON_SOCKET_1_2,
          new Object[] {this.name, this.theSocket, ioe}));
      }
      cleanup();
      return false;
    }
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Accepted handshake", this.name);
    }

    if (this.communicationMode == Acceptor.CLIENT_TO_SERVER_FOR_QUEUE) {
      this.stats.incCurrentQueueConnections();
    } else {
      this.stats.incCurrentClientConnections();
    }
    return true;
  }

//  public static AuthorizeRequestPP getPostAuthorizeCallback(ClientProxyMembershipID proxyId, String cqName)
//  {
//    ClientUserAuths cua = proxyIdVsClientUserAuths.get(proxyId);
//    UserAuthAttributes uaa =  cua.getUserAuthAttributes(cqName);
//    if (uaa != null)
//      return uaa.getPostAuthzRequest();
//    return null;
//  }
  
  public void setCq(String cqName, boolean isDurable) throws Exception
  {
    final boolean isDebugEnabled = logger.isDebugEnabled();
    if (this.requestMsg.isSecureMode()) {
      if (isDebugEnabled) {
        logger.debug("setCq() security header found registering CQname = {}", cqName);
      }
      try {
        byte[] secureBytes = this.requestMsg.getSecureBytes();

        secureBytes = ((HandShake)this.handshake).decryptBytes(secureBytes);
        AuthIds aIds = new AuthIds(secureBytes);

        long uniqueId = aIds.getUniqueId();

        CacheClientProxy proxy = getAcceptor().getCacheClientNotifier()
            .getClientProxy(this.proxyId);

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
    if (this.requestMsg.isSecureMode()) {
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
  
  static final class Counter {
    int cnt;
    void incr() { ++cnt; }
    int decr() { return --cnt; }
    int getCnt() { return cnt; }
  }

//  public void setUserAuthAttributes(ClientProxyMembershipID proxyId, AuthorizeRequest authzRequest, AuthorizeRequestPP postAuthzRequest) {
//    UserAuthAttributes uaa = new UserAuthAttributes(authzRequest, postAuthzRequest);
//  }

  /**
   * Set to false once handshake has been done
   */
  private boolean doHandshake = true;

  private boolean clientDisconnectedCleanly = false;
  private int failureCount = 0;
  private boolean processMessages = true;

  private void doHandshake() {
    //hitesh:to create new connection handshake
    if (verifyClientConnection()) {
      // Initialize the commands after the handshake so that the version
      // can be used.
      initializeCommands();
      //its initialized in verifyClientConnection call
      if(getCommunicationMode() != Acceptor.GATEWAY_TO_GATEWAY)
      	initializeClientUserAuths();
    }
    if (TEST_VERSION_AFTER_HANDSHAKE_FLAG) {
      Assert.assertTrue((this.handshake.getVersion().ordinal() == testVersionAfterHandshake), "Found different version after handshake");
      TEST_VERSION_AFTER_HANDSHAKE_FLAG = false;
    }  
  }  

  private void doNormalMsg() {
    Message msg = null;
    msg = BaseCommand.readRequest(this);
    try {
      if (msg != null) {
        //this.logger.fine("donormalMsg() msgType " + msg.getMessageType());
        // Since this thread is not interrupted when the cache server is
        // shutdown,
        // test again after a message has been read. This is a bit of a hack. I
        // think this thread should be interrupted, but currently AcceptorImpl
        // doesn't keep track of the threads that it launches.
        if (!this.processMessages || (crHelper.isShutdown())) {
          if (logger.isDebugEnabled()) {
            logger.debug("{} ignoring message of type {} from client {} due to shutdown.", getName(), MessageType.getString(msg.getMessageType()), this.proxyId);
          }
          return;
        }

        if (msg.getMessageType() != MessageType.PING) {
          // check for invalid number of message parts
          if (msg.getNumberOfParts() <= 0) {
            failureCount++;
            if (failureCount > 3) {
              this.processMessages = false;
              return;
            }
            else {
              return;
            }
          }
        }

        if (logger.isTraceEnabled()) {
          logger.trace("{} received {} with txid {}", getName(), MessageType.getString(msg.getMessageType()), msg.getTransactionId());
          if (msg.getTransactionId() < -1) { // TODO:WTF: why is this happening?
            msg.setTransactionId(-1);
          }
        }

        if (msg.getMessageType() != MessageType.PING) {
          // we have a real message (non-ping),
          // so let's call receivedPing to let the CHM know client is busy
          acceptor.getClientHealthMonitor().receivedPing(this.proxyId);
        }
        Command command = getCommand(Integer.valueOf(msg.getMessageType()));
        if (command == null) {
          command = Default.getCommand();
        }
        command.execute(msg, this);
      }
    }
    finally {
      // Keep track of the fact that a message is no longer being
      // processed.
      setNotProcessingMessage();
      clearRequestMsg();
    }

  }

  private final Object terminationLock = new Object();
  private boolean terminated = false;

  public boolean isTerminated() {
    synchronized (this.terminationLock) {
      return this.terminated;
    }
  }
  
  private void cleanClientAuths()
  {
    if (this.clientUserAuths != null )
    {
      this.clientUserAuths.cleanup(false);      
    }
  }

  // package access allowed so AcceptorImpl can call
  void handleTermination() {
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
      boolean unregisterClient=false;
      setNotProcessingMessage();
      synchronized(getCleanupTable()) {
        if (this.incedCleanupTableRef) {
          this.incedCleanupTableRef = false;
        cleanupStats = true;
          Counter numRefs = (Counter) getCleanupTable().get(this.handshake);
          if (numRefs != null) {
            numRefs.decr();
            if (numRefs.getCnt() <= 0) {
              clientDeparted = true;
              getCleanupTable().remove(this.handshake);
              this.stats.decCurrentClients();
            }
          }
          if (this.communicationMode == Acceptor.CLIENT_TO_SERVER_FOR_QUEUE) {
            this.stats.decCurrentQueueConnections();
          } else {
            this.stats.decCurrentClientConnections();
          }
        }
      }

      synchronized(getCleanupProxyIdTable()) {
        if (this.incedCleanupProxyIdTableRef) {
          this.incedCleanupProxyIdTableRef = false;
          Counter numRefs = (Counter) getCleanupProxyIdTable().get(this.proxyId);
          if (numRefs != null) {
            numRefs.decr();
            if (numRefs.getCnt() <= 0) {
              unregisterClient = true;
              getCleanupProxyIdTable().remove(this.proxyId);
              //here we can remove entry multiuser map for client
              proxyIdVsClientUserAuths.remove(this.proxyId);
              InternalDistributedMember idm = (InternalDistributedMember)this.proxyId.getDistributedMember();
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
            InternalClientMembership.notifyLeft(proxyId.getDistributedMember(), true);
          } else {
            InternalClientMembership.notifyCrashed(this.proxyId.getDistributedMember(), true);
          }
          // The client has departed. Remove this last connection and unregister it.
        }
      }
      
      { // moved out of above if to fix bug 36751
        
        boolean needsUnregister = false;
        synchronized (this.chmLock) {        
          if (this.chmRegistered) {
            needsUnregister = true;
            this.chmRegistered = false;
          }
        }
        if(unregisterClient)//last serverconnection call all close on auth objects
          cleanClientAuths();  
        this.clientUserAuths = null;
        if (needsUnregister) {
          this.acceptor.getClientHealthMonitor().removeConnection(this.proxyId, this);
          if (unregisterClient) {
            this.acceptor.getClientHealthMonitor().unregisterClient(this.proxyId, getAcceptor(),
                this.clientDisconnectedCleanly);
          }
        }
      }
    if (cleanupStats) {
      this.acceptor.getConnectionListener().connectionClosed(clientDeparted,
          communicationMode);
    }
  }

  private void doOneMessage() {
    if (this.doHandshake) {
      doHandshake();
      this.doHandshake = false;
    } else {
      this.resetTransientData();
      doNormalMsg();
    }
  }

  private void initializeClientUserAuths()
  {
    this.clientUserAuths = getClientUserAuths(this.proxyId);
  }
  
  static ClientUserAuths getClientUserAuths(ClientProxyMembershipID proxyId)
  {
    ClientUserAuths cua = new ClientUserAuths(proxyId.hashCode());
    ClientUserAuths retCua  = proxyIdVsClientUserAuths.putIfAbsent(proxyId, cua);
    
    if(retCua == null)
      return cua;
    return retCua;
  }
  
  private void initializeCommands() {
    // The commands are cached here, but are just referencing the ones
    // stored in the CommandInitializer
    this.commands = CommandInitializer.getCommands(this);
  }

  private Command getCommand(Integer messageType) {
    
    Command cc = (Command) this.commands.get(messageType);
    return cc;
  }

  public boolean removeUserAuth(Message msg, boolean keepalive)
  {
    try
    {
      byte [] secureBytes = msg.getSecureBytes();
      
      secureBytes =  ((HandShake)this.handshake).decryptBytes(secureBytes);
      
      //need to decrypt it first then get connectionid
      AuthIds aIds = new AuthIds(secureBytes);
      
      long connId = aIds.getConnectionId();
      
      if (connId != this.connectionId) {
        throw new  AuthenticationFailedException("Authentication failed");
      }
      
      return this.clientUserAuths.removeUserId(aIds.getUniqueId(), keepalive);      
    }
    catch(Exception ex)
    {
      throw new  AuthenticationFailedException("Authentication failed");
    }
  }
  public byte[] setCredentials(Message msg)
    throws Exception{
    
    try
    {
      //need to get connection id from secure part of message, before that need to insure encryption of id
      //need to check here, whether it matches with serverConnection id or not
      //need to decrpt bytes if its in DH mode
      //need to get properties of credentials(need to remove extra stuff if something is there from client)
      //need to generate unique-id for client
      //need to send back in response with encrption 
      if (!AcceptorImpl.isAuthenticationRequired() && msg.isSecureMode()) {
        // TODO (ashetkar)
        /*
         * This means that client and server VMs have different security settings.
         * The server does not have any security settings specified while client
         * has.
         * 
         * Here, should we just ignore this and send the dummy security part
         * (connectionId, userId) in the response (in this case, client needs to
         * know that it is not expected to read any security part in any of the
         * server response messages) or just throw an exception indicating bad
         * configuration?
         */
        // This is a CREDENTIALS_NORMAL case.;
        return new byte[0];
      }
      if (!msg.isSecureMode()) {
        //throw exception not authorized 
      }
      
      byte [] secureBytes = msg.getSecureBytes();
      
      secureBytes =  ((HandShake)this.handshake).decryptBytes(secureBytes);
      
      //need to decrypt it first then get connectionid
      AuthIds aIds = new AuthIds(secureBytes);
      
      long connId = aIds.getConnectionId();
      
      if (connId != this.connectionId) {
        throw new  AuthenticationFailedException("Authentication failed");
      }
      
      
      byte[] credBytes = msg.getPart(0).getSerializedForm();
      
      credBytes = ((HandShake)this.handshake).decryptBytes(credBytes);
      
      ByteArrayInputStream bis = new ByteArrayInputStream(credBytes);
      DataInputStream dinp = new DataInputStream(bis);
      Properties credentials = DataSerializer.readProperties(dinp);
      
      
      DistributedSystem system = this.getDistributedSystem();
      String methodName = system.getProperties().getProperty(
          DistributionConfig.SECURITY_CLIENT_AUTHENTICATOR_NAME);
      
      Principal principal = HandShake.verifyCredentials(methodName, credentials,
          system.getSecurityProperties(), (InternalLogWriter)system.getLogWriter(), (InternalLogWriter)system
              .getSecurityLogWriter(), this.proxyId.getDistributedMember());
  
      //this sets principal in map as well....
      long uniqueId = ServerHandShakeProcessor.getUniqueId(this, principal);
      
      //create secure part which will be send in respones    
      
      return encryptId(uniqueId, this);
    } catch (AuthenticationFailedException afe) {
      throw afe;
    } catch (AuthenticationRequiredException are) {
      throw are;
    } catch (Exception e) {
      throw new AuthenticationFailedException("REPLY_REFUSED", e);
    }
  }
  
  private void setSecurityPart() {
    try {
      this.connectionId = randomConnectionIdGen.nextLong();
      this.securePart = new Part();
      byte[] id = encryptId(this.connectionId, this);
      this.securePart.setPartState(id, false);
    } catch (Exception ex) {
      logger.warn(LocalizedMessage.create(LocalizedStrings.ServerConnection_SERVER_FAILED_TO_ENCRYPT_DATA_0, ex));
      throw new GemFireSecurityException("Server failed to encrypt response message.");
    }
  }

  /**
   * MessageType of the messages (typically internal commands) which do not need
   * to participate in security should be added in the following if block. 
   * 
   * @return Part
   * @see AbstractOp#processSecureBytes(Connection, Message)
   * @see AbstractOp#needsUserId()
   * @see AbstractOp#sendMessage(Connection)
   */
  public Part updateAndGetSecurityPart() {
    // need to take care all message types here
//    this.logger.fine("getSecurityPart() msgType = "
//        + this.requestMsg.msgType);
    if (AcceptorImpl.isAuthenticationRequired()
        && this.handshake.getVersion().compareTo(Version.GFE_65) >= 0
        && (this.communicationMode != Acceptor.GATEWAY_TO_GATEWAY)
        && (!this.requestMsg.getAndResetIsMetaRegion())
        && (!(this.requestMsg.msgType == MessageType.CLIENT_READY
            || this.requestMsg.msgType == MessageType.CLOSE_CONNECTION
            || this.requestMsg.msgType == MessageType.GETCQSTATS_MSG_TYPE
            || this.requestMsg.msgType == MessageType.GET_CLIENT_PARTITION_ATTRIBUTES
            || this.requestMsg.msgType == MessageType.GET_CLIENT_PR_METADATA 
            || this.requestMsg.msgType == MessageType.INVALID
            || this.requestMsg.msgType == MessageType.MAKE_PRIMARY
            || this.requestMsg.msgType == MessageType.MONITORCQ_MSG_TYPE
            || this.requestMsg.msgType == MessageType.PERIODIC_ACK
            || this.requestMsg.msgType == MessageType.PING
            || this.requestMsg.msgType == MessageType.REGISTER_DATASERIALIZERS
            || this.requestMsg.msgType == MessageType.REGISTER_INSTANTIATORS
            || this.requestMsg.msgType == MessageType.REQUEST_EVENT_VALUE
            || this.requestMsg.msgType == MessageType.ADD_PDX_TYPE
            || this.requestMsg.msgType == MessageType.GET_PDX_ID_FOR_TYPE
            || this.requestMsg.msgType == MessageType.GET_PDX_TYPE_BY_ID
            || this.requestMsg.msgType == MessageType.SIZE
            || this.requestMsg.msgType == MessageType.TX_FAILOVER
            || this.requestMsg.msgType == MessageType.TX_SYNCHRONIZATION
            || this.requestMsg.msgType == MessageType.GET_FUNCTION_ATTRIBUTES
            || this.requestMsg.msgType == MessageType.ADD_PDX_ENUM
            || this.requestMsg.msgType == MessageType.GET_PDX_ID_FOR_ENUM
            || this.requestMsg.msgType == MessageType.GET_PDX_ENUM_BY_ID
            || this.requestMsg.msgType == MessageType.GET_PDX_TYPES
            || this.requestMsg.msgType == MessageType.GET_PDX_ENUMS
            || this.requestMsg.msgType == MessageType.COMMIT
            || this.requestMsg.msgType == MessageType.ROLLBACK))) {
      setSecurityPart();
      return this.securePart;
    }
    else {
      if (AcceptorImpl.isAuthenticationRequired() && logger.isDebugEnabled()) {
        logger.debug("ServerConnection.updateAndGetSecurityPart() not adding security part for msg type {}",
            MessageType.getString(this.requestMsg.msgType));
      }
    }
    return null;
 }
  
  public void run() {
    setOwner();
    if (getAcceptor().isSelector()) {
      boolean finishedMsg = false;
      try {
        this.stats.decThreadQueueSize();
        if (!isTerminated()) {
          Message.setTLCommBuffer(getAcceptor().takeCommBuffer());
          doOneMessage();
          if (this.processMessages && !(this.crHelper.isShutdown())) {
            registerWithSelector(); // finished msg so reregister
            finishedMsg = true;
          }
        }
      }
      catch (java.nio.channels.ClosedChannelException ignore) {
        // ok shutting down
      }
      catch (CancelException e) {
        // ok shutting down
      }
      catch (IOException ex) {
        logger.warn(LocalizedMessage.create(LocalizedStrings.ServerConnection_0__UNEXPECTED_EXCEPTION, ex));
      }
      finally {
        getAcceptor().releaseCommBuffer(Message.setTLCommBuffer(null));
//        DistributedSystem.releaseThreadsSockets();
        unsetOwner();
        setNotProcessingMessage();
        //unset request specific timeout
        this.unsetRequestSpecificTimeout();
        if (!finishedMsg) {
          try {
            handleTermination();
          }
          catch (CancelException e) {
            // ignore
          }
        }
      }
    }
    else {
      try {
        while (this.processMessages && !(this.crHelper.isShutdown())) {
          try {
            doOneMessage();
          }
          catch (CancelException e) {
            // allow finally block to handle termination
          }
          finally {
            this.unsetRequestSpecificTimeout();
            Breadcrumbs.clearBreadcrumb();
          }
        }
      }
      finally {
        try {
          this.unsetRequestSpecificTimeout();
          handleTermination();
          DistributedSystem.releaseThreadsSockets();
        }
        catch (CancelException e) {
          // ignore
        }
      }
    }
  }

  /**
   * If registered with a selector then this will be the key we are registered with.
   */
  //private SelectionKey sKey = null;
  /**
   * Register this connection with the given selector for read events.
   * Note that switch the channel to non-blocking so it can be in a selector.
   */
  public void registerWithSelector() throws IOException {
    //logger.info("DEBUG: registerWithSelector " + this);
    getSelectableChannel().configureBlocking(false);
    getAcceptor().registerSC(this);
  }
  public SelectableChannel getSelectableChannel() {
    return this.theSocket.getChannel();
  }
  public void registerWithSelector2(Selector s) throws IOException {
    /*this.sKey = */getSelectableChannel().register(s, SelectionKey.OP_READ, this);
  }
  /**
   * Switch this guy to blocking mode so we can use oldIO to read and write msgs.
   */
  public void makeBlocking() throws IOException {
    //logger.info("DEBUG: makeBlocking " + this);

//     if (this.sKey != null) {
//       this.sKey = null;
//     }
    SelectableChannel c = this.theSocket.getChannel();
    c.configureBlocking(true);
  }

  private static boolean forceClientCrashEvent = false;
  public static void setForceClientCrashEvent(boolean value) {
    forceClientCrashEvent = value;
  }

  /**
   *
   * @return String representing the DistributedSystemMembership of the Client VM
   */
  public String getMembershipID() {
    return this.proxyId.getDSMembership();
  }
  public int getSocketPort()
  {
    return theSocket.getPort();
  }

  public String getSocketHost()
  {
    return theSocket.getInetAddress().getHostAddress();
  }

//  private DistributedMember getClientDistributedMember() {
//    return this.proxyId.getDistributedMember();
//  }

  protected byte getCommunicationMode() {
    return this.communicationMode;
  }

  protected String getCommunicationModeString() {
    return this.communicationModeStr;
  }

  protected InetAddress getSocketAddress() {
    return theSocket.getInetAddress();
  }

  public void setRequestSpecificTimeout( int requestSpecificTimeout) {
    this.requestSpecificTimeout = requestSpecificTimeout;
  }
  
  private void unsetRequestSpecificTimeout() {
    this.requestSpecificTimeout = -1;
  }
  
  int getClientReadTimeout() {
    if (this.requestSpecificTimeout == -1 )  
      return this.handshake.getClientReadTimeout();
    else
      return this.requestSpecificTimeout;
  }

  protected boolean isProcessingMessage() {
    if (isTerminated()) {
      return false;
    }
    synchronized (this.processingMessageLock) {
      return basicIsProcessingMessage();
    }
  }

  private boolean basicIsProcessingMessage() {
    return this.processingMessageStartTime != -1;
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

  protected void setNotProcessingMessage() {
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

  protected boolean hasBeenTimedOutOnClient() {
    int timeout = getClientReadTimeout();
    if (timeout > 0) { // 0 means no timeout
      timeout = timeout + TIMEOUT_BUFFER_FOR_CONNECTION_CLEANUP_MS;
      /* This is a buffer that we add to client readTimeout value before we cleanup the connection. 
      * This buffer time helps prevent EOF in the client instead of SocketTimeout */
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

  public String getSocketString()
  {
    try {
      StringBuffer buffer = new StringBuffer(50)
        .append(theSocket.getInetAddress())
        .append(':')
        .append(theSocket.getPort())
        .append(" timeout: " )
        .append(theSocket.getSoTimeout());
      return buffer.toString();
    } catch (Exception e) {
      return LocalizedStrings.ServerConnection_ERROR_IN_GETSOCKETSTRING_0.toLocalizedString(e.getLocalizedMessage());
    }
  }

  

  

  

  
  

  

//  private void writePingReply(Message origMsg) throws IOException {
//    replyMsg.setMessageType(MessageType.REPLY);
//    replyMsg.setNumberOfParts(1);
//    replyMsg.setTransactionId(origMsg.getTransactionId());
//    replyMsg.addBytesPart(OK_BYTES);
//    replyMsg.send(logger, origMsg.getTransactionId());
//    if (logger.finerEnabled()) {
//      logger.finer(getName() + ": rpl tx: " + origMsg.getTransactionId());
//    }
//  }

  

  

  
//  private void writeBatchException(Message origMsg, String message, int index) throws IOException {
//    Exception be = new BatchException(message, index);
//    errorMsg.setMessageType(MessageType.EXCEPTION);
//    errorMsg.setNumberOfParts(2);
//    errorMsg.setTransactionId(origMsg.getTransactionId());
//    errorMsg.addObjPart(be);
//    errorMsg.addStringPart(be.toString());
//    errorMsg.send();
//    if (logger.fineEnabled()) {
//      logger.fine(this.name + ": Wrote batch exception: ", be);
//    }
//  }

 
  void clearRequestMsg() {
    requestMsg.clear();
  }

  

  

//   /**
//    * Examine an entry, and build an InterestEvent for it
//    * @param region region we're fetching from
//    * @param entryKey entry key that we may want the event for
//    * @return the event or null if entry does not exist
//    */
//   private InterestEvent getInterestEvent(Region region, Object entryKey)
//   {
//     Region.Entry entry = null;
//     try {
//       entry = region.getEntry(entryKey);
//     } catch (Exception likelyAPartitionedRegion) { // ignore, change when a partitioned region supports getEntry
//     }
//     if (entry == null) {
//       return null;
//     }
//     if (entry instanceof LocalRegion.NonTXEntry) {
//       final LocalRegion.NonTXEntry regionEntry = (LocalRegion.NonTXEntry)entry;
//       boolean isDeserialized = true;
//       // Get the value in the VM
//       Object value = regionEntry.getRegionEntry().getValueInVM();
//       // If the value in the VM is a CachedDeserializable,
//       // get its value. If it is Token.REMOVED, Token.DESTROYED,
//       // Token.INVALID, or Token.LOCAL_INVALID
//       // set it to null. If it is NOT_AVAILABLE, get the value from
//       // disk. If it is already a byte[], set isObject to false.
//       if (value instanceof CachedDeserializable) {
//         value = ((CachedDeserializable)value).getValue();
//         isDeserialized = !(value instanceof byte[]);
//       }
//       else if (value == Token.REMOVED || value == Token.DESTROYED) {
//         return null;
//       }
//       else if (value == Token.INVALID || value == Token.LOCAL_INVALID) {
//         return null; // fix for bug 35884
//       }
//       else if (value instanceof byte[]) {
//         // key, value, and isDeserialized already set
//       }
//       else if (value == EntryEvent.NOT_AVAILABLE) {
//         // This will occur with a disk region entry where the value
//         // is on disk. Currently the getValue call will deserialize
//         // the value. This means that for disk regions, value classes
//         // must exist on the server. If this code is changed, look at
//         // the run method above for similar code to change.
//         value = regionEntry.getRegionEntry().getValue((LocalRegion)region);
//         if (value instanceof CachedDeserializable) {
//           value = ((CachedDeserializable)value).getValue();
//           isDeserialized = !(value instanceof byte[]);
//         }
//       }
//       return new InterestEvent(entryKey, value, isDeserialized);
//     } else {
//       return null;
//     }
//   }

//   /**
//    * Process an interest request of type {@link InterestType#FILTER_CLASS}
//    * @param region the region
//    * @param className the key
//    * @param policy the policy
//    * @throws IOException
//    */
//   private void handleFilter(LocalRegion region, String className,
//       InterestResultPolicy policy) throws IOException
//   {
//     ArrayList keyList = new ArrayList(this.maximumChunkSize);

//     // Handle the filtering class pattern

//     Class filterClass;
//     InterestFilter filter;
//     try {
//       filterClass = ClassLoadUtil.classFromName((String) className);
//       filter = (InterestFilter) filterClass.newInstance();
//     } catch(ClassNotFoundException cnfe) {
//       throw new RuntimeException("Class " + className + " not found in classpath.", cnfe);
//     } catch(Exception e) {
//       throw new RuntimeException("Class " + className + " could not be instantiated.", e);
//     }

// //    if(!(filter instanceof InterestFilter)) {
// //      throw new RuntimeException("Class " + key + " does not implement InterestFilter.");
// //    }

//     for (Iterator it = region.keys().iterator(); it.hasNext();) {
//       Object entryKey = it.next();
//       InterestEvent ie = getInterestEvent(region, entryKey);
//       if (ie == null) {
//         // key no longer existed so skip it
//         continue;
//       }
//       if(!filter.notifyOnRegister(ie)) {
//         //the filter does not want to know about this entry, so skip it.
//          continue;
//       }
//       appendInterestResponseKey(region, className, entryKey, keyList, "filter list");
//     }
//     // Send the last chunk (the only chunk for individual and list keys)
//     // always send it back, even if the list is of zero size.
//     sendRegisterInterestResponseChunk(region, className, keyList, true);
//   }

 

//  /**
//   * Process an interest request of type {@link InterestType#FILTER_CLASS}
//   * @param region the region
//   * @param className the key
//   * @param policy the policy
//   * @throws IOException
//   */
//  private void handleFilterPR(PartitionedRegion region, String className,
//      InterestResultPolicy policy) throws IOException
//  {
//    // this interest type isn't exposed to the customer yet
//    throw new UnsupportedOperationException("filter classes not yet supporeted");
//  }

  
  
 
  

  

  

  public void incrementLatestBatchIdReplied(int justProcessed) {
    // not synchronized because it only has a single caller
  if(justProcessed-this.latestBatchIdReplied!=1) {
    this.stats.incOutOfOrderBatchIds();
    logger.warn(LocalizedMessage.create(LocalizedStrings.ServerConnection_BATCH_IDS_ARE_OUT_OF_ORDER_SETTING_LATESTBATCHID_TO_0_IT_WAS_1,
      new Object[] {Integer.valueOf(justProcessed), Integer.valueOf(this.latestBatchIdReplied)}));
  }
    this.latestBatchIdReplied = justProcessed;
  }

  public int getLatestBatchIdReplied() {
    return this.latestBatchIdReplied;
  }

  private final Object ownerLock = new Object();

  protected void interruptOwner() {
    synchronized (this.ownerLock) {
      if (this.owner != null) {
        this.owner.interrupt();
      }
    }
  }

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

  
  private void initStreams(Socket s, int socketBufferSize, MessageStats msgStats) {
    try {
        theSocket = s;
        theSocket.setSendBufferSize(socketBufferSize);
        theSocket.setReceiveBufferSize(socketBufferSize);
        if (getAcceptor().isSelector()) {
          // set it on the message to null. This causes Message
          // to fetch it from a thread local. That way we only need
          // one per thread in our selector thread pool instead of
          // one per connection.
          commBuffer = null;
        } else {
          commBuffer = allocateCommBuffer(socketBufferSize, s);
        }
        requestMsg.setComms(this, theSocket, commBuffer, msgStats);
        replyMsg.setComms(this, theSocket, commBuffer, msgStats);
        responseMsg.setComms(this, theSocket, commBuffer, msgStats);
        errorMsg.setComms(this, theSocket, commBuffer, msgStats);

        chunkedResponseMsg.setComms(this, theSocket, commBuffer, msgStats);
        queryResponseMsg.setComms(this, theSocket, commBuffer, msgStats);
        executeFunctionResponseMsg.setComms(this, theSocket, commBuffer, msgStats);
        registerInterestResponseMsg.setComms(this, theSocket, commBuffer, msgStats);
        keySetResponseMsg.setComms(this, theSocket, commBuffer, msgStats);
      }
      catch(RuntimeException re) {
        throw re;
      }
      catch(Exception e) {
        logger.fatal(e.getMessage(), e);
      }
    }

  public boolean isOpen() {
    return !isClosed();
  }
  public boolean isClosed() {
    return this.theSocket == null 
        || !this.theSocket.isConnected()
        || this.theSocket.isClosed();
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
    if (this.communicationMode == Acceptor.CLIENT_TO_SERVER
        || this.communicationMode == Acceptor.GATEWAY_TO_GATEWAY
        || this.communicationMode == Acceptor.MONITOR_TO_SERVER
        /*|| this.communicationMode == Acceptor.CLIENT_TO_SERVER_FOR_QUEUE*/) {
      getAcceptor().decClientServerCnxCount();
    }
//       if (logger.fineEnabled()) {
//         logger.fine (this.name + ": about to close socket");
//       }
//       try {
//         theSocket.shutdownInput();
//       } catch (Exception e) {
//       }
//       try {
//         theSocket.shutdownOutput();
//       } catch (Exception e) {
//       }
    try {
      theSocket.close();
    } catch (Exception e) {
    }
    try {
      if (this.authzRequest != null) {
        this.authzRequest.close();
        this.authzRequest = null;
      }
    }
    catch (Exception ex) {
      if (securityLogWriter.warningEnabled()) {
        securityLogWriter.warning(
          LocalizedStrings.ServerConnection_0_AN_EXCEPTION_WAS_THROWN_WHILE_CLOSING_CLIENT_AUTHORIZATION_CALLBACK_1,
          new Object[] {this.name, ex});
      }
    }
    try {
      if (this.postAuthzRequest != null) {
        this.postAuthzRequest.close();
        this.postAuthzRequest = null;
      }
    }
    catch (Exception ex) {
      if (securityLogWriter.warningEnabled()) {
        securityLogWriter.warning(
          LocalizedStrings.ServerConnection_0_AN_EXCEPTION_WAS_THROWN_WHILE_CLOSING_CLIENT_POSTPROCESS_AUTHORIZATION_CALLBACK_1,
          new Object[] {this.name, ex});
      }
    }
    getAcceptor().unregisterSC(this);
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Closed connection", this.name);
    }
    releaseCommBuffer();
    return true;
  }
  
  private void releaseCommBuffer() {
    ByteBuffer bb = this.commBuffer;
    if (bb != null) {
      this.commBuffer = null;
      ServerConnection.releaseCommBuffer(bb);
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
      }
      catch (IOException e) {
        // ignore
      }
    }
  }
  
  @Override
  public String toString() {
    return this.name;
  }

  /** returns the name of this connection */
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
    return this.replyMsg;
  }
  
  /**    
   * @return The ChunkedResponseMessage associated with the ServerConnection
   */
  public ChunkedMessage getChunkedResponseMessage() {
    return this.chunkedResponseMsg;
  }

  /**    
   * @return The ErrorResponseMessage associated with the ServerConnection
   */
  public Message getErrorResponseMessage() {
    return this.errorMsg;
  }
  
  /**    
   * @return The ResponseMessage associated with the ServerConnection
   */
  public Message getResponseMessage() {
    return this.responseMsg;
  }
  
  /**    
   * @return The Request Message associated with the ServerConnection
   */
  public Message getRequestMessage() {
    return this.requestMsg;
  }
  
  /**    
   * @return The QueryResponseMessage associated with the ServerConnection
   */
  public ChunkedMessage getQueryResponseMessage() {
    return this.queryResponseMsg;
  }
  
  public ChunkedMessage getFunctionResponseMessage() {
    return this.executeFunctionResponseMsg;
  }
  
  public ChunkedMessage getKeySetResponseMessage() {
    return this.keySetResponseMsg;
  }
  
  public ChunkedMessage getRegisterInterestResponseMessage() {
    return this.registerInterestResponseMsg;
  }
 
  /* The four boolean fields and the String & Object field below are the transient data 
   * We have made it fields just because we know that they will
   * be operated by a single thread only & hence in effect behave as
   * local variables.
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
 
    queryResponseMsg.setNumberOfParts(2);
    chunkedResponseMsg.setNumberOfParts(1);
    executeFunctionResponseMsg.setNumberOfParts(1);
    registerInterestResponseMsg.setNumberOfParts(1);
    keySetResponseMsg.setNumberOfParts(1);
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

  public void setModificationInfo(boolean potentialModification,
      String modRegion, Object modKey) {
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
      throw new IllegalArgumentException(LocalizedStrings.ServerConnection_THE_ID_PASSED_IS_0_WHICH_DOES_NOT_CORRESPOND_WITH_ANY_TRANSIENT_DATA
          .toLocalizedString(Integer.valueOf(boolID)));
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
      throw new IllegalArgumentException(LocalizedStrings.ServerConnection_THE_ID_PASSED_IS_0_WHICH_DOES_NOT_CORRESPOND_WITH_ANY_TRANSIENT_DATA
          .toLocalizedString(Integer.valueOf(boolID)));
    }
    return retVal;
  }

  public void setFlagProcessMessagesAsFalse( ) {
    this.processMessages= false;
  }
  
  boolean getFlagProcessMessages( ) {
    return this.processMessages;
  }
  
  public InternalLogWriter getLogWriter() {
    return this.logWriter; // TODO:LOG:CONVERT: remove getLogWriter after callers are converted
  }

  //this is for old client before(<6.5), from 6.5 userAuthId comes in user request
  private long userAuthId;
  
  //this is for old client before(<6.5), from 6.5 userAuthId comes in user request
  public void setUserAuthId(long uniqueId) {
    this.userAuthId = uniqueId;
  }
  
  private static class AuthIds
  {
    private long connectionId;
    private long uniqueId;
    
    public AuthIds(byte[] bytes) throws Exception
    {
      DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));
      if (bytes.length == 8 ){
        //only connectionid        
        connectionId = dis.readLong();
      }
      else if (bytes.length == 16) {
        //first connectionId and then uniqueID
        connectionId = dis.readLong();
        uniqueId = dis.readLong();
      }
      else {
        throw new Exception("Auth ids are not in right form");
      }        
    }
       
    
    public long getConnectionId() {
      return connectionId;
    }
    
    public long getUniqueId() {
      return this.uniqueId;
    }
  }
  
  private byte[] encryptId(long id, ServerConnection servConn) throws Exception {
    // deserialize this using handshake keys
    HeapDataOutputStream hdos = null;
    try {
      hdos = new HeapDataOutputStream(Version.CURRENT);

      hdos.writeLong(id);

      return ((HandShake)this.handshake).encryptBytes(hdos.toByteArray());
    } finally {
      hdos.close();
    }
  }
  
  public AuthorizeRequest getAuthzRequest() 
      throws AuthenticationRequiredException {
    //look client version and return authzrequest
    //for backward client it will be store in member variable userAuthId 
    //for other look "requestMsg" here and get unique-id from this to get the authzrequest
    
    if (AcceptorImpl.isAuthenticationRequired()) {
      long uniqueId = 0;
      
      if (this.handshake.getVersion().compareTo(Version.GFE_65) < 0
          || this.communicationMode == Acceptor.GATEWAY_TO_GATEWAY) {
        uniqueId = this.userAuthId;
      } else {
        try {
          //this.logger.fine("getAuthzRequest() isSecureMode = " + this.requestMsg.isSecureMode());
          if (this.requestMsg.isSecureMode()) {
            //get uniqueID from message
            byte [] secureBytes = this.requestMsg.getSecureBytes();

            secureBytes =  ((HandShake)this.handshake).decryptBytes(secureBytes);
            AuthIds aIds = new AuthIds(secureBytes);

            if ( /*this.connectionId != Connection.DEFAULT_CONNECTION_ID &&*/ this.connectionId != aIds.getConnectionId()) {
              throw new AuthenticationRequiredException(
                  LocalizedStrings.HandShake_NO_SECURITY_PROPERTIES_ARE_PROVIDED
                    .toLocalizedString());
            } else {
              uniqueId = aIds.getUniqueId();
            }
            
          } else {
            throw new AuthenticationRequiredException(
                LocalizedStrings.HandShake_NO_SECURITY_PROPERTIES_ARE_PROVIDED
                  .toLocalizedString());
          }
        } catch (AuthenticationRequiredException are) {
          throw are;
        }
        catch(Exception ex ) {
          throw new AuthenticationRequiredException(
              LocalizedStrings.HandShake_NO_SECURITY_PROPERTIES_ARE_PROVIDED
                .toLocalizedString());
        }
      }
      
      UserAuthAttributes uaa = this.clientUserAuths.getUserAuthAttributes(uniqueId);
      if (uaa == null) {
        throw new AuthenticationRequiredException(
            "User authorization attributes not found.");
      }
      AuthorizeRequest authReq = uaa.getAuthzRequest();
      if (logger.isDebugEnabled()) {
        logger.debug("getAuthzRequest() authrequest: {}", ((authReq == null) ? "NULL (only authentication is required)" : "not null"));
      }
      
      return authReq;
    }
    else {
      return null;
    }
  }

  public AuthorizeRequestPP getPostAuthzRequest() 
  throws AuthenticationRequiredException{
  //look client version and return authzrequest
  //for backward client it will be store in member variable userAuthId 
  //for other look "requestMsg" here and get unique-id from this to get the authzrequest
    if (AcceptorImpl.isAuthenticationRequired()) {
      long uniqueId = 0;
      
      if (this.handshake.getVersion().compareTo(Version.GFE_65) < 0
          || this.communicationMode == Acceptor.GATEWAY_TO_GATEWAY) {
        uniqueId = this.userAuthId;
      } else {
        try {
          //this.logger.fine("getPostAuthzRequest() isSecureMode = " + this.requestMsg.isSecureMode());
          if (this.requestMsg.isSecureMode()) {
            byte [] secureBytes = this.requestMsg.getSecureBytes();
            
            secureBytes =  ((HandShake)this.handshake).decryptBytes(secureBytes);
            
            AuthIds aIds = new AuthIds(secureBytes);
            if ( /*this.connectionId != Connection.DEFAULT_CONNECTION_ID && */this.connectionId != aIds.getConnectionId()) {
              throw new AuthenticationRequiredException(
                  LocalizedStrings.HandShake_NO_SECURITY_PROPERTIES_ARE_PROVIDED
                    .toLocalizedString());
            } else {
              uniqueId = aIds.getUniqueId();
            }
            
          } 
          else{
            throw new AuthenticationRequiredException(
                LocalizedStrings.HandShake_NO_SECURITY_PROPERTIES_ARE_PROVIDED
                  .toLocalizedString());
          }
        } catch (AuthenticationRequiredException are) {
          throw are;
        }
        catch(Exception ex) {
          throw new AuthenticationRequiredException(
              LocalizedStrings.HandShake_NO_SECURITY_PROPERTIES_ARE_PROVIDED
                .toLocalizedString());
        }
      }
      
      UserAuthAttributes uaa = this.clientUserAuths.getUserAuthAttributes(uniqueId);
      if (uaa == null) {
        throw new AuthenticationRequiredException(
            "User authorization attributes not found.");
      }
      
      AuthorizeRequestPP postAuthReq = uaa.getPostAuthzRequest();
      
      return postAuthReq;
    }
    else
      return null;
    
    //return this.postAuthzRequest;
  }

  /** returns the member ID byte array to be used for creating EventID objects */
  public byte[] getEventMemberIDByteArray() {
    return this.memberIdByteArray;
  }
  
  public void setClientDisconnectCleanly() {
    this.clientDisconnectedCleanly = true;
  }
  
  public boolean isSqlFabricSystem() {
    return this.acceptor.isSqlFabricSystem();
  }
}
