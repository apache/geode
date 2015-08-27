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
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.NoRouteToHostException;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.net.ssl.SSLException;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.InvalidDeltaException;
import com.gemstone.gemfire.StatisticDescriptor;
import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.StatisticsTypeFactory;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.InterestResultPolicy;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.client.ServerRefusedConnectionException;
import com.gemstone.gemfire.cache.client.internal.ClientUpdater;
import com.gemstone.gemfire.cache.client.internal.Endpoint;
import com.gemstone.gemfire.cache.client.internal.EndpointManager;
import com.gemstone.gemfire.cache.client.internal.GetEventValueOp;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.client.internal.QueueManager;
import com.gemstone.gemfire.cache.query.internal.cq.CqService;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionStats;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem.DisconnectListener;
import com.gemstone.gemfire.distributed.internal.ServerLocation;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.MemberAttributes;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.InternalInstantiator;
import com.gemstone.gemfire.internal.SocketCreator;
import com.gemstone.gemfire.internal.SocketUtils;
import com.gemstone.gemfire.internal.StatisticsTypeFactoryImpl;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.cache.ClientServerObserver;
import com.gemstone.gemfire.internal.cache.ClientServerObserverHolder;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.tier.CachedRegionHelper;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.versions.ConcurrentCacheModificationException;
import com.gemstone.gemfire.internal.cache.versions.VersionSource;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.InternalLogWriter;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.LoggingThreadGroup;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.internal.logging.log4j.LogMarker;
import com.gemstone.gemfire.internal.sequencelog.EntryLogger;
import com.gemstone.gemfire.security.AuthenticationFailedException;
import com.gemstone.gemfire.security.AuthenticationRequiredException;
import com.gemstone.gemfire.security.GemFireSecurityException;

/**
 * <code>CacheClientUpdater</code> is a thread that processes update messages
 * from a cache server and
 * {@linkplain com.gemstone.gemfire.cache.Region#localInvalidate(Object) invalidates}
 * the local cache based on the contents of those messages.
 * 
 * @author Barry Oglesby
 * @since 3.5
 */
public class CacheClientUpdater extends Thread implements ClientUpdater,
    DisconnectListener {
  
  private static final Logger logger = LogService.getLogger();
  
  /**
   * true if the constructor successfully created a connection. If false, the
   * run method for this thread immediately exits.
   */
  private final boolean connected;

  /**
   * System of which we are a part
   */
  private final InternalDistributedSystem system;

  /**
   * The socket by which we communicate with the server
   */
  private final Socket socket;
  
  /**
   * The output stream of the socket
   */
  private final OutputStream out;

  /**
   * The input stream of the socket
   */
  private final InputStream in;
  /**
   * Failed updater from the endpoint previously known as the primary
   */
  private volatile ClientUpdater failedUpdater;

  /**
   * The buffer upon which we receive messages
   */
  private final ByteBuffer commBuffer;
  private boolean commBufferReleased;

  private final CCUStats stats;
  
  /**
   * Cache for which we provide service
   */
  private /*final*/ GemFireCacheImpl cache;
  private /*final*/ CachedRegionHelper cacheHelper;
  
  
  /**
   * Principle flag to signal thread's run loop to terminate
   */
  private final AtomicBoolean continueProcessing = new AtomicBoolean(true);

  /**
   * Is the client durable
   * Used for bug 39010 fix 
   */
  private final boolean isDurableClient;

  /**
   * Represents the server we are connected to
   */
  private final InternalDistributedMember serverId;

  /**
   * true if the EndPoint represented by this updater thread is primary
   */
  private final boolean isPrimary;

  /**
   * Added to avoid recording of the event if the concerned operation failed.
   * See #43247
   */
  private boolean isOpCompleted;

  public final static String CLIENT_UPDATER_THREAD_NAME = "Cache Client Updater Thread ";
  /*
   * to enable test flag
   */
  public static boolean isUsedByTest;

  /**
   * Indicates if full value was requested from server as a result of failure in
   * applying delta bytes.
   */
  public static boolean fullValueRequested = false;

//  /**
//   * True if this thread been initialized. Indicates that the run thread is
//   * initialized and ready to process messages
//   * <p>
//   * TODO is this still needed?
//   * <p>
//   * Accesses synchronized via <code>this</code>
//   * 
//   * @see #notifyInitializationComplete()
//   * @see #waitForInitialization()
//   */
//  private boolean initialized = false;


  private final ServerLocation location;

  // TODO - remove these fields
  private QueueManager qManager = null;
  private EndpointManager eManager = null;
  private Endpoint endpoint = null;

  static private final long MAX_CACHE_WAIT = 
    Long.getLong("gemfire.CacheClientUpdater.MAX_WAIT", 120).longValue(); // seconds
  /**
   * Return true if cache appears
   * @return true if cache appears
   */
  private boolean waitForCache() {
    GemFireCacheImpl c;
    long tilt = System.currentTimeMillis() + MAX_CACHE_WAIT * 1000;
    for (;;) {
      if (quitting()) {
        logger.warn(LocalizedMessage.create(LocalizedStrings.CacheClientUpdater_0_ABANDONED_WAIT_DUE_TO_CANCELLATION, this));
        return false;
      }
      if (!this.connected) {
        logger.warn(LocalizedMessage.create(LocalizedStrings.CacheClientUpdater_0_ABANDONED_WAIT_BECAUSE_IT_IS_NO_LONGER_CONNECTED, this));
        return false;
      }
      if (System.currentTimeMillis() > tilt) {
        logger.warn(LocalizedMessage.create(LocalizedStrings.CacheClientUpdater_0_WAIT_TIMED_OUT_MORE_THAN_1_SECONDS, new Object[] { this,MAX_CACHE_WAIT }));
        return false;
      }
      c = GemFireCacheImpl.getInstance();
      if (c != null && !c.isClosed()) {
        break;
      }
      boolean interrupted = Thread.interrupted();
      try {
        Thread.sleep(1000);
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
    this.cache = c;
    this.cacheHelper = new CachedRegionHelper(c);
    return true;
  }
  
  /**
   * Creates a new <code>CacheClientUpdater</code> with a given name that
   * waits for a server to connect on a given port.
   *
   * @param name
   *                descriptive name, used for our ThreadGroup
   * @param location
   *                the endpoint we represent
   * @param primary
   *                true if our endpoint is primary TODO ask the ep for this?
   * @param ids
   *                the system we are distributing messages through
   * 
   * @throws AuthenticationRequiredException
   *                 when client is not configured to send credentials using
   *                 security-* system properties but server expects credentials
   * @throws AuthenticationFailedException
   *                 when authentication of the client fails
   * @throws ServerRefusedConnectionException
   *                 when handshake fails for other reasons like using durable
   *                 client ID that is already in use by another client or some
   *                 server side exception while reading handshake/verifying
   *                 credentials
   */
  public CacheClientUpdater(
      String name, ServerLocation location,
      boolean primary, DistributedSystem ids,
      HandShake handshake, QueueManager qManager, EndpointManager eManager,
      Endpoint endpoint, int handshakeTimeout) throws AuthenticationRequiredException,
      AuthenticationFailedException, ServerRefusedConnectionException {
    super(LoggingThreadGroup.createThreadGroup("Client update thread"), name);
    this.setDaemon(true);
    this.system = (InternalDistributedSystem)ids;
    this.isDurableClient = handshake.getMembership().isDurable();
    this.isPrimary = primary;
    this.location = location;
    this.qManager = qManager;
    //this holds the connection which this threads reads
    this.eManager = eManager;
    this.endpoint = endpoint;
    this.stats = new CCUStats(this.system, this.location);
    // Create the connection...
    final boolean isDebugEnabled = logger.isDebugEnabled();
    if (isDebugEnabled) {
      logger.debug("Creating asynchronous update connection");
    }

    boolean success = false;
    Socket mySock = null;
    InternalDistributedMember sid = null;
    ByteBuffer cb = null;
    OutputStream tmpOut = null;
    InputStream tmpIn = null;
    try {
      /** Size of the server-to-client communication socket buffers */
      int socketBufferSize = Integer.getInteger(
          "BridgeServer.SOCKET_BUFFER_SIZE", 32768).intValue();

      if (!SocketCreator.getDefaultInstance()
          .isHostReachable(InetAddress.getByName(location.getHostName()))) {
        throw new NoRouteToHostException("Server is not reachable: " + location.getHostName());
      }

      mySock = SocketCreator.getDefaultInstance().connectForClient(
          location.getHostName(), location.getPort(), handshakeTimeout, socketBufferSize);
      mySock.setTcpNoDelay(true);
      mySock.setSendBufferSize(socketBufferSize);

      // Verify buffer sizes
      verifySocketBufferSize(socketBufferSize, mySock.getReceiveBufferSize(), "receive");
      verifySocketBufferSize(socketBufferSize, mySock.getSendBufferSize(), "send");

      // set the timeout for the handshake
      mySock.setSoTimeout(handshakeTimeout);
      tmpOut = SocketUtils.getOutputStream(mySock);
      tmpIn = SocketUtils.getInputStream(mySock);

      if (isDebugEnabled) {
        logger.debug("Initialized server-to-client socket with send buffer size: {} bytes and receive buffer size: {} bytes", mySock.getSendBufferSize(), mySock.getReceiveBufferSize());
      }

      if (isDebugEnabled) {
        logger.debug("Created connection from {}:{} to CacheClientNotifier on port {} for server-to-client communication", mySock.getInetAddress().getHostAddress(), mySock.getLocalPort(), mySock.getPort());
      }

      ServerQueueStatus sqs = handshake.greetNotifier(mySock, this.isPrimary, location);
      if (sqs.isPrimary() || sqs.isNonRedundant()) {
        PoolImpl pool = (PoolImpl)this.qManager.getPool();
        if (!pool.getReadyForEventsCalled()) {
          pool.setPendingEventCount(sqs.getServerQueueSize());
        }
      }

      {
        int bufSize = 1024;
        try {
          bufSize = mySock.getSendBufferSize();
          if (bufSize < 1024) {
            bufSize = 1024;
          }
        } 
        catch (SocketException ignore) {
        }
        cb = ServerConnection.allocateCommBuffer(bufSize, mySock);
      }
      {
        // create a "server" memberId we currently don't know much about the
        // server.
        // Would be nice for it to send us its member id
        // @todo - change the serverId to use the endpoint's getMemberId() which
        // returns a
        // DistributedMember (once gfecq branch is merged to trunk).
        MemberAttributes ma = new MemberAttributes(0, -1, DistributionManager.NORMAL_DM_TYPE, -1, null, null, null);
        sid = new InternalDistributedMember(mySock.getInetAddress(), mySock.getPort(), false, true, ma);
      }
      success = true;
    }
    catch (ConnectException e) {
      if (!quitting()) {
        logger.warn(LocalizedMessage.create(LocalizedStrings.CacheClientUpdater_0_CONNECTION_WAS_REFUSED, this));
      }
    } 
    catch (SSLException ex) {
      if (!quitting()) {
        getSecurityLogger().warning(
          LocalizedStrings.CacheClientUpdater_0_SSL_NEGOTIATION_FAILED_1,
          new Object[] { this, ex});
        throw new AuthenticationFailedException(
          LocalizedStrings.CacheClientUpdater_SSL_NEGOTIATION_FAILED_WITH_ENDPOINT_0
            .toLocalizedString(location), ex);
      }
    } 
    catch (GemFireSecurityException ex) {
      if (!quitting()) {
        getSecurityLogger().warning(
          LocalizedStrings.CacheClientUpdater_0_SECURITY_EXCEPTION_WHEN_CREATING_SERVERTOCLIENT_COMMUNICATION_SOCKET_1,
          new Object[] {this, ex});
        throw ex;
      }
    }
    catch (IOException e) {
     if (!quitting()) {
       logger.warn(LocalizedMessage.create(LocalizedStrings.CacheClientUpdater_0_CAUGHT_FOLLOWING_EXECPTION_WHILE_ATTEMPTING_TO_CREATE_A_SERVER_TO_CLIENT_COMMUNICATION_SOCKET_AND_WILL_EXIT_1, new Object[] {this, e}), logger.isDebugEnabled() ? e : null);
     }
     eManager.serverCrashed(this.endpoint);
    }
    catch (ClassNotFoundException e) {
      if (!quitting()) {
        logger.warn(LocalizedMessage.create(LocalizedStrings.CacheClientUpdater_CLASS_NOT_FOUND, e.getMessage()));
      }
    }
    finally {
      connected = success;
      if (mySock != null) {
        try {   
          mySock.setSoTimeout(0);
        } catch (SocketException e) {
          // ignore: nothing we can do about this
        }
      }
      if (connected) {
        socket = mySock;
        out = tmpOut;
        in = tmpIn;
        serverId = sid;
        commBuffer = cb;
        // Don't want the timeout after handshake
        if (mySock != null) {
          try {
            mySock.setSoTimeout(0);
          } catch (SocketException ignore) {
          }
        }
      }
      else {
        socket = null;
        serverId = null;
        commBuffer = null;
        out = null;
        in = null;
        
        if (mySock != null) {
          try {
            mySock.close();
          } 
          catch (IOException ioe) {
            logger.warn(LocalizedMessage.create(LocalizedStrings.CacheClientUpdater_CLOSING_SOCKET_IN_0_FAILED, this), ioe);
          }
        }
      }
    }
  }
  
  private void releaseCommBuffer() {
    if (!this.commBufferReleased) {
      if (this.commBuffer != null) {
        synchronized (this.commBuffer) {
          if (!this.commBufferReleased) {
            this.commBufferReleased = true;
            ServerConnection.releaseCommBuffer(this.commBuffer);
          }
        }
      }
    }
  }
  
  public boolean isConnected() {
    return connected;
  }
  
  public boolean isPrimary() {
    return isPrimary;
  }
  
  public InternalLogWriter getSecurityLogger() {
    return this.qManager.getSecurityLogger();
  }

  public void setFailedUpdater(ClientUpdater failedUpdater) {
    this.failedUpdater = failedUpdater;
  }

  /**
   * Performs the work of the client update thread. Creates a
   * <code>ServerSocket</code> and waits for the server to connect to it.
   */
  @Override
  public void run() {
    boolean addedListener = false;
    EntryLogger.setSource(serverId, "RI");
    try {
      this.system.addDisconnectListener(this);
      addedListener = true;
      
      if (!waitForCache()) {
        logger.warn(LocalizedMessage.create(LocalizedStrings.CacheClientUpdater_0_NO_CACHE_EXITING, this));
        return;
      }
      processMessages();
    } 
    catch (CancelException e) {
      return; // just bail
    } 
    finally {
      if (addedListener) {
        this.system.removeDisconnectListener(this);
      }
      this.close();
      EntryLogger.clearSource();
    }
  }

//  /**
//   * Waits for this thread to be initialized
//   * 
//   * @return true if initialized; false if stopped before init
//   */
//  public boolean waitForInitialization() {
//    boolean result = false;
//    // Yogesh : waiting on this thread object is a bad idea
//    // as when thread exits it notifies to the waiting threads.
//      synchronized (this) {
//        for (;;) {
//          if (quitting()) {
//            break;
//          }
//          boolean interrupted = Thread.interrupted();
//          try {
//            this.wait(100); // spurious wakeup ok // timed wait, should fix lost notification problem rahul.
//          }
//          catch (InterruptedException e) {
//            interrupted = true;
//          }
//          finally {
//            if (interrupted) {
//              Thread.currentThread().interrupt();
//            }
//          }
//        } // while
//        // Even if we succeed, there is a risk that we were shut down
//        // Can't check for cache; it isn't set yet :-(
//        this.system.getCancelCriterion().checkCancelInProgress(null);
//        result = this.continueProcessing;
//      } // synchronized
//    return result;
//  }

//  /**
//   * @see #waitForInitialization()
//   */
//  private void notifyInitializationComplete() {
//    synchronized (this) {
//      this.initialized = true;
//      this.notifyAll();
//    }
//  }

  /**
   * Notifies this thread to stop processing
   */
  protected void stopProcessing() {
    continueProcessing.set(false);// = false;
  }

  /**
   * Stops the updater. It will wait for a while for the thread to finish to try
   * to prevent duplicates. Note: this method is not named stop because this is
   * a Thread which has a deprecated stop method.
   */
  public void stopUpdater() {
    boolean isSelfDestroying = Thread.currentThread() == this;

    stopProcessing();
    // need to also close the socket for this interrupt to wakeup
    // the thread. This fixes bug 35691.
    // this.close(); // this should not be done here.

    if (this.isAlive()) {
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Stopping {}" , this.location, this);
      }
      if (!isSelfDestroying) {
        interrupt();
        try {
          if (socket != null) {
            socket.close();
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
          // dont care...
          if (logger.isDebugEnabled()) {
            logger.debug(t.getMessage(), t);
          }
        }
     } // !isSelfDestroying
    } // isAlive
  }

  /**
   * Signals the run thread to stop, closes underlying resources.
   */
  public void close() {
    this.continueProcessing.set(false);// = false; // signals we are done.

    // Close the socket
    // This will also cause the underlying streams to fail.
    try {
      if (socket != null) {
        socket.close();
      }
    } catch (Exception e) {
      // ignore
    }

    try {
      this.stats.close();
    } catch (Exception e) {
      // ignore
    }
    
    // close the helper
    try {
      if (cacheHelper != null) {
        cacheHelper.close();
      }
    } catch (Exception e) {
      // ignore
    }
    releaseCommBuffer();
  }

  /**
   * Creates a cached {@link Message}object whose state is filled in with a
   * message received from the server.
   */
  private Message initializeMessage() {
    Message _message = new Message(2, Version.CURRENT);
    try {
      _message.setComms(socket, in, out, commBuffer, this.stats);
    } 
    catch (IOException e) {
      if (!quitting()) {
        if (logger.isDebugEnabled()){
          logger.debug("{}: Caught following exception while attempting to initialize a server-to-client communication socket and will exit", this, e);
        }
        stopProcessing();
      }
    }
    return _message;
  }

  /* refinement of method inherited from Thread */
  @Override
  public String toString() {
    return this.getName() + " (" + this.location.getHostName() + ":"
        + this.location.getPort() + ")";
  }

  /**
   * Handle a marker message
   * 
   * @param m
   *                message containing the data
   */
  private void handleMarker(Message m) {
    try {
      final boolean isDebugEnabled = logger.isDebugEnabled();
      if (isDebugEnabled) {
    	  logger.debug("Received marker message of length ({} bytes)", m.getPayloadLength());
      }
      this.qManager.getState().processMarker();
      if (isDebugEnabled) {
    	  logger.debug("Processed marker message");
      }
    } catch (Exception e) {
      String message = 
        LocalizedStrings.CacheClientUpdater_THE_FOLLOWING_EXCEPTION_OCCURRED_WHILE_ATTEMPTING_TO_HANDLE_A_MARKER.toLocalizedString();
      handleException(message, e);
    }
  }

  /**
   * Create or update an entry
   * 
   * @param m
   *                message containing the data
   */
  private void handleUpdate(Message m) {
    String regionName = null;
    Object key = null;
    Part valuePart = null;
    Object newValue = null;
    byte[] deltaBytes = null;
    Object fullValue = null;
    boolean isValueObject = false;
    int partCnt = 0;
    final boolean isDebugEnabled = logger.isDebugEnabled();
    try {
      this.isOpCompleted = false;
      // Retrieve the data from the put message parts
      if (isDebugEnabled) {
        logger.debug("Received put message of length ({} bytes)", m.getPayloadLength());
      }

      Part regionNamePart = m.getPart(partCnt++);
      Part keyPart = m.getPart(partCnt++);
      boolean isDeltaSent = ((Boolean)m.getPart(partCnt++).getObject())
          .booleanValue();
      valuePart = m.getPart(partCnt++);
      Part callbackArgumentPart = m.getPart(partCnt++);
      VersionTag versionTag = (VersionTag)m.getPart(partCnt++).getObject();
      if (versionTag != null) {
        versionTag.replaceNullIDs((InternalDistributedMember) this.endpoint.getMemberId());
      }
      Part isInterestListPassedPart = m.getPart(partCnt++);
      Part hasCqsPart = m.getPart(partCnt++);
      
      EventID eventId = (EventID)m.getPart(m.getNumberOfParts() - 1)
          .getObject();

      boolean withInterest = ((Boolean)isInterestListPassedPart.getObject()).booleanValue();
      boolean withCQs = ((Boolean)hasCqsPart.getObject()).booleanValue();

      regionName = regionNamePart.getString();
      key = keyPart.getStringOrObject();
      Object callbackArgument = callbackArgumentPart.getObject();

      // Don't automatically deserialize the value.
      // Pass it onto the region as a byte[]. If it is a serialized
      // object, it will be stored as a CachedDeserializable and
      // deserialized only when requested.

      boolean isCreate = (m.getMessageType() == MessageType.LOCAL_CREATE);
      if (isDebugEnabled) {
        logger.debug("Putting entry for region: {} key: {} create: {}{} callbackArgument: {} withInterest={} withCQs={} eventID={} version={}",
                regionName, key, isCreate, (valuePart.isObject() ? new StringBuilder(" value: ").append(deserialize(valuePart.getSerializedForm())) : ""), 
                callbackArgument, withInterest, withCQs, eventId, versionTag);
      }

      LocalRegion region = (LocalRegion) cacheHelper.getRegion(regionName);

      if (!isDeltaSent) {
        // bug #42162 - must check for a serialized null here
        byte[] serializedForm = valuePart.getSerializedForm();
        if (isCreate && InternalDataSerializer.isSerializedNull(serializedForm)) {
          //newValue = null;  newValue is already null
        } else {
          newValue = valuePart.getSerializedForm();
        }
        if (withCQs) { 
          fullValue = valuePart.getObject();
        }
        isValueObject = valuePart.isObject();
      } else {
        deltaBytes = valuePart.getSerializedForm();
        isValueObject = true;
      }

      if (region == null) {
        if (isDebugEnabled && !quitting()) {
          logger.debug("{}: Region named {} does not exist", this, regionName);
        }
      }
      else if (region.hasServerProxy()
          && ServerResponseMatrix.checkForValidStateAfterNotification(region,
              key, m.getMessageType()) && (withInterest || !withCQs)) {
        EntryEventImpl newEvent = null;
        try {
          // Create an event and put the entry
          newEvent = EntryEventImpl.create(
              region,
              ((m.getMessageType() == MessageType.LOCAL_CREATE) ? Operation.CREATE
                  : Operation.UPDATE), key, null /* newValue */,
              callbackArgument /* callbackArg */, true /* originRemote */,
              eventId.getDistributedMember());
          newEvent.setVersionTag(versionTag);
          newEvent.setFromServer(true);
          region.basicBridgeClientUpdate(eventId.getDistributedMember(), key,
              newValue, deltaBytes, isValueObject, callbackArgument, m
                  .getMessageType() == MessageType.LOCAL_CREATE, qManager
                  .getState().getProcessedMarker()
                  || !this.isDurableClient, newEvent, eventId);
          this.isOpCompleted = true;
          // bug 45520 - ConcurrentCacheModificationException is not thrown and we must check this flag
//          if (newEvent.isConcurrencyConflict()) {
//            return; // this is logged elsewhere at fine level
//          }
          if (withCQs && isDeltaSent) {
            fullValue = newEvent.getNewValue();
          }
        } catch (InvalidDeltaException ide) {
          Part fullValuePart = requestFullValue(eventId,
              "Caught InvalidDeltaException.");
          region.getCachePerfStats().incDeltaFullValuesRequested();
          fullValue = newValue = fullValuePart.getObject();
          isValueObject = Boolean.valueOf(fullValuePart.isObject());
          region.basicBridgeClientUpdate(eventId.getDistributedMember(), key,
              newValue, null, isValueObject, callbackArgument, m
                  .getMessageType() == MessageType.LOCAL_CREATE, qManager
                  .getState().getProcessedMarker()
                  || !this.isDurableClient, newEvent, eventId);
          this.isOpCompleted = true;
        } finally {
          if (newEvent != null) newEvent.release();
        }

        if (isDebugEnabled) {
          logger.debug("Put entry for region: {} key: {} callbackArgument: {}", regionName, key, callbackArgument);
        }
      }
      
      // Update CQs. CQs can exist without client region.
      if (withCQs) {
        Part numCqsPart = m.getPart(partCnt++);
        if (isDebugEnabled) {
          logger.debug("Received message has CQ Event. Number of cqs interested in the event : {}", numCqsPart.getInt()/2);
        }        
        partCnt = processCqs(m, partCnt, numCqsPart.getInt(), m
            .getMessageType(), key, fullValue, deltaBytes, eventId);
        this.isOpCompleted = true;
      }
    } catch (Exception e) {
      String message = LocalizedStrings.CacheClientUpdater_THE_FOLLOWING_EXCEPTION_OCCURRED_WHILE_ATTEMPTING_TO_PUT_ENTRY_REGION_0_KEY_1_VALUE_2.toLocalizedString(new Object[] { regionName, key, deserialize(valuePart.getSerializedForm())});
      handleException(message, e);
    }
  }

  private Part requestFullValue(EventID eventId, String reason) throws Exception {
    if (isUsedByTest) {
      fullValueRequested = true;
    }
    final boolean isDebugEnabled = logger.isDebugEnabled();
    if (isDebugEnabled) {
      logger.debug("{} Requesting full value...", reason);
    }
    Part result = (Part)GetEventValueOp.executeOnPrimary(qManager.getPool(),
        eventId, null);
    
    if (result == null) {
      // Just log a warning. Do not stop CCU thread.
      throw new Exception("Could not retrieve full value for "
          + eventId);
    }
    if (isDebugEnabled) {
      logger.debug("Full value received.");
    }
    return result;
  }

  /**
   * Invalidate an entry
   * 
   * @param m
   *                message describing the entry
   */
  private void handleInvalidate(Message m) {
    String regionName = null;
    Object key = null;
    int partCnt = 0;

    final boolean isDebugEnabled = logger.isDebugEnabled();
    try {
      this.isOpCompleted = false;
      // Retrieve the data from the local-invalidate message parts
      if (isDebugEnabled) {
        logger.debug("Received invalidate message of length ({} bytes)", m.getPayloadLength());
      }

      Part regionNamePart = m.getPart(partCnt++);
      Part keyPart = m.getPart(partCnt++);
      Part callbackArgumentPart = m.getPart(partCnt++);

      VersionTag versionTag = (VersionTag)m.getPart(partCnt++).getObject();
      if (versionTag != null) {
        versionTag.replaceNullIDs((InternalDistributedMember) this.endpoint.getMemberId());
      }

      Part isInterestListPassedPart = m.getPart(partCnt++);
      Part hasCqsPart = m.getPart(partCnt++);
      
      regionName = regionNamePart.getString();
      key = keyPart.getStringOrObject();

      Object callbackArgument = callbackArgumentPart.getObject();
      boolean withInterest = ((Boolean)isInterestListPassedPart.getObject()).booleanValue();
      boolean withCQs = ((Boolean)hasCqsPart.getObject()).booleanValue();
      
      if (isDebugEnabled) {
        logger.debug("Invalidating entry for region: {} key: {} callbackArgument: {} withInterest={} withCQs={} version={}",
            regionName, key, callbackArgument, withInterest, withCQs, versionTag);
      }

      LocalRegion region = (LocalRegion) cacheHelper.getRegion(regionName);
      if (region == null) {
        if (isDebugEnabled && !quitting()) {
          logger.debug("Region named {} does not exist", regionName);
        }
      } else {
        if (region.hasServerProxy()
          && (withInterest || !withCQs)) {
        try {
          Part eid = m.getPart(m.getNumberOfParts() - 1);
          EventID eventId = (EventID)eid.getObject();
          try {
            region.basicBridgeClientInvalidate(eventId.getDistributedMember(), key,
              callbackArgument, qManager.getState().getProcessedMarker() || !this.isDurableClient,
              eventId, versionTag);
          } catch (ConcurrentCacheModificationException e) {
//            return; allow CQs to be processed
          }
          this.isOpCompleted = true;
          //fix for 36615
          qManager.getState().incrementInvalidatedStats();

          if (isDebugEnabled) {
            logger.debug("Invalidated entry for region: {} key: {} callbackArgument: {}", regionName, key, callbackArgument);
          }
        }
        catch (EntryNotFoundException e) {
          /*ignore*/
          if (isDebugEnabled && !quitting()) {
            logger.debug("Already invalidated entry for region: {} key: {} callbackArgument: {}", regionName, key, callbackArgument);
          }
          this.isOpCompleted = true;
        }
      }
      }

    if (withCQs) {
        // The client may have been registered to receive invalidates for 
        // create and updates operations. Get the actual region operation.
        Part regionOpType = m.getPart(partCnt++); 
        Part numCqsPart = m.getPart(partCnt++);
        if (isDebugEnabled) {
          logger.debug("Received message has CQ Event. Number of cqs interested in the event : {}", numCqsPart.getInt() / 2);
        }
        partCnt = processCqs(m, partCnt, numCqsPart.getInt(), regionOpType.getInt(), key, null);
        this.isOpCompleted = true;
      }
    }
    catch (Exception e) {
      final String message = LocalizedStrings.CacheClientUpdater_THE_FOLLOWING_EXCEPTION_OCCURRED_WHILE_ATTEMPTING_TO_INVALIDATE_ENTRY_REGION_0_KEY_1.toLocalizedString(new Object[] { regionName, key });
      handleException(message, e);
    }
  }

  /**
   * locally destroy an entry
   * 
   * @param m
   *                message describing the entry
   */
  private void handleDestroy(Message m) {
    String regionName = null;
    Object key = null;
    int partCnt = 0;

    final boolean isDebugEnabled = logger.isDebugEnabled();
    try {
      this.isOpCompleted = false;
      // Retrieve the data from the local-destroy message parts
      if (isDebugEnabled) {
        logger.debug("Received destroy message of length ({} bytes)", m.getPayloadLength());
      }

      Part regionNamePart = m.getPart(partCnt++);
      Part keyPart = m.getPart(partCnt++);
      Part callbackArgumentPart = m.getPart(partCnt++);

      VersionTag versionTag = (VersionTag)m.getPart(partCnt++).getObject();
      if (versionTag != null) {
        versionTag.replaceNullIDs((InternalDistributedMember) this.endpoint.getMemberId());
      }
      
      regionName = regionNamePart.getString();
      key = keyPart.getStringOrObject();

      Part isInterestListPassedPart = m.getPart(partCnt++);
      Part hasCqsPart = m.getPart(partCnt++);
      
      boolean withInterest = ((Boolean)isInterestListPassedPart.getObject()).booleanValue();
      boolean withCQs = ((Boolean)hasCqsPart.getObject()).booleanValue();

      Object callbackArgument = callbackArgumentPart.getObject();
      if (isDebugEnabled) {
        logger.debug("Destroying entry for region: {} key: {} callbackArgument: {} withInterest={} withCQs={} version={}",
            regionName, key, callbackArgument, withInterest, withCQs, versionTag);
      }
      
      LocalRegion region = (LocalRegion) cacheHelper.getRegion(regionName);
      EventID eventId = null;
      if (region == null) {
        if (isDebugEnabled && !quitting()) {
          logger.debug("Region named {} does not exist", regionName);
        }
      } 
      else if (region.hasServerProxy()
          && (withInterest || !withCQs)) {
        try {
          Part eid = m.getPart(m.getNumberOfParts() - 1);
          eventId = (EventID)eid.getObject();
          try {
            region.basicBridgeClientDestroy(eventId.getDistributedMember(),
              key, callbackArgument, 
              qManager.getState().getProcessedMarker() || !this.isDurableClient,
              eventId, versionTag);
          } catch (ConcurrentCacheModificationException e) {
//            return;  allow CQs to be processed
          }
          this.isOpCompleted = true;
          if (isDebugEnabled) {
            logger.debug("Destroyed entry for region: {} key: {} callbackArgument: {}", regionName, key, callbackArgument);
          }
        }
        catch (EntryNotFoundException e) {
          /*ignore*/
          if (isDebugEnabled && !quitting()) {
            logger.debug("Already destroyed entry for region: {} key: {} callbackArgument: {} eventId={}", regionName, key, callbackArgument, eventId.expensiveToString());
          }
          this.isOpCompleted = true;
        }
      }

      if (withCQs) {
        Part numCqsPart = m.getPart(partCnt++);
        if (isDebugEnabled) {
          logger.debug("Received message has CQ Event. Number of cqs interested in the event : {}", numCqsPart.getInt() / 2);
        }
        partCnt = processCqs(m, partCnt, numCqsPart.getInt(), m.getMessageType(), key, null);
        this.isOpCompleted = true;
      }
    }
    catch (Exception e) {
      String message = LocalizedStrings.CacheClientUpdater_THE_FOLLOWING_EXCEPTION_OCCURRED_WHILE_ATTEMPTING_TO_DESTROY_ENTRY_REGION_0_KEY_1.toLocalizedString(new Object[] { regionName, key });
      handleException(message, e);
    }
  }

  /**
   * Locally destroy a region
   * 
   * @param m
   *                message describing the region
   */
  private void handleDestroyRegion(Message m) {
    Part regionNamePart = null, callbackArgumentPart = null;
    String regionName = null;
    Object callbackArgument = null;
    LocalRegion region = null;
    int partCnt = 0;

    final boolean isDebugEnabled = logger.isDebugEnabled();
    try {
      // Retrieve the data from the local-destroy-region message parts
      if (isDebugEnabled) {
        logger.debug("Received destroy region message of length ({} bytes)", m.getPayloadLength());
      }
      regionNamePart = m.getPart(partCnt++);
      callbackArgumentPart = m.getPart(partCnt++);
      regionName = regionNamePart.getString();
      callbackArgument = callbackArgumentPart.getObject();

      Part hasCqsPart = m.getPart(partCnt++);

      if (isDebugEnabled) {
        logger.debug("Destroying region: {} callbackArgument: {}", regionName, callbackArgument);
      }

      // Handle CQs if any on this region.
      if (((Boolean)hasCqsPart.getObject()).booleanValue()) {
        Part numCqsPart = m.getPart(partCnt++);
        if (isDebugEnabled) {
          logger.debug("Received message has CQ Event. Number of cqs interested in the event : {}", numCqsPart.getInt() / 2);
        }
        partCnt = processCqs(m, partCnt, numCqsPart.getInt(), m.getMessageType(), null, null);
      }
      
      // Confirm that the region exists
      region = (LocalRegion) cacheHelper.getRegion(regionName);
      if (region == null) {
        if (isDebugEnabled && !quitting()) {
          logger.debug("Region named {} does not exist", regionName);
        }
        return;
      }

      // Verify that the region in question should respond to this message
      if (region.hasServerProxy()) {
        // Locally destroy the region
        region.localDestroyRegion(callbackArgument);

        if (isDebugEnabled) {
          logger.debug("Destroyed region: {} callbackArgument: {}", regionName, callbackArgument);
        }
      }
    } catch (RegionDestroyedException e) { // already destroyed
      if (isDebugEnabled) {
        logger.debug("region already destroyed: {}", regionName);
      }
    } catch (Exception e) {
      String message = LocalizedStrings.CacheClientUpdater_CAUGHT_AN_EXCEPTION_WHILE_ATTEMPTING_TO_DESTROY_REGION_0.toLocalizedString(regionName);
      handleException(message, e);
    }
  }

  /**
   * Locally clear a region
   * 
   * @param m
   *                message describing the region to clear
   */
  private void handleClearRegion(Message m) {
    String regionName = null;
    int partCnt = 0;

    final boolean isDebugEnabled = logger.isDebugEnabled();
    try {
      // Retrieve the data from the clear-region message parts
      if (isDebugEnabled) {
        logger.debug("{}: Received clear region message of length ({} bytes)", this, m.getPayloadLength());
      }

      Part regionNamePart = m.getPart(partCnt++);
      Part callbackArgumentPart = m.getPart(partCnt++);

      Part hasCqsPart = m.getPart(partCnt++);

      regionName = regionNamePart.getString();
      Object callbackArgument = callbackArgumentPart.getObject();
      if (isDebugEnabled) {
        logger.debug("Clearing region: {} callbackArgument: {}", regionName, callbackArgument);
      }

      if (((Boolean) hasCqsPart.getObject()).booleanValue()) {
        Part numCqsPart = m.getPart(partCnt++);
        if (isDebugEnabled) {
          logger.debug("Received message has CQ Event. Number of cqs interested in the event : {}", numCqsPart.getInt() / 2);
        }
        partCnt = processCqs(m, partCnt, numCqsPart.getInt(), m
            .getMessageType(), null, null);
      }

      // Confirm that the region exists
      LocalRegion region = (LocalRegion) cacheHelper.getRegion(regionName);
      if (region == null) {
        if (isDebugEnabled && !quitting()) {
          logger.debug("Region named {} does not exist", regionName);
        }
        return;
      }

      // Verify that the region in question should respond to this
      // message
      if (region.hasServerProxy()) {
        // Locally clear the region
        region.basicBridgeClientClear(callbackArgument, qManager.getState()
            .getProcessedMarker() || !this.isDurableClient);

        if (isDebugEnabled) {
          logger.debug("Cleared region: {} callbackArgument: {}", regionName, callbackArgument);
        }
      }
    } 
    catch (Exception e) {
      String message = LocalizedStrings.CacheClientUpdater_CAUGHT_THE_FOLLOWING_EXCEPTION_WHILE_ATTEMPTING_TO_CLEAR_REGION_0.toLocalizedString(regionName);
      handleException(message, e);
    }
  }

  /**
   * Locally invalidate a region
   * NOTE: Added as part of bug#38048. The code only takes care of CQ processing.
   *       Support needs to be added for local region invalidate.
   * @param m message describing the region to clear
   */
  private void handleInvalidateRegion(Message m) {
    String regionName = null;
    int partCnt = 0;

    final boolean isDebugEnabled = logger.isDebugEnabled();
    try {
      // Retrieve the data from the invalidate-region message parts
      if (isDebugEnabled) {
        logger.debug("{}: Received invalidate region message of length ({} bytes)", this, m.getPayloadLength());
      }

      Part regionNamePart = m.getPart(partCnt++);
      partCnt ++; // Part callbackArgumentPart = m.getPart(partCnt++);

      Part hasCqsPart = m.getPart(partCnt++);

      regionName = regionNamePart.getString();
//      Object callbackArgument = callbackArgumentPart.getObject();

      if (((Boolean) hasCqsPart.getObject()).booleanValue()) {
        Part numCqsPart = m.getPart(partCnt++);
        if (isDebugEnabled) {
          logger.debug("Received message has CQ Event. Number of cqs interested in the event : {}", numCqsPart.getInt() / 2);
        }
        partCnt = processCqs(m, partCnt, numCqsPart.getInt(), m.getMessageType(), null, null);
      }
      
      // Confirm that the region exists
      LocalRegion region = (LocalRegion) cacheHelper.getRegion(regionName);
      if (region == null) {
        if (isDebugEnabled && !quitting()) {
          logger.debug("Region named {} does not exist", regionName);
        }
        return;
      }

      // Verify that the region in question should respond to this
      // message
      if (region.hasServerProxy()){
        return;

        // NOTE:
        // As explained in the method description, this code is added as part
        // of CQ bug fix. Cache server team needs to look into changes relating
        // to local region.
        // 
        // Locally invalidate the region
        // region.basicBridgeClientInvalidate(callbackArgument,
        // proxy.getProcessedMarker());

        //if (logger.debugEnabled()) {
        //  logger.debug(toString() + ": Cleared region: " + regionName
        //               + " callbackArgument: " + callbackArgument);
        //}

      }

    } 
    catch (Exception e) {
      String message = 
        LocalizedStrings.CacheClientUpdater_CAUGHT_THE_FOLLOWING_EXCEPTION_WHILE_ATTEMPTING_TO_INVALIDATE_REGION_0.toLocalizedString(regionName);
      handleException(message, e);
    }
  }

  /**
   * Register instantiators locally
   *
   * @param msg
   *                message describing the new instantiators
   * @param eventId
   *                eventId of the instantiators
   */
  private void handleRegisterInstantiator(Message msg, EventID eventId) {
    String instantiatorClassName = null;
    final boolean isDebugEnabled = logger.isDebugEnabled();
    try {
      int noOfParts = msg.getNumberOfParts();
      if (isDebugEnabled) {
        logger.debug("{}: Received register instantiators message of parts {}", getName(), noOfParts);
      }
      Assert.assertTrue((noOfParts - 1) % 3 == 0);
      for (int i = 0; i < noOfParts - 1; i = i + 3) {
        instantiatorClassName = (String) CacheServerHelper
            .deserialize(msg.getPart(i).getSerializedForm());
        String instantiatedClassName = (String) CacheServerHelper
            .deserialize(msg.getPart(i + 1).getSerializedForm());
        int id = msg.getPart(i + 2).getInt();
        InternalInstantiator.register(instantiatorClassName, instantiatedClassName, id,
            false, eventId, null/* context */);
        // distribute is false because we don't want to propagate this to
        // servers recursively
      }
      
      // // CALLBACK TESTING PURPOSE ONLY ////
      if (PoolImpl.IS_INSTANTIATOR_CALLBACK) {
        ClientServerObserver bo = ClientServerObserverHolder.getInstance();
        bo.afterReceivingFromServer(eventId);
      }
      // /////////////////////////////////////
    }
    // TODO bug: can the following catch be more specific?
    catch (Exception e) {
      if (isDebugEnabled) {
        logger.debug("{}: Caught following exception while attempting to read Instantiator : {}", this, instantiatorClassName, e);
      }
    }
  }
  
  private void handleRegisterDataSerializer(Message msg, EventID eventId) {
    Class dataSerializerClass = null ;
    final boolean isDebugEnabled = logger.isDebugEnabled();
    try {
      int noOfParts = msg.getNumberOfParts();
//      int numOfClasses = noOfParts - 3; // 1 for ds classname, 1 for ds id and 1 for eventId.
      if (isDebugEnabled) {
        logger.debug("{}: Received register dataserializer message of parts {}", getName(), noOfParts);
      }
      
      for (int i = 0; i < noOfParts - 1;) {
        try {
          String dataSerializerClassName = (String) CacheServerHelper
              .deserialize(msg.getPart(i).getSerializedForm());
          int id = msg.getPart(i + 1).getInt();
          InternalDataSerializer.register(dataSerializerClassName, false, eventId, null/* context */, id);
          // distribute is false because we don't want to propagate this to
          // servers recursively

          int numOfClasses = msg.getPart(i + 2).getInt();
          int j = 0;
          for (; j < numOfClasses; j++) {
            String className = (String)CacheServerHelper.deserialize(msg
                .getPart(i + 3 + j).getSerializedForm());
            InternalDataSerializer.updateSupportedClassesMap(
                dataSerializerClassName, className);
          }
          i = i + 3 + j;
        } catch (ClassNotFoundException e) {
          if (isDebugEnabled) {
            logger.debug("{}: Caught following exception while attempting to read DataSerializer : {}", this, dataSerializerClass, e);
          }
        }
      }
      
      // // CALLBACK TESTING PURPOSE ONLY ////
      if (PoolImpl.IS_INSTANTIATOR_CALLBACK) {
        ClientServerObserver bo = ClientServerObserverHolder.getInstance();
        bo.afterReceivingFromServer(eventId);
      }
     ///////////////////////////////////////
    }
    // TODO bug: can the following catch be more specific?
    catch (Exception e) {
      if (isDebugEnabled) {
        logger.debug("{}: Caught following exception while attempting to read DataSerializer : {}", this, dataSerializerClass, e);
      }
    }
  }

  /**
   * Processes message to invoke CQ listeners.
   * 
   * @param startMessagePart
   * @param numCqParts
   * @param messageType
   * @param key
   * @param value
   */
  private int processCqs(Message m, int startMessagePart, int numCqParts,
      int messageType, Object key, Object value) {
    return processCqs(m, startMessagePart, numCqParts, messageType, key, value,
        null, null/* eventId */);
  }

  private int processCqs(Message m, int startMessagePart, int numCqParts,
      int messageType, Object key, Object value, byte[] delta, EventID eventId) {
    //String[] cqs = new String[numCqs/2];
    HashMap cqs = new HashMap();
    final boolean isDebugEnabled = logger.isDebugEnabled();

      for (int cqCnt=0; cqCnt < numCqParts;) {
        StringBuilder str = null;
        if (isDebugEnabled) {
          str = new StringBuilder(100);
          str.append("found these queries: ");
        }
        try {
          // Get CQ Name.
          Part cqNamePart = m.getPart(startMessagePart + (cqCnt++));
          // Get CQ Op.
          Part cqOpPart = m.getPart(startMessagePart + (cqCnt++));
          cqs.put(cqNamePart.getString(), Integer.valueOf(cqOpPart.getInt()));

          if (str != null) {
             str.append(cqNamePart.getString())
               .append(" op=").append(cqOpPart.getInt()).append("  ");
          }
        } catch (Exception ex) {
          logger.warn(LocalizedMessage.create(LocalizedStrings.CacheClientUpdater_ERROR_WHILE_PROCESSING_THE_CQ_MESSAGE_PROBLEM_WITH_READING_MESSAGE_FOR_CQ_0, cqCnt));
        }
        if (isDebugEnabled && str != null) {
          logger.debug(str);
        }
      }

      {
        CqService cqService = this.cache.getCqService();
        try {
          cqService.dispatchCqListeners(cqs, messageType, key, value, delta,
            qManager, eventId);
        }
        catch (Exception ex) {
          logger.warn(LocalizedMessage.create(LocalizedStrings.CacheClientUpdater_FAILED_TO_INVOKE_CQ_DISPATCHER_ERROR___0, ex.getMessage()));
          if (isDebugEnabled) {
            logger.debug("Failed to invoke CQ Dispatcher.", ex);
          }
        }
      }

    return (startMessagePart + numCqParts);
  }

  private void handleRegisterInterest(Message m) {
    String regionName = null;
    Object key = null;
    int interestType;
    byte interestResultPolicy;
    boolean isDurable;
    boolean receiveUpdatesAsInvalidates;
    int partCnt = 0;

    final boolean isDebugEnabled = logger.isDebugEnabled();
    try {
      // Retrieve the data from the add interest message parts
      if (isDebugEnabled) {
        logger.debug("{}: Received add interest message of length ({} bytes)", this, m.getPayloadLength());
      }
      Part regionNamePart = m.getPart(partCnt++);
      Part keyPart = m.getPart(partCnt++);
      Part interestTypePart = m.getPart(partCnt++);
      Part interestResultPolicyPart = m.getPart(partCnt++);
      Part isDurablePart = m.getPart(partCnt++);
      Part receiveUpdatesAsInvalidatesPart = m.getPart(partCnt++);

      regionName = regionNamePart.getString();
      key = keyPart.getStringOrObject();
      interestType = ((Integer) interestTypePart.getObject()).intValue();
      interestResultPolicy = ((Byte) interestResultPolicyPart.getObject()).byteValue();
      isDurable = ((Boolean) isDurablePart.getObject()).booleanValue();
      receiveUpdatesAsInvalidates = ((Boolean) receiveUpdatesAsInvalidatesPart.getObject()).booleanValue();

      // Confirm that region exists
      LocalRegion region = (LocalRegion)cacheHelper.getRegion(regionName);
      if (region == null) {
        if (isDebugEnabled && !quitting()) {
          logger.debug("{}: Region named {} does not exist", this, regionName);
        }
        return;
      }

      // Verify that the region in question should respond to this message
      if (!region.hasServerProxy()) {
        return;
      }

      if (key instanceof List) {
        region.getServerProxy().addListInterest((List) key,
            InterestResultPolicy.fromOrdinal(interestResultPolicy), isDurable,
            receiveUpdatesAsInvalidates);
      } else {
        region.getServerProxy().addSingleInterest(key, interestType,
            InterestResultPolicy.fromOrdinal(interestResultPolicy), isDurable,
            receiveUpdatesAsInvalidates);
      }
    }
    catch (Exception e) {
      String message = ": The following exception occurred while attempting to add interest (region: " + regionName
         + " key: " + key + "): ";
      handleException(message, e);
    }
  }

  private void handleUnregisterInterest(Message m) {
    String regionName = null;
    Object key = null;
    int interestType;
    boolean isDurable;
    boolean receiveUpdatesAsInvalidates;
    int partCnt = 0;

    final boolean isDebugEnabled = logger.isDebugEnabled();
    try {
      // Retrieve the data from the remove interest message parts
      if (isDebugEnabled) {
        logger.debug("{}: Received remove interest message of length ({} bytes)", this, m.getPayloadLength());
      }

      Part regionNamePart = m.getPart(partCnt++);
      Part keyPart = m.getPart(partCnt++);
      Part interestTypePart = m.getPart(partCnt++);
      Part isDurablePart = m.getPart(partCnt++);
      Part receiveUpdatesAsInvalidatesPart = m.getPart(partCnt++);
      // Not reading the eventId part

      regionName = regionNamePart.getString();
      key = keyPart.getStringOrObject();
      interestType = ((Integer) interestTypePart.getObject()).intValue();
      isDurable = ((Boolean) isDurablePart.getObject()).booleanValue();
      receiveUpdatesAsInvalidates =
        ((Boolean) receiveUpdatesAsInvalidatesPart.getObject()).booleanValue();

      // Confirm that region exists
      LocalRegion region = (LocalRegion)cacheHelper.getRegion(regionName);
      if (region == null) {
        if (isDebugEnabled) {
          logger.debug("{}: Region named {} does not exist", this, regionName);
        }
        return;
      }

      // Verify that the region in question should respond to this message
      if (!region.hasServerProxy()) {
        return;
      }

      if (key instanceof List) {
        region.getServerProxy().removeListInterest((List) key, isDurable,
            receiveUpdatesAsInvalidates);
      } else {
        region.getServerProxy().removeSingleInterest(key, interestType,
            isDurable, receiveUpdatesAsInvalidates);
      }
    }
    catch (Exception e) {
      String message = ": The following exception occurred while attempting to add interest (region: " + regionName
         + " key: " + key + "): ";
      handleException(message, e);
    }
  }
  
  
  private void handleTombstoneOperation(Message msg) {
    String regionName = "unknown";
    try { // not sure why this isn't done by the caller
      int partIdx = 0;
      // see ClientTombstoneMessage.getGFE70Message
      regionName = msg.getPart(partIdx++).getString();
      int op = msg.getPart(partIdx++).getInt();
      LocalRegion region = (LocalRegion)cacheHelper.getRegion(regionName);
      if (region == null) {
        if (!quitting()) {
          if (logger.isDebugEnabled()) {
            logger.debug("{}: Region named {} does not exist", this, regionName);
          }
        }
        return;
      }
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Received tombstone operation for region {} with operation={}", this, region, op);
      }
      if (!region.getConcurrencyChecksEnabled()) {
        return;
      }
      switch (op) {
      case 0:
        Map<VersionSource, Long> regionGCVersions = 
          (Map<VersionSource, Long>)msg.getPart(partIdx++).getObject();
        EventID eventID = (EventID)msg.getPart(partIdx++).getObject();
        region.expireTombstones(regionGCVersions, eventID, null);
        break;
      case 1:
        Set<Object> removedKeys = (Set<Object>)msg.getPart(partIdx++).getObject();
        region.expireTombstoneKeys(removedKeys);
        break;
      default:
        throw new IllegalArgumentException("unknown operation type " + op);
      }
    } catch (Exception e) {
      handleException(": exception while removing tombstones from " + regionName, e);
    }
  }

  /**
   * Indicate whether the updater or the system is trying to terminate
   *
   * @return true if we are trying to stop
   */
  private boolean quitting() {
    if (isInterrupted()) {
      // Any time an interrupt is thrown at this thread, regard it as a
      // request to terminate
      return true;
    }
    if (!continueProcessing.get()) {
      // de facto flag indicating we are to stop
      return true;
    }
    if (cache != null && cache.getCancelCriterion().cancelInProgress() != null) {
      // System is cancelling
      return true;
    }

    // The pool stuff is really sick, so it's possible for us to have a distributed
    // system that is not the same as our cache.  Check it just in case...
    if (system.getCancelCriterion().cancelInProgress() != null) {
      return true;
    }

    // All clear on this end, boss.
    return false;
  }
  
  private void waitForFailedUpdater() {
    boolean gotInterrupted = false;
    try {
      if (this.failedUpdater != null) {
        logger.info(LocalizedMessage.create(LocalizedStrings.CacheClientUpdater__0_IS_WAITING_FOR_1_TO_COMPLETE, new Object[] {this, this.failedUpdater}));
        while (this.failedUpdater.isAlive()){
          if (quitting()) {
            return;
          }
          this.failedUpdater.join(5000);
        }
      }
    }
    catch (InterruptedException ie) {
      gotInterrupted = true;
      return; // just bail, because I have not done anything yet
    }
    finally {
      if (!gotInterrupted && this.failedUpdater != null ) {
        logger.info(LocalizedMessage.create(LocalizedStrings.CacheClientUpdater_0_HAS_COMPLETED_WAITING_FOR_1, new Object[] {this, this.failedUpdater}));
        failedUpdater = null;
      }
    }
  }

  /**
   * Processes messages received from the server.
   * 
   * Only certain types of messages are handled.
   * 
   * @see MessageType#CLIENT_MARKER
   * @see MessageType#LOCAL_CREATE
   * @see MessageType#LOCAL_UPDATE
   * @see MessageType#LOCAL_INVALIDATE
   * @see MessageType#LOCAL_DESTROY
   * @see MessageType#LOCAL_DESTROY_REGION
   * @see MessageType#CLEAR_REGION
   * @see ClientUpdateMessage
   */
  protected void processMessages() {
    final boolean isDebugEnabled = logger.isDebugEnabled();
    try {
      Part eid = null;
      Message _message = initializeMessage();
      if (quitting()) {
        if (isDebugEnabled) {
          logger.debug("processMessages quitting early because we have stopped");
        }
        // our caller calls close which will notify all waiters for our init
        return;
      }
      logger.info(LocalizedMessage.create(LocalizedStrings.CacheClientUpdater_0_READY_TO_PROCESS_MESSAGES, this));

      while (continueProcessing.get()) {
        // SystemFailure.checkFailure(); dm will check this
        if (quitting()) {
          if (isDebugEnabled) {
            logger.debug("termination detected");
          }
          // our caller calls close which will notify all waiters for our init
          return;
        }

        // the endpoint died while this thread was sleeping.
        if (this.endpoint.isClosed()) {
          if (isDebugEnabled) {
            logger.debug("endpoint died");
          }
          this.continueProcessing.set(false);// = false;
          break;
        }

        try {
          // Read the message
          _message.recv();

          // Wait for the previously failed cache client updater
          // to finish. This will avoid out of order messages.
          waitForFailedUpdater();
          cache.waitForRegisterInterestsInProgress();
          if (quitting()) {
            if (isDebugEnabled) {
              logger.debug("processMessages quitting before processing message");
            }
            break;
          }
          
          // If the message is a ping, ignore it
          if (_message.getMessageType() == MessageType.SERVER_TO_CLIENT_PING) {
            if (isDebugEnabled) {
              logger.debug("{}: Received ping", this);
            }
            continue;
          }

          boolean isDeltaSent = false;
          boolean isCreateOrUpdate = _message.getMessageType() == MessageType.LOCAL_CREATE
              || _message.getMessageType() == MessageType.LOCAL_UPDATE;
          if (isCreateOrUpdate) {
            isDeltaSent = ((Boolean)_message.getPart(2).getObject())
                .booleanValue();
          }
          
          // extract the eventId and verify if it is a duplicate event
          // if it is a duplicate event, ignore
          // @since 5.1
          int numberOfParts = _message.getNumberOfParts();
          eid = _message.getPart(numberOfParts - 1);
          // TODO the message handling methods also deserialized the eventID - inefficient
          EventID eventId = (EventID)eid.getObject();

          // no need to verify if the instantiator msg is duplicate or not
          if (_message.getMessageType() != MessageType.REGISTER_INSTANTIATORS && _message.getMessageType() != MessageType.REGISTER_DATASERIALIZERS ) {
            if (this.qManager.getState().verifyIfDuplicate(eventId, !(this.isDurableClient || isDeltaSent))) {
              continue;
            }
          }
          if (logger.isTraceEnabled(LogMarker.BRIDGE_SERVER)) {
            logger.trace(LogMarker.BRIDGE_SERVER, "Processing event with id {}" + eventId.expensiveToString());
          }
          this.isOpCompleted = true;
          // Process the message
          switch (_message.getMessageType()) {
          case MessageType.LOCAL_CREATE:
          case MessageType.LOCAL_UPDATE:
            handleUpdate(_message);
            break;
          case MessageType.LOCAL_INVALIDATE:
            handleInvalidate(_message);
            break;
          case MessageType.LOCAL_DESTROY:
            handleDestroy(_message);
            break;
          case MessageType.LOCAL_DESTROY_REGION:
            handleDestroyRegion(_message);
            break;
          case MessageType.CLEAR_REGION:
            handleClearRegion(_message);
            break;
          case MessageType.REGISTER_INSTANTIATORS:
            handleRegisterInstantiator(_message, eventId);
            break;
          case MessageType.REGISTER_DATASERIALIZERS:
          handleRegisterDataSerializer(_message, eventId);
            break;
          case MessageType.CLIENT_MARKER:
            handleMarker(_message);
            break;
          case MessageType.INVALIDATE_REGION:
            handleInvalidateRegion(_message);
            break;
          case MessageType.CLIENT_REGISTER_INTEREST:
            handleRegisterInterest(_message);
            break;
          case MessageType.CLIENT_UNREGISTER_INTEREST:
            handleUnregisterInterest(_message);
            break;
          case MessageType.TOMBSTONE_OPERATION:
            handleTombstoneOperation(_message);
            break;
          default:
            logger.warn(LocalizedMessage.create(LocalizedStrings.CacheClientUpdater_0_RECEIVED_AN_UNSUPPORTED_MESSAGE_TYPE_1, new Object[] {this, MessageType.getString(_message.getMessageType())}));
            break;
          }

          if (this.isOpCompleted && (this.isDurableClient || isDeltaSent)) {
            this.qManager.getState().verifyIfDuplicate(eventId, true);
          }

          // TODO we should maintain the client's "live" view of the server
          // but we don't because the server health monitor needs traffic
          // originating from the client
          // and by updating the last update stat, the ServerMonitor is less
          // likely to send pings...
          // and the ClientHealthMonitor will cause a disconnect -- mthomas
          // 10/18/2006

          // this._endpoint.setLastUpdate();

        }
        catch (InterruptedIOException e) {
          // Per Sun's support web site, this exception seems to be peculiar
          // to Solaris, and may eventually not even be generated there.
          //
          // When this exception is thrown, the thread has been interrupted, but
          // isInterrupted() is false. (How very odd!)
          //
          // We regard it the same as an InterruptedException
          this.endPointDied = true;
          
          continueProcessing.set(false);// = false;
          if (isDebugEnabled) {
            logger.debug("InterruptedIOException");
          }
        }
        catch (IOException e) {
          this.endPointDied = true;
          // Either the server went away, or we caught a closing condition.
          if (!quitting()) {
            // Server departed; print a message.
            String message = ": Caught the following exception and will exit: ";
            String errMessage = e.getMessage();
            if (errMessage == null) {
              errMessage = "";
            }
            ClientServerObserver bo = ClientServerObserverHolder.getInstance();
            bo.beforeFailoverByCacheClientUpdater(this.location);
            eManager.serverCrashed(this.endpoint);
            if (isDebugEnabled) {
              logger.debug("" + message + e);
            }
          } // !quitting

          // In any event, terminate this thread.
          continueProcessing.set(false);// = false;
          if (isDebugEnabled) {
            logger.debug("terminated due to IOException");
          }
        }
        catch (Exception e) {
          if (!quitting()) {
            this.endPointDied = true;
            ClientServerObserver bo = ClientServerObserverHolder.getInstance();
            bo.beforeFailoverByCacheClientUpdater(this.location);
            eManager.serverCrashed(this.endpoint);
            String message = ": Caught the following exception and will exit: ";
            handleException(message, e);
          }
          // In any event, terminate this thread.
          continueProcessing.set(false);// = false; // force termination
          if (isDebugEnabled) {
            logger.debug("CCU terminated due to Exception");
          }
        }
        finally {
          _message.clear();
        }
      } // while
    }
    finally {
      if (isDebugEnabled) {
        logger.debug("has stopped and cleaning the helper ..");
      }
      this.close(); // added to fixes some race conditions associated with 38382
      //this will make sure that if this thread dies without starting QueueMgr then it will start..
      //1. above we ignore InterruptedIOException and this thread dies without informing QueueMgr
      //2. if there is some other race codition with continueProcessing flag
      this.qManager.checkEndpoint(this, endpoint);
    }
  }

  /**
   * Conditionally print a warning describing the failure
   * <p>
   * Signals run thread to stop. Messages are not printed if the thread or the
   * distributed system has already been instructed to terminate.
   * 
   * @param message
   *                contextual string for the failure
   * @param exception
   *                underlying exception
   */
  private void handleException(String message, Exception exception) {
    boolean unexpected = !quitting();

    // If this was a surprise, print a warning.
    if (unexpected && !(exception instanceof CancelException)) {
      logger.warn(LocalizedMessage.create(LocalizedStrings.CacheClientUpdater_0__1__2, new Object[] {this, message, exception}), exception);
    }
    // We can't shutdown the client updater just because of an exception.
    // Let the caller decide if we should continue running or not.
  }

  /**
   * Return an object from serialization. Only used in debug logging.
   * 
   * @param serializedBytes
   *                the serialized form
   * @return the deserialized object
   */
  private Object deserialize(byte[] serializedBytes) {
    Object deserializedObject = serializedBytes;
    // This is a debugging method so ignore all exceptions like
    // ClassNotFoundException
    try {
      DataInputStream dis = new DataInputStream(new ByteArrayInputStream(
          serializedBytes));
      deserializedObject = DataSerializer.readObject(dis);
    } catch (Exception e) {
    }
    return deserializedObject;
  }

  /**
   * @return the local port of our {@link #socket}
   */
  protected int getLocalPort() {
    return socket.getLocalPort();
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.gemstone.gemfire.distributed.internal.InternalDistributedSystem.DisconnectListener#onDisconnect(com.gemstone.gemfire.distributed.internal.InternalDistributedSystem)
   */
  public void onDisconnect(InternalDistributedSystem sys) {
    stopUpdater();
  }

  /**
   * true if the EndPoint represented by this updater thread has died.
   */
  private volatile boolean endPointDied = false;

  /**
   * Returns true if the end point represented by this updater is considered dead.
   * @return true if {@link #endpoint} died.
   */
  public boolean isEndPointDead() {
  return this.endPointDied;
  }
  
  private void verifySocketBufferSize(int requestedBufferSize, int actualBufferSize, String type) {
    if (actualBufferSize < requestedBufferSize) {
      logger.info(LocalizedMessage.create(
          LocalizedStrings.Connection_SOCKET_0_IS_1_INSTEAD_OF_THE_REQUESTED_2,
          new Object[] { type + " buffer size", actualBufferSize, requestedBufferSize }));
    }
  }

  /**
   * Stats for a CacheClientUpdater. Currently the only thing measured
   * are incoming bytes on the wire
   * @since 5.7
   * @author darrel
   */
  public static class CCUStats implements MessageStats {
    // static fields 
    private static final StatisticsType type;
    private final static int messagesBeingReceivedId;
    private final static int messageBytesBeingReceivedId;
    private final static int receivedBytesId;
    
    static {
      StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();
      type = f.createType(
                          "CacheClientUpdaterStats", 
                          "Statistics about incoming subscription data",
                          new StatisticDescriptor[] {
                            f.createLongCounter("receivedBytes",
                                                "Total number of bytes received from the server.",
                                                "bytes"),
                            f.createIntGauge("messagesBeingReceived",
                                             "Current number of message being received off the network or being processed after reception.",
                                             "messages"),
                            f.createLongGauge("messageBytesBeingReceived",
                                              "Current number of bytes consumed by messages being received or processed.",
                                              "bytes"),
                          });
      receivedBytesId = type.nameToId("receivedBytes");
      messagesBeingReceivedId = type.nameToId("messagesBeingReceived");
      messageBytesBeingReceivedId = type.nameToId("messageBytesBeingReceived");
    }

    // instance fields
    private final Statistics stats;

    public CCUStats(DistributedSystem ids, ServerLocation location) {
      // no need for atomic since only a single thread will be writing these
      this.stats = ids.createStatistics(type, "CacheClientUpdater-" + location);
    }
    
    public void close() {
      this.stats.close();
    }
    
    public final void incReceivedBytes(long v) {
      this.stats.incLong(receivedBytesId, v);
    }
    public final void incSentBytes(long v) {
      // noop since we never send messages
    }
    public void incMessagesBeingReceived(int bytes) {
      stats.incInt(messagesBeingReceivedId, 1);
      if (bytes > 0) {
        stats.incLong(messageBytesBeingReceivedId, bytes);
      }
    }
    public void decMessagesBeingReceived(int bytes) {
      stats.incInt(messagesBeingReceivedId, -1);
      if (bytes > 0) {
        stats.incLong(messageBytesBeingReceivedId, -bytes);
      }
    }

    /**
     * Returns the current time (ns).
     * @return the current time (ns)
     */
    public long startTime()
    {
      return DistributionStats.getStatTime();
    }

  }

  public boolean isProcessing() {
    // TODO Auto-generated method stub
    return continueProcessing.get();
  }
}
