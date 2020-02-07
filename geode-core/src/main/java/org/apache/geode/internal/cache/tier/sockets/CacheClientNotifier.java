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

import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_ACCESSOR_PP;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_AUTHENTICATOR;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.Socket;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import org.apache.logging.log4j.Logger;
import org.apache.shiro.subject.Subject;

import org.apache.geode.CancelException;
import org.apache.geode.InternalGemFireError;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.cache.CacheEvent;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.InterestRegistrationEvent;
import org.apache.geode.cache.InterestRegistrationListener;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.client.internal.PoolImpl.PoolTask;
import org.apache.geode.cache.query.CqException;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.internal.DefaultQuery;
import org.apache.geode.cache.query.internal.cq.CqService;
import org.apache.geode.cache.query.internal.cq.ServerCQ;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.ClassLoadUtil;
import org.apache.geode.internal.SystemTimer;
import org.apache.geode.internal.cache.CacheClientStatus;
import org.apache.geode.internal.cache.CacheDistributionAdvisor;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.ClientRegionEventImpl;
import org.apache.geode.internal.cache.ClientServerObserver;
import org.apache.geode.internal.cache.ClientServerObserverHolder;
import org.apache.geode.internal.cache.Conflatable;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EnumListenerEvent;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.FilterProfile;
import org.apache.geode.internal.cache.FilterRoutingInfo.FilterInfo;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalCacheEvent;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.RegionEventImpl;
import org.apache.geode.internal.cache.ha.HAContainerMap;
import org.apache.geode.internal.cache.ha.HAContainerRegion;
import org.apache.geode.internal.cache.ha.HAContainerWrapper;
import org.apache.geode.internal.cache.ha.HARegionQueue;
import org.apache.geode.internal.cache.ha.ThreadIdentifier;
import org.apache.geode.internal.cache.tier.CommunicationMode;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.OverflowAttributes;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.logging.InternalLogWriter;
import org.apache.geode.internal.net.SocketCloser;
import org.apache.geode.internal.serialization.Version;
import org.apache.geode.internal.statistics.DummyStatisticsFactory;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.security.AccessControl;
import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.security.AuthenticationRequiredException;
import org.apache.geode.security.GemFireSecurityException;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * Class {@code CacheClientNotifier} works on the server and manages client socket connections
 * to clients requesting notification of updates and notifies them when updates occur.
 *
 * @since GemFire 3.2
 */
public class CacheClientNotifier {
  private static final Logger logger = LogService.getLogger();

  @MakeNotStatic
  private static volatile CacheClientNotifier ccnSingleton;

  private final SocketMessageWriter socketMessageWriter = new SocketMessageWriter();
  private final ClientRegistrationEventQueueManager clientRegistrationEventQueueManager;
  private final CacheClientProxyFactory cacheClientProxyFactory;

  /**
   * Factory method to construct a CacheClientNotifier {@code CacheClientNotifier} instance.
   *
   * @param cache The GemFire {@code InternalCache}
   * @param clientRegistrationEventQueueManager Manages temporary registration queues for clients
   * @return A {@code CacheClientNotifier} instance
   */
  public static synchronized CacheClientNotifier getInstance(InternalCache cache,
      ClientRegistrationEventQueueManager clientRegistrationEventQueueManager,
      StatisticsClock statisticsClock,
      CacheServerStats acceptorStats,
      int maximumMessageCount,
      int messageTimeToLive,
      ConnectionListener listener,
      OverflowAttributes overflowAttributes,
      boolean isGatewayReceiver) {
    if (ccnSingleton == null) {
      ccnSingleton = new CacheClientNotifier(cache, clientRegistrationEventQueueManager,
          statisticsClock, acceptorStats, maximumMessageCount, messageTimeToLive, listener,
          isGatewayReceiver, new CacheClientProxyFactory());
    }

    if (!isGatewayReceiver && ccnSingleton.getHaContainer() == null) {
      // Gateway receiver might have create CCN instance without HaContainer
      // In this case, the HaContainer should be lazily created here
      ccnSingleton.initHaContainer(overflowAttributes);
    }
    return ccnSingleton;
  }

  public static CacheClientNotifier getInstance() {
    return ccnSingleton;
  }

  @VisibleForTesting
  public static void resetInstance() {
    ccnSingleton = null;
  }

  /**
   * Registers a new client updater that wants to receive updates with this server.
   *
   * @param socket The socket over which the server communicates with the client.
   * @param isPrimary Whether server is the primary subscription end point for this client
   * @param acceptorId ID of the acceptor used to clean up the client connection
   * @param notifyBySubscription Whether the Server is running in NotifyBySubscription mode
   * @throws IOException Can occur if there are issues communicating over the socket
   */
  public void registerClient(final ClientRegistrationMetadata clientRegistrationMetadata,
      final Socket socket, final boolean isPrimary, final long acceptorId,
      final boolean notifyBySubscription) throws IOException {
    long startTime = statistics.startTime();

    ClientProxyMembershipID clientProxyMembershipID =
        clientRegistrationMetadata.getClientProxyMembershipID();
    DataOutputStream dataOutputStream = clientRegistrationMetadata.getDataOutputStream();
    Version clientVersion = clientRegistrationMetadata.getClientVersion();

    try {
      if (isClientPermitted(clientRegistrationMetadata, clientProxyMembershipID)) {
        ClientRegistrationEventQueueManager.ClientRegistrationEventQueue clientRegistrationEventQueue =
            clientRegistrationEventQueueManager.create(clientProxyMembershipID,
                new ConcurrentLinkedQueue<>(),
                new ReentrantReadWriteLock());

        try {
          registerClientInternal(clientRegistrationMetadata, socket, isPrimary, acceptorId,
              notifyBySubscription);
        } finally {
          clientRegistrationEventQueueManager.drain(clientRegistrationEventQueue, this);
        }
      }
    } catch (AuthenticationRequiredException ex) {
      handleAuthenticationException(clientProxyMembershipID, dataOutputStream, clientVersion, ex,
          Handshake.REPLY_EXCEPTION_AUTHENTICATION_REQUIRED);
    } catch (AuthenticationFailedException ex) {
      handleAuthenticationException(clientProxyMembershipID, dataOutputStream, clientVersion, ex,
          Handshake.REPLY_EXCEPTION_AUTHENTICATION_FAILED);
    } catch (CacheException e) {
      logger.warn(
          String.format("%s :registerClient: Exception encountered in registration %s", this, e),
          e);
      throw new IOException(String.format(
          "Exception occurred while trying to register interest due to : %s", e.getMessage()), e);
    } catch (Exception ex) {
      logger.warn(String.format("An exception was thrown for client [%s].",
          clientProxyMembershipID != null ? clientProxyMembershipID : "unknown"), ex);
      socketMessageWriter.writeException(dataOutputStream,
          CommunicationMode.UnsuccessfulServerToClient.getModeNumber(), ex, clientVersion);
    }

    statistics.endClientRegistration(startTime);
  }

  /**
   * Continues the registration of a new client that wants to receive updates with this server.
   *
   * @param clientRegistrationMetadata Contains registration info pertaining to the client
   * @param socket The socket over which the server communicates with the client.
   * @param isPrimary Whether server is the primary subscription end point for this client
   * @param acceptorId ID of the acceptor used to clean up the client connection
   * @param notifyBySubscription Whether the Server is running in NotifyBySubscription mode
   * @throws IOException Can occur if there are issues communicating over the socket
   * @throws CacheException A generic exception, which indicates a cache error has occurred.
   * @throws ClassNotFoundException Thrown when the ClientProxyMembershipID class is not found
   * @throws NoSuchMethodException Potentially thrown by performPostAuthorization
   * @throws InvocationTargetException Potentially thrown by performPostAuthorization
   * @throws IllegalAccessException Potentially thrown by performPostAuthorization
   */
  void registerClientInternal(final ClientRegistrationMetadata clientRegistrationMetadata,
      final Socket socket,
      final boolean isPrimary,
      final long acceptorId, final boolean notifyBySubscription)
      throws IOException, CacheException, ClassNotFoundException, NoSuchMethodException,
      InvocationTargetException, IllegalAccessException {
    ClientProxyMembershipID clientProxyMembershipID =
        clientRegistrationMetadata.getClientProxyMembershipID();
    byte clientConflation = clientRegistrationMetadata.getClientConflation();
    Version clientVersion = clientRegistrationMetadata.getClientVersion();

    CacheClientProxy cacheClientProxy = getClientProxy(clientProxyMembershipID);
    DistributedMember member = clientProxyMembershipID.getDistributedMember();
    DistributedSystem system = getCache().getDistributedSystem();
    Properties sysProps = system.getProperties();
    String authenticator = sysProps.getProperty(SECURITY_CLIENT_AUTHENTICATOR);
    Object subjectOrPrincipal =
        getSubjectOrPrincipal(clientRegistrationMetadata, member, system, authenticator);
    Subject subject = subjectOrPrincipal instanceof Subject ? (Subject) subjectOrPrincipal : null;

    // Initialize the socket
    socket.setTcpNoDelay(true);
    socket.setSendBufferSize(CacheClientNotifier.socketBufferSize);
    socket.setReceiveBufferSize(CacheClientNotifier.socketBufferSize);

    if (logger.isDebugEnabled()) {
      logger.debug(
          "CacheClientNotifier: Initialized server-to-client socket with send buffer size: {} bytes and receive buffer size: {} bytes",
          socket.getSendBufferSize(), socket.getReceiveBufferSize());
    }

    // Determine whether the client is durable or not.
    byte responseByte = CommunicationMode.SuccessfulServerToClient.getModeNumber();
    boolean successful = true;
    boolean clientIsDurable = clientProxyMembershipID.isDurable();
    if (logger.isDebugEnabled()) {
      if (clientIsDurable) {
        logger.debug("CacheClientNotifier: Attempting to register durable client: {}",
            clientProxyMembershipID.getDurableId());
      } else {
        logger.debug("CacheClientNotifier: Attempting to register non-durable client");
      }
    }

    byte endpointType = 0x00;
    int queueSize = 0;
    String unsuccessfulMsg = null;
    if (clientIsDurable) {
      if (cacheClientProxy == null) {
        if (isTimedOut(clientProxyMembershipID)) {
          queueSize = PoolImpl.PRIMARY_QUEUE_TIMED_OUT;
        } else {
          queueSize = PoolImpl.PRIMARY_QUEUE_NOT_AVAILABLE;
        }
        // No proxy exists for this durable client. It must be created.
        if (logger.isDebugEnabled()) {
          logger.debug(
              "CacheClientNotifier: No proxy exists for durable client with id {}. It must be created.",
              clientProxyMembershipID.getDurableId());
        }
        cacheClientProxy =
            cacheClientProxyFactory.create(this, socket, clientProxyMembershipID, isPrimary,
                clientConflation, clientVersion, acceptorId, notifyBySubscription,
                cache.getSecurityService(), subject, statisticsClock);
        successful = initializeProxy(cacheClientProxy);
      } else {
        cacheClientProxy.setSubject(subject);
        if (cacheClientProxy.isPrimary()) {
          endpointType = (byte) 2;
        } else {
          endpointType = (byte) 1;
        }
        queueSize = cacheClientProxy.getQueueSize();
        // A proxy exists for this durable client. It must be reinitialized.
        if (cacheClientProxy.isPaused()) {
          if (CacheClientProxy.testHook != null) {
            CacheClientProxy.testHook.doTestHook("CLIENT_PRE_RECONNECT");
          }
          if (cacheClientProxy.lockDrain()) {
            try {
              if (logger.isDebugEnabled()) {
                logger.debug(
                    "CacheClientNotifier: A proxy exists for durable client with id {}. This proxy will be reinitialized: {}",
                    clientProxyMembershipID.getDurableId(), cacheClientProxy);
              }
              statistics.incDurableReconnectionCount();
              cacheClientProxy.getProxyID()
                  .updateDurableTimeout(clientProxyMembershipID.getDurableTimeout());
              cacheClientProxy.reinitialize(socket, clientProxyMembershipID, getCache(),
                  isPrimary, clientConflation,
                  clientVersion);
              cacheClientProxy.setMarkerEnqueued(true);
              if (CacheClientProxy.testHook != null) {
                CacheClientProxy.testHook.doTestHook("CLIENT_RECONNECTED");
              }
            } finally {
              cacheClientProxy.unlockDrain();
            }
          } else {
            unsuccessfulMsg =
                "CacheClientNotifier: Connection refused due to cq queue being drained from admin command, please wait...";
            logger.warn(unsuccessfulMsg);
            responseByte = Handshake.REPLY_REFUSED;
            if (CacheClientProxy.testHook != null) {
              CacheClientProxy.testHook.doTestHook("CLIENT_REJECTED_DUE_TO_CQ_BEING_DRAINED");
            }
          }
        } else {
          // The existing proxy is already running (which means that another
          // client is already using this durable id.
          unsuccessfulMsg =
              String.format(
                  "The requested durable client has the same identifier ( %s ) as an existing durable client ( %s ). Duplicate durable clients are not allowed.",
                  clientProxyMembershipID.getDurableId(), cacheClientProxy);
          logger.warn(unsuccessfulMsg);
          // Set the unsuccessful response byte.
          responseByte = Handshake.REPLY_EXCEPTION_DUPLICATE_DURABLE_CLIENT;
        }
      }
    } else {
      CacheClientProxy staleClientProxy = getClientProxy(clientProxyMembershipID);
      if (staleClientProxy != null) {
        // A proxy exists for this non-durable client. It must be closed.
        if (logger.isDebugEnabled()) {
          logger.debug(
              "CacheClientNotifier: A proxy exists for this non-durable client. It must be closed.");
        }
        if (staleClientProxy.startRemoval()) {
          staleClientProxy.waitRemoval();
        } else {
          staleClientProxy.close(false, false); // do not check for queue, just close it
          removeClientProxy(staleClientProxy); // remove old proxy from proxy set
        }
      } // non-null stale proxy

      // Create the new proxy for this non-durable client
      cacheClientProxy =
          new CacheClientProxy(this, socket, clientProxyMembershipID, isPrimary, clientConflation,
              clientVersion, acceptorId, notifyBySubscription, cache.getSecurityService(), subject,
              statisticsClock);
      successful = initializeProxy(cacheClientProxy);
    }

    if (!successful) {
      cacheClientProxy = null;
      responseByte = Handshake.REPLY_REFUSED;
      unsuccessfulMsg =
          String.format(
              "A previous connection attempt from this client is still being processed: %s",
              clientProxyMembershipID);
      logger.warn(unsuccessfulMsg);
    }

    // Tell the client that the proxy has been registered using the response
    // byte. This byte will be read on the client by the CacheClientUpdater to
    // determine whether the registration was successful. The times when
    // registration is unsuccessful currently are if a duplicate durable client
    // is attempted to be registered or authentication fails.
    try {
      DataOutputStream dos =
          new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
      // write the message type, message length and the error message (if any)
      socketMessageWriter.writeHandshakeMessage(dos, responseByte, unsuccessfulMsg, clientVersion,
          endpointType, queueSize);
    } catch (IOException ioe) {// remove the added proxy if we get IOException.
      if (cacheClientProxy != null) {
        // do not check for queue, just close it
        boolean keepProxy = cacheClientProxy.close(false, false);
        if (!keepProxy) {
          removeClientProxy(cacheClientProxy);
        }
      }
      throw ioe;
    }

    if (unsuccessfulMsg != null && logger.isDebugEnabled()) {
      logger.debug(unsuccessfulMsg);
    }

    // If the client is not durable, start its message processor
    // Starting it here (instead of in the CacheClientProxy constructor)
    // will ensure that the response byte is sent to the client before
    // the marker message. If the client is durable, the message processor
    // is not started until the clientReady message is received.
    if (!clientIsDurable && cacheClientProxy != null
        && responseByte == CommunicationMode.SuccessfulServerToClient.getModeNumber()) {
      // The startOrResumeMessageDispatcher tests if the proxy is a primary.
      // If this is a secondary proxy, the dispatcher is not started.
      // The false parameter signifies that a marker message has not already been
      // processed. This will generate and send one.
      cacheClientProxy.startOrResumeMessageDispatcher(false);
    }

    if (responseByte == CommunicationMode.SuccessfulServerToClient.getModeNumber()) {
      if (logger.isDebugEnabled()) {
        logger.debug("CacheClientNotifier: Successfully registered {}", cacheClientProxy);
      }
    } else {
      logger.warn(
          "CacheClientNotifier: Unsuccessfully registered client with identifier {} and response code {}",
          new Object[] {clientProxyMembershipID, responseByte});
    }

    performPostAuthorization(cacheClientProxy, clientProxyMembershipID, member,
        sysProps,
        subjectOrPrincipal);
  }

  private void handleAuthenticationException(final ClientProxyMembershipID clientProxyMembershipID,
      final DataOutputStream dataOutputStream, final Version clientVersion,
      final GemFireSecurityException ex, final byte replyExceptionAuthenticationFailed)
      throws IOException {
    securityLogWriter.warning(
        String.format("An exception was thrown for client [%s]. %s",
            clientProxyMembershipID, ex));
    socketMessageWriter.writeException(dataOutputStream, replyExceptionAuthenticationFailed, ex,
        clientVersion);
  }

  private boolean initializeProxy(CacheClientProxy l_proxy) throws CacheException {
    if (!isProxyInInitializationMode(l_proxy)) {
      if (logger.isDebugEnabled()) {
        logger.debug("Initializing proxy: {}", l_proxy);
      }
      try {
        // Add client proxy to initialization list. This has to be done before
        // the queue is created so that events can be buffered here for delivery
        // to the queue once it's initialized (bug #41681 and others)
        addClientInitProxy(l_proxy);
        l_proxy.initializeMessageDispatcher();
        // Initialization success. Add to client proxy list.
        addClientProxy(l_proxy);
        return true;
      } catch (RegionExistsException ree) {
        if (logger.isDebugEnabled()) {
          String name = ree.getRegion() != null ? ree.getRegion().getFullPath() : "null region";
          logger.debug("Found RegionExistsException while initializing proxy. Region name: {}",
              name);
        }
        // This will return false;
      } finally {
        removeClientInitProxy(l_proxy);
      }
    }
    boolean status = false;
    return status;
  }

  /**
   * Makes Primary to this CacheClientProxy and start the dispatcher of the CacheClientProxy
   *
   * @param isClientReady Whether the marker has already been processed. This value helps determine
   *        whether to start the dispatcher.
   */
  public void makePrimary(ClientProxyMembershipID proxyId, boolean isClientReady) {
    CacheClientProxy proxy = getClientProxy(proxyId);
    if (proxy != null) {
      proxy.setPrimary(true);

      /*
       * If the client represented by this proxy has: - already processed the marker message
       * (meaning the client is failing over to this server as its primary) <or> - is not durable
       * (meaning the marker message is being processed automatically
       *
       * Then, start or resume the dispatcher. Otherwise, let the clientReady message start the
       * dispatcher. See CacheClientProxy.startOrResumeMessageDispatcher if
       * (!proxy._messageDispatcher.isAlive()) {
       */
      if (isClientReady || !proxy.isDurable()) {
        if (logger.isDebugEnabled()) {
          logger.debug("CacheClientNotifier: Notifying proxy to start dispatcher for: {}", proxy);
        }
        proxy.startOrResumeMessageDispatcher(false);
      }
    } else {
      throw new InternalGemFireError("No cache client proxy on this node for proxyId " + proxyId);
    }
  }

  /**
   * Adds or updates entry in the dispatched message map when client sends an ack.
   */
  public boolean processDispatchedMessage(ClientProxyMembershipID proxyId, EventID eid) {
    boolean success = false;
    CacheClientProxy proxy = getClientProxy(proxyId);
    if (proxy != null) {
      HARegionQueue harq = proxy.getHARegionQueue();
      harq.addDispatchedMessage(new ThreadIdentifier(eid.getMembershipID(), eid.getThreadID()),
          eid.getSequenceID());
      success = true;
    }
    return success;
  }

  /**
   * Sets keepalive on the proxy of the given membershipID
   *
   * @param membershipID Uniquely identifies the client pool
   * @since GemFire 5.7
   */
  public void setKeepAlive(ClientProxyMembershipID membershipID, boolean keepAlive) {
    if (logger.isDebugEnabled()) {
      logger.debug("CacheClientNotifier: setKeepAlive client: {}", membershipID);
    }
    CacheClientProxy proxy = getClientProxy(membershipID);
    if (proxy != null) {
      proxy.setKeepAlive(keepAlive);
    }
  }

  /**
   * Unregisters an existing client from this server.
   *
   * @param memberId Uniquely identifies the client
   */
  void unregisterClient(ClientProxyMembershipID memberId, boolean normalShutdown) {
    if (logger.isDebugEnabled()) {
      logger.debug("CacheClientNotifier: Unregistering all clients with member id: {}", memberId);
    }
    CacheClientProxy proxy = getClientProxy(memberId);
    if (proxy != null) {
      final boolean isTraceEnabled = logger.isTraceEnabled();
      if (isTraceEnabled) {
        logger.trace("CacheClientNotifier: Potential client: {}", proxy);
      }

      // If the proxy's member id is the same as the input member id, add
      // it to the set of dead proxies.
      if (!proxy.startRemoval()) {
        if (isTraceEnabled) {
          logger.trace("CacheClientNotifier: Potential client: {} matches {}", proxy, memberId);
        }
        closeDeadProxies(Collections.singletonList(proxy), normalShutdown);
      }
    }
  }

  /**
   * The client represented by the proxyId is ready to receive updates.
   */
  public void readyForEvents(ClientProxyMembershipID proxyId) {
    CacheClientProxy proxy = getClientProxy(proxyId);
    if (proxy != null) {
      // False signifies that a marker message has not already been processed.
      // Generate and send one.
      proxy.startOrResumeMessageDispatcher(false);
    }
  }

  ClientUpdateMessageImpl constructClientMessage(InternalCacheEvent event) {
    ClientUpdateMessageImpl clientMessage = null;
    EnumListenerEvent operation = event.getEventType();

    try {
      clientMessage = initializeMessage(operation, event);
    } catch (Exception e) {
      logger.fatal(String.format(
          "CacheClientNotifier: Cannot notify clients to perform operation %s on event %s",
          operation, event),
          e);
    }
    return clientMessage;
  }

  /**
   * notify interested clients of the given cache event. The event should have routing information
   * in it that determines which clients will receive the event.
   */
  public static void notifyClients(InternalCacheEvent event) {
    CacheClientNotifier instance = ccnSingleton;
    if (instance != null) {
      instance.singletonNotifyClients(event, null);
    }
  }

  public static boolean singletonHasClientProxies() {
    CacheClientNotifier instance = ccnSingleton;
    if (instance != null) {
      return instance.hasClientProxies();
    }
    return false;
  }

  private boolean hasClientProxies() {
    return !this._initClientProxies.isEmpty() || !this._clientProxies.isEmpty();
  }

  /**
   * notify interested clients of the given cache event using the given update message. The event
   * should have routing information in it that determines which clients will receive the event.
   */
  public static void notifyClients(InternalCacheEvent event, ClientUpdateMessage cmsg) {
    CacheClientNotifier instance = ccnSingleton;
    if (instance != null) {
      instance.singletonNotifyClients(event, cmsg);
    }
  }

  private void singletonNotifyClients(InternalCacheEvent event, ClientUpdateMessage cmsg) {
    if (!hasClientProxies()) {
      return;
    }

    FilterInfo filterInfo = event.getLocalFilterInfo();

    if (filterInfo != null) {
      // if the routing was made using an old profile we need to recompute it
      if (logger.isTraceEnabled()) {
        logger.trace("Event isOriginRemote={}", event.isOriginRemote());
      }
    }

    if (filterInfo == null
        || filterInfo.getCQs() == null && filterInfo.getInterestedClients() == null
            && filterInfo.getInterestedClientsInv() == null) {
      return;
    }

    long startTime = statistics.startTime();

    ClientUpdateMessageImpl clientMessage;
    if (cmsg == null) {
      clientMessage = constructClientMessage(event);
    } else {
      clientMessage = (ClientUpdateMessageImpl) cmsg;
    }
    if (clientMessage == null) {
      return;
    }

    FilterProfile regionProfile = ((InternalRegion) event.getRegion()).getFilterProfile();

    Set<ClientProxyMembershipID> filterClients =
        getFilterClientIDs(event, regionProfile, filterInfo, clientMessage);

    Conflatable conflatable;

    if (clientMessage instanceof ClientTombstoneMessage) {
      // HAEventWrapper deserialization can't handle subclasses of ClientUpdateMessageImpl, so don't
      // wrap them
      conflatable = clientMessage;
      // Remove clients older than 70 from the filterClients if the message is
      // ClientTombstoneMessage
      Object[] objects = filterClients.toArray();
      for (Object id : objects) {
        CacheClientProxy ccp = getClientProxy((ClientProxyMembershipID) id, true);
        if (ccp != null && ccp.getVersion().compareTo(Version.GFE_70) < 0) {
          filterClients.remove(id);
        }
      }
    } else {
      HAEventWrapper wrapper = new HAEventWrapper(clientMessage);
      wrapper.incrementPutInProgressCounter("notify clients");
      conflatable = wrapper;
    }
    if (!filterClients.isEmpty()) {
      if (event.getOperation().isEntry()) {
        EntryEventImpl entryEvent = (EntryEventImpl) event;
        entryEvent.exportNewValue(clientMessage);
      }
    }

    clientRegistrationEventQueueManager.add(event, conflatable, filterClients, this);

    singletonRouteClientMessage(conflatable, filterClients);

    statistics.endEvent(startTime);

    // Cleanup destroyed events in CQ result cache.
    // While maintaining the CQ results key caching. the destroy event
    // keys are marked as destroyed instead of removing them, this is
    // to take care, arrival of duplicate events. The key marked as
    // destroyed are removed after the event is placed in clients HAQueue.
    if (filterInfo.filterProcessedLocally) {
      removeDestroyTokensFromCqResultKeys(event, filterInfo);
    }
  }

  Set<ClientProxyMembershipID> getFilterClientIDs(final InternalCacheEvent event,
      final FilterProfile regionProfile,
      final FilterInfo filterInfo,
      final ClientUpdateMessageImpl clientMessage) {
    // Holds the clientIds to which filter message needs to be sent.
    Set<ClientProxyMembershipID> filterClients = new HashSet<>();

    // Add CQ info.
    if (filterInfo.getCQs() != null) {
      for (Map.Entry<Long, Integer> e : filterInfo.getCQs().entrySet()) {
        Long cqID = e.getKey();
        String cqName = regionProfile.getRealCqID(cqID);
        if (cqName == null) {
          continue;
        }
        ServerCQ cq = regionProfile.getCq(cqName);
        if (cq != null) {
          ClientProxyMembershipID id = cq.getClientProxyId();
          filterClients.add(id);
          if (logger.isDebugEnabled()) {
            logger.debug("Adding cq routing info to message for id: {} and cq: {}", id, cqName);
          }

          clientMessage.addClientCq(id, cq.getName(), e.getValue());
        }
      }
    }

    // Add interestList info.
    if (filterInfo.getInterestedClientsInv() != null) {
      Set<Object> rawIDs = regionProfile.getRealClientIDs(filterInfo.getInterestedClientsInv());
      Set<ClientProxyMembershipID> ids = getProxyIDs(rawIDs);
      incMessagesNotQueuedOriginatorStat(event, ids);
      if (!ids.isEmpty()) {
        if (logger.isTraceEnabled()) {
          logger.trace("adding invalidation routing to message for {}", ids);
        }
        clientMessage.addClientInterestList(ids, false);
        filterClients.addAll(ids);
      }
    }
    if (filterInfo.getInterestedClients() != null) {
      Set<Object> rawIDs = regionProfile.getRealClientIDs(filterInfo.getInterestedClients());
      Set<ClientProxyMembershipID> ids = getProxyIDs(rawIDs);
      incMessagesNotQueuedOriginatorStat(event, ids);
      if (!ids.isEmpty()) {
        if (logger.isTraceEnabled()) {
          logger.trace("adding routing to message for {}", ids);
        }
        clientMessage.addClientInterestList(ids, true);
        filterClients.addAll(ids);
      }
    }

    return filterClients;
  }

  private boolean isClientPermitted(ClientRegistrationMetadata clientRegistrationMetadata,
      ClientProxyMembershipID clientProxyMembershipID) throws IOException {
    if (getDenylistedClient().contains(clientProxyMembershipID)) {
      Exception deniedException = new Exception("This client is denylisted by server");
      socketMessageWriter.writeException(clientRegistrationMetadata.getDataOutputStream(),
          Handshake.REPLY_INVALID, deniedException, clientRegistrationMetadata.getClientVersion());
      return false;
    }
    return true;
  }

  private void incMessagesNotQueuedOriginatorStat(final InternalCacheEvent event,
      final Set<ClientProxyMembershipID> ids) {
    // don't send to member of origin
    ClientProxyMembershipID eventOriginator = event.getContext();
    if (eventOriginator != null) {
      if (ids.remove(eventOriginator)) {
        CacheClientProxy ccp = getClientProxy(eventOriginator);
        if (ccp != null) {
          ccp.getStatistics().incMessagesNotQueuedOriginator();
        }
      }
    }
  }

  private void removeDestroyTokensFromCqResultKeys(InternalCacheEvent event,
      FilterInfo filterInfo) {
    FilterProfile regionProfile = ((InternalRegion) event.getRegion()).getFilterProfile();
    if (event.getOperation().isEntry() && filterInfo.getCQs() != null) {
      EntryEvent entryEvent = (EntryEvent) event;
      for (Map.Entry<Long, Integer> e : filterInfo.getCQs().entrySet()) {
        Long cqID = e.getKey();
        String cqName = regionProfile.getRealCqID(cqID);
        if (cqName != null) {
          ServerCQ cq = regionProfile.getCq(cqName);
          if (cq != null && e.getValue().equals(MessageType.LOCAL_DESTROY)) {
            cq.removeFromCqResultKeys(entryEvent.getKey(), true);
          }
        }
      }
    }
  }

  private void performPostAuthorization(final CacheClientProxy proxy,
      final ClientProxyMembershipID clientProxyMembershipID, final DistributedMember member,
      final Properties sysProps, final Object subjectOrPrincipal)
      throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException,
      InvocationTargetException {
    if (proxy != null && subjectOrPrincipal != null) {
      if (subjectOrPrincipal instanceof Principal) {
        Principal principal = (Principal) subjectOrPrincipal;
        if (securityLogWriter.fineEnabled()) {
          securityLogWriter
              .fine("CacheClientNotifier: successfully verified credentials for proxyID: "
                  + clientProxyMembershipID
                  + " having principal: " + principal.getName());
        }

        String postAuthzFactoryName = sysProps.getProperty(SECURITY_CLIENT_ACCESSOR_PP);
        AccessControl authzCallback = null;
        if (postAuthzFactoryName != null && !postAuthzFactoryName.isEmpty()) {
          Method authzMethod = ClassLoadUtil.methodFromName(postAuthzFactoryName);
          authzCallback = (AccessControl) authzMethod.invoke(null, (Object[]) null);
          authzCallback.init(principal, member, getCache());
        }
        proxy.setPostAuthzCallback(authzCallback);
      }
    }
  }

  private Object getSubjectOrPrincipal(final ClientRegistrationMetadata clientRegistrationMetadata,
      final DistributedMember member, final DistributedSystem system, final String authenticator) {
    final Object subjectOrPrincipal;

    if (clientRegistrationMetadata.getClientCredentials() != null) {
      if (securityLogWriter.fineEnabled()) {
        securityLogWriter
            .fine("CacheClientNotifier: verifying credentials for proxyID: "
                + clientRegistrationMetadata.getClientProxyMembershipID());
      }
      subjectOrPrincipal = Handshake
          .verifyCredentials(authenticator,
              clientRegistrationMetadata.getClientCredentials(),
              system.getSecurityProperties(),
              logWriter, securityLogWriter, member,
              cache.getSecurityService());
    } else {
      subjectOrPrincipal = null;
    }

    return subjectOrPrincipal;
  }

  /**
   * delivers the given message to all proxies for routing. The message should already have client
   * interest established, or override the isClientInterested method to implement its own routing
   */
  public static void routeClientMessage(Conflatable clientMessage) {
    CacheClientNotifier instance = ccnSingleton;
    if (instance != null) {
      // ok to use keySet here because all we do is call getClientProxy with these keys
      instance.singletonRouteClientMessage(clientMessage, instance._clientProxies.keySet());
    }
  }

  /**
   * this is for server side registration of client queue
   */
  static void routeSingleClientMessage(ClientUpdateMessage clientMessage,
      ClientProxyMembershipID clientProxyMembershipId) {
    CacheClientNotifier instance = ccnSingleton;
    if (instance != null) {
      instance.singletonRouteClientMessage(clientMessage,
          Collections.singleton(clientProxyMembershipId));
    }
  }

  private void singletonRouteClientMessage(Conflatable conflatable,
      Collection<ClientProxyMembershipID> filterClients) {

    cache.getCancelCriterion().checkCancelInProgress(null);

    List<CacheClientProxy> deadProxies = null;
    for (ClientProxyMembershipID clientId : filterClients) {
      CacheClientProxy proxy;
      proxy = getClientProxy(clientId, true);
      if (proxy != null) {
        if (proxy.isAlive() || proxy.isPaused() || proxy.isConnected() || proxy.isDurable()) {
          proxy.deliverMessage(conflatable);
        } else {
          proxy.getStatistics().incMessagesFailedQueued();
          if (deadProxies == null) {
            deadProxies = new ArrayList<>();
          }
          deadProxies.add(proxy);
        }
        denyListSlowReceiver(proxy);
      }
    }

    if (conflatable instanceof HAEventWrapper) {
      ((HAEventWrapper) conflatable).decrementPutInProgressCounter();
    }

    // Remove any dead clients from the clients to notify
    if (deadProxies != null) {
      closeDeadProxies(deadProxies, false);
    }
  }

  /**
   * processes the given collection of durable and non-durable client identifiers, returning a
   * collection of non-durable identifiers of clients connected to this VM
   */
  Set<ClientProxyMembershipID> getProxyIDs(Set mixedDurableAndNonDurableIDs) {
    Set<ClientProxyMembershipID> result = ConcurrentHashMap.newKeySet();
    for (Object id : mixedDurableAndNonDurableIDs) {
      if (id instanceof String) {
        CacheClientProxy clientProxy = getClientProxy((String) id, true);
        if (clientProxy != null) {
          result.add(clientProxy.getProxyID());
        }
        // else { we don't have a proxy for the given durable ID }
      } else {
        // try to canonicalize the ID.
        CacheClientProxy proxy = getClientProxy((ClientProxyMembershipID) id, true);
        if (proxy != null) {
          result.add(proxy.getProxyID());
        }
      }
    }
    return result;
  }

  private void denyListSlowReceiver(CacheClientProxy clientProxy) {
    final CacheClientProxy proxy = clientProxy;
    if (proxy.getHARegionQueue() != null && proxy.getHARegionQueue().isClientSlowReceiver()
        && !denyListedClients.contains(proxy.getProxyID())) {
      // log alert with client info.
      logger.warn("Client {} is a slow receiver.",
          new Object[] {proxy.getProxyID()});
      addToDenylistedClient(proxy.getProxyID());
      InternalDistributedSystem ids =
          (InternalDistributedSystem) getCache().getDistributedSystem();
      final DistributionManager dm = ids.getDistributionManager();
      dm.getExecutors().getWaitingThreadPool().execute(() -> {

        CacheDistributionAdvisor advisor =
            proxy.getHARegionQueue().getRegion().getCacheDistributionAdvisor();
        Set members = advisor.adviseCacheOp();

        // Send client denylist message
        ClientDenylistProcessor.sendDenylistedClient(proxy.getProxyID(), dm, members);

        // close the proxy for slow receiver.
        proxy.close(false, false);
        removeClientProxy(proxy);

        if (PoolImpl.AFTER_QUEUE_DESTROY_MESSAGE_FLAG) {
          ClientServerObserver bo = ClientServerObserverHolder.getInstance();
          bo.afterQueueDestroyMessage();
        }

        // send remove from denylist.
        RemoveClientFromDenylistMessage rcm = new RemoveClientFromDenylistMessage();
        rcm.setProxyID(proxy.getProxyID());
        dm.putOutgoing(rcm);
        denyListedClients.remove(proxy.getProxyID());
      });
    }
  }

  /**
   * Initializes a {@code ClientUpdateMessage} from an operation and event
   *
   * @param operation The operation that occurred (e.g. AFTER_CREATE)
   * @param event The event containing the data to be updated
   * @return a {@code ClientUpdateMessage}
   */
  private ClientUpdateMessageImpl initializeMessage(EnumListenerEvent operation, CacheEvent event)
      throws Exception {
    if (!supportsOperation(operation)) {
      throw new Exception(
          String.format("The cache client notifier does not support operations of type %s",
              operation));
    }
    Object keyOfInterest = null;
    final EventID eventIdentifier;
    ClientProxyMembershipID membershipID = null;
    boolean isNetLoad = false;
    Object callbackArgument;
    byte[] delta = null;
    VersionTag versionTag = null;

    if (event.getOperation().isEntry()) {
      EntryEventImpl entryEvent = (EntryEventImpl) event;
      versionTag = entryEvent.getVersionTag();
      delta = entryEvent.getDeltaBytes();
      callbackArgument = entryEvent.getRawCallbackArgument();
      if (entryEvent.isBridgeEvent()) {
        membershipID = entryEvent.getContext();
      }
      keyOfInterest = entryEvent.getKey();
      eventIdentifier = entryEvent.getEventId();
      isNetLoad = entryEvent.isNetLoad();
    } else {
      RegionEventImpl regionEvent = (RegionEventImpl) event;
      callbackArgument = regionEvent.getRawCallbackArgument();
      eventIdentifier = regionEvent.getEventId();
      if (event instanceof ClientRegionEventImpl) {
        ClientRegionEventImpl bridgeEvent = (ClientRegionEventImpl) event;
        membershipID = bridgeEvent.getContext();
      }
    }

    // NOTE: If delta is non-null, value MUST be in Object form of type Delta.
    ClientUpdateMessageImpl clientUpdateMsg =
        new ClientUpdateMessageImpl(operation, (LocalRegion) event.getRegion(), keyOfInterest, null,
            delta, (byte) 0x01, callbackArgument, membershipID, eventIdentifier, versionTag);

    if (isNetLoad) {
      clientUpdateMsg.setIsNetLoad(isNetLoad);
    }

    return clientUpdateMsg;
  }

  /**
   * Returns whether the {@code CacheClientNotifier} supports the input operation.
   *
   * @param operation The operation that occurred (e.g. AFTER_CREATE)
   * @return whether the {@code CacheClientNotifier} supports the input operation
   */
  private boolean supportsOperation(EnumListenerEvent operation) {
    return operation == EnumListenerEvent.AFTER_CREATE
        || operation == EnumListenerEvent.AFTER_UPDATE
        || operation == EnumListenerEvent.AFTER_DESTROY
        || operation == EnumListenerEvent.AFTER_INVALIDATE
        || operation == EnumListenerEvent.AFTER_REGION_DESTROY
        || operation == EnumListenerEvent.AFTER_REGION_CLEAR
        || operation == EnumListenerEvent.AFTER_REGION_INVALIDATE;
  }

  /**
   * Registers client interest in the input region and key.
   *
   * @param regionName The name of the region of interest
   * @param keyOfInterest The name of the key of interest
   * @param membershipID clients ID
   * @param interestType type of registration
   * @param isDurable whether the registration persists when client goes away
   * @param sendUpdatesAsInvalidates client wants invalidation messages
   * @param manageEmptyRegions whether to book keep empty region information
   * @param regionDataPolicy (0=empty)
   */
  public void registerClientInterest(String regionName, Object keyOfInterest,
      ClientProxyMembershipID membershipID, int interestType, boolean isDurable,
      boolean sendUpdatesAsInvalidates, boolean manageEmptyRegions, int regionDataPolicy,
      boolean flushState) throws IOException, RegionDestroyedException {

    CacheClientProxy proxy = getClientProxy(membershipID, true);

    if (logger.isDebugEnabled()) {
      logger.debug(
          "CacheClientNotifier: Client {} registering interest in: {} -> {} (an instance of {})",
          proxy, regionName, keyOfInterest, keyOfInterest.getClass().getName());
    }

    if (proxy == null) {
      // client should see this and initiates failover
      throw new IOException(
          "CacheClientProxy for this client is no longer on the server , so registerInterest operation is unsuccessful");
    }

    boolean done = false;
    try {
      proxy.registerClientInterest(regionName, keyOfInterest, interestType, isDurable,
          sendUpdatesAsInvalidates, flushState);

      if (manageEmptyRegions) {
        updateMapOfEmptyRegions(proxy.getRegionsWithEmptyDataPolicy(), regionName,
            regionDataPolicy);
      }

      done = true;
    } finally {
      if (!done) {
        proxy.unregisterClientInterest(regionName, keyOfInterest, interestType, false);
      }
    }
  }

  /**
   * Store region and delta relation
   *
   * @param regionDataPolicy (0==empty)
   * @since GemFire 6.1
   */
  public void updateMapOfEmptyRegions(Map regionsWithEmptyDataPolicy, String regionName,
      int regionDataPolicy) {
    if (regionDataPolicy == 0) {
      if (!regionsWithEmptyDataPolicy.containsKey(regionName)) {
        regionsWithEmptyDataPolicy.put(regionName, 0);
      }
    }
  }

  /**
   * Unregisters client interest in the input region and key.
   *
   * @param regionName The name of the region of interest
   * @param keyOfInterest The name of the key of interest
   * @param isClosing Whether the caller is closing
   * @param membershipID The {@code ClientProxyMembershipID} of the client no longer interested
   *        in this {@code Region} and key
   */
  public void unregisterClientInterest(String regionName, Object keyOfInterest, int interestType,
      boolean isClosing, ClientProxyMembershipID membershipID, boolean keepalive) {
    if (logger.isDebugEnabled()) {
      logger.debug(
          "CacheClientNotifier: Client {} unregistering interest in: {} -> {} (an instance of {})",
          membershipID, regionName, keyOfInterest, keyOfInterest.getClass().getName());
    }
    CacheClientProxy proxy = getClientProxy(membershipID);
    if (proxy != null) {
      proxy.setKeepAlive(keepalive);
      proxy.unregisterClientInterest(regionName, keyOfInterest, interestType, isClosing);
    }
  }

  /**
   * Registers client interest in the input region and list of keys.
   *
   * @param regionName The name of the region of interest
   * @param keysOfInterest The list of keys of interest
   * @param membershipID The {@code ClientProxyMembershipID} of the client no longer interested
   *        in this {@code Region} and key
   */
  public void registerClientInterest(String regionName, List keysOfInterest,
      ClientProxyMembershipID membershipID, boolean isDurable, boolean sendUpdatesAsInvalidates,
      boolean manageEmptyRegions, int regionDataPolicy, boolean flushState)
      throws IOException, RegionDestroyedException {
    CacheClientProxy proxy = getClientProxy(membershipID, true);

    if (logger.isDebugEnabled()) {
      logger.debug("CacheClientNotifier: Client {} registering interest in: {} -> {}", proxy,
          regionName, keysOfInterest);
    }

    if (proxy == null) {
      throw new IOException(
          "CacheClientProxy for this client is no longer on the server , so registerInterest operation is unsuccessful");
    }

    proxy.registerClientInterestList(regionName, keysOfInterest, isDurable,
        sendUpdatesAsInvalidates, flushState);

    if (manageEmptyRegions) {
      updateMapOfEmptyRegions(proxy.getRegionsWithEmptyDataPolicy(), regionName, regionDataPolicy);
    }
  }

  /**
   * Unregisters client interest in the input region and list of keys.
   *
   * @param regionName The name of the region of interest
   * @param keysOfInterest The list of keys of interest
   * @param isClosing Whether the caller is closing
   * @param membershipID The {@code ClientProxyMembershipID} of the client no longer interested
   *        in this {@code Region} and key
   */
  public void unregisterClientInterest(String regionName, List keysOfInterest, boolean isClosing,
      ClientProxyMembershipID membershipID, boolean keepalive) {
    if (logger.isDebugEnabled()) {
      logger.debug("CacheClientNotifier: Client {} unregistering interest in: {} -> {}",
          membershipID, regionName, keysOfInterest);
    }
    CacheClientProxy proxy = getClientProxy(membershipID);
    if (proxy != null) {
      proxy.setKeepAlive(keepalive);
      proxy.unregisterClientInterest(regionName, keysOfInterest, isClosing);
    }
  }

  /**
   * Returns the {@code CacheClientProxy} associated to the membershipID *
   *
   * @return the {@code CacheClientProxy} associated to the membershipID
   */
  public CacheClientProxy getClientProxy(ClientProxyMembershipID membershipID) {
    return (CacheClientProxy) _clientProxies.get(membershipID);
  }

  /**
   * Returns the CacheClientProxy associated to the membershipID. This looks at both proxies that
   * are initialized and those that are still in initialization mode.
   */
  public CacheClientProxy getClientProxy(ClientProxyMembershipID membershipID,
      boolean proxyInInitMode) {
    CacheClientProxy proxy = getClientProxy(membershipID);
    if (proxyInInitMode && proxy == null) {
      proxy = (CacheClientProxy) _initClientProxies.get(membershipID);
    }
    return proxy;
  }

  /**
   * Returns the {@code CacheClientProxy} associated to the durableClientId
   *
   * @return the {@code CacheClientProxy} associated to the durableClientId
   */
  public CacheClientProxy getClientProxy(String durableClientId) {
    return getClientProxy(durableClientId, false);
  }

  /**
   * Returns the {@code CacheClientProxy} associated to the durableClientId. This version of
   * the method can check for initializing proxies as well as fully initialized proxies.
   *
   * @return the {@code CacheClientProxy} associated to the durableClientId
   */
  public CacheClientProxy getClientProxy(String durableClientId, boolean proxyInInitMode) {
    final boolean isDebugEnabled = logger.isDebugEnabled();
    final boolean isTraceEnabled = logger.isTraceEnabled();

    if (isDebugEnabled) {
      logger.debug("CacheClientNotifier: Determining client for {}", durableClientId);
    }
    CacheClientProxy proxy = null;
    for (CacheClientProxy clientProxy : getClientProxies()) {
      if (isTraceEnabled) {
        logger.trace("CacheClientNotifier: Checking client {}", clientProxy);
      }
      if (clientProxy.getDurableId().equals(durableClientId)) {
        proxy = clientProxy;
        if (isDebugEnabled) {
          logger.debug("CacheClientNotifier: {} represents the durable client {}", proxy,
              durableClientId);
        }
        break;
      }
    }
    if (proxy == null && proxyInInitMode) {
      for (Object o : _initClientProxies.values()) {
        CacheClientProxy clientProxy = (CacheClientProxy) o;
        if (isTraceEnabled) {
          logger.trace("CacheClientNotifier: Checking initializing client {}", clientProxy);
        }
        if (clientProxy.getDurableId().equals(durableClientId)) {
          proxy = clientProxy;
          if (isDebugEnabled) {
            logger.debug(
                "CacheClientNotifier: initializing client {} represents the durable client {}",
                proxy, durableClientId);
          }
          break;
        }
      }
    }
    return proxy;
  }

  /**
   * It will remove the clients connected to the passed acceptorId. If its the only server, shuts
   * down this instance.
   */
  public synchronized void shutdown(long acceptorId) {
    final boolean isDebugEnabled = logger.isDebugEnabled();
    if (isDebugEnabled) {
      logger.debug("At cache server shutdown time, the number of cache servers in the cache is {}",
          getCache().getCacheServers().size());
    }

    Iterator it = _clientProxies.values().iterator();
    // Close all the client proxies
    while (it.hasNext()) {
      CacheClientProxy proxy = (CacheClientProxy) it.next();
      if (proxy.getAcceptorId() != acceptorId) {
        continue;
      }
      it.remove();
      try {
        if (isDebugEnabled) {
          logger.debug("CacheClientNotifier: Closing {}", proxy);
        }
        proxy.terminateDispatching(true);
      } catch (Exception ignore) {
        if (isDebugEnabled) {
          logger.debug("{}: Exception in closing down the CacheClientProxy", this, ignore);
        }
      }
    }

    if (noActiveServer() && ccnSingleton != null) {
      ccnSingleton = null;
      if (haContainer != null) {
        haContainer.cleanUp();
        if (isDebugEnabled) {
          logger.debug("haContainer ({}) is now cleaned up.", haContainer.getName());
        }
      }
      clearCompiledQueries();
      denyListedClients.clear();

      // cancel the ping task
      clientPingTask.cancel();

      // Close the statistics
      statistics.close();

      socketCloser.close();
    }
  }

  private boolean noActiveServer() {
    for (CacheServer server : getCache().getCacheServers()) {
      if (server.isRunning()) {
        return false;
      }
    }
    return true;
  }

  /**
   * Adds a new {@code CacheClientProxy} to the list of known client proxies
   *
   * @param proxy The {@code CacheClientProxy} to add
   */
  void addClientProxy(CacheClientProxy proxy) {
    getCache(); // ensure cache reference is up to date so firstclient state is correct
    _clientProxies.put(proxy.getProxyID(), proxy);
    // Remove this proxy from the init proxy list.
    removeClientInitProxy(proxy);
    _connectionListener.queueAdded(proxy.getProxyID());
    if (!(proxy.clientConflation == Handshake.CONFLATION_ON)) {
      // Delta not supported with conflation ON
      ClientHealthMonitor chm = ClientHealthMonitor.getInstance();
      /*
       * If the client connection init starts while cache/member is shutting down,
       * ClientHealthMonitor.getInstance() might return null.
       */
      if (chm != null) {
        chm.numOfClientsPerVersion.incrementAndGet(proxy.getVersion().ordinal());
      }
    }
    timedOutDurableClientProxies.remove(proxy.getProxyID());
  }

  void addClientInitProxy(CacheClientProxy proxy) {
    _initClientProxies.put(proxy.getProxyID(), proxy);
  }

  private void removeClientInitProxy(CacheClientProxy proxy) {
    _initClientProxies.remove(proxy.getProxyID());
  }

  private boolean isProxyInInitializationMode(CacheClientProxy proxy) {
    return _initClientProxies.containsKey(proxy.getProxyID());
  }

  /**
   * Returns (possibly stale) set of memberIds for all clients being actively notified by this
   * server.
   *
   * @return set of memberIds
   */
  public Set getActiveClients() {
    Set clients = new HashSet();
    for (CacheClientProxy proxy : getClientProxies()) {
      if (proxy.hasRegisteredInterested()) {
        ClientProxyMembershipID proxyID = proxy.getProxyID();
        clients.add(proxyID);
      }
    }
    return clients;
  }

  /**
   * Return (possibly stale) list of all clients and their status
   *
   * @return Map, with CacheClientProxy as a key and CacheClientStatus as a value
   */
  public Map getAllClients() {
    Map clients = new HashMap();
    for (Object o : _clientProxies.values()) {
      CacheClientProxy proxy = (CacheClientProxy) o;
      ClientProxyMembershipID proxyID = proxy.getProxyID();
      clients.put(proxyID, new CacheClientStatus(proxyID));
    }
    return clients;
  }

  /**
   * Checks if there is any proxy present for the given durable client
   *
   * @param durableId - id for the durable-client
   * @return - true if a proxy is present for the given durable client
   *
   * @since GemFire 5.6
   */
  public boolean hasDurableClient(String durableId) {
    for (Object o : _clientProxies.values()) {
      CacheClientProxy proxy = (CacheClientProxy) o;
      ClientProxyMembershipID proxyID = proxy.getProxyID();
      if (durableId.equals(proxyID.getDurableId())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Checks if there is any proxy which is primary for the given durable client
   *
   * @param durableId - id for the durable-client
   * @return - true if a primary proxy is present for the given durable client
   *
   * @since GemFire 5.6
   */
  public boolean hasPrimaryForDurableClient(String durableId) {
    for (Object o : _clientProxies.values()) {
      CacheClientProxy proxy = (CacheClientProxy) o;
      ClientProxyMembershipID proxyID = proxy.getProxyID();
      if (durableId.equals(proxyID.getDurableId())) {
        return proxy.isPrimary();
      }
    }
    return false;
  }

  /**
   * Returns (possibly stale) map of queue sizes for all clients notified by this server.
   *
   * @return map with CacheClientProxy as key, and Integer as a value
   */
  public Map getClientQueueSizes() {
    Map/* <ClientProxyMembershipID,Integer> */ queueSizes = new HashMap();
    for (Object o : _clientProxies.values()) {
      CacheClientProxy proxy = (CacheClientProxy) o;
      queueSizes.put(proxy.getProxyID(), proxy.getQueueSize());
    }
    return queueSizes;
  }

  public int getDurableClientHAQueueSize(String durableClientId) {
    CacheClientProxy ccp = getClientProxy(durableClientId);
    if (ccp == null) {
      return -1;
    }
    return ccp.getQueueSizeStat();
  }

  // closes the cq and drains the queue
  public boolean closeClientCq(String durableClientId, String clientCQName) throws CqException {
    CacheClientProxy proxy = getClientProxy(durableClientId);
    // close and drain
    if (proxy != null) {
      return proxy.closeClientCq(clientCQName);
    }
    return false;
  }

  /**
   * Removes an existing {@code CacheClientProxy} from the list of known client proxies
   *
   * @param proxy The {@code CacheClientProxy} to remove
   */
  void removeClientProxy(CacheClientProxy proxy) {
    ClientProxyMembershipID client = proxy.getProxyID();
    _clientProxies.remove(client);
    _connectionListener.queueRemoved();
    getCache().cleanupForClient(this, client);
    if (!(proxy.clientConflation == Handshake.CONFLATION_ON)) {
      ClientHealthMonitor chm = ClientHealthMonitor.getInstance();
      if (chm != null) {
        chm.numOfClientsPerVersion.decrementAndGet(proxy.getVersion().ordinal());
      }
    }
  }

  void durableClientTimedOut(ClientProxyMembershipID client) {
    timedOutDurableClientProxies.add(client);
  }

  private boolean isTimedOut(ClientProxyMembershipID client) {
    return timedOutDurableClientProxies.contains(client);
  }

  /**
   * Returns an unmodifiable Collection of known {@code CacheClientProxy} instances. The
   * collection is not static so its contents may change.
   *
   * @return the collection of known {@code CacheClientProxy} instances
   */
  public Collection<CacheClientProxy> getClientProxies() {
    return Collections.unmodifiableCollection(_clientProxies.values());
  }

  private void closeAllClientCqs(CacheClientProxy proxy) {
    CqService cqService = proxy.getCache().getCqService();
    if (cqService != null) {
      final boolean isDebugEnabled = logger.isDebugEnabled(); // LocalizedMessage.create(
      try {
        if (isDebugEnabled) {
          logger.debug("CacheClientNotifier: Closing client CQs: {}", proxy);
        }
        cqService.closeClientCqs(proxy.getProxyID());
      } catch (CqException e1) {
        logger.warn("Unable to close CQs for the client: {}", proxy.getProxyID());
        if (isDebugEnabled) {
          logger.debug(e1.getMessage(), e1);
        }
      }
    }
  }

  /**
   * Shuts down durable client proxy
   */
  public boolean closeDurableClientProxy(String durableClientId) throws CacheException {
    CacheClientProxy ccp = getClientProxy(durableClientId);
    if (ccp == null) {
      return false;
    }
    // we can probably remove the isPaused check
    if (ccp.isPaused() && !ccp.isConnected()) {
      ccp.setKeepAlive(false);
      closeDeadProxies(Collections.singletonList(ccp), true);
      return true;
    }
    if (logger.isDebugEnabled()) {
      logger.debug("Cannot close running durable client: {}", durableClientId);
    }
    // TODO: never throw an anonymous inner class
    throw new CacheException("Cannot close a running durable client : " + durableClientId) {};
  }

  /**
   * Close dead {@code CacheClientProxy} instances
   *
   * @param deadProxies The list of {@code CacheClientProxy} instances to close
   */
  private void closeDeadProxies(List deadProxies, boolean stoppedNormally) {
    final boolean isDebugEnabled = logger.isDebugEnabled();
    for (Object deadProxy : deadProxies) {
      CacheClientProxy proxy = (CacheClientProxy) deadProxy;
      if (isDebugEnabled) {
        logger.debug("CacheClientNotifier: Closing dead client: {}", proxy);
      }

      // Close the proxy
      boolean keepProxy = false;
      try {
        keepProxy = proxy.close(false, stoppedNormally);
      } catch (CancelException e) {
        throw e;
      } catch (Exception e) {
        logger.warn("CacheClientNotifier: Caught exception attempting to close client: {}", proxy,
            e);
      }

      // Remove the proxy if necessary. It might not be necessary to remove the proxy if it is
      // durable.
      if (keepProxy) {
        logger.info(
            "CacheClientNotifier: Keeping proxy for durable client named {} for {} seconds {}.",
            proxy.getDurableId(), proxy.getDurableTimeout(), proxy);
      } else {
        closeAllClientCqs(proxy);
        if (isDebugEnabled) {
          logger.debug("CacheClientNotifier: Not keeping proxy for non-durable client: {}", proxy);
        }
        removeClientProxy(proxy);
      }
      proxy.notifyRemoval();
    } // for
  }

  /**
   * Registers a new {@code InterestRegistrationListener} with the set of
   * {@code InterestRegistrationListener}s.
   *
   * @param listener The {@code InterestRegistrationListener} to register
   *
   * @since GemFire 5.8Beta
   */
  public void registerInterestRegistrationListener(InterestRegistrationListener listener) {
    writableInterestRegistrationListeners.add(listener);
  }

  /**
   * Unregisters an existing {@code InterestRegistrationListener} from the set of
   * {@code InterestRegistrationListener}s.
   *
   * @param listener The {@code InterestRegistrationListener} to unregister
   *
   * @since GemFire 5.8Beta
   */
  public void unregisterInterestRegistrationListener(InterestRegistrationListener listener) {
    writableInterestRegistrationListeners.remove(listener);
  }

  /**
   * Returns a read-only collection of {@code InterestRegistrationListener}s registered with
   * this notifier.
   *
   * @return a read-only collection of {@code InterestRegistrationListener}s registered with
   *         this notifier
   *
   * @since GemFire 5.8Beta
   */
  public Set getInterestRegistrationListeners() {
    return readableInterestRegistrationListeners;
  }

  /**
   * @since GemFire 5.8Beta
   */
  boolean containsInterestRegistrationListeners() {
    return !writableInterestRegistrationListeners.isEmpty();
  }

  /**
   * @since GemFire 5.8Beta
   */
  void notifyInterestRegistrationListeners(InterestRegistrationEvent event) {
    for (Object writableInterestRegistrationListener : writableInterestRegistrationListeners) {
      InterestRegistrationListener listener =
          (InterestRegistrationListener) writableInterestRegistrationListener;
      if (event.isRegister()) {
        listener.afterRegisterInterest(event);
      } else {
        listener.afterUnregisterInterest(event);
      }
    }
  }

  /**
   * Test method used to determine the state of the CacheClientNotifier
   *
   * @return the statistics for the notifier
   */
  public CacheClientNotifierStats getStats() {
    return statistics;
  }

  /**
   * Returns this {@code CacheClientNotifier}'s {@code InternalCache}.
   */
  public InternalCache getCache() {
    if (cache != null && cache.isClosed()) {
      InternalCache cache = GemFireCacheImpl.getInstance();
      if (cache != null) {
        this.cache = cache;
        logWriter = cache.getInternalLogWriter();
        securityLogWriter = cache.getSecurityInternalLogWriter();
      }
    }
    return cache;
  }

  /**
   * Returns this {@code CacheClientNotifier}'s maximum message count.
   *
   * @return this {@code CacheClientNotifier}'s maximum message count
   */
  protected int getMaximumMessageCount() {
    return maximumMessageCount;
  }

  /**
   * Returns this {@code CacheClientNotifier}'s message time-to-live.
   *
   * @return this {@code CacheClientNotifier}'s message time-to-live
   */
  protected int getMessageTimeToLive() {
    return messageTimeToLive;
  }

  protected void handleInterestEvent(InterestRegistrationEvent event) {
    LocalRegion region = (LocalRegion) event.getRegion();
    region.handleInterestEvent(event);
  }

  /**
   * @param cache The GemFire {@code InternalCache}
   * @param listener a listener which should receive notifications abouts queues being added or
   */
  private CacheClientNotifier(InternalCache cache,
      ClientRegistrationEventQueueManager clientRegistrationEventQueueManager,
      StatisticsClock statisticsClock,
      CacheServerStats acceptorStats,
      int maximumMessageCount,
      int messageTimeToLive,
      ConnectionListener listener,
      boolean isGatewayReceiver,
      CacheClientProxyFactory cacheClientProxyFactory) {
    this.cacheClientProxyFactory = cacheClientProxyFactory;
    // Set the Cache
    setCache(cache);
    this.clientRegistrationEventQueueManager = clientRegistrationEventQueueManager;
    this.statisticsClock = statisticsClock;
    this.acceptorStats = acceptorStats;
    // we only need one thread per client and wait 50ms for close
    socketCloser = new SocketCloser(1, 50);

    // Set the LogWriter
    logWriter = (InternalLogWriter) cache.getLogger();

    _connectionListener = listener;

    // Set the security LogWriter
    securityLogWriter = (InternalLogWriter) cache.getSecurityLogger();

    this.maximumMessageCount = maximumMessageCount;
    this.messageTimeToLive = messageTimeToLive;

    // Initialize the statistics
    StatisticsFactory factory;
    if (isGatewayReceiver) {
      factory = new DummyStatisticsFactory();
    } else {
      factory = getCache().getInternalDistributedSystem().getStatisticsManager();
    }
    statistics = new CacheClientNotifierStats(factory);

    try {
      logFrequency = Long.valueOf(System.getProperty(MAX_QUEUE_LOG_FREQUENCY));
      if (logFrequency <= 0) {
        logFrequency = DEFAULT_LOG_FREQUENCY;
      }
    } catch (Exception e) {
      logFrequency = DEFAULT_LOG_FREQUENCY;
    }

    eventEnqueueWaitTime =
        Integer.getInteger(EVENT_ENQUEUE_WAIT_TIME_NAME, DEFAULT_EVENT_ENQUEUE_WAIT_TIME);
    if (eventEnqueueWaitTime < 0) {
      eventEnqueueWaitTime = DEFAULT_EVENT_ENQUEUE_WAIT_TIME;
    }

    // Schedule task to periodically ping clients.
    scheduleClientPingTask();
  }

  void deliverInterestChange(ClientProxyMembershipID proxyID,
      ClientInterestMessageImpl message) {
    DistributionManager dm = ((InternalDistributedSystem) getCache().getDistributedSystem())
        .getDistributionManager();
    ServerInterestRegistrationMessage.sendInterestChange(dm, proxyID, message);
  }

  CacheServerStats getAcceptorStats() {
    return acceptorStats;
  }

  SocketCloser getSocketCloser() {
    return socketCloser;
  }

  public void addCompiledQuery(DefaultQuery query) {
    if (compiledQueries.putIfAbsent(query.getQueryString(), query) == null) {
      // Added successfully.
      statistics.incCompiledQueryCount(1);
      if (logger.isDebugEnabled()) {
        logger.debug(
            "Added compiled query into ccn.compliedQueries list. Query: {}. Total compiled queries: {}",
            query.getQueryString(), statistics.getCompiledQueryCount());
      }
      // Start the clearIdleCompiledQueries thread.
      startCompiledQueryCleanupThread();
    }
  }

  public Query getCompiledQuery(String queryString) {
    return compiledQueries.get(queryString);
  }

  private void clearCompiledQueries() {
    if (!compiledQueries.isEmpty()) {
      statistics.incCompiledQueryCount(-compiledQueries.size());
      compiledQueries.clear();
      if (logger.isDebugEnabled()) {
        logger.debug(
            "Removed all compiled queries from ccn.compliedQueries list. Total compiled queries: {}",
            statistics.getCompiledQueryCount());
      }
    }
  }

  /**
   * This starts the cleanup thread that periodically (DefaultQuery.TEST_COMPILED_QUERY_CLEAR_TIME)
   * checks for the compiled queries that are not used and removes them.
   */
  private void startCompiledQueryCleanupThread() {
    if (isCompiledQueryCleanupThreadStarted) {
      return;
    }

    SystemTimer.SystemTimerTask task = new SystemTimer.SystemTimerTask() {
      @Override
      public void run2() {
        final boolean isDebugEnabled = logger.isDebugEnabled();
        for (Map.Entry<String, DefaultQuery> e : compiledQueries.entrySet()) {
          DefaultQuery q = e.getValue();
          // Check if the query last used flag.
          // If its true set it to false. If its false it means it is not used
          // from the its last checked.
          if (q.getLastUsed()) {
            q.setLastUsed(false);
          } else {
            if (compiledQueries.remove(e.getKey()) != null) {
              // If successfully removed decrement the counter.
              statistics.incCompiledQueryCount(-1);
              if (isDebugEnabled) {
                logger.debug("Removed compiled query from ccn.compliedQueries list. Query: "
                    + q.getQueryString() + ". Total compiled queries are : "
                    + statistics.getCompiledQueryCount());
              }
            }
          }
        }
      }
    };

    synchronized (lockIsCompiledQueryCleanupThreadStarted) {
      if (!isCompiledQueryCleanupThreadStarted) {
        long period = DefaultQuery.TEST_COMPILED_QUERY_CLEAR_TIME > 0
            ? DefaultQuery.TEST_COMPILED_QUERY_CLEAR_TIME : DefaultQuery.COMPILED_QUERY_CLEAR_TIME;
        cache.getCCPTimer().scheduleAtFixedRate(task, period, period);
      }
      isCompiledQueryCleanupThreadStarted = true;
    }
  }

  void scheduleClientPingTask() {
    clientPingTask = new SystemTimer.SystemTimerTask() {

      @Override
      public void run2() {
        // If there are no proxies, return
        if (_clientProxies.isEmpty()) {
          return;
        }

        // Create ping message
        ClientMessage message = new ClientPingMessageImpl();

        // Determine clients to ping
        for (CacheClientProxy proxy : getClientProxies()) {
          logger.debug("Checking whether to ping {}", proxy);
          // Ping clients whose version is GE 6.6.2.2
          if (proxy.getVersion().compareTo(Version.GFE_6622) >= 0) {
            // Send the ping message directly to the client. Do not qo through
            // the queue. If the queue were used, the secondary connection would
            // not be pinged. Instead, pings would just build up in secondary
            // queue and never be sent. The counter is used to help scalability.
            // If normal messages are sent by the proxy, then the counter will
            // be reset and no pings will be sent.
            if (proxy.incrementAndGetPingCounter() >= CLIENT_PING_TASK_COUNTER) {
              logger.debug("Pinging {}", proxy);
              proxy.sendMessageDirectly(message);
              logger.debug("Done pinging {}", proxy);
            } else {
              logger.debug("Not pinging because not idle: {}", proxy);
            }
          } else {
            logger.debug("Ignoring because of version: {}", proxy);
          }
        }
      }
    };

    if (logger.isDebugEnabled()) {
      logger.debug("Scheduling client ping task with period={} ms", CLIENT_PING_TASK_PERIOD);
    }
    cache.getCCPTimer().scheduleAtFixedRate(clientPingTask,
        CLIENT_PING_TASK_PERIOD, CLIENT_PING_TASK_PERIOD);
  }

  /**
   * The map of known {@code CacheClientProxy} instances. Maps ClientProxyMembershipID to
   * CacheClientProxy. Note that the keys in this map are not updated when a durable client
   * reconnects. To make sure you get the updated ClientProxyMembershipID use this map to lookup the
   * CacheClientProxy and then call getProxyID on it.
   */
  private final ConcurrentMap/* <ClientProxyMembershipID, CacheClientProxy> */ _clientProxies =
      new ConcurrentHashMap();

  /**
   * The map of {@code CacheClientProxy} instances which are getting initialized. Maps
   * ClientProxyMembershipID to CacheClientProxy.
   */
  private final ConcurrentMap/* <ClientProxyMembershipID, CacheClientProxy> */ _initClientProxies =
      new ConcurrentHashMap();

  private final Set<ClientProxyMembershipID> timedOutDurableClientProxies =
      new HashSet<>();

  /**
   * The GemFire {@code InternalCache}. Note that since this is a singleton class you should
   * not use a direct reference to cache in CacheClientNotifier code. Instead, you should always use
   * {@code getCache()}
   */
  private InternalCache cache;

  private InternalLogWriter logWriter;

  /**
   * The GemFire security {@code LogWriter}
   */
  private InternalLogWriter securityLogWriter;

  /** the maximum number of messages that can be enqueued in a client-queue. */
  private final int maximumMessageCount;

  /**
   * the time (in seconds) after which a message in the client queue will expire.
   */
  private final int messageTimeToLive;

  /**
   * A listener which receives notifications about queues that are added or removed
   */
  private final ConnectionListener _connectionListener;

  private final StatisticsClock statisticsClock;

  private final CacheServerStats acceptorStats;

  /**
   * haContainer can hold either the name of the client-messages-region (in case of eviction
   * policies "mem" or "entry") or an instance of HashMap (in case of eviction policy "none"). In
   * both the cases, it'll store HAEventWrapper as its key and ClientUpdateMessage as its value.
   */
  private volatile HAContainerWrapper haContainer;

  /**
   * The size of the server-to-client communication socket buffers. This can be modified using the
   * BridgeServer.SOCKET_BUFFER_SIZE system property.
   */
  private static final int socketBufferSize =
      Integer.getInteger("BridgeServer.SOCKET_BUFFER_SIZE", 32768);

  /**
   * The statistics for this notifier
   */
  protected final CacheClientNotifierStats statistics;

  /**
   * The {@code InterestRegistrationListener} instances registered in this VM. This is used
   * when modifying the set of listeners.
   */
  private final Set writableInterestRegistrationListeners = new CopyOnWriteArraySet();

  /**
   * The {@code InterestRegistrationListener} instances registered in this VM. This is used to
   * provide a read-only {@code Set} of listeners.
   */
  private final Set readableInterestRegistrationListeners =
      Collections.unmodifiableSet(writableInterestRegistrationListeners);

  /**
   * System property name for indicating how much frequently the "Queue full" message should be
   * logged.
   */
  private static final String MAX_QUEUE_LOG_FREQUENCY =
      GeodeGlossary.GEMFIRE_PREFIX + "logFrequency.clientQueueReachedMaxLimit";

  public static final long DEFAULT_LOG_FREQUENCY = 1000;

  private static final String EVENT_ENQUEUE_WAIT_TIME_NAME =
      GeodeGlossary.GEMFIRE_PREFIX + "subscription.EVENT_ENQUEUE_WAIT_TIME";

  private static final int DEFAULT_EVENT_ENQUEUE_WAIT_TIME = 100;

  /**
   * System property value denoting the time in milliseconds. Any thread putting an event into a
   * subscription queue, which is full, will wait this much time for the queue to make space. It'll
   * then enque the event possibly causing the queue to grow beyond its capacity/max-size. See
   * #51400.
   */
  @MakeNotStatic
  public static int eventEnqueueWaitTime;

  /**
   * The frequency of logging the "Queue full" message.
   */
  private long logFrequency;

  private final ConcurrentHashMap<String, DefaultQuery> compiledQueries =
      new ConcurrentHashMap<>();

  private volatile boolean isCompiledQueryCleanupThreadStarted = false;

  private final Object lockIsCompiledQueryCleanupThreadStarted = new Object();

  private SystemTimer.SystemTimerTask clientPingTask;

  private final SocketCloser socketCloser;

  private static final int CLIENT_PING_TASK_PERIOD =
      Integer.getInteger(GeodeGlossary.GEMFIRE_PREFIX + "serverToClientPingPeriod", 60000);

  private static final long CLIENT_PING_TASK_COUNTER =
      Long.getLong(GeodeGlossary.GEMFIRE_PREFIX + "serverToClientPingCounter", 3);

  public long getLogFrequency() {
    return logFrequency;
  }

  /** returns the interval between "ping" messages sent to clients on idle connections */
  static int getClientPingInterval() {
    return CLIENT_PING_TASK_PERIOD;
  }

  /**
   * @return the haContainer
   */
  public Map getHaContainer() {
    return haContainer;
  }

  private void initHaContainer(OverflowAttributes overflowAttributes) {
    // lazily initialize haContainer in case this CCN instance was created by a gateway receiver
    if (overflowAttributes != null
        && !HARegionQueue.HA_EVICTION_POLICY_NONE.equals(overflowAttributes.getEvictionPolicy())) {
      haContainer = new HAContainerRegion(cache.getRegion(Region.SEPARATOR
          + CacheServerImpl.clientMessagesRegion(
              cache,
              overflowAttributes.getEvictionPolicy(),
              overflowAttributes.getQueueCapacity(),
              overflowAttributes.getPort(),
              overflowAttributes.isDiskStore() ? overflowAttributes.getDiskStoreName()
                  : overflowAttributes.getOverflowDirectory(),
              overflowAttributes.isDiskStore())));
    } else {
      haContainer = new HAContainerMap(new ConcurrentHashMap());
    }
    assert haContainer != null;

    if (logger.isDebugEnabled()) {
      logger.debug("ha container ({}) has been created.", haContainer.getName());
    }
  }

  private final Set denyListedClients = new CopyOnWriteArraySet();

  void addToDenylistedClient(ClientProxyMembershipID proxyID) {
    denyListedClients.add(proxyID);
    // ensure that cache and distributed system state are current and open
    getCache();
    new ScheduledThreadPoolExecutor(1).schedule(new ExpireDenyListTask(proxyID), 120,
        TimeUnit.SECONDS);
  }

  Set getDenylistedClient() {
    return denyListedClients;
  }

  /**
   * @param _cache the cache to set
   */
  private void setCache(InternalCache _cache) {
    cache = _cache;
  }

  private class ExpireDenyListTask extends PoolTask {

    private final ClientProxyMembershipID proxyID;

    ExpireDenyListTask(ClientProxyMembershipID proxyID) {
      this.proxyID = proxyID;
    }

    @Override
    public void run2() {
      if (denyListedClients.remove(proxyID)) {
        if (logger.isDebugEnabled()) {
          logger.debug("{} client is no longer denylisted", proxyID);
        }
      }
    }
  }

  @VisibleForTesting
  public static CacheClientNotifierProvider singletonProvider() {
    return CacheClientNotifier::getInstance;
  }

  @VisibleForTesting
  public static Supplier<CacheClientNotifier> singletonGetter() {
    return CacheClientNotifier::getInstance;
  }

  @FunctionalInterface
  @VisibleForTesting
  public interface CacheClientNotifierProvider {
    CacheClientNotifier get(InternalCache cache,
        ClientRegistrationEventQueueManager clientRegistrationEventQueueManager,
        StatisticsClock statisticsClock,
        CacheServerStats acceptorStats, int maximumMessageCount, int messageTimeToLive,
        ConnectionListener listener, OverflowAttributes overflowAttributes,
        boolean isGatewayReceiver);
  }

  @VisibleForTesting
  public void addClientProxyToMap(CacheClientProxy proxy) {
    _clientProxies.put(proxy.getProxyID(), proxy);
  }
}
