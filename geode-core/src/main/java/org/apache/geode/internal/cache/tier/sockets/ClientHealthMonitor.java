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


import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.SystemFailure;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.CacheClientStatus;
import org.apache.geode.internal.cache.IncomingGatewayStatus;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.TXId;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.tier.Acceptor;
import org.apache.geode.internal.cache.tier.ServerSideHandshake;
import org.apache.geode.internal.lang.utils.JavaWorkarounds;
import org.apache.geode.logging.internal.executors.LoggingThread;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Class <code>ClientHealthMonitor</code> is a server-side singleton that monitors the health of
 * clients by looking at their heartbeats. If too much time elapses between heartbeats, the monitor
 * determines that the client is dead and interrupts its threads.
 *
 * @since GemFire 4.2.3
 */
public class ClientHealthMonitor {
  private static final Logger logger = LogService.getLogger();
  public static final String CLIENT_HEALTH_MONITOR_INTERVAL_PROPERTY =
      "geode.client-health-monitor-interval";

  /**
   * The map of known clients and last time seen.
   */
  private final ConcurrentMap<ClientProxyMembershipID, AtomicLong> clientHeartbeats =
      new ConcurrentHashMap<>();

  /**
   * The map of known clients and maximum time between pings.
   */
  private final ConcurrentMap<ClientProxyMembershipID, Integer> clientMaximumTimeBetweenPings =
      new ConcurrentHashMap<>();

  /**
   * THe GemFire <code>Cache</code>
   */
  private final InternalCache cache;

  public int getMaximumTimeBetweenPings() {
    return maximumTimeBetweenPings;
  }

  private final int maximumTimeBetweenPings;

  /**
   * A thread that validates client connections
   */
  private final ClientHealthMonitorThread clientMonitor;

  /**
   * The singleton <code>CacheClientNotifier</code> instance
   */
  @MakeNotStatic
  private static ClientHealthMonitor instance;

  /**
   * Reference count in the event that multiple cache servers are using the health monitor
   */

  @MakeNotStatic
  private static int refCount = 0;

  /**
   * The interval between client monitor iterations
   */
  private static final long DEFAULT_CLIENT_MONITOR_INTERVAL_IN_MILLIS = 1000;

  private final CacheClientNotifierStats stats;

  /**
   * Used to track the number of handshakes in a VM primary use, license enforcement.
   *
   * note, these were moved from static fields in ServerConnection so that they will be cleaned up
   * when the client health monitor is shutdown.
   */
  private final HashMap<ServerSideHandshake, MutableInt> cleanupTable = new HashMap<>();

  private final HashMap<ClientProxyMembershipID, MutableInt> cleanupProxyIdTable = new HashMap<>();

  /**
   * Used to track the connections for a particular client
   */
  private final HashMap<ClientProxyMembershipID, ServerConnectionCollection> proxyIdConnections =
      new HashMap<>();

  /**
   * Gives the number of clients connected to the cache servers in this cache, which
   * are capable of processing received deltas.
   *
   * @see CacheClientNotifier#addClientProxy(CacheClientProxy)
   */
  AtomicInteger numberOfClientsWithConflationOff = new AtomicInteger(0);

  public long getMonitorInterval() {
    return monitorInterval;
  }

  private final long monitorInterval;

  /**
   * Factory method to construct or return the singleton <code>ClientHealthMonitor</code> instance.
   *
   * @param cache The GemFire <code>Cache</code>
   * @param maximumTimeBetweenPings The maximum time allowed between pings before determining the
   *        client has died and interrupting its sockets.
   * @return The singleton <code>ClientHealthMonitor</code> instance
   */
  public static ClientHealthMonitor getInstance(InternalCache cache, int maximumTimeBetweenPings,
      CacheClientNotifierStats stats) {
    createInstance(cache, maximumTimeBetweenPings, stats);
    return instance;
  }

  /**
   * Factory method to return the singleton <code>ClientHealthMonitor</code> instance.
   *
   * @return the singleton <code>ClientHealthMonitor</code> instance
   */
  public static ClientHealthMonitor getInstance() {
    return instance;
  }

  /**
   * Shuts down the singleton <code>ClientHealthMonitor</code> instance.
   */
  public static synchronized void shutdownInstance() {
    refCount--;
    if (instance == null)
      return;
    if (refCount > 0)
      return;
    instance.shutdown();

    boolean interrupted = false; // Don't clear, let join fail if already interrupted
    try {
      if (instance.clientMonitor != null) {
        instance.clientMonitor.join();
      }
    } catch (InterruptedException e) {
      interrupted = true;
      if (logger.isDebugEnabled()) {
        logger.debug(":Interrupted joining with the ClientHealthMonitor Thread", e);
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
    instance = null;
    refCount = 0;
  }

  public void registerClient(ClientProxyMembershipID proxyID) {
    registerClient(proxyID, maximumTimeBetweenPings);
  }

  /**
   * Registers a new client to be monitored.
   *
   * @param proxyID The id of the client to be registered
   */
  public void registerClient(ClientProxyMembershipID proxyID, int maximumTimeBetweenPings) {
    if (!clientMaximumTimeBetweenPings.containsKey(proxyID)) {
      clientMaximumTimeBetweenPings.putIfAbsent(proxyID, maximumTimeBetweenPings);
    }
    if (!clientHeartbeats.containsKey(proxyID)) {
      if (null == clientHeartbeats.putIfAbsent(proxyID,
          new AtomicLong(System.currentTimeMillis()))) {
        // don't do this in computeIfAbsent because segment is locked while logging/stats
        if (stats != null) {
          stats.incClientRegisterRequests();
        }
        if (logger.isDebugEnabled()) {
          logger.debug("ClientHealthMonitor: Registering client with member id {}", proxyID);
        }
      }
    }
  }

  /**
   * Takes care of unregistering from the _clientHeartBeats map.
   *
   * @param proxyID The id of the client to be unregistered
   */
  private void unregisterClient(ClientProxyMembershipID proxyID, boolean clientDisconnectedCleanly,
      Throwable clientDisconnectException) {
    if (clientHeartbeats.remove(proxyID) != null) {
      if (clientDisconnectedCleanly) {
        if (logger.isDebugEnabled()) {
          logger.debug("ClientHealthMonitor: Unregistering client with member id {}", proxyID);
        }
      } else {
        logger.warn("ClientHealthMonitor: Unregistering client with member id {} due to: {}",
            proxyID, clientDisconnectException == null ? "Unknown reason"
                : clientDisconnectException.getLocalizedMessage());
      }
      if (stats != null) {
        stats.incClientUnRegisterRequests();
      }
      expireTXStates(proxyID);
    }
    clientMaximumTimeBetweenPings.remove(proxyID);
  }

  /**
   * Unregisters a client to be monitored.
   *
   * @param proxyID The id of the client to be unregistered
   * @param acceptor non-null if the call is from a <code>ServerConnection</code> (as opposed to a
   *        <code>CacheClientProxy</code>).
   * @param clientDisconnectedCleanly Whether the client disconnected cleanly or crashed
   */
  void unregisterClient(ClientProxyMembershipID proxyID, Acceptor acceptor,
      boolean clientDisconnectedCleanly, Throwable clientDisconnectException) {
    unregisterClient(proxyID, clientDisconnectedCleanly, clientDisconnectException);
    // Unregister any CacheClientProxy instances associated with this member id
    // if this method was invoked from a ServerConnection and the client did
    // not disconnect cleanly.
    if (acceptor != null) {
      CacheClientNotifier ccn = acceptor.getCacheClientNotifier();
      if (ccn != null) {
        try {
          ccn.unregisterClient(proxyID, clientDisconnectedCleanly);
        } catch (CancelException ignore) {
        }
      }
    }
  }

  /**
   * provide a test hook to track client transactions to be removed
   */
  @SuppressWarnings("unused") // do not delete
  public Set<TXId> getScheduledToBeRemovedTx() {
    final TXManagerImpl txMgr = (TXManagerImpl) cache.getCacheTransactionManager();
    return txMgr.getScheduledToBeRemovedTx();
  }

  /**
   * expire the transaction states for the given client. This uses the transactionTimeToLive setting
   * that is inherited from the TXManagerImpl. If that setting is non-positive we expire the states
   * immediately
   *
   */
  private void expireTXStates(ClientProxyMembershipID proxyID) {
    if (cache.isClosed()) {
      return;
    }

    final TXManagerImpl txMgr = (TXManagerImpl) cache.getCacheTransactionManager();
    if (null == txMgr) {
      return;
    }

    final Set<TXId> txIds =
        txMgr.getTransactionsForClient((InternalDistributedMember) proxyID.getDistributedMember());
    if (!txIds.isEmpty()) {
      txMgr.expireDisconnectedClientTransactions(txIds, true);
    }
  }

  public void removeAllConnectionsAndUnregisterClient(ClientProxyMembershipID proxyID,
      Throwable t) {
    // Remove all connections
    cleanupClientThreads(proxyID, false);

    unregisterClient(proxyID, false, t);
  }

  /**
   * Adds a <code>ServerConnection</code> to the client's processing threads
   *
   * @param proxyID The membership id of the client to be updated
   * @param connection The thread processing client requests
   */
  public ServerConnectionCollection addConnection(ClientProxyMembershipID proxyID,
      ServerConnection connection) {
    synchronized (proxyIdConnections) {
      ServerConnectionCollection collection = getProxyIdCollection(proxyID);
      collection.addConnection(connection);
      return collection;
    }
  }

  /**
   * Removes a <code>ServerConnection</code> from the client's processing threads
   *
   * @param proxyID The id of the client to be updated
   * @param connection The thread processing client requests
   */
  void removeConnection(ClientProxyMembershipID proxyID, ServerConnection connection) {
    synchronized (proxyIdConnections) {
      ServerConnectionCollection collection = proxyIdConnections.get(proxyID);
      if (collection != null) {
        collection.removeConnection(connection);
        if (collection.getConnections().isEmpty()) {
          proxyIdConnections.remove(proxyID);
        }
      }
    }
  }

  /**
   * Processes a received ping for a client.
   *
   * @param proxyID The id of the client from which the ping was received
   */
  public void receivedPing(ClientProxyMembershipID proxyID) {
    if (clientMonitor == null) {
      return;
    }

    if (logger.isTraceEnabled()) {
      logger.trace("ClientHealthMonitor: Received ping from client with member id {}", proxyID);
    }

    AtomicLong heartbeat = clientHeartbeats.get(proxyID);
    if (null == heartbeat) {
      registerClient(proxyID, getMaximumTimeBetweenPings(proxyID));
    } else {
      heartbeat.set(System.currentTimeMillis());
    }
  }

  /**
   * Returns modifiable map (changes do not effect this class) of client membershipID to connection
   * count. This is different from the map contained in this class as here the key is client
   * membershipID & not the the proxyID. It is to be noted that a given client can have multiple
   * proxies.
   *
   * @param filterProxies Set identifying the Connection proxies which should be fetched. These
   *        ConnectionProxies may be from same client member or different. If it is null this would
   *        mean to fetch the Connections of all the ConnectionProxy objects.
   */
  public Map<String, Object[]> getConnectedClients(Set<ClientProxyMembershipID> filterProxies) {
    Map<String, Object[]> map = new HashMap<>(); // KEY=proxyID, VALUE=connectionCount (Integer)
    synchronized (proxyIdConnections) {
      for (Map.Entry<ClientProxyMembershipID, ServerConnectionCollection> entry : proxyIdConnections
          .entrySet()) {
        // proxyID includes FQDN
        ClientProxyMembershipID proxyID = entry.getKey();
        if (filterProxies == null || filterProxies.contains(proxyID)) {
          String membershipID = null;
          Set<ServerConnection> connections = entry.getValue().getConnections();
          int socketPort = 0;
          InetAddress socketAddress = null;
          // Get data from one.
          for (ServerConnection sc : connections) {
            socketPort = sc.getSocketPort();
            socketAddress = sc.getSocketAddress();
            membershipID = sc.getMembershipID();
            break;
          }
          int connectionCount = connections.size();
          String clientString;
          if (socketAddress == null) {
            clientString = "client member id=" + membershipID;
          } else {
            clientString = "host name=" + socketAddress.toString() + " host ip="
                + socketAddress.getHostAddress() + " client port=" + socketPort
                + " client member id=" + membershipID;
          }
          Object[] data = map.get(membershipID);
          if (data == null) {
            map.put(membershipID, new Object[] {clientString, connectionCount});
          } else {
            data[1] = (Integer) data[1] + connectionCount;
          }
        }
      }

    }
    return map;
  }

  /**
   * This method returns the CacheClientStatus for all the clients that are connected to this
   * server. This method returns all clients irrespective of whether subscription is enabled or not.
   *
   * @return Map of ClientProxyMembershipID against CacheClientStatus objects.
   */
  public Map<ClientProxyMembershipID, CacheClientStatus> getStatusForAllClients() {
    Map<ClientProxyMembershipID, CacheClientStatus> result = new HashMap<>();
    synchronized (proxyIdConnections) {
      for (Map.Entry<ClientProxyMembershipID, ServerConnectionCollection> entry : proxyIdConnections
          .entrySet()) {
        ClientProxyMembershipID proxyID = entry.getKey();
        CacheClientStatus cci = new CacheClientStatus(proxyID);
        Set<ServerConnection> connections = entry.getValue().getConnections();
        if (connections != null) {
          String memberId;
          for (ServerConnection sc : connections) {
            if (sc.isClientServerConnection()) {
              // each ServerConnection has the same member id
              memberId = sc.getMembershipID();
              cci.setMemberId(memberId);
              cci.setNumberOfConnections(connections.size());
              result.put(proxyID, cci);
              break;
            }
          }
        }
      }
    }
    return result;
  }

  public void fillInClientInfo(Map<ClientProxyMembershipID, CacheClientStatus> allClients) {
    // The allClients parameter includes only actual clients (not remote
    // gateways). This monitor will include remote gateway connections,
    // so weed those out.
    synchronized (proxyIdConnections) {
      for (Map.Entry<ClientProxyMembershipID, CacheClientStatus> entry : allClients.entrySet()) {
        // proxyID includes FQDN
        ClientProxyMembershipID proxyID = entry.getKey();
        CacheClientStatus cci = entry.getValue();
        ServerConnectionCollection collection = proxyIdConnections.get(proxyID);
        Set<ServerConnection> connections = collection != null ? collection.getConnections() : null;
        if (connections != null) {
          String memberId = null;
          cci.setNumberOfConnections(connections.size());
          List<Integer> socketPorts = new ArrayList<>();
          List<InetAddress> socketAddresses = new ArrayList<>();
          for (ServerConnection sc : connections) {
            socketPorts.add(sc.getSocketPort());
            socketAddresses.add(sc.getSocketAddress());
            // each ServerConnection has the same member id
            memberId = sc.getMembershipID();
          }
          cci.setMemberId(memberId);
          cci.setSocketPorts(socketPorts);
          cci.setSocketAddresses(socketAddresses);
        }
      }
    }
  }

  public Map<String, IncomingGatewayStatus> getConnectedIncomingGateways() {
    Map<String, IncomingGatewayStatus> connectedIncomingGateways = new HashMap<>();
    synchronized (proxyIdConnections) {
      for (Map.Entry<ClientProxyMembershipID, ServerConnectionCollection> entry : proxyIdConnections
          .entrySet()) {
        ClientProxyMembershipID proxyID = entry.getKey();
        Set<ServerConnection> connections = entry.getValue().getConnections();
        for (ServerConnection sc : connections) {
          if (sc.getCommunicationMode().isWAN()) {
            IncomingGatewayStatus status = new IncomingGatewayStatus(proxyID.getDSMembership(),
                sc.getSocketAddress(), sc.getSocketPort());
            connectedIncomingGateways.put(proxyID.getDSMembership(), status);
          }
        }
      }
    }
    return connectedIncomingGateways;
  }

  private boolean cleanupClientThreads(ClientProxyMembershipID proxyID, boolean timedOut) {
    boolean result = false;
    Set<ServerConnection> serverConnections = null;
    synchronized (proxyIdConnections) {
      ServerConnectionCollection collection = proxyIdConnections.remove(proxyID);
      if (collection != null) {
        serverConnections = collection.getConnections();
      }
    }
    {
      if (serverConnections != null) {
        result = true;
        for (ServerConnection serverConnection : serverConnections) {
          serverConnection.handleTermination(timedOut);
        }
      }
    }
    return result;
  }

  // This will return true if the proxyID is truly idle (or if no connections are found), or false
  // if there was a active connection.
  private boolean prepareToTerminateIfNoConnectionIsProcessing(ClientProxyMembershipID proxyID) {
    synchronized (proxyIdConnections) {
      ServerConnectionCollection collection = proxyIdConnections.get(proxyID);
      if (collection == null) {
        return true;
      }
      if (collection.connectionsProcessing.get() == 0) {
        collection.isTerminating = true;
        return true;
      } else {
        return false;
      }
    }
  }

  private void validateThreads(ClientProxyMembershipID proxyID) {
    Set<ServerConnection> serverConnections;
    synchronized (proxyIdConnections) {
      ServerConnectionCollection collection = proxyIdConnections.get(proxyID);
      serverConnections =
          collection != null ? new HashSet<>(collection.getConnections()) : Collections.emptySet();
    }
    // release sync and operation on copy
    for (ServerConnection serverConnection : serverConnections) {
      if (serverConnection.hasBeenTimedOutOnClient()) {
        logger.warn("{} is being terminated because its client timeout of {} has expired.",
            serverConnection, serverConnection.getClientReadTimeout());
        try {
          serverConnection.handleTermination(true);
          // Not all the code in a ServerConnection correctly
          // handles interrupt. In particular it is possible to be doing
          // p2p distribution and to have sent a message to one peer but
          // to never send it to another due to interrupt.
          // serverConnection.interruptOwner();
        } finally {
          // Just to be sure we clean it up.
          // This call probably isn't needed.
          removeConnection(proxyID, serverConnection);
        }
      }

    }
  }

  /**
   * Returns the map of known clients.
   *
   * @return the map of known clients
   *
   *         Test hook only.
   */
  @VisibleForTesting
  Map<ClientProxyMembershipID, Long> getClientHeartbeats() {
    return clientHeartbeats.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().get()));
  }

  /**
   * Shuts down the singleton <code>CacheClientNotifier</code> instance.
   */
  protected synchronized void shutdown() {
    // Stop the client monitor
    if (clientMonitor != null) {
      clientMonitor.stopMonitoring();
    }
  }

  /**
   * Creates the singleton <code>CacheClientNotifier</code> instance.
   *
   * @param cache The GemFire <code>Cache</code>
   * @param maximumTimeBetweenPings The maximum time allowed between pings before determining the
   *        client has died and interrupting its sockets
   */
  protected static synchronized void createInstance(InternalCache cache,
      int maximumTimeBetweenPings, CacheClientNotifierStats stats) {
    refCount++;
    if (instance != null) {
      return;
    }
    instance = new ClientHealthMonitor(cache, maximumTimeBetweenPings, stats);
  }

  /**
   *
   * Constructor.
   *
   * @param cache The GemFire <code>Cache</code>
   * @param maximumTimeBetweenPings The maximum time allowed between pings before determining the
   *        client has died and interrupting its sockets
   */
  private ClientHealthMonitor(InternalCache cache, int maximumTimeBetweenPings,
      CacheClientNotifierStats stats) {
    // Set the Cache
    this.cache = cache;
    this.maximumTimeBetweenPings = maximumTimeBetweenPings;

    monitorInterval = Long.getLong(CLIENT_HEALTH_MONITOR_INTERVAL_PROPERTY,
        DEFAULT_CLIENT_MONITOR_INTERVAL_IN_MILLIS);
    logger.debug("Setting monitorInterval to {}", monitorInterval);

    if (maximumTimeBetweenPings > 0) {
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Initializing client health monitor thread", this);
      }
      clientMonitor = new ClientHealthMonitorThread(maximumTimeBetweenPings);
      clientMonitor.start();
    } else {
      // LOG:CONFIG: changed from config to info
      logger.info(
          "Client health monitor thread disabled due to maximumTimeBetweenPings setting: {}",
          maximumTimeBetweenPings);
      clientMonitor = null;
    }

    this.stats = stats;
  }

  /**
   * Returns a brief description of this <code>ClientHealthMonitor</code>
   *
   * @since GemFire 5.1
   */
  @Override
  public String toString() {
    return "ClientHealthMonitor@" + Integer.toHexString(System.identityHashCode(this));
  }

  private ServerConnectionCollection getProxyIdCollection(ClientProxyMembershipID proxyID) {
    return JavaWorkarounds.computeIfAbsent(proxyIdConnections, proxyID,
        key -> new ServerConnectionCollection());
  }

  Map<ClientProxyMembershipID, MutableInt> getCleanupProxyIdTable() {
    return cleanupProxyIdTable;
  }

  Map<ServerSideHandshake, MutableInt> getCleanupTable() {
    return cleanupTable;
  }

  public boolean hasDeltaClients() {
    return numberOfClientsWithConflationOff.get() > 0;
  }

  private int getMaximumTimeBetweenPings(ClientProxyMembershipID proxyID) {
    return clientMaximumTimeBetweenPings.getOrDefault(proxyID, maximumTimeBetweenPings);
  }

  /**
   * Interface for changing the heartbeat timeout behavior in the ClientHealthMonitorThread, should
   * only be used for testing
   */
  interface HeartbeatTimeoutCheck {
    boolean timedOut(long current, long lastHeartbeat, long interval);
  }

  @VisibleForTesting
  void testUseCustomHeartbeatCheck(HeartbeatTimeoutCheck check) {
    clientMonitor.overrideHeartbeatTimeoutCheck(check);
  }

  /**
   * Class <code>ClientHealthMonitorThread</code> is a <code>Thread</code> that verifies all clients
   * are still alive.
   */
  class ClientHealthMonitorThread extends LoggingThread {
    private HeartbeatTimeoutCheck checkHeartbeat = (long currentTime, long lastHeartbeat,
        long allowedInterval) -> currentTime - lastHeartbeat > allowedInterval;

    void overrideHeartbeatTimeoutCheck(HeartbeatTimeoutCheck newCheck) {
      checkHeartbeat = newCheck;
    }

    /**
     * The maximum time allowed between pings before determining the client has died and
     * interrupting its sockets.
     */
    final int _maximumTimeBetweenPings;

    /**
     * Whether the monitor is stopped
     */
    volatile boolean _isStopped = false;

    /**
     * Constructor.
     *
     * @param maximumTimeBetweenPings The maximum time allowed between pings before determining the
     *        client has died and interrupting its sockets
     */
    ClientHealthMonitorThread(int maximumTimeBetweenPings) {
      super("ClientHealthMonitor Thread");

      // Set the client connection timeout
      _maximumTimeBetweenPings = maximumTimeBetweenPings;
      // LOG:CONFIG: changed from config to info
      logger.info("ClientHealthMonitorThread maximum allowed time between pings: {}",
          _maximumTimeBetweenPings);
      if (maximumTimeBetweenPings == 0) {
        if (logger.isDebugEnabled()) {
          logger.debug("zero ping interval detected", new Exception(
              "stack trace"));
        }
      }
    }

    /**
     * Notifies the monitor to stop monitoring.
     */
    protected synchronized void stopMonitoring() {
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Stopping monitoring", ClientHealthMonitor.this);
      }
      _isStopped = true;
      interrupt();
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Stopped dispatching", ClientHealthMonitor.this);
      }
    }

    /**
     * Returns whether the dispatcher is stopped
     *
     * @return whether the dispatcher is stopped
     */
    protected boolean isStopped() {
      return _isStopped;
    }

    /**
     * Runs the monitor by iterating the map of clients and testing the latest ping time received
     * against the current time.
     */
    @Override
    public void run() {
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Beginning to monitor clients", ClientHealthMonitor.this);
      }

      while (!_isStopped) {
        SystemFailure.checkFailure();
        try {
          Thread.sleep(monitorInterval);
          if (logger.isTraceEnabled()) {
            logger.trace("Monitoring {} client(s)", getClientHeartbeats().size());
          }

          // Get the current time
          long currentTime = System.currentTimeMillis();
          if (logger.isTraceEnabled()) {
            logger.trace("{} starting sweep at {}", ClientHealthMonitor.this, currentTime);
          }

          // Iterate through the clients and verify that they are all still
          // alive
          for (Map.Entry<ClientProxyMembershipID, Long> entry : getClientHeartbeats().entrySet()) {
            ClientProxyMembershipID proxyID = entry.getKey();
            // Validate all ServerConnection threads. If a thread has been
            // processing a message for more than the socket timeout time,
            // close it it since the client will have timed out and resent.
            validateThreads(proxyID);

            Long latestHeartbeatValue = entry.getValue();
            // Compare the current value with the current time if it is not null
            // If it is null, that means that the client was just registered
            // and has not done a heartbeat yet.
            if (latestHeartbeatValue != null) {
              long latestHeartbeat = latestHeartbeatValue;
              if (logger.isTraceEnabled()) {
                logger.trace(
                    "{} ms have elapsed since the latest heartbeat for client with member id {}",
                    (currentTime - latestHeartbeat), proxyID);
              }

              int maximumTimeBetweenPingsForClient = getMaximumTimeBetweenPings(proxyID);
              if (checkHeartbeat.timedOut(currentTime, latestHeartbeat,
                  maximumTimeBetweenPingsForClient)) {
                // This client has been idle for too long. Determine whether
                // any of its ServerConnection threads are currently processing
                // a message. If so, let it go. If not, disconnect it.
                if (prepareToTerminateIfNoConnectionIsProcessing(proxyID)) {
                  if (cleanupClientThreads(proxyID, true)) {
                    logger.warn(
                        "Monitoring client with member id {}. It had been {} ms since the latest heartbeat. Max interval is {}. Terminated client.",
                        entry.getKey(), currentTime - latestHeartbeat,
                        maximumTimeBetweenPingsForClient);
                  }
                } else {
                  if (logger.isDebugEnabled()) {
                    logger.debug(
                        "Monitoring client with member id {}. It has been {} ms since the latest heartbeat. This client would have been terminated but at least one of its threads is processing a message.",
                        entry.getKey(), (currentTime - latestHeartbeat));
                  }
                }
              } else {
                if (logger.isTraceEnabled()) {
                  logger.trace(
                      "Monitoring client with member id {}. It has been {} ms since the latest heartbeat. This client is healthy.",
                      entry.getKey(), (currentTime - latestHeartbeat));
                }
              }
            }
          }
        } catch (InterruptedException e) {
          // no need to reset the bit; we're exiting
          if (_isStopped) {
            break;
          }
          logger.warn("Unexpected interrupt, exiting", e);
          break;
        } catch (Exception e) {
          // An exception occurred while monitoring the clients. If the monitor
          // is not stopped, log it and continue processing.
          if (!_isStopped) {
            logger.fatal(ClientHealthMonitor.this.toString() + ": An unexpected Exception occurred",
                e);
          }
        }
      } // while
    }
  } // ClientHealthMonitorThread

  @VisibleForTesting
  public static ClientHealthMonitorProvider singletonProvider() {
    return ClientHealthMonitor::getInstance;
  }

  @VisibleForTesting
  public static Supplier<ClientHealthMonitor> singletonGetter() {
    return ClientHealthMonitor::getInstance;
  }

  @FunctionalInterface
  @VisibleForTesting
  public interface ClientHealthMonitorProvider {
    ClientHealthMonitor get(InternalCache cache, int maximumTimeBetweenPings,
        CacheClientNotifierStats stats);
  }
}
