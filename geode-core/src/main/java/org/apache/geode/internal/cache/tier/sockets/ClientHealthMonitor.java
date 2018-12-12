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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicIntegerArray;

import org.apache.commons.lang.mutable.MutableInt;
import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.SystemFailure;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.CacheClientStatus;
import org.apache.geode.internal.cache.IncomingGatewayStatus;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.TXId;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.tier.ServerSideHandshake;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.LoggingThread;

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
   * The map of known clients
   *
   * Accesses must be locked by _clientHeartbeatsLock
   */
  private Map<ClientProxyMembershipID, Long> _clientHeartbeats = Collections.emptyMap();

  /**
   * An object used to lock the map of known clients
   */
  private final Object _clientHeartbeatsLock = new Object();

  /**
   * THe GemFire <code>Cache</code>
   */
  private final InternalCache _cache;

  public int getMaximumTimeBetweenPings() {
    return maximumTimeBetweenPings;
  }

  private final int maximumTimeBetweenPings;

  /**
   * A thread that validates client connections
   */
  private final ClientHealthMonitorThread _clientMonitor;

  /**
   * The singleton <code>CacheClientNotifier</code> instance
   */
  static ClientHealthMonitor _instance;

  /**
   * Reference count in the event that multiple cache servers are using the health monitor
   */

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
   * Gives, version-wise, the number of clients connected to the cache servers in this cache, which
   * are capable of processing received deltas.
   *
   * NOTE: It does not necessarily give the actual number of clients per version connected to the
   * cache servers in this cache.
   *
   * @see CacheClientNotifier#addClientProxy(CacheClientProxy)
   */
  AtomicIntegerArray numOfClientsPerVersion = new AtomicIntegerArray(Version.HIGHEST_VERSION + 1);

  public long getMonitorInterval() {
    return monitorInterval;
  }

  private long monitorInterval;

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
    return _instance;
  }

  /**
   * Factory method to return the singleton <code>ClientHealthMonitor</code> instance.
   *
   * @return the singleton <code>ClientHealthMonitor</code> instance
   */
  public static ClientHealthMonitor getInstance() {
    return _instance;
  }

  /**
   * Shuts down the singleton <code>ClientHealthMonitor</code> instance.
   */
  public static synchronized void shutdownInstance() {
    refCount--;
    if (_instance == null)
      return;
    if (refCount > 0)
      return;
    _instance.shutdown();

    boolean interrupted = false; // Don't clear, let join fail if already interrupted
    try {
      if (_instance._clientMonitor != null) {
        _instance._clientMonitor.join();
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
    _instance = null;
    refCount = 0;
  }

  /**
   * Registers a new client to be monitored.
   *
   * @param proxyID The id of the client to be registered
   */
  public void registerClient(ClientProxyMembershipID proxyID) {
    boolean registerClient = false;
    synchronized (_clientHeartbeatsLock) {
      Map<ClientProxyMembershipID, Long> oldClientHeartbeats = this._clientHeartbeats;
      if (!oldClientHeartbeats.containsKey(proxyID)) {
        Map<ClientProxyMembershipID, Long> newClientHeartbeats = new HashMap<>(oldClientHeartbeats);
        newClientHeartbeats.put(proxyID, System.currentTimeMillis());
        this._clientHeartbeats = newClientHeartbeats;
        registerClient = true;
      }
    }

    if (registerClient) {
      if (this.stats != null) {
        this.stats.incClientRegisterRequests();
      }
      if (logger.isDebugEnabled()) {
        logger.debug("ClientHealthMonitor: Registering client with member id {}",
            proxyID);
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
    boolean unregisterClient = false;
    synchronized (_clientHeartbeatsLock) {
      Map<ClientProxyMembershipID, Long> oldClientHeartbeats = this._clientHeartbeats;
      if (oldClientHeartbeats.containsKey(proxyID)) {
        unregisterClient = true;
        Map<ClientProxyMembershipID, Long> newClientHeartbeats = new HashMap<>(oldClientHeartbeats);
        newClientHeartbeats.remove(proxyID);
        this._clientHeartbeats = newClientHeartbeats;
      }
    }

    if (unregisterClient) {
      if (clientDisconnectedCleanly) {
        if (logger.isDebugEnabled()) {
          logger.debug("ClientHealthMonitor: Unregistering client with member id {}",
              proxyID);
        }
      } else {
        logger.warn("ClientHealthMonitor: Unregistering client with member id {} due to: {}",
            new Object[] {proxyID, clientDisconnectException == null ? "Unknown reason"
                : clientDisconnectException.getLocalizedMessage()});
      }
      if (this.stats != null) {
        this.stats.incClientUnRegisterRequests();
      }
      expireTXStates(proxyID);
    }
  }

  /**
   * Unregisters a client to be monitored.
   *
   * @param proxyID The id of the client to be unregistered
   * @param acceptor non-null if the call is from a <code>ServerConnection</code> (as opposed to a
   *        <code>CacheClientProxy</code>).
   * @param clientDisconnectedCleanly Whether the client disconnected cleanly or crashed
   */
  public void unregisterClient(ClientProxyMembershipID proxyID, AcceptorImpl acceptor,
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
  public Set<TXId> getScheduledToBeRemovedTx() {
    final TXManagerImpl txMgr = (TXManagerImpl) this._cache.getCacheTransactionManager();
    return txMgr.getScheduledToBeRemovedTx();
  }

  /**
   * expire the transaction states for the given client. This uses the transactionTimeToLive setting
   * that is inherited from the TXManagerImpl. If that setting is non-positive we expire the states
   * immediately
   *
   */
  private void expireTXStates(ClientProxyMembershipID proxyID) {
    final TXManagerImpl txMgr = (TXManagerImpl) this._cache.getCacheTransactionManager();
    final Set<TXId> txIds =
        txMgr.getTransactionsForClient((InternalDistributedMember) proxyID.getDistributedMember());
    if (this._cache.isClosed()) {
      return;
    }
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
  public void removeConnection(ClientProxyMembershipID proxyID, ServerConnection connection) {
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
    if (this._clientMonitor == null) {
      return;
    }
    if (logger.isTraceEnabled()) {
      logger.trace("ClientHealthMonitor: Received ping from client with member id {}", proxyID);
    }
    synchronized (_clientHeartbeatsLock) {
      if (!this._clientHeartbeats.containsKey(proxyID)) {
        registerClient(proxyID);
      } else {
        this._clientHeartbeats.put(proxyID, Long.valueOf(System.currentTimeMillis()));
      }
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
  public Map<String, Object[]> getConnectedClients(Set filterProxies) {
    Map<String, Object[]> map = new HashMap<>(); // KEY=proxyID, VALUE=connectionCount (Integer)
    synchronized (proxyIdConnections) {
      for (Map.Entry<ClientProxyMembershipID, ServerConnectionCollection> entry : proxyIdConnections
          .entrySet()) {
        ClientProxyMembershipID proxyID = entry.getKey();// proxyID
        // includes FQDN
        if (filterProxies == null || filterProxies.contains(proxyID)) {
          String membershipID = null;
          Set<ServerConnection> connections = entry.getValue().getConnections();
          int socketPort = 0;
          InetAddress socketAddress = null;
          /// *
          // Get data from one.
          for (ServerConnection sc : connections) {
            socketPort = sc.getSocketPort();
            socketAddress = sc.getSocketAddress();
            membershipID = sc.getMembershipID();
            break;
          }
          // */
          int connectionCount = connections.size();
          String clientString = null;
          if (socketAddress == null) {
            clientString = "client member id=" + membershipID;
          } else {
            clientString = "host name=" + socketAddress.toString() + " host ip="
                + socketAddress.getHostAddress() + " client port=" + socketPort
                + " client member id=" + membershipID;
          }
          Object[] data = null;
          data = map.get(membershipID);
          if (data == null) {
            map.put(membershipID, new Object[] {clientString, Integer.valueOf(connectionCount)});
          } else {
            data[1] = Integer.valueOf(((Integer) data[1]).intValue() + connectionCount);
          }
          /*
           * Note: all client addresses are same... Iterator serverThreads = ((Set)
           * entry.getValue()).iterator(); while (serverThreads.hasNext()) { ServerConnection
           * connection = (ServerConnection) serverThreads.next(); InetAddress clientAddress =
           * connection.getClientAddress(); logger.severe("getConnectedClients: proxyID=" + proxyID
           * + " clientAddress=" + clientAddress + " FQDN=" + clientAddress.getCanonicalHostName());
           * }
           */
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
          String memberId = null;
          for (ServerConnection sc : connections) {
            if (sc.isClientServerConnection()) {
              memberId = sc.getMembershipID(); // each ServerConnection has the same member id
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
        ClientProxyMembershipID proxyID = entry.getKey();// proxyID
        // includes FQDN
        CacheClientStatus cci = entry.getValue();
        ServerConnectionCollection collection = proxyIdConnections.get(proxyID);
        Set<ServerConnection> connections = collection != null ? collection.getConnections() : null;
        if (connections != null) {
          String memberId = null;
          cci.setNumberOfConnections(connections.size());
          List socketPorts = new ArrayList();
          List socketAddresses = new ArrayList();
          for (ServerConnection sc : connections) {
            socketPorts.add(Integer.valueOf(sc.getSocketPort()));
            socketAddresses.add(sc.getSocketAddress());
            memberId = sc.getMembershipID(); // each ServerConnection has the
            // same member id
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

  protected boolean cleanupClientThreads(ClientProxyMembershipID proxyID, boolean timedOut) {
    boolean result = false;
    Set serverConnections = null;
    synchronized (proxyIdConnections) {
      ServerConnectionCollection collection = proxyIdConnections.remove(proxyID);
      if (collection != null) {
        serverConnections = collection.getConnections();
      }
    }
    {
      if (serverConnections != null) { // fix for bug 35343
        result = true;
        for (Iterator it = serverConnections.iterator(); it.hasNext();) {
          ServerConnection serverConnection = (ServerConnection) it.next();
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

  protected void validateThreads(ClientProxyMembershipID proxyID) {
    Set<ServerConnection> serverConnections;
    synchronized (proxyIdConnections) {
      ServerConnectionCollection collection = proxyIdConnections.get(proxyID);
      serverConnections =
          collection != null ? new HashSet<>(collection.getConnections()) : Collections.emptySet();
    }
    // release sync and operation on copy to fix bug 37675
    for (ServerConnection serverConnection : serverConnections) {
      if (serverConnection.hasBeenTimedOutOnClient()) {
        logger.warn("{} is being terminated because its client timeout of {} has expired.",
            new Object[] {serverConnection,
                Integer.valueOf(serverConnection.getClientReadTimeout())});
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
  Map<ClientProxyMembershipID, Long> getClientHeartbeats() {
    synchronized (this._clientHeartbeatsLock) {
      return new HashMap<>(this._clientHeartbeats);
    }
  }

  /**
   * Shuts down the singleton <code>CacheClientNotifier</code> instance.
   */
  protected synchronized void shutdown() {
    // Stop the client monitor
    if (this._clientMonitor != null) {
      this._clientMonitor.stopMonitoring();
    }
  }

  /**
   * Creates the singleton <code>CacheClientNotifier</code> instance.
   *
   * @param cache The GemFire <code>Cache</code>
   * @param maximumTimeBetweenPings The maximum time allowed between pings before determining the
   */
  protected static synchronized void createInstance(InternalCache cache,
      int maximumTimeBetweenPings, CacheClientNotifierStats stats) {
    refCount++;
    if (_instance != null) {
      return;
    }
    _instance = new ClientHealthMonitor(cache, maximumTimeBetweenPings, stats);
  }

  /**
   *
   * Constructor.
   *
   * @param cache The GemFire <code>Cache</code>
   * @param maximumTimeBetweenPings The maximum time allowed between pings before determining the
   */
  private ClientHealthMonitor(InternalCache cache, int maximumTimeBetweenPings,
      CacheClientNotifierStats stats) {
    // Set the Cache
    this._cache = cache;
    this.maximumTimeBetweenPings = maximumTimeBetweenPings;

    this.monitorInterval = Long.getLong(CLIENT_HEALTH_MONITOR_INTERVAL_PROPERTY,
        DEFAULT_CLIENT_MONITOR_INTERVAL_IN_MILLIS);
    logger.debug("Setting monitorInterval to {}", this.monitorInterval);

    if (maximumTimeBetweenPings > 0) {
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Initializing client health monitor thread", this);
      }
      this._clientMonitor = new ClientHealthMonitorThread(maximumTimeBetweenPings);
      this._clientMonitor.start();
    } else {
      // LOG:CONFIG: changed from config to info
      logger.info(
          "Client health monitor thread disabled due to maximumTimeBetweenPings setting: {}",
          maximumTimeBetweenPings);
      this._clientMonitor = null;
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

  public ServerConnectionCollection getProxyIdCollection(ClientProxyMembershipID proxyID) {
    return proxyIdConnections.computeIfAbsent(proxyID, key -> new ServerConnectionCollection());
  }

  public Map<ClientProxyMembershipID, MutableInt> getCleanupProxyIdTable() {
    return cleanupProxyIdTable;
  }

  public Map<ServerSideHandshake, MutableInt> getCleanupTable() {
    return cleanupTable;
  }

  public int getNumberOfClientsAtOrAboveVersion(Version version) {
    int number = 0;
    for (int i = version.ordinal(); i < numOfClientsPerVersion.length(); i++) {
      number += numOfClientsPerVersion.get(i);
    }
    return number;
  }

  public boolean hasDeltaClients() {
    return getNumberOfClientsAtOrAboveVersion(Version.GFE_61) > 0;
  }

  /**
   * Interface for changing the heartbeat timeout behavior in the ClientHealthMonitorThread, should
   * only be used for testing
   */
  interface HeartbeatTimeoutCheck {
    boolean timedOut(long current, long lastHeartbeat, long interval);
  }

  void testUseCustomHeartbeatCheck(HeartbeatTimeoutCheck check) {
    _clientMonitor.overrideHeartbeatTimeoutCheck(check);
  }

  /**
   * Class <code>ClientHealthMonitorThread</code> is a <code>Thread</code> that verifies all clients
   * are still alive.
   */
  class ClientHealthMonitorThread extends LoggingThread {
    private HeartbeatTimeoutCheck checkHeartbeat = (long currentTime, long lastHeartbeat,
        long allowedInterval) -> currentTime - lastHeartbeat > allowedInterval;

    protected void overrideHeartbeatTimeoutCheck(HeartbeatTimeoutCheck newCheck) {
      checkHeartbeat = newCheck;
    }

    /**
     * The maximum time allowed between pings before determining the client has died and
     * interrupting its sockets.
     */
    protected final int _maximumTimeBetweenPings;

    /**
     * Whether the monitor is stopped
     */
    protected volatile boolean _isStopped = false;

    /**
     * Constructor.
     *
     * @param maximumTimeBetweenPings The maximum time allowed between pings before determining the
     *        client has died and interrupting its sockets
     */
    protected ClientHealthMonitorThread(int maximumTimeBetweenPings) {
      super("ClientHealthMonitor Thread");

      // Set the client connection timeout
      this._maximumTimeBetweenPings = maximumTimeBetweenPings;
      // LOG:CONFIG: changed from config to info
      logger.info("ClientHealthMonitorThread maximum allowed time between pings: {}",
          this._maximumTimeBetweenPings);
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
      this._isStopped = true;
      this.interrupt();
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
      return this._isStopped;
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

      while (!this._isStopped) {
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
          for (Iterator i = getClientHeartbeats().entrySet().iterator(); i.hasNext();) {
            Map.Entry entry = (Map.Entry) i.next();
            ClientProxyMembershipID proxyID = (ClientProxyMembershipID) entry.getKey();
            // Validate all ServerConnection threads. If a thread has been
            // processing a message for more than the socket timeout time,
            // close it it since the client will have timed out and resent.
            validateThreads(proxyID);

            Long latestHeartbeatValue = (Long) entry.getValue();
            // Compare the current value with the current time if it is not null
            // If it is null, that means that the client was just registered
            // and has not done a heartbeat yet.
            if (latestHeartbeatValue != null) {
              long latestHeartbeat = latestHeartbeatValue.longValue();
              if (logger.isTraceEnabled()) {
                logger.trace(
                    "{} ms have elapsed since the latest heartbeat for client with member id {}",
                    (currentTime - latestHeartbeat), proxyID);
              }

              if (checkHeartbeat.timedOut(currentTime, latestHeartbeat,
                  this._maximumTimeBetweenPings)) {
                // This client has been idle for too long. Determine whether
                // any of its ServerConnection threads are currently processing
                // a message. If so, let it go. If not, disconnect it.
                if (prepareToTerminateIfNoConnectionIsProcessing(proxyID)) {
                  if (cleanupClientThreads(proxyID, true)) {
                    logger.warn(
                        "Monitoring client with member id {}. It had been {} ms since the latest heartbeat. Max interval is {}. Terminated client.",
                        new Object[] {entry.getKey(), currentTime - latestHeartbeat,
                            this._maximumTimeBetweenPings});
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
          if (this._isStopped) {
            break;
          }
          logger.warn("Unexpected interrupt, exiting", e);
          break;
        } catch (Exception e) {
          // An exception occurred while monitoring the clients. If the monitor
          // is not stopped, log it and continue processing.
          if (!this._isStopped) {
            logger.fatal(ClientHealthMonitor.this.toString() + ": An unexpected Exception occurred",
                e);
          }
        }
      } // while
    }
  } // ClientHealthMonitorThread
}
