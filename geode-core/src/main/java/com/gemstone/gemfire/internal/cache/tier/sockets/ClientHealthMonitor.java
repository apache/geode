/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gemstone.gemfire.internal.cache.tier.sockets;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.SystemTimer.SystemTimerTask;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.cache.*;
import com.gemstone.gemfire.internal.cache.tier.Acceptor;
import com.gemstone.gemfire.internal.concurrent.ConcurrentHashSet;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.LoggingThreadGroup;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import org.apache.logging.log4j.Logger;

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * Class <code>ClientHealthMonitor</code> is a server-side singleton that
 * monitors the health of clients by looking at their heartbeats. If too much
 * time elapses between heartbeats, the monitor determines that the client is
 * dead and interrupts its threads.
 * 
 * 
 * @since GemFire 4.2.3
 */
public class ClientHealthMonitor {
  private static final Logger logger = LogService.getLogger();

  /**
   * The map of known clients
   */
  protected volatile Map _clientHeartbeats = Collections.EMPTY_MAP;

  /**
   * An object used to lock the map of known clients
   */
  final protected Object _clientHeartbeatsLock = new Object();

  /**
   * The map of known client threads
   */
  final protected Map _clientThreads;

  /**
   * An object used to lock the map of client threads
   */
  final private Object _clientThreadsLock = new Object();

  /**
   * THe GemFire <code>Cache</code>
   */
  final protected Cache _cache;

  /**
   * A thread that validates client connections
   */
  final private ClientHealthMonitorThread _clientMonitor;

  /**
   * The singleton <code>CacheClientNotifier</code> instance
   */
  static ClientHealthMonitor _instance;

  /**
   * Reference count in the event that multiple bridge servers are using the health monitor
   */

  private static int refCount = 0;

  /**
   * The interval between client monitor iterations
   */
  final protected static long CLIENT_MONITOR_INTERVAL = 1000;

  final private CacheClientNotifierStats stats;

  /**
   * Used to track the number of handshakes in a VM primary use, license
   * enforcement.
   *
   * note, these were moved from static fields in ServerConnection
   * so that they will be cleaned up when the client health monitor is shutdown.
   */
  private final HashMap cleanupTable = new HashMap();

  private final HashMap cleanupProxyIdTable = new HashMap();

  /**
   * Gives, version-wise, the number of clients connected to the cache servers
   * in this cache, which are capable of processing recieved deltas.
   * 
   * NOTE: It does not necessarily give the actual number of clients per version
   * connected to the cache servers in this cache.
   * 
   * @see CacheClientNotifier#addClientProxy(CacheClientProxy)
   */
  AtomicIntegerArray numOfClientsPerVersion = new AtomicIntegerArray(Version.HIGHEST_VERSION+1);
  
  /**
   * Factory method to construct or return the singleton
   * <code>ClientHealthMonitor</code> instance.
   * 
   * @param cache
   *          The GemFire <code>Cache</code>
   * @param maximumTimeBetweenPings
   *          The maximum time allowed between pings before determining the
   *          client has died and interrupting its sockets.
   * @return The singleton <code>ClientHealthMonitor</code> instance
   */
  public static ClientHealthMonitor getInstance(Cache cache,
      int maximumTimeBetweenPings, CacheClientNotifierStats stats)
  {
    createInstance(cache, maximumTimeBetweenPings, stats);
    return _instance;
  }

  /**
   * Factory method to return the singleton <code>ClientHealthMonitor</code>
   * instance.
   * 
   * @return the singleton <code>ClientHealthMonitor</code> instance
   */
  public static ClientHealthMonitor getInstance()
  {
    return _instance;
  }

  /**
   * Shuts down the singleton <code>ClientHealthMonitor</code> instance.
   */
  public synchronized static void shutdownInstance()
  {
    refCount--;
    if (_instance == null)
      return;
    if (refCount > 0)
      return;
    _instance.shutdown();    
    
    boolean interrupted = false; // Don't clear, let join fail if already interrupted
    try{
      if (_instance._clientMonitor != null) {
        _instance._clientMonitor.join();
      }
    }
    catch(InterruptedException e) {
      interrupted = true;
      if(logger.isDebugEnabled()) {
        logger.debug(":Interrupted joining with the ClientHealthMonitor Thread", e);
      }
    }
    finally {
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
   * @param proxyID
   *          The id of the client to be registered
   */
  public void registerClient(ClientProxyMembershipID proxyID)
  {
    boolean registerClient = false;
    synchronized (_clientHeartbeatsLock) {
      Map oldClientHeartbeats = this._clientHeartbeats;
      if (!oldClientHeartbeats.containsKey(proxyID)) {
        Map newClientHeartbeats = new HashMap(oldClientHeartbeats);
        newClientHeartbeats.put(proxyID, Long.valueOf(System.currentTimeMillis()));
        this._clientHeartbeats = newClientHeartbeats;
        registerClient = true;
      }
    }

    if (registerClient) {
      if (this.stats != null) {
        this.stats.incClientRegisterRequests();
      }
      if (logger.isDebugEnabled()) {
        logger.debug(LocalizedMessage.create(LocalizedStrings.ClientHealthMonitor_CLIENTHEALTHMONITOR_REGISTERING_CLIENT_WITH_MEMBER_ID_0, new Object[] { proxyID }));
      }
    }
  }

  /**
   * Takes care of unregistering from the _clientHeatBeats map.
   * @param proxyID
   *          The id of the client to be unregistered
   */
  private void unregisterClient(ClientProxyMembershipID proxyID) {
    boolean unregisterClient = false;
    synchronized (_clientHeartbeatsLock) {
      Map oldClientHeartbeats = this._clientHeartbeats;
      if (oldClientHeartbeats.containsKey(proxyID)) {
        unregisterClient = true;
        Map newClientHeartbeats = new HashMap(oldClientHeartbeats);
        newClientHeartbeats.remove(proxyID);
        this._clientHeartbeats = newClientHeartbeats;
      }
    }

    if (unregisterClient) {
      if (logger.isDebugEnabled()) {
        logger.debug(LocalizedMessage.create(LocalizedStrings.ClientHealthMonitor_CLIENTHEALTHMONITOR_UNREGISTERING_CLIENT_WITH_MEMBER_ID_0, new Object[] { proxyID }));
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
   * @param proxyID
   *          The id of the client to be unregistered
   * @param acceptor
   *          non-null if the call is from a <code>ServerConnection</code> (as
   *          opposed to a <code>CacheClientProxy</code>).
   * @param clientDisconnectedCleanly
   *          Whether the client disconnected cleanly or crashed
   */
  public void unregisterClient(ClientProxyMembershipID proxyID,
                               AcceptorImpl acceptor,
                               boolean clientDisconnectedCleanly)
  {
    unregisterClient(proxyID);
    // Unregister any CacheClientProxy instances associated with this member id
    // if this method was invoked from a ServerConnection and the client did
    // not disconnect cleanly.
    if (acceptor != null) {
      CacheClientNotifier ccn = acceptor.getCacheClientNotifier();
      if (ccn != null) {
        try {
          ccn.unregisterClient(proxyID, clientDisconnectedCleanly);
        }
        catch (CancelException ignore) {
        }
      }
    }
  }

  private final Set<TXId> scheduledToBeRemovedTx = Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "trackScheduledToBeRemovedTx") ?
      new ConcurrentHashSet<TXId>() : null;

  /**
   * provide a test hook to track client transactions to be removed
   */
  public Set<TXId> getScheduledToBeRemovedTx() {
    return scheduledToBeRemovedTx;
  }

  /**
   * expire the transaction states for the given client.  This uses the
   * transactionTimeToLive setting that is inherited from the CacheServer.
   * If that setting is non-positive we expire the states immediately
   * @param proxyID
   */
  private void expireTXStates(ClientProxyMembershipID proxyID) {
    final TXManagerImpl txMgr = (TXManagerImpl)this._cache.getCacheTransactionManager(); 
    final Set<TXId> txids = txMgr.getTransactionsForClient(
          (InternalDistributedMember)proxyID.getDistributedMember());
    CacheClientNotifier notifier = CacheClientNotifier.getInstance();
    if (notifier == null || this._cache.isClosed()) {
      return; // notifier is null when shutting down
    }
    long timeout = notifier.getTransactionTimeToLive() * 1000;
    if (txids.size() > 0) {
      if (logger.isDebugEnabled()) {
        logger.debug("expiring {} transaction contexts for {} timeout={}", txids.size(), proxyID, timeout/1000);
      }

      if (timeout <= 0) {
        txMgr.removeTransactions(txids, true);
      } else {
        if (scheduledToBeRemovedTx != null) scheduledToBeRemovedTx.addAll(txids);       
        SystemTimerTask task = new SystemTimerTask() {
          @Override
          public void run2() {
            txMgr.removeTransactions(txids, true);
            if (scheduledToBeRemovedTx != null) scheduledToBeRemovedTx.removeAll(txids);
          }
        };
        ((GemFireCacheImpl)this._cache).getCCPTimer().schedule(task, timeout);
      }
    }
  }

  public void removeAllConnectionsAndUnregisterClient(
      ClientProxyMembershipID proxyID)
  {
    // Remove all connections
    cleanupClientThreads(proxyID, false);

    unregisterClient(proxyID);
  }

  /**
   * Adds a <code>ServerConnection</code> to the client's processing threads
   * 
   * @param proxyID
   *          The membership id of the client to be updated
   * @param connection
   *          The thread processing client requests
   */
  public void addConnection(ClientProxyMembershipID proxyID,
      ServerConnection connection)
  {
    //logger.info("ClientHealthMonitor: Adding " + connection + " to
    // client with member id " + proxyID);
    synchronized (_clientThreadsLock) {
      Set serverConnections = (Set)this._clientThreads.get(proxyID);
      if (serverConnections == null) {
        serverConnections = new HashSet();
        this._clientThreads.put(proxyID, serverConnections);
      }
      serverConnections.add(connection);
      //logger.info("ClientHealthMonitor: The client with member id " +
      // proxyID + " contains " + serverConnections.size() + " threads");
    }
  }

  /**
   * Removes a <code>ServerConnection</code> from the client's processing
   * threads
   * 
   * @param proxyID
   *          The id of the client to be updated
   * @param connection
   *          The thread processing client requests
   */
  public void removeConnection(ClientProxyMembershipID proxyID,
      ServerConnection connection)
  {
    //logger.info("ClientHealthMonitor: Removing " + connection + " from
    // client with member id " + proxyID);
    synchronized (_clientThreadsLock) {
      Set serverConnections = (Set)this._clientThreads.get(proxyID);
      if (serverConnections != null) { // fix for bug 35343
        serverConnections.remove(connection);
        //logger.info("ClientHealthMonitor: The client with member id " +
        // proxyID + " contains " + serverConnections.size() + " threads");
        if (serverConnections.isEmpty()) {
          //logger.info("ClientHealthMonitor: The client with member id "
          // + proxyID + " is being removed since it contains 0 threads");
          this._clientThreads.remove(proxyID);
        }
      }
    }
  }

  /**
   * Processes a received ping for a client.
   * 
   * @param proxyID
   *          The id of the client from which the ping was received
   */
  public void receivedPing(ClientProxyMembershipID proxyID)
  {
    if (this._clientMonitor == null) {
      return;
    }
    if (logger.isTraceEnabled()) {
      logger.trace("ClientHealthMonitor: Received ping from client with member id {}", proxyID);
    }
    synchronized (_clientHeartbeatsLock) {
      if (!this._clientHeartbeats.containsKey(proxyID)) {
        registerClient(proxyID);
      }
      else {
        this._clientHeartbeats.put(proxyID,
            Long.valueOf(System.currentTimeMillis()));
      }
    }
  }

  //   /**
  //    * Returns modifiable map (changes do not effect this class) of memberId
  //    * to connection count.
  //    */
  //   public Map getConnectedClients() {
  //     Map map = new HashMap(); // KEY=memberId, VALUE=connectionCount (Integer)
  //     synchronized (_clientThreadsLock) {
  //       Iterator connectedClients = this._clientThreads.entrySet().iterator();
  //       while (connectedClients.hasNext()) {
  //         Map.Entry entry = (Map.Entry) connectedClients.next();
  //         String memberId = (String) entry.getKey();// memberId includes FQDN
  //         Set connections = (Set) entry.getValue();
  //         int socketPort = 0;
  //         InetAddress socketAddress = null;
  //         ///*
  //         Iterator serverConnections = connections.iterator();
  //         // Get data from one.
  //         while (serverConnections.hasNext()) {
  //           ServerConnection sc = (ServerConnection) serverConnections.next();
  //           socketPort = sc.getSocketPort();
  //           socketAddress = sc.getSocketAddress();
  //           break;
  //         }
  //         //*/
  //         int connectionCount = connections.size();
  //         String clientString = null;
  //         if (socketAddress == null) {
  //           clientString = "client member id=" + memberId;
  //         } else {
  //           clientString = "host name=" + socketAddress.toString() + " host ip=" +
  // socketAddress.getHostAddress() + " client port=" + socketPort + " client
  // member id=" + memberId;
  //         }
  //         map.put(memberId, new Object[] {clientString, new
  // Integer(connectionCount)});
  //         /* Note: all client addresses are same...
  //         Iterator serverThreads = ((Set) entry.getValue()).iterator();
  //         while (serverThreads.hasNext()) {
  //           ServerConnection connection = (ServerConnection) serverThreads.next();
  //           InetAddress clientAddress = connection.getClientAddress();
  //           logger.severe("getConnectedClients: memberId=" + memberId +
  //             " clientAddress=" + clientAddress + " FQDN=" +
  //             clientAddress.getCanonicalHostName());
  //         }*/
  //       }
  //     }
  //     return map;
  //   }

  /**
   * Returns modifiable map (changes do not effect this class) of client
   * membershipID to connection count. This is different from the map contained
   * in this class as here the key is client membershipID & not the the proxyID.
   * It is to be noted that a given client can have multiple proxies.
   * 
   * @param filterProxies
   *          Set identifying the Connection proxies which should be fetched.
   *          These ConnectionProxies may be from same client member or
   *          different. If it is null this would mean to fetch the Connections
   *          of all the ConnectionProxy objects.
   *  
   */
  public Map getConnectedClients(Set filterProxies)
  {
    Map map = new HashMap(); // KEY=proxyID, VALUE=connectionCount (Integer)
    synchronized (_clientThreadsLock) {
      Iterator connectedClients = this._clientThreads.entrySet().iterator();
      while (connectedClients.hasNext()) {
        Map.Entry entry = (Map.Entry)connectedClients.next();
        ClientProxyMembershipID proxyID = (ClientProxyMembershipID)entry
            .getKey();// proxyID includes FQDN
        if (filterProxies == null || filterProxies.contains(proxyID)) {
          String membershipID = null;
          Set connections = (Set)entry.getValue();
          int socketPort = 0;
          InetAddress socketAddress = null;
          ///*
          Iterator serverConnections = connections.iterator();
          // Get data from one.
          while (serverConnections.hasNext()) {
            ServerConnection sc = (ServerConnection)serverConnections.next();
            socketPort = sc.getSocketPort();
            socketAddress = sc.getSocketAddress();
            membershipID = sc.getMembershipID();
            break;
          }
          //*/
          int connectionCount = connections.size();
          String clientString = null;
          if (socketAddress == null) {
            clientString = "client member id=" + membershipID;
          }
          else {
            clientString = "host name=" + socketAddress.toString()
                + " host ip=" + socketAddress.getHostAddress()
                + " client port=" + socketPort + " client member id="
                + membershipID;
          }
          Object[] data = null;
          data = (Object[])map.get(membershipID);
          if (data == null) {
            map.put(membershipID, new Object[] { clientString,
                Integer.valueOf(connectionCount) });
          }
          else {
            data[1] = Integer.valueOf(((Integer)data[1]).intValue()
                + connectionCount);
          }
          /*
           * Note: all client addresses are same... Iterator serverThreads =
           * ((Set) entry.getValue()).iterator(); while
           * (serverThreads.hasNext()) { ServerConnection connection =
           * (ServerConnection) serverThreads.next(); InetAddress clientAddress =
           * connection.getClientAddress();
           * logger.severe("getConnectedClients: proxyID=" + proxyID + "
           * clientAddress=" + clientAddress + " FQDN=" +
           * clientAddress.getCanonicalHostName()); }
           */
        }
      }

    }
    return map;
  }
  
  /**
   * This method returns the CacheClientStatus for all the clients that are
   * connected to this server. This method returns all clients irrespective of
   * whether subscription is enabled or not. 
   * 
   * @return Map of ClientProxyMembershipID against CacheClientStatus objects.
   */
  public Map getStatusForAllClients() {
    Map result = new HashMap();
    synchronized (_clientThreadsLock) {
      Iterator connectedClients = this._clientThreads.entrySet().iterator();
      while (connectedClients.hasNext()) {
        Map.Entry entry = (Map.Entry)connectedClients.next();
        ClientProxyMembershipID proxyID = (ClientProxyMembershipID)entry.getKey();
        CacheClientStatus cci = new CacheClientStatus(proxyID); 
        Set connections = (Set)this._clientThreads.get(proxyID);
        if (connections != null) {
          String memberId = null;
          Iterator connectionsIterator = connections.iterator();
          while (connectionsIterator.hasNext()) {
            ServerConnection sc = (ServerConnection)connectionsIterator.next();
            byte communicationMode = sc.getCommunicationMode();
            /* Check for all modes that could be used for Client-Server communication*/
            if (communicationMode == Acceptor.CLIENT_TO_SERVER
                || communicationMode == Acceptor.PRIMARY_SERVER_TO_CLIENT
                || communicationMode == Acceptor.SECONDARY_SERVER_TO_CLIENT
                || communicationMode == Acceptor.CLIENT_TO_SERVER_FOR_QUEUE) {
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
  
  public void fillInClientInfo(Map allClients)
  {
    // The allClients parameter includes only actual clients (not remote
    // gateways). This monitor will include remote gateway connections,
    // so weed those out.
    synchronized (_clientThreadsLock) {
      Iterator allClientsIterator = allClients.entrySet().iterator();
      while (allClientsIterator.hasNext()) {
        Map.Entry entry = (Map.Entry)allClientsIterator.next();
        ClientProxyMembershipID proxyID = (ClientProxyMembershipID)entry
            .getKey();// proxyID includes FQDN
        CacheClientStatus cci = (CacheClientStatus)entry.getValue();
        Set connections = (Set)this._clientThreads.get(proxyID);
        if (connections != null) {
          String memberId = null;
          cci.setNumberOfConnections(connections.size());
          List socketPorts = new ArrayList();
          List socketAddresses = new ArrayList();
          Iterator connectionsIterator = connections.iterator();
          while (connectionsIterator.hasNext()) {
            ServerConnection sc = (ServerConnection)connectionsIterator.next();
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

  public Map getConnectedIncomingGateways()
  {
    Map connectedIncomingGateways = new HashMap();
    synchronized (_clientThreadsLock) {
      Iterator connectedClients = this._clientThreads.entrySet().iterator();
      while (connectedClients.hasNext()) {
        Map.Entry entry = (Map.Entry)connectedClients.next();
        ClientProxyMembershipID proxyID = (ClientProxyMembershipID)entry
            .getKey();
        Set connections = (Set)entry.getValue();
        Iterator connectionsIterator = connections.iterator();
        while (connectionsIterator.hasNext()) {
          ServerConnection sc = (ServerConnection)connectionsIterator.next();
          if (sc.getCommunicationMode() == Acceptor.GATEWAY_TO_GATEWAY) {
            IncomingGatewayStatus status = new IncomingGatewayStatus(proxyID
                .getDSMembership(), sc.getSocketAddress(), sc.getSocketPort());
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
    synchronized (this._clientThreadsLock) {
      serverConnections = (Set) this._clientThreads.remove(proxyID);
      // It is ok to modify the set after releasing the sync
      // because it has been removed from the map while holding
      // the sync.
    } // end sync here to fix bug 37576 and 36740
    {
      if (serverConnections != null) { // fix for bug 35343
        result = true;
        //logger.warning("Terminating " + serverConnections.size() + "
        // connections");
        for (Iterator it = serverConnections.iterator(); it.hasNext();) {
          ServerConnection serverConnection = (ServerConnection)it.next();
          //logger.warning("Terminating " + serverConnection);
          serverConnection.handleTermination(timedOut);
        }
      }
    }
    return result;
  }

  protected boolean isAnyThreadProcessingMessage(ClientProxyMembershipID proxyID) {
    boolean processingMessage = false;
    synchronized (this._clientThreadsLock) {
      Set serverConnections = (Set) this._clientThreads.get(proxyID);
      if (serverConnections != null) {
        for (Iterator it = serverConnections.iterator(); it.hasNext();) {
          ServerConnection serverConnection = (ServerConnection) it.next();
          if (serverConnection.isProcessingMessage()) {
            processingMessage = true;
            break;
          }
        }
      }
    }
    return processingMessage;
  }

  protected void validateThreads(ClientProxyMembershipID proxyID) {
    Set serverConnections = null;
    synchronized (this._clientThreadsLock) {
       serverConnections = (Set) this._clientThreads.get(proxyID);
       if (serverConnections != null) {
         serverConnections = new HashSet(serverConnections);
       }
    }
    // release sync and operation on copy to fix bug 37675
    if (serverConnections != null) {
      for (Iterator it = serverConnections.iterator(); it.hasNext();) {
        ServerConnection serverConnection = (ServerConnection) it.next();
        if (serverConnection.hasBeenTimedOutOnClient()) {
          logger.warn(LocalizedMessage.create(
            LocalizedStrings.ClientHealtMonitor_0_IS_BEING_TERMINATED_BECAUSE_ITS_CLIENT_TIMEOUT_OF_1_HAS_EXPIRED,
              new Object[] {serverConnection, Integer.valueOf(serverConnection.getClientReadTimeout())}));                              
          try {
            serverConnection.handleTermination(true);
            // Not all the code in a ServerConnection correctly
            // handles interrupt. In particular it is possible to be doing
            // p2p distribution and to have sent a message to one peer but
            // to never send it to another due to interrupt.
            //serverConnection.interruptOwner();
          }
          finally {
            // Just to be sure we clean it up.
            // This call probably isn't needed.
            removeConnection(proxyID, serverConnection);
          }
        }
      }
    }
  }
  
  /**
   * Returns the map of known clients.
   * 
   * @return the map of known clients
   */
  public Map getClientHeartbeats()
  {
    return this._clientHeartbeats;
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
   * @param cache
   *          The GemFire <code>Cache</code>
   * @param maximumTimeBetweenPings
   *          The maximum time allowed between pings before determining the
   *          client has died and interrupting its sockets.
   */
  protected static synchronized void createInstance(Cache cache,
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
   * @param cache
   *          The GemFire <code>Cache</code>
   * @param maximumTimeBetweenPings
   *          The maximum time allowed between pings before determining the
   *          client has died and interrupting its sockets.
   */
  private ClientHealthMonitor(Cache cache, int maximumTimeBetweenPings, CacheClientNotifierStats stats) {
    // Set the Cache
    this._cache = cache;

    // Initialize the client threads map
    this._clientThreads = new HashMap();
    
    if (maximumTimeBetweenPings > 0) {
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Initializing client health monitor thread", this);
      }
      this._clientMonitor = new ClientHealthMonitorThread(maximumTimeBetweenPings);
      this._clientMonitor.start();
    } else {
      // LOG:CONFIG: changed from config to info
      logger.info(LocalizedMessage.create(LocalizedStrings.ClientHealthMonitor_CLIENT_HEALTH_MONITOR_THREAD_DISABLED_DUE_TO_MAXIMUMTIMEBETWEENPINGS_SETTING__0, maximumTimeBetweenPings));
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
  public String toString()
  {
    return "ClientHealthMonitor@"
      + Integer.toHexString(System.identityHashCode(this));
  }
  
  public Map getCleanupProxyIdTable() {
    return cleanupProxyIdTable;
  }
  
  public Map getCleanupTable() {
    return cleanupTable;
  }

  public int getNumberOfClientsAtVersion(Version version) {
    return numOfClientsPerVersion.get(version.ordinal());
  }

  // TODO assumes that version ordinals always increase as versions increase
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
   * Class <code>ClientHealthMonitorThread</code> is a <code>Thread</code>
   * that verifies all clients are still alive.
   */
  class ClientHealthMonitorThread extends Thread
   {

    /**
     * The maximum time allowed between pings before determining the client has
     * died and interrupting its sockets.
     */
    final protected int _maximumTimeBetweenPings;

    /**
     * Whether the monitor is stopped
     */
    protected volatile boolean _isStopped = false;

    /**
     * Constructor.
     * 
     * @param maximumTimeBetweenPings
     *          The maximum time allowed between pings before determining the
     *          client has died and interrupting its sockets
     */
    protected ClientHealthMonitorThread(int maximumTimeBetweenPings) {
      super(LoggingThreadGroup.createThreadGroup("ClientHealthMonitor Thread Group",
          logger), "ClientHealthMonitor Thread");
      setDaemon(true);

      // Set the client connection timeout
      this._maximumTimeBetweenPings = maximumTimeBetweenPings;
      // LOG:CONFIG: changed from config to info
      logger.info(LocalizedMessage.create(
          LocalizedStrings.ClientHealthMonitor_CLIENTHEALTHMONITORTHREAD_MAXIMUM_ALLOWED_TIME_BETWEEN_PINGS_0,
          this._maximumTimeBetweenPings));
      if (maximumTimeBetweenPings == 0) {
        if (logger.isDebugEnabled()) {
          logger.debug("zero ping interval detected",
            new Exception(LocalizedStrings.ClientHealthMonitor_STACK_TRACE_0.toLocalizedString()));
        }
      }
    }

    /**
     * Notifies the monitor to stop monitoring.
     */
    protected synchronized void stopMonitoring()
    {
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
    protected boolean isStopped()
    {
      return this._isStopped;
    }

    /**
     * Runs the monitor by iterating the map of clients and testing the latest
     * ping time received against the current time.
     */
    @Override
    public void run()
    {
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Beginning to monitor clients", ClientHealthMonitor.this);
      }

      while (!this._isStopped) {
        SystemFailure.checkFailure();
        try {
          Thread.sleep(CLIENT_MONITOR_INTERVAL);
          if (logger.isTraceEnabled()) {
            logger.trace("Monitoring {} client(s)", getClientHeartbeats().size());
          }
          //logger.warning("Monitoring " + getClientHeartbeats().size() +
          // " client(s).");

          // Get the current time
          long currentTime = System.currentTimeMillis();
          if (logger.isTraceEnabled()) {
            logger.trace("{} starting sweep at {}", ClientHealthMonitor.this, currentTime);
          }

          // Iterate through the clients and verify that they are all still
          // alive
          for (Iterator i = getClientHeartbeats().entrySet().iterator(); i
              .hasNext();) {
            Map.Entry entry = (Map.Entry)i.next();
            ClientProxyMembershipID proxyID = (ClientProxyMembershipID) entry.getKey();
            // Validate all ServerConnection threads. If a thread has been
            // processing a message for more than the socket timeout time,
            // close it it since the client will have timed out and resent.
            validateThreads(proxyID);
            
            Long latestHeartbeatValue = (Long)entry.getValue();
            // Compare the current value with the current time if it is not null
            // If it is null, that means that the client was just registered
            // and has not done a heartbeat yet.
            if (latestHeartbeatValue != null) {
              long latestHeartbeat = latestHeartbeatValue.longValue();
              if (logger.isTraceEnabled()) {
                logger.trace("{} ms have elapsed since the latest heartbeat for client with member id {}", (currentTime - latestHeartbeat), proxyID);
              }
              
              if ( (currentTime-latestHeartbeat) > this._maximumTimeBetweenPings) {
                // This client has been idle for too long. Determine whether
                // any of its ServerConnection threads are currently processing
                // a message. If so, let it go. If not, disconnect it.
                if (isAnyThreadProcessingMessage(proxyID)) {
                  if (logger.isDebugEnabled()) {
                    logger.debug("Monitoring client with member id {}. It has been {} ms since the latest heartbeat. This client would have been terminated but at least one of its threads is processing a message.", entry.getKey(), (currentTime - latestHeartbeat));
                  }
                } else {
                  if (cleanupClientThreads(proxyID, true)) {
                    logger.warn(LocalizedMessage.create(LocalizedStrings.ClientHealthMonitor_MONITORING_CLIENT_WITH_MEMBER_ID_0_IT_HAD_BEEN_1_MS_SINCE_THE_LATEST_HEARTBEAT_MAX_INTERVAL_IS_2_TERMINATED_CLIENT, new Object[] {entry.getKey(), currentTime - latestHeartbeat, this._maximumTimeBetweenPings}));
                  }
                }
              } else {
                if (logger.isTraceEnabled()) {
                  logger.trace("Monitoring client with member id {}. It has been {} ms since the latest heartbeat. This client is healthy.", entry.getKey(), (currentTime - latestHeartbeat));
                }
                //logger.warning("Monitoring client with member id " +
                // entry.getKey() + ". It has been " + (currentTime -
                // latestHeartbeat) + " ms since the latest heartbeat. This
                // client is healthy.");
              }
            }
          }
        }
        catch (InterruptedException e) {
          // no need to reset the bit; we're exiting
          if (this._isStopped) {
            break;
          }
          logger.warn(LocalizedMessage.create(LocalizedStrings.ClientHealthMonitor_UNEXPECTED_INTERRUPT_EXITING), e);
          break;
        }
        catch (Exception e) {
          // An exception occurred while monitoring the clients. If the monitor
          // is not stopped, log it and continue processing.
          if (!this._isStopped) {
            logger.fatal(LocalizedMessage.create(
                LocalizedStrings.ClientHealthMonitor_0_AN_UNEXPECTED_EXCEPTION_OCCURRED,
                ClientHealthMonitor.this), e);
          }
        }
      } // while
    }
  } // ClientHealthMonitorThread
}
