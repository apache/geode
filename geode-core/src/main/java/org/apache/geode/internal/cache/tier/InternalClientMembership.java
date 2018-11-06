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
package org.apache.geode.internal.cache.tier;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.SystemFailure;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.tier.sockets.AcceptorImpl;
import org.apache.geode.internal.cache.tier.sockets.ClientHealthMonitor;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.LoggingExecutors;
import org.apache.geode.management.membership.ClientMembershipEvent;
import org.apache.geode.management.membership.ClientMembershipListener;

/**
 * Handles registration and event notification duties for <code>ClientMembershipListener</code>s.
 * The public counterpart for this class is
 * {@link org.apache.geode.management.membership.ClientMembership}.
 *
 * @since GemFire 4.2.1
 */
public class InternalClientMembership {

  private static final Logger logger = LogService.getLogger();

  /**
   * The membership listeners registered on this InternalClientMembership
   *
   * This list is never modified in place, and a new list is installed only under the control of
   * (@link #membershipLock}.
   */
  private static volatile List<ClientMembershipListener> clientMembershipListeners =
      Collections.emptyList();

  /**
   * Must be locked whenever references to the volatile field {@link #clientMembershipListeners} is
   * changed.
   */
  private static final Object membershipLock = new Object();

  /**
   * QueuedExecutor for firing ClientMembershipEvents
   *
   * Access synchronized via {@link #systems}
   */
  private static ExecutorService executor;

  /** List of connected <code>DistributedSystem</code>s */
  private static final List systems = new ArrayList(1);

  /**
   * True if class is monitoring systems
   *
   * guarded.By InternalClientMembership.class
   */
  private static boolean isMonitoring = false;

  /**
   * This work used to be in a class initializer. Unfortunately, this allowed the class to escape
   * before it was fully initialized, so now we just make sure this work is done before any public
   * static method on it is invoked.
   */
  private static synchronized void startMonitoring() {
    if (isMonitoring) {
      return;
    }

    synchronized (systems) {
      // Initialize our own list of distributed systems via a connect listener
      List existingSystems = InternalDistributedSystem
          .addConnectListener(new InternalDistributedSystem.ConnectListener() {
            public void onConnect(InternalDistributedSystem sys) {
              addInternalDistributedSystem(sys);
            }
          });

      isMonitoring = true;

      // While still holding the lock on systems, add all currently known
      // systems to our own list
      for (Iterator iter = existingSystems.iterator(); iter.hasNext();) {
        InternalDistributedSystem sys = (InternalDistributedSystem) iter.next();
        try {
          if (sys.isConnected()) {
            addInternalDistributedSystem(sys);
          }
        } catch (DistributedSystemDisconnectedException e) {
          // it doesn't care (bug 37379)
        }
      }

    } // synchronized
  }

  private InternalClientMembership() {}

  /**
   * Registers a {@link ClientMembershipListener} for notification of connection changes for
   * CacheServer and clients.
   *
   * @param listener a ClientMembershipListener to be registered
   */
  public static void registerClientMembershipListener(ClientMembershipListener listener) {
    startMonitoring();
    synchronized (membershipLock) {
      List<ClientMembershipListener> oldListeners = clientMembershipListeners;
      if (!oldListeners.contains(listener)) {
        List<ClientMembershipListener> newListeners =
            new ArrayList<ClientMembershipListener>(oldListeners);
        newListeners.add(listener);
        clientMembershipListeners = newListeners;
      }
    }
  }

  /**
   * Removes registration of a previously registered {@link ClientMembershipListener}.
   *
   * @param listener a ClientMembershipListener to be unregistered
   */
  public static void unregisterClientMembershipListener(ClientMembershipListener listener) {
    startMonitoring();
    synchronized (membershipLock) {
      List<ClientMembershipListener> oldListeners = clientMembershipListeners;
      if (oldListeners.contains(listener)) {
        List<ClientMembershipListener> newListeners =
            new ArrayList<ClientMembershipListener>(oldListeners);
        if (newListeners.remove(listener)) {
          clientMembershipListeners = newListeners;
        }
      }
    }
  }

  /**
   * Returns an array of all the currently registered <code>ClientMembershipListener</code>s.
   * Modifications to the returned array will not effect the registration of these listeners.
   *
   * @return the registered <code>ClientMembershipListener</code>s; an empty array if no listeners
   */
  public static ClientMembershipListener[] getClientMembershipListeners() {
    startMonitoring();
    // Synchronization is not needed because we never modify this list
    // in place.

    List<ClientMembershipListener> l = clientMembershipListeners; // volatile fetch
    // convert to an array
    ClientMembershipListener[] listeners =
        (ClientMembershipListener[]) l.toArray(new ClientMembershipListener[l.size()]);
    return listeners;
  }

  /**
   * Removes registration of all currently registered <code>ClientMembershipListener<code>s. and
   * <code>ClientMembershipListener<code>s.
   */
  public static void unregisterAllListeners() {
    startMonitoring();
    synchronized (membershipLock) {
      clientMembershipListeners = new ArrayList<ClientMembershipListener>();
    }
  }



  /**
   * Returns a map of client memberIds to count of connections to that client. The map entry key is
   * a String representation of the client memberId, and the map entry value is an Integer count of
   * connections to that client. Since a single client can have multiple ConnectionProxy objects,
   * this map will contain all the Connection objects across the ConnectionProxies
   *
   * @param onlyClientsNotifiedByThisServer true will return only those clients that are actively
   *        being updated by this server
   * @return map of client memberIds to count of connections to that client
   *
   *
   */
  public static Map getConnectedClients(boolean onlyClientsNotifiedByThisServer,
      InternalCache cache) {
    ClientHealthMonitor chMon = ClientHealthMonitor.getInstance();
    Set filterProxyIDs = null;
    if (onlyClientsNotifiedByThisServer) {
      // Note it is not necessary to synchronize on the list of Client servers here,
      // since this is only a status (snapshot) of the system.
      for (Iterator bsii = cache.getCacheServers().iterator(); bsii.hasNext();) {
        CacheServerImpl bsi = (CacheServerImpl) bsii.next();
        AcceptorImpl ai = bsi.getAcceptor();
        if (ai != null && ai.getCacheClientNotifier() != null) {
          if (filterProxyIDs != null) {
            // notifierClients is a copy set from CacheClientNotifier
            filterProxyIDs.addAll(ai.getCacheClientNotifier().getActiveClients());
          } else {
            // notifierClients is a copy set from CacheClientNotifier
            filterProxyIDs = ai.getCacheClientNotifier().getActiveClients();
          }
        }
      }
    }

    Map map = chMon.getConnectedClients(filterProxyIDs);
    /*
     * if (onlyClientsNotifiedByThisServer) { Map notifyMap = new HashMap();
     *
     * for (Iterator iter = map.keySet().iterator(); iter.hasNext();) { String memberId = (String)
     * iter.next(); if (notifierClients.contains(memberId)) { // found memberId that is notified by
     * this server notifyMap.put(memberId, map.get(memberId)); } } map = notifyMap; }
     */
    return map;
  }

  /**
   * This method returns the CacheClientStatus for all the clients that are connected to this
   * server. This method returns all clients irrespective of whether subscription is enabled or not.
   *
   * @return Map of ClientProxyMembershipID against CacheClientStatus objects.
   */
  public static Map getStatusForAllClientsIgnoreSubscriptionStatus() {
    Map result = new HashMap();
    if (ClientHealthMonitor.getInstance() != null)
      result = ClientHealthMonitor.getInstance().getStatusForAllClients();

    return result;
  }

  /**
   * Caller must synchronize on cache.allClientServersLock
   *
   * @return all the clients
   */
  public static Map getConnectedClients(InternalCache cache) {

    // Get all clients
    Map allClients = new HashMap();
    for (Iterator bsii = cache.getCacheServers().iterator(); bsii.hasNext();) {
      CacheServerImpl bsi = (CacheServerImpl) bsii.next();
      AcceptorImpl ai = bsi.getAcceptor();
      if (ai != null && ai.getCacheClientNotifier() != null) {
        allClients.putAll(ai.getCacheClientNotifier().getAllClients());
      }
    }

    // Fill in the missing info, if HealthMonitor started
    if (ClientHealthMonitor.getInstance() != null)
      ClientHealthMonitor.getInstance().fillInClientInfo(allClients);

    return allClients;
  }

  public static Map getClientQueueSizes(final InternalCache cache) {
    if (cache == null) {
      return Collections.emptyMap();
    }

    Map clientQueueSizes = new HashMap();
    for (CacheServer cacheServer : cache.getCacheServers()) {
      CacheServerImpl cacheServerImpl = (CacheServerImpl) cacheServer;
      AcceptorImpl acceptor = cacheServerImpl.getAcceptor();
      if (acceptor != null && acceptor.getCacheClientNotifier() != null) {
        clientQueueSizes.putAll(acceptor.getCacheClientNotifier().getClientQueueSizes());
      }
    }
    return clientQueueSizes;
  }

  /**
   * Returns a map of servers to count of pools connected to that server. The map entry key is a
   * String representation of the server,
   *
   * @return map of servers to count of pools using that server
   */
  public static Map getConnectedServers() {
    final Map map = new HashMap(); // KEY:server (String), VALUE:List of active endpoints
    // returns an unmodifiable set
    Map/* <String,Pool> */ poolMap = PoolManager.getAll();
    Iterator pools = poolMap.values().iterator();
    while (pools.hasNext()) {
      PoolImpl pi = (PoolImpl) pools.next();
      Map/* <ServerLocation,Endpoint> */ eps = pi.getEndpointMap();
      Iterator it = eps.entrySet().iterator();
      while (it.hasNext()) {
        Map.Entry entry = (Map.Entry) it.next();
        ServerLocation loc = (ServerLocation) entry.getKey();
        org.apache.geode.cache.client.internal.Endpoint ep =
            (org.apache.geode.cache.client.internal.Endpoint) entry.getValue();
        String server = loc.getHostName() + "[" + loc.getPort() + "]";
        Integer count = (Integer) map.get(server);
        if (count == null) {
          map.put(server, Integer.valueOf(1));
        } else {
          map.put(server, Integer.valueOf(count.intValue() + 1));
        }
      }
    }
    return map;
  }

  public static Map getConnectedIncomingGateways() {
    Map connectedIncomingGateways = null;
    ClientHealthMonitor chMon = ClientHealthMonitor.getInstance();
    if (chMon == null) {
      connectedIncomingGateways = new HashMap();
    } else {
      connectedIncomingGateways = chMon.getConnectedIncomingGateways();
    }
    return connectedIncomingGateways;
  }



  /**
   * Notifies registered listeners that a Client member has connected
   *
   * @param clientId the representing the client
   */
  public static void notifyClientJoined(final DistributedMember clientId) {
    notifyListeners(clientId, true, EventType.JOINED);
  }

  /**
   * Notifies registered listeners that a Client member has left
   *
   * @param clientId the representing the client
   */
  public static void notifyClientLeft(final DistributedMember clientId) {
    notifyListeners(clientId, true, EventType.LEFT);
  }

  /**
   * Notifies registered listeners that a Client member has crashed
   *
   * @param clientId the representing the client
   */
  public static void notifyClientCrashed(final DistributedMember clientId) {
    notifyListeners(clientId, true, EventType.CRASHED);
  }



  /**
   * Notifies registered listeners that a Client member has connected
   *
   * @param location the address of the server
   */
  public static void notifyServerJoined(final ServerLocation location) {
    DistributedMember id = new InternalDistributedMember(location);
    notifyListeners(id, false, EventType.JOINED);
  }

  /**
   * Notifies registered listeners that a Client member has left
   *
   * @param location the address of the server
   */
  public static void notifyServerLeft(final ServerLocation location) {
    DistributedMember id = new InternalDistributedMember(location);
    notifyListeners(id, false, EventType.LEFT);
  }

  /**
   * Notifies registered listeners that a Client member has crashed
   *
   * @param location the address of the server
   */
  public static void notifyServerCrashed(final ServerLocation location) {
    DistributedMember id = new InternalDistributedMember(location);
    notifyListeners(id, false, EventType.CRASHED);
  }



  /**
   * Notifies registered listeners that a Client member has joined. The new member may be a client
   * connecting to this process or a server that this process has just connected to.
   *
   * @param member the <code>DistributedMember</code>
   * @param client true if the member is a client; false if server
   * @param typeOfEvent joined/left/crashed
   */
  private static void notifyListeners(final DistributedMember member, final boolean client,
      final EventType typeOfEvent) {
    startMonitoring();
    ExecutorService queuedExecutor = executor;
    if (queuedExecutor == null) {
      return;
    }

    final ClientMembershipEvent event = new InternalClientMembershipEvent(member, client);
    if (forceSynchronous) {
      doNotifyClientMembershipListener(member, client, event, typeOfEvent);
    } else {
      try {
        queuedExecutor.execute(() -> {
          doNotifyClientMembershipListener(member, client, event, typeOfEvent);
        });
      } catch (RejectedExecutionException e) {
        // executor must have been shutdown
      }
    }
  }


  private static void doNotifyClientMembershipListener(DistributedMember member, boolean client,
      ClientMembershipEvent clientMembershipEvent, EventType eventType) {

    for (Iterator<ClientMembershipListener> iter = clientMembershipListeners.iterator(); iter
        .hasNext();) {

      ClientMembershipListener listener = iter.next();
      try {
        if (eventType.equals(EventType.JOINED)) {
          listener.memberJoined(clientMembershipEvent);
        } else if (eventType.equals(EventType.LEFT)) {
          listener.memberLeft(clientMembershipEvent);
        } else {
          listener.memberCrashed(clientMembershipEvent);
        }
      } catch (CancelException e) {
        // this can be thrown by a server when the system is shutting
        // down
        return;
      } catch (VirtualMachineError e) {
        SystemFailure.initiateFailure(e);
        throw e;
      } catch (Throwable t) {
        SystemFailure.checkFailure();
        logger.warn("unexpected exception", t);
      }
    }
  }

  // /**
  // * Returns true if there are any registered
  // * <code>ClientMembershipListener</code>s.
  // */
  // private static boolean hasClientMembershipListeners() {
  // synchronized (membershipLock) {
  // return !membershipListeners.isEmpty();
  // }
  // }

  protected static void addInternalDistributedSystem(InternalDistributedSystem s) {
    synchronized (systems) {
      s.addDisconnectListener(new InternalDistributedSystem.DisconnectListener() {
        @Override
        public String toString() {
          return "Disconnect listener for InternalClientMembership";
        }

        public void onDisconnect(InternalDistributedSystem ss) {
          removeInternalDistributedSystem(ss);
        }
      });
      systems.add(s);
      // make sure executor is alive
      ensureExecutorIsRunning(); // optimized to do nothing if already running
    }
  }

  protected static void removeInternalDistributedSystem(InternalDistributedSystem sys) {
    synchronized (systems) {
      systems.remove(sys);
      if (systems.isEmpty()) {
        // clean up executor
        /*
         * Object[] queueElementsBefore = new Object[executorQueue.size()]; queueElementsBefore =
         * executorQueue.toArray(queueElementsBefore);
         * System.out.println("Before shut down, the executor's queue contains the following " +
         * queueElementsBefore.length + " elements"); for (int i=0; i<queueElementsBefore.length;
         * i++) { System.out.println("\t" + queueElementsBefore[i]); }
         */
        if (executor != null) {
          executor.shutdown();
        }
        /*
         * Object[] queueElementsAfter = new Object[executorQueue.size()]; queueElementsAfter =
         * executorQueue.toArray(queueElementsAfter);
         * System.out.println("After shut down, the executor's queue contains the following " +
         * queueElementsAfter.length + " elements"); for (int i=0; i<queueElementsAfter.length; i++)
         * { System.out.println("\t" + queueElementsAfter[i]); }
         */
        // deadcoded this clear to fix bug 35675 - clearing removed the shutdown token from the
        // queue!
        // executorQueue.clear();
        executor = null;
      }
    }
  }

  /**
   * guarded.By {@link #systems}
   */
  private static void ensureExecutorIsRunning() {
    // protected by calling method synchronized on systems
    if (executor == null) {
      executor =
          LoggingExecutors.newFixedThreadPoolWithTimeout("ClientMembership Event Invoker", 1, 15);
    }
  }

  /**
   * Internal implementation of ClientMembershipEvent. This class is used in both clients and
   * servers to notify listeners about events concerning client/server communications. In servers
   * listeners are notified about clients connecting/disconnecting. In clients listeners notified
   * about new connections being formed to servers. The instance variable "client" is used to
   * distinguish between the two.
   */
  protected static class InternalClientMembershipEvent implements ClientMembershipEvent {

    private final DistributedMember member;

    /**
     * If true this means that a client has joined/left/crashed.
     * <p>
     * If false this means that a server has joined/left/crashed
     */
    private final boolean client;

    protected InternalClientMembershipEvent(DistributedMember member, boolean isClient) {
      this.member = member;
      this.client = isClient;
    }

    public DistributedMember getMember() {
      return this.member;
    }

    public String getMemberId() {
      return this.member == null ? "unknown" : this.member.getId();
    }

    public boolean isClient() {
      return this.client;
    }

    @Override // GemStoneAddition
    public String toString() {
      final StringBuffer sb = new StringBuffer("[ClientMembershipEvent: ");
      sb.append("member=").append(this.member);
      sb.append(", isClient=").append(this.client);
      sb.append("]");
      return sb.toString();
    }
  }

  /** If set to true for testing then notification will be synchronous */
  private static boolean forceSynchronous = false;

  /** Set to true if synchronous notification is needed for testing */
  public static void setForceSynchronous(boolean value) {
    forceSynchronous = value;
  }

  private static enum EventType {
    JOINED, LEFT, CRASHED
  }
}
