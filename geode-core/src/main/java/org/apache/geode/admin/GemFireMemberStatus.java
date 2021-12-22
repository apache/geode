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
package org.apache.geode.admin;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.admin.ClientHealthMonitoringRegion;
import org.apache.geode.internal.admin.remote.ClientHealthStats;
import org.apache.geode.internal.cache.CacheClientStatus;
import org.apache.geode.internal.cache.HARegion;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionStatus;
import org.apache.geode.internal.cache.RegionStatus;
import org.apache.geode.internal.cache.tier.InternalClientMembership;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.inet.LocalHostUtil;

/**
 * Class <code>GemFireMemberStatus</code> provides the status of a specific GemFire member VM. This
 * VM can be a peer, a client, a server and/or a gateway.
 *
 * @deprecated as of 7.0 use the <code><a href=
 *             "{@docRoot}/org/apache/geode/management/package-summary.html">management</a></code>
 *             package instead
 */
public class GemFireMemberStatus implements Serializable {
  private static final long serialVersionUID = 3389997790525991310L;

  /**
   * Notifies whether this member is a client to a cache server.
   */
  protected boolean _isClient;

  /**
   * Notifies whether this member is a cache server.
   */
  protected boolean _isServer;

  /**
   * Notifies whether this member is a hub for WAN gateways.
   */
  protected boolean _isGatewayHub;

  /**
   * Notifies whether this member is a locator.
   */
  protected boolean _isLocator;

  protected boolean _isPrimaryGatewayHub;

  protected Object/* GatewayHubStatus */ _gatewayHubStatus;

  protected boolean _isConnected;
  protected Serializable _memberId;
  protected Set _connectedPeers;
  protected Set _connectedServers;
  protected Set _unconnectedServers;
  protected Set _connectedClients;
  protected Map _connectedIncomingGateways;
  protected Map _outgoingGateways;

  protected Map _clientHostNames;
  protected Map _clientQueueSizes;
  protected Map _gatewayQueueSizes;
  protected Map _regionStatuses;
  protected Map _clientHealthStats;

  protected String _memberName;
  protected int _mcastPort;
  protected int _serverPort;
  protected InetAddress _mcastAddress;
  protected String _bindAddress;
  protected String _locators;
  protected InetAddress _hostAddress;

  protected long _maximumHeapSize;
  protected long _freeHeapSize;

  protected long upTime = -1;

  protected final transient Cache cache;

  public GemFireMemberStatus() {
    this(null);
  }

  public GemFireMemberStatus(Cache cache) {
    this.cache = cache;
    DistributedSystem ds = null;
    if (cache != null) {
      ds = cache.getDistributedSystem();
    }
    initialize(ds);
  }

  public boolean getIsConnected() {
    return _isConnected;
  }

  protected void setIsConnected(boolean isConnected) {
    _isConnected = isConnected;
  }

  /**
   * Returns whether this member is a client to a cache server
   *
   * @return whether this member is a client to a cache server
   */
  public boolean getIsClient() {
    return _isClient;
  }

  /**
   * Sets whether this member is a client to a cache server
   *
   * @param isClient Boolean defining whether this member is a client to a cache server
   */
  protected void setIsClient(boolean isClient) {
    _isClient = isClient;
  }

  /**
   * Returns whether this member is a cache server
   *
   * @return whether this member is a cache server
   */
  public boolean getIsServer() {
    return _isServer;
  }

  /**
   * Sets whether this member is a cache server
   *
   * @param isServer Boolean defining whether this member is a cache server
   */
  protected void setIsServer(boolean isServer) {
    _isServer = isServer;
  }

  public int getServerPort() {
    return _serverPort;
  }

  protected void setServerPort(int port) {
    _serverPort = port;
  }

  /**
   * Returns whether this member is a hub for WAN gateways
   *
   * @return whether this member is a hub for WAN gateways
   */
  public boolean getIsGatewayHub() {
    return _isGatewayHub;
  }

  /**
   * Sets whether this member is a cache server
   *
   * @param isGatewayHub Boolean defining whether this member is a hub for WAN gateways
   */
  protected void setIsGatewayHub(boolean isGatewayHub) {
    _isGatewayHub = isGatewayHub;
  }

  public boolean getIsLocator() {
    return _isLocator;
  }

  protected void setIsLocator(boolean isLocator) {
    _isLocator = isLocator;
  }

  public boolean getIsPrimaryGatewayHub() {
    return _isPrimaryGatewayHub;
  }

  protected void setIsPrimaryGatewayHub(boolean isPrimaryGatewayHub) {
    _isPrimaryGatewayHub = isPrimaryGatewayHub;
  }

  /**
   * For internal use only
   *
   * @return status of the gateway hub
   */
  public Object getGatewayHubStatus() {
    return _gatewayHubStatus;
  }

  public boolean getIsSecondaryGatewayHub() {
    return !_isPrimaryGatewayHub;
  }

  public Set getConnectedPeers() {
    return _connectedPeers;
  }

  protected void setConnectedPeers(Set connectedPeers) {
    _connectedPeers = connectedPeers;
  }

  public Set getConnectedServers() {
    return _connectedServers;
  }

  protected void setConnectedServers(Set connectedServers) {
    _connectedServers = connectedServers;
  }

  protected void addConnectedServer(String connectedServer) {
    _connectedServers.add(connectedServer);
  }

  public Set getUnconnectedServers() {
    return _unconnectedServers;
  }

  protected void setUnconnectedServers(Set unconnectedServers) {
    _unconnectedServers = unconnectedServers;
  }

  protected void addUnconnectedServer(String unconnectedServer) {
    _unconnectedServers.add(unconnectedServer);
  }

  public Set getConnectedClients() {
    return _connectedClients;
  }

  protected void addConnectedClient(String connectedClient) {
    _connectedClients.add(connectedClient);
  }

  public Map getOutgoingGateways() {
    return _outgoingGateways;
  }

  public Map getConnectedIncomingGateways() {
    return _connectedIncomingGateways;
  }

  protected void setConnectedIncomingGateways(Map connectedIncomingGateways) {
    _connectedIncomingGateways = connectedIncomingGateways;
  }

  public Map getClientQueueSizes() {
    return _clientQueueSizes;
  }

  protected void setClientQueueSizes(Map clientQueueSizes) {
    _clientQueueSizes = clientQueueSizes;
  }

  public int getClientQueueSize(String clientMemberId) {
    Integer clientQueueSize = (Integer) getClientQueueSizes().get(clientMemberId);
    return clientQueueSize == null ? 0 : clientQueueSize;
  }

  protected void putClientQueueSize(String clientMemberId, int size) {
    getClientQueueSizes().put(clientMemberId, Integer.valueOf(size));
  }

  public Map getClientHealthStats() {
    return _clientHealthStats;
  }

  protected void setClientHealthStats(Map stats) {
    _clientHealthStats = stats;
  }

  /**
   * For internal use only
   *
   * @param clientID client for health
   * @return the client's health
   */
  public Object/* ClientHealthStats */ getClientHealthStats(String clientID) {
    return _clientHealthStats.get(clientID);
  }

  protected void setClientHealthStats(String clientID, ClientHealthStats stats) {
    _clientHealthStats.put(clientID, stats);
  }

  protected void putClientHostName(String clientId, String hostName) {
    _clientHostNames.put(clientId, hostName);
  }

  public String getClientHostName(String clientId) {
    return (String) _clientHostNames.get(clientId);
  }

  public Map getRegionStatuses() {
    return _regionStatuses;
  }

  /**
   * For internal use only
   *
   * @param fullRegionPath region path
   * @return status for the region
   */
  public Object/* RegionStatus */ getRegionStatus(String fullRegionPath) {
    return getRegionStatuses().get(fullRegionPath);
  }

  protected void putRegionStatus(String fullRegionPath, RegionStatus status) {
    getRegionStatuses().put(fullRegionPath, status);
  }

  public Serializable getMemberId() {
    return _memberId;
  }

  protected void setMemberId(Serializable memberId) {
    _memberId = memberId;
  }

  public String getMemberName() {
    return _memberName;
  }

  protected void setMemberName(String memberName) {
    _memberName = memberName;
  }

  public int getMcastPort() {
    return _mcastPort;
  }

  protected void setMcastPort(int mcastPort) {
    _mcastPort = mcastPort;
  }

  public InetAddress getMcastAddress() {
    return _mcastAddress;
  }

  protected void setMcastAddress(InetAddress mcastAddress) {
    _mcastAddress = mcastAddress;
  }

  public InetAddress getHostAddress() {
    return _hostAddress;
  }

  protected void setHostAddress(InetAddress hostAddress) {
    _hostAddress = hostAddress;
  }

  public String getBindAddress() {
    return _bindAddress;
  }

  protected void setBindAddress(String bindAddress) {
    _bindAddress = bindAddress;
  }

  public String getLocators() {
    return _locators;
  }

  protected void setLocators(String locators) {
    _locators = locators;
  }

  public long getMaximumHeapSize() {
    return _maximumHeapSize;
  }

  protected void setMaximumHeapSize(long size) {
    _maximumHeapSize = size;
  }

  public long getFreeHeapSize() {
    return _freeHeapSize;
  }

  protected void setFreeHeapSize(long size) {
    _freeHeapSize = size;
  }

  public long getUsedHeapSize() {
    return getMaximumHeapSize() - getFreeHeapSize();
  }

  public long getUpTime() {
    return upTime;
  }

  public void setUpTime(long upTime) {
    this.upTime = upTime;
  }

  @Override
  public String toString() {
    StringBuilder buffer = new StringBuilder();
    buffer.append("GemFireMemberStatus[").append("isConnected=").append(_isConnected)
        .append("; memberName=").append(_memberName).append("; memberId=")
        .append(_memberId).append("; hostAddress=").append(_hostAddress)
        .append("; mcastPort=").append(_mcastPort).append("; mcastAddress=")
        .append(_mcastAddress).append("; bindAddress=").append(_bindAddress)
        .append("; serverPort=").append(_serverPort).append("; locators=")
        .append(_locators).append("; isClient=").append(_isClient).append("; isServer=")
        .append(_isServer).append("; isGatewayHub=").append(_isGatewayHub)
        .append("; isLocator=").append(_isLocator).append("; isPrimaryGatewayHub=")
        .append(_isPrimaryGatewayHub).append("; gatewayHubStatus=")
        .append(_gatewayHubStatus).append("; connectedPeers=").append(_connectedPeers)
        .append("; connectedServers=").append(_connectedServers)
        .append("; unconnectedServers=").append(_unconnectedServers)
        .append("; connectedClients=").append(_connectedClients).append("; clientHostNames=")
        .append(_clientHostNames).append("; clientQueueSizes=").append(_clientQueueSizes)
        .append("; clientHealthStats=").append(_clientHealthStats)
        .append("; gatewayQueueSizes=").append(_gatewayQueueSizes).append("; regionStatuses=")
        .append(_regionStatuses).append("; maximumHeapSize=").append(_maximumHeapSize)
        .append("; freeHeapSize=").append(_freeHeapSize).append("; upTime=")
        .append(upTime).append("]");
    return buffer.toString();
  }

  protected void initialize(DistributedSystem distributedSystem) {
    // Initialize instance variables
    initializeInstanceVariables();

    // If the cache is set, initialize the status.
    // If the cache is not set, then this is most
    // likely an unconnected status.
    if (cache != null) {
      // Initialize server
      initializeServer();

      // Initialize client
      initializeClient();

      // Initialize region sizes
      initializeRegionSizes();
    }

    if (distributedSystem != null) {
      // Initialize all
      initializeAll(distributedSystem);
    }

    // If this is a locator, initialize the locator status
    if (Locator.getLocators().size() > 0) {
      setIsLocator(true);
    }
  }

  protected void initializeInstanceVariables() {
    // Variables for servers
    _connectedClients = new HashSet();
    _clientQueueSizes = new HashMap();
    _clientHealthStats = new HashMap();
    _clientHostNames = new HashMap();

    // Variables for gateway hubs
    _outgoingGateways = new HashMap();
    _connectedIncomingGateways = new HashMap();
    _gatewayQueueSizes = new HashMap();

    // Variables for clients
    _connectedServers = new HashSet();
    _unconnectedServers = new HashSet();

    // Variables for all
    _connectedPeers = new HashSet();
    _regionStatuses = new HashMap();
  }

  protected void initializeServer() {
    Collection servers = cache.getCacheServers();
    if (servers.size() == 0) {
      setIsServer(false);
    } else {
      setIsServer(true);

      // Get connected clients.
      // The following method returns a map of client member id to a cache
      // client info. For now, keep track of the member ids in the set of
      // _connectedClients.
      Map allConnectedClients =
          InternalClientMembership.getStatusForAllClientsIgnoreSubscriptionStatus();
      for (final Object o : allConnectedClients.values()) {
        CacheClientStatus ccs = (CacheClientStatus) o;
        addConnectedClient(ccs.getMemberId());
        // host address is available directly by id, hence CacheClientStatus need not be populated
        putClientHostName(ccs.getMemberId(), ccs.getHostAddress());
      }

      // Get client queue sizes
      Map clientQueueSize =
          getClientIDMap(InternalClientMembership.getClientQueueSizes((InternalCache) cache));
      setClientQueueSizes(clientQueueSize);

      // Set server acceptor port (set it based on the first CacheServer)
      CacheServer server = (CacheServer) servers.toArray()[0];
      setServerPort(server.getPort());

      // Get Client Health Stats
      Region clientHealthMonitoringRegion =
          ClientHealthMonitoringRegion.getInstance((InternalCache) cache);
      if (clientHealthMonitoringRegion != null) {
        String[] clients = (String[]) clientHealthMonitoringRegion.keySet().toArray(new String[0]);
        for (String clientId : clients) {
          ClientHealthStats stats = (ClientHealthStats) clientHealthMonitoringRegion.get(clientId);
          setClientHealthStats(clientId, stats);
        }
      }
    }
  }

  /**
   * returning Map of client queue size against client Id
   *
   * param clientMap is a Map of client queue size against ClientProxyMembershipID
   */
  private Map getClientIDMap(Map ClientProxyMembershipIDMap) {
    Map clientIdMap = new HashMap();
    Set entrySet = ClientProxyMembershipIDMap.entrySet();
    for (final Object o : entrySet) {
      Map.Entry entry = (Map.Entry) o;
      ClientProxyMembershipID key = (ClientProxyMembershipID) entry.getKey();
      Integer size = (Integer) entry.getValue();
      clientIdMap.put(key.getDSMembership(), size);
    }
    return clientIdMap;
  }

  protected void initializeClient() {
    Map poolMap = PoolManager.getAll();
    if (poolMap.size() == 0) {
      setIsClient(false);
    } else {
      setIsClient(true);

      // Get connected servers.
      // The following method returns a map of server name to a count of logical
      // connections. A logical connection will be made for each region that
      // references the live server. If the client is not connected to the server,
      // the logical connections for that server will be 0. For now, keep track
      // of the keys (server names) of this map in the sets of _connectedServers
      // and _unconnectedServers.
      Map connectedServers = InternalClientMembership.getConnectedServers();
      if (!connectedServers.isEmpty()) {
        for (final Object o : connectedServers.entrySet()) {
          Map.Entry entry = (Map.Entry) o;
          String server = (String) entry.getKey();
          addConnectedServer(server);
        }
      }
    }
  }

  protected void initializeAll(DistributedSystem distributedSystem) {
    // Initialize isConnected
    setIsConnected(true);

    // Initialize distributed system status
    initializeDistributedSystem(distributedSystem);

    // Initialize peers
    initializePeers(distributedSystem);

    // Initialize memory
    initializeMemory();
  }

  protected void initializeDistributedSystem(DistributedSystem distributedSystem) {
    InternalDistributedSystem ids = (InternalDistributedSystem) distributedSystem;
    setMemberId(ids.getMemberId());
    DistributionConfig config = ids.getConfig();
    setMemberName(config.getName());
    setMcastPort(config.getMcastPort());
    setMcastAddress(config.getMcastAddress());
    String bindAddress = config.getBindAddress();
    setBindAddress(bindAddress);
    setLocators(config.getLocators());
    setUpTime(System.currentTimeMillis() - ids.getStartTime());
    try {
      setHostAddress((bindAddress != null && bindAddress.length() > 0)
          ? InetAddress.getByName(bindAddress) : LocalHostUtil.getLocalHost());
    } catch (IOException e) {
      /* ignore - leave null host address */}
  }

  protected void initializePeers(DistributedSystem distributedSystem) {
    InternalDistributedSystem ids = (InternalDistributedSystem) distributedSystem;
    DistributionManager dm = ids.getDistributionManager();
    Set connections = dm.getOtherNormalDistributionManagerIds();
    Set connectionsIDs = new HashSet(connections.size());
    for (final Object connection : connections) {
      InternalDistributedMember idm = (InternalDistributedMember) connection;
      connectionsIDs.add(idm.getId());
    }
    setConnectedPeers(connectionsIDs);
  }

  protected void initializeMemory() {
    Runtime rt = Runtime.getRuntime();
    setMaximumHeapSize(rt.maxMemory());
    setFreeHeapSize(rt.freeMemory());
  }

  protected void initializeRegionSizes() {

    for (final Region<?, ?> region : cache.rootRegions()) {
      LocalRegion rootRegion = (LocalRegion) region;
      if (!(rootRegion instanceof HARegion)) {
        RegionStatus rootRegionStatus = rootRegion instanceof PartitionedRegion
            ? new PartitionedRegionStatus((PartitionedRegion) rootRegion)
            : new RegionStatus(rootRegion);
        putRegionStatus(rootRegion.getFullPath(), rootRegionStatus);
        for (final Object o : rootRegion.subregions(true)) {
          LocalRegion subRegion = (LocalRegion) o;
          RegionStatus subRegionStatus = subRegion instanceof PartitionedRegion
              ? new PartitionedRegionStatus((PartitionedRegion) subRegion)
              : new RegionStatus(subRegion);
          putRegionStatus(subRegion.getFullPath(), subRegionStatus);
        }
      }
    }
  }
}
