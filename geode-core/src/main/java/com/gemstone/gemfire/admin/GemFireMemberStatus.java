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
package com.gemstone.gemfire.admin;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.net.SocketCreator;
import com.gemstone.gemfire.internal.admin.ClientHealthMonitoringRegion;
import com.gemstone.gemfire.internal.admin.remote.ClientHealthStats;
import com.gemstone.gemfire.internal.cache.*;
import com.gemstone.gemfire.internal.cache.tier.InternalClientMembership;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.*;

/**
 * Class <code>GemFireMemberStatus</code> provides the status of a specific
 * GemFire member VM. This VM can be a peer, a client, a server and/or a
 * gateway.
 * @deprecated as of 7.0 use the <code><a href="{@docRoot}/com/gemstone/gemfire/management/package-summary.html">management</a></code> package instead
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

  protected Object/*GatewayHubStatus*/ _gatewayHubStatus;

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

  protected transient final Cache cache;
  
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
    return this._isConnected;
  }

  protected void setIsConnected(boolean isConnected) {
    this._isConnected = isConnected;
  }

  /**
   * Returns whether this member is a client to a cache server
   * @return whether this member is a client to a cache server
   */
  public boolean getIsClient() {
    return this._isClient;
  }

  /**
   * Sets whether this member is a client to a cache server
   * @param isClient Boolean defining whether this member is a client to a
   * cache server
   */
  protected void setIsClient(boolean isClient) {
    this._isClient = isClient;
  }

  /**
   * Returns whether this member is a cache server
   * @return whether this member is a cache server
   */
  public boolean getIsServer() {
    return this._isServer;
  }

  /**
   * Sets whether this member is a cache server
   * @param isServer Boolean defining whether this member is a cache server
   */
  protected void setIsServer(boolean isServer) {
    this._isServer = isServer;
  }

  public int getServerPort() {
	  return this._serverPort;
  }
  
  protected void setServerPort(int port) {
	  this._serverPort = port;
  }
  
  /**
   * Returns whether this member is a hub for WAN gateways
   * @return whether this member is a hub for WAN gateways
   */
  public boolean getIsGatewayHub() {
    return this._isGatewayHub;
  }

  /**
   * Sets whether this member is a cache server
   * @param isGatewayHub Boolean defining whether this member is a hub for
   * WAN gateways
   */
  protected void setIsGatewayHub(boolean isGatewayHub) {
    this._isGatewayHub = isGatewayHub;
  }

  public boolean getIsLocator() {
    return this._isLocator;
  }

  protected void setIsLocator(boolean isLocator) {
    this._isLocator = isLocator;
  }

  public boolean getIsPrimaryGatewayHub() {
    return this._isPrimaryGatewayHub;
  }

  protected void setIsPrimaryGatewayHub(boolean isPrimaryGatewayHub) {
    this._isPrimaryGatewayHub = isPrimaryGatewayHub;
  }

  /**
   * For internal use only
   * @return status of the gateway hub
   */
  public Object/*GatewayHubStatus*/ getGatewayHubStatus() {
    return this._gatewayHubStatus;
  }

//  protected void setGatewayHubStatus(GatewayHubStatus gatewayHubStatus) {
//    this._gatewayHubStatus = gatewayHubStatus;
//  }

  public boolean getIsSecondaryGatewayHub() {
    return !this._isPrimaryGatewayHub;
  }

  public Set getConnectedPeers() {
    return this._connectedPeers;
  }

  protected void setConnectedPeers(Set connectedPeers) {
    this._connectedPeers = connectedPeers;
  }

  public Set getConnectedServers() {
    return this._connectedServers;
  }

  protected void setConnectedServers(Set connectedServers) {
    this._connectedServers = connectedServers;
  }

  protected void addConnectedServer(String connectedServer) {
    this._connectedServers.add(connectedServer);
  }

  public Set getUnconnectedServers() {
    return this._unconnectedServers;
  }

  protected void setUnconnectedServers(Set unconnectedServers) {
    this._unconnectedServers = unconnectedServers;
  }

  protected void addUnconnectedServer(String unconnectedServer) {
    this._unconnectedServers.add(unconnectedServer);
  }

  public Set getConnectedClients() {
    return this._connectedClients;
  }

  protected void addConnectedClient(String connectedClient) {
    this._connectedClients.add(connectedClient);
  }

  public Map getOutgoingGateways() {
	    return this._outgoingGateways;
  }

  public Map getConnectedIncomingGateways() {
    return this._connectedIncomingGateways;
  }

  protected void setConnectedIncomingGateways(Map connectedIncomingGateways) {
    this._connectedIncomingGateways = connectedIncomingGateways;
  }

  public Map getClientQueueSizes() {
    return this._clientQueueSizes;
  }

  protected void setClientQueueSizes(Map clientQueueSizes) {
    this._clientQueueSizes = clientQueueSizes;
  }

  public int getClientQueueSize(String clientMemberId) {
    Integer clientQueueSize = (Integer) getClientQueueSizes().get(clientMemberId);
    return clientQueueSize == null ? 0 : clientQueueSize.intValue();
  }

  protected void putClientQueueSize(String clientMemberId, int size) {
    getClientQueueSizes().put(clientMemberId, Integer.valueOf(size));
  }

  public Map getClientHealthStats() {
    return this._clientHealthStats;
  }
  
  protected void setClientHealthStats(Map stats) {
    this._clientHealthStats = stats;
  }
  
  /**
   * For internal use only
   * @param clientID client for health
   * @return the client's health
   */
  public Object/*ClientHealthStats*/ getClientHealthStats(String clientID) {
    return this._clientHealthStats.get(clientID);
  }
  
  protected void setClientHealthStats(String clientID, ClientHealthStats stats) {
    this._clientHealthStats.put(clientID, stats);
  }
  
  protected void putClientHostName(String clientId, String hostName) {
    this._clientHostNames.put(clientId, hostName);
  }
  
  public String getClientHostName(String clientId) {
    return (String)this._clientHostNames.get(clientId);
  }
  
  public Map getRegionStatuses() {
    return this._regionStatuses;
  }

  /**
   * For internal use only
   * @param fullRegionPath region path
   * @return status for the region
   */
  public Object/*RegionStatus*/ getRegionStatus(String fullRegionPath) {
    return getRegionStatuses().get(fullRegionPath);
  }

  protected void putRegionStatus(String fullRegionPath, RegionStatus status) {
    getRegionStatuses().put(fullRegionPath, status);
  }

  public Serializable getMemberId() {
    return this._memberId;
  }

  protected void setMemberId(Serializable memberId) {
    this._memberId = memberId;
  }

  public String getMemberName() {
    return this._memberName;
  }

  protected void setMemberName(String memberName) {
    this._memberName = memberName;
  }

  public int getMcastPort() {
    return this._mcastPort;
  }

  protected void setMcastPort(int mcastPort) {
    this._mcastPort = mcastPort;
  }

  public InetAddress getMcastAddress() {
    return this._mcastAddress;
  }

  protected void setMcastAddress(InetAddress mcastAddress) {
    this._mcastAddress = mcastAddress;
  }

  public InetAddress getHostAddress() {
    return this._hostAddress;
  }

  protected void setHostAddress(InetAddress hostAddress) {
    this._hostAddress = hostAddress;
  }

  public String getBindAddress() {
    return this._bindAddress;
  }

  protected void setBindAddress(String bindAddress) {
    this._bindAddress = bindAddress;
  }

  public String getLocators() {
    return this._locators;
  }

  protected void setLocators(String locators) {
    this._locators = locators;
  }

  public long getMaximumHeapSize() {
    return this._maximumHeapSize;
  }

  protected void setMaximumHeapSize(long size) {
    this._maximumHeapSize = size;
  }

  public long getFreeHeapSize() {
    return this._freeHeapSize;
  }

  protected void setFreeHeapSize(long size) {
    this._freeHeapSize = size;
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
    StringBuffer buffer = new StringBuffer();
    buffer
      .append("GemFireMemberStatus[")
      .append("isConnected=")
      .append(this._isConnected)
      .append("; memberName=")
      .append(this._memberName)
      .append("; memberId=")
      .append(this._memberId)
      .append("; hostAddress=")
      .append(this._hostAddress)
      .append("; mcastPort=")
      .append(this._mcastPort)
      .append("; mcastAddress=")
      .append(this._mcastAddress)
      .append("; bindAddress=")
      .append(this._bindAddress)
      .append("; serverPort=")
      .append(this._serverPort)
      .append("; locators=")
      .append(this._locators)
      .append("; isClient=")
      .append(this._isClient)
      .append("; isServer=")
      .append(this._isServer)
      .append("; isGatewayHub=")
      .append(this._isGatewayHub)
      .append("; isLocator=")
      .append(this._isLocator)
      .append("; isPrimaryGatewayHub=")
      .append(this._isPrimaryGatewayHub)
      .append("; gatewayHubStatus=")
      .append(this._gatewayHubStatus)
      .append("; connectedPeers=")
      .append(this._connectedPeers)
      .append("; connectedServers=")
      .append(this._connectedServers)
      .append("; unconnectedServers=")
      .append(this._unconnectedServers)
      .append("; connectedClients=")
      .append(this._connectedClients)
      .append("; clientHostNames=")
      .append(this._clientHostNames)
      .append("; clientQueueSizes=")
      .append(this._clientQueueSizes)
      .append("; clientHealthStats=")
      .append(this._clientHealthStats)
      .append("; gatewayQueueSizes=")
      .append(this._gatewayQueueSizes)
      .append("; regionStatuses=")
      .append(this._regionStatuses)
      .append("; maximumHeapSize=")
      .append(this._maximumHeapSize)
      .append("; freeHeapSize=")
      .append(this._freeHeapSize)
      .append("; upTime=")
      .append(this.upTime)
      .append("]");
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
    this._connectedClients = new HashSet();
    this._clientQueueSizes = new HashMap();
    this._clientHealthStats = new HashMap();
    this._clientHostNames = new HashMap();

    // Variables for gateway hubs
    this._outgoingGateways = new HashMap();
    //this._connectedOutgoingGateways = new HashSet();
    //this._unconnectedOutgoingGateways = new HashSet();
    this._connectedIncomingGateways = new HashMap();
    this._gatewayQueueSizes = new HashMap();

    // Variables for clients
    this._connectedServers = new HashSet();
    this._unconnectedServers = new HashSet();

    // Variables for all
    this._connectedPeers = new HashSet();
    this._regionStatuses = new HashMap();
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
      Map allConnectedClients = InternalClientMembership.getStatusForAllClientsIgnoreSubscriptionStatus();
      Iterator allConnectedClientsIterator = allConnectedClients.values().iterator();
      while (allConnectedClientsIterator.hasNext()) {
        CacheClientStatus ccs = (CacheClientStatus) allConnectedClientsIterator.next();
        addConnectedClient(ccs.getMemberId());
        // host address is available directly by id, hence CacheClientStatus need not be populated
        putClientHostName(ccs.getMemberId(), ccs.getHostAddress());  
      }

      // Get client queue sizes
      Map clientQueueSize = getClientIDMap(InternalClientMembership.getClientQueueSizes());
      setClientQueueSizes(clientQueueSize);
      
      // Set server acceptor port (set it based on the first CacheServer)
      CacheServer server = (CacheServer) servers.toArray()[0];
      setServerPort(server.getPort());
      
      // Get Client Health Stats
//      Assert.assertTrue(cache != null); (cannot be null)
      Region clientHealthMonitoringRegion = ClientHealthMonitoringRegion.getInstance(
          (GemFireCacheImpl)cache);
      if(clientHealthMonitoringRegion != null) {
        String [] clients = (String[])clientHealthMonitoringRegion.keySet().toArray(new String[0]);
        for (int i = 0; i < clients.length; i++) {
          String clientId = clients[i];
          ClientHealthStats stats = (ClientHealthStats)clientHealthMonitoringRegion.get(clientId);
          setClientHealthStats(clientId, stats);
        } 
      }
    }
  }
  
	/**
	 * returning  Map of client queue size against client Id
	 *  
	 * param clientMap is a  Map of client queue size against ClientProxyMembershipID
	 */
	private Map getClientIDMap(Map ClientProxyMembershipIDMap) {
	   Map clientIdMap = new HashMap();
	   Set entrySet = ClientProxyMembershipIDMap.entrySet();
	   Iterator entries = entrySet.iterator();
	   while (entries.hasNext()) {
             Map.Entry entry = (Map.Entry)entries.next();
             ClientProxyMembershipID key = (ClientProxyMembershipID)entry.getKey();
             Integer size = (Integer)entry.getValue();
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
        Iterator connected = connectedServers.entrySet().iterator();
        while (connected.hasNext()) {
          Map.Entry entry = (Map.Entry) connected.next();
          String server = (String) entry.getKey();
//          Integer connections = (Integer) entry.getValue();
//          if (connections.intValue()==0) {
//            addUnconnectedServer(server);
//          } else {
            addConnectedServer(server);
//          }
          //System.out.println(connections.size() + " logical connnections to server " + server);
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
        ? InetAddress.getByName(bindAddress)
        : SocketCreator.getLocalHost());
    } catch (IOException e) {/*ignore - leave null host address*/}
  }

  protected void initializePeers(DistributedSystem distributedSystem) {
    InternalDistributedSystem ids = (InternalDistributedSystem) distributedSystem;
    DM dm = ids.getDistributionManager();
    Set connections = dm.getOtherNormalDistributionManagerIds();
    Set connectionsIDs = new HashSet(connections.size());
    for (Iterator iter=connections.iterator(); iter.hasNext() ; ) {
      InternalDistributedMember idm = (InternalDistributedMember)iter.next();
      connectionsIDs.add(idm.getId());
    }
    setConnectedPeers(connectionsIDs);
  }

  protected void initializeMemory() {
    //InternalDistributedSystem system = (InternalDistributedSystem) region.getCache().getDistributedSystem();
    //GemFireStatSampler sampler = system.getStatSampler();
    //VMStatsContract statsContract = sampler.getVMStats();

    Runtime rt = Runtime.getRuntime();
    setMaximumHeapSize(rt.maxMemory());
    setFreeHeapSize(rt.freeMemory());
  }

  protected void initializeRegionSizes() {
    Iterator rootRegions = cache.rootRegions().iterator();

    while (rootRegions.hasNext()) {
      LocalRegion rootRegion = (LocalRegion) rootRegions.next();
      if (!(rootRegion instanceof HARegion)) {
        RegionStatus rootRegionStatus = rootRegion instanceof PartitionedRegion
          ? new PartitionedRegionStatus((PartitionedRegion) rootRegion)
          : new RegionStatus(rootRegion);
        putRegionStatus(rootRegion.getFullPath(), rootRegionStatus);
        Iterator subRegions = rootRegion.subregions(true).iterator();
        while (subRegions.hasNext()) {
          LocalRegion subRegion = (LocalRegion) subRegions.next();
          RegionStatus subRegionStatus = subRegion instanceof PartitionedRegion
            ? new PartitionedRegionStatus((PartitionedRegion) subRegion)
            : new RegionStatus(subRegion);
          putRegionStatus(subRegion.getFullPath(), subRegionStatus);
        }
      }
    }
  }
}

