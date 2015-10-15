/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  ========================================================================
 */
package com.gemstone.gemfire.management.internal.beans;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.CqClosedException;
import com.gemstone.gemfire.cache.query.CqException;
import com.gemstone.gemfire.cache.query.CqQuery;
import com.gemstone.gemfire.cache.query.CqState;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.RegionNotFoundException;
import com.gemstone.gemfire.cache.query.internal.CqStateImpl;
import com.gemstone.gemfire.cache.query.internal.cq.CqService;
import com.gemstone.gemfire.cache.query.internal.cq.InternalCqQuery;
import com.gemstone.gemfire.cache.query.internal.cq.ServerCQ;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache.server.ServerLoad;
import com.gemstone.gemfire.cache.server.ServerLoadProbe;
import com.gemstone.gemfire.cache.server.internal.ServerMetricsImpl;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.admin.ClientHealthMonitoringRegion;
import com.gemstone.gemfire.internal.admin.remote.ClientHealthStats;
import com.gemstone.gemfire.internal.cache.CacheServerImpl;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.ha.HARegionQueue;
import com.gemstone.gemfire.internal.cache.tier.InternalClientMembership;
import com.gemstone.gemfire.internal.cache.tier.sockets.AcceptorImpl;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientNotifier;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientProxy;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.process.PidUnavailableException;
import com.gemstone.gemfire.internal.process.ProcessUtils;
import com.gemstone.gemfire.management.ClientHealthStatus;
import com.gemstone.gemfire.management.ClientQueueDetail;
import com.gemstone.gemfire.management.ServerLoadData;
import com.gemstone.gemfire.management.internal.ManagementConstants;
import com.gemstone.gemfire.management.internal.beans.stats.StatType;
import com.gemstone.gemfire.management.internal.beans.stats.StatsAverageLatency;
import com.gemstone.gemfire.management.internal.beans.stats.StatsKey;
import com.gemstone.gemfire.management.internal.beans.stats.StatsRate;
import com.gemstone.gemfire.management.internal.cli.CliUtil;
import com.gemstone.gemfire.management.membership.ClientMembershipListener;

/**
 * Represents the GemFire CacheServer . Provides data and notifications about
 * server, subscriptions,durable queues and indices
 * 
 * @author rishim
 * 
 */
public class CacheServerBridge extends ServerBridge{  

  private static final Logger logger = LogService.getLogger();
  
  private CacheServer cacheServer;

  private GemFireCacheImpl cache;

  private QueryService qs;
   
  private StatsRate clientNotificationRate;
  
  private StatsAverageLatency clientNotificatioAvgLatency;

  protected StatsRate queryRequestRate;
  
  private MemberMBeanBridge memberMBeanBridge;

  private ClientMembershipListener membershipListener;
  
  public static ThreadLocal<Version> clientVersion = new ThreadLocal<Version>();

  protected static int identifyPid() {
    try {
      return ProcessUtils.identifyPid();
    }
    catch (PidUnavailableException e) {
      if (logger.isDebugEnabled()) {
        logger.debug(e.getMessage(), e);
      }
      return 0;
    }
  }

  public CacheServerBridge(CacheServer cacheServer, GemFireCacheImpl cache) {
    super(cacheServer);
    this.cacheServer = cacheServer;
    this.cache = cache;
    this.qs = cache.getQueryService();
    
    initializeCacheServerStats();
  }

  // Dummy constructor for testing purpose only TODO why is this public then?
  public CacheServerBridge() {
    super();
    initializeCacheServerStats();
  }

  public void setMemberMBeanBridge(MemberMBeanBridge memberMBeanBridge) {
    this.memberMBeanBridge = memberMBeanBridge;
  }

  public void stopMonitor(){
    super.stopMonitor();
    monitor.stopListener();    
  }

  private void initializeCacheServerStats() {

    clientNotificationRate = new StatsRate(StatsKey.NUM_CLIENT_NOTIFICATION_REQUEST, StatType.INT_TYPE, monitor);

    clientNotificatioAvgLatency = new StatsAverageLatency(StatsKey.NUM_CLIENT_NOTIFICATION_REQUEST, StatType.INT_TYPE,
        StatsKey.CLIENT_NOTIFICATION_PROCESS_TIME, monitor);

    queryRequestRate = new StatsRate(StatsKey.QUERY_REQUESTS, StatType.INT_TYPE, monitor);
  }
  
  /**
   * Returns the configured buffer size of the socket connection for this server
   **/
  public int getSocketBufferSize() {
    return cacheServer.getSocketBufferSize();
  }
  
  public boolean getTcpNoDelay() {
    return cacheServer.getTcpNoDelay();
  }

  /** port of the server **/
  public int getPort() {
    return cacheServer.getPort();
  }

  public int getCapacity() {
    if (cacheServer.getClientSubscriptionConfig() != null) {
      return cacheServer.getClientSubscriptionConfig().getCapacity();
    }
    return 0;
  }

  /** disk store name for overflow **/
  public String getDiskStoreName() {
    if (cacheServer.getClientSubscriptionConfig() != null) {
      return cacheServer.getClientSubscriptionConfig().getDiskStoreName();
    }

    return null;
  }

  /** Returns the maximum allowed client connections **/
  public int getMaxConnections() {
    return cacheServer.getMaxConnections();
  }

  /**
   * Get the frequency in milliseconds to poll the load probe on this cache
   * server.
   **/
  public long getLoadPollInterval() {
    return cacheServer.getLoadPollInterval();
  }

  /** Get the load probe for this cache server **/
  public ServerLoadData fetchLoadProbe() {
    ServerLoadProbe probe = cacheServer.getLoadProbe();
    ServerLoad load = probe.getLoad(new ServerMetricsImpl(cacheServer.getMaxConnections()));
    ServerLoadData data = new ServerLoadData(load.getConnectionLoad(), load.getSubscriptionConnectionLoad(), load.getLoadPerConnection(), load
        .getLoadPerSubscriptionConnection());
    return data;
  }

  /**
   * Returns the maxium number of threads allowed in this server to service
   * client requests.
   **/
  public int getMaxThreads() {
    return cacheServer.getMaxThreads();
  }

  /** Sets maximum number of messages that can be enqueued in a client-queue. **/
  public int getMaximumMessageCount() {
    return cacheServer.getMaximumMessageCount();
  }

  /** Returns the maximum amount of time between client pings **/
  public int getMaximumTimeBetweenPings() {
    return cacheServer.getMaximumTimeBetweenPings();
  }

  /**
   * Returns the time (in seconds ) after which a message in the client queue
   * will expire.
   **/
  public int getMessageTimeToLive() {
    return cacheServer.getMessageTimeToLive();
  }

  /** is the server running **/
  public boolean isRunning() {
    return cacheServer.isRunning();
  }

  /**
   * Returns the eviction policy that is executed when capacity of the client
   * queue is reached
   **/
  public String getEvictionPolicy() {
    if (cacheServer.getClientSubscriptionConfig() != null) {
      return cacheServer.getClientSubscriptionConfig().getEvictionPolicy();
    }

    return null;
  }

  /**
   * The hostname or IP address to pass to the client as the loca- tion where
   * the server is listening. When the server connects to the locator it tells
   * the locator the host and port where it is listening for client connections.
   * If the host the server uses by default is one that the client can’t
   * translate into an IP address, the client will have no route to the server’s
   * host and won’t be able to find the server. For this situation, you must
   * supply the server’s alternate hostname for the locator to pass to the cli-
   * ent. If null, the server’s bind-address (page 177) setting is used.
   * Default: null.
   **/
  public String getHostnameForClients() {
    return cacheServer.getHostnameForClients();
  }

  /**
   * The hostname or IP address that the server is to listen on for client
   * connections. If null, the server listens on the machine’s default address.
   * Default: null.
   */
  public String getBindAddress() {
    return cacheServer.getBindAddress();
  }


  public String[] getContinuousQueryList() {
    CqService cqService = cache.getCqService();
    if (cqService != null) {
      Collection<? extends InternalCqQuery> allCqs = cqService.getAllCqs();
      if (allCqs != null && allCqs.size() > 0) {
        String[] allCqStr = new String[allCqs.size()];
        int i = 0;
        for (InternalCqQuery cq : allCqs) {
          allCqStr[i] = cq.getName();
          i++;
        }
        return allCqStr;
      }
    }

    return ManagementConstants.NO_DATA_STRING;
  }

  /**
   * Gets currently executing query count
   */
  public long getRegisteredCQCount() {
    CqService cqService = cache.getCqService();
    if (cqService != null) {
      Collection<? extends InternalCqQuery> allCqs = cqService.getAllCqs();
      return allCqs != null && allCqs.size() > 0 ? allCqs.size() : 0;
    }
    return 0;
  }
  
  public String[] getIndexList() {
    Collection<Index> idxs = qs.getIndexes();
    if (!idxs.isEmpty()) {
      Iterator<Index> idx = idxs.iterator();
      String[] indexList = new String[idxs.size()];
      int i = 0;
      while (idx.hasNext()) {
        Index index = idx.next();
        indexList[i] = index.getName();
        i++;

      }
      return indexList;
    }

    return ManagementConstants.NO_DATA_STRING;
  }

  /**
   * 
   * @return a list of client Ids connected to this particular server instance
   */
  public String[] listClientIds() throws Exception {
    String[] allConnectedClientStr = null;
    Map<String, ClientConnInfo> uniqueIds = getUniqueClientIds();
    if (uniqueIds.size() > 0) {
      allConnectedClientStr = new String[uniqueIds.size()];
      int j = 0;
      for (String clientId : uniqueIds.keySet()) {
        allConnectedClientStr[j] = clientId;
        j++;
      }
      return allConnectedClientStr;
    } else {
      return new String[0];
    }

  }
  

  private Map<String, ClientConnInfo> getUniqueClientIds() {
    Map<String, ClientConnInfo> uniqueIds = null;

    ServerConnection[] serverConnections = acceptor.getAllServerConnectionList();
    Collection<CacheClientProxy> clientProxies = acceptor.getCacheClientNotifier().getClientProxies();

    if (clientProxies.size() > 0) {
      uniqueIds = new HashMap<String, ClientConnInfo>();

      for (CacheClientProxy p : clientProxies) {
        ClientConnInfo clientConInfo = new ClientConnInfo(p.getProxyID(), p.getSocketHost(), p.getRemotePort(),
            p.isPrimary());
        uniqueIds.put(p.getProxyID().getDSMembership(), clientConInfo);
      }
    }

    if (serverConnections != null && serverConnections.length > 0) {
      if (uniqueIds == null) {
        uniqueIds = new HashMap<String, ClientConnInfo>();
      }
      for (ServerConnection conn : serverConnections) {
        ClientProxyMembershipID clientId = conn.getProxyID();
        if (clientId != null) { // Check added to fix bug 51987
        if (uniqueIds.get(clientId.getDSMembership()) == null) {
          ClientConnInfo clientConInfo = new ClientConnInfo(conn.getProxyID(), conn.getSocketHost(),
              conn.getSocketPort(), false);
          uniqueIds.put(clientId.getDSMembership(), clientConInfo);
        }
        }
      }
    }

    if (uniqueIds == null) {
      return Collections.emptyMap();
    }
    return uniqueIds;
  }

  private static class ClientConnInfo {

    private ClientProxyMembershipID clientId;

    private String hostName;
    
    private int port;
    
    boolean isPrimary;

    public ClientConnInfo(ClientProxyMembershipID clientId, String hostName, int port, boolean isPrimary) {
      this.clientId = clientId;
      this.hostName = hostName;
      this.port = port;
      this.isPrimary = isPrimary;
    }

    public String getHostName() {
      return hostName;
    }

    public ClientProxyMembershipID getClientId() {
      return clientId;
    }
    
    public String toString(){
      StringBuffer buffer = new StringBuffer();
      buffer.append("[").append(clientId).append("; port=").append(port).append("; primary=")
          .append(isPrimary).append("]");
      return buffer.toString();
    }
  }
  
  public Version getClientVersion(ClientConnInfo connInfo){
    GemFireCacheImpl cache = (GemFireCacheImpl)CacheFactory.getAnyInstance();    
    
    if (cache.getCacheServers().size() == 0) {
      return null;      
    }
       
    CacheServerImpl server = (CacheServerImpl)cache.getCacheServers().iterator().next();
    
    if(server == null){
      return null;
    }
    
    AcceptorImpl  acceptorImpl  = server.getAcceptor(); 
    if(acceptorImpl == null){
      return null;
    }         
       
    ServerConnection[] serverConnections  = acceptorImpl.getAllServerConnectionList();  
   
    boolean flag = false;
    if(connInfo.toString().contains("primary=true")){
      flag = true;
    }
        
    for (ServerConnection conn : serverConnections){       
      ClientProxyMembershipID cliIdFrmProxy = conn.getProxyID();
      ClientConnInfo cci = new ClientConnInfo(conn.getProxyID(), conn.getSocketHost(), conn.getSocketPort(), flag);   
      if (connInfo.toString().equals(cci.toString() )) {
        return cliIdFrmProxy.getClientVersion();        
      }
    }  
    
    //check form ccp
    ClientProxyMembershipID proxyId = connInfo.getClientId();
    CacheClientProxy proxy = CacheClientNotifier.getInstance().getClientProxy(proxyId);
    if(proxy != null){
      return proxy.getVersion();
    }else{
      return null;
    }
  } 

  public ClientHealthStatus showClientStats(String clientId) throws Exception {
    try {
      Map<String, ClientConnInfo> uniqueClientIds = getUniqueClientIds();
      ClientConnInfo clientConnInfo = uniqueClientIds.get(clientId);
      ClientHealthStatus status = getClientHealthStatus(clientConnInfo);
      return status;
    } finally {
      CacheServerBridge.clientVersion.set(null);
    }
  }

  public ClientHealthStatus[] showAllClientStats() throws Exception {
    try {
      List<ClientHealthStatus> clientHealthStatusList = null;
      Map<String, ClientConnInfo> uniqueClientIds = getUniqueClientIds();
      if (!uniqueClientIds.isEmpty()) {
        clientHealthStatusList = new ArrayList<ClientHealthStatus>();

        for (Map.Entry<String, ClientConnInfo> p : uniqueClientIds.entrySet()) {
          ClientHealthStatus status = getClientHealthStatus(p.getValue());
          if (status != null) {
            clientHealthStatusList.add(status);
          }
        }
        ClientHealthStatus[] statusArr = new ClientHealthStatus[clientHealthStatusList.size()];
        return clientHealthStatusList.toArray(statusArr);
      }
      return new ClientHealthStatus[0];
    } finally {
      CacheServerBridge.clientVersion.set(null);
    }
  }

  private ClientHealthStatus getClientHealthStatus(ClientConnInfo connInfo) {
    ClientProxyMembershipID proxyId = connInfo.getClientId();
    CacheClientProxy proxy = CacheClientNotifier.getInstance().getClientProxy(proxyId);
  
    if(proxy != null && !proxy.isConnected() && !proxyId.isDurable()){
      return null;
    }
    
    CacheServerBridge.clientVersion.set(getClientVersion(connInfo));

    int clientCQCount = 0;
    CqService cqService = cache.getCqService();
    if (cqService != null) {
      List<ServerCQ> cqs = cqService.getAllClientCqs(proxyId);
      clientCQCount = cqs.size();
    }
    
    ClientHealthStatus status = new ClientHealthStatus();    

    Region clientHealthMonitoringRegion = ClientHealthMonitoringRegion.getInstance((GemFireCacheImpl) cache);
    String clientName = proxyId.getDSMembership();
    status.setClientId(connInfo.toString());
    status.setName(clientName);
    status.setHostName(connInfo.getHostName());
    status.setClientCQCount(clientCQCount); 
    
    //Only available for clients having subscription enabled true
    if(proxy != null){
      status.setUpTime(proxy.getUpTime());
      status.setQueueSize(proxy.getQueueSizeStat());
      status.setConnected(proxy.isConnected());
      status.setSubscriptionEnabled(true); 
    }else{
      status.setConnected(true);
      status.setSubscriptionEnabled(false); 
    }

    ClientHealthStats stats = (ClientHealthStats) clientHealthMonitoringRegion.get(clientName);    

    if (stats != null) {
      status.setCpus(stats.getCpus());
      status.setNumOfCacheListenerCalls(stats.getNumOfCacheListenerCalls());
      status.setNumOfGets(stats.getNumOfGets());
      status.setNumOfMisses(stats.getNumOfMisses());
      status.setNumOfPuts(stats.getNumOfPuts());
      status.setNumOfThreads(stats.getNumOfThreads());
      status.setProcessCpuTime(stats.getProcessCpuTime());
      status.setPoolStats(stats.getPoolStats());
    }
    return status;
  }


  /**
   * closes a continuous query and releases all the resources associated with
   * it.
   * 
   * @param queryName
   */
  public void closeContinuousQuery(String queryName) throws Exception{
    CqService cqService = cache.getCqService();
    if (cqService != null) {
      Collection<? extends InternalCqQuery> allCqs = cqService.getAllCqs();
      for (InternalCqQuery query : allCqs) {
        if (query.getName().equals(queryName)) {
          try {
            query.close();
            return;
          } catch (CqClosedException e) {
            throw new Exception(e.getMessage());
          } catch (CqException e) {
            throw new Exception(e.getMessage());
          }

        }
      }
    }
  }

  /**
   * Execute a continuous query
   * 
   * @param queryName
   */
  public void executeContinuousQuery(String queryName) throws Exception{
    CqService cqService = cache.getCqService();
    if (cqService != null) {
      Collection<? extends InternalCqQuery> allCqs = cqService.getAllCqs();
      for (InternalCqQuery query : allCqs) {
        if (query.getName().equals(queryName)) {
          try {
            cqService.resumeCQ(CqStateImpl.RUNNING, (ServerCQ) query);
            return;
          } catch (CqClosedException e) {
            throw new Exception(e.getMessage());
          }
        }
      }
    }
    
  }

  /**
   * Stops a given query witout releasing any of the resources associated with
   * it.
   * 
   * @param queryName
   */
  public void stopContinuousQuery(String queryName) throws Exception{
    CqService cqService = cache.getCqService();
    if (cqService != null) {
      Collection<? extends InternalCqQuery> allCqs = cqService.getAllCqs();
      for (InternalCqQuery query : allCqs) {
        if (query.getName().equals(queryName)) {
          try {
            query.stop();
            return ;
          } catch (CqClosedException e) {
            throw new Exception(e.getMessage());
          } catch (CqException e) {
            throw new Exception(e.getMessage());
          }

        }
      }
    }
  }

  /**
   * remove a given index
   * 
   * @param indexName
   */
  public void removeIndex(String indexName) throws Exception{
    try{
      Collection<Index> idxs = qs.getIndexes();
      if (!idxs.isEmpty()) {
        Iterator<Index> idx = idxs.iterator();
        while (idx.hasNext()) {
          Index index = idx.next();
          if (index.getName().equals(indexName)) {
            qs.removeIndex(index);
          }
          return ;
        }
      }
    }catch(Exception e){
      throw new Exception(e.getMessage());
    }
  }


  public int getIndexCount() {
    return qs.getIndexes().size();
  }

  
  public int getNumClientNotificationRequests() {
    return getStatistic(StatsKey.NUM_CLIENT_NOTIFICATION_REQUEST).intValue();
  }

  public long getClientNotificationAvgLatency() {
    return clientNotificatioAvgLatency.getAverageLatency();
  }

 
  public float getClientNotificationRate() {
    return clientNotificationRate.getRate();
  }
  
  public float getQueryRequestRate() {
    return queryRequestRate.getRate();
  }

  public long getTotalIndexMaintenanceTime() {    
    return memberMBeanBridge.getTotalIndexMaintenanceTime();
  }
  

  public long getActiveCQCount() {
    CqService cqService = cache.getCqService();
    if(cqService != null && cqService.isRunning()){
      return cqService.getCqStatistics().numCqsActive();
    }
    return 0;
  }

  public int getNumSubscriptions() {
    Map clientProxyMembershipIDMap = InternalClientMembership.getClientQueueSizes();
    return clientProxyMembershipIDMap.keySet().size();
  }

  public void setClientMembershipListener(
      ClientMembershipListener membershipListener) {
    this.membershipListener = membershipListener;
  }
  
  public ClientMembershipListener getClientMembershipListener() {
    return this.membershipListener;
  }
  
  /**
   * @return Client Queue Details for all clients
   */
  public ClientQueueDetail[] getClientQueueDetails() throws Exception {
    List<ClientQueueDetail> clientQueueDetailList = null;
    try {
      if (acceptor != null && acceptor.getCacheClientNotifier() != null) {
        Collection<CacheClientProxy> clientProxies = acceptor.getCacheClientNotifier().getClientProxies();

        if (clientProxies.size() > 0) {
          clientQueueDetailList = new ArrayList<ClientQueueDetail>();
        } else {
          return new ClientQueueDetail[0];
        }

        for (CacheClientProxy p : clientProxies) {
          ClientQueueDetail status = getClientQueueDetail(p);
          if (status != null) {
            clientQueueDetailList.add(status);
          }

        }
        ClientQueueDetail[] queueDetailArr = new ClientQueueDetail[clientQueueDetailList.size()];
        return clientQueueDetailList.toArray(queueDetailArr);

      }
      return new ClientQueueDetail[0];

    } catch (Exception e) {
      throw new Exception(e.getMessage());
    }
  }
  

  private ClientQueueDetail getClientQueueDetail(CacheClientProxy p) {

    ClientQueueDetail queueDetail = new ClientQueueDetail();
    ClientProxyMembershipID proxyID = p.getProxyID();

    if (!p.isConnected() && !proxyID.isDurable()) {
      return null;
    }
    queueDetail.setClientId(CliUtil.getClientIdFromCacheClientProxy(p));
    
    HARegionQueue queue = p.getHARegionQueue();
    if (queue == null) {
      return queueDetail;
    }
    queueDetail.setQueueSize(p.getQueueSizeStat());
    queueDetail.setEventsConflated(queue.getStatistics().getEventsConflated());
    queueDetail.setEventsEnqued(queue.getStatistics().getEventsEnqued());
    queueDetail.setEventsExpired(queue.getStatistics().getEventsExpired());
    queueDetail.setEventsRemoved(queue.getStatistics().getEventsRemoved());
    queueDetail.setEventsRemovedByQrm(queue.getStatistics().getEventsRemovedByQrm());
    queueDetail.setEventsTaken(queue.getStatistics().getEventsTaken());
    queueDetail.setMarkerEventsConflated(queue.getStatistics().getMarkerEventsConflated());
    queueDetail.setNumVoidRemovals(queue.getStatistics().getNumVoidRemovals());
    return queueDetail;
  }
  
  /**
   * 
   * @param clientId
   * @return stats for a given client ID
   */
  public ClientQueueDetail getClientQueueDetail(String clientId) throws Exception {

    try {
      if (acceptor != null && acceptor.getCacheClientNotifier() != null) {
        Collection<CacheClientProxy> clientProxies = acceptor.getCacheClientNotifier().getClientProxies();
        for (CacheClientProxy p : clientProxies) {
          String buffer = CliUtil.getClientIdFromCacheClientProxy(p);
          if (buffer.equals(clientId)) {
            ClientQueueDetail queueDetail = getClientQueueDetail(p);
            return queueDetail;
          }
        }

      }
    } catch (Exception e) {
      throw new Exception(e.getMessage());
    }
    return null;
  }

}
