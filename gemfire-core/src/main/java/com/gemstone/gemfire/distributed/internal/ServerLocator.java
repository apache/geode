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
package com.gemstone.gemfire.distributed.internal;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.InternalGemFireException;
import com.gemstone.gemfire.cache.GemFireCache;
import com.gemstone.gemfire.cache.client.internal.locator.ClientConnectionRequest;
import com.gemstone.gemfire.cache.client.internal.locator.ClientConnectionResponse;
import com.gemstone.gemfire.cache.client.internal.locator.ClientReplacementRequest;
import com.gemstone.gemfire.cache.client.internal.locator.GetAllServersRequest;
import com.gemstone.gemfire.cache.client.internal.locator.GetAllServersResponse;
import com.gemstone.gemfire.cache.client.internal.locator.LocatorListRequest;
import com.gemstone.gemfire.cache.client.internal.locator.LocatorListResponse;
import com.gemstone.gemfire.cache.client.internal.locator.LocatorStatusRequest;
import com.gemstone.gemfire.cache.client.internal.locator.LocatorStatusResponse;
import com.gemstone.gemfire.cache.client.internal.locator.QueueConnectionRequest;
import com.gemstone.gemfire.cache.client.internal.locator.QueueConnectionResponse;
import com.gemstone.gemfire.cache.client.internal.locator.ServerLocationRequest;
import com.gemstone.gemfire.cache.server.ServerLoad;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionAdvisor.Profile;
import com.gemstone.gemfire.distributed.internal.tcpserver.TcpHandler;
import com.gemstone.gemfire.distributed.internal.tcpserver.TcpServer;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.SocketCreator;
import com.gemstone.gemfire.internal.cache.CacheServerAdvisor.CacheServerProfile;
import com.gemstone.gemfire.internal.cache.ControllerAdvisor;
import com.gemstone.gemfire.internal.cache.ControllerAdvisor.ControllerProfile;
import com.gemstone.gemfire.internal.cache.FindDurableQueueProcessor;
import com.gemstone.gemfire.internal.cache.GridAdvisor.GridProfile;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * 
 * @author dsmith
 * @since 5.7
 */
public class ServerLocator implements TcpHandler, DistributionAdvisee {
  private static final Logger logger = LogService.getLogger();
  
  private final int port;
  private final String hostNameForClients;
  private InternalDistributedSystem ds;
  private ControllerAdvisor advisor;
  private final int serialNumber = createSerialNumber();
  private final LocatorStats stats;
  private LocatorLoadSnapshot loadSnapshot = new LocatorLoadSnapshot();
  private Map<ServerLocation, DistributedMember> ownerMap = new HashMap<ServerLocation, DistributedMember>();
  private volatile ArrayList cachedLocators;
  private final Object cachedLocatorsLock = new Object();
  
  private final static AtomicInteger profileSN = new AtomicInteger();
  
  private final String logFile;
  private final String hostName;
  private final String memberName;
  
  private ProductUseLog productUseLog;

  ServerLocator() throws IOException {
    this.port = 10334;
    this.hostName = SocketCreator.getLocalHost().getCanonicalHostName();
    this.hostNameForClients= this.hostName;
    this.logFile = null;
    this.memberName = null;
    this.ds = null;
    this.advisor = null;
    this.stats = null;
  }
  
  public ServerLocator(int port,
                       InetAddress bindAddress,
                       String hostNameForClients,
                       File logFile,
                       ProductUseLog productUseLogWriter,
                       String memberName,
                       InternalDistributedSystem ds,
                       LocatorStats stats)
    throws IOException
  {
    this.port = port;

    if (bindAddress == null) {
      this.hostName = SocketCreator.getLocalHost().getCanonicalHostName();
    } else {
      this.hostName= bindAddress.getHostAddress();
    }

    if (hostNameForClients != null && !hostNameForClients.equals("")) {
      this.hostNameForClients = hostNameForClients;
    } else {
      this.hostNameForClients= this.hostName;
    }
    
    this.logFile = logFile != null ? logFile.getCanonicalPath() : null;
    this.memberName = memberName;
    this.productUseLog = productUseLogWriter;

    this.ds = ds;
    this.advisor = ControllerAdvisor.createControllerAdvisor(this); // escapes constructor but allows field to be final
    this.stats = stats;
  }

  public String getHostName() {
    return this.hostNameForClients;
  }

  public int getPort() {
    return this.port;
  }

  public CancelCriterion getCancelCriterion() {
    return this.ds.getCancelCriterion();
  }
  
  public void init(TcpServer tcpServer) {
    // if the ds is reconnecting we don't want to start server
    // location services until the DS finishes connecting
    if (this.ds != null && !this.ds.isReconnecting()) {
      this.advisor.handshake();
    }
  }
  
  /**
   * A ServerLocator may be in place but non-functional during auto-reconnect
   * because peer location services have been initialized while the servers
   * reconnect but server location services aren't reconnected until a cache
   * is available
   */
  protected boolean readyToProcessRequests() {
    return this.ds != null;
  }

  public Object processRequest(Object request) {
    if (!readyToProcessRequests()) {
      return null;
    }
    
    if(logger.isDebugEnabled()) {
      logger.debug("ServerLocator: Received request {}", request);
    }

    Object response;

    if (request instanceof ServerLocationRequest) {
      if (request instanceof LocatorStatusRequest) {
        response = new LocatorStatusResponse()
          .initialize(this.port, this.hostName, this.logFile, this.memberName);
      }
      else if (request instanceof LocatorListRequest) {
        response = getLocatorListResponse((LocatorListRequest) request);
      }
      else if (request instanceof ClientReplacementRequest) {
        response = pickReplacementServer((ClientReplacementRequest) request);
      }
      else if (request instanceof GetAllServersRequest) {
        response = pickAllServers((GetAllServersRequest) request);
      }
      else if (request instanceof ClientConnectionRequest) {
        response = pickServer((ClientConnectionRequest) request);
      }
      else if (request instanceof QueueConnectionRequest) {
        response = pickQueueServers((QueueConnectionRequest) request);
      }
      else {
        throw new InternalGemFireException("Unknown ServerLocationRequest: " + request.getClass());
      }
    }
    else {
      throw new InternalGemFireException("Expected ServerLocationRequest, got " + request.getClass());
    }

    if(logger.isDebugEnabled()) {
      logger.debug("ServerLocator: Sending response {}", response);
    }

    return response;
  }

  private ClientConnectionResponse pickServer(ClientConnectionRequest clientRequest) {
    ServerLocation location = loadSnapshot.getServerForConnection(clientRequest
        .getServerGroup(), clientRequest.getExcludedServers());
    return new ClientConnectionResponse(location);
  }

  private ClientConnectionResponse pickReplacementServer(ClientReplacementRequest clientRequest) {
    ServerLocation location = loadSnapshot
      .getReplacementServerForConnection(clientRequest.getCurrentServer(),
                                         clientRequest.getServerGroup(),
                                         clientRequest.getExcludedServers());
    return new ClientConnectionResponse(location);
  }

  
  private GetAllServersResponse pickAllServers(
      GetAllServersRequest clientRequest) {
    ArrayList servers = loadSnapshot.getServers(clientRequest.getServerGroup());
    return new GetAllServersResponse(servers);
  }
  
  private Object getLocatorListResponse(LocatorListRequest request) {
    ArrayList controllers = getLocators();
    boolean balanced = loadSnapshot.hasBalancedConnections(request.getServerGroup());
    return new LocatorListResponse(controllers, balanced);
  }
  
  private Object pickQueueServers(QueueConnectionRequest clientRequest) {
    Set excludedServers = new HashSet(clientRequest.getExcludedServers());
    
    /* If this is a request to find durable queues, lets go find them */
    
    ArrayList servers = new ArrayList();
    boolean durableQueueFound = false;
    if(clientRequest.isFindDurable() && clientRequest.getProxyId().isDurable()) {
      servers = FindDurableQueueProcessor.sendAndFind(this, clientRequest.getProxyId(), getDistributionManager());
      /* add the found durables to exclude list so they aren't candidates for more queues */
      excludedServers.addAll(servers);
      durableQueueFound = servers.size()>0;
    }
    
    List candidates;
    if(clientRequest.getRedundantCopies() == -1) {
      /* We need all the servers we can get */
      candidates = loadSnapshot.getServersForQueue(clientRequest.getProxyId(),
                                                   clientRequest.getServerGroup(),
                                                   excludedServers,-1);
    } else if(clientRequest.getRedundantCopies() > servers.size()) {
      /* We need more servers. */
      int count = clientRequest.getRedundantCopies() - servers.size();
      candidates = loadSnapshot.getServersForQueue(clientRequest.getProxyId(),
                                                   clientRequest.getServerGroup(),
                                                   excludedServers,
                                                   count);
    } else {
      /* Otherwise, we don't need any more servers */
      candidates = Collections.EMPTY_LIST;
    }
   
    if(candidates.size()>1) {
       Collections.shuffle(candidates);
     }
    servers.addAll(candidates);
    
    return new QueueConnectionResponse(durableQueueFound, servers);
  }

  public void shutDown() {
    this.advisor.close();
    this.loadSnapshot.shutDown();
  }

  public void restarting(DistributedSystem ds, GemFireCache cache, SharedConfiguration sharedConfig) {
    if (ds != null) {
      this.loadSnapshot = new LocatorLoadSnapshot();
      this.ds = (InternalDistributedSystem)ds;
      this.advisor = ControllerAdvisor.createControllerAdvisor(this); // escapes constructor but allows field to be final
    }
  }
  
  // DistributionAdvisee methods
  public DM getDistributionManager() {
    return getSystem().getDistributionManager();
  }
  
  public DistributionAdvisor getDistributionAdvisor() {
    return this.advisor;
  }
  
  public Profile getProfile() {
    return getDistributionAdvisor().createProfile();
  }
  
  public DistributionAdvisee getParentAdvisee() {
    return null;
  }
  
  public InternalDistributedSystem getSystem() {
    return this.ds;
  }
  
  public String getName() {
    return "ServerLocator";
  }

  public int getSerialNumber() {
    return this.serialNumber;
  }
  
  public String getFullPath() {
    return getName();
  }
  
  public InternalDistributedSystem getDs() {
    return ds;
  }

  private static int createSerialNumber() {
    return profileSN.incrementAndGet();
  }
  
  public void fillInProfile(Profile profile) {
    assert profile instanceof ControllerProfile;
    ControllerProfile cp = (ControllerProfile)profile;
    cp.setHost(this.hostNameForClients);
    cp.setPort(this.port);
    cp.serialNumber = getSerialNumber();
    cp.finishInit();
  }
  

  public void setLocatorCount(int count) {
    this.stats.setLocatorCount(count);
  }
  public void setServerCount(int count) {
    this.stats.setServerCount(count);
  }
  
  public void endRequest(Object request,long startTime) {
    stats.endLocatorRequest(startTime);
  }
  
  public void endResponse(Object request,long startTime) {
    stats.endLocatorResponse(startTime);
  }
  

  
  private ArrayList getLocators() {
    if(cachedLocators != null) {
      return cachedLocators;
    }
    else {
      synchronized(cachedLocatorsLock) {
        List profiles = advisor.fetchControllers();
        ArrayList result = new ArrayList(profiles.size() + 1);
        for (Iterator itr = profiles.iterator(); itr.hasNext(); ) {
          result.add(buildServerLocation((ControllerProfile) itr.next()));
        }
        result.add(new ServerLocation(hostNameForClients,port));
        cachedLocators = result;
        return result;
      }
    }
  }
  
  protected static ServerLocation buildServerLocation(GridProfile p) {
    return new ServerLocation(p.getHost(), p.getPort());
  }
  
  /**
   * @param profile
   */
  public void profileCreated(Profile profile) {
    if(profile instanceof CacheServerProfile) {
      CacheServerProfile bp = (CacheServerProfile) profile;
      ServerLocation location = buildServerLocation(bp);
      String[] groups = bp.getGroups();
      loadSnapshot.addServer(location, groups,
                             bp.getInitialLoad(),
                             bp.getLoadPollInterval());
      if(logger.isDebugEnabled()) {
        logger.debug("ServerLocator: Received load from a new server {}, {}", location, bp.getInitialLoad());
      }
      synchronized(ownerMap) {
        ownerMap.put(location, profile.getDistributedMember());
      }
    } else {
      cachedLocators = null;
    }
    logServers();
  }

  /**
   * @param profile
   */
  public void profileRemoved(Profile profile) {
    if(profile instanceof CacheServerProfile) {
      CacheServerProfile bp = (CacheServerProfile) profile;
      //InternalDistributedMember id = bp.getDistributedMember();
      ServerLocation location = buildServerLocation(bp);
      loadSnapshot.removeServer(location);
      if(logger.isDebugEnabled()) {
        logger.debug("ServerLocator: server departed {}", location);
      }
      synchronized(ownerMap) {
        ownerMap.remove(location);
      }
    } else {
      cachedLocators = null;
    }
  }

  public void profileUpdated(Profile profile) {
    cachedLocators = null;
    getLogWriterI18n().warning(LocalizedStrings.ServerLocator_SERVERLOCATOR_UNEXPECTED_PROFILE_UPDATE);
  }
  
  public void updateLoad(ServerLocation location, ServerLoad load, List clientIds) {
    if(getLogWriterI18n().fineEnabled()) {
      getLogWriterI18n().fine("ServerLocator: Received a load update from " + location +", " + load);
    }
    loadSnapshot.updateLoad(location, load, clientIds);
    this.stats.incServerLoadUpdates();
  }

  private void logServers() {
    if (productUseLog != null) {
      StringBuilder sb = new StringBuilder(1000);
      Map<ServerLocation, ServerLoad> loadMap = getLoadMap();
      if (loadMap.size() == 0) {
        return;
      }
      
      int connections = 0;
      for (ServerLoad l: loadMap.values()) {
        connections += l.getConnectionLoad();
      }
      sb.append("server summary: ")
        .append(loadMap.size())
        .append(" cache servers with ")
        .append(connections)
        .append(" client connections")
        .append(File.separator)
      .append("current cache servers : ");
      
      synchronized(ownerMap) {
        String[] ids = new String[ownerMap.size()];
        int i=0;
        for (DistributedMember id: ownerMap.values()) {
          ids[i++] = id.toString();
        }
        Arrays.sort(ids);
        for (i=0; i<ids.length; i++) {
          sb.append(ids[i]).append(' ');
        }
      }
      productUseLog.log(sb.toString());
    }
  }
  /**
   * Test hook to get the load on all of the servers. Returns a map of
   * ServerLocation-> Load object with the current load on that server
   */
  
  public Map getLoadMap() {
    return loadSnapshot.getLoadMap();
  }

  LogWriterI18n getLogWriterI18n() {
    return ds.getLogWriter().convertToLogWriterI18n();
  }
  
}
