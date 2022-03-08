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
package org.apache.geode.distributed.internal;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelCriterion;
import org.apache.geode.InternalGemFireException;
import org.apache.geode.LogWriter;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.cache.client.internal.locator.ClientConnectionRequest;
import org.apache.geode.cache.client.internal.locator.ClientConnectionResponse;
import org.apache.geode.cache.client.internal.locator.ClientReplacementRequest;
import org.apache.geode.cache.client.internal.locator.GetAllServersRequest;
import org.apache.geode.cache.client.internal.locator.GetAllServersResponse;
import org.apache.geode.cache.client.internal.locator.LocatorListRequest;
import org.apache.geode.cache.client.internal.locator.LocatorListResponse;
import org.apache.geode.cache.client.internal.locator.LocatorStatusResponse;
import org.apache.geode.cache.client.internal.locator.QueueConnectionRequest;
import org.apache.geode.cache.client.internal.locator.QueueConnectionResponse;
import org.apache.geode.cache.client.internal.locator.ServerLocationRequest;
import org.apache.geode.cache.server.ServerLoad;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.DistributionAdvisor.Profile;
import org.apache.geode.distributed.internal.tcpserver.TcpHandler;
import org.apache.geode.distributed.internal.tcpserver.TcpServer;
import org.apache.geode.internal.cache.CacheServerAdvisor.CacheServerProfile;
import org.apache.geode.internal.cache.ControllerAdvisor;
import org.apache.geode.internal.cache.ControllerAdvisor.ControllerProfile;
import org.apache.geode.internal.cache.FindDurableQueueProcessor;
import org.apache.geode.internal.cache.GridAdvisor.GridProfile;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 *
 * @since GemFire 5.7
 */
public class ServerLocator implements TcpHandler, RestartHandler, DistributionAdvisee {
  private static final Logger logger = LogService.getLogger();

  private final int port;
  private final String hostNameForClients;
  private InternalDistributedSystem ds;
  private ControllerAdvisor advisor;
  private final int serialNumber = createSerialNumber();
  private final LocatorStats stats;
  private LocatorLoadSnapshot loadSnapshot = new LocatorLoadSnapshot();
  private final Map<ServerLocation, DistributedMember> ownerMap =
      new HashMap<>();
  private volatile List<ServerLocation> cachedLocators;
  private final Object cachedLocatorsLock = new Object();

  @MakeNotStatic
  private static final AtomicInteger profileSN = new AtomicInteger();

  private final String logFile;
  private final String hostName;
  private final String memberName;

  ServerLocator() throws IOException {
    port = 10334;
    hostName = LocalHostUtil.getCanonicalLocalHostName();
    hostNameForClients = hostName;
    logFile = null;
    memberName = null;
    ds = null;
    advisor = null;
    stats = null;
  }

  public LocatorLoadSnapshot getLoadSnapshot() {
    return loadSnapshot;
  }

  public ServerLocator(int port, String bindAddress, String hostNameForClients, File logFile,
      String memberName, InternalDistributedSystem ds,
      LocatorStats stats) throws IOException {
    this.port = port;

    if (bindAddress == null || bindAddress.isEmpty()) {
      hostName = LocalHostUtil.getCanonicalLocalHostName();
    } else {
      hostName = bindAddress;
    }

    if (hostNameForClients != null && !hostNameForClients.equals("")) {
      this.hostNameForClients = hostNameForClients;
    } else {
      this.hostNameForClients = hostName;
    }

    this.logFile = logFile != null ? logFile.getCanonicalPath() : null;
    this.memberName = memberName;
    this.ds = ds;
    advisor = ControllerAdvisor.createControllerAdvisor(this); // escapes constructor but
                                                               // allows field to be final
    this.stats = stats;
  }

  public String getHostName() {
    return hostNameForClients;
  }

  public int getPort() {
    return port;
  }

  @Override
  public CancelCriterion getCancelCriterion() {
    return ds.getCancelCriterion();
  }

  @Override
  public void init(TcpServer tcpServer) {
    // if the ds is reconnecting we don't want to start server
    // location services until the DS finishes connecting
    if (ds != null && !ds.isReconnecting()) {
      advisor.handshake();
    }
  }

  /**
   * A ServerLocator may be in place but non-functional during auto-reconnect because peer location
   * services have been initialized while the servers reconnect but server location services aren't
   * reconnected until a cache is available
   */
  protected boolean readyToProcessRequests() {
    return ds != null;
  }

  @Override
  public Object processRequest(Object request) {
    if (!readyToProcessRequests()) {
      return null;
    }

    if (logger.isDebugEnabled()) {
      logger.debug("ServerLocator: Received request {}", request);
    }

    if (!(request instanceof ServerLocationRequest)) {
      throw new InternalGemFireException(
          "Expected ServerLocationRequest, got " + request.getClass());
    }

    Object response;
    int id = ((DataSerializableFixedID) request).getDSFID();
    switch (id) {
      case DataSerializableFixedID.LOCATOR_STATUS_REQUEST:
        response = new LocatorStatusResponse().initialize(port, hostName, logFile,
            memberName);
        break;
      case DataSerializableFixedID.LOCATOR_LIST_REQUEST:
        response = getLocatorListResponse((LocatorListRequest) request);
        break;
      case DataSerializableFixedID.CLIENT_REPLACEMENT_REQUEST:
        response = pickReplacementServer((ClientReplacementRequest) request);
        break;
      case DataSerializableFixedID.GET_ALL_SERVERS_REQUEST:
        response = pickAllServers((GetAllServersRequest) request);
        break;
      case DataSerializableFixedID.CLIENT_CONNECTION_REQUEST:
        response = pickServer((ClientConnectionRequest) request);
        break;
      case DataSerializableFixedID.QUEUE_CONNECTION_REQUEST:
        response = pickQueueServers((QueueConnectionRequest) request);
        break;
      default:
        throw new InternalGemFireException("Unknown ServerLocationRequest: " + request.getClass());
    }

    if (logger.isDebugEnabled()) {
      logger.debug("ServerLocator: Sending response {}", response);
    }

    return response;
  }

  private ClientConnectionResponse pickServer(ClientConnectionRequest clientRequest) {
    ServerLocation location = loadSnapshot.getServerForConnection(clientRequest.getServerGroup(),
        clientRequest.getExcludedServers());
    return new ClientConnectionResponse(location);
  }

  private ClientConnectionResponse pickReplacementServer(ClientReplacementRequest clientRequest) {
    ServerLocation location =
        loadSnapshot.getReplacementServerForConnection(clientRequest.getCurrentServer(),
            clientRequest.getServerGroup(), clientRequest.getExcludedServers());
    return new ClientConnectionResponse(location);
  }


  private GetAllServersResponse pickAllServers(GetAllServersRequest clientRequest) {
    ArrayList<ServerLocation> servers = loadSnapshot.getServers(clientRequest.getServerGroup());
    return new GetAllServersResponse(servers);
  }

  private Object getLocatorListResponse(LocatorListRequest request) {
    List<ServerLocation> controllers = getLocators();
    boolean balanced = loadSnapshot.hasBalancedConnections(request.getServerGroup());
    return new LocatorListResponse(controllers, balanced);
  }

  private Object pickQueueServers(QueueConnectionRequest clientRequest) {
    Set<ServerLocation> excludedServers = new HashSet<>(clientRequest.getExcludedServers());

    /* If this is a request to find durable queues, lets go find them */

    List<ServerLocation> servers = new ArrayList<>();
    boolean durableQueueFound = false;
    if (clientRequest.isFindDurable() && clientRequest.getProxyId().isDurable()) {
      servers = FindDurableQueueProcessor.sendAndFind(this, clientRequest.getProxyId(),
          getDistributionManager());
      /* add the found durables to exclude list so they aren't candidates for more queues */
      excludedServers.addAll(servers);
      durableQueueFound = servers.size() > 0;
    }

    List<ServerLocation> candidates;
    if (clientRequest.getRedundantCopies() == -1) {
      /* We need all the servers we can get */
      candidates = loadSnapshot.getServersForQueue(clientRequest.getProxyId(),
          clientRequest.getServerGroup(), excludedServers, -1);
    } else if (clientRequest.getRedundantCopies() > servers.size()) {
      /* We need more servers. */
      int count = clientRequest.getRedundantCopies() - servers.size();
      candidates = loadSnapshot.getServersForQueue(clientRequest.getProxyId(),
          clientRequest.getServerGroup(), excludedServers, count);
    } else {
      /* Otherwise, we don't need any more servers */
      candidates = Collections.EMPTY_LIST;
    }

    if (candidates.size() > 1) {
      Collections.shuffle(candidates);
    }
    servers.addAll(candidates);

    return new QueueConnectionResponse(durableQueueFound, servers);
  }

  @Override
  public void shutDown() {
    advisor.close();
    loadSnapshot.shutDown();
  }

  @Override
  public void restarting(DistributedSystem ds, GemFireCache cache,
      InternalConfigurationPersistenceService sharedConfig) {
    if (ds != null) {
      loadSnapshot = new LocatorLoadSnapshot();
      this.ds = (InternalDistributedSystem) ds;
      advisor = ControllerAdvisor.createControllerAdvisor(this); // escapes constructor but
    }
  }

  public void restartCompleted(DistributedSystem ds) {
    if (ds.isConnected()) {
      advisor.handshake(); // GEODE-1393: need to get server information during restart
    }
  }

  // DistributionAdvisee methods
  @Override
  public DistributionManager getDistributionManager() {
    return getSystem().getDistributionManager();
  }

  @Override
  public DistributionAdvisor getDistributionAdvisor() {
    return advisor;
  }

  @Override
  public Profile getProfile() {
    return getDistributionAdvisor().createProfile();
  }

  @Override
  public DistributionAdvisee getParentAdvisee() {
    return null;
  }

  @Override
  public InternalDistributedSystem getSystem() {
    return ds;
  }

  @Override
  public String getName() {
    return "ServerLocator";
  }

  @Override
  public int getSerialNumber() {
    return serialNumber;
  }

  @Override
  public String getFullPath() {
    return getName();
  }

  public InternalDistributedSystem getDs() {
    return ds;
  }

  private static int createSerialNumber() {
    return profileSN.incrementAndGet();
  }

  @Override
  public void fillInProfile(Profile profile) {
    assert profile instanceof ControllerProfile;
    ControllerProfile cp = (ControllerProfile) profile;
    cp.setHost(hostNameForClients);
    cp.setPort(port);
    cp.serialNumber = getSerialNumber();
    cp.finishInit();
  }


  public void setLocatorCount(int count) {
    stats.setLocatorCount(count);
  }

  public void setServerCount(int count) {
    stats.setServerCount(count);
  }

  @Override
  public void endRequest(Object request, long startTime) {
    stats.endLocatorRequest(startTime);
  }

  @Override
  public void endResponse(Object request, long startTime) {
    stats.endLocatorResponse(startTime);
  }

  private List<ServerLocation> getLocators() {
    if (cachedLocators != null) {
      return cachedLocators;
    } else {
      synchronized (cachedLocatorsLock) {
        List<ControllerProfile> profiles = advisor.fetchControllers();
        List<ServerLocation> result = new ArrayList<>(profiles.size() + 1);
        for (ControllerProfile profile : profiles) {
          result.add(buildServerLocation(profile));
        }
        result.add(new ServerLocation(hostNameForClients, port));
        cachedLocators = result;
        return result;
      }
    }
  }

  protected static ServerLocation buildServerLocation(GridProfile p) {
    return new ServerLocation(p.getHost(), p.getPort());
  }

  public void profileCreated(Profile profile) {
    if (profile instanceof CacheServerProfile) {
      CacheServerProfile bp = (CacheServerProfile) profile;
      ServerLocation location = buildServerLocation(bp);
      String[] groups = bp.getGroups();
      loadSnapshot.addServer(
          location, bp.getDistributedMember().getUniqueId(), groups,
          bp.getInitialLoad(), bp.getLoadPollInterval());
      if (logger.isDebugEnabled()) {
        logger.debug("ServerLocator: Received load from a new server {}, {}", location,
            bp.getInitialLoad());
      }
      synchronized (ownerMap) {
        ownerMap.put(location, profile.getDistributedMember());
      }
    } else {
      cachedLocators = null;
    }
  }

  public void profileRemoved(Profile profile) {
    if (profile instanceof CacheServerProfile) {
      CacheServerProfile bp = (CacheServerProfile) profile;
      // InternalDistributedMember id = bp.getDistributedMember();
      ServerLocation location = buildServerLocation(bp);
      loadSnapshot.removeServer(location, bp.getDistributedMember().getUniqueId());
      if (logger.isDebugEnabled()) {
        logger.debug("ServerLocator: server departed {}", location);
      }
      synchronized (ownerMap) {
        ownerMap.remove(location);
      }
    } else {
      cachedLocators = null;
    }
  }

  public void profileUpdated(Profile profile) {
    cachedLocators = null;
    getLogWriter()
        .warning("ServerLocator - unexpected profile update.");
  }

  public void updateLoad(ServerLocation location, String memberId, ServerLoad load,
      List<ClientProxyMembershipID> clientIds) {
    if (getLogWriter().fineEnabled()) {
      getLogWriter()
          .fine("ServerLocator: Received a load update from " + location + " at " + memberId + " , "
              + load);
    }
    loadSnapshot.updateLoad(location, memberId, load, clientIds);
    stats.incServerLoadUpdates();
  }

  /**
   * Test hook to get the load on all of the servers. Returns a map of ServerLocation-> Load object
   * with the current load on that server
   */

  public Map<ServerLocation, ServerLoad> getLoadMap() {
    return loadSnapshot.getLoadMap();
  }

  LogWriter getLogWriter() {
    return ds.getLogWriter();
  }

}
