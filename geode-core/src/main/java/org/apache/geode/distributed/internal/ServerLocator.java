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
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
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
  private Map<ServerLocation, DistributedMember> ownerMap =
      new HashMap<ServerLocation, DistributedMember>();
  private volatile List<ServerLocation> cachedLocators;
  private final Object cachedLocatorsLock = new Object();

  @MakeNotStatic
  private static final AtomicInteger profileSN = new AtomicInteger();

  private static final long SERVER_LOAD_LOG_INTERVAL = (60 * 60 * 1000); // log server load once an
                                                                         // hour

  private final String logFile;
  private final String hostName;
  private final String memberName;

  private ProductUseLog productUseLog;

  private volatile long lastLogTime;

  ServerLocator() throws IOException {
    this.port = 10334;
    this.hostName = LocalHostUtil.getCanonicalLocalHostName();
    this.hostNameForClients = this.hostName;
    this.logFile = null;
    this.memberName = null;
    this.ds = null;
    this.advisor = null;
    this.stats = null;
  }

  public LocatorLoadSnapshot getLoadSnapshot() {
    return loadSnapshot;
  }

  public ServerLocator(int port, InetAddress bindAddress, String hostNameForClients, File logFile,
      ProductUseLog productUseLogWriter, String memberName, InternalDistributedSystem ds,
      LocatorStats stats) throws IOException {
    this.port = port;

    if (bindAddress == null) {
      this.hostName = LocalHostUtil.getCanonicalLocalHostName();
    } else {
      this.hostName = bindAddress.getHostAddress();
    }

    if (hostNameForClients != null && !hostNameForClients.equals("")) {
      this.hostNameForClients = hostNameForClients;
    } else {
      this.hostNameForClients = this.hostName;
    }

    this.logFile = logFile != null ? logFile.getCanonicalPath() : null;
    this.memberName = memberName;
    this.productUseLog = productUseLogWriter;

    this.ds = ds;
    this.advisor = ControllerAdvisor.createControllerAdvisor(this); // escapes constructor but
                                                                    // allows field to be final
    this.stats = stats;
  }

  public String getHostName() {
    return this.hostNameForClients;
  }

  public int getPort() {
    return this.port;
  }

  @Override
  public CancelCriterion getCancelCriterion() {
    return this.ds.getCancelCriterion();
  }

  @Override
  public void init(TcpServer tcpServer) {
    // if the ds is reconnecting we don't want to start server
    // location services until the DS finishes connecting
    if (this.ds != null && !this.ds.isReconnecting()) {
      this.advisor.handshake();
    }
  }

  /**
   * A ServerLocator may be in place but non-functional during auto-reconnect because peer location
   * services have been initialized while the servers reconnect but server location services aren't
   * reconnected until a cache is available
   */
  protected boolean readyToProcessRequests() {
    return this.ds != null;
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
        response = new LocatorStatusResponse().initialize(this.port, this.hostName, this.logFile,
            this.memberName);
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
    ArrayList servers = loadSnapshot.getServers(clientRequest.getServerGroup());
    return new GetAllServersResponse(servers);
  }

  private Object getLocatorListResponse(LocatorListRequest request) {
    List<ServerLocation> controllers = getLocators();
    boolean balanced = loadSnapshot.hasBalancedConnections(request.getServerGroup());
    return new LocatorListResponse(controllers, balanced);
  }

  private Object pickQueueServers(QueueConnectionRequest clientRequest) {
    Set excludedServers = new HashSet(clientRequest.getExcludedServers());

    /* If this is a request to find durable queues, lets go find them */

    ArrayList servers = new ArrayList();
    boolean durableQueueFound = false;
    if (clientRequest.isFindDurable() && clientRequest.getProxyId().isDurable()) {
      servers = FindDurableQueueProcessor.sendAndFind(this, clientRequest.getProxyId(),
          getDistributionManager());
      /* add the found durables to exclude list so they aren't candidates for more queues */
      excludedServers.addAll(servers);
      durableQueueFound = servers.size() > 0;
    }

    List candidates;
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
    this.advisor.close();
    this.loadSnapshot.shutDown();
  }

  @Override
  public void restarting(DistributedSystem ds, GemFireCache cache,
      InternalConfigurationPersistenceService sharedConfig) {
    if (ds != null) {
      this.loadSnapshot = new LocatorLoadSnapshot();
      this.ds = (InternalDistributedSystem) ds;
      this.advisor = ControllerAdvisor.createControllerAdvisor(this); // escapes constructor but
    }
  }

  public void restartCompleted(DistributedSystem ds) {
    if (ds.isConnected()) {
      this.advisor.handshake(); // GEODE-1393: need to get server information during restart
    }
  }

  // DistributionAdvisee methods
  @Override
  public DistributionManager getDistributionManager() {
    return getSystem().getDistributionManager();
  }

  @Override
  public DistributionAdvisor getDistributionAdvisor() {
    return this.advisor;
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
    return this.ds;
  }

  @Override
  public String getName() {
    return "ServerLocator";
  }

  @Override
  public int getSerialNumber() {
    return this.serialNumber;
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
      loadSnapshot.addServer(location, groups, bp.getInitialLoad(), bp.getLoadPollInterval());
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
      loadSnapshot.removeServer(location);
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

  public void updateLoad(ServerLocation location, ServerLoad load, List clientIds) {
    if (getLogWriter().fineEnabled()) {
      getLogWriter()
          .fine("ServerLocator: Received a load update from " + location + ", " + load);
    }
    loadSnapshot.updateLoad(location, load, clientIds);
    this.stats.incServerLoadUpdates();
    logServers();
  }

  private void logServers() {
    if (productUseLog != null) {
      Map<ServerLocation, ServerLoad> loadMap = getLoadMap();
      if (loadMap.size() == 0) {
        return;
      }

      long now = System.currentTimeMillis();
      long lastLogTime = this.lastLogTime;
      if (now < lastLogTime + SERVER_LOAD_LOG_INTERVAL) {
        return;
      }
      this.lastLogTime = now;

      int queues = 0;
      int connections = 0;
      for (ServerLoad l : loadMap.values()) {
        queues += l.getSubscriptionConnectionLoad();
        connections = (int) Math.ceil(l.getConnectionLoad() / l.getLoadPerConnection());
      }

      Set<DistributedMember> servers;
      synchronized (ownerMap) {
        servers = new HashSet<>(ownerMap.values());
      }

      StringBuilder sb = new StringBuilder(1000);
      sb.append("server count: ").append(servers.size()).append(" connected client count: ")
          .append(connections).append(" client subscription queue count: ").append(queues)
          .append(System.lineSeparator()).append("current servers : ");

      String[] ids = new String[servers.size()];
      int i = 0;
      for (DistributedMember id : servers) {
        ids[i++] = id.toString();
      }
      Arrays.sort(ids);
      for (i = 0; i < ids.length; i++) {
        sb.append(ids[i]).append(' ');
      }
      productUseLog.log(sb.toString());
    }
  }

  /**
   * Test hook to get the load on all of the servers. Returns a map of ServerLocation-> Load object
   * with the current load on that server
   */

  public Map getLoadMap() {
    return loadSnapshot.getLoadMap();
  }

  LogWriter getLogWriter() {
    return ds.getLogWriter();
  }

}
