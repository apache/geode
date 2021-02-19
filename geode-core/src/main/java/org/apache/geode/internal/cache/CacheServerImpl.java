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
package org.apache.geode.internal.cache;

import static java.lang.Integer.MAX_VALUE;
import static java.lang.Integer.getInteger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelCriterion;
import org.apache.geode.GemFireIOException;
import org.apache.geode.InternalGemFireError;
import org.apache.geode.InvalidValueException;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.cache.ClientSession;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.DynamicRegionFactory;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.InterestRegistrationListener;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.server.ClientSubscriptionConfig;
import org.apache.geode.cache.server.ServerLoadProbe;
import org.apache.geode.cache.server.internal.LoadMonitor;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionAdvisee;
import org.apache.geode.distributed.internal.DistributionAdvisor;
import org.apache.geode.distributed.internal.DistributionAdvisor.Profile;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ResourceEvent;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.distributed.internal.membership.api.MemberDataBuilder;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.admin.ClientHealthMonitoringRegion;
import org.apache.geode.internal.cache.CacheServerAdvisor.CacheServerProfile;
import org.apache.geode.internal.cache.ha.HARegionQueue;
import org.apache.geode.internal.cache.tier.Acceptor;
import org.apache.geode.internal.cache.tier.OverflowAttributes;
import org.apache.geode.internal.cache.tier.sockets.AcceptorBuilder;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier.CacheClientNotifierProvider;
import org.apache.geode.internal.cache.tier.sockets.ClientHealthMonitor.ClientHealthMonitorProvider;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.ConnectionListener;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.cache.tier.sockets.ServerConnectionFactory;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.logging.internal.OSProcess;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.membership.ClientMembership;
import org.apache.geode.management.membership.ClientMembershipListener;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * An implementation of the{@code CacheServer} interface that delegates most of the heavy lifting to
 * an {@link Acceptor}.
 *
 * @since GemFire 4.0
 */
@SuppressWarnings("deprecation")
public class CacheServerImpl extends AbstractCacheServer implements DistributionAdvisee {
  private static final Logger logger = LogService.getLogger();

  private static final int FORCE_LOAD_UPDATE_FREQUENCY = getInteger(
      GeodeGlossary.GEMFIRE_PREFIX + "BridgeServer.FORCE_LOAD_UPDATE_FREQUENCY", 10);

  static final String CACHE_SERVER_BIND_ADDRESS_NOT_AVAILABLE_EXCEPTION_MESSAGE =
      "A cache server's bind address is only available if it has been started";

  private final SecurityService securityService;

  private final StatisticsClock statisticsClock;

  private final AcceptorBuilder acceptorBuilder;

  private final boolean sendResourceEvents;

  private final boolean includeMembershipGroups;

  /**
   * The server connection factory, that provides a {@link ServerConnection}.
   */
  private final ServerConnectionFactory serverConnectionFactory = new ServerConnectionFactory();

  /** The acceptor that does the actual serving */
  private volatile Acceptor acceptor;

  /**
   * The advisor used by this cache server.
   *
   * @since GemFire 5.7
   */
  private volatile CacheServerAdvisor advisor;

  /**
   * The monitor used to monitor load on this cache server and distribute load to the locators
   *
   * @since GemFire 5.7
   */
  private volatile LoadMonitor loadMonitor;

  /** is this a server created by a launcher as opposed to by an application or XML? */
  private boolean isDefaultServer;

  /**
   * Needed because this server is an advisee
   *
   * @since GemFire 5.7
   */
  private int serialNumber; // changed on each start

  private final Supplier<SocketCreator> socketCreatorSupplier;
  private final CacheClientNotifierProvider cacheClientNotifierProvider;
  private final ClientHealthMonitorProvider clientHealthMonitorProvider;
  private final Function<DistributionAdvisee, CacheServerAdvisor> cacheServerAdvisorProvider;

  public static final boolean ENABLE_NOTIFY_BY_SUBSCRIPTION_FALSE = Boolean.getBoolean(
      GeodeGlossary.GEMFIRE_PREFIX + "cache-server.enable-notify-by-subscription-false");

  CacheServerImpl(final InternalCache cache,
      final SecurityService securityService,
      final StatisticsClock statisticsClock,
      final AcceptorBuilder acceptorBuilder,
      final boolean sendResourceEvents,
      final boolean includeMembershipGroups,
      final Supplier<SocketCreator> socketCreatorSupplier,
      final CacheClientNotifierProvider cacheClientNotifierProvider,
      final ClientHealthMonitorProvider clientHealthMonitorProvider,
      final Function<DistributionAdvisee, CacheServerAdvisor> cacheServerAdvisorProvider) {
    super(cache);
    this.securityService = securityService;
    this.statisticsClock = statisticsClock;
    this.acceptorBuilder = acceptorBuilder;
    this.sendResourceEvents = sendResourceEvents;
    this.includeMembershipGroups = includeMembershipGroups;
    this.socketCreatorSupplier = socketCreatorSupplier;
    this.cacheClientNotifierProvider = cacheClientNotifierProvider;
    this.clientHealthMonitorProvider = clientHealthMonitorProvider;
    this.cacheServerAdvisorProvider = cacheServerAdvisorProvider;
  }

  @Override
  public CancelCriterion getCancelCriterion() {
    return cache.getCancelCriterion();
  }

  @Override
  public StatisticsClock getStatisticsClock() {
    return statisticsClock;
  }

  /**
   * Checks to see whether or not this cache server is running. If so, an
   * {@link IllegalStateException} is thrown.
   */
  private void checkRunning() {
    if (this.isRunning()) {
      throw new IllegalStateException(
          "A cache server's configuration cannot be changed once it is running.");
    }
  }

  @Override
  public int getPort() {
    if (this.acceptor != null) {
      return this.acceptor.getPort();
    } else {
      return super.getPort();
    }
  }

  @Override
  public void setPort(int port) {
    checkRunning();
    super.setPort(port);
  }

  @Override
  public void setBindAddress(String address) {
    checkRunning();
    super.setBindAddress(address);
  }

  @Override
  public void setHostnameForClients(String name) {
    checkRunning();
    super.setHostnameForClients(name);
  }

  @Override
  public void setMaxConnections(int maxCon) {
    checkRunning();
    super.setMaxConnections(maxCon);
  }

  @Override
  public void setMaxThreads(int maxThreads) {
    checkRunning();
    super.setMaxThreads(maxThreads);
  }

  @Override
  public void setNotifyBySubscription(boolean b) {
    checkRunning();
    if (CacheServerImpl.ENABLE_NOTIFY_BY_SUBSCRIPTION_FALSE) {
      this.notifyBySubscription = b;
    }
  }

  @Override
  public void setMaximumMessageCount(int maximumMessageCount) {
    checkRunning();
    super.setMaximumMessageCount(maximumMessageCount);
  }

  @Override
  public void setSocketBufferSize(int socketBufferSize) {
    this.socketBufferSize = socketBufferSize;
  }

  @Override
  public int getSocketBufferSize() {
    return this.socketBufferSize;
  }

  @Override
  public void setMaximumTimeBetweenPings(int maximumTimeBetweenPings) {
    this.maximumTimeBetweenPings = maximumTimeBetweenPings;
  }

  @Override
  public int getMaximumTimeBetweenPings() {
    return this.maximumTimeBetweenPings;
  }


  @Override
  public void setLoadPollInterval(long loadPollInterval) {
    checkRunning();
    super.setLoadPollInterval(loadPollInterval);
  }

  @Override
  public int getMaximumMessageCount() {
    return this.maximumMessageCount;
  }

  @Override
  public void setLoadProbe(ServerLoadProbe loadProbe) {
    checkRunning();
    super.setLoadProbe(loadProbe);
  }

  @Override
  public int getMessageTimeToLive() {
    return this.messageTimeToLive;
  }

  @Override
  public ClientSubscriptionConfig getClientSubscriptionConfig() {
    return this.clientSubscriptionConfig;
  }

  public boolean isDefaultServer() {
    return isDefaultServer;
  }

  public void setIsDefaultServer() {
    this.isDefaultServer = true;
  }

  /**
   * Sets the configuration of <b>this</b>{@code CacheServer} based on the configuration of
   * <b>another</b>{@code CacheServer}.
   */
  public void configureFrom(CacheServer other) {
    setPort(other.getPort());
    setBindAddress(other.getBindAddress());
    setHostnameForClients(other.getHostnameForClients());
    setMaxConnections(other.getMaxConnections());
    setMaxThreads(other.getMaxThreads());
    setNotifyBySubscription(other.getNotifyBySubscription());
    setSocketBufferSize(other.getSocketBufferSize());
    setTcpNoDelay(other.getTcpNoDelay());
    setMaximumTimeBetweenPings(other.getMaximumTimeBetweenPings());
    setMaximumMessageCount(other.getMaximumMessageCount());
    setMessageTimeToLive(other.getMessageTimeToLive());
    setGroups(other.getGroups());
    setLoadProbe(other.getLoadProbe());
    setLoadPollInterval(other.getLoadPollInterval());
    ClientSubscriptionConfig cscOther = other.getClientSubscriptionConfig();
    ClientSubscriptionConfig cscThis = this.getClientSubscriptionConfig();
    // added for configuration of ha overflow
    cscThis.setEvictionPolicy(cscOther.getEvictionPolicy());
    cscThis.setCapacity(cscOther.getCapacity());
    String diskStoreName = cscOther.getDiskStoreName();
    if (diskStoreName != null) {
      cscThis.setDiskStoreName(diskStoreName);
    } else {
      cscThis.setOverflowDirectory(cscOther.getOverflowDirectory());
    }
  }

  @Override
  public synchronized void start() throws IOException {
    Assert.assertTrue(this.cache != null);

    this.serialNumber = createSerialNumber();
    if (DynamicRegionFactory.get().isOpen()) {
      // force notifyBySubscription to be true so that meta info is pushed
      // from servers to clients instead of invalidates.
      if (!this.notifyBySubscription) {
        logger.info("Forcing notifyBySubscription to support dynamic regions");
        this.notifyBySubscription = true;
      }
    }
    this.advisor = cacheServerAdvisorProvider.apply(this);
    this.loadMonitor = new LoadMonitor(loadProbe, maxConnections, loadPollInterval,
        FORCE_LOAD_UPDATE_FREQUENCY, advisor);

    ClientSubscriptionConfig clientSubscriptionConfig = getClientSubscriptionConfig();
    String diskStoreName = clientSubscriptionConfig.getDiskStoreName();
    OverflowAttributes overflowAttributes = new OverflowAttributes() {

      @Override
      public String getEvictionPolicy() {
        return clientSubscriptionConfig.getEvictionPolicy();
      }

      @Override
      public int getQueueCapacity() {
        return clientSubscriptionConfig.getCapacity();
      }

      @Override
      public int getPort() {
        return port;
      }

      @Override
      public boolean isDiskStore() {
        return diskStoreName != null;
      }

      @Override
      public String getOverflowDirectory() {
        return clientSubscriptionConfig.getOverflowDirectory();
      }

      @Override
      public String getDiskStoreName() {
        return diskStoreName;
      }
    };

    acceptor = createAcceptor(overflowAttributes);

    this.acceptor.start();
    this.advisor.handshake();
    this.loadMonitor.start(new ServerLocation(getExternalAddress(), getPort()),
        acceptor.getStats());

    // TODO : Need to provide facility to enable/disable client health monitoring.
    // Creating ClientHealthMonitoring region.
    // Force initialization on current cache
    ClientHealthMonitoringRegion.getInstance(this.cache);
    logger.info(String.format("CacheServer Configuration:  %s", getConfig()));

    /*
     * If the stopped cache server is restarted, we'll need to re-register the client membership
     * listener. If the listener is already registered it won't be registered as would the case when
     * start() is invoked for the first time.
     */
    ClientMembershipListener[] membershipListeners =
        ClientMembership.getClientMembershipListeners();

    boolean membershipListenerRegistered = false;
    for (ClientMembershipListener membershipListener : membershipListeners) {
      // just checking by reference as the listener instance is final
      if (listener == membershipListener) {
        membershipListenerRegistered = true;
        break;
      }
    }

    if (!membershipListenerRegistered) {
      ClientMembership.registerClientMembershipListener(listener);
    }

    if (sendResourceEvents) {
      InternalDistributedSystem system = cache.getInternalDistributedSystem();
      system.handleResourceEvent(ResourceEvent.CACHE_SERVER_START, this);
    }
  }

  @Override
  public Acceptor createAcceptor(OverflowAttributes overflowAttributes) throws IOException {
    acceptorBuilder.forServer(this);
    return acceptorBuilder.create(overflowAttributes);
  }

  /**
   * Gets the address that this cache server can be contacted on from external processes.
   *
   * @since GemFire 5.7
   */
  @Override
  public String getExternalAddress() {
    return getExternalAddress(true);
  }

  public String getExternalAddress(boolean checkServerRunning) {
    if (checkServerRunning) {
      if (!this.isRunning()) {
        this.cache.getCancelCriterion().checkCancelInProgress(null);
        throw new IllegalStateException(CACHE_SERVER_BIND_ADDRESS_NOT_AVAILABLE_EXCEPTION_MESSAGE);
      }
    }
    if (this.hostnameForClients == null || this.hostnameForClients.isEmpty()) {
      if (this.acceptor != null) {
        return this.acceptor.getExternalAddress();
      } else {
        return null;
      }
    } else {
      return this.hostnameForClients;
    }
  }

  @Override
  public boolean isRunning() {
    return this.acceptor != null && this.acceptor.isRunning();
  }

  @Override
  public synchronized void stop() {
    if (!isRunning()) {
      return;
    }

    RuntimeException firstException = null;

    try {
      if (this.loadMonitor != null) {
        this.loadMonitor.stop();
      }
    } catch (RuntimeException e) {
      logger.warn("CacheServer - Error closing load monitor", e);
      firstException = e;
    }

    try {
      if (this.advisor != null) {
        this.advisor.close();
      }
    } catch (RuntimeException e) {
      logger.warn("CacheServer - Error closing advisor", e);
      firstException = e;
    }

    try {
      if (this.acceptor != null) {
        this.acceptor.close();
      }
    } catch (RuntimeException e) {
      logger.warn("CacheServer - Error closing acceptor monitor", e);
      if (firstException != null) {
        firstException = e;
      }
    }

    if (firstException != null) {
      throw firstException;
    }

    // TODO : We need to clean up the admin region created for client
    // monitoring.

    // BridgeServer is still available, just not running, so we don't take
    // it out of the cache's list...
    // cache.removeBridgeServer(this);

    /* Assuming start won't be called after stop */
    ClientMembership.unregisterClientMembershipListener(listener);

    TXManagerImpl txMgr = (TXManagerImpl) cache.getCacheTransactionManager();
    txMgr.removeHostedTXStatesForClients();

    if (sendResourceEvents) {
      InternalDistributedSystem system = cache.getInternalDistributedSystem();
      system.handleResourceEvent(ResourceEvent.CACHE_SERVER_STOP, this);
    }
  }

  private String getConfig() {
    ClientSubscriptionConfig csc = this.getClientSubscriptionConfig();
    String str = "port=" + getPort() + " max-connections=" + getMaxConnections() + " max-threads="
        + getMaxThreads() + " notify-by-subscription=" + getNotifyBySubscription()
        + " socket-buffer-size=" + getSocketBufferSize() + " maximum-time-between-pings="
        + getMaximumTimeBetweenPings() + " maximum-message-count=" + getMaximumMessageCount()
        + " message-time-to-live=" + getMessageTimeToLive() + " eviction-policy="
        + csc.getEvictionPolicy() + " capacity=" + csc.getCapacity() + " overflow directory=";
    if (csc.getDiskStoreName() != null) {
      str += csc.getDiskStoreName();
    } else {
      str += csc.getOverflowDirectory();
    }
    str += " groups=" + Arrays.asList(getGroups()) + " loadProbe=" + loadProbe
        + " loadPollInterval=" + loadPollInterval + " tcpNoDelay=" + tcpNoDelay;
    return str;
  }

  @Override
  public String toString() {
    ClientSubscriptionConfig csc = this.getClientSubscriptionConfig();
    String str = "CacheServer on port=" + getPort() + " client subscription config policy="
        + csc.getEvictionPolicy() + " client subscription config capacity=" + csc.getCapacity();
    if (csc.getDiskStoreName() != null) {
      str += " client subscription config overflow disk store=" + csc.getDiskStoreName();
    } else {
      str += " client subscription config overflow directory=" + csc.getOverflowDirectory();
    }
    return str;
  }

  /**
   * Test method used to access the internal acceptor
   *
   * @return the internal acceptor
   */
  @Override
  public Acceptor getAcceptor() {
    return this.acceptor;
  }

  // DistributionAdvisee methods

  @Override
  public DistributionManager getDistributionManager() {
    return getSystem().getDistributionManager();
  }

  @Override
  public ClientSession getClientSession(String durableClientId) {
    return getCacheClientNotifier().getClientProxy(durableClientId);
  }

  @Override
  public ClientSession getClientSession(DistributedMember member) {
    return getCacheClientNotifier().getClientProxy(ClientProxyMembershipID.getClientId(member));
  }

  @Override
  public Set getAllClientSessions() {
    return new HashSet(getCacheClientNotifier().getClientProxies());
  }

  /**
   * create client subscription
   *
   * @return client subscription name
   * @since GemFire 5.7
   */
  public static String clientMessagesRegion(InternalCache cache, String ePolicy, int capacity,
      int port, String overFlowDir, boolean isDiskStore) {
    InternalRegionFactory factory =
        getRegionFactoryForClientMessagesRegion(cache, ePolicy, capacity, overFlowDir, isDiskStore);
    return createClientMessagesRegion(factory, port);
  }

  private static InternalRegionFactory getRegionFactoryForClientMessagesRegion(InternalCache cache,
      String ePolicy, int capacity, String overflowDir, boolean isDiskStore)
      throws InvalidValueException, GemFireIOException {
    InternalRegionFactory factory = cache.createInternalRegionFactory();
    factory.setScope(Scope.LOCAL);

    if (isDiskStore) {
      // overflowDir parameter is actually diskstore name
      factory.setDiskStoreName(overflowDir);
      // client subscription queue is always overflow to disk, so do async
      // see feature request #41479
      factory.setDiskSynchronous(true);
    } else if (overflowDir == null
        || overflowDir.equals(ClientSubscriptionConfig.DEFAULT_OVERFLOW_DIRECTORY)) {
      factory.setDiskStoreName(null);
      // client subscription queue is always overflow to disk, so do async
      // see feature request #41479
      factory.setDiskSynchronous(true);
    } else {
      File dir = new File(
          overflowDir + File.separatorChar + generateNameForClientMsgsRegion(OSProcess.getId()));
      // This will delete the overflow directory when virtual machine terminates.
      dir.deleteOnExit();
      if (!dir.mkdirs() && !dir.isDirectory()) {
        throw new GemFireIOException(
            "Could not create client subscription overflow directory: " + dir.getAbsolutePath());
      }
      File[] dirs = {dir};

      DiskStoreFactory dsf = cache.createDiskStoreFactory();
      dsf.setAutoCompact(true).setDiskDirsAndSizes(dirs, new int[] {MAX_VALUE}).create("bsi");

      factory.setDiskStoreName("bsi");
      // backward compatibility, it was sync
      factory.setDiskSynchronous(true);
    }
    factory.setDataPolicy(DataPolicy.NORMAL);
    // enable statistics
    factory.setStatisticsEnabled(true);
    /* setting LIFO related eviction attributes */
    if (HARegionQueue.HA_EVICTION_POLICY_ENTRY.equals(ePolicy)) {
      factory.setEvictionAttributes(
          EvictionAttributes.createLIFOEntryAttributes(capacity, EvictionAction.OVERFLOW_TO_DISK));
    } else if (HARegionQueue.HA_EVICTION_POLICY_MEMORY.equals(ePolicy)) {
      // condition refinement
      factory.setEvictionAttributes(
          EvictionAttributes.createLIFOMemoryAttributes(capacity, EvictionAction.OVERFLOW_TO_DISK));
    } else {
      // throw invalid eviction policy exception
      throw new InvalidValueException(
          String.format("%s Invalid eviction policy", ePolicy));
    }
    return factory;
  }

  private static String createClientMessagesRegion(InternalRegionFactory factory, int port) {
    // generating unique name in VM for ClientMessagesRegion
    String regionName = generateNameForClientMsgsRegion(port);
    try {
      factory.setDestroyLockFlag(true).setRecreateFlag(false)
          .setSnapshotInputStream(null).setImageTarget(null).setIsUsedForMetaRegion(true);
      factory.create(regionName);
    } catch (RegionExistsException ree) {
      InternalGemFireError assErr = new InternalGemFireError("unexpected exception");
      assErr.initCause(ree);
      throw assErr;
    }
    return regionName;
  }

  /**
   * Generates the name for the client subscription using the given id.
   *
   * @since GemFire 5.7
   */
  public static String generateNameForClientMsgsRegion(int id) {
    return ClientSubscriptionConfigImpl.CLIENT_SUBSCRIPTION + "_" + id;
  }

  /*
   * Marker class name to identify the lock more easily in thread dumps private static class
   * ClientMessagesRegionLock extends Object { }
   */
  @Override
  public DistributionAdvisor getDistributionAdvisor() {
    return this.advisor;
  }

  /**
   * Returns the BridgeServerAdvisor for this server
   */
  public CacheServerAdvisor getCacheServerAdvisor() {
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

  /**
   * Returns the underlying{@code InternalDistributedSystem} connection.
   *
   * @return the underlying{@code InternalDistributedSystem}
   */
  @Override
  public InternalDistributedSystem getSystem() {
    return cache.getInternalDistributedSystem();
  }

  @Override
  public String getName() {
    return "CacheServer";
  }

  @Override
  public String getFullPath() {
    return getName();
  }

  @MakeNotStatic
  private static final AtomicInteger profileSN = new AtomicInteger();

  private static int createSerialNumber() {
    return profileSN.incrementAndGet();
  }

  /**
   * Returns an array of all the groups of this cache server. This includes those from the groups
   * gemfire property and those explicitly added to this server.
   */
  @Override
  public String[] getCombinedGroups() {
    ArrayList<String> groupList = new ArrayList<String>();
    if (includeMembershipGroups) {
      for (String g : MemberDataBuilder.parseGroups(null, getSystem().getConfig().getGroups())) {
        if (!groupList.contains(g)) {
          groupList.add(g);
        }
      }
    }
    for (String g : getGroups()) {
      if (!groupList.contains(g)) {
        groupList.add(g);
      }
    }
    String[] groups = new String[groupList.size()];
    return groupList.toArray(groups);
  }

  @Override
  public /* synchronized causes deadlock */ void fillInProfile(Profile profile) {
    assert profile instanceof CacheServerProfile;
    CacheServerProfile bp = (CacheServerProfile) profile;
    bp.setHost(getExternalAddress(false));
    bp.setPort(getPort());
    bp.setGroups(getCombinedGroups());
    bp.setMaxConnections(maxConnections);
    bp.setInitialLoad(loadMonitor.getLastLoad());
    bp.setLoadPollInterval(getLoadPollInterval());
    bp.serialNumber = getSerialNumber();
    bp.finishInit();
  }

  @Override
  public int getSerialNumber() {
    return this.serialNumber;
  }


  protected CacheClientNotifier getCacheClientNotifier() {
    return getAcceptor().getCacheClientNotifier();
  }

  /**
   * Registers a new{@code InterestRegistrationListener} with the set of
   * {@code InterestRegistrationListener}s.
   *
   * @param listener The{@code InterestRegistrationListener} to register
   * @throws IllegalStateException if the BridgeServer has not been started
   * @since GemFire 5.8Beta
   */
  @Override
  public void registerInterestRegistrationListener(InterestRegistrationListener listener) {
    if (!this.isRunning()) {
      throw new IllegalStateException(
          "The cache server must be running to use this operation");
    }
    getCacheClientNotifier().registerInterestRegistrationListener(listener);
  }

  /**
   * Unregisters an existing{@code InterestRegistrationListener} from the set of
   * {@code InterestRegistrationListener}s.
   *
   * @param listener The{@code InterestRegistrationListener} to unregister
   *
   * @since GemFire 5.8Beta
   */
  @Override
  public void unregisterInterestRegistrationListener(InterestRegistrationListener listener) {
    getCacheClientNotifier().unregisterInterestRegistrationListener(listener);
  }

  /**
   * Returns a read-only set of{@code InterestRegistrationListener}s registered with this notifier.
   *
   * @return a read-only set of{@code InterestRegistrationListener}s registered with this notifier
   *
   * @since GemFire 5.8Beta
   */
  @Override
  public Set getInterestRegistrationListeners() {
    return getCacheClientNotifier().getInterestRegistrationListeners();
  }

  @Override
  public ConnectionListener getConnectionListener() {
    return loadMonitor;
  }

  @Override
  public ServerConnectionFactory getServerConnectionFactory() {
    return serverConnectionFactory;
  }

  @Override
  public SecurityService getSecurityService() {
    return securityService;
  }

  @Override
  public long getTimeLimitMillis() {
    return 120_000;
  }

  @Override
  public Supplier<SocketCreator> getSocketCreatorSupplier() {
    return socketCreatorSupplier;
  }

  @Override
  public CacheClientNotifierProvider getCacheClientNotifierProvider() {
    return cacheClientNotifierProvider;
  }

  @Override
  public ClientHealthMonitorProvider getClientHealthMonitorProvider() {
    return clientHealthMonitorProvider;
  }
}
