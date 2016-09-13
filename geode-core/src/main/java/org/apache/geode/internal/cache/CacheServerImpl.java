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
package org.apache.geode.internal.cache;

import org.apache.geode.CancelCriterion;
import org.apache.geode.GemFireIOException;
import org.apache.geode.InternalGemFireError;
import org.apache.geode.InvalidValueException;
import org.apache.geode.cache.*;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.server.ClientSubscriptionConfig;
import org.apache.geode.cache.server.ServerLoadProbe;
import org.apache.geode.cache.server.internal.LoadMonitor;
import org.apache.geode.cache.wan.GatewayTransportFilter;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.*;
import org.apache.geode.distributed.internal.DistributionAdvisor.Profile;
import org.apache.geode.distributed.internal.membership.MemberAttributes;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.OSProcess;
import org.apache.geode.internal.admin.ClientHealthMonitoringRegion;
import org.apache.geode.internal.cache.CacheServerAdvisor.CacheServerProfile;
import org.apache.geode.internal.cache.ha.HARegionQueue;
import org.apache.geode.internal.cache.tier.Acceptor;
import org.apache.geode.internal.cache.tier.sockets.AcceptorImpl;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;
import org.apache.geode.management.membership.ClientMembership;
import org.apache.geode.management.membership.ClientMembershipListener;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An implementation of the <code>CacheServer</code> interface that delegates
 * most of the heavy lifting to an {@link Acceptor}.
 * 
 * @since GemFire 4.0
 */
@SuppressWarnings("deprecation")
public class CacheServerImpl
  extends AbstractCacheServer
  implements DistributionAdvisee {

  private static final Logger logger = LogService.getLogger();

  private static final int FORCE_LOAD_UPDATE_FREQUENCY = Integer.getInteger(DistributionConfig.GEMFIRE_PREFIX + "BridgeServer.FORCE_LOAD_UPDATE_FREQUENCY", 10)
      .intValue();
  
  /** The acceptor that does the actual serving */
  private volatile AcceptorImpl acceptor;

  /**
   * The advisor used by this cache server.
   * @since GemFire 5.7
   */
  private volatile CacheServerAdvisor advisor;

  /**
   * The monitor used to monitor load on this
   * bridge server and distribute load to the locators
   * @since GemFire 5.7
   */
  private volatile LoadMonitor loadMonitor;

  /**
   * boolean that represents whether this server is a GatewayReceiver or a simple BridgeServer
   */
  private boolean isGatewayReceiver;
  
  private List<GatewayTransportFilter> gatewayTransportFilters = Collections.EMPTY_LIST;
  
  /** is this a server created by a launcher as opposed to by an application or XML? */
  private boolean isDefaultServer;
  
  /**
   * Needed because this guy is an advisee
   * @since GemFire 5.7
   */
  private int serialNumber; // changed on each start

  public static final boolean ENABLE_NOTIFY_BY_SUBSCRIPTION_FALSE =
      Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "cache-server.enable-notify-by-subscription-false");
  
 
  // ////////////////////// Constructors //////////////////////

  /**
   * Creates a new <code>BridgeServerImpl</code> that serves the contents of
   * the give <code>Cache</code>. It has the default configuration.
   */
  public CacheServerImpl(GemFireCacheImpl cache, boolean isGatewayReceiver) {
    super(cache);
    this.isGatewayReceiver = isGatewayReceiver;
  }

  // //////////////////// Instance Methods ///////////////////
  
  public CancelCriterion getCancelCriterion() {
    return cache.getCancelCriterion();    
  }

  /**
   * Checks to see whether or not this bridge server is running. If so, an
   * {@link IllegalStateException} is thrown.
   */
  private void checkRunning() {
    if (this.isRunning()) {
      throw new IllegalStateException(LocalizedStrings.CacheServerImpl_A_CACHE_SERVERS_CONFIGURATION_CANNOT_BE_CHANGED_ONCE_IT_IS_RUNNING.toLocalizedString());
    }
  }

  public boolean isGatewayReceiver() {
    return this.isGatewayReceiver;
  }
  
  @Override
  public int getPort() {
    if (this.acceptor != null) {
      return this.acceptor.getPort();
    }
    else {
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

  public void setGatewayTransportFilter(
      List<GatewayTransportFilter> transportFilters) {
    this.gatewayTransportFilters = transportFilters;
  }
  
  @Override
  public int getMessageTimeToLive() {
    return this.messageTimeToLive;
  }
  

  public ClientSubscriptionConfig getClientSubscriptionConfig(){
    return this.clientSubscriptionConfig;
  }

  public boolean isDefaultServer() {
    return isDefaultServer;
  }

  public void setIsDefaultServer() {
    this.isDefaultServer = true;
  }

  /**
   * Sets the configuration of <b>this</b> <code>CacheServer</code> based on
   * the configuration of <b>another</b> <code>CacheServer</code>.
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
        logger.info(LocalizedMessage.create(LocalizedStrings.CacheServerImpl_FORCING_NOTIFYBYSUBSCRIPTION_TO_SUPPORT_DYNAMIC_REGIONS));
        this.notifyBySubscription = true;
      }
    }
    this.advisor = CacheServerAdvisor.createCacheServerAdvisor(this);
    this.loadMonitor = new LoadMonitor(loadProbe, maxConnections,
        loadPollInterval, FORCE_LOAD_UPDATE_FREQUENCY, 
        advisor);
    List overflowAttributesList = new LinkedList();
    ClientSubscriptionConfig csc = this.getClientSubscriptionConfig();
    overflowAttributesList.add(0, csc.getEvictionPolicy());
    overflowAttributesList.add(1, Integer.valueOf(csc.getCapacity()));
    overflowAttributesList.add(2, Integer.valueOf(this.port));
    String diskStoreName = csc.getDiskStoreName();
    if (diskStoreName != null) {
      overflowAttributesList.add(3, diskStoreName);
      overflowAttributesList.add(4, true); // indicator to use diskstore
    } else {
      overflowAttributesList.add(3, csc.getOverflowDirectory());
      overflowAttributesList.add(4, false);
    }

    this.acceptor = new AcceptorImpl(getPort(), 
                                     getBindAddress(),
                                     getNotifyBySubscription(),
                                     getSocketBufferSize(), 
                                     getMaximumTimeBetweenPings(), 
                                     this.cache,
                                     getMaxConnections(), 
                                     getMaxThreads(), 
                                     getMaximumMessageCount(),
                                     getMessageTimeToLive(),
                                     this.loadMonitor,
                                     overflowAttributesList, 
                                     this.isGatewayReceiver,
                                     this.gatewayTransportFilters, this.tcpNoDelay);

    this.acceptor.start();
    this.advisor.handshake();
    this.loadMonitor.start(new ServerLocation(getExternalAddress(),
        getPort()), acceptor.getStats());
    
    // TODO : Need to provide facility to enable/disable client health monitoring.
    //Creating ClientHealthMonitoring region.
    // Force initialization on current cache
    if(cache instanceof GemFireCacheImpl) {
      ClientHealthMonitoringRegion.getInstance((GemFireCacheImpl)cache);
    }
    this.cache.getLoggerI18n().config(LocalizedStrings.CacheServerImpl_CACHESERVER_CONFIGURATION___0, getConfig());
    
    /* 
     * If the stopped bridge server is restarted, we'll need to re-register the 
     * client membership listener. If the listener is already registered it 
     * won't be registered as would the case when start() is invoked for the 
     * first time.  
     */
    ClientMembershipListener[] membershipListeners = 
                                ClientMembership.getClientMembershipListeners();
    
    boolean membershipListenerRegistered = false;
    for (ClientMembershipListener membershipListener : membershipListeners) {
      //just checking by reference as the listener instance is final
      if (listener == membershipListener) {
        membershipListenerRegistered = true;
        break;
      }
    }
    
    if (!membershipListenerRegistered) {
      ClientMembership.registerClientMembershipListener(listener);
    }
    
    if (!isGatewayReceiver) {
      InternalDistributedSystem system = ((GemFireCacheImpl) this.cache)
          .getDistributedSystem();
      system.handleResourceEvent(ResourceEvent.CACHE_SERVER_START, this);
    }
    
  }

  
  /**
   * Gets the address that this bridge server can be contacted on from external
   * processes.
   * @since GemFire 5.7
   */
  public String getExternalAddress() {
    return getExternalAddress(true);
  }
  
  public String getExternalAddress(boolean checkServerRunning) {
    if (checkServerRunning) {
      if (!this.isRunning()) {
        String s = "A bridge server's bind address is only available if it has been started";
        this.cache.getCancelCriterion().checkCancelInProgress(null);
        throw new IllegalStateException(s);
      }
    }
    if (this.hostnameForClients == null || this.hostnameForClients.equals("")) {
      if (this.acceptor != null) {
        return this.acceptor.getExternalAddress();
      }
      else {
        return null;
      }
    }
    else {
      return this.hostnameForClients;
    }
  }

  public boolean isRunning() {
    return this.acceptor != null && this.acceptor.isRunning();
  }

  public synchronized void stop() {
    if (!isRunning()) {
      return;
    }
    
    RuntimeException firstException = null;
    
    try {
      if(this.loadMonitor != null) {
        this.loadMonitor.stop();
      }
    } catch(RuntimeException e) {
      cache.getLoggerI18n().warning(LocalizedStrings.CacheServerImpl_CACHESERVER_ERROR_CLOSING_LOAD_MONITOR, e);
      firstException = e;
    }
    
    try {
      if (this.advisor != null) {
        this.advisor.close();
      }
    } catch(RuntimeException e) {
      cache.getLoggerI18n().warning(LocalizedStrings.CacheServerImpl_CACHESERVER_ERROR_CLOSING_ADVISOR, e);
      firstException = e;
    } 
    
    try {
      if (this.acceptor != null) {
        this.acceptor.close();
      }
    } catch(RuntimeException e) {
      logger.warn(LocalizedMessage.create(LocalizedStrings.CacheServerImpl_CACHESERVER_ERROR_CLOSING_ACCEPTOR_MONITOR), e);
      if (firstException != null) {
        firstException = e;
      }
    }
    
    if(firstException != null) {
      throw firstException;
    }
    
    //TODO : We need to clean up the admin region created for client
    //monitoring.
    
    // BridgeServer is still available, just not running, so we don't take
    // it out of the cache's list...
    // cache.removeBridgeServer(this);

    /* Assuming start won't be called after stop */
    ClientMembership.unregisterClientMembershipListener(listener);
    
    TXManagerImpl txMgr = (TXManagerImpl) cache.getCacheTransactionManager();
    txMgr.removeHostedTXStatesForClients();
    
    if (!isGatewayReceiver) {
      InternalDistributedSystem system = ((GemFireCacheImpl) this.cache)
          .getDistributedSystem();
      system.handleResourceEvent(ResourceEvent.CACHE_SERVER_STOP, this);
    }

  }

  private String getConfig() {
    ClientSubscriptionConfig csc = this.getClientSubscriptionConfig();
    String str =
    "port=" + getPort() + " max-connections=" + getMaxConnections()
        + " max-threads=" + getMaxThreads() + " notify-by-subscription="
        + getNotifyBySubscription() + " socket-buffer-size="
        + getSocketBufferSize() + " maximum-time-between-pings="
        + getMaximumTimeBetweenPings() + " maximum-message-count="
        + getMaximumMessageCount() + " message-time-to-live="
        + getMessageTimeToLive() + " eviction-policy=" + csc.getEvictionPolicy()
        + " capacity=" + csc.getCapacity() + " overflow directory=";
    if (csc.getDiskStoreName() != null) {
      str += csc.getDiskStoreName();
    } else {
      str += csc.getOverflowDirectory(); 
    }
    str += 
        " groups=" + Arrays.asList(getGroups())
        + " loadProbe=" + loadProbe
        + " loadPollInterval=" + loadPollInterval
        + " tcpNoDelay=" + tcpNoDelay;
    return str;
  }

  @Override
  public String toString() {
    ClientSubscriptionConfig csc = this.getClientSubscriptionConfig();
    String str = 
    "CacheServer on port=" + getPort() + " client subscription config policy="
        + csc.getEvictionPolicy() + " client subscription config capacity="
        + csc.getCapacity();
    if (csc.getDiskStoreName() != null) {
      str += " client subscription config overflow disk store="
        + csc.getDiskStoreName();
    } else {
      str += " client subscription config overflow directory="
        + csc.getOverflowDirectory();
    }
    return str;
  }

  /**
   * Test method used to access the internal acceptor
   * 
   * @return the internal acceptor
   */
  public AcceptorImpl getAcceptor() {
    return this.acceptor;
  }

  // DistributionAdvisee methods

  public DM getDistributionManager() {
    return getSystem().getDistributionManager();
  }
  
  public ClientSession getClientSession(String durableClientId) {
    return getCacheClientNotifier().getClientProxy(durableClientId);
  }

  public ClientSession getClientSession(DistributedMember member) {
    return getCacheClientNotifier().getClientProxy(
        ClientProxyMembershipID.getClientId(member));
  }
  
  public Set getAllClientSessions() {
    return new HashSet(getCacheClientNotifier().getClientProxies());
  }

  /**
   * create client subscription
   * 
   * @param cache
   * @param ePolicy
   * @param capacity
   * @param port
   * @param overFlowDir
   * @param isDiskStore
   * @return client subscription name
   * @since GemFire 5.7
   */
  public static String clientMessagesRegion(GemFireCacheImpl cache, String ePolicy,
      int capacity, int port, String overFlowDir, boolean isDiskStore) {
    AttributesFactory factory = getAttribFactoryForClientMessagesRegion(cache, 
        ePolicy, capacity, overFlowDir, isDiskStore);
    RegionAttributes attr = factory.create();

    return createClientMessagesRegion(attr, cache, capacity, port);
  }

  public static AttributesFactory getAttribFactoryForClientMessagesRegion(
      GemFireCacheImpl cache,
      String ePolicy, int capacity, String overflowDir, boolean isDiskStore)
      throws InvalidValueException, GemFireIOException {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);

    if (isDiskStore) {
      // overflowDir parameter is actually diskstore name
      factory.setDiskStoreName(overflowDir);
      // client subscription queue is always overflow to disk, so do async
      // see feature request #41479
      factory.setDiskSynchronous(true);
    } else if  (overflowDir == null || overflowDir.equals(ClientSubscriptionConfig.DEFAULT_OVERFLOW_DIRECTORY)) {
      factory.setDiskStoreName(null);
      // client subscription queue is always overflow to disk, so do async
      // see feature request #41479
      factory.setDiskSynchronous(true);
    } else {
      File dir = new File(overflowDir + File.separatorChar
          + generateNameForClientMsgsRegion(OSProcess.getId()));
      // This will delete the overflow directory when virtual machine terminates.
      dir.deleteOnExit();
      if (!dir.mkdirs() && !dir.isDirectory()) {
        throw new GemFireIOException("Could not create client subscription overflow directory: "
            + dir.getAbsolutePath());
      }
      File[] dirs = { dir };
      DiskStoreFactory dsf = cache.createDiskStoreFactory();
      DiskStore bsi = dsf.setAutoCompact(true)
      .setDiskDirsAndSizes(dirs, new int[] { Integer.MAX_VALUE })
      .create("bsi");
      factory.setDiskStoreName("bsi");
      // backward compatibility, it was sync
      factory.setDiskSynchronous(true);
    }
    factory.setDataPolicy(DataPolicy.NORMAL);
    // enable statistics
    factory.setStatisticsEnabled(true);
    /* setting LIFO related eviction attributes */
    if (HARegionQueue.HA_EVICTION_POLICY_ENTRY.equals(ePolicy)) {
      factory
          .setEvictionAttributes(EvictionAttributesImpl.createLIFOEntryAttributes(
              capacity, EvictionAction.OVERFLOW_TO_DISK));
    }
    else if (HARegionQueue.HA_EVICTION_POLICY_MEMORY.equals(ePolicy)) { // condition refinement
      factory
          .setEvictionAttributes(EvictionAttributesImpl.createLIFOMemoryAttributes(
              capacity, EvictionAction.OVERFLOW_TO_DISK));
    }
    else {
      // throw invalid eviction policy exception
      throw new InvalidValueException(
        LocalizedStrings.CacheServerImpl__0_INVALID_EVICTION_POLICY.toLocalizedString(ePolicy));
    }
    return factory;
  }

  public static String createClientMessagesRegion(RegionAttributes attr,
      GemFireCacheImpl cache, int capacity, int port) {
    // generating unique name in VM for ClientMessagesRegion
    String regionName = generateNameForClientMsgsRegion(port);
    try {
      cache.createVMRegion(regionName, attr,
          new InternalRegionArguments().setDestroyLockFlag(true)
              .setRecreateFlag(false).setSnapshotInputStream(null)
              .setImageTarget(null).setIsUsedForMetaRegion(true));
    }
    catch (RegionExistsException ree) {
      InternalGemFireError assErr = new InternalGemFireError(
          "unexpected exception");
      assErr.initCause(ree);
      throw assErr;
    }
    catch (IOException e) {
      // only if loading snapshot, not here
      InternalGemFireError assErr = new InternalGemFireError(
          "unexpected exception");
      assErr.initCause(e);
      throw assErr;
    }
    catch (ClassNotFoundException e) {
      // only if loading snapshot, not here
      InternalGemFireError assErr = new InternalGemFireError(
          "unexpected exception");
      assErr.initCause(e);
      throw assErr;
    }
    return regionName;
  }

  public static String createClientMessagesRegionForTesting(GemFireCacheImpl cache,
      String ePolicy, int capacity, int port, int expiryTime, String overFlowDir, boolean isDiskStore) {
    AttributesFactory factory = getAttribFactoryForClientMessagesRegion(cache, 
        ePolicy, capacity, overFlowDir, isDiskStore);
    ExpirationAttributes ea = new ExpirationAttributes(expiryTime,
        ExpirationAction.LOCAL_INVALIDATE);
    factory.setEntryTimeToLive(ea);
    RegionAttributes attr = factory.create();

    return createClientMessagesRegion(attr, cache, capacity, port);
  }

  /**
   * Generates the name for the client subscription using the given id.
   * 
   * @param id
   * @return String
   * @since GemFire 5.7
   */
  public static String generateNameForClientMsgsRegion(int id) {
    return ClientSubscriptionConfigImpl.CLIENT_SUBSCRIPTION + "_" + id;
  }

  /*
   * Marker class name to identify the lock more easily in thread dumps private
   * static class ClientMessagesRegionLock extends Object { }
   */
  public DistributionAdvisor getDistributionAdvisor() {
    return this.advisor;
  }
  
  /**
   * Returns the BridgeServerAdvisor for this server
   */
  public CacheServerAdvisor getCacheServerAdvisor() {
    return this.advisor;
  }
  
  public Profile getProfile() {
    return getDistributionAdvisor().createProfile();
  }
  
  public DistributionAdvisee getParentAdvisee() {
    return null;
  }
  
  /**
   * Returns the underlying <code>InternalDistributedSystem</code> connection.
   * @return the underlying <code>InternalDistributedSystem</code>
   */
  public InternalDistributedSystem getSystem() {
    return (InternalDistributedSystem)this.cache.getDistributedSystem();
  }
  
  public String getName() {
    return "CacheServer";
  }
  
  public String getFullPath() {
    return getName();
  }

  private final static AtomicInteger profileSN = new AtomicInteger();
  
  private static int createSerialNumber() {
    return profileSN.incrementAndGet();
  }

  /**
   * Returns an array of all the groups of this bridge server.
   * This includes those from the groups gemfire property
   * and those explicitly added to this server.
   */
  public String[] getCombinedGroups() {
    ArrayList<String> groupList = new ArrayList<String>();
    for (String g: MemberAttributes.parseGroups(null, getSystem().getConfig().getGroups())) {
      if (!groupList.contains(g)) {
        groupList.add(g);
      }
    }
    for (String g: getGroups()) {
      if (!groupList.contains(g)) {
        groupList.add(g);
      }
    }
    String[] groups = new String[groupList.size()];
    return groupList.toArray(groups);
  }
  
  public /*synchronized causes deadlock*/ void fillInProfile(Profile profile) {
    assert profile instanceof CacheServerProfile;
    CacheServerProfile bp = (CacheServerProfile)profile;
    bp.setHost(getExternalAddress(false));
    bp.setPort(getPort());
    bp.setGroups(getCombinedGroups());
    bp.setMaxConnections(maxConnections);
    bp.setInitialLoad(loadMonitor.getLastLoad());
    bp.setLoadPollInterval(getLoadPollInterval());
    bp.serialNumber = getSerialNumber();
    bp.finishInit();
  }

  public int getSerialNumber() {
    return this.serialNumber;
  }

  
   protected CacheClientNotifier getCacheClientNotifier() {
    return getAcceptor().getCacheClientNotifier();
  } 
   
  /**
   * Registers a new <code>InterestRegistrationListener</code> with the set of
   * <code>InterestRegistrationListener</code>s.
   * 
   * @param listener
   *                The <code>InterestRegistrationListener</code> to register
   * @throws IllegalStateException if the BridgeServer has not been started
   * @since GemFire 5.8Beta
   */
  public void registerInterestRegistrationListener(
      InterestRegistrationListener listener) {
    if (!this.isRunning()) {
      throw new IllegalStateException(LocalizedStrings.CacheServerImpl_MUST_BE_RUNNING.toLocalizedString());
    }
    getCacheClientNotifier().registerInterestRegistrationListener(listener); 
  }

  /**
   * Unregisters an existing <code>InterestRegistrationListener</code> from
   * the set of <code>InterestRegistrationListener</code>s.
   * 
   * @param listener
   *                The <code>InterestRegistrationListener</code> to
   *                unregister
   * 
   * @since GemFire 5.8Beta
   */
  public void unregisterInterestRegistrationListener(
      InterestRegistrationListener listener) {
    getCacheClientNotifier().unregisterInterestRegistrationListener(listener);     
  }

  /**
   * Returns a read-only set of <code>InterestRegistrationListener</code>s
   * registered with this notifier.
   * 
   * @return a read-only set of <code>InterestRegistrationListener</code>s
   *         registered with this notifier
   * 
   * @since GemFire 5.8Beta
   */
  public Set getInterestRegistrationListeners() {
    return getCacheClientNotifier().getInterestRegistrationListeners(); 
  }
}
