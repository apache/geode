/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.GemFireIOException;
import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.InvalidValueException;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.ClientSession;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.DiskStoreFactory;
import com.gemstone.gemfire.cache.DynamicRegionFactory;
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.ExpirationAction;
import com.gemstone.gemfire.cache.ExpirationAttributes;
import com.gemstone.gemfire.cache.InterestRegistrationListener;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionExistsException;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache.server.ClientSubscriptionConfig;
import com.gemstone.gemfire.cache.server.ServerLoadProbe;
import com.gemstone.gemfire.cache.server.internal.LoadMonitor;
import com.gemstone.gemfire.cache.wan.GatewayTransportFilter;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionAdvisee;
import com.gemstone.gemfire.distributed.internal.DistributionAdvisor;
import com.gemstone.gemfire.distributed.internal.DistributionAdvisor.Profile;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.ResourceEvent;
import com.gemstone.gemfire.distributed.internal.ServerLocation;
import com.gemstone.gemfire.distributed.internal.membership.MemberAttributes;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.OSProcess;
import com.gemstone.gemfire.internal.admin.ClientHealthMonitoringRegion;
import com.gemstone.gemfire.internal.cache.CacheServerAdvisor.CacheServerProfile;
import com.gemstone.gemfire.internal.cache.ha.HARegionQueue;
import com.gemstone.gemfire.internal.cache.tier.Acceptor;
import com.gemstone.gemfire.internal.cache.tier.sockets.AcceptorImpl;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientNotifier;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.management.membership.ClientMembership;
import com.gemstone.gemfire.management.membership.ClientMembershipListener;

/**
 * An implementation of the <code>CacheServer</code> interface that delegates
 * most of the heavy lifting to an {@link Acceptor}.
 * 
 * @author David Whitlock
 * @since 4.0
 */
@SuppressWarnings("deprecation")
public class CacheServerImpl
  extends AbstractCacheServer
  implements DistributionAdvisee {

  private static final Logger logger = LogService.getLogger();
  
  private static final int FORCE_LOAD_UPDATE_FREQUENCY= Integer.getInteger("gemfire.BridgeServer.FORCE_LOAD_UPDATE_FREQUENCY", 10).intValue();
  
  /** The acceptor that does the actual serving */
  private volatile AcceptorImpl acceptor;

  /**
   * The advisor used by this cache server.
   * @since 5.7
   */
  private volatile CacheServerAdvisor advisor;

  /**
   * The monitor used to monitor load on this
   * bridge server and distribute load to the locators
   * @since 5.7
   */
  private volatile LoadMonitor loadMonitor;

  /**
   * boolean that represents whether this server is a GatewayReceiver or a simple BridgeServer
   */
  private boolean isGatewayReceiver;
  
  private List<GatewayTransportFilter> gatewayTransportFilters = Collections.EMPTY_LIST;
  
  /**
   * Needed because this guy is an advisee
   * @since 5.7
   */
  private int serialNumber; // changed on each start

  public static final boolean ENABLE_NOTIFY_BY_SUBSCRIPTION_FALSE = 
  Boolean.getBoolean("gemfire.cache-server.enable-notify-by-subscription-false");
  
 
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
//    setTransactionTimeToLive(other.getTransactionTimeToLive());  not implemented in CacheServer for v6.6
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
    boolean isSqlFabricSystem = ((GemFireCacheImpl)this.cache).isSqlfSystem();
    
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
                                     getTransactionTimeToLive(),
                                     this.loadMonitor,
                                     overflowAttributesList, 
                                     isSqlFabricSystem,
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
   * @since 5.7
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
   * @since 5.7
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
   * @since 5.7 
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
   * @since 5.8Beta
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
   * @since 5.8Beta
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
   * @since 5.8Beta
   */
  public Set getInterestRegistrationListeners() {
    return getCacheClientNotifier().getInterestRegistrationListeners(); 
  }
}
