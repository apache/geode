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
package org.apache.geode.management.internal;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toSet;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.management.Notification;
import javax.management.ObjectName;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ResourceEvent;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalCacheForClientAccess;
import org.apache.geode.internal.serialization.filter.FilterConfiguration;
import org.apache.geode.internal.serialization.filter.Java9SystemPropertyConfigurationFactory;
import org.apache.geode.internal.serialization.filter.OpenMBeanFilterPattern;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.logging.internal.executors.LoggingExecutors;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.AlreadyRunningException;
import org.apache.geode.management.AsyncEventQueueMXBean;
import org.apache.geode.management.CacheServerMXBean;
import org.apache.geode.management.DiskStoreMXBean;
import org.apache.geode.management.DistributedLockServiceMXBean;
import org.apache.geode.management.DistributedRegionMXBean;
import org.apache.geode.management.DistributedSystemMXBean;
import org.apache.geode.management.GatewayReceiverMXBean;
import org.apache.geode.management.GatewaySenderMXBean;
import org.apache.geode.management.LocatorMXBean;
import org.apache.geode.management.LockServiceMXBean;
import org.apache.geode.management.ManagementException;
import org.apache.geode.management.ManagerMXBean;
import org.apache.geode.management.MemberMXBean;
import org.apache.geode.management.RegionMXBean;
import org.apache.geode.management.membership.MembershipEvent;
import org.apache.geode.management.membership.MembershipListener;

/**
 * This is the concrete implementation of ManagementService which is the gateway to various JMX
 * operations over a GemFire System
 *
 * @since GemFire 7.0
 */
public class SystemManagementService extends BaseManagementService {
  private static final Logger logger = LogService.getLogger();

  @Immutable
  @VisibleForTesting
  static final String FEDERATING_MANAGER_FACTORY_PROPERTY = "FEDERATING_MANAGER_FACTORY";

  /**
   * The concrete implementation of DistributedSystem that provides internal-only functionality.
   */
  private final InternalDistributedSystem system;

  /**
   * This is a notification hub to listen all the notifications emitted from all the MBeans in a
   * peer cache./cache server
   */
  private final NotificationHub notificationHub;

  /**
   * Adapter to interact with platform MBean server
   */
  private final MBeanJMXAdapter jmxAdapter;

  private final InternalCacheForClientAccess cache;

  private final ManagementAgent agent;

  private final ManagementResourceRepo repo;

  /**
   * Proxy aggregator to create aggregate MBeans e.g. DistributedSystem and DistributedRegion
   * GemFire comes with a default aggregator.
   */
  private final List<ProxyListener> proxyListeners;

  private final UniversalListenerContainer universalListenerContainer =
      new UniversalListenerContainer();

  private final StatisticsFactory statisticsFactory;
  private final StatisticsClock statisticsClock;
  private final FederatingManagerFactory federatingManagerFactory;
  private final Function<SystemManagementService, LocalManager> localManagerFactory;

  /**
   * whether the service is closed or not if cache is closed automatically this service will be
   * closed
   */
  private volatile boolean closed;

  /**
   * has the management service has started yet
   */
  private volatile boolean isStarted;

  private LocalManager localManager;

  private FederatingManager federatingManager;

  /**
   * This membership listener will listen on membership events after the node has transformed into a
   * Managing node.
   */
  private ManagementMembershipListener listener;

  static BaseManagementService newSystemManagementService(
      InternalCacheForClientAccess cache) {
    return newSystemManagementService(
        cache,
        NotificationHub::new,
        SystemManagementService::createLocalManager,
        createFederatingManagerFactory(),
        ManagementAgent::new);
  }

  @VisibleForTesting
  static BaseManagementService newSystemManagementService(
      InternalCacheForClientAccess cache,
      Function<ManagementResourceRepo, NotificationHub> notificationHubFactory,
      Function<SystemManagementService, LocalManager> localManagerFactory,
      FederatingManagerFactory federatingManagerFactory,
      ManagementAgentFactory managementAgentFactory) {
    return new SystemManagementService(cache, notificationHubFactory, localManagerFactory,
        federatingManagerFactory, managementAgentFactory).init();
  }

  private SystemManagementService(
      InternalCacheForClientAccess cache,
      Function<ManagementResourceRepo, NotificationHub> notificationHubFactory,
      Function<SystemManagementService, LocalManager> localManagerFactory,
      FederatingManagerFactory federatingManagerFactory,
      ManagementAgentFactory managementAgentFactory) {
    this.cache = cache;
    system = cache.getInternalDistributedSystem();
    this.localManagerFactory = localManagerFactory;

    if (!system.isConnected()) {
      throw new DistributedSystemDisconnectedException(
          "This connection to a distributed system has been disconnected.");
    }

    statisticsFactory = system.getStatisticsManager();
    statisticsClock = cache.getStatisticsClock();
    jmxAdapter = new MBeanJMXAdapter(system.getDistributedMember());
    repo = new ManagementResourceRepo();
    notificationHub = notificationHubFactory.apply(repo);

    if (system.getConfig().getJmxManager()) {
      String filterPattern = new OpenMBeanFilterPattern().pattern();

      FilterConfiguration filterConfiguration = new Java9SystemPropertyConfigurationFactory()
          .create("jmx.remote.rmi.server.serial.filter.pattern", filterPattern);

      agent = managementAgentFactory.create(system.getConfig(), cache, filterConfiguration);
    } else {
      agent = null;
    }

    FunctionService.registerFunction(new ManagementFunction(notificationHub));

    proxyListeners = new CopyOnWriteArrayList<>();
    this.federatingManagerFactory = federatingManagerFactory;
  }

  @Override
  public void close() {
    synchronized (instances) {
      if (closed) {
        return;
      }

      if (logger.isDebugEnabled()) {
        logger.debug("Closing Management Service");
      }
      if (listener != null && system.isConnected()) {
        system.getDistributionManager().removeMembershipListener(listener);
      }
      // Stop the Federating Manager first to avoid federating while un-registering
      if (federatingManager != null && federatingManager.isRunning()) {
        federatingManager.stopManager();
      }
      notificationHub.cleanUpListeners();
      jmxAdapter.cleanJMXResource();
      if (localManager.isRunning()) {
        localManager.stopManager();
      }
      if (agent != null && agent.isRunning()) {
        agent.stopAgent();
      }

      cache.getJmxManagerAdvisor().broadcastChange();
      instances.remove(cache);
      localManager = null;
      closed = true;
    }
  }

  @Override
  public <T> void federate(ObjectName objectName, Class<T> interfaceClass,
      boolean notificationEmitter) {
    verifyManagementService();

    if (!objectName.getDomain().equalsIgnoreCase(ManagementConstants.OBJECTNAME__DEFAULTDOMAIN)) {
      throw new ManagementException("Not A GemFire Domain MBean, can not Federate");
    }
    if (!jmxAdapter.isRegistered(objectName)) {
      throw new ManagementException("MBean Not Registered In GemFire Domain");
    }
    if (notificationEmitter && !jmxAdapter.hasNotificationSupport(objectName)) {
      throw new ManagementException("MBean Does Not Have Notification Support");
    }

    // All validation Passed. Now create the federation Component
    Object object = jmxAdapter.getMBeanObject(objectName);
    FederationComponent federationComponent =
        new FederationComponent(object, objectName, interfaceClass, notificationEmitter);
    if (asList(RegionMXBean.class, MemberMXBean.class).contains(interfaceClass)) {
      federationComponent.refreshObjectState(true);
    }
    localManager.markForFederation(objectName, federationComponent);

    if (isManager()) {
      afterCreateProxy(objectName, interfaceClass, object, federationComponent);
    }
  }

  @Override
  public CacheServerMXBean getLocalCacheServerMXBean(int serverPort) {
    return jmxAdapter.getClientServiceMXBean(serverPort);
  }

  @Override
  public long getLastUpdateTime(ObjectName objectName) {
    if (!isStartedAndOpen()) {
      return 0;
    }
    if (federatingManager == null) {
      return 0;
    }
    if (!federatingManager.isRunning()) {
      return 0;
    }
    if (jmxAdapter.isLocalMBean(objectName)) {
      return 0;
    }
    return federatingManager.getLastUpdateTime(objectName);
  }

  @Override
  public DiskStoreMXBean getLocalDiskStoreMBean(String diskStoreName) {
    return jmxAdapter.getLocalDiskStoreMXBean(diskStoreName);
  }

  @Override
  public LockServiceMXBean getLocalLockServiceMBean(String lockServiceName) {
    return jmxAdapter.getLocalLockServiceMXBean(lockServiceName);
  }

  @Override
  public RegionMXBean getLocalRegionMBean(String regionPath) {
    return jmxAdapter.getLocalRegionMXBean(regionPath);
  }

  @Override
  public MemberMXBean getMemberMXBean() {
    return jmxAdapter.getMemberMXBean();
  }

  @Override
  public Set<ObjectName> queryMBeanNames(DistributedMember member) {
    if (!isStartedAndOpen()) {
      return Collections.emptySet();
    }
    if (system.getDistributedMember().equals(member)) {
      return jmxAdapter.getLocalGemFireMBean().keySet();
    }
    if (federatingManager == null) {
      return Collections.emptySet();
    }
    if (!federatingManager.isRunning()) {
      return Collections.emptySet();
    }
    return federatingManager.findAllProxies(member);
  }

  @Override
  public Set<ObjectName> getAsyncEventQueueMBeanNames(DistributedMember member) {
    return queryMBeanNames(member).stream()
        .filter(x -> "AsyncEventQueue".equals(x.getKeyProperty("service")))
        .collect(toSet());
  }

  @Override
  public ObjectName registerMBean(Object object, ObjectName objectName) {
    verifyManagementService();
    return jmxAdapter.registerMBean(object, objectName, false);
  }

  @Override
  public void unregisterMBean(ObjectName objectName) {
    if (!isStartedAndOpen()) {
      return;
    }

    verifyManagementService();

    if (isManager()) {
      FederationComponent removed = localManager.getFedComponents().get(objectName);
      if (removed != null) {
        // only for MBeans local to Manager, not proxies
        afterRemoveProxy(objectName, removed.getInterfaceClass(), removed.getMBeanObject(),
            removed);
      }
    }

    jmxAdapter.unregisterMBean(objectName);
    localManager.unMarkForFederation(objectName);
  }

  @Override
  public boolean isManager() {
    return isManagerCreated() && federatingManager.isRunning();
  }

  @Override
  public void startManager() {
    if (!cache.getInternalDistributedSystem().getConfig().getJmxManager()) {
      throw new ManagementException(
          "Could not start the manager because the gemfire property \"jmx-manager\" is false.");
    }

    synchronized (instances) {
      verifyManagementService();

      if (federatingManager != null && federatingManager.isRunning()) {
        throw new AlreadyRunningException(
            "Manager is already running");
      }

      if (!isManagerCreated()) {
        createManager();
      }

      boolean started = false;
      try {
        system.handleResourceEvent(ResourceEvent.MANAGER_START, null);
        federatingManager.startManager();
        if (agent != null) {
          agent.startAgent();
        }
        cache.getJmxManagerAdvisor().broadcastChange();
        started = true;
      } catch (RuntimeException | Error e) {
        logger.error("Jmx manager could not be started because {}", e.getMessage(), e);
        throw e;
      } finally {
        if (!started) {
          if (federatingManager != null) {
            federatingManager.stopManager();
          }
          system.handleResourceEvent(ResourceEvent.MANAGER_STOP, null);
        }
      }
    }
  }

  /**
   * It will stop the federating Manager and restart the Local cache operation
   */
  @Override
  public void stopManager() {
    synchronized (instances) {
      verifyManagementService();
      if (federatingManager != null) {
        federatingManager.stopManager();
        system.handleResourceEvent(ResourceEvent.MANAGER_STOP, null);
        cache.getJmxManagerAdvisor().broadcastChange();
        if (agent != null && agent.isRunning()) {
          agent.stopAgent();
        }
      }
    }
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  @Override
  public DistributedLockServiceMXBean getDistributedLockServiceMXBean(String lockServiceName) {
    return jmxAdapter.getDistributedLockServiceMXBean(lockServiceName);
  }

  @Override
  public DistributedRegionMXBean getDistributedRegionMXBean(String regionName) {
    return jmxAdapter.getDistributedRegionMXBean(regionName);
  }

  @Override
  public DistributedSystemMXBean getDistributedSystemMXBean() {
    return jmxAdapter.getDistributedSystemMXBean();
  }

  @Override
  public ManagerMXBean getManagerMXBean() {
    return jmxAdapter.getManagerMXBean();
  }

  @Override
  public ObjectName getCacheServerMBeanName(int serverPort, DistributedMember member) {
    return MBeanJMXAdapter.getClientServiceMBeanName(serverPort, member);
  }

  @Override
  public ObjectName getDiskStoreMBeanName(DistributedMember member, String diskName) {
    return MBeanJMXAdapter.getDiskStoreMBeanName(member, diskName);
  }

  @Override
  public ObjectName getDistributedLockServiceMBeanName(String lockService) {
    return MBeanJMXAdapter.getDistributedLockServiceName(lockService);
  }

  @Override
  public ObjectName getDistributedRegionMBeanName(String regionPath) {
    return MBeanJMXAdapter.getDistributedRegionMbeanName(regionPath);
  }

  @Override
  public ObjectName getDistributedSystemMBeanName() {
    return MBeanJMXAdapter.getDistributedSystemName();
  }

  @Override
  public ObjectName getGatewayReceiverMBeanName(DistributedMember member) {
    return MBeanJMXAdapter.getGatewayReceiverMBeanName(member);
  }

  @Override
  public ObjectName getGatewaySenderMBeanName(DistributedMember member, String gatewaySenderId) {
    return MBeanJMXAdapter.getGatewaySenderMBeanName(member, gatewaySenderId);
  }

  @Override
  public ObjectName getAsyncEventQueueMBeanName(DistributedMember member, String queueId) {
    return MBeanJMXAdapter.getAsyncEventQueueMBeanName(member, queueId);
  }

  @Override
  public ObjectName getLockServiceMBeanName(DistributedMember member, String lockServiceName) {
    return MBeanJMXAdapter.getLockServiceMBeanName(member, lockServiceName);
  }

  @Override
  public ObjectName getManagerMBeanName() {
    return MBeanJMXAdapter.getManagerName();
  }

  @Override
  public ObjectName getMemberMBeanName(DistributedMember member) {
    return MBeanJMXAdapter.getMemberMBeanName(member);
  }

  @Override
  public ObjectName getRegionMBeanName(DistributedMember member, String regionPath) {
    return MBeanJMXAdapter.getRegionMBeanName(member, regionPath);
  }

  @Override
  public GatewayReceiverMXBean getLocalGatewayReceiverMXBean() {
    return jmxAdapter.getGatewayReceiverMXBean();
  }

  @Override
  public GatewaySenderMXBean getLocalGatewaySenderMXBean(String senderId) {
    return jmxAdapter.getGatewaySenderMXBean(senderId);
  }

  @Override
  public AsyncEventQueueMXBean getLocalAsyncEventQueueMXBean(String queueId) {
    return jmxAdapter.getAsyncEventQueueMXBean(queueId);
  }

  @Override
  public ObjectName getLocatorMBeanName(DistributedMember member) {
    return MBeanJMXAdapter.getLocatorMBeanName(member);
  }

  @Override
  public LocatorMXBean getLocalLocatorMXBean() {
    return jmxAdapter.getLocatorMXBean();
  }

  @Override
  public <T> T getMBeanInstance(ObjectName objectName, Class<T> interfaceClass) {
    if (jmxAdapter.isLocalMBean(objectName)) {
      return jmxAdapter.findMBeanByName(objectName, interfaceClass);
    }
    return getMBeanProxy(objectName, interfaceClass);
  }

  @Override
  public void addMembershipListener(MembershipListener listener) {
    universalListenerContainer.addMembershipListener(listener);
  }

  @Override
  public void removeMembershipListener(MembershipListener listener) {
    universalListenerContainer.removeMembershipListener(listener);
  }

  public LocalManager getLocalManager() {
    return localManager;
  }

  public FederatingManager getFederatingManager() {
    return federatingManager;
  }

  public MBeanJMXAdapter getJMXAdapter() {
    return jmxAdapter;
  }

  public ManagementAgent getManagementAgent() {
    return agent;
  }

  public <T> T getMBeanProxy(ObjectName objectName, Class<T> interfaceClass) {
    if (!isStartedAndOpen()) {
      return null;
    }
    if (federatingManager == null) {
      return null;
    }
    if (!federatingManager.isRunning()) {
      return null;
    }
    return federatingManager.findProxy(objectName, interfaceClass);
  }

  public ObjectName registerInternalMBean(Object object, ObjectName objectName) {
    verifyManagementService();
    return jmxAdapter.registerMBean(object, objectName, true);
  }

  public boolean isManagerCreated() {
    return isStartedAndOpen() && federatingManager != null;
  }

  /**
   * Creates a Manager instance in stopped state.
   */
  public boolean createManager() {
    synchronized (instances) {
      if (federatingManager != null) {
        return false;
      }
      system.handleResourceEvent(ResourceEvent.MANAGER_CREATE, null);
      // An initialised copy of federating manager
      federatingManager = federatingManagerFactory.create(repo, system, this, cache,
          statisticsFactory, statisticsClock, new MBeanProxyFactory(jmxAdapter, this),
          new MemberMessenger(jmxAdapter, system),
          () -> LoggingExecutors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(),
              "FederatingManager", true));
      cache.getJmxManagerAdvisor().broadcastChange();
      return true;
    }
  }

  public void addProxyListener(ProxyListener listener) {
    proxyListeners.add(listener);
  }

  public void removeProxyListener(ProxyListener listener) {
    proxyListeners.remove(listener);
  }

  public void afterCreateProxy(ObjectName objectName, Class interfaceClass, Object proxyObject,
      FederationComponent newVal) {
    for (ProxyListener listener : proxyListeners) {
      listener.afterCreateProxy(objectName, interfaceClass, proxyObject, newVal);
    }
  }

  public void afterRemoveProxy(ObjectName objectName, Class interfaceClass, Object proxyObject,
      FederationComponent oldVal) {
    for (ProxyListener listener : proxyListeners) {
      listener.afterRemoveProxy(objectName, interfaceClass, proxyObject, oldVal);
    }
  }

  public void handleNotification(Notification notification) {
    for (ProxyListener listener : proxyListeners) {
      listener.handleNotification(notification);
    }
  }

  void memberJoined(InternalDistributedMember id) {
    for (ProxyListener listener : proxyListeners) {
      listener.memberJoined(system.getDistributionManager(), id);
    }
  }

  void memberDeparted(InternalDistributedMember id, boolean crashed) {
    for (ProxyListener listener : proxyListeners) {
      listener.memberDeparted(system.getDistributionManager(), id, crashed);
    }
  }

  void memberSuspect(InternalDistributedMember id, InternalDistributedMember whoSuspected,
      String reason) {
    for (ProxyListener listener : proxyListeners) {
      listener.memberSuspect(system.getDistributionManager(), id, whoSuspected, reason);
    }
  }

  void afterPseudoCreateProxy(ObjectName objectName, Class interfaceClass, Object proxyObject,
      FederationComponent newVal) {
    for (ProxyListener listener : proxyListeners) {
      listener.afterPseudoCreateProxy(objectName, interfaceClass, proxyObject, newVal);
    }
  }

  boolean isStartedAndOpen() {
    return isStarted && !closed && system.isConnected();
  }

  void afterUpdateProxy(ObjectName objectName, Class interfaceClass, Object proxyObject,
      FederationComponent newVal, FederationComponent oldVal) {
    for (ProxyListener listener : proxyListeners) {
      listener.afterUpdateProxy(objectName, interfaceClass, proxyObject, newVal, oldVal);
    }
  }

  UniversalListenerContainer getUniversalListenerContainer() {
    return universalListenerContainer;
  }

  private void verifyManagementService() {
    if (!isStarted) {
      throw new ManagementException(
          "Management Service Not Started Yet");
    }
    if (!system.isConnected()) {
      throw new ManagementException(
          "Not Connected To Distributed System");
    }
    if (closed) {
      throw new ManagementException(
          "Management Service Is Closed");
    }
  }

  /**
   * This method will initialize all the internal components for Management and Monitoring
   *
   * It will: <br>
   * a) start an JMX connectorServer <br>
   * b) create a notification hub <br>
   * c) register the ManagementFunction
   */
  private SystemManagementService init() {
    try {
      localManager = localManagerFactory.apply(this);
      listener = new ManagementMembershipListener(this);

      localManager.startManager();
      system.getDistributionManager().addMembershipListener(listener);
      isStarted = true;
      return this;
    } catch (CancelException e) {
      // Rethrow all CancelExceptions
      throw e;
    } catch (Exception e) {
      // Wrap all other exceptions as ManagementExceptions
      logger.error(e.getMessage(), e);
      throw new ManagementException(e);
    }
  }

  private static LocalManager createLocalManager(SystemManagementService service) {
    return service.newLocalManager();
  }

  private LocalManager newLocalManager() {
    return new LocalManager(repo, system, this, cache, statisticsFactory, statisticsClock);
  }

  private static FederatingManagerFactory createFederatingManagerFactory() {
    try {
      String federatingManagerFactoryName =
          System.getProperty(FEDERATING_MANAGER_FACTORY_PROPERTY,
              FederatingManagerFactoryImpl.class.getName());
      Class<? extends FederatingManagerFactory> federatingManagerFactoryClass =
          Class.forName(federatingManagerFactoryName)
              .asSubclass(FederatingManagerFactory.class);
      Constructor<? extends FederatingManagerFactory> constructor =
          federatingManagerFactoryClass.getConstructor();
      return constructor.newInstance();
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException
        | NoSuchMethodException | InvocationTargetException e) {
      return new FederatingManagerFactoryImpl();
    }
  }

  @VisibleForTesting
  public NotificationHub getNotificationHub() {
    return notificationHub;
  }

  private static class FederatingManagerFactoryImpl implements FederatingManagerFactory {

    public FederatingManagerFactoryImpl() {
      // must be public for instantiation by reflection
    }

    @Override
    public FederatingManager create(ManagementResourceRepo repo, InternalDistributedSystem system,
        SystemManagementService service, InternalCache cache, StatisticsFactory statisticsFactory,
        StatisticsClock statisticsClock, MBeanProxyFactory proxyFactory, MemberMessenger messenger,
        Supplier<ExecutorService> executorServiceSupplier) {
      return new FederatingManager(repo, system, service, cache, statisticsFactory,
          statisticsClock, proxyFactory, messenger, executorServiceSupplier);
    }
  }

  static class UniversalListenerContainer {

    private final Collection<MembershipListener> membershipListeners = new CopyOnWriteArrayList<>();

    void memberJoined(InternalDistributedMember id) {
      MembershipEvent event = createEvent(id);
      for (MembershipListener listener : membershipListeners) {
        try {
          listener.memberJoined(event);
        } catch (Exception e) {
          logger.error("Could not invoke listener event memberJoined for listener[{}] due to ",
              listener.getClass(), e.getMessage(), e);
        }
      }
    }

    void memberDeparted(InternalDistributedMember id, boolean crashed) {
      MembershipEvent event = createEvent(id);
      if (crashed) {
        for (MembershipListener listener : membershipListeners) {
          try {
            listener.memberCrashed(event);
          } catch (Exception e) {
            logger.error("Could not invoke listener event memberCrashed for listener[{}] due to ",
                listener.getClass(), e.getMessage(), e);
          }
        }
      } else {
        for (MembershipListener listener : membershipListeners) {
          try {
            listener.memberLeft(event);
          } catch (Exception e) {
            logger.error("Could not invoke listener event memberLeft for listener[{}] due to ",
                listener.getClass(), e.getMessage(), e);
          }
        }
      }
    }

    /**
     * Registers a listener that receives call backs when a member joins or leaves the distributed
     * system.
     */
    private void addMembershipListener(MembershipListener listener) {
      membershipListeners.add(listener);
    }

    /**
     * Unregisters a membership listener
     *
     * @see #addMembershipListener
     */
    private void removeMembershipListener(MembershipListener listener) {
      membershipListeners.remove(listener);
    }

    private MembershipEvent createEvent(DistributedMember id) {
      return new MembershipEvent() {

        @Override
        public String getMemberId() {
          return id.getId();
        }

        @Override
        public DistributedMember getDistributedMember() {
          return id;
        }
      };
    }
  }
}
