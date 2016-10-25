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
package org.apache.geode.management.internal;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.management.Notification;
import javax.management.ObjectName;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ResourceEvent;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
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
import org.apache.geode.management.internal.beans.ManagementAdapter;
import org.apache.geode.management.membership.MembershipEvent;
import org.apache.geode.management.membership.MembershipListener;

/**
 * This is the concrete implementation of ManagementService
 * which is the gateway to various JMX operations over a GemFire
 * System
 * 
 * @since GemFire 7.0
 */
public final class SystemManagementService extends BaseManagementService {
  private static final Logger logger = LogService.getLogger();

  /**
   * The concrete implementation of DistributedSystem that provides
   * internal-only functionality.
   */

  private InternalDistributedSystem system;

  /**
   * core component for distribution
   */

  private LocalManager localManager;

  /**
   * This is a notification hub to listen all the notifications emitted from all
   * the MBeans in a peer cache./cache server
   */
  private NotificationHub notificationHub;

  /**
   * Local Filter chain for local MBean filters
   */

  private LocalFilterChain localFilterChain;

  /**
   * whether the service is closed or not if cache is closed automatically this
   * service will be closed
   */
  private volatile boolean closed = false;

  /**
   * has the management service has started yet
   */
  private volatile boolean isStarted = false;

  /**
   * Adapter to interact with platform MBean server
   */
  private MBeanJMXAdapter jmxAdapter;


  private Cache cache;

  private FederatingManager federatingManager;

  private final ManagementAgent agent;

  private ManagementResourceRepo repo;
  
  /**
   * This membership listener will listen on membership events after the node
   * has transformed into a Managing node.
   */
  private ManagementMembershipListener listener;
  
  
  /**
   * Proxy aggregator to create aggregate MBeans e.g. DistributedSystem and DistributedRegion
   * GemFire comes with a default aggregator. 
   */
  private List<ProxyListener> proxyListeners;

  private UniversalListenerContainer universalListenerContainer = new UniversalListenerContainer();
  
  public static BaseManagementService newSystemManagementService(Cache cache) {
    return new SystemManagementService(cache).init();
  }

  protected SystemManagementService(Cache cache) {
    this.cache = cache;
    this.system = (InternalDistributedSystem) cache.getDistributedSystem();
    // This is a safe check to ensure Management service does not start for a
    // system which is disconnected.
    // Most likely scenario when this will happen is when a cache is closed and we are at this point.
    if (!system.isConnected()) {
      throw new DistributedSystemDisconnectedException(
          LocalizedStrings.InternalDistributedSystem_THIS_CONNECTION_TO_A_DISTRIBUTED_SYSTEM_HAS_BEEN_DISCONNECTED
              .toLocalizedString());
    }
    this.localFilterChain = new LocalFilterChain();
    this.jmxAdapter = new MBeanJMXAdapter();      
    this.repo = new ManagementResourceRepo();


    this.notificationHub = new NotificationHub(repo);
    if (system.getConfig().getJmxManager()) {
      this.agent = new ManagementAgent(system.getConfig());
    } else {
      this.agent = null;
    }
    ManagementFunction function = new ManagementFunction(notificationHub);
    FunctionService.registerFunction(function);
    this.proxyListeners = new CopyOnWriteArrayList<ProxyListener>();
  }

  /**
   * This method will initialize all the internal components for Management and
   * Monitoring
   * 
   * It will a)start an JMX connectorServer b) create a notification hub
   * c)register the ManagementFunction
   */
  private SystemManagementService init() {

    try {
      this.localManager = new LocalManager(repo, system, this,cache);
      this.localManager.startManager();
      this.listener = new ManagementMembershipListener(this);
      system.getDistributionManager().addMembershipListener(listener);
      isStarted = true;
      return this;
    } catch (CancelException e) {
      // Rethrow all CancelExceptions (fix for defect 46339)
      throw e;      
    } catch (Exception e) {
      // Wrap all other exceptions as ManagementExceptions
      logger.error(e.getMessage(), e);
      throw new ManagementException(e);
    }

  }

  /**
   *For internal Use only
   */
  public LocalManager getLocalManager() {
    return localManager;
  }

  public NotificationHub getNotificationHub() {
    return notificationHub;
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
  
  public boolean isStartedAndOpen() {
    if (!isStarted) {
      return false;
    }
    if (closed) {
      return false;
    }
    if (!system.isConnected()) {
      return false;
    }
    return true;
  }
  
  private void verifyManagementService() {
    if (!isStarted) {
      throw new ManagementException(
          ManagementStrings.Management_Service_MANAGEMENT_SERVICE_NOT_STARTED_YET
              .toLocalizedString());
    }
    if (!system.isConnected()) {
      throw new ManagementException(
          ManagementStrings.Management_Service_NOT_CONNECTED_TO_DISTRIBUTED_SYSTEM
              .toLocalizedString());
    }
    if (closed) {
      throw new ManagementException(
          ManagementStrings.Management_Service_MANAGEMENT_SERVICE_IS_CLOSED
              .toLocalizedString());
    }
  }

  @Override
  public void close() {
    synchronized (instances) {
      if (closed) {
        // its a no op, hence not logging any exception
        return;
      }
      if (logger.isDebugEnabled()) {
        logger.debug("Closing Management Service");
      }
      if(listener != null && system.isConnected()){
        system.getDistributionManager().removeMembershipListener(listener);  
      }
      // Stop the Federating Manager first . It will ensure MBeans are not getting federated.
      // while un-registering 
      if (federatingManager != null && federatingManager.isRunning()) {
        federatingManager.stopManager();
      }
      this.notificationHub.cleanUpListeners();
      jmxAdapter.cleanJMXResource();
      if (localManager.isRunning()) {
        localManager.stopManager();
      }
      if (this.agent != null && this.agent.isRunning()) {
        this.agent.stopAgent();
      }

      getGemFireCacheImpl().getJmxManagerAdvisor().broadcastChange();
      instances.remove(cache);
      localManager  = null;
      closed = true;
    }

  }

  @Override
  public <T> void federate(ObjectName objectName, Class<T> interfaceClass,
      boolean notificationEmitter) {
    verifyManagementService();
    if (!objectName.getDomain().equalsIgnoreCase(
        ManagementConstants.OBJECTNAME__DEFAULTDOMAIN)) {
      throw new ManagementException(
          ManagementStrings.Management_Service_NOT_A_GEMFIRE_DOMAIN_MBEAN
              .toLocalizedString());
    }
   
    if(!jmxAdapter.isRegistered(objectName)){
      throw new ManagementException(
          ManagementStrings.Management_Service_MBEAN_NOT_REGISTERED_IN_GEMFIRE_DOMAIN
              .toLocalizedString());
    }
    if(notificationEmitter && !jmxAdapter.hasNotificationSupport(objectName)){
      throw new ManagementException(
          ManagementStrings.Management_Service_MBEAN_DOES_NOT_HAVE_NOTIFICATION_SUPPORT
              .toLocalizedString());
    }
    
    //All validation Passed. Now create the federation Component
    Object object = jmxAdapter.getMBeanObject(objectName);
    FederationComponent fedComp = new FederationComponent(object, objectName,
        interfaceClass, notificationEmitter);
    if (ManagementAdapter.refreshOnInit.contains(interfaceClass)) {
        fedComp.refreshObjectState(true);// Fixes 46387
    }
    localManager.markForFederation(objectName, fedComp);
    
    if (isManager()) {
      afterCreateProxy(objectName, interfaceClass, object, fedComp);
    }

  }

  @Override
  public CacheServerMXBean getLocalCacheServerMXBean(int serverPort) {
    CacheServerMXBean bean =  jmxAdapter.getClientServiceMXBean(serverPort);
    return bean;
  }

  @Override
  public long getLastUpdateTime(ObjectName objectName) {
    if (!isStartedAndOpen()) {
      return 0;
    }
    if (federatingManager == null) {
      return 0;

    } else if (!federatingManager.isRunning()) {
      return 0;
    }
    if (jmxAdapter.isLocalMBean(objectName)) {
      return 0;
    }
    return federatingManager.getLastUpdateTime(objectName);
  }

  @Override
  public DiskStoreMXBean getLocalDiskStoreMBean(String diskStoreName) {
    DiskStoreMXBean bean =  jmxAdapter.getLocalDiskStoreMXBean(diskStoreName);
    return bean;
  }

  @Override
  public LockServiceMXBean getLocalLockServiceMBean(String lockSreviceName) {
    LockServiceMXBean bean =  jmxAdapter.getLocalLockServiceMXBean(lockSreviceName);
    return bean;
  }

  @Override
  public RegionMXBean getLocalRegionMBean(String regionPath) {
    RegionMXBean bean = jmxAdapter.getLocalRegionMXBean(regionPath);
    return bean;
  }


  public <T> T getMBeanProxy(ObjectName objectName, Class<T> interfaceClass) {
    if (!isStartedAndOpen()) {
      return null;
    }
    if (federatingManager == null) {
      return null;

    } else if (!federatingManager.isRunning()) {
      return null;
    }

    return federatingManager.findProxy(objectName, interfaceClass);
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
    if (cache.getDistributedSystem().getDistributedMember().equals(member)) {
      return jmxAdapter.getLocalGemFireMBean().keySet();
    } else {
      if (federatingManager == null) {
        return Collections.emptySet();

      } else if (!federatingManager.isRunning()) {
        return Collections.emptySet();
      }
      return federatingManager.findAllProxies(member);
    }

  }

  @Override
  public ObjectName registerMBean(Object object, ObjectName objectName) {
    verifyManagementService();
    if (localFilterChain.isFiltered(objectName)) {
      return null;
    }
    return jmxAdapter.registerMBean(object, objectName, false);
  }
  
  public ObjectName registerInternalMBean(Object object, ObjectName objectName) {
    verifyManagementService();
    if (localFilterChain.isFiltered(objectName)) {
      return null;
    }
    return jmxAdapter.registerMBean(object, objectName, true);
  }

  @Override
  public void unregisterMBean(ObjectName objectName) {
    if (!isStartedAndOpen()) {
      return;
    }
    verifyManagementService();
    
    if (isManager()) {
      FederationComponent removedObj = localManager.getFedComponents().get(objectName);
      if (removedObj != null) { // only for MBeans local to Manager , not
                                // proxies
        afterRemoveProxy(objectName, removedObj.getInterfaceClass(), removedObj.getMBeanObject(), removedObj);
      }
    }
    
    jmxAdapter.unregisterMBean(objectName);
    localManager.unMarkForFederation(objectName);
  }

  @Override
  public boolean isManager() {
    return isManagerCreated() && federatingManager.isRunning();
  }
  
  public boolean isManagerCreated() {
    if(!isStartedAndOpen()){
      return false;
    }
    return federatingManager != null;
  }

  @Override
  public void startManager() {
    if (!getGemFireCacheImpl().getSystem().getConfig().getJmxManager()) {
      // fix for 45900
      throw new ManagementException("Could not start the manager because the gemfire property \"jmx-manager\" is false.");
    }
    synchronized (instances) {
      verifyManagementService();
      if (federatingManager != null && federatingManager.isRunning()) {
        throw new AlreadyRunningException(
            ManagementStrings.Management_Service_MANAGER_ALREADY_RUNNING
                .toLocalizedString());
      }

      boolean needsToBeStarted = false;
      if (!isManagerCreated()) {
        createManager();
        needsToBeStarted = true;
      } else if (!federatingManager.isRunning()) {
        needsToBeStarted = true;
      }
      if (needsToBeStarted) {
        boolean started = false;
        try {
          system.handleResourceEvent(ResourceEvent.MANAGER_START, null);
          federatingManager.startManager();
          if (this.agent != null) {
            this.agent.startAgent(getGemFireCacheImpl());
          }
          getGemFireCacheImpl().getJmxManagerAdvisor().broadcastChange();
          started = true;
        } catch (RuntimeException e) {
          logger.error("Jmx manager could not be started because {}", e.getMessage(), e);
          throw e;
        } catch (Error e) {
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
  }
  
  private GemFireCacheImpl getGemFireCacheImpl() {
    return (GemFireCacheImpl)this.cache;
  }

  /**
   * Creates a Manager instance in stopped state.
   * 
   */
  public boolean createManager() {
    synchronized (instances) {
      if (federatingManager != null) {
        return false;
      } 
      system.handleResourceEvent(ResourceEvent.MANAGER_CREATE, null);
      // An initialised copy of federating manager
      federatingManager = new FederatingManager(jmxAdapter, repo, system, this, cache);
      getGemFireCacheImpl().getJmxManagerAdvisor().broadcastChange();
      return true;
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
        getGemFireCacheImpl().getJmxManagerAdvisor().broadcastChange();
        if (this.agent != null && (this.agent.isRunning() || this.agent.isHttpServiceRunning())) {
          this.agent.stopAgent();
        }
      }
    }
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  @Override
  public DistributedLockServiceMXBean getDistributedLockServiceMXBean(
      String lockServiceName) {
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
  
  public void addProxyListener(ProxyListener listener) {
    this.proxyListeners.add(listener);
  }

  public void removeProxyListener(ProxyListener listener) {
    this.proxyListeners.remove(listener);
  }

  public List<ProxyListener> getProxyListeners() {
    return this.proxyListeners;
  }
  
  @Override
  public ManagerMXBean getManagerMXBean() {
    return jmxAdapter.getManagerMXBean();
  }

  @Override
  public ObjectName getCacheServerMBeanName(int serverPort,DistributedMember member) {
    return MBeanJMXAdapter.getClientServiceMBeanName(serverPort,member);
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
  public ObjectName getGatewaySenderMBeanName(DistributedMember member,
      String gatwaySenderId) {
    return MBeanJMXAdapter.getGatewaySenderMBeanName(member, gatwaySenderId);
  }
  
  @Override
  public ObjectName getAsyncEventQueueMBeanName(DistributedMember member, String queueId) {
    return MBeanJMXAdapter.getAsycnEventQueueMBeanName(member, queueId);
  }

  @Override
  public ObjectName getLockServiceMBeanName(DistributedMember member,
      String lockServiceName) {
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

  public boolean afterCreateProxy(ObjectName objectName, Class interfaceClass, Object proxyObject,
      FederationComponent newVal) {
    for (ProxyListener listener : proxyListeners) {
      listener.afterCreateProxy(objectName, interfaceClass, proxyObject, newVal);
    }
    return true;
  }

  
  public boolean afterPseudoCreateProxy(ObjectName objectName, Class interfaceClass, Object proxyObject,
      FederationComponent newVal) {    
    for (ProxyListener listener : proxyListeners) {
      listener.afterPseudoCreateProxy(objectName, interfaceClass, proxyObject, newVal);
    }
    return true;
  }
  
  public boolean afterRemoveProxy(ObjectName objectName, Class interfaceClass, Object proxyObject,
      FederationComponent oldVal) {
    for (ProxyListener listener : proxyListeners) {
      listener.afterRemoveProxy(objectName, interfaceClass, proxyObject, oldVal);
    }
    return true;
  }

  public boolean afterUpdateProxy(ObjectName objectName, Class interfaceClass, Object proxyObject,
      FederationComponent newVal, FederationComponent oldVal) {
    for (ProxyListener listener : proxyListeners) {
      listener.afterUpdateProxy(objectName, interfaceClass, proxyObject, newVal, oldVal);
    }
    return true;
  }

  public void handleNotification(Notification notification) {
    for (ProxyListener listener : proxyListeners) {
      listener.handleNotification(notification);
    }
  }
  
  

  @Override
  public <T> T getMBeanInstance(ObjectName objectName, Class<T> interfaceClass) {
    if (jmxAdapter.isLocalMBean(objectName)) {
      return jmxAdapter.findMBeanByName(objectName, interfaceClass);
    } else {
      return this.getMBeanProxy(objectName, interfaceClass);
    }
  }
  
  public void logFine(String s){
    if (logger.isDebugEnabled()) {
      logger.debug(s);
    }
  }
  

  public void memberJoined(InternalDistributedMember id) {
    for (ProxyListener listener : proxyListeners) {
      listener.memberJoined(id);
    }
  }

  public void memberDeparted(InternalDistributedMember id, boolean crashed) {
    for (ProxyListener listener : proxyListeners) {
      listener.memberDeparted(id, crashed);
    }
  }

  public void memberSuspect(InternalDistributedMember id, InternalDistributedMember whoSuspected, String reason) {
    for (ProxyListener listener : proxyListeners) {
      listener.memberSuspect(id, whoSuspected, reason);
    }
  }
  

  public void quorumLost(Set<InternalDistributedMember> failures, List<InternalDistributedMember> remaining) {
    for (ProxyListener listener : proxyListeners) {
      listener.quorumLost(failures, remaining);
    }
  }
  
  
  
  public class UniversalListenerContainer {

    private List<MembershipListener> membershipListeners = new CopyOnWriteArrayList<MembershipListener>();

    public void memberJoined(InternalDistributedMember id) {
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

    public void memberDeparted(InternalDistributedMember id, boolean crashed) {
      MembershipEvent event = createEvent(id);
      if (!crashed) {
        for (MembershipListener listener : membershipListeners) {
          try {
            listener.memberLeft(event);
          } catch (Exception e) {
            logger.error("Could not invoke listener event memberLeft for listener[{}] due to ",
                listener.getClass(), e.getMessage(), e);
          }
        }
      } else {
        for (MembershipListener listener : membershipListeners) {
          try {
            listener.memberCrashed(event);
          } catch (Exception e) {
            logger.error("Could not invoke listener event memberCrashed for listener[{}] due to ",
                listener.getClass(), e.getMessage(), e);
          }
        }
      }
    }

    private MembershipEvent createEvent(InternalDistributedMember id) {
      final String memberId = id.getId();
      final DistributedMember member = id;
      MembershipEvent event = new MembershipEvent() {

        @Override
        public String getMemberId() {
          return memberId;
        }

        @Override
        public DistributedMember getDistributedMember() {
          return member;
        }
      };

      return event;
    }

    /**
     * Registers a listener that receives call backs when a member joins or
     * leaves the distributed system.
     */
    public void addMembershipListener(MembershipListener listener) {
      membershipListeners.add(listener);
    }

    /**
     * Unregisters a membership listener
     *
     * @see #addMembershipListener
     */
    public void removeMembershipListener(MembershipListener listener) {
      membershipListeners.remove(listener);
    }
  }

  public UniversalListenerContainer getUniversalListenerContainer() {
    return universalListenerContainer;
  }

  @Override
  public void addMembershipListener(MembershipListener listener) {
    universalListenerContainer.addMembershipListener(listener);
    
  }

  @Override
  public void removeMembershipListener(MembershipListener listener) {
    universalListenerContainer.removeMembershipListener(listener);    
  }
}
