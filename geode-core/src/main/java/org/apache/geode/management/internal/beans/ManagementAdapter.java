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
package org.apache.geode.management.internal.beans;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;
import javax.management.ObjectInstance;
import javax.management.ObjectName;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.locks.DLockService;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.ClassLoadUtil;
import org.apache.geode.internal.cache.CacheService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegionHelper;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.AsyncEventQueueMXBean;
import org.apache.geode.management.CacheServerMXBean;
import org.apache.geode.management.DiskStoreMXBean;
import org.apache.geode.management.GatewayReceiverMXBean;
import org.apache.geode.management.GatewaySenderMXBean;
import org.apache.geode.management.JMXNotificationType;
import org.apache.geode.management.JMXNotificationUserData;
import org.apache.geode.management.LocatorMXBean;
import org.apache.geode.management.LockServiceMXBean;
import org.apache.geode.management.ManagementException;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.ManagerMXBean;
import org.apache.geode.management.MemberMXBean;
import org.apache.geode.management.RegionMXBean;
import org.apache.geode.management.internal.AlertDetails;
import org.apache.geode.management.internal.FederationComponent;
import org.apache.geode.management.internal.MBeanJMXAdapter;
import org.apache.geode.management.internal.ManagementConstants;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.management.membership.ClientMembership;
import org.apache.geode.management.membership.ClientMembershipEvent;
import org.apache.geode.management.membership.ClientMembershipListener;
import org.apache.geode.management.membership.ClientMembershipListenerAdapter;
import org.apache.geode.pdx.internal.PeerTypeRegistration;

/**
 * Acts as an intermediate between MBean layer and Federation Layer. Handles all Call backs from
 * GemFire to instantiate or remove MBeans from GemFire Domain.
 *
 * Even though this class have a lot of utility functions it interacts with the state of the system
 * and contains some state itself.
 */
public class ManagementAdapter {

  private static final Logger logger = LogService.getLogger();

  /** Internal ManagementService Instance **/
  private SystemManagementService service;

  /** GemFire Cache impl **/
  private InternalCache internalCache;

  /** Member Name **/
  private String memberSource;

  /**
   * emitter is a helper class for sending notifications on behalf of the MemberMBean
   **/
  private NotificationBroadcasterSupport memberLevelNotifEmitter;

  /** The <code>MBeanServer</code> for this application */
  public static final MBeanServer mbeanServer = MBeanJMXAdapter.mbeanServer;

  /** MemberMBean instance **/
  private MemberMBean memberBean;

  private volatile boolean serviceInitialised = false;

  private MBeanAggregator aggregator;

  public static final List<Class> refreshOnInit = new ArrayList<>();

  public static final List<String> internalLocks = new ArrayList<>();

  static {
    refreshOnInit.add(RegionMXBean.class);
    refreshOnInit.add(MemberMXBean.class);

    internalLocks.add(DLockService.DTLS); // From reserved lock service name
    internalLocks.add(DLockService.LTLS); // From reserved lock service name
    internalLocks.add(PartitionedRegionHelper.PARTITION_LOCK_SERVICE_NAME);
    internalLocks.add(PeerTypeRegistration.LOCK_SERVICE_NAME);
  }

  protected MemberMBeanBridge memberMBeanBridge;

  private final Object regionOpLock = new Object();

  /**
   * Adapter life cycle is tied with the Cache . So its better to make all cache level artifacts as
   * instance variable
   *
   * @param cache gemfire cache
   */
  protected void handleCacheCreation(InternalCache cache) throws ManagementException {
    try {
      this.internalCache = cache;
      this.service =
          (SystemManagementService) ManagementService.getManagementService(internalCache);

      this.memberMBeanBridge = new MemberMBeanBridge(internalCache, service).init();
      this.memberBean = new MemberMBean(memberMBeanBridge);
      this.memberLevelNotifEmitter = memberBean;

      ObjectName memberMBeanName = MBeanJMXAdapter.getMemberMBeanName(
          InternalDistributedSystem.getConnectedInstance().getDistributedMember());

      memberSource = MBeanJMXAdapter
          .getMemberNameOrId(internalCache.getDistributedSystem().getDistributedMember());

      // Type casting to MemberMXBean to expose only those methods described in
      // the interface;
      ObjectName changedMBeanName = service.registerInternalMBean(memberBean, memberMBeanName);
      service.federate(changedMBeanName, MemberMXBean.class, true);

      this.serviceInitialised = true;

      // Service initialised is only for ManagementService and not necessarily
      // Manager service.

      // For situations where locator is created before any cache is created
      if (InternalLocator.hasLocator()) {
        Locator loc = InternalLocator.getLocator();
        handleLocatorStart(loc);
      }

      if (cache.getInternalDistributedSystem().getConfig().getJmxManager()) {
        this.service.createManager();
        if (cache.getInternalDistributedSystem().getConfig().getJmxManagerStart()) {
          this.service.startManager();
        }
      }

    } finally {
      if (!serviceInitialised && service != null) {
        service.close();
        if (logger.isDebugEnabled()) {
          logger.debug("Management Service Could not initialise hence closing");
        }

      } else {
        if (logger.isDebugEnabled()) {
          logger.debug("Management Service is initialised and Running");
        }

      }
    }
  }

  /**
   * Handles all the distributed mbean creation part when a Manager is started
   */
  protected void handleManagerStart() throws ManagementException {
    if (!isServiceInitialised("handleManagerStart")) {
      return;
    }
    MBeanJMXAdapter jmxAdapter = service.getJMXAdapter();
    Map<ObjectName, Object> registeredMBeans = jmxAdapter.getLocalGemFireMBean();

    DistributedSystemBridge dsBridge = new DistributedSystemBridge(service, internalCache);
    this.aggregator = new MBeanAggregator(dsBridge);
    // register the aggregator for Federation framework to use
    service.addProxyListener(aggregator);

    /*
     * get the local member mbean as it need to be provided to aggregator first
     */

    MemberMXBean localMember = service.getMemberMXBean();
    ObjectName memberObjectName = MBeanJMXAdapter.getMemberMBeanName(
        InternalDistributedSystem.getConnectedInstance().getDistributedMember());

    FederationComponent addedComp =
        service.getLocalManager().getFedComponents().get(memberObjectName);

    service.afterCreateProxy(memberObjectName, MemberMXBean.class, localMember, addedComp);

    for (ObjectName objectName : registeredMBeans.keySet()) {
      if (objectName.equals(memberObjectName)) {
        continue;
      }
      Object object = registeredMBeans.get(objectName);
      ObjectInstance instance;
      try {
        instance = mbeanServer.getObjectInstance(objectName);
        String className = instance.getClassName();
        Class cls = ClassLoadUtil.classFromName(className);
        Type[] intfTyps = cls.getGenericInterfaces();

        FederationComponent newObj = service.getLocalManager().getFedComponents().get(objectName);

        for (Type intfTyp1 : intfTyps) {
          Class intfTyp = (Class) intfTyp1;
          service.afterCreateProxy(objectName, intfTyp, object, newObj);

        }
      } catch (InstanceNotFoundException e) {
        if (logger.isDebugEnabled()) {
          logger.debug("Failed in Registering distributed mbean ");
        }
        throw new ManagementException(e);
      } catch (ClassNotFoundException e) {
        if (logger.isDebugEnabled()) {
          logger.debug("Failed in Registering distributed mbean");
        }
        throw new ManagementException(e);
      }
    }
  }

  /**
   * Handles all the clean up activities when a Manager is stopped It clears the distributed mbeans
   * and underlying data structures
   */
  protected void handleManagerStop() throws ManagementException {
    if (!isServiceInitialised("handleManagerStop")) {
      return;
    }
    MBeanJMXAdapter jmxAdapter = service.getJMXAdapter();
    Map<ObjectName, Object> registeredMBeans = jmxAdapter.getLocalGemFireMBean();

    ObjectName aggregatemMBeanPattern;
    try {
      aggregatemMBeanPattern = new ObjectName(ManagementConstants.AGGREGATE_MBEAN_PATTERN);
    } catch (MalformedObjectNameException | NullPointerException e1) {
      throw new ManagementException(e1);
    }

    MemberMXBean localMember = service.getMemberMXBean();

    ObjectName memberObjectName = MBeanJMXAdapter.getMemberMBeanName(
        InternalDistributedSystem.getConnectedInstance().getDistributedMember());

    FederationComponent removedComp =
        service.getLocalManager().getFedComponents().get(memberObjectName);

    service.afterRemoveProxy(memberObjectName, MemberMXBean.class, localMember, removedComp);

    for (ObjectName objectName : registeredMBeans.keySet()) {
      if (objectName.equals(memberObjectName)) {
        continue;
      }
      if (aggregatemMBeanPattern.apply(objectName)) {
        continue;
      }
      Object object = registeredMBeans.get(objectName);
      ObjectInstance instance;
      try {
        instance = mbeanServer.getObjectInstance(objectName);
        String className = instance.getClassName();
        Class cls = ClassLoadUtil.classFromName(className);
        Type[] intfTyps = cls.getGenericInterfaces();

        FederationComponent oldObj = service.getLocalManager().getFedComponents().get(objectName);

        for (Type intfTyp1 : intfTyps) {
          Class intfTyp = (Class) intfTyp1;
          service.afterRemoveProxy(objectName, intfTyp, object, oldObj);
        }
      } catch (InstanceNotFoundException | ClassNotFoundException e) {
        logger.warn("Failed to invoke aggregator for {} with exception {}", objectName,
            e.getMessage(), e);
      }
    }
    service.removeProxyListener(this.aggregator);
    this.aggregator = null;
  }

  /**
   * Assumption is always cache and MemberMbean has been will be created first
   */
  protected void handleManagerCreation() throws ManagementException {
    if (!isServiceInitialised("handleManagerCreation")) {
      return;
    }

    ObjectName managerMBeanName = MBeanJMXAdapter.getManagerName();

    ManagerMBeanBridge bridge = new ManagerMBeanBridge(service);

    ManagerMXBean bean = new ManagerMBean(bridge);

    service.registerInternalMBean(bean, managerMBeanName);
  }

  /**
   * Handles Region Creation. This is the call back which will create the specified RegionMXBean and
   * will send a notification on behalf of Member Mbean
   *
   * @param region the region for which the call back is invoked
   */
  public <K, V> void handleRegionCreation(Region<K, V> region) throws ManagementException {
    if (!isServiceInitialised("handleRegionCreation")) {
      return;
    }
    // Moving region creation operation inside a guarded block
    // After getting access to regionOpLock it again checks for region
    // destroy status

    synchronized (regionOpLock) {
      LocalRegion localRegion = (LocalRegion) region;
      if (localRegion.isDestroyed()) {
        return;
      }
      // Bridge is responsible for extracting data from GemFire Layer
      RegionMBeanBridge<K, V> bridge = RegionMBeanBridge.getInstance(region);

      RegionMXBean regionMBean = new RegionMBean<>(bridge);
      ObjectName regionMBeanName = MBeanJMXAdapter.getRegionMBeanName(
          internalCache.getDistributedSystem().getDistributedMember(), region.getFullPath());
      ObjectName changedMBeanName = service.registerInternalMBean(regionMBean, regionMBeanName);
      service.federate(changedMBeanName, RegionMXBean.class, true);

      Notification notification = new Notification(JMXNotificationType.REGION_CREATED, memberSource,
          SequenceNumber.next(), System.currentTimeMillis(),
          ManagementConstants.REGION_CREATED_PREFIX + region.getFullPath());
      memberLevelNotifEmitter.sendNotification(notification);
      memberMBeanBridge.addRegion(region);
    }
  }

  /**
   * Handles Disk Creation. Will create DiskStoreMXBean and will send a notification
   *
   * @param disk the disk store for which the call back is invoked
   */
  protected void handleDiskCreation(DiskStore disk) throws ManagementException {
    if (!isServiceInitialised("handleDiskCreation")) {
      return;
    }
    DiskStoreMBeanBridge bridge = new DiskStoreMBeanBridge(disk);
    DiskStoreMXBean diskStoreMBean = new DiskStoreMBean(bridge);
    ObjectName diskStoreMBeanName = MBeanJMXAdapter.getDiskStoreMBeanName(
        internalCache.getDistributedSystem().getDistributedMember(), disk.getName());
    ObjectName changedMBeanName = service.registerInternalMBean(diskStoreMBean, diskStoreMBeanName);

    service.federate(changedMBeanName, DiskStoreMXBean.class, true);

    Notification notification = new Notification(JMXNotificationType.DISK_STORE_CREATED,
        memberSource, SequenceNumber.next(), System.currentTimeMillis(),
        ManagementConstants.DISK_STORE_CREATED_PREFIX + disk.getName());
    memberLevelNotifEmitter.sendNotification(notification);
    memberMBeanBridge.addDiskStore(disk);
  }

  /**
   * Handles LockService Creation
   *
   */
  protected void handleLockServiceCreation(DLockService lockService) throws ManagementException {
    if (!isServiceInitialised("handleLockServiceCreation")) {
      return;
    }
    // Internal Locks Should not be exposed to client for monitoring
    if (internalLocks.contains(lockService.getName())) {
      return;
    }
    LockServiceMBeanBridge bridge = new LockServiceMBeanBridge(lockService);
    LockServiceMXBean lockServiceMBean = new LockServiceMBean(bridge);

    ObjectName lockServiceMBeanName = MBeanJMXAdapter.getLockServiceMBeanName(
        internalCache.getDistributedSystem().getDistributedMember(), lockService.getName());

    ObjectName changedMBeanName =
        service.registerInternalMBean(lockServiceMBean, lockServiceMBeanName);

    service.federate(changedMBeanName, LockServiceMXBean.class, true);

    Notification notification = new Notification(JMXNotificationType.LOCK_SERVICE_CREATED,
        memberSource, SequenceNumber.next(), System.currentTimeMillis(),
        ManagementConstants.LOCK_SERVICE_CREATED_PREFIX + lockService.getName());
    memberLevelNotifEmitter.sendNotification(notification);

    memberMBeanBridge.addLockServiceStats(lockService);
  }

  /**
   * Handles GatewaySender creation
   *
   * @param sender the specific gateway sender
   */
  protected void handleGatewaySenderCreation(GatewaySender sender) throws ManagementException {
    if (!isServiceInitialised("handleGatewaySenderCreation")) {
      return;
    }
    GatewaySenderMBeanBridge bridge = new GatewaySenderMBeanBridge(sender);

    GatewaySenderMXBean senderMBean = new GatewaySenderMBean(bridge);
    ObjectName senderObjectName = MBeanJMXAdapter.getGatewaySenderMBeanName(
        internalCache.getDistributedSystem().getDistributedMember(), sender.getId());

    ObjectName changedMBeanName = service.registerInternalMBean(senderMBean, senderObjectName);

    service.federate(changedMBeanName, GatewaySenderMXBean.class, true);

    Notification notification = new Notification(JMXNotificationType.GATEWAY_SENDER_CREATED,
        memberSource, SequenceNumber.next(), System.currentTimeMillis(),
        ManagementConstants.GATEWAY_SENDER_CREATED_PREFIX);
    memberLevelNotifEmitter.sendNotification(notification);
  }

  /**
   * Handles Gateway receiver creation
   *
   * @param recv specific gateway receiver
   */
  protected void handleGatewayReceiverCreate(GatewayReceiver recv) throws ManagementException {
    if (!isServiceInitialised("handleGatewayReceiverCreate")) {
      return;
    }
    if (!recv.isManualStart()) {
      return;
    }

    createGatewayReceiverMBean(recv);
  }

  private void createGatewayReceiverMBean(GatewayReceiver recv) {
    GatewayReceiverMBeanBridge bridge = new GatewayReceiverMBeanBridge(recv);

    GatewayReceiverMXBean receiverMBean = new GatewayReceiverMBean(bridge);
    ObjectName recvObjectName = MBeanJMXAdapter
        .getGatewayReceiverMBeanName(internalCache.getDistributedSystem().getDistributedMember());

    ObjectName changedMBeanName = service.registerInternalMBean(receiverMBean, recvObjectName);

    service.federate(changedMBeanName, GatewayReceiverMXBean.class, true);

    Notification notification = new Notification(JMXNotificationType.GATEWAY_RECEIVER_CREATED,
        memberSource, SequenceNumber.next(), System.currentTimeMillis(),
        ManagementConstants.GATEWAY_RECEIVER_CREATED_PREFIX);
    memberLevelNotifEmitter.sendNotification(notification);
  }

  /**
   * Handles Gateway receiver destroy
   *
   * @param recv specific gateway receiver
   */
  protected void handleGatewayReceiverDestroy(GatewayReceiver recv) throws ManagementException {
    if (!isServiceInitialised("handleGatewayReceiverDestroy")) {
      return;
    }

    GatewayReceiverMBean mbean = (GatewayReceiverMBean) service.getLocalGatewayReceiverMXBean();
    GatewayReceiverMBeanBridge bridge = mbean.getBridge();

    bridge.destroyServer();
    ObjectName objectName = (MBeanJMXAdapter
        .getGatewayReceiverMBeanName(internalCache.getDistributedSystem().getDistributedMember()));

    service.unregisterMBean(objectName);
    Notification notification = new Notification(JMXNotificationType.GATEWAY_RECEIVER_DESTROYED,
        memberSource, SequenceNumber.next(), System.currentTimeMillis(),
        ManagementConstants.GATEWAY_RECEIVER_DESTROYED_PREFIX);
    memberLevelNotifEmitter.sendNotification(notification);
  }

  /**
   * Handles Gateway receiver creation
   *
   * @param recv specific gateway receiver
   */
  protected void handleGatewayReceiverStart(GatewayReceiver recv) throws ManagementException {
    if (!isServiceInitialised("handleGatewayReceiverStart")) {
      return;
    }

    if (!recv.isManualStart()) {
      createGatewayReceiverMBean(recv);
    }

    GatewayReceiverMBean mbean = (GatewayReceiverMBean) service.getLocalGatewayReceiverMXBean();
    GatewayReceiverMBeanBridge bridge = mbean.getBridge();

    bridge.startServer();

    Notification notification = new Notification(JMXNotificationType.GATEWAY_RECEIVER_STARTED,
        memberSource, SequenceNumber.next(), System.currentTimeMillis(),
        ManagementConstants.GATEWAY_RECEIVER_STARTED_PREFIX);
    memberLevelNotifEmitter.sendNotification(notification);
  }

  /**
   * Handles Gateway receiver creation
   *
   * @param recv specific gateway receiver
   */
  protected void handleGatewayReceiverStop(GatewayReceiver recv) throws ManagementException {
    if (!isServiceInitialised("handleGatewayReceiverStop")) {
      return;
    }
    GatewayReceiverMBean mbean = (GatewayReceiverMBean) service.getLocalGatewayReceiverMXBean();
    GatewayReceiverMBeanBridge bridge = mbean.getBridge();

    bridge.stopServer();

    Notification notification = new Notification(JMXNotificationType.GATEWAY_RECEIVER_STOPPED,
        memberSource, SequenceNumber.next(), System.currentTimeMillis(),
        ManagementConstants.GATEWAY_RECEIVER_STOPPED_PREFIX);
    memberLevelNotifEmitter.sendNotification(notification);
  }

  protected void handleAsyncEventQueueCreation(AsyncEventQueue queue) throws ManagementException {
    if (!isServiceInitialised("handleAsyncEventQueueCreation")) {
      return;
    }
    AsyncEventQueueMBeanBridge bridge = new AsyncEventQueueMBeanBridge(queue);
    AsyncEventQueueMXBean queueMBean = new AsyncEventQueueMBean(bridge);
    ObjectName senderObjectName = MBeanJMXAdapter.getAsyncEventQueueMBeanName(
        internalCache.getDistributedSystem().getDistributedMember(), queue.getId());

    ObjectName changedMBeanName = service.registerInternalMBean(queueMBean, senderObjectName);

    service.federate(changedMBeanName, AsyncEventQueueMXBean.class, true);

    Notification notification = new Notification(JMXNotificationType.ASYNC_EVENT_QUEUE_CREATED,
        memberSource, SequenceNumber.next(), System.currentTimeMillis(),
        ManagementConstants.ASYNC_EVENT_QUEUE_CREATED_PREFIX);
    memberLevelNotifEmitter.sendNotification(notification);
  }

  /**
   * Handles AsyncEventQueue Removal
   *
   * @param queue The AsyncEventQueue being removed
   */
  protected void handleAsyncEventQueueRemoval(AsyncEventQueue queue) throws ManagementException {
    if (!isServiceInitialised("handleAsyncEventQueueRemoval")) {
      return;
    }

    ObjectName asycnEventQueueMBeanName = MBeanJMXAdapter.getAsyncEventQueueMBeanName(
        internalCache.getDistributedSystem().getDistributedMember(), queue.getId());
    AsyncEventQueueMBean bean;
    try {
      bean = (AsyncEventQueueMBean) service.getLocalAsyncEventQueueMXBean(queue.getId());
      if (bean == null) {
        return;
      }
    } catch (ManagementException e) {
      // If no bean found its a NO-OP
      if (logger.isDebugEnabled()) {
        logger.debug(e.getMessage(), e);
      }
      return;
    }

    bean.stopMonitor();

    service.unregisterMBean(asycnEventQueueMBeanName);

    Notification notification = new Notification(JMXNotificationType.ASYNC_EVENT_QUEUE_CLOSED,
        memberSource, SequenceNumber.next(), System.currentTimeMillis(),
        ManagementConstants.ASYNC_EVENT_QUEUE_CLOSED_PREFIX + queue.getId());
    memberLevelNotifEmitter.sendNotification(notification);
  }

  /**
   * Sends the alert with the Object source as member. This notification will get filtered out for
   * particular alert level
   *
   */
  protected void handleSystemNotification(AlertDetails details) {
    if (!isServiceInitialised("handleSystemNotification")) {
      return;
    }
    if (service.isManager()) {
      String systemSource = "DistributedSystem("
          + service.getDistributedSystemMXBean().getDistributedSystemId() + ")";
      Map<String, String> userData = prepareUserData(details);


      Notification notification = new Notification(JMXNotificationType.SYSTEM_ALERT, systemSource,
          SequenceNumber.next(), details.getMsgTime().getTime(), details.getMsg());

      notification.setUserData(userData);
      service.handleNotification(notification);
    }
  }

  private Map<String, String> prepareUserData(AlertDetails details) {
    Map<String, String> userData = new HashMap<>();
    userData.put(JMXNotificationUserData.ALERT_LEVEL,
        AlertDetails.getAlertLevelAsString(details.getAlertLevel()));

    String source = details.getSource();
    userData.put(JMXNotificationUserData.THREAD, source);

    InternalDistributedMember sender = details.getSender();
    String nameOrId = memberSource;
    if (sender != null) {
      nameOrId = sender.getName();
      nameOrId = StringUtils.isNotBlank(nameOrId) ? nameOrId : sender.getId();
    }

    userData.put(JMXNotificationUserData.MEMBER, nameOrId);

    return userData;
  }

  /**
   * Assumption is its a cache server instance. For Gateway receiver there will be a separate method
   *
   * @param cacheServer cache server instance
   */
  protected void handleCacheServerStart(CacheServer cacheServer) {
    if (!isServiceInitialised("handleCacheServerStart")) {
      return;
    }

    CacheServerBridge cacheServerBridge = new CacheServerBridge(internalCache, cacheServer);
    cacheServerBridge.setMemberMBeanBridge(memberMBeanBridge);

    CacheServerMBean cacheServerMBean = new CacheServerMBean(cacheServerBridge);

    ObjectName cacheServerMBeanName = MBeanJMXAdapter.getClientServiceMBeanName(
        cacheServer.getPort(), internalCache.getDistributedSystem().getDistributedMember());

    ObjectName changedMBeanName =
        service.registerInternalMBean(cacheServerMBean, cacheServerMBeanName);

    ClientMembershipListener managementClientListener = new CacheServerMembershipListenerAdapter(
        cacheServerMBean, memberLevelNotifEmitter, changedMBeanName);
    ClientMembership.registerClientMembershipListener(managementClientListener);

    cacheServerBridge.setClientMembershipListener(managementClientListener);

    service.federate(changedMBeanName, CacheServerMXBean.class, true);

    Notification notification = new Notification(JMXNotificationType.CACHE_SERVER_STARTED,
        memberSource, SequenceNumber.next(), System.currentTimeMillis(),
        ManagementConstants.CACHE_SERVER_STARTED_PREFIX);

    memberLevelNotifEmitter.sendNotification(notification);

    memberMBeanBridge.setCacheServer(true);
  }

  /**
   * Assumption is its a cache server instance. For Gateway receiver there will be a separate method
   *
   * @param server cache server instance
   */
  protected void handleCacheServerStop(CacheServer server) {
    if (!isServiceInitialised("handleCacheServerStop")) {
      return;
    }

    CacheServerMBean mbean = (CacheServerMBean) service.getLocalCacheServerMXBean(server.getPort());

    ClientMembershipListener listener = mbean.getBridge().getClientMembershipListener();

    if (listener != null) {
      ClientMembership.unregisterClientMembershipListener(listener);
    }

    mbean.stopMonitor();

    ObjectName cacheServerMBeanName = MBeanJMXAdapter.getClientServiceMBeanName(server.getPort(),
        internalCache.getDistributedSystem().getDistributedMember());
    service.unregisterMBean(cacheServerMBeanName);

    Notification notification = new Notification(JMXNotificationType.CACHE_SERVER_STOPPED,
        memberSource, SequenceNumber.next(), System.currentTimeMillis(),
        ManagementConstants.CACHE_SERVER_STOPPED_PREFIX);

    memberLevelNotifEmitter.sendNotification(notification);

    memberMBeanBridge.setCacheServer(false);
  }

  /**
   * Handles Cache removal. It will automatically remove all MBeans from GemFire Domain
   *
   * @param cache GemFire Cache instance. For now client cache is not supported
   */
  protected void handleCacheRemoval(Cache cache) throws ManagementException {
    if (!isServiceInitialised("handleCacheRemoval")) {
      return;
    }

    this.serviceInitialised = false;
    try {
      cleanUpMonitors();
      cleanBridgeResources();
    } catch (Exception e) {
      if (logger.isDebugEnabled()) {
        logger.debug(e.getMessage(), e);
      }
    }

    try {
      service.close();
    } catch (Exception e) {
      logger.warn(e.getMessage(), e);
    } finally {
      this.internalCache = null;
      this.service = null;
      this.memberMBeanBridge = null;
      this.memberBean = null;
      this.memberLevelNotifEmitter = null;
    }
  }

  private void cleanUpMonitors() {
    MemberMBean bean = (MemberMBean) service.getMemberMXBean();
    if (bean != null) {
      bean.stopMonitor();
    }

    Set<GatewaySender> senders = internalCache.getGatewaySenders();

    if (senders != null && senders.size() > 0) {
      for (GatewaySender sender : senders) {
        GatewaySenderMBean senderMBean =
            (GatewaySenderMBean) service.getLocalGatewaySenderMXBean(sender.getId());
        if (senderMBean != null) {
          senderMBean.stopMonitor();
        }
      }
    }

    GatewayReceiverMBean receiver = (GatewayReceiverMBean) service.getLocalGatewayReceiverMXBean();
    if (receiver != null) {
      receiver.stopMonitor();
    }
  }

  private void cleanBridgeResources() {
    List<CacheServer> servers = internalCache.getCacheServers();

    if (servers != null && servers.size() > 0) {
      for (CacheServer server : servers) {
        CacheServerMBean mbean =
            (CacheServerMBean) service.getLocalCacheServerMXBean(server.getPort());

        if (mbean != null) {
          ClientMembershipListener listener = mbean.getBridge().getClientMembershipListener();

          if (listener != null) {
            ClientMembership.unregisterClientMembershipListener(listener);
          }
        }
      }
    }
  }

  /**
   * Handles particular region destroy or close operation it will remove the corresponding MBean
   *
   */
  protected void handleRegionRemoval(Region region) throws ManagementException {
    if (!isServiceInitialised("handleRegionRemoval")) {
      return;
    }
    /*
     * Moved region remove operation to a guarded block. If a region is getting created it wont
     * allow it to destroy any region.
     */

    synchronized (regionOpLock) {
      ObjectName regionMBeanName = MBeanJMXAdapter.getRegionMBeanName(
          internalCache.getDistributedSystem().getDistributedMember(), region.getFullPath());
      RegionMBean bean;
      try {
        bean = (RegionMBean) service.getLocalRegionMBean(region.getFullPath());
      } catch (ManagementException e) {
        // If no bean found its a NO-OP
        // Mostly for situation like DiskAccessException while creating region
        // which does a compensatory close region
        if (logger.isDebugEnabled()) {
          logger.debug(e.getMessage(), e);
        }
        return;
      }

      if (bean != null) {
        bean.stopMonitor();
      }
      service.unregisterMBean(regionMBeanName);

      Notification notification = new Notification(JMXNotificationType.REGION_CLOSED, memberSource,
          SequenceNumber.next(), System.currentTimeMillis(),
          ManagementConstants.REGION_CLOSED_PREFIX + region.getFullPath());
      memberLevelNotifEmitter.sendNotification(notification);
      memberMBeanBridge.removeRegion(region);
    }
  }

  /**
   * Handles DiskStore Removal
   *
   */
  protected void handleDiskRemoval(DiskStore disk) throws ManagementException {
    if (!isServiceInitialised("handleDiskRemoval")) {
      return;
    }

    ObjectName diskStoreMBeanName = MBeanJMXAdapter.getDiskStoreMBeanName(
        internalCache.getDistributedSystem().getDistributedMember(), disk.getName());

    DiskStoreMBean bean;
    try {
      bean = (DiskStoreMBean) service.getLocalDiskStoreMBean(disk.getName());
      if (bean == null) {
        return;
      }
    } catch (ManagementException e) {
      // If no bean found its a NO-OP
      if (logger.isDebugEnabled()) {
        logger.debug(e.getMessage(), e);
      }
      return;
    }

    bean.stopMonitor();

    service.unregisterMBean(diskStoreMBeanName);

    Notification notification = new Notification(JMXNotificationType.DISK_STORE_CLOSED,
        memberSource, SequenceNumber.next(), System.currentTimeMillis(),
        ManagementConstants.DISK_STORE_CLOSED_PREFIX + disk.getName());
    memberLevelNotifEmitter.sendNotification(notification);
    memberMBeanBridge.removeDiskStore(disk);
  }

  /**
   * Handles Lock Service Removal
   *
   * @param lockService lock service instance
   */
  protected void handleLockServiceRemoval(DLockService lockService) throws ManagementException {
    if (!isServiceInitialised("handleLockServiceRemoval")) {
      return;
    }

    ObjectName lockServiceMBeanName = MBeanJMXAdapter.getLockServiceMBeanName(
        internalCache.getDistributedSystem().getDistributedMember(), lockService.getName());

    LockServiceMXBean bean = service.getLocalLockServiceMBean(lockService.getName());

    service.unregisterMBean(lockServiceMBeanName);

    Notification notification = new Notification(JMXNotificationType.LOCK_SERVICE_CLOSED,
        memberSource, SequenceNumber.next(), System.currentTimeMillis(),
        ManagementConstants.LOCK_SERVICE_CLOSED_PREFIX + lockService.getName());
    memberLevelNotifEmitter.sendNotification(notification);
  }

  /**
   * Handles management side call backs for a locator creation and start. Assumption is a cache will
   * be created before hand.
   *
   * There is no corresponding handleStopLocator() method. Locator will close the cache whenever its
   * stopped and it should also shutdown all the management services by closing the cache.
   *
   * @param locator instance of locator which is getting started
   */
  protected void handleLocatorStart(Locator locator) throws ManagementException {
    if (!isServiceInitialised("handleLocatorCreation")) {
      return;
    }

    ObjectName locatorMBeanName = MBeanJMXAdapter
        .getLocatorMBeanName(internalCache.getDistributedSystem().getDistributedMember());

    LocatorMBeanBridge bridge = new LocatorMBeanBridge(locator);
    LocatorMBean locatorMBean = new LocatorMBean(bridge);

    ObjectName changedMBeanName = service.registerInternalMBean(locatorMBean, locatorMBeanName);

    service.federate(changedMBeanName, LocatorMXBean.class, true);

    Notification notification =
        new Notification(JMXNotificationType.LOCATOR_STARTED, memberSource, SequenceNumber.next(),
            System.currentTimeMillis(), ManagementConstants.LOCATOR_STARTED_PREFIX);

    memberLevelNotifEmitter.sendNotification(notification);

  }

  protected void handleGatewaySenderStart(GatewaySender sender) throws ManagementException {
    if (!isServiceInitialised("handleGatewaySenderStart")) {
      return;
    }
    if ((sender.getRemoteDSId() < 0)) {
      return;
    }
    GatewaySenderMBean bean =
        (GatewaySenderMBean) service.getLocalGatewaySenderMXBean(sender.getId());

    bean.getBridge().setDispatcher();

    Notification notification = new Notification(JMXNotificationType.GATEWAY_SENDER_STARTED,
        memberSource, SequenceNumber.next(), System.currentTimeMillis(),
        ManagementConstants.GATEWAY_SENDER_STARTED_PREFIX + sender.getId());

    memberLevelNotifEmitter.sendNotification(notification);
  }

  protected void handleGatewaySenderStop(GatewaySender sender) throws ManagementException {
    if (!isServiceInitialised("handleGatewaySenderStop")) {
      return;
    }

    Notification notification = new Notification(JMXNotificationType.GATEWAY_SENDER_STOPPED,
        memberSource, SequenceNumber.next(), System.currentTimeMillis(),
        ManagementConstants.GATEWAY_SENDER_STOPPED_PREFIX + sender.getId());

    memberLevelNotifEmitter.sendNotification(notification);
  }

  protected void handleGatewaySenderPaused(GatewaySender sender) throws ManagementException {
    if (!isServiceInitialised("handleGatewaySenderPaused")) {
      return;
    }

    Notification notification = new Notification(JMXNotificationType.GATEWAY_SENDER_PAUSED,
        memberSource, SequenceNumber.next(), System.currentTimeMillis(),
        ManagementConstants.GATEWAY_SENDER_PAUSED_PREFIX + sender.getId());

    memberLevelNotifEmitter.sendNotification(notification);
  }

  protected void handleGatewaySenderResumed(GatewaySender sender) throws ManagementException {
    if (!isServiceInitialised("handleGatewaySenderResumed")) {
      return;
    }

    Notification notification = new Notification(JMXNotificationType.GATEWAY_SENDER_RESUMED,
        memberSource, SequenceNumber.next(), System.currentTimeMillis(),
        ManagementConstants.GATEWAY_SENDER_RESUMED_PREFIX + sender.getId());

    memberLevelNotifEmitter.sendNotification(notification);
  }

  protected void handleGatewaySenderRemoved(GatewaySender sender) throws ManagementException {
    if (!isServiceInitialised("handleGatewaySenderRemoved")) {
      return;
    }
    if ((sender.getRemoteDSId() < 0)) {
      return;
    }

    GatewaySenderMBean bean =
        (GatewaySenderMBean) service.getLocalGatewaySenderMXBean(sender.getId());
    bean.stopMonitor();

    ObjectName gatewaySenderName = MBeanJMXAdapter.getGatewaySenderMBeanName(
        internalCache.getDistributedSystem().getDistributedMember(), sender.getId());
    service.unregisterMBean(gatewaySenderName);

    Notification notification = new Notification(JMXNotificationType.GATEWAY_SENDER_REMOVED,
        memberSource, SequenceNumber.next(), System.currentTimeMillis(),
        ManagementConstants.GATEWAY_SENDER_REMOVED_PREFIX + sender.getId());
    memberLevelNotifEmitter.sendNotification(notification);
  }

  protected void handleCacheServiceCreation(CacheService cacheService) throws ManagementException {
    if (!isServiceInitialised("handleCacheServiceCreation")) {
      return;
    }
    // Don't register the CacheServices in the Locator
    InternalDistributedMember member =
        internalCache.getInternalDistributedSystem().getDistributedMember();
    if (member.getVmKind() == ClusterDistributionManager.LOCATOR_DM_TYPE) {
      return;
    }
    CacheServiceMBeanBase mbean = cacheService.getMBean();
    if (mbean != null) {
      String id = mbean.getId();
      ObjectName cacheServiceObjectName = MBeanJMXAdapter.getCacheServiceMBeanName(member, id);

      ObjectName changedMBeanName = service.registerInternalMBean(mbean, cacheServiceObjectName);

      service.federate(changedMBeanName, mbean.getInterfaceClass(), true);

      Notification notification = new Notification(JMXNotificationType.CACHE_SERVICE_CREATED,
          memberSource, SequenceNumber.next(), System.currentTimeMillis(),
          ManagementConstants.CACHE_SERVICE_CREATED_PREFIX + id);
      memberLevelNotifEmitter.sendNotification(notification);
    }
  }

  /**
   * Private class which acts as a ClientMembershipListener to propagate client joined/left
   * notifications
   */
  private static class CacheServerMembershipListenerAdapter
      extends ClientMembershipListenerAdapter {

    private NotificationBroadcasterSupport serverLevelNotifEmitter;
    private NotificationBroadcasterSupport memberLevelNotifEmitter;

    private String serverSource;

    public CacheServerMembershipListenerAdapter(
        NotificationBroadcasterSupport serverLevelNotifEmitter,
        NotificationBroadcasterSupport memberLevelNotifEmitter, ObjectName serverSource) {
      this.serverLevelNotifEmitter = serverLevelNotifEmitter;
      this.memberLevelNotifEmitter = memberLevelNotifEmitter;
      this.serverSource = serverSource.toString();
    }

    /**
     * Invoked when a client has connected to this process or when this process has connected to a
     * CacheServer.
     */
    public void memberJoined(ClientMembershipEvent event) {
      Notification notification = new Notification(JMXNotificationType.CLIENT_JOINED, serverSource,
          SequenceNumber.next(), System.currentTimeMillis(),
          ManagementConstants.CLIENT_JOINED_PREFIX + event.getMemberId());
      serverLevelNotifEmitter.sendNotification(notification);
      memberLevelNotifEmitter.sendNotification(notification);
    }

    /**
     * Invoked when a client has gracefully disconnected from this process or when this process has
     * gracefully disconnected from a CacheServer.
     */
    public void memberLeft(ClientMembershipEvent event) {
      Notification notification = new Notification(JMXNotificationType.CLIENT_LEFT, serverSource,
          SequenceNumber.next(), System.currentTimeMillis(),
          ManagementConstants.CLIENT_LEFT_PREFIX + event.getMemberId());
      serverLevelNotifEmitter.sendNotification(notification);
      memberLevelNotifEmitter.sendNotification(notification);
    }

    /**
     * Invoked when a client has unexpectedly disconnected from this process or when this process
     * has unexpectedly disconnected from a CacheServer.
     */
    public void memberCrashed(ClientMembershipEvent event) {
      Notification notification = new Notification(JMXNotificationType.CLIENT_CRASHED, serverSource,
          SequenceNumber.next(), System.currentTimeMillis(),
          ManagementConstants.CLIENT_CRASHED_PREFIX + event.getMemberId());
      serverLevelNotifEmitter.sendNotification(notification);
      memberLevelNotifEmitter.sendNotification(notification);
    }

  }

  private boolean isServiceInitialised(String method) {
    if (!serviceInitialised) {
      if (logger.isDebugEnabled()) {
        logger.debug("Management Service is not initialised hence returning from {}", method);
      }
      return false;
    }

    return true;
  }

}
