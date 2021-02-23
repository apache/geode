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

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.geode.management.JMXNotificationType.ASYNC_EVENT_QUEUE_CLOSED;
import static org.apache.geode.management.JMXNotificationType.ASYNC_EVENT_QUEUE_CREATED;
import static org.apache.geode.management.JMXNotificationType.CACHE_SERVER_STARTED;
import static org.apache.geode.management.JMXNotificationType.CACHE_SERVER_STOPPED;
import static org.apache.geode.management.JMXNotificationType.CACHE_SERVICE_CREATED;
import static org.apache.geode.management.JMXNotificationType.CLIENT_CRASHED;
import static org.apache.geode.management.JMXNotificationType.CLIENT_JOINED;
import static org.apache.geode.management.JMXNotificationType.CLIENT_LEFT;
import static org.apache.geode.management.JMXNotificationType.DISK_STORE_CLOSED;
import static org.apache.geode.management.JMXNotificationType.DISK_STORE_CREATED;
import static org.apache.geode.management.JMXNotificationType.GATEWAY_RECEIVER_CREATED;
import static org.apache.geode.management.JMXNotificationType.GATEWAY_RECEIVER_DESTROYED;
import static org.apache.geode.management.JMXNotificationType.GATEWAY_RECEIVER_STARTED;
import static org.apache.geode.management.JMXNotificationType.GATEWAY_RECEIVER_STOPPED;
import static org.apache.geode.management.JMXNotificationType.GATEWAY_SENDER_CREATED;
import static org.apache.geode.management.JMXNotificationType.GATEWAY_SENDER_PAUSED;
import static org.apache.geode.management.JMXNotificationType.GATEWAY_SENDER_REMOVED;
import static org.apache.geode.management.JMXNotificationType.GATEWAY_SENDER_RESUMED;
import static org.apache.geode.management.JMXNotificationType.GATEWAY_SENDER_STARTED;
import static org.apache.geode.management.JMXNotificationType.GATEWAY_SENDER_STOPPED;
import static org.apache.geode.management.JMXNotificationType.LOCATOR_STARTED;
import static org.apache.geode.management.JMXNotificationType.LOCK_SERVICE_CLOSED;
import static org.apache.geode.management.JMXNotificationType.LOCK_SERVICE_CREATED;
import static org.apache.geode.management.JMXNotificationType.MANAGER_STARTED;
import static org.apache.geode.management.JMXNotificationType.REGION_CLOSED;
import static org.apache.geode.management.JMXNotificationType.REGION_CREATED;
import static org.apache.geode.management.JMXNotificationType.SYSTEM_ALERT;
import static org.apache.geode.management.JMXNotificationUserData.ALERT_LEVEL;
import static org.apache.geode.management.JMXNotificationUserData.MEMBER;
import static org.apache.geode.management.JMXNotificationUserData.THREAD;
import static org.apache.geode.management.internal.AlertDetails.getAlertLevelAsString;
import static org.apache.geode.management.internal.ManagementConstants.AGGREGATE_MBEAN_PATTERN;
import static org.apache.geode.management.internal.ManagementConstants.ASYNC_EVENT_QUEUE_CLOSED_PREFIX;
import static org.apache.geode.management.internal.ManagementConstants.ASYNC_EVENT_QUEUE_CREATED_PREFIX;
import static org.apache.geode.management.internal.ManagementConstants.CACHE_SERVER_STARTED_PREFIX;
import static org.apache.geode.management.internal.ManagementConstants.CACHE_SERVER_STOPPED_PREFIX;
import static org.apache.geode.management.internal.ManagementConstants.CACHE_SERVICE_CREATED_PREFIX;
import static org.apache.geode.management.internal.ManagementConstants.CLIENT_CRASHED_PREFIX;
import static org.apache.geode.management.internal.ManagementConstants.CLIENT_JOINED_PREFIX;
import static org.apache.geode.management.internal.ManagementConstants.CLIENT_LEFT_PREFIX;
import static org.apache.geode.management.internal.ManagementConstants.DISK_STORE_CLOSED_PREFIX;
import static org.apache.geode.management.internal.ManagementConstants.DISK_STORE_CREATED_PREFIX;
import static org.apache.geode.management.internal.ManagementConstants.GATEWAY_RECEIVER_CREATED_PREFIX;
import static org.apache.geode.management.internal.ManagementConstants.GATEWAY_RECEIVER_DESTROYED_PREFIX;
import static org.apache.geode.management.internal.ManagementConstants.GATEWAY_RECEIVER_STARTED_PREFIX;
import static org.apache.geode.management.internal.ManagementConstants.GATEWAY_RECEIVER_STOPPED_PREFIX;
import static org.apache.geode.management.internal.ManagementConstants.GATEWAY_SENDER_CREATED_PREFIX;
import static org.apache.geode.management.internal.ManagementConstants.GATEWAY_SENDER_PAUSED_PREFIX;
import static org.apache.geode.management.internal.ManagementConstants.GATEWAY_SENDER_REMOVED_PREFIX;
import static org.apache.geode.management.internal.ManagementConstants.GATEWAY_SENDER_RESUMED_PREFIX;
import static org.apache.geode.management.internal.ManagementConstants.GATEWAY_SENDER_STARTED_PREFIX;
import static org.apache.geode.management.internal.ManagementConstants.GATEWAY_SENDER_STOPPED_PREFIX;
import static org.apache.geode.management.internal.ManagementConstants.LOCATOR_STARTED_PREFIX;
import static org.apache.geode.management.internal.ManagementConstants.LOCK_SERVICE_CLOSED_PREFIX;
import static org.apache.geode.management.internal.ManagementConstants.LOCK_SERVICE_CREATED_PREFIX;
import static org.apache.geode.management.internal.ManagementConstants.MANAGER_STARTED_PREFIX;
import static org.apache.geode.management.internal.ManagementConstants.REGION_CLOSED_PREFIX;
import static org.apache.geode.management.internal.ManagementConstants.REGION_CREATED_PREFIX;

import java.lang.management.ManagementFactory;
import java.lang.reflect.Type;
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

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.locks.DLockService;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.ClassLoadUtils;
import org.apache.geode.internal.cache.CacheService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PartitionedRegionHelper;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.AsyncEventQueueMXBean;
import org.apache.geode.management.CacheServerMXBean;
import org.apache.geode.management.DiskStoreMXBean;
import org.apache.geode.management.GatewayReceiverMXBean;
import org.apache.geode.management.GatewaySenderMXBean;
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
 * <p>
 * Even though this class have a lot of utility functions it interacts with the state of the system
 * and contains some state itself.
 */
public class ManagementAdapter {

  private static final Logger logger = LogService.getLogger();

  @Immutable
  private static final List<String> INTERNAL_LOCK_SERVICES =
      unmodifiableList(asList(DLockService.DTLS, DLockService.LTLS,
          PartitionedRegionHelper.PARTITION_LOCK_SERVICE_NAME,
          PeerTypeRegistration.LOCK_SERVICE_NAME));

  private final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
  private final Object regionOpLock = new Object();

  private SystemManagementService service;
  private InternalCache internalCache;
  private String memberSource;
  private NotificationBroadcasterSupport memberLevelNotificationEmitter;
  private MemberMBean memberBean;
  private MBeanAggregator aggregator;
  private MemberMBeanBridge memberMBeanBridge;

  private volatile boolean serviceInitialised;

  /**
   * Adapter life cycle is tied with the Cache. So its better to make all cache level artifacts as
   * instance variable
   */
  void handleCacheCreation(InternalCache cache) throws ManagementException {
    try {
      internalCache = cache;
      service = (SystemManagementService) ManagementService.getManagementService(internalCache);

      memberMBeanBridge = new MemberMBeanBridge(internalCache, service).init();
      memberBean = new MemberMBean(memberMBeanBridge);
      memberLevelNotificationEmitter = memberBean;
      memberSource = MBeanJMXAdapter.getMemberNameOrUniqueId(cache.getMyId());

      // Type casting to MemberMXBean to expose only those methods described in interface
      ObjectName objectName = MBeanJMXAdapter.getMemberMBeanName(cache.getMyId());
      ObjectName federatedName = service.registerInternalMBean(memberBean, objectName);
      service.federate(federatedName, MemberMXBean.class, true);

      serviceInitialised = true;

      // Service initialised is only for ManagementService and not necessarily Manager service.

      // For situations where locator is created before any cache is created
      if (InternalLocator.hasLocator()) {
        handleLocatorStart(InternalLocator.getLocator());
      }

      if (cache.getInternalDistributedSystem().getConfig().getJmxManager()) {
        service.createManager();
        if (cache.getInternalDistributedSystem().getConfig().getJmxManagerStart()) {
          service.startManager();
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
  void handleManagerStart() throws ManagementException {
    if (!isServiceInitialised("handleManagerStart")) {
      return;
    }

    DistributedSystemBridge distributedSystemBridge =
        new DistributedSystemBridge(service, internalCache);
    aggregator = new MBeanAggregator(distributedSystemBridge);

    // register the aggregator for Federation framework to use
    service.addProxyListener(aggregator);

    // get the local member mbean as it need to be provided to aggregator first
    MemberMXBean localMemberMXBean = service.getMemberMXBean();

    ObjectName memberObjectName = MBeanJMXAdapter.getMemberMBeanName(
        InternalDistributedSystem.getConnectedInstance().getDistributedMember());

    FederationComponent memberFederation =
        service.getLocalManager().getFedComponents().get(memberObjectName);

    service.afterCreateProxy(memberObjectName, MemberMXBean.class, localMemberMXBean,
        memberFederation);

    MBeanJMXAdapter jmxAdapter = service.getJMXAdapter();
    Map<ObjectName, Object> registeredMBeans = jmxAdapter.getLocalGemFireMBean();

    for (ObjectName objectName : registeredMBeans.keySet()) {
      if (objectName.equals(memberObjectName)) {
        continue;
      }
      Object object = registeredMBeans.get(objectName);
      try {
        ObjectInstance instance = mbeanServer.getObjectInstance(objectName);
        String className = instance.getClassName();
        Class clazz = ClassLoadUtils.classFromName(className);
        Type[] interfaceTypes = clazz.getGenericInterfaces();

        FederationComponent federation =
            service.getLocalManager().getFedComponents().get(objectName);

        for (Type interfaceType : interfaceTypes) {
          Class interfaceTypeAsClass = (Class) interfaceType;
          service.afterCreateProxy(objectName, interfaceTypeAsClass, object, federation);

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
  void handleManagerStop() throws ManagementException {
    if (!isServiceInitialised("handleManagerStop")) {
      return;
    }

    MemberMXBean localMemberMXBean = service.getMemberMXBean();

    ObjectName memberObjectName = MBeanJMXAdapter.getMemberMBeanName(
        InternalDistributedSystem.getConnectedInstance().getDistributedMember());

    FederationComponent memberFederation =
        service.getLocalManager().getFedComponents().get(memberObjectName);

    service.afterRemoveProxy(memberObjectName, MemberMXBean.class, localMemberMXBean,
        memberFederation);

    ObjectName aggregateMBeanPattern = aggregateMBeanPattern();
    MBeanJMXAdapter jmxAdapter = service.getJMXAdapter();
    Map<ObjectName, Object> registeredMBeans = jmxAdapter.getLocalGemFireMBean();
    for (ObjectName objectName : registeredMBeans.keySet()) {
      if (objectName.equals(memberObjectName)) {
        continue;
      }
      if (aggregateMBeanPattern.apply(objectName)) {
        continue;
      }
      Object object = registeredMBeans.get(objectName);
      try {
        ObjectInstance instance = mbeanServer.getObjectInstance(objectName);
        String className = instance.getClassName();
        Class clazz = ClassLoadUtils.classFromName(className);
        Type[] interfaceTypes = clazz.getGenericInterfaces();

        FederationComponent federation =
            service.getLocalManager().getFedComponents().get(objectName);

        for (Type interfaceType : interfaceTypes) {
          Class interfaceTypeClass = (Class) interfaceType;
          service.afterRemoveProxy(objectName, interfaceTypeClass, object, federation);
        }
      } catch (InstanceNotFoundException | ClassNotFoundException e) {
        logger.warn("Failed to invoke aggregator for {} with exception {}", objectName,
            e.getMessage(), e);
      }
    }

    service.removeProxyListener(aggregator);
    aggregator = null;
  }

  /**
   * Assumption is always cache and MemberMbean has been will be created first
   */
  void handleManagerCreation() throws ManagementException {
    if (!isServiceInitialised("handleManagerCreation")) {
      return;
    }

    ObjectName objectName = MBeanJMXAdapter.getManagerName();
    ManagerMBeanBridge managerMBeanBridge = new ManagerMBeanBridge(service);
    ManagerMXBean managerMXBean = new ManagerMBean(managerMBeanBridge);

    ObjectName federatedName = service.registerInternalMBean(managerMXBean, objectName);
    service.federate(federatedName, ManagerMXBean.class, true);

    Notification notification = new Notification(MANAGER_STARTED, memberSource,
        SequenceNumber.next(), System.currentTimeMillis(), MANAGER_STARTED_PREFIX);
    memberLevelNotificationEmitter.sendNotification(notification);
  }

  /**
   * Handles Region Creation. This is the call back which will create the specified RegionMXBean and
   * will send a notification on behalf of Member Mbean
   *
   * @param region the region for which the call back is invoked
   */
  <K, V> void handleRegionCreation(Region<K, V> region) throws ManagementException {
    if (!isServiceInitialised("handleRegionCreation")) {
      return;
    }

    // Moving region creation operation inside a guarded block
    // After getting access to regionOpLock it again checks for region
    // destroy status

    synchronized (regionOpLock) {
      if (region.isDestroyed()) {
        return;
      }

      // Bridge is responsible for extracting data from GemFire Layer
      RegionMBeanBridge<K, V> regionMBeanBridge = RegionMBeanBridge.getInstance(region);
      RegionMXBean regionMXBean = new RegionMBean<>(regionMBeanBridge);
      ObjectName objectName = MBeanJMXAdapter.getRegionMBeanName(
          internalCache.getDistributedSystem().getDistributedMember(), region.getFullPath());
      ObjectName federatedName = service.registerInternalMBean(regionMXBean, objectName);
      service.federate(federatedName, RegionMXBean.class, true);

      Notification notification = new Notification(REGION_CREATED, memberSource,
          SequenceNumber.next(), System.currentTimeMillis(),
          REGION_CREATED_PREFIX + region.getFullPath());
      memberLevelNotificationEmitter.sendNotification(notification);

      memberMBeanBridge.addRegion(region);
    }
  }

  /**
   * Handles Disk Creation. Will create DiskStoreMXBean and will send a notification
   */
  void handleDiskCreation(DiskStore diskStore) throws ManagementException {
    if (!isServiceInitialised("handleDiskCreation")) {
      return;
    }

    DiskStoreMBeanBridge diskStoreMBeanBridge = new DiskStoreMBeanBridge(diskStore);
    DiskStoreMXBean diskStoreMXBean = new DiskStoreMBean(diskStoreMBeanBridge);
    ObjectName objectName = MBeanJMXAdapter.getDiskStoreMBeanName(
        internalCache.getDistributedSystem().getDistributedMember(), diskStore.getName());
    ObjectName federatedName = service.registerInternalMBean(diskStoreMXBean, objectName);
    service.federate(federatedName, DiskStoreMXBean.class, true);

    Notification notification = new Notification(DISK_STORE_CREATED, memberSource,
        SequenceNumber.next(), System.currentTimeMillis(),
        DISK_STORE_CREATED_PREFIX + diskStore.getName());
    memberLevelNotificationEmitter.sendNotification(notification);

    memberMBeanBridge.addDiskStore(diskStore);
  }

  /**
   * Handles LockService Creation
   *
   */
  void handleLockServiceCreation(DLockService lockService) throws ManagementException {
    if (!isServiceInitialised("handleLockServiceCreation")) {
      return;
    }
    // Internal Locks Should not be exposed to client for monitoring
    if (INTERNAL_LOCK_SERVICES.contains(lockService.getName())) {
      return;
    }

    LockServiceMBeanBridge lockServiceMBeanBridge = new LockServiceMBeanBridge(lockService);
    LockServiceMXBean lockServiceMXBean = new LockServiceMBean(lockServiceMBeanBridge);
    ObjectName objectName = MBeanJMXAdapter.getLockServiceMBeanName(
        internalCache.getDistributedSystem().getDistributedMember(), lockService.getName());
    ObjectName federatedName =
        service.registerInternalMBean(lockServiceMXBean, objectName);
    service.federate(federatedName, LockServiceMXBean.class, true);

    Notification notification = new Notification(LOCK_SERVICE_CREATED, memberSource,
        SequenceNumber.next(), System.currentTimeMillis(),
        LOCK_SERVICE_CREATED_PREFIX + lockService.getName());
    memberLevelNotificationEmitter.sendNotification(notification);

    memberMBeanBridge.addLockServiceStats(lockService);
  }

  /**
   * Handles GatewaySender creation
   *
   * @param sender the specific gateway sender
   */
  void handleGatewaySenderCreation(GatewaySender sender) throws ManagementException {
    if (!isServiceInitialised("handleGatewaySenderCreation")) {
      return;
    }

    GatewaySenderMBeanBridge gatewaySenderMBeanBridge = new GatewaySenderMBeanBridge(sender);
    GatewaySenderMXBean gatewaySenderMXBean = new GatewaySenderMBean(gatewaySenderMBeanBridge);
    ObjectName objectName = MBeanJMXAdapter.getGatewaySenderMBeanName(
        internalCache.getDistributedSystem().getDistributedMember(), sender.getId());
    ObjectName federatedName = service.registerInternalMBean(gatewaySenderMXBean, objectName);
    service.federate(federatedName, GatewaySenderMXBean.class, true);

    Notification notification = new Notification(GATEWAY_SENDER_CREATED, memberSource,
        SequenceNumber.next(), System.currentTimeMillis(), GATEWAY_SENDER_CREATED_PREFIX);
    memberLevelNotificationEmitter.sendNotification(notification);
  }

  /**
   * Handles Gateway receiver creation
   *
   * @param gatewayReceiver specific gateway receiver
   */
  void handleGatewayReceiverCreate(GatewayReceiver gatewayReceiver) throws ManagementException {
    if (!isServiceInitialised("handleGatewayReceiverCreate")) {
      return;
    }
    if (!gatewayReceiver.isManualStart()) {
      return;
    }

    createGatewayReceiverMBean(gatewayReceiver);
  }

  private void createGatewayReceiverMBean(GatewayReceiver gatewayReceiver) {
    GatewayReceiverMBeanBridge gatewayReceiverMBeanBridge =
        new GatewayReceiverMBeanBridge(gatewayReceiver);
    GatewayReceiverMXBean gatewayReceiverMXBean =
        new GatewayReceiverMBean(gatewayReceiverMBeanBridge);
    ObjectName objectName = MBeanJMXAdapter
        .getGatewayReceiverMBeanName(internalCache.getDistributedSystem().getDistributedMember());
    ObjectName federatedName = service.registerInternalMBean(gatewayReceiverMXBean, objectName);
    service.federate(federatedName, GatewayReceiverMXBean.class, true);

    Notification notification = new Notification(GATEWAY_RECEIVER_CREATED, memberSource,
        SequenceNumber.next(), System.currentTimeMillis(), GATEWAY_RECEIVER_CREATED_PREFIX);
    memberLevelNotificationEmitter.sendNotification(notification);
  }

  void handleGatewayReceiverDestroy() throws ManagementException {
    if (!isServiceInitialised("handleGatewayReceiverDestroy")) {
      return;
    }

    GatewayReceiverMBean gatewayReceiverMBean =
        (GatewayReceiverMBean) service.getLocalGatewayReceiverMXBean();
    GatewayReceiverMBeanBridge gatewayReceiverMBeanBridge = gatewayReceiverMBean.getBridge();

    gatewayReceiverMBeanBridge.destroyServer();

    ObjectName objectName = MBeanJMXAdapter
        .getGatewayReceiverMBeanName(internalCache.getDistributedSystem().getDistributedMember());
    service.unregisterMBean(objectName);

    Notification notification = new Notification(GATEWAY_RECEIVER_DESTROYED, memberSource,
        SequenceNumber.next(), System.currentTimeMillis(), GATEWAY_RECEIVER_DESTROYED_PREFIX);
    memberLevelNotificationEmitter.sendNotification(notification);
  }

  /**
   * Handles Gateway receiver creation
   *
   * @param gatewayReceiver specific gateway receiver
   */
  void handleGatewayReceiverStart(GatewayReceiver gatewayReceiver) throws ManagementException {
    if (!isServiceInitialised("handleGatewayReceiverStart")) {
      return;
    }

    if (!gatewayReceiver.isManualStart()) {
      createGatewayReceiverMBean(gatewayReceiver);
    }

    GatewayReceiverMBean gatewayReceiverMBean =
        (GatewayReceiverMBean) service.getLocalGatewayReceiverMXBean();
    GatewayReceiverMBeanBridge gatewayReceiverMBeanBridge = gatewayReceiverMBean.getBridge();

    gatewayReceiverMBeanBridge.startServer();

    Notification notification = new Notification(GATEWAY_RECEIVER_STARTED, memberSource,
        SequenceNumber.next(), System.currentTimeMillis(), GATEWAY_RECEIVER_STARTED_PREFIX);
    memberLevelNotificationEmitter.sendNotification(notification);
  }

  void handleGatewayReceiverStop() throws ManagementException {
    if (!isServiceInitialised("handleGatewayReceiverStop")) {
      return;
    }

    GatewayReceiverMBean gatewayReceiverMBean =
        (GatewayReceiverMBean) service.getLocalGatewayReceiverMXBean();
    GatewayReceiverMBeanBridge gatewayReceiverMBeanBridge = gatewayReceiverMBean.getBridge();

    gatewayReceiverMBeanBridge.stopServer();

    Notification notification = new Notification(GATEWAY_RECEIVER_STOPPED, memberSource,
        SequenceNumber.next(), System.currentTimeMillis(), GATEWAY_RECEIVER_STOPPED_PREFIX);
    memberLevelNotificationEmitter.sendNotification(notification);
  }

  void handleAsyncEventQueueCreation(AsyncEventQueue asyncEventQueue) throws ManagementException {
    if (!isServiceInitialised("handleAsyncEventQueueCreation")) {
      return;
    }

    AsyncEventQueueMBeanBridge asyncEventQueueMBeanBridge =
        new AsyncEventQueueMBeanBridge(asyncEventQueue);
    AsyncEventQueueMXBean asyncEventQueueMXBean =
        new AsyncEventQueueMBean(asyncEventQueueMBeanBridge);
    ObjectName objectName = MBeanJMXAdapter.getAsyncEventQueueMBeanName(
        internalCache.getDistributedSystem().getDistributedMember(), asyncEventQueue.getId());
    ObjectName federatedName = service.registerInternalMBean(asyncEventQueueMXBean, objectName);
    service.federate(federatedName, AsyncEventQueueMXBean.class, true);

    Notification notification = new Notification(ASYNC_EVENT_QUEUE_CREATED, memberSource,
        SequenceNumber.next(), System.currentTimeMillis(), ASYNC_EVENT_QUEUE_CREATED_PREFIX);
    memberLevelNotificationEmitter.sendNotification(notification);
  }

  void handleAsyncEventQueueRemoval(AsyncEventQueue asyncEventQueue) throws ManagementException {
    if (!isServiceInitialised("handleAsyncEventQueueRemoval")) {
      return;
    }

    ObjectName objectName = MBeanJMXAdapter.getAsyncEventQueueMBeanName(
        internalCache.getDistributedSystem().getDistributedMember(), asyncEventQueue.getId());

    try {
      AsyncEventQueueMBean asyncEventQueueMBean =
          (AsyncEventQueueMBean) service.getLocalAsyncEventQueueMXBean(asyncEventQueue.getId());
      if (asyncEventQueueMBean == null) {
        return;
      }

      asyncEventQueueMBean.stopMonitor();

    } catch (ManagementException e) {
      // If no bean found its a NO-OP
      if (logger.isDebugEnabled()) {
        logger.debug(e.getMessage(), e);
      }
      return;
    }

    service.unregisterMBean(objectName);

    Notification notification = new Notification(ASYNC_EVENT_QUEUE_CLOSED, memberSource,
        SequenceNumber.next(), System.currentTimeMillis(),
        ASYNC_EVENT_QUEUE_CLOSED_PREFIX + asyncEventQueue.getId());
    memberLevelNotificationEmitter.sendNotification(notification);
  }

  /**
   * Sends the alert with the Object source as member. This notification will get filtered out for
   * particular alert level
   */
  void handleSystemNotification(AlertDetails alertDetails) {
    if (!isServiceInitialised("handleSystemNotification")) {
      return;
    }

    if (service.isManager()) {
      String systemSource = "DistributedSystem("
          + service.getDistributedSystemMXBean().getDistributedSystemId() + ")";
      Notification notification = new Notification(SYSTEM_ALERT, systemSource,
          SequenceNumber.next(), alertDetails.getMsgTime().getTime(), alertDetails.getMsg());
      notification.setUserData(prepareUserData(alertDetails));
      service.handleNotification(notification);
    }
  }

  private Map<String, String> prepareUserData(AlertDetails alertDetails) {
    Map<String, String> userData = new HashMap<>();

    userData.put(ALERT_LEVEL, getAlertLevelAsString(alertDetails.getAlertLevel()));
    userData.put(THREAD, alertDetails.getSource());
    userData.put(MEMBER, getNameOrId(alertDetails.getSender()));

    return userData;
  }

  /**
   * Assumption is its a cache server instance. For Gateway receiver there will be a separate method
   *
   * @param cacheServer cache server instance
   */
  void handleCacheServerStart(CacheServer cacheServer) {
    if (!isServiceInitialised("handleCacheServerStart")) {
      return;
    }

    CacheServerBridge cacheServerBridge = new CacheServerBridge(internalCache, cacheServer);
    cacheServerBridge.setMemberMBeanBridge(memberMBeanBridge);
    CacheServerMBean cacheServerMBean = new CacheServerMBean(cacheServerBridge);
    ObjectName objectName = MBeanJMXAdapter.getClientServiceMBeanName(
        cacheServer.getPort(), internalCache.getDistributedSystem().getDistributedMember());
    ObjectName federatedName = service.registerInternalMBean(cacheServerMBean, objectName);

    ClientMembershipListener managementClientListener = new CacheServerMembershipListenerAdapter(
        cacheServerMBean, memberLevelNotificationEmitter, federatedName);
    ClientMembership.registerClientMembershipListener(managementClientListener);
    cacheServerBridge.setClientMembershipListener(managementClientListener);

    service.federate(federatedName, CacheServerMXBean.class, true);

    Notification notification = new Notification(CACHE_SERVER_STARTED, memberSource,
        SequenceNumber.next(), System.currentTimeMillis(), CACHE_SERVER_STARTED_PREFIX);
    memberLevelNotificationEmitter.sendNotification(notification);

    memberMBeanBridge.setCacheServer(true);
  }

  /**
   * Assumption is its a cache server instance. For Gateway receiver there will be a separate method
   */
  void handleCacheServerStop(CacheServer server) {
    if (!isServiceInitialised("handleCacheServerStop")) {
      return;
    }

    CacheServerMBean cacheServerMBean =
        (CacheServerMBean) service.getLocalCacheServerMXBean(server.getPort());

    ClientMembershipListener clientMembershipListener =
        cacheServerMBean.getBridge().getClientMembershipListener();
    if (clientMembershipListener != null) {
      ClientMembership.unregisterClientMembershipListener(clientMembershipListener);
    }

    cacheServerMBean.stopMonitor();

    ObjectName objectName = MBeanJMXAdapter.getClientServiceMBeanName(server.getPort(),
        internalCache.getDistributedSystem().getDistributedMember());
    service.unregisterMBean(objectName);

    Notification notification = new Notification(CACHE_SERVER_STOPPED, memberSource,
        SequenceNumber.next(), System.currentTimeMillis(), CACHE_SERVER_STOPPED_PREFIX);
    memberLevelNotificationEmitter.sendNotification(notification);

    memberMBeanBridge.setCacheServer(false);
  }

  void handleCacheRemoval() throws ManagementException {
    if (!isServiceInitialised("handleCacheRemoval")) {
      return;
    }

    serviceInitialised = false;
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
      internalCache = null;
      service = null;
      memberMBeanBridge = null;
      memberBean = null;
      memberLevelNotificationEmitter = null;
    }
  }

  private String getNameOrId(DistributedMember member) {
    if (member == null) {
      return memberSource;
    }
    return isNotBlank(member.getName()) ? member.getName() : member.getId();
  }

  private void cleanUpMonitors() {
    MemberMBean memberMBean = (MemberMBean) service.getMemberMXBean();
    if (memberMBean != null) {
      memberMBean.stopMonitor();
    }

    Set<GatewaySender> gatewaySenders = internalCache.getGatewaySenders();

    if (gatewaySenders != null && !gatewaySenders.isEmpty()) {
      for (GatewaySender gatewaySender : gatewaySenders) {
        GatewaySenderMBean gatewaySenderMBean =
            (GatewaySenderMBean) service.getLocalGatewaySenderMXBean(gatewaySender.getId());
        if (gatewaySenderMBean != null) {
          gatewaySenderMBean.stopMonitor();
        }
      }
    }

    GatewayReceiverMBean gatewayReceiverMBean =
        (GatewayReceiverMBean) service.getLocalGatewayReceiverMXBean();
    if (gatewayReceiverMBean != null) {
      gatewayReceiverMBean.stopMonitor();
    }
  }

  private void cleanBridgeResources() {
    List<CacheServer> cacheServers = internalCache.getCacheServers();

    if (cacheServers != null && !cacheServers.isEmpty()) {
      for (CacheServer cacheServer : cacheServers) {
        CacheServerMBean cacheServerMBean =
            (CacheServerMBean) service.getLocalCacheServerMXBean(cacheServer.getPort());

        if (cacheServerMBean != null) {
          ClientMembershipListener listener =
              cacheServerMBean.getBridge().getClientMembershipListener();

          if (listener != null) {
            ClientMembership.unregisterClientMembershipListener(listener);
          }
        }
      }
    }
  }

  /**
   * Handles particular region destroy or close operation it will remove the corresponding MBean
   */
  void handleRegionRemoval(Region region) throws ManagementException {
    if (!isServiceInitialised("handleRegionRemoval")) {
      return;
    }

    // Moved region remove operation to a guarded block. If a region is getting created it won't
    // allow it to destroy any region.

    synchronized (regionOpLock) {
      ObjectName objectName = MBeanJMXAdapter.getRegionMBeanName(
          internalCache.getDistributedSystem().getDistributedMember(), region.getFullPath());

      try {
        RegionMBean regionMBean = (RegionMBean) service.getLocalRegionMBean(region.getFullPath());
        if (regionMBean != null) {
          regionMBean.stopMonitor();
        }
      } catch (ManagementException e) {
        // If no bean found its a NO-OP
        // Mostly for situation like DiskAccessException while creating region
        // which does a compensatory close region
        if (logger.isDebugEnabled()) {
          logger.debug(e.getMessage(), e);
        }
        return;
      }

      service.unregisterMBean(objectName);

      Notification notification = new Notification(REGION_CLOSED, memberSource,
          SequenceNumber.next(), System.currentTimeMillis(),
          REGION_CLOSED_PREFIX + region.getFullPath());
      memberLevelNotificationEmitter.sendNotification(notification);

      memberMBeanBridge.removeRegion(region);
    }
  }

  void handleDiskRemoval(DiskStore diskStore) throws ManagementException {
    if (!isServiceInitialised("handleDiskRemoval")) {
      return;
    }

    ObjectName objectName = MBeanJMXAdapter.getDiskStoreMBeanName(
        internalCache.getDistributedSystem().getDistributedMember(), diskStore.getName());

    try {
      DiskStoreMBean diskStoreMBean =
          (DiskStoreMBean) service.getLocalDiskStoreMBean(diskStore.getName());
      if (diskStoreMBean == null) {
        return;
      }
      diskStoreMBean.stopMonitor();
    } catch (ManagementException e) {
      // If no bean found its a NO-OP
      if (logger.isDebugEnabled()) {
        logger.debug(e.getMessage(), e);
      }
      return;
    }

    service.unregisterMBean(objectName);

    Notification notification = new Notification(DISK_STORE_CLOSED, memberSource,
        SequenceNumber.next(), System.currentTimeMillis(),
        DISK_STORE_CLOSED_PREFIX + diskStore.getName());
    memberLevelNotificationEmitter.sendNotification(notification);

    memberMBeanBridge.removeDiskStore(diskStore);
  }

  void handleLockServiceRemoval(DLockService lockService) throws ManagementException {
    if (!isServiceInitialised("handleLockServiceRemoval")) {
      return;
    }

    ObjectName objectName = MBeanJMXAdapter.getLockServiceMBeanName(
        internalCache.getDistributedSystem().getDistributedMember(), lockService.getName());

    service.unregisterMBean(objectName);

    Notification notification = new Notification(LOCK_SERVICE_CLOSED, memberSource,
        SequenceNumber.next(), System.currentTimeMillis(),
        LOCK_SERVICE_CLOSED_PREFIX + lockService.getName());
    memberLevelNotificationEmitter.sendNotification(notification);
  }

  /**
   * Handles management side call backs for a locator creation and start. Assumption is a cache will
   * be created before hand.
   *
   * <p>
   * There is no corresponding handleStopLocator() method. Locator will close the cache whenever its
   * stopped and it should also shutdown all the management services by closing the cache.
   *
   * @param locator instance of locator which is getting started
   */
  void handleLocatorStart(Locator locator) throws ManagementException {
    if (!isServiceInitialised("handleLocatorCreation")) {
      return;
    }

    ObjectName objectName = MBeanJMXAdapter
        .getLocatorMBeanName(internalCache.getDistributedSystem().getDistributedMember());
    LocatorMBeanBridge locatorMBeanBridge = new LocatorMBeanBridge(locator);
    LocatorMBean locatorMBean = new LocatorMBean(locatorMBeanBridge);
    ObjectName federatedName = service.registerInternalMBean(locatorMBean, objectName);
    service.federate(federatedName, LocatorMXBean.class, true);

    Notification notification = new Notification(LOCATOR_STARTED, memberSource,
        SequenceNumber.next(), System.currentTimeMillis(), LOCATOR_STARTED_PREFIX);
    memberLevelNotificationEmitter.sendNotification(notification);
  }

  void handleGatewaySenderStart(GatewaySender gatewaySender) throws ManagementException {
    if (!isServiceInitialised("handleGatewaySenderStart")) {
      return;
    }
    if (gatewaySender.getRemoteDSId() < 0) {
      return;
    }

    GatewaySenderMBean gatewaySenderMBean =
        (GatewaySenderMBean) service.getLocalGatewaySenderMXBean(gatewaySender.getId());

    gatewaySenderMBean.getBridge().setDispatcher();

    Notification notification = new Notification(GATEWAY_SENDER_STARTED, memberSource,
        SequenceNumber.next(), System.currentTimeMillis(),
        GATEWAY_SENDER_STARTED_PREFIX + gatewaySender.getId());
    memberLevelNotificationEmitter.sendNotification(notification);
  }

  void handleGatewaySenderStop(GatewaySender gatewaySender) throws ManagementException {
    if (!isServiceInitialised("handleGatewaySenderStop")) {
      return;
    }

    Notification notification = new Notification(GATEWAY_SENDER_STOPPED, memberSource,
        SequenceNumber.next(), System.currentTimeMillis(),
        GATEWAY_SENDER_STOPPED_PREFIX + gatewaySender.getId());
    memberLevelNotificationEmitter.sendNotification(notification);
  }

  void handleGatewaySenderPaused(GatewaySender gatewaySender) throws ManagementException {
    if (!isServiceInitialised("handleGatewaySenderPaused")) {
      return;
    }

    Notification notification = new Notification(GATEWAY_SENDER_PAUSED, memberSource,
        SequenceNumber.next(), System.currentTimeMillis(),
        GATEWAY_SENDER_PAUSED_PREFIX + gatewaySender.getId());
    memberLevelNotificationEmitter.sendNotification(notification);
  }

  void handleGatewaySenderResumed(GatewaySender gatewaySender) throws ManagementException {
    if (!isServiceInitialised("handleGatewaySenderResumed")) {
      return;
    }

    Notification notification = new Notification(GATEWAY_SENDER_RESUMED, memberSource,
        SequenceNumber.next(), System.currentTimeMillis(),
        GATEWAY_SENDER_RESUMED_PREFIX + gatewaySender.getId());
    memberLevelNotificationEmitter.sendNotification(notification);
  }

  void handleGatewaySenderRemoved(GatewaySender gatewaySender) throws ManagementException {
    if (!isServiceInitialised("handleGatewaySenderRemoved")) {
      return;
    }
    if (gatewaySender.getRemoteDSId() < 0) {
      return;
    }

    GatewaySenderMBean gatewaySenderMBean =
        (GatewaySenderMBean) service.getLocalGatewaySenderMXBean(gatewaySender.getId());
    gatewaySenderMBean.stopMonitor();

    ObjectName objectName = MBeanJMXAdapter.getGatewaySenderMBeanName(
        internalCache.getDistributedSystem().getDistributedMember(), gatewaySender.getId());
    service.unregisterMBean(objectName);

    Notification notification = new Notification(GATEWAY_SENDER_REMOVED, memberSource,
        SequenceNumber.next(), System.currentTimeMillis(),
        GATEWAY_SENDER_REMOVED_PREFIX + gatewaySender.getId());
    memberLevelNotificationEmitter.sendNotification(notification);
  }

  void handleCacheServiceCreation(CacheService cacheService) throws ManagementException {
    if (!isServiceInitialised("handleCacheServiceCreation")) {
      return;
    }

    // Don't register the CacheServices in the Locator
    InternalDistributedMember member =
        internalCache.getInternalDistributedSystem().getDistributedMember();
    if (member.getVmKind() == ClusterDistributionManager.LOCATOR_DM_TYPE) {
      return;
    }

    CacheServiceMBeanBase cacheServiceMBean = cacheService.getMBean();
    if (cacheServiceMBean != null) {
      String id = cacheServiceMBean.getId();
      ObjectName objectName = MBeanJMXAdapter.getCacheServiceMBeanName(member, id);
      ObjectName federatedName = service.registerInternalMBean(cacheServiceMBean, objectName);
      service.federate(federatedName, cacheServiceMBean.getInterfaceClass(), true);

      Notification notification = new Notification(CACHE_SERVICE_CREATED, memberSource,
          SequenceNumber.next(), System.currentTimeMillis(),
          CACHE_SERVICE_CREATED_PREFIX + id);
      memberLevelNotificationEmitter.sendNotification(notification);
    }
  }

  private ObjectName aggregateMBeanPattern() {
    try {
      return new ObjectName(AGGREGATE_MBEAN_PATTERN);
    } catch (MalformedObjectNameException | NullPointerException e) {
      throw new ManagementException(e);
    }
  }

  private boolean isServiceInitialised(String method) {
    if (!serviceInitialised) {
      if (logger.isDebugEnabled()) {
        logger.debug("Management Service is not initialised hence returning from {}", method);
      }
    }

    return serviceInitialised;
  }

  /**
   * Propagates client joined/left notifications
   */
  private static class CacheServerMembershipListenerAdapter
      extends ClientMembershipListenerAdapter {

    private final NotificationBroadcasterSupport serverLevelNotificationEmitter;
    private final NotificationBroadcasterSupport memberLevelNotificationEmitter;
    private final String serverSource;

    private CacheServerMembershipListenerAdapter(
        NotificationBroadcasterSupport serverLevelNotificationEmitter,
        NotificationBroadcasterSupport memberLevelNotificationEmitter,
        ObjectName serverSource) {
      this.serverLevelNotificationEmitter = serverLevelNotificationEmitter;
      this.memberLevelNotificationEmitter = memberLevelNotificationEmitter;
      this.serverSource = serverSource.toString();
    }

    /**
     * Invoked when a client has connected to this process or when this process has connected to a
     * CacheServer.
     */
    @Override
    public void memberJoined(ClientMembershipEvent event) {
      Notification notification = new Notification(CLIENT_JOINED, serverSource,
          SequenceNumber.next(), System.currentTimeMillis(),
          CLIENT_JOINED_PREFIX + event.getMemberId());

      serverLevelNotificationEmitter.sendNotification(notification);
      memberLevelNotificationEmitter.sendNotification(notification);
    }

    /**
     * Invoked when a client has gracefully disconnected from this process or when this process has
     * gracefully disconnected from a CacheServer.
     */
    @Override
    public void memberLeft(ClientMembershipEvent event) {
      Notification notification = new Notification(CLIENT_LEFT, serverSource,
          SequenceNumber.next(), System.currentTimeMillis(),
          CLIENT_LEFT_PREFIX + event.getMemberId());

      serverLevelNotificationEmitter.sendNotification(notification);
      memberLevelNotificationEmitter.sendNotification(notification);
    }

    /**
     * Invoked when a client has unexpectedly disconnected from this process or when this process
     * has unexpectedly disconnected from a CacheServer.
     */
    @Override
    public void memberCrashed(ClientMembershipEvent event) {
      Notification notification = new Notification(CLIENT_CRASHED, serverSource,
          SequenceNumber.next(), System.currentTimeMillis(),
          CLIENT_CRASHED_PREFIX + event.getMemberId());

      serverLevelNotificationEmitter.sendNotification(notification);
      memberLevelNotificationEmitter.sendNotification(notification);
    }
  }
}
