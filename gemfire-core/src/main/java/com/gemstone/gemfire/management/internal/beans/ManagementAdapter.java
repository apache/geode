/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  ========================================================================
 */
package com.gemstone.gemfire.management.internal.beans;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
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

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueue;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache.wan.GatewayReceiver;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalLocator;
import com.gemstone.gemfire.distributed.internal.locks.DLockService;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.ClassLoadUtil;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegionHelper;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.management.AsyncEventQueueMXBean;
import com.gemstone.gemfire.management.CacheServerMXBean;
import com.gemstone.gemfire.management.DiskStoreMXBean;
import com.gemstone.gemfire.management.GatewayReceiverMXBean;
import com.gemstone.gemfire.management.GatewaySenderMXBean;
import com.gemstone.gemfire.management.JMXNotificationType;
import com.gemstone.gemfire.management.JMXNotificationUserData;
import com.gemstone.gemfire.management.LocatorMXBean;
import com.gemstone.gemfire.management.LockServiceMXBean;
import com.gemstone.gemfire.management.ManagementException;
import com.gemstone.gemfire.management.ManagementService;
import com.gemstone.gemfire.management.ManagerMXBean;
import com.gemstone.gemfire.management.MemberMXBean;
import com.gemstone.gemfire.management.RegionMXBean;
import com.gemstone.gemfire.management.internal.AlertDetails;
import com.gemstone.gemfire.management.internal.FederationComponent;
import com.gemstone.gemfire.management.internal.MBeanJMXAdapter;
import com.gemstone.gemfire.management.internal.ManagementConstants;
import com.gemstone.gemfire.management.internal.SystemManagementService;
import com.gemstone.gemfire.management.membership.ClientMembership;
import com.gemstone.gemfire.management.membership.ClientMembershipEvent;
import com.gemstone.gemfire.management.membership.ClientMembershipListener;
import com.gemstone.gemfire.management.membership.ClientMembershipListenerAdapter;
import com.gemstone.gemfire.pdx.internal.PeerTypeRegistration;

/**
 * Acts as an intermediate between MBean layer and Federation Layer. Handles all
 * Call backs from GemFire to instantiate or remove MBeans from GemFire Domain.
 * 
 * Even though this class have a lot of utility functions it interacts with the state 
 * of the system and contains some state itself.
 * 
 * 
 * @author rishim
 * 
 */
public class ManagementAdapter {

  private static final Logger logger = LogService.getLogger();

  /** Internal ManagementService Instance **/
  private SystemManagementService service;

  /** GemFire Cache impl **/
  private GemFireCacheImpl cacheImpl;
  
  /** Member Name **/
  private String memberSource;

  /**
   * emitter is a helper class for sending notifications on behalf of the
   * MemberMBean
   **/
  private NotificationBroadcasterSupport memberLevelNotifEmitter;

  
  /** The <code>MBeanServer</code> for this application */
  public static final MBeanServer mbeanServer = MBeanJMXAdapter.mbeanServer;
  

  /** MemberMBean instance **/
  private MemberMBean memberBean;
  
  
 
  private volatile boolean serviceInitialised = false;  
  
  private MBeanAggregator aggregator;
  
  public static final List<Class> refreshOnInit = new ArrayList<Class>();
  
  
  public static final List<String> internalLocks = new ArrayList<String>();
  

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
   * Adapter life cycle is tied with the Cache . So its better to make all cache
   * level artifacts as instance variable
   * 
   * @param cache
   *          gemfire cache
   */
  public void handleCacheCreation(GemFireCacheImpl cache) throws ManagementException
  {
    try{
      this.cacheImpl = (GemFireCacheImpl) cache;
      this.service = (SystemManagementService) ManagementService
          .getManagementService(cacheImpl);
      
      this.memberMBeanBridge = new MemberMBeanBridge(cacheImpl, service).init();
      this.memberBean = new MemberMBean(memberMBeanBridge);
      this.memberLevelNotifEmitter = memberBean;
      
      ObjectName memberMBeanName = MBeanJMXAdapter
          .getMemberMBeanName(InternalDistributedSystem.getConnectedInstance()
              .getDistributedMember());

     
      memberSource = MBeanJMXAdapter.getMemberNameOrId(cacheImpl.getDistributedSystem().getDistributedMember());


      // Type casting to MemberMXBean to expose only those methods described in
      // the interface;
      ObjectName changedMBeanName = service.registerInternalMBean(
          (MemberMXBean) memberBean, memberMBeanName);
      service.federate(changedMBeanName, MemberMXBean.class, true);
     
      
      
      this.serviceInitialised = true;
      
      // Service initialised is only for ManagementService and not necessarily
      // Manager service.
      
      
      /** For situations where locator is created before any cache is created **/
      if (InternalLocator.hasLocator()) {
        Locator loc = InternalLocator.getLocator();
        handleLocatorStart(loc);
      }

      
      if (cache.getDistributedSystem().getConfig().getJmxManager()) {
        this.service.createManager();
        if (cache.getDistributedSystem().getConfig().getJmxManagerStart()) {
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
  public void handleManagerStart() throws ManagementException{
    if (!isServiceInitialised("handleManagerStart")) {
      return;
    }
    MBeanJMXAdapter jmxAdapter = service.getJMXAdapter();
    Map<ObjectName, Object> registeredMBeans = jmxAdapter
        .getLocalGemFireMBean();

    DistributedSystemBridge dsBridge = new DistributedSystemBridge(service);
    this.aggregator = new MBeanAggregator(dsBridge);
    // register the aggregator for Federation framework to use
    service.addProxyListener(aggregator);
   
    /**
     * get the local member mbean as it need to be provided to aggregator first
     */

    MemberMXBean localMember = service.getMemberMXBean();
    ObjectName memberObjectName = MBeanJMXAdapter
        .getMemberMBeanName(InternalDistributedSystem.getConnectedInstance()
            .getDistributedMember());

    FederationComponent addedComp = service.getLocalManager().getFedComponents().get(
        memberObjectName);
    
    service.afterCreateProxy(memberObjectName, MemberMXBean.class,
        localMember, addedComp);

    Iterator<ObjectName> it = registeredMBeans.keySet().iterator();
    while (it.hasNext()) {
      ObjectName objectName = it.next();
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
        
        FederationComponent newObj = service.getLocalManager().getFedComponents().get(
            objectName);
        
        for (int i = 0; i < intfTyps.length; i++) {
          Class intfTyp = (Class) intfTyps[i];
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
   * Handles all the clean up activities when a Manager is stopped
   * It clears the distributed mbeans and underlying data structures
   */
  public void handleManagerStop() throws ManagementException{
    if (!isServiceInitialised("handleManagerStop")) {
      return;
    }
    MBeanJMXAdapter jmxAdapter = service.getJMXAdapter();
    Map<ObjectName, Object> registeredMBeans = jmxAdapter
        .getLocalGemFireMBean();
    
    ObjectName aggregatemMBeanPattern = null;
    try {
      aggregatemMBeanPattern = new ObjectName(
          ManagementConstants.AGGREGATE_MBEAN_PATTERN);
    } catch (MalformedObjectNameException e1) {
      throw new ManagementException(e1);
    } catch (NullPointerException e1) {
      throw new ManagementException(e1);
    }
    
    MemberMXBean localMember = service.getMemberMXBean();
    
    ObjectName memberObjectName = MBeanJMXAdapter
        .getMemberMBeanName(InternalDistributedSystem.getConnectedInstance()
            .getDistributedMember());
    
    FederationComponent removedComp = service.getLocalManager().getFedComponents().get(
        memberObjectName);
    
    service.afterRemoveProxy(memberObjectName, MemberMXBean.class,
        localMember, removedComp);
    
    Iterator<ObjectName> it = registeredMBeans.keySet().iterator();
    
    while (it.hasNext()) {
      ObjectName objectName = it.next();
      if (objectName.equals(memberObjectName)) {
        continue;
      }
      if(aggregatemMBeanPattern.apply(objectName)){
        continue;
      }
      Object object = registeredMBeans.get(objectName);
      ObjectInstance instance;
      try {
        instance = mbeanServer.getObjectInstance(objectName);
        String className = instance.getClassName();
        Class cls = ClassLoadUtil.classFromName(className);
        Type[] intfTyps = cls.getGenericInterfaces();
        
        FederationComponent oldObj = service.getLocalManager().getFedComponents().get(
            objectName);
        
        for (int i = 0; i < intfTyps.length; i++) {
          Class intfTyp = (Class) intfTyps[i];
          service.afterRemoveProxy(objectName, intfTyp, object, oldObj);
        }
      } catch (InstanceNotFoundException e) {
        logger.warn("Failed to invoke aggregator for {} with exception {}", objectName, e.getMessage(), e);
      } catch (ClassNotFoundException e) {
        logger.warn("Failed to invoke aggregator for {} with exception {}", objectName, e.getMessage(), e);
      }

    }
    service.removeProxyListener(this.aggregator);
    this.aggregator = null;
  }
  
  /**
   * Assumption is always cache and MemberMbean has been will be created first
   * 
   */
  public void handleManagerCreation() throws ManagementException{
    if (!isServiceInitialised("handleManagerCreation")) {
      return;
    }
    
    ObjectName managerMBeanName = MBeanJMXAdapter.getManagerName();

    ManagerMBeanBridge bridge = new ManagerMBeanBridge(service);

    ManagerMXBean bean = new ManagerMBean(bridge);

    service.registerInternalMBean(bean, managerMBeanName);

  }
  
  /**
   * Handles Region Creation. This is the call back which will create the
   * specified RegionMXBean and will send a notification on behalf of Member
   * Mbean
   * 
   * @param region
   *          the region for which the call back is invoked
   */
  public <K, V> void handleRegionCreation(Region<K, V> region)
      throws ManagementException {
    if (!isServiceInitialised("handleRegionCreation")) {
      return;
    }
    //Moving region creation operation inside a guarded block
    //After getting access to regionOpLock it again checks for region
    //destroy status 
    
    synchronized(regionOpLock){
      LocalRegion localRegion = (LocalRegion) region;
      if(localRegion.isDestroyed()){
        return;
      }
      /** Bridge is responsible for extracting data from GemFire Layer **/
      RegionMBeanBridge<K, V> bridge = RegionMBeanBridge.getInstance(region);
      
      RegionMXBean regionMBean = new RegionMBean<K, V>(bridge);
      ObjectName regionMBeanName = MBeanJMXAdapter.getRegionMBeanName(cacheImpl
          .getDistributedSystem().getDistributedMember(), region.getFullPath());
      ObjectName changedMBeanName = service.registerInternalMBean(regionMBean,
          regionMBeanName);
      service.federate(changedMBeanName, RegionMXBean.class, true);
      
      Notification notification = new Notification(
          JMXNotificationType.REGION_CREATED, memberSource, SequenceNumber
              .next(), System.currentTimeMillis(), ManagementConstants.REGION_CREATED_PREFIX
              + region.getFullPath());
      memberLevelNotifEmitter.sendNotification(notification);
      memberMBeanBridge.addRegion(region);
    
      
    }
    
   
  }

  /**
   * Handles Disk Creation. Will create DiskStoreMXBean and will send a
   * notification
   * 
   * @param disk
   *          the disk store for which the call back is invoked
   */
  public void handleDiskCreation(DiskStore disk) throws ManagementException{
    if (!isServiceInitialised("handleDiskCreation")) {
      return;
    }
    DiskStoreMBeanBridge bridge = new DiskStoreMBeanBridge(disk);
    DiskStoreMXBean diskStoreMBean = new DiskStoreMBean(bridge);
    ObjectName diskStoreMBeanName = MBeanJMXAdapter.getDiskStoreMBeanName(
        cacheImpl.getDistributedSystem().getDistributedMember(), disk.getName());
    ObjectName changedMBeanName = service.registerInternalMBean(diskStoreMBean,
        diskStoreMBeanName);

    service.federate(changedMBeanName, DiskStoreMXBean.class, true);

    Notification notification = new Notification(
        JMXNotificationType.DISK_STORE_CREATED, memberSource,
        SequenceNumber.next(), System.currentTimeMillis(),
        ManagementConstants.DISK_STORE_CREATED_PREFIX + disk.getName());
    memberLevelNotifEmitter.sendNotification(notification);
    memberMBeanBridge.addDiskStore(disk);
  }


  /**
   * Handles LockService Creation
   * 
   * @param lockService
   */
  public void handleLockServiceCreation(DLockService lockService) throws ManagementException{
    if (!isServiceInitialised("handleLockServiceCreation")) {
      return;
    }
    /** Internal Locks Should not be exposed to client for monitoring**/
    if(internalLocks.contains(lockService.getName())){
      return;
    }
    LockServiceMBeanBridge bridge = new LockServiceMBeanBridge(lockService);
    LockServiceMXBean lockServiceMBean = new LockServiceMBean(bridge);
    
    ObjectName lockServiceMBeanName = MBeanJMXAdapter.getLockServiceMBeanName(
        cacheImpl.getDistributedSystem().getDistributedMember(), lockService.getName());
    
    ObjectName changedMBeanName = service.registerInternalMBean(
        lockServiceMBean, lockServiceMBeanName);

    service.federate(changedMBeanName, LockServiceMXBean.class, true);

    Notification notification = new Notification(
        JMXNotificationType.LOCK_SERVICE_CREATED, memberSource,
        SequenceNumber.next(), System.currentTimeMillis(),
        ManagementConstants.LOCK_SERVICE_CREATED_PREFIX + lockService.getName());
    memberLevelNotifEmitter.sendNotification(notification);
    
    memberMBeanBridge.addLockServiceStats(lockService);
  }

  /**
   * Handles GatewaySender creation
   * 
   * @param sender
   *          the specific gateway sender
   * @throws ManagementException
   */
  public void handleGatewaySenderCreation(GatewaySender sender)
      throws ManagementException {
    if (!isServiceInitialised("handleGatewaySenderCreation")) {
      return;
    }
    GatewaySenderMBeanBridge bridge = new GatewaySenderMBeanBridge(sender);

    GatewaySenderMXBean senderMBean = new GatewaySenderMBean(bridge);
    ObjectName senderObjectName = MBeanJMXAdapter.getGatewaySenderMBeanName(
        cacheImpl.getDistributedSystem().getDistributedMember(), sender.getId());

    ObjectName changedMBeanName = service.registerInternalMBean(senderMBean,
        senderObjectName);

    service.federate(changedMBeanName, GatewaySenderMXBean.class, true);

    Notification notification = new Notification(
        JMXNotificationType.GATEWAY_SENDER_CREATED, memberSource,
        SequenceNumber.next(), System.currentTimeMillis(),
        ManagementConstants.GATEWAY_SENDER_CREATED_PREFIX);
    memberLevelNotifEmitter.sendNotification(notification);

  }

  /**
   * Handles Gateway receiver creation
   * 
   * @param recv
   *          specific gateway receiver
   * @throws ManagementException
   */
  public void handleGatewayReceiverCreate(GatewayReceiver recv)
      throws ManagementException {
    if (!isServiceInitialised("handleGatewayReceiverCreate")) {
      return;
    }
    if(!recv.isManualStart()){
      return;
    }
    
    createGatewayReceiverMBean(recv);
   
  }
  
  private void createGatewayReceiverMBean(GatewayReceiver recv){
    GatewayReceiverMBeanBridge bridge = new GatewayReceiverMBeanBridge(recv);

    GatewayReceiverMXBean receiverMBean = new GatewayReceiverMBean(bridge);
    ObjectName recvObjectName = MBeanJMXAdapter
        .getGatewayReceiverMBeanName(cacheImpl.getDistributedSystem()
            .getDistributedMember());

    ObjectName changedMBeanName = service.registerInternalMBean(receiverMBean,
        recvObjectName);

    service.federate(changedMBeanName, GatewayReceiverMXBean.class, true);

    Notification notification = new Notification(
        JMXNotificationType.GATEWAY_RECEIVER_CREATED, memberSource,
        SequenceNumber.next(), System.currentTimeMillis(),
        ManagementConstants.GATEWAY_RECEIVER_CREATED_PREFIX);
    memberLevelNotifEmitter.sendNotification(notification);

  }
  
  
  /**
   * Handles Gateway receiver creation
   * 
   * @param recv
   *          specific gateway receiver
   * @throws ManagementException
   */
  public void handleGatewayReceiverStart(GatewayReceiver recv)
      throws ManagementException {
    if (!isServiceInitialised("handleGatewayReceiverStart")) {
      return;
    }
    
    if(!recv.isManualStart()){
      createGatewayReceiverMBean(recv);
    }
    
    GatewayReceiverMBean mbean = (GatewayReceiverMBean)service.getLocalGatewayReceiverMXBean();
    GatewayReceiverMBeanBridge bridge = mbean.getBridge();
    
    bridge.startServer();

    Notification notification = new Notification(
        JMXNotificationType.GATEWAY_RECEIVER_STARTED, memberSource,
        SequenceNumber.next(), System.currentTimeMillis(),
        ManagementConstants.GATEWAY_RECEIVER_STARTED_PREFIX);
    memberLevelNotifEmitter.sendNotification(notification);

  }
  
  
  /**
   * Handles Gateway receiver creation
   * 
   * @param recv
   *          specific gateway receiver
   * @throws ManagementException
   */
  public void handleGatewayReceiverStop(GatewayReceiver recv)
      throws ManagementException {
    if (!isServiceInitialised("handleGatewayReceiverStop")) {
      return;
    }
    GatewayReceiverMBean mbean = (GatewayReceiverMBean)service.getLocalGatewayReceiverMXBean();
    GatewayReceiverMBeanBridge bridge = mbean.getBridge();
    
    bridge.stopServer();
      
    Notification notification = new Notification(
        JMXNotificationType.GATEWAY_RECEIVER_STOPPED, memberSource,
        SequenceNumber.next(), System.currentTimeMillis(),
        ManagementConstants.GATEWAY_RECEIVER_STOPPED_PREFIX);
    memberLevelNotifEmitter.sendNotification(notification);

  }

  public void handleAsyncEventQueueCreation(AsyncEventQueue queue)
      throws ManagementException {
    if (!isServiceInitialised("handleAsyncEventQueueCreation")) {
      return;
    }
    AsyncEventQueueMBeanBridge bridge = new AsyncEventQueueMBeanBridge(queue);
    AsyncEventQueueMXBean queueMBean = new AsyncEventQueueMBean(bridge);
    ObjectName senderObjectName = MBeanJMXAdapter.getAsycnEventQueueMBeanName(
        cacheImpl.getDistributedSystem().getDistributedMember(), queue.getId());

    ObjectName changedMBeanName = service.registerInternalMBean(queueMBean,
        senderObjectName);

    service.federate(changedMBeanName, AsyncEventQueueMXBean.class, true);

    Notification notification = new Notification(
        JMXNotificationType.ASYNC_EVENT_QUEUE_CREATED, memberSource,
        SequenceNumber.next(), System.currentTimeMillis(),
        ManagementConstants.ASYNC_EVENT_QUEUE_CREATED_PREFIX);
    memberLevelNotifEmitter.sendNotification(notification);
  }
  
  /**
   * Sends the alert with the Object source as member. This notification will
   * get filtered out for particular alert level
   * 
   * @param details
   */
  public void handleSystemNotification(AlertDetails details) {
    if (!isServiceInitialised("handleSystemNotification")) {
      return;
    }
    if (service.isManager()) {
      String systemSource = "DistributedSystem(" + service.getDistributedSystemMXBean().getDistributedSystemId() + ")";
      Map<String, String> userData = prepareUserData(details);
      

      Notification notification = new Notification(JMXNotificationType.SYSTEM_ALERT, systemSource, SequenceNumber.next(),
          details.getMsgTime().getTime(), details.getMsg());

      notification.setUserData(userData);
      service.handleNotification(notification);
    }

  }

  private Map<String, String> prepareUserData(AlertDetails details) {
    Map<String, String> userData = new HashMap<String, String>();
    userData.put(JMXNotificationUserData.ALERT_LEVEL, AlertDetails.getAlertLevelAsString(details.getAlertLevel()));
  

    String source = details.getSource();
    userData.put(JMXNotificationUserData.THREAD, source);
    
    InternalDistributedMember sender = details.getSender();
    String nameOrId = memberSource; // TODO Rishi/Abhishek - what if sender is
    // null?
    if (sender != null) {
      nameOrId = sender.getName();
      nameOrId = nameOrId != null && !nameOrId.trim().isEmpty() ? nameOrId : sender.getId();
    }

    userData.put(JMXNotificationUserData.MEMBER, nameOrId);

    return userData;
  }

  /**
   * Assumption is its a cache server instance. For Gateway receiver there will
   * be a separate method
   * 
   * @param cacheServer
   *          cache server instance
   */
  public void handleCacheServerStart(CacheServer cacheServer) {
    if (!isServiceInitialised("handleCacheServerStart")) {
      return;
    }

    CacheServerBridge cacheServerBridge = new CacheServerBridge(cacheServer, cacheImpl);
    cacheServerBridge.setMemberMBeanBridge(memberMBeanBridge);
    
    CacheServerMBean cacheServerMBean = new CacheServerMBean(cacheServerBridge);

    ObjectName cacheServerMBeanName = MBeanJMXAdapter
        .getClientServiceMBeanName(cacheServer.getPort(), cacheImpl
            .getDistributedSystem().getDistributedMember());
    
    ObjectName changedMBeanName = service.registerInternalMBean(
        (CacheServerMXBean) cacheServerMBean, cacheServerMBeanName);
    
    ClientMembershipListener managementClientListener = new CacheServerMembershipListenerAdapter(cacheServerMBean,
        memberLevelNotifEmitter, changedMBeanName);
    ClientMembership.registerClientMembershipListener(managementClientListener);
    
    cacheServerBridge.setClientMembershipListener(managementClientListener);
    
    service.federate(changedMBeanName, CacheServerMXBean.class, true);
    
    Notification notification = new Notification(
        JMXNotificationType.CACHE_SERVER_STARTED, memberSource, SequenceNumber
            .next(), System.currentTimeMillis(),
            ManagementConstants.CACHE_SERVER_STARTED_PREFIX);

    memberLevelNotifEmitter.sendNotification(notification);
    
    memberMBeanBridge.setCacheServer(true);

  }

  /**
   * Assumption is its a cache server instance. For Gateway receiver there will
   * be a separate method
   * 
   * @param server
   *          cache server instance
   */
  public void handleCacheServerStop(CacheServer server) {
    if (!isServiceInitialised("handleCacheServerStop")) {
      return;
    }

    CacheServerMBean mbean = (CacheServerMBean) service
        .getLocalCacheServerMXBean(server.getPort());
    
    ClientMembershipListener listener = mbean.getBridge()
        .getClientMembershipListener();
    
    if(listener != null){
      ClientMembership.unregisterClientMembershipListener(listener);
    }
   

    mbean.stopMonitor();

    ObjectName cacheServerMBeanName = MBeanJMXAdapter
        .getClientServiceMBeanName(server.getPort(), cacheImpl
            .getDistributedSystem().getDistributedMember());
    service.unregisterMBean(cacheServerMBeanName);
    
    Notification notification = new Notification(
        JMXNotificationType.CACHE_SERVER_STOPPED, memberSource, SequenceNumber
            .next(), System.currentTimeMillis(),
            ManagementConstants.CACHE_SERVER_STOPPED_PREFIX);

    memberLevelNotifEmitter.sendNotification(notification);
    
    memberMBeanBridge.setCacheServer(false);
   
  }

  /**
   * Handles Cache removal. It will automatically remove all MBeans from GemFire
   * Domain
   * 
   * @param cache
   *          GemFire Cache instance. For now client cache is not supported
   */
  public void handleCacheRemoval(Cache cache) throws ManagementException {
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
    }finally{
      this.cacheImpl = null;
      this.service = null;
      this.memberMBeanBridge =null;
      this.memberBean = null;
      this.memberLevelNotifEmitter = null;
      
      
    }
  }

  private void cleanUpMonitors() {
    
    MemberMBean bean = (MemberMBean) service.getMemberMXBean();
    if (bean != null) {
      bean.stopMonitor();
    }

    Set<GatewaySender> senders = cacheImpl.getGatewaySenders();

    if (senders != null && senders.size() > 0) {
      for (GatewaySender sender : senders) {
        GatewaySenderMBean senderMBean = (GatewaySenderMBean) service
            .getLocalGatewaySenderMXBean(sender.getId());
        if (senderMBean != null) {
          senderMBean.stopMonitor();
        }
      }
    }

    GatewayReceiverMBean receiver = (GatewayReceiverMBean) service
        .getLocalGatewayReceiverMXBean();
    if (receiver != null) {
      receiver.stopMonitor();
    }
  }
  
  
  private void cleanBridgeResources() {
    List<CacheServer> servers = cacheImpl.getCacheServers();

    if (servers != null && servers.size() > 0) {
      for (CacheServer server : servers) {
        CacheServerMBean mbean = (CacheServerMBean) service
            .getLocalCacheServerMXBean(server.getPort());

        if (mbean != null) {
          ClientMembershipListener listener = mbean.getBridge()
            .getClientMembershipListener();

          if (listener != null) {
            ClientMembership.unregisterClientMembershipListener(listener);
          }
        }

      }
    }
  }

  /**
   * Handles particular region destroy or close operation it will remove the
   * corresponding MBean
   * 
   * @param region
   */
  public void handleRegionRemoval(Region region) throws ManagementException{
    if (!isServiceInitialised("handleRegionRemoval")) {
      return;
    }
    /**
     * Moved region remove operation to a guarded block. If a region is getting
     * created it wont allow it to destroy any region.
     */

    synchronized(regionOpLock){

      ObjectName regionMBeanName = MBeanJMXAdapter.getRegionMBeanName(cacheImpl
          .getDistributedSystem().getDistributedMember(), region.getFullPath());
      RegionMBean bean = null;
      try {
        bean = (RegionMBean)service.getLocalRegionMBean(region.getFullPath());
      } catch (ManagementException e) {
        // If no bean found its a NO-OP
        // Mostly for situation like DiskAccessException while creating region
        // which does a compensatory close region
        if (logger.isDebugEnabled()) {
          logger.debug(e.getMessage(), e);          
        }
        return;
      }
      
      if(bean != null){
        bean.stopMonitor();
      }
      service.unregisterMBean(regionMBeanName);

      Notification notification = new Notification(
          JMXNotificationType.REGION_CLOSED, memberSource, SequenceNumber.next(),
          System.currentTimeMillis(), ManagementConstants.REGION_CLOSED_PREFIX
              + region.getFullPath());
      memberLevelNotifEmitter.sendNotification(notification);
      memberMBeanBridge.removeRegion(region);
    }
  
  }

  /**
   * Handles DiskStore Removal
   * 
   * @param disk
   */
  public void handleDiskRemoval(DiskStore disk) throws ManagementException{
    
    if (!isServiceInitialised("handleDiskRemoval")) {
      return;
    }

    ObjectName diskStoreMBeanName = MBeanJMXAdapter.getDiskStoreMBeanName(
        cacheImpl.getDistributedSystem().getDistributedMember(), disk.getName());
    
    DiskStoreMBean bean = null;
    try {
      bean = (DiskStoreMBean)service.getLocalDiskStoreMBean(disk.getName());
      if(bean == null) {
        return ;
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

    Notification notification = new Notification(
        JMXNotificationType.DISK_STORE_CLOSED, memberSource, SequenceNumber.next(),
        System.currentTimeMillis(), ManagementConstants.DISK_STORE_CLOSED_PREFIX
            + disk.getName());
    memberLevelNotifEmitter.sendNotification(notification);
    memberMBeanBridge.removeDiskStore(disk);
  }


  /**
   * Handles Lock Service Removal
   * 
   * @param lockService
   *          lock service instance
   */
  public void handleLockServiceRemoval(DLockService lockService) throws ManagementException {
   
    if (!isServiceInitialised("handleLockServiceRemoval")) {
      return;
    }

    ObjectName lockServiceMBeanName = MBeanJMXAdapter.getLockServiceMBeanName(
        cacheImpl.getDistributedSystem().getDistributedMember(), lockService.getName());

    LockServiceMXBean bean = service.getLocalLockServiceMBean(lockService
        .getName());
    
    service.unregisterMBean(lockServiceMBeanName);

    Notification notification = new Notification(
        JMXNotificationType.LOCK_SERVICE_CLOSED, memberSource, SequenceNumber.next(),
        System.currentTimeMillis(), ManagementConstants.LOCK_SERVICE_CLOSED_PREFIX
            + lockService.getName());
    memberLevelNotifEmitter.sendNotification(notification);
  }
  
  /**
   * Handles management side call backs for a locator creation and start.
   * Assumption is a cache will be created before hand.
   * 
   * There is no corresponding handleStopLocator() method. Locator will close
   * the cache whenever its stopped and it should also shutdown all the
   * management services by closing the cache.
   * 
   * @param locator
   *          instance of locator which is getting started
   * @throws ManagementException
   */
  public void handleLocatorStart(Locator locator) throws ManagementException {
    if (!isServiceInitialised("handleLocatorCreation")) {
      return;
    }

    ObjectName locatorMBeanName = MBeanJMXAdapter.getLocatorMBeanName(cacheImpl
        .getDistributedSystem().getDistributedMember());

    LocatorMBeanBridge bridge = new LocatorMBeanBridge(locator);
    LocatorMBean locatorMBean = new LocatorMBean(bridge);

    ObjectName changedMBeanName = service.registerInternalMBean(
        (LocatorMXBean) locatorMBean, locatorMBeanName);

    service.federate(changedMBeanName, LocatorMXBean.class, true);

    Notification notification = new Notification(
        JMXNotificationType.LOCATOR_STARTED, memberSource, SequenceNumber
            .next(), System.currentTimeMillis(),
            ManagementConstants.LOCATOR_STARTED_PREFIX);

    memberLevelNotifEmitter.sendNotification(notification);

  }
  
  public void handleGatewaySenderStart(GatewaySender sender)
      throws ManagementException {
    if (!isServiceInitialised("handleGatewaySenderStart")) {
      return;
    }
    if ((sender.getRemoteDSId() < 0)) {
      return;
    }
    GatewaySenderMBean bean = (GatewaySenderMBean) service
        .getLocalGatewaySenderMXBean(sender.getId());
    
    bean.getBridge().setDispatcher();
    
    Notification notification = new Notification(
        JMXNotificationType.GATEWAY_SENDER_STARTED, memberSource,
        SequenceNumber.next(), System.currentTimeMillis(),
        ManagementConstants.GATEWAY_SENDER_STARTED_PREFIX + sender.getId());

    memberLevelNotifEmitter.sendNotification(notification);

  }

  public void handleGatewaySenderStop(GatewaySender sender)
      throws ManagementException {
    if (!isServiceInitialised("handleGatewaySenderStop")) {
      return;
    }

    Notification notification = new Notification(
        JMXNotificationType.GATEWAY_SENDER_STOPPED, memberSource,
        SequenceNumber.next(), System.currentTimeMillis(),
        ManagementConstants.GATEWAY_SENDER_STOPPED_PREFIX + sender.getId());

    memberLevelNotifEmitter.sendNotification(notification);

  }

  public void handleGatewaySenderPaused(GatewaySender sender)
      throws ManagementException {
    if (!isServiceInitialised("handleGatewaySenderPaused")) {
      return;
    }

    Notification notification = new Notification(
        JMXNotificationType.GATEWAY_SENDER_PAUSED, memberSource,
        SequenceNumber.next(), System.currentTimeMillis(),
        ManagementConstants.GATEWAY_SENDER_PAUSED_PREFIX + sender.getId());

    memberLevelNotifEmitter.sendNotification(notification);

  }

  public void handleGatewaySenderResumed(GatewaySender sender)
      throws ManagementException {
    if (!isServiceInitialised("handleGatewaySenderResumed")) {
      return;
    }

    Notification notification = new Notification(
        JMXNotificationType.GATEWAY_SENDER_RESUMED, memberSource,
        SequenceNumber.next(), System.currentTimeMillis(),
        ManagementConstants.GATEWAY_SENDER_RESUMED_PREFIX + sender.getId());

    memberLevelNotifEmitter.sendNotification(notification);

  }

  
  /**
   * Private class which acts as a ClientMembershipListener to propagate client
   * joined/left notifications
   */

  private static class CacheServerMembershipListenerAdapter extends
      ClientMembershipListenerAdapter {
    
    private NotificationBroadcasterSupport serverLevelNotifEmitter;
    private NotificationBroadcasterSupport memberLevelNotifEmitter;
    
    private String serverSource;
    
    public CacheServerMembershipListenerAdapter(NotificationBroadcasterSupport serverLevelNotifEmitter,
        NotificationBroadcasterSupport memberLevelNotifEmitter, ObjectName serverSource) {
      this.serverLevelNotifEmitter = serverLevelNotifEmitter;
      this.memberLevelNotifEmitter = memberLevelNotifEmitter;
      this.serverSource = serverSource.toString();
    }

    /**
     * Invoked when a client has connected to this process or when this process
     * has connected to a CacheServer.
     */
    public void memberJoined(ClientMembershipEvent event) {
      Notification notification = new Notification(JMXNotificationType.CLIENT_JOINED, serverSource, SequenceNumber
          .next(), System.currentTimeMillis(), ManagementConstants.CLIENT_JOINED_PREFIX + event.getMemberId());
      serverLevelNotifEmitter.sendNotification(notification);
      memberLevelNotifEmitter.sendNotification(notification);

    }

    /**
     * Invoked when a client has gracefully disconnected from this process or
     * when this process has gracefully disconnected from a CacheServer.
     */
    public void memberLeft(ClientMembershipEvent event) {
      Notification notification = new Notification(JMXNotificationType.CLIENT_LEFT, serverSource, SequenceNumber
          .next(), System.currentTimeMillis(), ManagementConstants.CLIENT_LEFT_PREFIX + event.getMemberId());
      serverLevelNotifEmitter.sendNotification(notification);
      memberLevelNotifEmitter.sendNotification(notification);
    }

    /**
     * Invoked when a client has unexpectedly disconnected from this process or
     * when this process has unexpectedly disconnected from a CacheServer.
     */
    public void memberCrashed(ClientMembershipEvent event) {
      Notification notification = new Notification(JMXNotificationType.CLIENT_CRASHED, serverSource, SequenceNumber
          .next(), System.currentTimeMillis(), ManagementConstants.CLIENT_CRASHED_PREFIX + event.getMemberId());
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