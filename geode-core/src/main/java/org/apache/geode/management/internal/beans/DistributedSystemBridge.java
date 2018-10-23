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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import javax.management.InstanceNotFoundException;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanServer;
import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;
import javax.management.NotificationListener;
import javax.management.ObjectName;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.persistence.PersistentID;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.admin.remote.MissingPersistentIDsRequest;
import org.apache.geode.internal.admin.remote.PrepareRevokePersistentIDRequest;
import org.apache.geode.internal.admin.remote.RevokePersistentIDRequest;
import org.apache.geode.internal.admin.remote.ShutdownAllRequest;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.backup.BackupOperation;
import org.apache.geode.internal.cache.persistence.PersistentMemberPattern;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.BackupStatus;
import org.apache.geode.management.CacheServerMXBean;
import org.apache.geode.management.DiskBackupStatus;
import org.apache.geode.management.DiskMetrics;
import org.apache.geode.management.DiskStoreMXBean;
import org.apache.geode.management.DistributedLockServiceMXBean;
import org.apache.geode.management.DistributedRegionMXBean;
import org.apache.geode.management.DistributedSystemMXBean;
import org.apache.geode.management.GatewayReceiverMXBean;
import org.apache.geode.management.GatewaySenderMXBean;
import org.apache.geode.management.GemFireProperties;
import org.apache.geode.management.JMXNotificationType;
import org.apache.geode.management.JVMMetrics;
import org.apache.geode.management.LockServiceMXBean;
import org.apache.geode.management.ManagementException;
import org.apache.geode.management.MemberMXBean;
import org.apache.geode.management.NetworkMetrics;
import org.apache.geode.management.OSMetrics;
import org.apache.geode.management.PersistentMemberDetails;
import org.apache.geode.management.RegionMXBean;
import org.apache.geode.management.internal.DiskBackupStatusImpl;
import org.apache.geode.management.internal.FederationComponent;
import org.apache.geode.management.internal.MBeanJMXAdapter;
import org.apache.geode.management.internal.ManagementConstants;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.management.internal.beans.stats.GatewayReceiverClusterStatsMonitor;
import org.apache.geode.management.internal.beans.stats.GatewaySenderClusterStatsMonitor;
import org.apache.geode.management.internal.beans.stats.MemberClusterStatsMonitor;
import org.apache.geode.management.internal.beans.stats.ServerClusterStatsMonitor;
import org.apache.geode.management.internal.cli.json.TypedJson;

/**
 * This is the gateway to distributed system as a whole. Aggregated metrics and stats are shown
 * which collects data from all the available proxies.
 *
 * It also include information of member on which it is hosted.
 *
 * Operation strategy is not fixed. Some of the operations operate on local proxies. Some uses admin
 * messaging for distributed message.
 */
public class DistributedSystemBridge {

  private static final Logger logger = LogService.getLogger();

  /**
   * Map of the member proxies
   */
  private Map<ObjectName, MemberMXBean> mapOfMembers;

  /**
   * Map of cache server proxies
   */
  private Map<ObjectName, CacheServerMXBean> mapOfServers;

  /**
   * Map of Gateway Sender proxies
   */
  private Map<ObjectName, GatewaySenderMXBean> mapOfGatewaySenders;

  /**
   * Map of Gateway Receiver proxies
   */
  private Map<ObjectName, GatewayReceiverMXBean> mapOfGatewayReceivers;

  /**
   * Member Proxy set size
   */
  private volatile int memberSetSize;

  /**
   * Server Proxy set size
   */
  private volatile int serverSetSize;

  /**
   * Gateway Sender Proxy set size
   */
  private volatile int gatewaySenderSetSize;

  /**
   * Gateway Receiver Proxy set size
   */
  private volatile int gatewayReceiverSetSize;

  /**
   * Cache instance
   */
  private InternalCache cache;

  /**
   * private instance of SystemManagementService
   */
  private SystemManagementService service;

  /**
   * Internal distributed system
   */
  private InternalDistributedSystem system;

  /**
   * distributed-system-id of this DS.
   */
  private int distributedSystemId;

  /**
   * Distribution manager
   */
  private DistributionManager dm;

  private String alertLevel;

  private ObjectName thisMemberName;


  private Map<ObjectName, DistributedRegionBridge> distrRegionMap;

  private Map<ObjectName, DistributedLockServiceBridge> distrLockServiceMap;

  private MemberClusterStatsMonitor memberMBeanMonitor;

  private ServerClusterStatsMonitor serverMBeanMonitor;

  private GatewaySenderClusterStatsMonitor senderMonitor;

  private GatewayReceiverClusterStatsMonitor receiverMonitor;

  /**
   * Distributed System level listener to listen on all the member level notifications It will then
   * send the notification up the JMX layer in the name of DistributedSystemMBean.
   */
  private DistributedSystemNotifListener distListener;

  /**
   * Static reference to the platform mbean server
   */
  private static MBeanServer mbeanServer = MBeanJMXAdapter.mbeanServer;

  /**
   * emitter is a helper class for sending notifications on behalf of the MemberMBean
   **/
  private NotificationBroadcasterSupport systemLevelNotifEmitter;

  /**
   * Number of rows queryData operation will return. By default it will be 1000
   */
  private int queryResultSetLimit = ManagementConstants.DEFAULT_QUERY_LIMIT;

  /**
   * NUmber of elements to be shown in queryData operation if query results contain collections like
   * Map, List etc.
   */
  private int queryCollectionsDepth = TypedJson.DEFAULT_COLLECTION_ELEMENT_LIMIT;

  /**
   * Helper method to get a member bean reference given a member name or id
   *
   * @param member name or id of the member
   * @return the proxy reference
   */
  protected MemberMXBean getProxyByMemberNameOrId(String member) {
    try {
      ObjectName objectName = MBeanJMXAdapter.getMemberMBeanName(member);
      return mapOfMembers.get(objectName);
    } catch (ManagementException mx) {
      return null;
    }
  }

  /**
   * Constructor to create a distributed system bridge
   *
   * @param service Management service
   */
  public DistributedSystemBridge(SystemManagementService service, InternalCache cache) {
    this.distrLockServiceMap = new ConcurrentHashMap<>();
    this.distrRegionMap = new ConcurrentHashMap<>();
    this.mapOfMembers = new ConcurrentHashMap<>();
    this.mapOfServers = new ConcurrentHashMap<>();
    this.mapOfGatewayReceivers = new ConcurrentHashMap<>();
    this.mapOfGatewaySenders = new ConcurrentHashMap<>();
    this.service = service;
    this.cache = cache;
    this.system = cache.getInternalDistributedSystem();
    this.dm = system.getDistributionManager();
    this.alertLevel = ManagementConstants.DEFAULT_ALERT_LEVEL;
    this.thisMemberName = MBeanJMXAdapter.getMemberMBeanName(system.getDistributedMember());

    this.distributedSystemId = this.system.getConfig().getDistributedSystemId();

    initClusterMonitors();
  }

  private void initClusterMonitors() {
    this.memberMBeanMonitor = new MemberClusterStatsMonitor();
    this.serverMBeanMonitor = new ServerClusterStatsMonitor();
    this.senderMonitor = new GatewaySenderClusterStatsMonitor();
    this.receiverMonitor = new GatewayReceiverClusterStatsMonitor();
  }

  public InternalCache getCache() {
    return cache;
  }

  /**
   * Add a proxy to the map to be used by bridge.
   *
   * @param objectName object name of the proxy
   * @param proxy actual proxy instance
   */
  public void addMemberToSystem(ObjectName objectName, MemberMXBean proxy,
      FederationComponent newState) {

    if (objectName.equals(thisMemberName)) {
      ObjectName distrObjectName = MBeanJMXAdapter.getDistributedSystemName();
      DistributedSystemMXBean systemMBean = new DistributedSystemMBean(this);
      service.registerInternalMBean(systemMBean, distrObjectName);
      this.systemLevelNotifEmitter = (DistributedSystemMBean) service.getDistributedSystemMXBean();
      this.distListener = new DistributedSystemNotifListener();
    }

    if (mapOfMembers != null) {
      mapOfMembers.put(objectName, proxy);
      memberSetSize = mapOfMembers.values().size();

    }
    updateMember(objectName, newState, null);

    try {
      mbeanServer.addNotificationListener(objectName, distListener, null, null);
    } catch (InstanceNotFoundException e) {

      if (logger.isDebugEnabled()) {
        logger.debug(e.getMessage());
      }

      logger.info("{} Instance Not Found in Platform MBean Server", objectName);
    }
  }

  public void updateMember(ObjectName objectName, FederationComponent newState,
      FederationComponent oldState) {
    memberMBeanMonitor.aggregate(newState, oldState);
  }

  public void updateCacheServer(ObjectName objectName, FederationComponent newState,
      FederationComponent oldState) {
    serverMBeanMonitor.aggregate(newState, oldState);
  }

  public void updateGatewaySender(ObjectName objectName, FederationComponent newState,
      FederationComponent oldState) {
    senderMonitor.aggregate(newState, oldState);
  }

  public void updateGatewayReceiver(ObjectName objectName, FederationComponent newState,
      FederationComponent oldState) {
    receiverMonitor.aggregate(newState, oldState);
  }

  /**
   * Removed the proxy from the map.
   *
   * @param objectName name of the proxy to be removed.
   * @param proxy actual reference to the proxy object
   * @return whether all proxies have been removed or not. In this case it will always be false.
   *         Kept it for consistency for MBeanAggregator.
   */
  public boolean removeMemberFromSystem(ObjectName objectName, MemberMXBean proxy,
      FederationComponent oldState) {

    if (thisMemberName.equals(objectName)) {
      ObjectName distrObjectName = MBeanJMXAdapter.getDistributedSystemName();
      service.unregisterMBean(distrObjectName);
    }

    if (mapOfMembers != null) {
      mapOfMembers.remove(objectName);
      memberSetSize = mapOfMembers.values().size();
      if (mapOfMembers.values().size() == 0) {
        memberSetSize = 0;
        return true;
      }
    }
    updateMember(objectName, null, oldState);

    try {
      mbeanServer.removeNotificationListener(objectName, distListener);
    } catch (ListenerNotFoundException e) {
      logger.info("Listener Not Found For MBean : {}", objectName);
      if (logger.isDebugEnabled()) {
        logger.debug(e.getMessage(), e);
      }
    } catch (InstanceNotFoundException e) {
      logger.info("{} Instance Not Found in Platform MBean Server", objectName);
      if (logger.isDebugEnabled()) {
        logger.debug(e.getMessage(), e);
      }
    }

    return false;
  }

  /**
   * Add a proxy to the map to be used by bridge.
   *
   * @param objectName object name of the proxy
   * @param proxy actual proxy instance
   */
  public void addServerToSystem(ObjectName objectName, CacheServerMXBean proxy,
      FederationComponent newState) {
    if (mapOfServers != null) {
      mapOfServers.put(objectName, proxy);
      serverSetSize = mapOfServers.values().size();
    }
    updateCacheServer(objectName, newState, null);
  }

  /**
   * Removed the proxy from the map.
   *
   * @param objectName name of the proxy to be removed.
   * @param proxy actual reference to the proxy object
   * @return whether all proxies have been removed or not. In this case it will always be false.
   *         Kept it for consistency for MBeanAggregator.
   */
  public boolean removeServerFromSystem(ObjectName objectName, CacheServerMXBean proxy,
      FederationComponent oldState) {
    if (mapOfServers != null) {
      mapOfServers.remove(objectName);
      serverSetSize = mapOfServers.values().size();
      if (mapOfServers.values().size() == 0) {
        serverSetSize = 0;
        return true;
      }
    }
    updateCacheServer(objectName, null, oldState);
    return false;
  }

  /**
   * Add a proxy to the map to be used by bridge.
   *
   * @param objectName object name of the proxy
   * @param proxy actual proxy instance
   */
  public void addGatewaySenderToSystem(ObjectName objectName, GatewaySenderMXBean proxy,
      FederationComponent newState) {
    if (mapOfGatewaySenders != null) {
      mapOfGatewaySenders.put(objectName, proxy);
      gatewaySenderSetSize = mapOfGatewaySenders.values().size();
    }
    updateGatewaySender(objectName, newState, null);
  }

  /**
   * Removed the proxy from the map.
   *
   * @param objectName name of the proxy to be removed.
   * @param proxy actual reference to the proxy object
   * @return whether all proxies have been removed or not. In this case it will always be false.
   *         Kept it for consistency for MBeanAggregator.
   */
  public boolean removeGatewaySenderFromSystem(ObjectName objectName, GatewaySenderMXBean proxy,
      FederationComponent oldState) {
    if (mapOfGatewaySenders != null) {
      mapOfGatewaySenders.remove(objectName);
      gatewaySenderSetSize = mapOfGatewaySenders.values().size();
      if (mapOfGatewaySenders.values().size() == 0) {
        gatewaySenderSetSize = 0;
        return true;
      }
    }
    updateGatewaySender(objectName, null, oldState);
    return false;
  }

  /**
   * Add a proxy to the map to be used by bridge.
   *
   * @param objectName object name of the proxy
   * @param proxy actual proxy instance
   */
  public void addGatewayReceiverToSystem(ObjectName objectName, GatewayReceiverMXBean proxy,
      FederationComponent newState) {
    if (mapOfGatewayReceivers != null) {
      mapOfGatewayReceivers.put(objectName, proxy);
      gatewayReceiverSetSize = mapOfGatewayReceivers.values().size();
    }
    updateGatewayReceiver(objectName, newState, null);
  }

  /**
   * Removed the proxy from the map.
   *
   * @param objectName name of the proxy to be removed.
   * @param proxy actual reference to the proxy object
   * @return whether all proxies have been removed or not. In this case it will always be false.
   *         Kept it for consistency for MBeanAggregator.
   */
  public boolean removeGatewayReceiverFromSystem(ObjectName objectName, GatewayReceiverMXBean proxy,
      FederationComponent oldState) {
    if (mapOfGatewayReceivers != null) {
      mapOfGatewayReceivers.remove(objectName);
      gatewayReceiverSetSize = mapOfGatewayReceivers.values().size();
      if (mapOfGatewayReceivers.values().size() == 0) {
        gatewayReceiverSetSize = 0;
        return true;
      }
    }
    updateGatewayReceiver(objectName, null, oldState);
    return false;
  }

  /**
   *
   * @param targetDirPath path of the directory where the back up files should be placed.
   * @param baselineDirPath path of the directory for baseline backup.
   * @return open type DiskBackupStatus containing each member wise disk back up status
   */
  public DiskBackupStatus backupAllMembers(String targetDirPath, String baselineDirPath) {
    BackupStatus result =
        new BackupOperation(dm, dm.getCache()).backupAllMembers(targetDirPath, baselineDirPath);
    DiskBackupStatusImpl diskBackupStatus = new DiskBackupStatusImpl();
    diskBackupStatus.generateBackedUpDiskStores(result.getBackedUpDiskStores());
    diskBackupStatus.generateOfflineDiskStores(result.getOfflineDiskStores());
    return diskBackupStatus;
  }

  /**
   * @return Minimum level for alerts to be delivered to listeners. Should be one of: WARNING,
   *         ERROR, SEVERE, OFF. It is not case-sensitive.
   */
  public String getAlertLevel() {
    return alertLevel;
  }

  /**
   *
   * @return a list of receivers present in the system
   */
  public String[] listGatewayReceivers() {
    Iterator<GatewayReceiverMXBean> gatewayReceiverIterator =
        mapOfGatewayReceivers.values().iterator();
    if (gatewayReceiverIterator != null) {
      List<String> listOfReceivers = new ArrayList<>();
      while (gatewayReceiverIterator.hasNext()) {
        listOfReceivers.add(gatewayReceiverIterator.next().getBindAddress());
      }
      String[] receivers = new String[listOfReceivers.size()];
      return listOfReceivers.toArray(receivers);
    }
    return ManagementConstants.NO_DATA_STRING;
  }

  /**
   *
   * @param alertLevel Minimum level for alerts to be delivered to listeners. Should be one of:
   *        WARNING, ERROR, SEVERE, NONE. It is not case-sensitive.
   */
  public void changeAlertLevel(String alertLevel) throws Exception {
    if (alertLevel.equalsIgnoreCase("WARNING") || alertLevel.equalsIgnoreCase("ERROR")
        || alertLevel.equalsIgnoreCase("SEVERE") || alertLevel.equalsIgnoreCase("NONE")) {
      this.alertLevel = alertLevel;
      service.getFederatingManager().getMessenger().setAlertLevel(alertLevel);
    } else {
      throw new Exception("Unknown log-level \"" + alertLevel
          + "\". Valid levels are: WARNING, ERROR, SEVERE, NONE");
    }
  }

  /**
   * @return list of members hosting cache server
   */
  public String[] listCacheServers() {
    Iterator<MemberMXBean> memberIterator = mapOfMembers.values().iterator();
    if (memberIterator != null) {

      List<String> listOfServer = new ArrayList<>();
      while (memberIterator.hasNext()) {
        MemberMXBean bean = memberIterator.next();
        if (bean.isCacheServer()) {
          listOfServer.add(bean.getMember());
        }
      }
      String[] members = new String[listOfServer.size()];
      return listOfServer.toArray(members);
    }
    return ManagementConstants.NO_DATA_STRING;
  }

  /**
   * @return list of members hosting servers, which are started from GFSH
   */
  public String[] listServers() {
    Iterator<MemberMXBean> memberIterator = mapOfMembers.values().iterator();
    if (memberIterator != null) {

      List<String> listOfServer = new ArrayList<>();
      while (memberIterator.hasNext()) {
        MemberMXBean bean = memberIterator.next();
        if (bean.isServer()) {
          listOfServer.add(bean.getMember());
        }
      }
      String[] members = new String[listOfServer.size()];
      return listOfServer.toArray(members);
    }
    return ManagementConstants.NO_DATA_STRING;

  }

  public DiskMetrics showDiskMetrics(String member) throws Exception {
    MemberMXBean bean = validateMember(member);

    DiskMetrics dm = new DiskMetrics();

    dm.setDiskReadsRate(bean.getDiskReadsRate());
    dm.setDiskWritesRate(bean.getDiskWritesRate());
    dm.setTotalBackupInProgress(bean.getTotalBackupInProgress());
    dm.setTotalBackupCompleted(bean.getTotalBackupCompleted());
    dm.setDiskFlushAvgLatency(bean.getDiskFlushAvgLatency());
    dm.setTotalBytesOnDisk(bean.getTotalDiskUsage());
    return dm;
  }

  /**
   * @return a list of Gateway Senders
   */
  public String[] listGatewaySenders() {
    Iterator<GatewaySenderMXBean> gatewaySenderIterator = mapOfGatewaySenders.values().iterator();
    if (gatewaySenderIterator != null) {
      List<String> listOfSenders = new ArrayList<>();
      while (gatewaySenderIterator.hasNext()) {
        listOfSenders.add(gatewaySenderIterator.next().getSenderId());
      }
      String[] senders = new String[listOfSenders.size()];
      return listOfSenders.toArray(senders);
    }
    return ManagementConstants.NO_DATA_STRING;
  }

  /**
   * Requests the corresponding member to provide the basic JVM metrics which are listed in class
   * JVMMetrics to be sent to Managing node.
   *
   * @param member name or id of the member
   * @return basic metrics related to the JVM of the member
   */
  public JVMMetrics showJVMMetrics(String member) throws Exception {
    MemberMXBean bean = validateMember(member);
    return bean.showJVMMetrics();
  }

  private MemberMXBean validateMember(String member) throws Exception {
    ObjectName objectName = MBeanJMXAdapter.getMemberMBeanName(member);
    MemberMXBean bean = mapOfMembers.get(objectName);

    if (bean != null) {
      return bean;
    } else {
      throw new Exception(
          String.format("%s is an invalid member name or Id. Current members are %s", member,
              mapOfMembers.keySet()));
    }
  }

  /**
   * @return Total number of locators for this DS
   */
  public int getLocatorCount() {
    if (cache != null) {
      return listLocators().length;
    }
    return 0;
  }

  /**
   * @return the list of all locators present in the system
   */
  public String[] listLocators() {
    if (cache != null) {
      // each locator is a string of the form host[port] or bind-addr[port]
      Set<String> set = new HashSet<>();
      Map<InternalDistributedMember, Collection<String>> map =
          cache.getDistributionManager().getAllHostedLocators();

      for (Collection<String> hostedLocators : map.values()) {
        set.addAll(hostedLocators);
      }

      return set.toArray(new String[set.size()]);
    }
    return ManagementConstants.NO_DATA_STRING;
  }

  /**
   * @param member name or id of the member
   * @return GemFire configuration data
   */
  public GemFireProperties fetchMemberConfiguration(String member) throws Exception {
    MemberMXBean bean = validateMember(member);
    return bean.listGemFireProperties();
  }

  /**
   * @return Distributed System Id of this GemFire Distributed System
   */
  public int getDistributedSystemId() {
    return distributedSystemId;
  }

  /**
   * @return Total number of members for this DS
   */
  public int getMemberCount() {
    return memberSetSize;
  }

  /**
   * @return Lists all the members disk stores
   */
  public Map<String, String[]> getMemberDiskstoreMap() {
    Iterator<MemberMXBean> memberIterator = mapOfMembers.values().iterator();
    if (memberIterator != null) {

      Map<String, String[]> mapOfDisks = new HashMap<>();
      while (memberIterator.hasNext()) {
        MemberMXBean bean = memberIterator.next();
        mapOfDisks.put(bean.getMember(), bean.getDiskStores());
      }

      return mapOfDisks;
    }
    return Collections.emptyMap();
  }

  /**
   * @param member name or id of the member
   * @return for how long the member is up.
   */
  public long getMemberUpTime(String member) throws Exception {
    MemberMXBean bean = validateMember(member);
    return bean.getMemberUpTime();
  }

  /**
   * @return list of members visible to the Managing node and which can be manageable.
   */
  public String[] getMembers() {
    Iterator<MemberMXBean> memberIterator = mapOfMembers.values().iterator();

    if (memberIterator != null) {
      String[] members = new String[memberSetSize];
      int i = 0;
      while (memberIterator.hasNext()) {
        members[i] = memberIterator.next().getMember();
        i++;
      }
      return members;
    }

    return ManagementConstants.NO_DATA_STRING;
  }

  public String[] listLocatorMembers(boolean onlyStandAloneLocators) {
    String[] locatorMembers = ManagementConstants.NO_DATA_STRING;

    if (onlyStandAloneLocators) {
      locatorMembers = listStandAloneLocatorMembers();
    } else {
      Iterator<MemberMXBean> memberIterator = mapOfMembers.values().iterator();

      if (memberIterator != null) {
        Set<String> locatorMemberSet = new TreeSet<>();
        while (memberIterator.hasNext()) {
          MemberMXBean memberMxBean = memberIterator.next();
          if (memberMxBean.isLocator()) {
            locatorMemberSet.add(memberMxBean.getMember());
          }
        }
        if (!locatorMemberSet.isEmpty()) {
          locatorMembers = locatorMemberSet.toArray(locatorMembers);
        }
      }
    }

    return locatorMembers;
  }

  private String[] listStandAloneLocatorMembers() {
    String[] locatorMembers = ManagementConstants.NO_DATA_STRING;

    Set<DistributedMember> members = new HashSet<>();
    members.add(system.getDistributedMember());
    members.addAll(system.getAllOtherMembers());

    if (!members.isEmpty()) {
      Set<String> locatorMemberSet = new TreeSet<>();
      for (DistributedMember member : members) {
        if (ClusterDistributionManager.LOCATOR_DM_TYPE == ((InternalDistributedMember) member)
            .getVmKind()) {
          String name = member.getName();
          name = StringUtils.isNotBlank(name) ? name : member.getId();
          locatorMemberSet.add(name);
        }
      }
      locatorMembers = locatorMemberSet.toArray(locatorMembers);
      members.clear();
      locatorMemberSet.clear();
    }

    return locatorMembers;
  }

  /**
   * @return list of groups visible to the Manager node
   */
  public String[] getGroups() {
    String[] groups = new String[0];
    Collection<MemberMXBean> values = mapOfMembers.values();

    if (values != null) {
      Set<String> groupSet = new TreeSet<>();
      for (MemberMXBean memberMXBean : values) {
        String[] memberGroups = memberMXBean.getGroups();
        if (memberGroups != null && memberGroups.length != 0) {
          groupSet.addAll(Arrays.asList(memberGroups));
        }
      }
      if (!groupSet.isEmpty()) {
        groups = new String[groupSet.size()];
        groups = groupSet.toArray(groups);
      }
    }

    return groups;
  }

  public NetworkMetrics showNetworkMetric(String member) throws Exception {
    MemberMXBean bean = validateMember(member);
    NetworkMetrics nm = new NetworkMetrics();
    nm.setBytesReceivedRate(bean.getBytesReceivedRate());
    nm.setBytesSentRate(bean.getBytesSentRate());
    return nm;
  }

  /**
   * @param member name or id of the member
   * @return basic Operating metrics for a given member.
   */
  public OSMetrics showOSMetrics(String member) throws Exception {
    MemberMXBean bean = validateMember(member);
    return bean.showOSMetrics();
  }

  /**
   * @return a list of region names hosted on the system
   */
  public String[] listAllRegions() {
    Iterator<DistributedRegionBridge> it = distrRegionMap.values().iterator();
    if (distrRegionMap.values().size() == 0) {
      return ManagementConstants.NO_DATA_STRING;
    }
    String[] listOfRegions = new String[distrRegionMap.values().size()];
    int j = 0;
    while (it.hasNext()) {
      DistributedRegionBridge bridge = it.next();
      listOfRegions[j] = bridge.getName();
      j++;
    }
    return listOfRegions;
  }

  /**
   * @return a list of region names hosted on the system
   */
  public String[] listAllRegionPaths() {
    if (distrRegionMap.values().size() == 0) {
      return ManagementConstants.NO_DATA_STRING;
    }
    // Sort region paths
    SortedSet<String> regionPathsSet = new TreeSet<>();
    for (DistributedRegionBridge bridge : distrRegionMap.values()) {
      regionPathsSet.add(bridge.getFullPath());
    }
    String[] regionPaths = new String[regionPathsSet.size()];
    regionPaths = regionPathsSet.toArray(regionPaths);
    regionPathsSet.clear();

    return regionPaths;
  }


  /**
   * @return the set of members successfully shutdown
   */
  @SuppressWarnings("unchecked")
  public String[] shutDownAllMembers() throws Exception {
    try {
      DistributionManager dm = cache.getDistributionManager();
      Set<InternalDistributedMember> members = ShutdownAllRequest.send(dm, 0);
      String[] shutDownMembers = new String[members.size()];
      int j = 0;
      for (InternalDistributedMember member : members) {
        shutDownMembers[j] = member.getId();
        j++;
      }
      return shutDownMembers;
    } catch (Exception e) {
      throw new Exception(e.getLocalizedMessage());
    }
  }

  /**
   * In case of replicated region during recovery all region recovery will wait till all the
   * replicated region member are up and running so that the recovered data from the disk will be in
   * sync;
   *
   * @return Array of PersistentMemberDetails (which contains host, directory and disk store id)
   */
  public PersistentMemberDetails[] listMissingDiskStores() {
    PersistentMemberDetails[] missingDiskStores = null;

    // No need to try and send anything if we're a Loner
    if (dm.isLoner()) {
      return missingDiskStores;
    }

    Set<PersistentID> persistentMemberSet = MissingPersistentIDsRequest.send(dm);
    if (persistentMemberSet != null && persistentMemberSet.size() > 0) {
      missingDiskStores = new PersistentMemberDetails[persistentMemberSet.size()];
      int j = 0;
      for (PersistentID id : persistentMemberSet) {
        missingDiskStores[j] = new PersistentMemberDetails(id.getHost().getCanonicalHostName(),
            id.getDirectory(), id.getUUID().toString());
        j++;
      }
    }

    return missingDiskStores;
  }

  /**
   * Revokes or ignores the missing diskStore for which the region Initialization is stopped
   *
   * @param diskStoreId UUID of the disk store to revoke
   * @return successful or failure
   */
  public boolean revokeMissingDiskStores(final String diskStoreId) {
    // make sure that the disk store we're revoking is actually missing
    boolean found = false;
    PersistentMemberDetails[] details = listMissingDiskStores();
    if (details != null) {
      for (PersistentMemberDetails member : details) {
        if (member.getDiskStoreId().equalsIgnoreCase(diskStoreId)) {
          found = true;
          break;
        }
      }
    }
    if (!found) {
      return false;
    }

    PersistentMemberPattern pattern = new PersistentMemberPattern(UUID.fromString(diskStoreId));
    boolean success = false;
    try {
      PrepareRevokePersistentIDRequest.send(dm, pattern);
      success = true;
    } finally {
      if (success) {
        // revoke the persistent member if were able to prepare the revoke
        RevokePersistentIDRequest.send(dm, pattern);
      } else {
        // otherwise, cancel the revoke.
        PrepareRevokePersistentIDRequest.cancel(dm, pattern);
      }
    }
    return success;
  }

  /** Navigation APIS **/
  public ObjectName getMemberObjectName() {
    return this.thisMemberName;
  }

  public ObjectName getManagerObjectName() {
    return MBeanJMXAdapter.getManagerName();
  }

  public ObjectName fetchMemberObjectName(String member) throws Exception {
    validateMember(member);
    return MBeanJMXAdapter.getMemberMBeanName(member);
  }

  public ObjectName[] listMemberObjectNames() {
    Set<ObjectName> memberSet = mapOfMembers.keySet();
    if (memberSet != null && memberSet.size() > 0) {
      ObjectName[] memberSetArray = new ObjectName[memberSet.size()];
      return memberSet.toArray(memberSetArray);
    }
    return ManagementConstants.NO_DATA_OBJECTNAME;
  }

  public ObjectName fetchDistributedRegionObjectName(String regionPath) throws Exception {
    ObjectName distributedRegionMBeanName =
        MBeanJMXAdapter.getDistributedRegionMbeanName(regionPath);

    if (distrRegionMap.get(distributedRegionMBeanName) != null) {
      return distributedRegionMBeanName;
    } else {
      throw new Exception("DistributedRegionMBean Not Found In Distributed System");
    }
  }

  public ObjectName fetchRegionObjectName(String member, String regionPath) throws Exception {
    validateMember(member);

    ObjectName distributedRegionMBeanName =
        MBeanJMXAdapter.getDistributedRegionMbeanName(regionPath);

    if (distrRegionMap.get(distributedRegionMBeanName) != null) {
      ObjectName regionMBeanName = MBeanJMXAdapter.getRegionMBeanName(member, regionPath);
      RegionMXBean bean = service.getMBeanInstance(regionMBeanName, RegionMXBean.class);
      if (bean != null) {
        return regionMBeanName;
      }
    }
    throw new Exception("RegionMBean Not Found In Distributed System");
  }

  public ObjectName[] fetchRegionObjectNames(ObjectName memberMBeanName) throws Exception {
    List<ObjectName> list = new ArrayList<>();
    if (mapOfMembers.get(memberMBeanName) != null) {
      MemberMXBean bean = mapOfMembers.get(memberMBeanName);
      String member =
          memberMBeanName.getKeyProperty(ManagementConstants.OBJECTNAME_MEMBER_APPENDER);
      String[] regions = bean.listRegions();
      for (String region : regions) {
        ObjectName regionMBeanName = MBeanJMXAdapter.getRegionMBeanName(member, region);
        list.add(regionMBeanName);
      }
      ObjectName[] objNames = new ObjectName[list.size()];
      return list.toArray(objNames);
    } else {
      throw new Exception("Member MBean Not Found In Distributed System");
    }
  }

  public ObjectName[] listDistributedRegionObjectNames() {
    List<ObjectName> list = new ArrayList<>();
    list.addAll(distrRegionMap.keySet());
    ObjectName[] objNames = new ObjectName[list.size()];
    return list.toArray(objNames);
  }

  public ObjectName fetchCacheServerObjectName(String member, int port) throws Exception {
    validateMember(member);
    ObjectName serverName = MBeanJMXAdapter.getClientServiceMBeanName(port, member);

    CacheServerMXBean bean = service.getMBeanInstance(serverName, CacheServerMXBean.class);
    if (bean != null) {
      return serverName;
    } else {
      bean = service.getLocalCacheServerMXBean(port);
      if (bean != null) {
        return serverName;
      } else {
        throw new Exception("Cache Server MBean not Found in DS");
      }
    }
  }

  public ObjectName fetchDiskStoreObjectName(String member, String diskStore) throws Exception {
    validateMember(member);
    ObjectName diskStoreName = MBeanJMXAdapter.getDiskStoreMBeanName(member, diskStore);

    DiskStoreMXBean bean = service.getMBeanInstance(diskStoreName, DiskStoreMXBean.class);

    if (bean != null) {
      return diskStoreName;
    } else { // check for local Disk Stores
      bean = service.getLocalDiskStoreMBean(diskStore);
    }
    if (bean != null) {
      return diskStoreName;
    } else {
      throw new Exception("Disk Store MBean not Found in DS");
    }
  }

  public ObjectName fetchDistributedLockServiceObjectName(String lockServiceName) throws Exception {
    DistributedLockServiceMXBean bean = service.getDistributedLockServiceMXBean(lockServiceName);
    if (bean != null) {
      return service.getDistributedLockServiceMBeanName(lockServiceName);
    } else {
      throw new Exception(
          "Distributed Lock Service MBean not Found in DS");
    }
  }

  public ObjectName fetchGatewayReceiverObjectName(String member) throws Exception {
    validateMember(member);
    ObjectName receiverName = MBeanJMXAdapter.getGatewayReceiverMBeanName(member);
    GatewayReceiverMXBean bean =
        service.getMBeanInstance(receiverName, GatewayReceiverMXBean.class);

    if (bean != null) {
      return receiverName;
    } else {
      // check for local MBean
      bean = service.getLocalGatewayReceiverMXBean();
      if (bean != null) {
        return receiverName;
      } else {
        throw new Exception(
            "Gateway Receiver MBean not Found in DS");
      }
    }
  }

  public ObjectName fetchGatewaySenderObjectName(String member, String senderId) throws Exception {
    validateMember(member);

    ObjectName senderName = MBeanJMXAdapter.getGatewaySenderMBeanName(member, senderId);

    GatewaySenderMXBean bean = service.getMBeanInstance(senderName, GatewaySenderMXBean.class);
    if (bean != null) {
      return senderName;
    } else {
      // check for local MBean
      bean = service.getLocalGatewaySenderMXBean(senderId);
      if (bean != null) {
        return senderName;
      } else {
        throw new Exception("Gateway Sender MBean not Found in DS");
      }
    }
  }

  public ObjectName fetchLockServiceObjectName(String member, String lockService) throws Exception {
    validateMember(member);

    ObjectName lockServiceName = MBeanJMXAdapter.getLockServiceMBeanName(member, lockService);
    LockServiceMXBean bean = service.getMBeanInstance(lockServiceName, LockServiceMXBean.class);
    if (bean != null) {
      return lockServiceName;
    } else {
      // check for local MBean
      bean = service.getLocalLockServiceMBean(lockService);
      if (bean != null) {
        return lockServiceName;
      } else {
        throw new Exception("Lock Service MBean not Found in DS");
      }
    }
  }

  public ObjectName[] listCacheServerObjectNames() {
    ObjectName[] arr = new ObjectName[mapOfServers.keySet().size()];
    return mapOfServers.keySet().toArray(arr);
  }

  public ObjectName[] listGatewayReceiverObjectNames() {
    ObjectName[] arr = new ObjectName[mapOfGatewayReceivers.keySet().size()];
    return mapOfGatewayReceivers.keySet().toArray(arr);
  }

  public ObjectName[] listGatewaySenderObjectNames() {
    ObjectName[] arr = new ObjectName[mapOfGatewaySenders.keySet().size()];
    return mapOfGatewaySenders.keySet().toArray(arr);
  }

  public ObjectName[] listGatewaySenderObjectNames(String member) throws Exception {
    validateMember(member);
    DistributedMember distributedMember = BeanUtilFuncs.getDistributedMemberByNameOrId(member);

    List<ObjectName> listName = null;

    ObjectName pattern = new ObjectName(ManagementConstants.GATEWAY_SENDER_PATTERN);

    Set<ObjectName> mbeanSet = service.queryMBeanNames(distributedMember);

    if (mbeanSet != null && mbeanSet.size() > 0) {
      listName = new ArrayList<>();
      for (ObjectName name : mbeanSet) {
        if (pattern.apply(name)) {
          listName.add(name);
        }
      }
    }

    if (listName != null && listName.size() > 0) {
      ObjectName[] array = new ObjectName[listName.size()];
      return listName.toArray(array);
    }
    return ManagementConstants.NO_DATA_OBJECTNAME;
  }

  /**
   * We have to iterate through the Cache servers to get Unique Client list across system. Stats
   * will give duplicate client numbers;
   *
   * @return total number of client vm connected to the system
   */
  public int getNumClients() {
    if (mapOfServers.keySet().size() > 0) {
      Set<String> uniqueClientSet = new HashSet<>();
      for (CacheServerMXBean cacheServerMXBean : mapOfServers.values()) {
        String[] clients;
        try {
          clients = cacheServerMXBean.getClientIds();
        } catch (Exception e) {
          // Mostly due to condition where member is departed and proxy is still
          // with Manager.
          clients = null;
        }
        if (clients != null) {
          Collections.addAll(uniqueClientSet, clients);
        }
      }
      return uniqueClientSet.size();
    }
    return 0;
  }

  /**
   * @return total number of query running
   */
  public long getActiveCQCount() {
    return serverMBeanMonitor.getActiveCQCount();
  }

  /**
   * @return average query request rate
   */
  public float getQueryRequestRate() {
    return serverMBeanMonitor.getQueryRequestRate();
  }

  /**
   * @return rate of disk reads
   */
  public float getDiskReadsRate() {
    return memberMBeanMonitor.getDiskReadsRate();
  }

  /**
   * @return rate of disk writes
   */
  public float getDiskWritesRate() {
    return memberMBeanMonitor.getDiskWritesRate();
  }

  /**
   * @return disk flush avg latency
   */
  public long getDiskFlushAvgLatency() {
    return MetricsCalculator.getAverage(memberMBeanMonitor.getDiskFlushAvgLatency(), memberSetSize);
  }

  public float getGatewayReceiverCreateRequestsRate() {
    return receiverMonitor.getGatewayReceiverCreateRequestsRate();
  }

  public float getGatewayReceiverDestroyRequestsRate() {
    return receiverMonitor.getGatewayReceiverDestroyRequestsRate();
  }

  public float getGatewayReceiverUpdateRequestsRate() {
    return receiverMonitor.getGatewayReceiverUpdateRequestsRate();
  }

  /**
   * @return average events received rate across system
   */
  public float getGatewayReceiverEventsReceivedRate() {
    return receiverMonitor.getGatewayReceiverEventsReceivedRate();
  }

  /**
   * @return Average number of batches of events removed from the event queue and sent per second
   */
  public long getGatewaySenderAverageDistributionTimePerBatch() {
    return MetricsCalculator.getAverage(
        senderMonitor.getGatewaySenderAverageDistributionTimePerBatch(), gatewaySenderSetSize);
  }

  /**
   * @return average gateway sender batch dispatch rate
   */
  public float getGatewaySenderBatchesDispatchedRate() {
    return senderMonitor.getGatewaySenderBatchesDispatchedRate();
  }

  /**
   * @return event queue size
   */
  public int getGatewaySenderEventQueueSize() {
    return senderMonitor.getGatewaySenderEventQueueSize();
  }

  /**
   * @return events queued rate
   */
  public float getGatewaySenderEventsQueuedRate() {
    return senderMonitor.getGatewaySenderEventsQueuedRate();
  }

  /**
   * @return total batches redistributed
   */
  public int getGatewaySenderTotalBatchesRedistributed() {
    return senderMonitor.getGatewaySenderTotalBatchesRedistributed();
  }

  /**
   * @return total number of events conflated
   */
  public int getGatewaySenderTotalEventsConflated() {
    return senderMonitor.getGatewaySenderTotalEventsConflated();
  }

  /**
   * @return the total count of disk stores present in the system
   */
  public int getSystemDiskStoreCount() {
    return memberMBeanMonitor.getSystemDiskStoreCount();
  }

  /**
   * @return total number of disk back up going on across system
   */
  public int getTotalBackupInProgress() {
    return memberMBeanMonitor.getTotalBackupInProgress();
  }

  /**
   * @return total heap size occupied by the DS
   */
  public long getTotalHeapSize() {
    return memberMBeanMonitor.getTotalHeapSize();
  }

  public long getOffHeapFreeSize() {
    return memberMBeanMonitor.getOffHeapFreeMemory();
  }

  public long getOffHeapUsedSize() {
    return memberMBeanMonitor.getOffHeapUsedMemory();
  }

  public int getTransactionCommitted() {
    return memberMBeanMonitor.getTransactionCommitted();
  }

  public int getTransactionRolledBack() {
    return memberMBeanMonitor.getTransactionRolledBack();
  }

  /**
   * @return total hit count across DS
   */
  public int getTotalHitCount() {
    return memberMBeanMonitor.getTotalHitCount();
  }

  /**
   * @return total miss count across the system
   */
  public int getTotalMissCount() {
    return memberMBeanMonitor.getTotalMissCount();
  }

  /**
   * @return number of regions
   */
  public int getTotalRegionCount() {
    return distrRegionMap.keySet().size();
  }

  /**
   * @return total number of region entries
   */
  public long getTotalRegionEntryCount() {
    Iterator<DistributedRegionBridge> memberIterator = distrRegionMap.values().iterator();
    if (memberIterator != null) {
      long sum = 0;
      while (memberIterator.hasNext()) {
        sum = sum + memberIterator.next().getSystemRegionEntryCount();
      }
      return sum;
    }
    return 0;
  }

  /**
   * @return Number of Initial image operations that are in progress across system
   */
  public int getNumInitialImagesInProgress() {
    return memberMBeanMonitor.getNumInitialImagesInProgress();
  }

  public int getNumRunningFunctions() {
    return memberMBeanMonitor.getNumRunningFunctions();
  }

  public long getRegisteredCQCount() {
    return serverMBeanMonitor.getRegisteredCQCount();
  }

  public long getTotalDiskUsage() {
    return memberMBeanMonitor.getTotalDiskUsage();
  }

  public float getAverageReads() {
    return memberMBeanMonitor.getAverageReads();
  }

  public float getAverageWrites() {
    return memberMBeanMonitor.getAverageWrites();
  }

  public long getUsedHeapSize() {
    return memberMBeanMonitor.getUsedHeapSize();
  }

  public int getNumSubscriptions() {
    return serverMBeanMonitor.getNumSubscriptions();
  }

  public long getGarbageCollectionCount() {
    return memberMBeanMonitor.getGarbageCollectionCount();
  }

  public long getJVMPauses() {
    return memberMBeanMonitor.getJVMPauses();
  }

  public Map<String, Boolean> viewRemoteClusterStatus() {
    if (mapOfGatewaySenders.values().size() > 0) {
      Map<String, Boolean> senderMap = new HashMap<>();
      for (GatewaySenderMXBean bean : mapOfGatewaySenders.values()) {
        Integer dsId = bean.getRemoteDSId();
        if (dsId == null || dsId <= -1) {
          continue;
        }
        if (bean.isParallel()) {
          senderMap.merge(dsId.toString(), bean.isConnected(), Boolean::logicalAnd);
        } else {
          if (bean.isPrimary()) {
            senderMap.put(dsId.toString(), bean.isConnected());
          }
        }
      }
      return senderMap;
    }

    return Collections.emptyMap();
  }

  public String queryData(String query, String members, int limit) throws Exception {
    Object result = QueryDataFunction.queryData(query, members, limit, false, queryResultSetLimit,
        queryCollectionsDepth);
    return (String) result;
  }

  public byte[] queryDataForCompressedResult(String query, String members, int limit)
      throws Exception {
    Object result = QueryDataFunction.queryData(query, members, limit, true, queryResultSetLimit,
        queryCollectionsDepth);
    return (byte[]) result;
  }

  public int getQueryResultSetLimit() {
    return queryResultSetLimit;
  }

  public void setQueryResultSetLimit(int queryResultSetLimit) {
    this.queryResultSetLimit = queryResultSetLimit;
  }

  public int getQueryCollectionsDepth() {
    return queryCollectionsDepth;
  }

  public void setQueryCollectionsDepth(int queryCollectionsDepth) {
    this.queryCollectionsDepth = queryCollectionsDepth;
  }

  /**
   * User defined notification handler
   */
  private class DistributedSystemNotifListener implements NotificationListener {

    @Override
    public void handleNotification(Notification notification, Object handback) {
      notification.setSequenceNumber(SequenceNumber.next());
      systemLevelNotifEmitter.sendNotification(notification);
    }
  }

  public void sendSystemLevelNotification(Notification notification) {
    distListener.handleNotification(notification, null);
  }

  public void addRegion(ObjectName proxyName, RegionMXBean regionProxy,
      FederationComponent fedComp) {

    String fullPath = proxyName.getKeyProperty("name");
    ObjectName distributedRegionObjectName =
        MBeanJMXAdapter.getDistributedRegionMbeanNameInternal(fullPath);

    synchronized (distrRegionMap) {
      DistributedRegionBridge bridge = distrRegionMap.get(distributedRegionObjectName);
      if (bridge != null) {
        FederationComponent newObj = fedComp;
        bridge.addProxyToMap(proxyName, regionProxy, newObj);
      } else {
        FederationComponent newObj = fedComp;
        bridge = new DistributedRegionBridge(proxyName, regionProxy, newObj);
        DistributedRegionMXBean mbean = new DistributedRegionMBean(bridge);

        service.registerInternalMBean(mbean, distributedRegionObjectName);
        distrRegionMap.put(distributedRegionObjectName, bridge);
      }
    }
  }

  public void removeRegion(ObjectName proxyName, RegionMXBean regionProxy,
      FederationComponent fedComp) {

    String fullPath = proxyName.getKeyProperty("name");
    ObjectName distributedRegionObjectName =
        MBeanJMXAdapter.getDistributedRegionMbeanNameInternal(fullPath);

    synchronized (distrRegionMap) {
      if (distrRegionMap.get(distributedRegionObjectName) != null) {
        DistributedRegionBridge bridge = distrRegionMap.get(distributedRegionObjectName);
        if (bridge.removeProxyFromMap(proxyName, regionProxy, fedComp)) {
          service.unregisterMBean(distributedRegionObjectName);
          distrRegionMap.remove(distributedRegionObjectName);
        }

      } else {
        return;
      }
    }
  }

  public void updateRegion(ObjectName proxyName, FederationComponent oldValue,
      FederationComponent newValue) {

    String fullPath = proxyName.getKeyProperty("name");
    ObjectName distributedRegionObjectName =
        MBeanJMXAdapter.getDistributedRegionMbeanNameInternal(fullPath);

    DistributedRegionBridge bridge = distrRegionMap.get(distributedRegionObjectName);
    if (bridge != null) {
      FederationComponent newProxy = newValue;
      FederationComponent oldProxy = null;
      if (oldValue != null) {
        oldProxy = oldValue;
      }
      bridge.updateRegion(newProxy, oldProxy);
    }
  }

  public void addLockService(ObjectName proxyName, LockServiceMXBean lockServiceProxy,
      FederationComponent fedComp) {

    String lockServiceName = proxyName.getKeyProperty("name");

    ObjectName distributedLockObjectName =
        MBeanJMXAdapter.getDistributedLockServiceName(lockServiceName);

    synchronized (distrLockServiceMap) {
      if (distrLockServiceMap.get(distributedLockObjectName) != null) {
        DistributedLockServiceBridge bridge = distrLockServiceMap.get(distributedLockObjectName);
        bridge.addProxyToMap(proxyName, lockServiceProxy);
      } else {

        DistributedLockServiceBridge bridge =
            new DistributedLockServiceBridge(proxyName, lockServiceProxy, fedComp);
        DistributedLockServiceMXBean mbean = new DistributedLockServiceMBean(bridge);

        service.registerInternalMBean(mbean, distributedLockObjectName);
        distrLockServiceMap.put(distributedLockObjectName, bridge);
      }
    }
  }

  public void removeLockService(ObjectName proxyName, LockServiceMXBean lockServiceProxy,
      FederationComponent fedComp) {

    String lockServiceName = proxyName.getKeyProperty("name");

    ObjectName distributedLockObjectName =
        MBeanJMXAdapter.getDistributedLockServiceName(lockServiceName);

    synchronized (distrLockServiceMap) {
      if (distrLockServiceMap.get(distributedLockObjectName) != null) {
        DistributedLockServiceBridge bridge = distrLockServiceMap.get(distributedLockObjectName);

        if (bridge.removeProxyFromMap(proxyName, lockServiceProxy)) {
          service.unregisterMBean(distributedLockObjectName);
          distrLockServiceMap.remove(distributedLockObjectName);
        }

      } else {
        return;
      }
    }
  }

  public void updateLockService(ObjectName proxyName, FederationComponent oldValue,
      FederationComponent newValue) {
    // No body is calling this method right now.
    // If aggregate stats are added in Distributed Lock Service it will be
    // needed.
  }

  public void memberDeparted(InternalDistributedMember id, boolean crashed) {
    Notification notification = new Notification(JMXNotificationType.CACHE_MEMBER_DEPARTED,
        MBeanJMXAdapter.getMemberNameOrId(id), SequenceNumber.next(), System.currentTimeMillis(),
        ManagementConstants.CACHE_MEMBER_DEPARTED_PREFIX + MBeanJMXAdapter.getMemberNameOrId(id)
            + " has crashed = " + crashed);
    systemLevelNotifEmitter.sendNotification(notification);
  }

  public void memberJoined(InternalDistributedMember id) {
    Notification notification = new Notification(JMXNotificationType.CACHE_MEMBER_JOINED,
        MBeanJMXAdapter.getMemberNameOrId(id), SequenceNumber.next(), System.currentTimeMillis(),
        ManagementConstants.CACHE_MEMBER_JOINED_PREFIX + MBeanJMXAdapter.getMemberNameOrId(id));
    systemLevelNotifEmitter.sendNotification(notification);
  }

  public void memberSuspect(InternalDistributedMember id, InternalDistributedMember whoSuspected) {
    Notification notification = new Notification(JMXNotificationType.CACHE_MEMBER_SUSPECT,
        MBeanJMXAdapter.getMemberNameOrId(id), SequenceNumber.next(), System.currentTimeMillis(),
        ManagementConstants.CACHE_MEMBER_SUSPECT_PREFIX + MBeanJMXAdapter.getMemberNameOrId(id)
            + " By : " + whoSuspected.getName());
    systemLevelNotifEmitter.sendNotification(notification);
  }
}
