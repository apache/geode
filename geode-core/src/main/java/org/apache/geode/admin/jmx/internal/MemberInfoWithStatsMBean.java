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
package org.apache.geode.admin.jmx.internal;

import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_TIME_STATISTICS;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_SAMPLING_ENABLED;

import java.net.InetAddress;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.InstanceNotFoundException;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanNotificationInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanParameterInfo;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;
import javax.management.NotificationEmitter;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.ObjectName;
import javax.management.OperationsException;
import javax.management.ReflectionException;

import mx4j.AbstractDynamicMBean;
import org.apache.logging.log4j.Logger;

import org.apache.geode.admin.AdminDistributedSystem;
import org.apache.geode.admin.AdminException;
import org.apache.geode.admin.CacheVm;
import org.apache.geode.admin.ConfigurationParameter;
import org.apache.geode.admin.GemFireMemberStatus;
import org.apache.geode.admin.RegionSubRegionSnapshot;
import org.apache.geode.admin.StatisticResource;
import org.apache.geode.admin.SystemMember;
import org.apache.geode.admin.SystemMemberCacheServer;
import org.apache.geode.admin.jmx.Agent;
import org.apache.geode.cache.InterestPolicy;
import org.apache.geode.cache.SubscriptionAttributes;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.GemFireVersion;
import org.apache.geode.internal.admin.remote.ClientHealthStats;
import org.apache.geode.internal.logging.LogService;

/**
 * This class uses the JMX Attributes/Operations that use (return/throw) GemFire types. This is the
 * single MBean accessible with ObjectName string {@link MemberInfoWithStatsMBean#MBEAN_NAME}}. This
 * MBean can be used to retrieve the all member details as plain java types.
 *
 * This MBean also acts as a Notification Hub for all the Notifications that are defined for Admin
 * Distributed System.
 *
 *
 * @since GemFire 6.5
 */
public class MemberInfoWithStatsMBean extends AbstractDynamicMBean implements NotificationEmitter {
  private static final Logger logger = LogService.getLogger();

  /* constants defining max no of attributes/operations/notifications */
  private static final int MAX_ATTRIBUTES_COUNT = 3;
  private static final int MAX_OPERATIONS_COUNT = 3;
  private static final int MAX_NOTIFICATIONS_COUNT = 9;

  private static final String NOT_AVAILABLE_STR = "N/A";
  private static final String NOT_AVAILABLE = null;
  private static final Number NOT_AVAILABLE_NUMBER = null;

  /*
   * String constant used for a region that is used on admin side just as a root for rootRegions
   * defined on the member
   */
  private static final String PLACE_HOLDER_ROOT_REGION = "/Root/";

  /* String that are used to form QueryExp/ObjectName for querying MBeanServer */
  private static final String REGION_QUERY_EXPRESSION = "*GemFire.Cache*:*,owner={0},type=Region";
  private static final String STATS_QUERY_EXPRESSION = "*GemFire.Statistic*:*,source={0},name={1}";

  /** mbean name string for this MBean */
  /* default */static final String MBEAN_NAME = "GemFire:type=MemberInfoWithStatsMBean";

  /** ObjectName handle for this MBean */
  private ObjectName objectName;

  /** version of the GemFire Enterprise system that is running */
  private String version;
  private int refreshInterval;
  private String id;

  private Agent agent;
  private AdminDistributedSystemJmxImpl adminDSJmx;

  private NotificationForwarder forwarder;
  private boolean isInitialized;// needs synchronization?

  /**
   * Default Constructor
   *
   * @param agent Admin Agent instance
   * @throws OperationsException if ObjectName can't be formed for this MBean
   */
  MemberInfoWithStatsMBean(Agent agent)
      throws OperationsException, MBeanRegistrationException, AdminException {
    this.agent = agent;
    this.objectName = ObjectName.getInstance(MBEAN_NAME);
    this.version = GemFireVersion.getGemFireVersion();
    this.refreshInterval = -1;
    this.id = NOT_AVAILABLE_STR;
    this.forwarder = new NotificationForwarder(agent.getMBeanServer());
  }

  /**
   * Returns attributes defined for this MBean as an array of MBeanAttributeInfo objects.
   *
   * @return attributes defined as an array of MBeanAttributeInfo objects.
   */
  @Override
  protected MBeanAttributeInfo[] createMBeanAttributeInfo() {
    MBeanAttributeInfo[] attributesInfo = new MBeanAttributeInfo[MAX_ATTRIBUTES_COUNT];

    /*
     * First letter in attribute name has to be 'V' so that getVersion is called. With 'v' it looks
     * for getversion, same for others
     */
    attributesInfo[0] = new MBeanAttributeInfo("Version", String.class.getName(),
        "GemFire Enterprise Version", true, /* readable */
        false, /* writable */
        false);/* has getter with name like 'is****' */

    attributesInfo[1] = new MBeanAttributeInfo("RefreshInterval", String.class.getName(),
        "The interval (in seconds) between auto-polling for updating member & statistics resources. If this is '-1', it means the this MBean has not yet been initialized. First call to getMembers operation will initialize this MBean.",
        true, /* readable */
        false, /* writable */
        false);/* has getter with name like 'is****' */

    attributesInfo[2] = new MBeanAttributeInfo("Id", String.class.getName(),
        "Identifier of the GemFire Enterprise. If this is 'N/A', it means the this MBean has not yet been initialized. First call to getMembers operation will initialize this MBean.",
        true, /* readable */
        false, /* writable */
        false);/* has getter with name like 'is****' */


    return attributesInfo;
  }

  /**
   * Returns operations defined for this MBean as an array of MBeanOperationInfo objects.
   *
   * @return operations defined as an array of MBeanOperationInfo objects.
   */
  @Override
  protected MBeanOperationInfo[] createMBeanOperationInfo() {
    MBeanOperationInfo[] operationsInfo = new MBeanOperationInfo[MAX_OPERATIONS_COUNT];

    operationsInfo[0] = new MBeanOperationInfo("getMembers",
        "Returns ids as strings for all the members - Application Peers & Cache Servers.",
        new MBeanParameterInfo[] {}, String[].class.getName(), MBeanOperationInfo.ACTION_INFO);

    MBeanParameterInfo[] getMemberDetailsArgs = new MBeanParameterInfo[1];
    getMemberDetailsArgs[0] = new MBeanParameterInfo("memberId", String.class.getName(),
        "Id of the member for all the details are to be retrieved.");
    operationsInfo[1] =
        new MBeanOperationInfo("getMemberDetails", "Returns details for a given member",
            getMemberDetailsArgs, Map.class.getName(), MBeanOperationInfo.ACTION_INFO);

    /*
     * For retrieving ObjectNames of existing Region MBeans, MBeanServerConnection.queryMBeans(),
     * could be called
     */
    MBeanParameterInfo[] getRegionSnapArgs = new MBeanParameterInfo[1];
    getRegionSnapArgs[0] = new MBeanParameterInfo("memberId", String.class.getName(),
        "Id of the member on which we want to discover all the region MBean.");
    operationsInfo[2] = new MBeanOperationInfo("getRegions",
        "Returns a java.util.Map of details of regions on a member", getRegionSnapArgs,
        Map.class.getName(), MBeanOperationInfo.ACTION_INFO);


    return operationsInfo;
  }

  /**
   * Returns notifications defined for this MBean as an array of MBeanNotificationInfo objects.
   *
   * @return notification definitions as an array of MBeanNotificationInfo objects.
   */
  @Override
  protected MBeanNotificationInfo[] createMBeanNotificationInfo() {
    MBeanNotificationInfo[] notificationsInfo = new MBeanNotificationInfo[MAX_NOTIFICATIONS_COUNT];

    String[] notificationTypes = new String[] {AdminDistributedSystemJmxImpl.NOTIF_MEMBER_JOINED};
    notificationsInfo[0] =
        new MBeanNotificationInfo(notificationTypes, Notification.class.getName(),
            "A GemFire manager, cache, or other member has joined this distributed system.");

    notificationTypes = new String[] {AdminDistributedSystemJmxImpl.NOTIF_MEMBER_LEFT};
    notificationsInfo[1] =
        new MBeanNotificationInfo(notificationTypes, Notification.class.getName(),
            "A GemFire manager, cache, or other member has left the distributed system.");

    notificationTypes = new String[] {AdminDistributedSystemJmxImpl.NOTIF_MEMBER_CRASHED};
    notificationsInfo[2] =
        new MBeanNotificationInfo(notificationTypes, Notification.class.getName(),
            "A member of this distributed system has crashed instead of leaving cleanly.");

    notificationTypes = new String[] {AdminDistributedSystemJmxImpl.NOTIF_ALERT};
    notificationsInfo[3] =
        new MBeanNotificationInfo(notificationTypes, Notification.class.getName(),
            "A member of this distributed system has generated an alert.");

    notificationTypes = new String[] {AdminDistributedSystemJmxImpl.NOTIF_ADMIN_SYSTEM_DISCONNECT};
    notificationsInfo[4] =
        new MBeanNotificationInfo(notificationTypes, Notification.class.getName(),
            "A GemFire manager, cache, or other member has joined this distributed system.");

    notificationTypes = new String[] {SystemMemberJmx.NOTIF_CACHE_CREATED};
    notificationsInfo[5] =
        new MBeanNotificationInfo(notificationTypes, Notification.class.getName(),
            "A cache got created on a member of this distributed system.");

    notificationTypes = new String[] {SystemMemberJmx.NOTIF_CACHE_CLOSED};
    notificationsInfo[6] = new MBeanNotificationInfo(notificationTypes,
        Notification.class.getName(), "A cache is closed on a member of this distributed system.");

    notificationTypes = new String[] {SystemMemberJmx.NOTIF_REGION_CREATED};
    notificationsInfo[7] =
        new MBeanNotificationInfo(notificationTypes, Notification.class.getName(),
            "A region is created in a cache on a member of this distributed system.");

    notificationTypes = new String[] {SystemMemberJmx.NOTIF_REGION_LOST};
    notificationsInfo[8] =
        new MBeanNotificationInfo(notificationTypes, Notification.class.getName(),
            "A region was removed from a cache on a member of this distributed system.");

    return notificationsInfo;
  }

  /**
   *
   * @return ObjectName of this MBean
   */
  /* default */ ObjectName getObjectName() {
    return objectName;
  }

  /**
   * Returns the version of the GemFire Enterprise instance as a string.
   *
   * @return GemFire Enterprise version string derived from {@link GemFireVersion}
   */
  /* getter for attribute - Version */
  public String getVersion() {
    return version;
  }

  /**
   * @return the refreshInterval
   */
  public int getRefreshInterval() {
    return refreshInterval;
  }

  /**
   * @return the id
   */
  public String getId() {
    return id;
  }

  /**
   * Connects the Admin Agent in the DS
   *
   * @return AdminDistributedSystem MBean ObjectName
   * @throws OperationsException if connection to the DS fails
   * @throws AdminException if connection to the DS fails
   */
  private ObjectName connectToSystem() throws OperationsException, AdminException {
    ObjectName adminDsObjName = agent.connectToSystem();

    AdminDistributedSystem adminDS = agent.getDistributedSystem();
    if (adminDSJmx == null && adminDS instanceof AdminDistributedSystemJmxImpl) {// instanceof
                                                                                 // checks for null
      adminDSJmx = (AdminDistributedSystemJmxImpl) adminDS;
      refreshInterval = adminDSJmx.getRefreshInterval();
      id = adminDSJmx.getId();
      forwarder.registerNotificationListener(adminDSJmx.getObjectName());
    }

    return adminDsObjName;
  }

  /**
   *
   * @return SystemMemberJmx instance for given memberId
   */
  private SystemMemberJmx findMember(String memberId) throws AdminException {
    SystemMemberJmx foundMember = null;

    if (agent.isConnected()) {
      SystemMember[] members = adminDSJmx.getSystemMemberApplications();
      for (SystemMember app : members) {
        if (app.getId().equals(memberId)) {
          foundMember = (SystemMemberJmx) app;
          break;
        }
      }

      if (foundMember == null) {
        members = adminDSJmx.getCacheVms();
        for (SystemMember cacheVm : members) {
          if (cacheVm.getId().equals(memberId)) {
            foundMember = (SystemMemberJmx) cacheVm;
            break;
          }
        }
      }
    }

    return foundMember;
  }

  /**
   * Return ObjectNames for all the Member MBeans in the DS.
   *
   * @return Array of ObjectNames of all Member MBeans
   * @throws OperationsException if (1)agent could not connect in the DS OR (2)Notification Listener
   *         could not be registered for the Admin DS MBean OR (3)fails to retrieve information from
   *         Admin DS
   */
  public String[] getMembers() throws OperationsException {
    String[] members = new String[0];

    try {
      if (!isInitialized) {
        initializeAll(); // initialize if not yet
      }

      if (adminDSJmx != null) {
        CacheVm[] cacheVms = adminDSJmx.getCacheVms();
        SystemMember[] appVms = adminDSJmx.getSystemMemberApplications();

        List<String> membersList = new ArrayList<String>();
        if (cacheVms != null && cacheVms.length != 0) {
          for (SystemMember cacheVm : cacheVms) {
            membersList.add(cacheVm.getId());
          }
        }
        if (appVms != null && appVms.length != 0) {
          for (SystemMember appVm : appVms) {
            membersList.add(appVm.getId());
          }
        }
        members = new String[membersList.size()];
        members = membersList.toArray(members);
      }
    } catch (AdminException e) {
      logger.warn("Exception occurred for operation: getMembers",
          e);
      throw new OperationsException(e.getMessage());
    } catch (Exception e) {
      logger.warn(
          "Exception occurred for operation: getMembers",
          e);
      throw new OperationsException(e.getMessage());
    }

    return members;
  }

  /**
   * Returns information including ObjectNames for all regions on a member with given member id.
   *
   * @param memberId member identifier as a String
   * @return Map of details of all regions on a member with given id
   * @throws OperationsException if fails to retrieve the regions information
   */
  public Map<String, Map<String, ?>> getRegions(String memberId) throws OperationsException {
    Map<String, Map<String, ?>> regionsInfo = new LinkedHashMap<String, Map<String, ?>>();

    if (memberId != null) {
      try {
        SystemMemberJmx foundMember = findMember(memberId);
        if (foundMember != null) {
          SystemMemberCacheJmxImpl cache = (SystemMemberCacheJmxImpl) foundMember.getCache();
          if (cache != null) {
            Map<String, ObjectName> existingRegionMbeans =
                getExistingRegionMbeansFullPaths(memberId);
            // TODO: this is in-efficient
            // Can a region.create JMX notification be used?
            regionsInfo = getAllRegionsDetails(cache, existingRegionMbeans);
            existingRegionMbeans.clear();
          }
        }
      } catch (AdminException e) {
        logger.warn(String.format("Exception occurred for operation: %s for member: %s",
            new Object[] {"getRegions", memberId}),
            e);
        throw new OperationsException(e.getMessage());
      } catch (Exception e) {
        logger.warn(String.format("Exception occurred for operation: %s for member: %s",
            new Object[] {"getRegions", memberId}),
            e);
        throw new OperationsException(e.getMessage());
      }
    }

    return regionsInfo;
  }

  /* **************************************************************************/
  /* ************* INITIALIZE THE ENTIRE ADMIN DS AT A TIME *******************/
  /* **************************************************************************/
  /**
   * Initializes all the possible MBeans for all the members.
   *
   */
  private void initializeAll() throws OperationsException {
    try {
      connectToSystem();
      if (adminDSJmx != null) {
        // Members are already inited after connectToSystem. Now init Cache, Region & Stats MBeans
        SystemMember[] cacheVms = adminDSJmx.getCacheVms();
        for (int i = 0; i < cacheVms.length; i++) {
          try {
            initializeCacheRegionsAndStats((SystemMemberJmx) cacheVms[i]);
          } catch (AdminException e) {
            logger.info(String.format(
                "Exception occurred while intializing : %s. Contiuning with next  ...",
                cacheVms[i].getId()),
                e);
          }
        }
        SystemMember[] appVms = adminDSJmx.getSystemMemberApplications();
        for (int i = 0; i < appVms.length; i++) {
          try {
            initializeCacheRegionsAndStats((SystemMemberJmx) appVms[i]);
          } catch (AdminException e) {
            logger.info(String.format(
                "Exception occurred while intializing : %s. Contiuning with next  ...",
                appVms[i].getId()),
                e);
          }
        }
      }
    } catch (AdminException e) {
      logger.warn("Exception occurred while intializing.", e);
      throw new OperationsException(e.getMessage());
    } catch (Exception e) {
      logger.warn("Exception occurred while intializing.", e);
      throw new OperationsException(e.getMessage());
    }

    isInitialized = true;
  }

  /**
   * Initializes Cache, Regions & Statistics Types MBeans for the given Member.
   *
   * @param memberJmx Member Mbean instance
   * @throws OperationsException if fails to initialize required MBeans
   * @throws AdminException if fails to initialize required MBeans
   */
  private void initializeCacheRegionsAndStats(SystemMemberJmx memberJmx)
      throws OperationsException, AdminException {
    if (memberJmx != null) {
      SystemMemberCacheJmxImpl cache = (SystemMemberCacheJmxImpl) memberJmx.getCache();
      if (cache != null) {
        RegionSubRegionSnapshot regionSnapshot = cache.getRegionSnapshot();
        initializeRegionSubRegions(cache, regionSnapshot);
      }
      initStats(memberJmx);
    }
  }

  /**
   * Initializes statistics for a member with the given mbean.
   *
   * @param memberJmx Member Mbean instance
   * @throws AdminException if fails to initialize required statistic MBeans
   */
  private void initStats(SystemMemberJmx memberJmx) throws AdminException {
    StatisticResource[] statResources = memberJmx.getStats();
    for (StatisticResource statResource : statResources) {
      statResource.getStatistics();
    }
  }

  /**
   * Initializes all regions & its subregions using the Cache MBean and the RegionSubRegionSnapshot
   * for this cache MBean.
   *
   * @param cache Cache MBean resource
   * @param regionSnapshot RegionSubRegionSnapshot instance for the cache
   * @throws MalformedObjectNameException if fails to initialize the region MBean
   * @throws AdminException if fails to initialize the region MBean
   */
  @SuppressWarnings("rawtypes")
  private void initializeRegionSubRegions(SystemMemberCacheJmxImpl cache,
      RegionSubRegionSnapshot regionSnapshot) throws MalformedObjectNameException, AdminException {
    String fullPath = regionSnapshot.getFullPath();
    if (!fullPath.equals(PLACE_HOLDER_ROOT_REGION)) {
      fullPath = fullPath.substring(PLACE_HOLDER_ROOT_REGION.length() - 1);

      cache.manageRegion(fullPath);
    }

    Set subRegionSnapshots = regionSnapshot.getSubRegionSnapshots();

    for (Iterator iterator = subRegionSnapshots.iterator(); iterator.hasNext();) {
      RegionSubRegionSnapshot subRegion = (RegionSubRegionSnapshot) iterator.next();
      try {
        initializeRegionSubRegions(cache, subRegion);
      } catch (AdminException e) {
        logger.info(
            String.format("Exception occurred while intializing : %s. Contiuning with next  ...",
                subRegion.getFullPath()),
            e);
      }
    }
  }


  /* **************************************************************************/
  /* ********************** EVERYTHING HYPERIC NEEDS **************************/
  /* **************************************************************************/

  /* constants defined that could be used simply retrieve needed info from Map */
  private static final String TYPE_NAME_CACHESERVER = "Cache Server";
  private static final String TYPE_NAME_APPLICATION = "Application Peer";
  /*
   * NOTE - (My Understanding about the followings - abhishek) 1. CacheVM - a VM started using Cache
   * Server Launcher. This is considered to be a dedicated cache VM because there is only GemFire
   * Cache code running here. 2. ApplicationVM - a VM started with a written code using APIs and we
   * can not guarantee that there will be ONLY GemFire code running in this VM. 3. Cache Server -
   * Responsible for serving requests from the clients. There could be multiple of these per Cache
   * and hence per VM - one of 1 or 2 above. These could be specified by <cache-server> (or
   * deprecated <bridge-server>) element(s) in the cache-xml file or using an API
   * Cache.addCacheServer().
   */

  private static final String MEMBER_ID = DistributionConfig.GEMFIRE_PREFIX + "member.id.string";
  private static final String MEMBER_NAME =
      DistributionConfig.GEMFIRE_PREFIX + "member.name.string";
  private static final String MEMBER_HOST =
      DistributionConfig.GEMFIRE_PREFIX + "member.host.string";
  private static final String MEMBER_PORT = DistributionConfig.GEMFIRE_PREFIX + "member.port.int";
  private static final String MEMBER_UPTIME =
      DistributionConfig.GEMFIRE_PREFIX + "member.uptime.long";
  private static final String MEMBER_CLIENTS =
      DistributionConfig.GEMFIRE_PREFIX + "member.clients.map";
  private static final String MEMBER_REGIONS =
      DistributionConfig.GEMFIRE_PREFIX + "member.regions.map";
  private static final String MEMBER_TYPE =
      DistributionConfig.GEMFIRE_PREFIX + "member.type.string";
  private static final String IS_SERVER =
      DistributionConfig.GEMFIRE_PREFIX + "member.isserver.boolean";
  private static final String IS_GATEWAY =
      DistributionConfig.GEMFIRE_PREFIX + "member.isgateway.boolean";

  private static final String MEMBER_STATSAMPLING_ENABLED =
      DistributionConfig.GEMFIRE_PREFIX + "member.config.statsamplingenabled.boolean";
  private static final String MEMBER_TIME_STATS_ENABLED =
      DistributionConfig.GEMFIRE_PREFIX + "member.config.timestatsenabled.boolean";

  private static final String STATS_PROCESSCPUTIME =
      DistributionConfig.GEMFIRE_PREFIX + "member.stat.processcputime.long";
  private static final String STATS_CPUS =
      DistributionConfig.GEMFIRE_PREFIX + "member.stat.cpus.int";
  private static final String STATS_USEDMEMORY =
      DistributionConfig.GEMFIRE_PREFIX + "member.stat.usedmemory.long";
  private static final String STATS_MAXMEMORY =
      DistributionConfig.GEMFIRE_PREFIX + "member.stat.maxmemory.long";
  private static final String STATS_GETS =
      DistributionConfig.GEMFIRE_PREFIX + "member.stat.gets.int";
  private static final String STATS_GETTIME =
      DistributionConfig.GEMFIRE_PREFIX + "member.stat.gettime.long";
  private static final String STATS_PUTS =
      DistributionConfig.GEMFIRE_PREFIX + "member.stat.puts.int";
  private static final String STATS_PUTTIME =
      DistributionConfig.GEMFIRE_PREFIX + "member.stat.puttime.long";

  private static final String REGION_NAME =
      DistributionConfig.GEMFIRE_PREFIX + "region.name.string";
  private static final String REGION_PATH =
      DistributionConfig.GEMFIRE_PREFIX + "region.path.string";
  private static final String REGION_SCOPE =
      DistributionConfig.GEMFIRE_PREFIX + "region.scope.string";
  private static final String REGION_DATAPOLICY =
      DistributionConfig.GEMFIRE_PREFIX + "region.datapolicy.string";
  private static final String REGION_INTERESTPOLICY =
      DistributionConfig.GEMFIRE_PREFIX + "region.interestpolicy.string";
  private static final String REGION_ENTRYCOUNT =
      DistributionConfig.GEMFIRE_PREFIX + "region.entrycount.int";
  private static final String REGION_DISKATTRS =
      DistributionConfig.GEMFIRE_PREFIX + "region.diskattrs.string";

  private static final String CLIENT_ID = DistributionConfig.GEMFIRE_PREFIX + "client.id.string";
  private static final String CLIENT_NAME =
      DistributionConfig.GEMFIRE_PREFIX + "client.name.string";
  private static final String CLIENT_HOST =
      DistributionConfig.GEMFIRE_PREFIX + "client.host.string";
  private static final String CLIENT_QUEUESIZE =
      DistributionConfig.GEMFIRE_PREFIX + "client.queuesize.int";
  private static final String CLIENT_STATS_GETS =
      DistributionConfig.GEMFIRE_PREFIX + "client.stats.gets.int";
  private static final String CLIENT_STATS_PUTS =
      DistributionConfig.GEMFIRE_PREFIX + "client.stats.puts.int";
  private static final String CLIENT_STATS_CACHEMISSES =
      DistributionConfig.GEMFIRE_PREFIX + "client.stats.cachemisses.int";
  private static final String CLIENT_STATS_CPUUSAGE =
      DistributionConfig.GEMFIRE_PREFIX + "client.stats.cpuusage.long";
  private static final String CLIENT_STATS_CPUS =
      DistributionConfig.GEMFIRE_PREFIX + "client.stats.cpus.int";
  private static final String CLIENT_STATS_UPDATETIME =
      DistributionConfig.GEMFIRE_PREFIX + "client.stats.updatetime.long";
  private static final String CLIENT_STATS_THREADS =
      DistributionConfig.GEMFIRE_PREFIX + "client.stats.threads.int";

  /**
   *
   * @return All the required details for a member with given memberId
   */
  public Map<String, Object> getMemberDetails(String memberId) throws OperationsException {
    Map<String, Object> allDetails = new TreeMap<String, Object>();

    if (memberId != null) {
      try {
        SystemMemberJmx member = findMember(memberId);
        if (member != null) {
          SystemMemberCacheJmxImpl cache = (SystemMemberCacheJmxImpl) member.getCache();
          GemFireMemberStatus snapshot = cache.getSnapshot();
          boolean isServer = snapshot.getIsServer();
          boolean isGatewayHub = snapshot.getIsGatewayHub();

          // 1. Member info
          allDetails.put(MEMBER_ID, member.getId());
          allDetails.put(MEMBER_NAME, member.getName());
          String host = member.getHost();// from of GemFireVM.getHost
          InetAddress hostAddr = member.getHostAddress();
          // possibility of null host address
          if (hostAddr != null) {
            host = hostAddr.getHostName();
          }
          allDetails.put(MEMBER_HOST, host);
          allDetails.put(MEMBER_UPTIME, snapshot.getUpTime());
          allDetails.put(IS_SERVER, isServer);
          allDetails.put(IS_GATEWAY, isGatewayHub);

          String memberType = "";
          if (member instanceof CacheServerJmxImpl) {
            memberType = TYPE_NAME_CACHESERVER;
          } else {// Mark it of Application type if neither a gateway hub nor a server
            memberType = TYPE_NAME_APPLICATION;
          }
          allDetails.put(MEMBER_TYPE, memberType);

          // 2. Region info
          Map<String, ObjectName> existingRegionMbeans = getExistingRegionMbeansFullPaths(memberId);
          allDetails.put(MEMBER_REGIONS, getAllRegionsDetails(cache, existingRegionMbeans));
          existingRegionMbeans.clear();

          // 3. Clients info
          allDetails.put(MEMBER_CLIENTS, getClientDetails(snapshot));

          boolean statSamplingEnabled = true;
          // assuming will never return as per current implementation
          ConfigurationParameter[] configParams = member.getConfiguration();
          for (ConfigurationParameter configParam : configParams) {
            if (STATISTIC_SAMPLING_ENABLED.equals(configParam.getName())) {
              allDetails.put(MEMBER_STATSAMPLING_ENABLED, configParam.getValue());
              statSamplingEnabled = Boolean.parseBoolean("" + configParam.getValue());
            } else if (ENABLE_TIME_STATISTICS.equals(configParam.getName())) {
              allDetails.put(MEMBER_TIME_STATS_ENABLED, configParam.getValue());
            }
          }

          // 5. Stats info
          allDetails.putAll(getRequiredStats(member, statSamplingEnabled));

          SystemMemberCacheServer[] cacheServers = cache.getCacheServers();
          // attempt refreshing the cache info once
          if (cacheServers.length == 0) {
            cache.refresh();
            cacheServers = cache.getCacheServers();
          }
          Integer memberCacheServerPort = Integer.valueOf(0);
          if (cacheServers.length != 0) {
            /*
             * Taking the first cache server port. We don't recommend multiple cache severs for a
             * cache.
             */
            memberCacheServerPort = Integer.valueOf(cacheServers[0].getPort());
          }
          allDetails.put(MEMBER_PORT, memberCacheServerPort);
        }

      } catch (AdminException e) {
        logger.warn(String.format("Exception occurred for operation: %s for member: %s",
            new Object[] {"getMemberDetails", memberId}),
            e);
        throw new OperationsException(e.getMessage());
      } catch (Exception e) {
        logger.warn(String.format("Exception occurred for operation: %s for member: %s",
            new Object[] {"getMemberDetails", memberId}),
            e);
        throw new OperationsException(e.getMessage());
      }
    }

    return allDetails;
  }

  /**
   *
   * @return Map of client details
   */
  @SuppressWarnings("rawtypes")
  private Map<String, Map<String, ?>> getClientDetails(GemFireMemberStatus snapshot) {
    Map<String, Map<String, ?>> clientsInfo = new LinkedHashMap<String, Map<String, ?>>();

    Set connectedClients = snapshot.getConnectedClients();
    if (!connectedClients.isEmpty()) {
      Map clientHealthStatsMap = snapshot.getClientHealthStats();

      for (Iterator iterator = connectedClients.iterator(); iterator.hasNext();) {
        Map<String, Object> clientData = new HashMap<String, Object>();
        String clientId = (String) iterator.next();
        String host = snapshot.getClientHostName(clientId);
        clientData.put(CLIENT_ID, clientId);
        clientData.put(CLIENT_NAME, extractClientName(clientId, host));
        clientData.put(CLIENT_HOST, host);
        clientData.put(CLIENT_QUEUESIZE, snapshot.getClientQueueSize(clientId));

        ClientHealthStats clientHealthStats =
            (ClientHealthStats) clientHealthStatsMap.get(clientId);
        if (clientHealthStats != null) {
          clientData.put(CLIENT_STATS_GETS, clientHealthStats.getNumOfGets());
          clientData.put(CLIENT_STATS_PUTS, clientHealthStats.getNumOfPuts());
          clientData.put(CLIENT_STATS_CACHEMISSES, clientHealthStats.getNumOfMisses());
          clientData.put(CLIENT_STATS_CPUUSAGE, clientHealthStats.getProcessCpuTime());
          clientData.put(CLIENT_STATS_CPUS, clientHealthStats.getCpus());
          clientData.put(CLIENT_STATS_UPDATETIME, clientHealthStats.getUpdateTime().getTime());
          clientData.put(CLIENT_STATS_THREADS, clientHealthStats.getNumOfThreads());
        } else {
          clientData.put(CLIENT_STATS_GETS, Integer.valueOf(0));
          clientData.put(CLIENT_STATS_PUTS, Integer.valueOf(0));
          clientData.put(CLIENT_STATS_CACHEMISSES, Integer.valueOf(0));
          clientData.put(CLIENT_STATS_CPUUSAGE, Long.valueOf(0));
          clientData.put(CLIENT_STATS_CPUS, Integer.valueOf(0));
          clientData.put(CLIENT_STATS_UPDATETIME, Long.valueOf(0));
          clientData.put(CLIENT_STATS_THREADS, Integer.valueOf(0));
        }

        clientsInfo.put(clientId, clientData);
      }
    }

    return clientsInfo;
  }

  /**
   * Returns a Map containing information about regions.
   *
   * @param cache Reference to an MBean representing a Cache on a member
   * @param existingRegionMbeans Map of Path against Region MBean ObjectNames
   * @return Map of all region details
   * @throws OperationsException if fails to retrieve
   */
  private Map<String, Map<String, ?>> getAllRegionsDetails(SystemMemberCacheJmxImpl cache,
      Map<String, ObjectName> existingRegionMbeans) throws OperationsException {
    Map<String, Map<String, ?>> regionsInfo = new TreeMap<String, Map<String, ?>>();

    if (cache != null) {
      try {
        RegionSubRegionSnapshot regionSnapshot = cache.getRegionSnapshot();
        collectAllRegionsDetails(cache, regionSnapshot, regionsInfo, existingRegionMbeans);
      } catch (AdminException e) {
        logger.warn("Exception occurred while getting region details.", e);
        throw new OperationsException(e.getMessage());
      } catch (Exception e) {
        logger.warn("Exception occurred while getting region details.", e);
        throw new OperationsException(e.getMessage());
      }
    }

    return regionsInfo;
  }

  /**
   * Collects all the region details from the RegionSubRegionSnapshot instance passed and the Cache
   * MBean. Checks in the set of existingRegionMbeans before initializing Region Mbeans if there are
   * not initialized yet.
   *
   * @param cache Cache MBean instance
   * @param regionSnapshot RegionSubRegionSnapshot instance
   * @param regionsInfo Map of regions information that gets populated recursively
   * @param existingRegionMbeans Map of ObjectNames of existing region MBeans
   * @throws AdminException if unable to initialize region MBean
   * @throws OperationsException if fails to retrieve the Region MBean attribute info
   * @throws MBeanException if fails to retrieve the Region MBean attribute info
   * @throws ReflectionException if fails to retrieve the Region MBean attribute info
   */
  @SuppressWarnings("rawtypes")
  private void collectAllRegionsDetails(SystemMemberCacheJmxImpl cache,
      RegionSubRegionSnapshot regionSnapshot, Map<String, Map<String, ?>> regionsInfo,
      Map<String, ObjectName> existingRegionMbeans)
      throws AdminException, OperationsException, MBeanException, ReflectionException {
    String fullPath = regionSnapshot.getFullPath();
    if (!fullPath.equals(PLACE_HOLDER_ROOT_REGION)) {
      fullPath = fullPath.substring(PLACE_HOLDER_ROOT_REGION.length() - 1);
      String name = regionSnapshot.getName();
      Integer entryCount = Integer.valueOf(regionSnapshot.getEntryCount());
      Map<String, Object> details = new TreeMap<String, Object>();
      details.put(REGION_NAME, name);
      details.put(REGION_PATH, fullPath);
      details.put(REGION_ENTRYCOUNT, entryCount);

      ObjectName regionObjectName = existingRegionMbeans.get(fullPath);
      if (regionObjectName == null) {// initialize if has not yet been
        regionObjectName = cache.manageRegion(fullPath);
      }

      Object attribute = getAttribute(regionObjectName, "scope", NOT_AVAILABLE);
      attribute = attribute != null ? attribute.toString() : attribute;
      details.put(REGION_SCOPE, attribute);

      attribute = getAttribute(regionObjectName, "dataPolicy", NOT_AVAILABLE);
      attribute = attribute != null ? attribute.toString() : attribute;
      details.put(REGION_DATAPOLICY, attribute);

      SubscriptionAttributes interestPolicyAttr =
          (SubscriptionAttributes) getAttribute(regionObjectName, "subscriptionAttributes", null);
      String interestPolicyStr = NOT_AVAILABLE;
      if (interestPolicyAttr != null) {
        InterestPolicy interestPolicy = interestPolicyAttr.getInterestPolicy();
        if (interestPolicy != null) {
          interestPolicyStr = interestPolicy.toString();
        }
      }
      details.put(REGION_INTERESTPOLICY, interestPolicyStr);

      attribute = getAttribute(regionObjectName, "diskWriteAttributes", NOT_AVAILABLE);
      attribute = attribute != null ? attribute.toString() : attribute;
      details.put(REGION_DISKATTRS, attribute);

      regionsInfo.put(fullPath, details);
    }

    Set subRegionSnapshots = regionSnapshot.getSubRegionSnapshots();

    for (Iterator iterator = subRegionSnapshots.iterator(); iterator.hasNext();) {
      RegionSubRegionSnapshot subRegion = (RegionSubRegionSnapshot) iterator.next();
      collectAllRegionsDetails(cache, subRegion, regionsInfo, existingRegionMbeans);
    }
  }

  /**
   * Checks if the given host name string contains ':' as in IPv6 host address.
   *
   * @param host host name string
   * @return true if the host string contains ':', false otherwise
   */
  private static boolean isIPv6(String host) {
    return host.contains(":");
  }

  /**
   * Checks if the given host name is actually a String representation of an IPv4 address.
   *
   * @param host host name string
   * @return true if given host name is a String representation of an IPv4 address, false otherwise
   */
  private static boolean isIPv4(String host) {
    String regex = "\\d{1,3}.\\d{1,3}.\\d{1,3}.\\d{1,3}";

    return host.matches(regex);
  }

  /**
   * Excludes the host name from the client id and returns the String. If the host name can not be
   * detected, returns an empty string. Typically, the client id looks like:
   * HOST(VM_PID:VM_KIND):PORT:RANDOM_STRING:CLIENT_NAME
   *
   * Extracts the client name from the client id. If the client id is not in the expected format,
   * returns 'N/A'
   *
   * @param clientId string identifier for a client
   * @param host host name (FQDN) the client is running on
   * @return name extracted from given client id
   */
  /*
   * Some examples of Client Id format: (1) Java Client:
   * nase(21716:loner):51789:42e9a0bf:client_nase_21716 nase(2560:loner):2:7a84729a:Feeder
   *
   * (2) Native Client: nase(21045:loner):2:GFNative_OnNnEpyRWL:ExampleDistributedSystem
   *
   * (3) IPv6 Host whose name can not be resolved:
   * fdf0:76cf:a0ed:9449:0:0:0:1001(21716:loner):51789:42e9a0b:client_nase_21716
   * fdf0:76cf:a0ed:9449:0:0:0:1001:51789:42e9a0b:client_nase_21716
   */
  private static String extractClientName(String clientId, String host) {
    /* This isIPv6, isIPv4, extractClientName is taken from GFMon code base */
    String hostExcludedId = "";
    if ((isIPv6(host) || isIPv4(host)) && clientId.startsWith(host)) {
      hostExcludedId = clientId.substring(host.length());
    } else {
      int firstDotIndex = host.indexOf(".");
      if (firstDotIndex != -1) {
        String hostShortName = host.substring(0, firstDotIndex);
        hostExcludedId = clientId.substring(hostShortName.length());
      }
    }

    String vmPIDAndKindRegex = "\\(\\w+:\\w+\\)";
    String regex = "(\\<ec\\>)?:[0-9]+(:\\w+){2}+";
    String name = NOT_AVAILABLE;
    String temp = hostExcludedId;

    int openIndex = temp.indexOf("(");
    if (openIndex != -1) {
      regex = vmPIDAndKindRegex + regex;
    }

    if (temp.matches(regex)) {
      String[] splitted = temp.split(":");
      name = splitted[splitted.length - 1];
    }

    return name;
  }

  /**
   * Returns a Map of all the statistics required for Hyperic currently. It relies on the attribute
   * of the StatisticsResource Mbeans.
   *
   * @param member instance for which the stats are needed
   * @return Map of all the statistics required for Hyperic currently.
   * @throws OperationsException exceptions thrown while retrieving the attributes
   */
  private Map<String, Object> getRequiredStats(SystemMemberJmx member, boolean statSamplingEnabled)
      throws OperationsException {
    Map<String, Object> statDetails = new TreeMap<String, Object>();

    try {
      if (!statSamplingEnabled) {
        statDetails.put(STATS_PROCESSCPUTIME, NOT_AVAILABLE_NUMBER);
        statDetails.put(STATS_CPUS, NOT_AVAILABLE_NUMBER);
        statDetails.put(STATS_MAXMEMORY, NOT_AVAILABLE_NUMBER);
        statDetails.put(STATS_USEDMEMORY, NOT_AVAILABLE_NUMBER);
        statDetails.put(STATS_GETS, NOT_AVAILABLE_NUMBER);
        statDetails.put(STATS_GETTIME, NOT_AVAILABLE_NUMBER);
        statDetails.put(STATS_PUTS, NOT_AVAILABLE_NUMBER);
        statDetails.put(STATS_PUTTIME, NOT_AVAILABLE_NUMBER);
      } else {
        MBeanServer mBeanServer = agent.getMBeanServer();
        Number defaultVal = NOT_AVAILABLE_NUMBER;
        Number processCpuTime = defaultVal;
        Number cpus = defaultVal;
        Number maxMemory = defaultVal;
        Number usedMemory = defaultVal;
        Number gets = defaultVal;
        Number getTime = defaultVal;
        Number puts = defaultVal;
        Number putTime = defaultVal;

        ObjectName[] vmMemoryUsageStats = getExistingStats(member.getId(), "vmHeapMemoryStats");
        ObjectName[] vmStats = getExistingStats(member.getId(), "vmStats");
        ObjectName[] cachePerfStats = getExistingStats(member.getId(), "cachePerfStats");
        boolean needToReinit = false;
        if (vmMemoryUsageStats.length == 0 || vmStats.length == 0 || cachePerfStats.length == 0) {
          // if the StatisticResource MBeans are not created
          needToReinit = true;
        }
        if (!needToReinit) {
          /*
           * To handle a case when the StatisticResource MBeans are created but not registered with
           * RefreshTimer. If VMMemoryUsageStats are present, maxMemory should always be non-zero.
           */
          for (int i = 0; i < vmMemoryUsageStats.length; i++) {// ideally there should be a single
                                                               // instance
            String type = (String) mBeanServer.getAttribute(vmMemoryUsageStats[i], "type");

            if ("VMMemoryUsageStats".equals(type)) { // first instance that has Statistics Type name
              maxMemory = (Number) getAttribute(vmMemoryUsageStats[i], "maxMemory", defaultVal);
              break;
            }
          }

          needToReinit = 0 == maxMemory.longValue();
        }

        if (needToReinit) {
          logger.info("Re-initializing statistics for: {}",
              member.getId());
          initStats(member);

          vmMemoryUsageStats = getExistingStats(member.getId(), "vmHeapMemoryStats");
          vmStats = getExistingStats(member.getId(), "vmStats");
          cachePerfStats = getExistingStats(member.getId(), "cachePerfStats");
        }

        for (int i = 0; i < vmMemoryUsageStats.length; i++) {// ideally there should be a single
                                                             // instance
          String type = (String) mBeanServer.getAttribute(vmMemoryUsageStats[i], "type");

          if ("VMMemoryUsageStats".equals(type)) { // first instance that has Statistics Type name
            maxMemory = (Number) getAttribute(vmMemoryUsageStats[i], "maxMemory", defaultVal);
            usedMemory = (Number) getAttribute(vmMemoryUsageStats[i], "usedMemory", defaultVal);
            break;
          }
        }

        for (int i = 0; i < vmStats.length; i++) {// ideally there should be a single instance
          String type = (String) mBeanServer.getAttribute(vmStats[i], "type");

          if ("VMStats".equals(type)) { // first instance that has Statistics Type name
            processCpuTime = (Number) getAttribute(vmStats[i], "processCpuTime", defaultVal);
            cpus = (Number) getAttribute(vmStats[i], "cpus", defaultVal);
            break;
          }
        }

        for (int i = 0; i < cachePerfStats.length; i++) {// ideally there should be a single
                                                         // instance
          String type = (String) mBeanServer.getAttribute(cachePerfStats[i], "type");

          if ("CachePerfStats".equals(type)) { // first instance that has Statistics Type name
            gets = (Number) getAttribute(cachePerfStats[i], "gets", defaultVal);
            getTime = (Number) getAttribute(cachePerfStats[i], "getTime", defaultVal);
            puts = (Number) getAttribute(cachePerfStats[i], "puts", defaultVal);
            putTime = (Number) getAttribute(cachePerfStats[i], "putTime", defaultVal);
            break;
          }
        }

        statDetails.put(STATS_PROCESSCPUTIME, processCpuTime == NOT_AVAILABLE_NUMBER
            ? NOT_AVAILABLE_NUMBER : processCpuTime.longValue());
        statDetails.put(STATS_CPUS,
            cpus == NOT_AVAILABLE_NUMBER ? NOT_AVAILABLE_NUMBER : cpus.intValue());
        statDetails.put(STATS_MAXMEMORY,
            maxMemory == NOT_AVAILABLE_NUMBER ? NOT_AVAILABLE_NUMBER : maxMemory.longValue());
        statDetails.put(STATS_USEDMEMORY,
            usedMemory == NOT_AVAILABLE_NUMBER ? NOT_AVAILABLE_NUMBER : usedMemory.longValue());
        statDetails.put(STATS_GETS,
            gets == NOT_AVAILABLE_NUMBER ? NOT_AVAILABLE_NUMBER : gets.intValue());
        statDetails.put(STATS_GETTIME,
            getTime == NOT_AVAILABLE_NUMBER ? NOT_AVAILABLE_NUMBER : getTime.intValue());
        statDetails.put(STATS_PUTS,
            puts == NOT_AVAILABLE_NUMBER ? NOT_AVAILABLE_NUMBER : puts.intValue());
        statDetails.put(STATS_PUTTIME,
            putTime == NOT_AVAILABLE_NUMBER ? NOT_AVAILABLE_NUMBER : putTime.longValue());
      }
    } catch (Exception e) {
      logger.warn(e.getMessage(), e);
      throw new OperationsException(e.getMessage());
    }

    return statDetails;
  }

  /**
   * Returns attribute with given attribute name on MBean with given ObjectName.
   *
   *
   * @param objectName ObjectName for the MBean
   * @param attribute attribute name
   * @param unavailableValue return this value if the attribute value is null
   * @return value of attribute with given attribute name
   * @throws OperationsException if attribute is not found for MBean with this ObjectName or MBean
   *         instance is not found
   * @throws MBeanException if MBeans getter throws exception
   * @throws ReflectionException thrown when trying to invoke the setter.
   */
  private Object getAttribute(ObjectName objectName, String attribute, Object unavailableValue)
      throws OperationsException, MBeanException, ReflectionException {
    /* NOTE: callers methods rely on non-null value being returned */
    Object value = null;

    MBeanServer mBeanServer = agent.getMBeanServer();
    value = mBeanServer.getAttribute(objectName, attribute);

    value = (value != null) ? value : unavailableValue;

    return value;
  }

  /**
   * Return Map of region full against the ObjectName of existing region MBeans.
   *
   * @param memberId string identifier of a member
   * @return Map of region path vs ObjectName for existing region MBeans
   * @throws MalformedObjectNameException If the query expression used is not valid
   */
  private Map<String, ObjectName> getExistingRegionMbeansFullPaths(String memberId)
      throws MalformedObjectNameException {
    Map<String, ObjectName> pathsToObjName = new HashMap<String, ObjectName>();

    if (memberId != null && memberId.trim().length() != 0) {
      Object[] params = new Object[] {MBeanUtil.makeCompliantMBeanNameProperty(memberId)};
      Set<ObjectName> queryNames = queryObjectNames(REGION_QUERY_EXPRESSION, params);
      for (ObjectName objectName : queryNames) {
        pathsToObjName.put(objectName.getKeyProperty("path"), objectName);
      }
    }

    return pathsToObjName;
  }

  /**
   * Returns an array of ObjectNames existing statistics types MBeans
   *
   * @param memberId string identifier of a member
   * @param name text id of the stats which appears in the stats ObjectName as name keyProperty
   * @return Array of Stats MBean ObjectNames
   * @throws MalformedObjectNameException If the query expression used is not valid
   */
  private ObjectName[] getExistingStats(String memberId, String name)
      throws MalformedObjectNameException {
    ObjectName[] statObjectNames = new ObjectName[0];

    if (memberId != null && memberId.trim().length() != 0) {
      Object[] params = new Object[] {MBeanUtil.makeCompliantMBeanNameProperty(memberId), name};
      Set<ObjectName> queryNames = queryObjectNames(STATS_QUERY_EXPRESSION, params);
      statObjectNames = new ObjectName[queryNames.size()];
      statObjectNames = queryNames.toArray(statObjectNames);
    }

    return statObjectNames;
  }

  /**
   * Queries the MBean server with the string formed using placing the params in the parameterized
   * string passed as queryStr.
   *
   * @param queryStr parameterized string
   * @param params params to put in the string
   * @return results of an ObjectName query
   * @throws MalformedObjectNameException If the query expression ObjectName formed is not valid
   */
  private Set<ObjectName> queryObjectNames(String queryStr, Object... params)
      throws MalformedObjectNameException {
    Set<ObjectName> queried = Collections.emptySet();

    queryStr = MessageFormat.format(queryStr, params);
    ObjectName queryExp = ObjectName.getInstance(queryStr);
    queried = agent.getMBeanServer().queryNames(null, queryExp);

    return queried;
  }


  /* *************************************************************************/
  /* **************** NOTIFICATION EMITTER IMPLEMENTATION ********************/
  /* *************************************************************************/

  /**
   * @see NotificationEmitter#addNotificationListener(NotificationListener, NotificationFilter,
   *      Object)
   */
  public void addNotificationListener(NotificationListener listener, NotificationFilter filter,
      Object handback) throws IllegalArgumentException {
    forwarder.addNotificationListener(listener, filter, handback);
  }

  /**
   * @see NotificationEmitter#removeNotificationListener(NotificationListener)
   */
  public void removeNotificationListener(NotificationListener listener)
      throws ListenerNotFoundException {
    forwarder.removeNotificationListener(listener);
  }

  /**
   * @see NotificationEmitter#getNotificationInfo()
   */
  public MBeanNotificationInfo[] getNotificationInfo() {
    return getMBeanInfo().getNotifications();
  }

  /**
   * @see NotificationEmitter#removeNotificationListener(NotificationListener, NotificationFilter,
   *      Object)
   */
  public void removeNotificationListener(NotificationListener listener, NotificationFilter filter,
      Object handback) throws ListenerNotFoundException {
    forwarder.removeNotificationListener(listener, filter, handback);
  }

}


/**
 * This class acts as a hub for the Notifications defined on AdminDistributedSystem & SystemMember
 * MBeans. This acts as a listener for these notifications and broadcasts them as notifications from
 * the {@link MemberInfoWithStatsMBean} MBean. This class extends
 * {@link NotificationBroadcasterSupport} only to have the functionality to send notifications.
 *
 *
 * @since GemFire 6.5
 */
class NotificationForwarder extends NotificationBroadcasterSupport implements NotificationListener {

  private static final Logger logger = LogService.getLogger();

  /* sequence generator for notifications from GemFireTypesWrapper MBean */
  private static AtomicLong notificationSequenceNumber = new AtomicLong();

  /* reference to the MBeanServer instance */
  private MBeanServer mBeanServer;

  /**
   * Default Constructor
   *
   * @param mBeanServer reference to the MBeanServer instance
   */
  /* default */ NotificationForwarder(MBeanServer mBeanServer) {
    this.mBeanServer = mBeanServer;
  }

  /**
   * Handles notifications as: 1. Member Joined: Registers this NotificationForwarder as a
   * notification listener for Cache/Region Notifications. 2. Member Left/Crashed: Unregisters this
   * NotificationForwarder as a notification listener for Cache/Region Notifications. 3.
   * AdminDistributedSystem Disconnected: Unregisters this NotificationForwarder as a notification
   * listener for member Notifications.
   *
   * Forwards the notifications to the JMX Clients that have registered for notifications on this
   * MBean
   *
   * @param notification notification to be handled
   * @param handback handback object used while NotificationForwarder was registered
   *
   * @see NotificationListener#handleNotification(Notification, Object)
   */
  public void handleNotification(Notification notification, Object handback) {
    Object notifSource = notification.getSource();
    if (AdminDistributedSystemJmxImpl.NOTIF_MEMBER_JOINED.equals(notification.getType())) {
      ObjectName source = (ObjectName) notifSource;
      // initialize statistics/register with refreshTimer for new member
      String[] noArgs = {};
      try {
        ObjectName[] stats =
            (ObjectName[]) mBeanServer.invoke(source, "manageStats", noArgs, noArgs);
        if (stats != null) {
          for (ObjectName stat : stats) {
            mBeanServer.invoke(stat, "getStatistics", noArgs, noArgs);
          }
        }
        logger.debug("getStatistics call completed with no exceptions.");
      } catch (ReflectionException e) {
        logger.info(String.format("Exception while initializing statistics for: %s",
            source.toString()),
            e);
      } catch (MBeanException e) {
        logger.info(String.format("Exception while initializing statistics for: %s",
            source.toString()),
            e);
      } catch (InstanceNotFoundException e) {
        logger.info(String.format("Exception while initializing statistics for: %s",
            source.toString()),
            e);
      }
      // register this listener for joined member's cache/region notifications
      try {
        registerNotificationListener(source);
      } catch (OperationsException e) {
        logger.info(String.format("Exception while registering notification listener for: %s",
            source.toString()),
            e);
      }
    }

    // TODO: Check if same notification instance can be reused by simply changing the sequence
    // number
    notification = new Notification(notification.getType(), notifSource,
        notificationSequenceNumber.addAndGet(1L), notification.getTimeStamp(),
        notification.getMessage());

    sendNotification(notification);
  }

  /**
   * Registers itself as a NotificationListener for Notifications sent from MBean with the
   * ObjectName given as source.
   *
   * @param source source of notifications
   * @throws InstanceNotFoundException The MBean name provided does not match any of the registered
   *         MBeans.
   */
  /* default */void registerNotificationListener(ObjectName source)
      throws InstanceNotFoundException {
    mBeanServer.addNotificationListener(source, this, null/* handback */, source);
  }

  /**
   * Unregisters itself as a NotificationListener for Notifications sent from MBean with the
   * ObjectName given as source.
   *
   * @param source source of notifications
   * @throws InstanceNotFoundException The MBean name provided does not match any of the registered
   *         MBeans.
   * @throws ListenerNotFoundException The listener is not registered in the MBean.
   */
  /* default */void unregisterNotificationListener(ObjectName source)
      throws InstanceNotFoundException, ListenerNotFoundException {
    mBeanServer.removeNotificationListener(source, this);
  }
}
