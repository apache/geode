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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.management.MalformedObjectNameException;
import javax.management.Notification;
import javax.management.ObjectName;
import javax.management.modelmbean.ModelMBean;
import javax.naming.OperationNotSupportedException;

import org.apache.commons.modeler.ManagedBean;
import org.apache.logging.log4j.Logger;

import org.apache.geode.admin.AdminException;
import org.apache.geode.admin.ConfigurationParameter;
import org.apache.geode.admin.StatisticResource;
import org.apache.geode.admin.SystemMemberCache;
import org.apache.geode.admin.SystemMemberCacheEvent;
import org.apache.geode.admin.SystemMemberRegionEvent;
import org.apache.geode.admin.internal.ConfigurationParameterImpl;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.admin.ApplicationVM;
import org.apache.geode.internal.admin.ClientMembershipMessage;
import org.apache.geode.internal.admin.GemFireVM;
import org.apache.geode.internal.admin.StatResource;
import org.apache.geode.internal.logging.LogService;

/**
 * Provides MBean support for managing a SystemMember application.
 * <p>
 * TODO: refactor to implement SystemMember and delegate to SystemMemberImpl. Wrap all delegate
 * calls w/ e.printStackTrace() since the HttpAdaptor devours them
 *
 * @since GemFire 3.5
 *
 */
public class SystemMemberJmxImpl extends org.apache.geode.admin.internal.SystemMemberImpl
    implements SystemMemberJmx, javax.management.NotificationListener,
    org.apache.geode.admin.jmx.internal.ManagedResource {

  private static final Logger logger = LogService.getLogger();

  /**
   * Interval in seconds between refreshes. Value less than one results in no refreshing
   */
  private int refreshInterval = 0;

  /** The JMX object name of this managed resource */
  private ObjectName objectName;

  /** Reference to the cache MBean representing a Cache in the Cache VM Member */
  private SystemMemberCacheJmxImpl managedSystemMemberCache;

  /** collection to collect all the resources created for this member */
  private Map<StatResource, StatisticResourceJmxImpl> managedStatisticsResourcesMap =
      new HashMap<StatResource, StatisticResourceJmxImpl>();


  // -------------------------------------------------------------------------
  // Constructor(s)
  // -------------------------------------------------------------------------

  /**
   * Constructs an instance of SystemMemberJmxImpl.
   *
   * @param system the distributed system this SystemMember is a member of
   * @param application the internal admin application to delegate actual work
   */
  public SystemMemberJmxImpl(AdminDistributedSystemJmxImpl system, ApplicationVM application)
      throws org.apache.geode.admin.AdminException {
    super(system, application);
    initializeMBean();
  }

  /**
   * Constructs the instance of SystemMember using the corresponding InternalDistributedMember
   * instance of a DS member for the given AdminDistributedSystem.
   *
   * @param system Current AdminDistributedSystem instance
   * @param member InternalDistributedMember instance for which a SystemMember instance is to be
   *        constructed.
   * @throws AdminException if construction of SystemMember fails
   *
   * @since GemFire 6.5
   */
  protected SystemMemberJmxImpl(AdminDistributedSystemJmxImpl system,
      InternalDistributedMember member) throws AdminException {
    super(system, member);
    initializeMBean();
  }

  /** Create and register the MBean to manage this resource */
  private void initializeMBean() throws org.apache.geode.admin.AdminException {
    this.mbeanName = new StringBuffer("GemFire.Member:id=")
        .append(MBeanUtil.makeCompliantMBeanNameProperty(getId())).append(",type=")
        .append(MBeanUtil.makeCompliantMBeanNameProperty(getType().getName())).toString();

    this.objectName =
        MBeanUtil.createMBean(this, addDynamicAttributes(MBeanUtil.lookupManagedBean(this)));

    // Refresh Interval
    AdminDistributedSystemJmxImpl sysJmx = (AdminDistributedSystemJmxImpl) system;
    if (sysJmx.getRefreshInterval() > 0)
      this.refreshInterval = sysJmx.getRefreshInterval();
  }

  // -------------------------------------------------------------------------
  // MBean attributes - accessors/mutators
  // -------------------------------------------------------------------------

  /**
   * Gets the interval in seconds between config refreshes
   *
   * @return the current refresh interval in seconds
   */
  public int getRefreshInterval() {
    return this.refreshInterval;
  }

  /**
   * RefreshInterval is now set only through the AdminDistributedSystem property refreshInterval.
   * Attempt to set refreshInterval on SystemMemberJmx MBean would result in an
   * OperationNotSupportedException Auto-refresh is enabled on demand when a call to refreshConfig
   * is made
   *
   * @param refreshInterval the new refresh interval in seconds
   * @deprecated since 6.0 use DistributedSystemConfig.refreshInterval instead
   */
  @Deprecated
  public void setRefreshInterval(int refreshInterval) throws OperationNotSupportedException {
    throw new OperationNotSupportedException(
        "RefreshInterval can not be set directly. Use DistributedSystemConfig.refreshInterval.");
  }

  /**
   * Sets interval in seconds between member config refreshes; zero or less turns off auto
   * refreshing. Manual refreshing has no effect on when the next scheduled refresh will occur.
   *
   * @param refreshInterval the new refresh interval in seconds
   */
  public void _setRefreshInterval(int refreshInterval) {
    boolean isRegistered = MBeanUtil.isRefreshNotificationRegistered(this,
        RefreshNotificationType.SYSTEM_MEMBER_CONFIG);

    if (isRegistered && (getRefreshInterval() == refreshInterval))
      return;

    this.refreshInterval = Helper.setAndReturnRefreshInterval(this, refreshInterval);
  }

  // -------------------------------------------------------------------------
  // MBean Operations
  // -------------------------------------------------------------------------

  public void refreshConfig() throws org.apache.geode.admin.AdminException {
    // 1st call to refreshConfig would trigger
    // the auto-refresh if an interval is set
    if (this.refreshInterval > 0) {
      this._setRefreshInterval(this.refreshInterval);
    }

    super.refreshConfig();
  }

  /**
   * Gets this member's cache.
   *
   * @return <code>ObjectName</code> for this member's cache
   *
   * @throws AdminException If this system member does not host a cache
   */
  public ObjectName manageCache() throws AdminException, MalformedObjectNameException {

    return Helper.manageCache(this);
  }

  /**
   * Gets all active StatisticResources for this manager.
   *
   * @return array of ObjectName instances
   */
  public ObjectName[] manageStats() throws AdminException, MalformedObjectNameException {

    return Helper.manageStats(this);
  }

  /**
   * Gets the active StatisticResources for this manager, based on the typeName as the key
   *
   * @return ObjectName of StatisticResourceJMX instance
   */
  public ObjectName[] manageStat(String statisticsTypeName)
      throws AdminException, MalformedObjectNameException {

    return Helper.manageStat(this, statisticsTypeName);
  }

  // -------------------------------------------------------------------------
  // JMX Notification listener
  // -------------------------------------------------------------------------

  /**
   * Handles notification to refresh. Reacts by refreshing the values of this SystemMember's
   * ConfigurationParamaters. Any other notification is ignored. Given notification is handled only
   * if there is any JMX client connected to the system.
   *
   * @param notification the JMX notification being received
   * @param hb handback object is unused
   */
  public void handleNotification(Notification notification, Object hb) {
    AdminDistributedSystemJmxImpl systemJmx = (AdminDistributedSystemJmxImpl) this.system;

    if (!systemJmx.isRmiClientCountZero()) {
      Helper.handleNotification(this, notification, hb);
    }
  }

  // -------------------------------------------------------------------------
  // Template methods overriden from superclass...
  // -------------------------------------------------------------------------

  /**
   * Template method for creating instance of ConfigurationParameter. Overridden to return
   * ConfigurationParameterJmxImpl.
   */
  @Override
  protected ConfigurationParameter createConfigurationParameter(String name, String description,
      Object value, Class type, boolean userModifiable) {
    return new ConfigurationParameterJmxImpl(name, description, value, type, userModifiable);
  }

  /**
   * Override createStatisticResource by instantiating StatisticResourceJmxImpl if it was not
   * created earlier otherwise returns the same instance.
   *
   * @param stat StatResource reference for which this JMX resource is to be created
   * @return StatisticResourceJmxImpl - JMX Implementation of StatisticResource
   * @throws AdminException if constructing StatisticResourceJmxImpl instance fails
   */
  @Override
  protected StatisticResource createStatisticResource(StatResource stat)
      throws org.apache.geode.admin.AdminException {
    StatisticResourceJmxImpl managedStatisticResource = null;

    synchronized (this.managedStatisticsResourcesMap) {
      /*
       * Ensuring that a single instance of Statistic Resource is created per StatResource.
       */
      StatisticResourceJmxImpl statisticResourceJmxImpl = managedStatisticsResourcesMap.get(stat);
      if (statisticResourceJmxImpl != null) {
        managedStatisticResource = statisticResourceJmxImpl;
      } else {
        managedStatisticResource = new StatisticResourceJmxImpl(stat, this);
        managedStatisticResource.getStatistics();// inits timer
        managedStatisticsResourcesMap.put(stat, managedStatisticResource);
      }
    }
    return managedStatisticResource;
  }

  /**
   * Override createSystemMemberCache by instantiating SystemMemberCacheJmxImpl if it was not
   * created earlier.
   *
   * @param vm GemFireVM reference for which this JMX resource is to be created
   * @return SystemMemberCacheJmxImpl - JMX Implementation of SystemMemberCache
   * @throws AdminException if constructing SystemMemberCacheJmxImpl instance fails
   */
  @Override
  protected SystemMemberCache createSystemMemberCache(GemFireVM vm)
      throws org.apache.geode.admin.AdminException {
    if (managedSystemMemberCache == null) {
      managedSystemMemberCache = new SystemMemberCacheJmxImpl(vm);
    }
    return managedSystemMemberCache;
  }

  // -------------------------------------------------------------------------
  // Create MBean attributes for each ConfigurationParameter
  // -------------------------------------------------------------------------

  /**
   * Add MBean attribute definitions for each ConfigurationParameter.
   *
   * @param managed the mbean definition to add attributes to
   * @return a new instance of ManagedBean copied from <code>managed</code> but with the new
   *         attributes added
   */
  public ManagedBean addDynamicAttributes(ManagedBean managed) throws AdminException {

    return Helper.addDynamicAttributes(this, managed);
  }

  // -------------------------------------------------------------------------
  // ManagedResource implementation
  // -------------------------------------------------------------------------

  /** The name of the MBean that will manage this resource */
  private String mbeanName;

  /** The ModelMBean that is configured to manage this resource */
  private ModelMBean modelMBean;

  public String getMBeanName() {
    return this.mbeanName;
  }

  public ModelMBean getModelMBean() {
    return this.modelMBean;
  }

  public void setModelMBean(ModelMBean modelMBean) {
    this.modelMBean = modelMBean;
  }

  public ObjectName getObjectName() {
    return this.objectName;
  }

  public ManagedResourceType getManagedResourceType() {
    return ManagedResourceType.SYSTEM_MEMBER;
  }

  /**
   * Un-registers all the statistics & cache managed resource created for this member. After
   * un-registering the resource MBean instances, clears managedStatisticsResourcesMap collection.
   */
  public void cleanupResource() {
    synchronized (this.managedStatisticsResourcesMap) {
      ConfigurationParameter[] names = getConfiguration();
      if (names != null) {
        for (int i = 0; i < names.length; i++) {
          ConfigurationParameter parm = names[i];
          ((ConfigurationParameterImpl) parm).removeConfigurationParameterListener(this);
        }
      }
      this.parms.clear();

      Collection<StatisticResourceJmxImpl> statisticResources =
          managedStatisticsResourcesMap.values();

      for (StatisticResourceJmxImpl statisticResource : statisticResources) {
        MBeanUtil.unregisterMBean(statisticResource);
      }

      this.managedStatisticsResourcesMap.clear();
    }
    MBeanUtil.unregisterMBean(managedSystemMemberCache);
  }


  /**
   * Cleans up Managed Resources created for the client that was connected to the server represented
   * by this class.
   *
   * @param clientId id of the client to be removed
   * @return List of ManagedResources associated with the client of given client id
   */
  /*
   * This clean up is for the clients. The clients are started with a loner DM. Hence the clientId
   * is not supposed to contain '/' as per InternalDistributedMember.toString().
   */
  public List<ManagedResource> cleanupBridgeClientResources(String clientId) {
    List<ManagedResource> returnedResources = new ArrayList<ManagedResource>();

    String compatibleId = "id_" + MBeanUtil.makeCompliantMBeanNameProperty(clientId);
    synchronized (this.managedStatisticsResourcesMap) {
      Set<Entry<StatResource, StatisticResourceJmxImpl>> entrySet =
          this.managedStatisticsResourcesMap.entrySet();

      for (Iterator<Entry<StatResource, StatisticResourceJmxImpl>> it = entrySet.iterator(); it
          .hasNext();) {
        Entry<StatResource, StatisticResourceJmxImpl> entry = it.next();
        StatisticResourceJmxImpl resource = entry.getValue();
        if (resource.getMBeanName().contains(compatibleId)) {
          it.remove(); // remove matching entry
          returnedResources.add(resource);
        }
      }
    }
    return returnedResources;
  }

  /**
   * Implementation handles client membership changes.
   *
   * @param clientId id of the client for whom membership change happened
   * @param eventType membership change type; one of {@link ClientMembershipMessage#JOINED},
   *        {@link ClientMembershipMessage#LEFT}, {@link ClientMembershipMessage#CRASHED}
   */
  public void handleClientMembership(String clientId, int eventType) {
    String notifType = null;
    List<ManagedResource> cleanedUp = null;

    if (eventType == ClientMembershipMessage.LEFT) {
      notifType = NOTIF_CLIENT_LEFT;
      cleanedUp = cleanupBridgeClientResources(clientId);
    } else if (eventType == ClientMembershipMessage.CRASHED) {
      notifType = NOTIF_CLIENT_CRASHED;
      cleanedUp = cleanupBridgeClientResources(clientId);
    } else if (eventType == ClientMembershipMessage.JOINED) {
      notifType = NOTIF_CLIENT_JOINED;
    }

    if (cleanedUp != null) {
      for (ManagedResource resource : cleanedUp) {
        MBeanUtil.unregisterMBean(resource);
      }
    }

    Helper.sendNotification(this, new Notification(notifType, this.modelMBean,
        Helper.getNextNotificationSequenceNumber(), clientId));
  }

  /**
   * Implementation handles creation of cache by extracting the details from the given event object
   * and sending the {@link SystemMemberJmx#NOTIF_CACHE_CREATED} notification to the connected JMX
   * Clients.
   *
   * @param event event object corresponding to the creation of the cache
   */
  public void handleCacheCreate(SystemMemberCacheEvent event) {
    Helper.sendNotification(this, new Notification(NOTIF_CACHE_CREATED, this.modelMBean,
        Helper.getNextNotificationSequenceNumber(), Helper.getCacheEventDetails(event)));
  }

  /**
   * Implementation handles closure of cache by extracting the details from the given event object
   * and sending the {@link SystemMemberJmx#NOTIF_CACHE_CLOSED} notification to the connected JMX
   * Clients.
   *
   * @param event event object corresponding to the closure of the cache
   */
  public void handleCacheClose(SystemMemberCacheEvent event) {
    Helper.sendNotification(this, new Notification(NOTIF_CACHE_CLOSED, this.modelMBean,
        Helper.getNextNotificationSequenceNumber(), Helper.getCacheEventDetails(event)));
  }

  /**
   * Implementation handles creation of region by extracting the details from the given event object
   * and sending the {@link SystemMemberJmx#NOTIF_REGION_CREATED} notification to the connected JMX
   * Clients. Region Path is set as User Data in Notification.
   *
   * @param event event object corresponding to the creation of a region
   */
  public void handleRegionCreate(SystemMemberRegionEvent event) {
    Notification notification = new Notification(NOTIF_REGION_CREATED, this.modelMBean,
        Helper.getNextNotificationSequenceNumber(), Helper.getRegionEventDetails(event));

    notification.setUserData(event.getRegionPath());

    Helper.sendNotification(this, notification);
  }

  /**
   * Implementation should handle loss of region by extracting the details from the given event
   * object and sending the {@link SystemMemberJmx#NOTIF_REGION_LOST} notification to the connected
   * JMX Clients. Region Path is set as User Data in Notification. Additionally, it also clears the
   * ManagedResources created for the region that is lost.
   *
   * @param event event object corresponding to the loss of a region
   */
  public void handleRegionLoss(SystemMemberRegionEvent event) {
    SystemMemberCacheJmxImpl cacheResource = this.managedSystemMemberCache;

    if (cacheResource != null) {
      ManagedResource cleanedUp = cacheResource.cleanupRegionResources(event.getRegionPath());

      if (cleanedUp != null) {
        MBeanUtil.unregisterMBean(cleanedUp);
      }
    }

    Notification notification = new Notification(NOTIF_REGION_LOST, this.modelMBean,
        Helper.getNextNotificationSequenceNumber(), Helper.getRegionEventDetails(event));

    notification.setUserData(event.getRegionPath());

    Helper.sendNotification(this, notification);
  }
}
