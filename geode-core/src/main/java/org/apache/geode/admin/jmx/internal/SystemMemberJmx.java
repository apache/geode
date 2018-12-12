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

import java.util.concurrent.atomic.AtomicInteger;

import javax.management.MBeanException;
import javax.management.MalformedObjectNameException;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectName;
import javax.management.RuntimeOperationsException;
import javax.naming.OperationNotSupportedException;

import org.apache.commons.modeler.ManagedBean;
import org.apache.logging.log4j.Logger;

import org.apache.geode.SystemFailure;
import org.apache.geode.admin.AdminException;
import org.apache.geode.admin.ConfigurationParameter;
import org.apache.geode.admin.OperationCancelledException;
import org.apache.geode.admin.StatisticResource;
import org.apache.geode.admin.SystemMember;
import org.apache.geode.admin.SystemMemberCache;
import org.apache.geode.admin.SystemMemberCacheEvent;
import org.apache.geode.admin.SystemMemberRegionEvent;
import org.apache.geode.cache.Operation;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.admin.ClientMembershipMessage;
import org.apache.geode.internal.logging.LogService;

/**
 * Defines methods that all <code>SystemMember</code> MBeans should implement.
 *
 * @since GemFire 4.0
 */
public interface SystemMemberJmx extends SystemMember, NotificationListener {
  /**
   * Notification type for indicating a cache got created on a member of this distributed system.
   */
  String NOTIF_CACHE_CREATED =
      DistributionConfig.GEMFIRE_PREFIX + "distributedsystem.cache.created";
  /**
   * Notification type for indicating a cache is closed on a member of this distributed system.
   */
  String NOTIF_CACHE_CLOSED = DistributionConfig.GEMFIRE_PREFIX + "distributedsystem.cache.closed";
  /**
   * Notification type for indicating a region is created in a cache on a member of this distributed
   * system.
   */
  String NOTIF_REGION_CREATED =
      DistributionConfig.GEMFIRE_PREFIX + "distributedsystem.cache.region.created";
  /**
   * Notification type for indicating a region was removed from a cache on a member of this
   * distributed system.
   */
  String NOTIF_REGION_LOST =
      DistributionConfig.GEMFIRE_PREFIX + "distributedsystem.cache.region.lost";

  /** Notification type for indicating client joined */
  String NOTIF_CLIENT_JOINED =
      DistributionConfig.GEMFIRE_PREFIX + "distributedsystem.cache.client.joined";

  /** Notification type for indicating client left */
  String NOTIF_CLIENT_LEFT =
      DistributionConfig.GEMFIRE_PREFIX + "distributedsystem.cache.client.left";

  /** Notification type for indicating client crashed */
  String NOTIF_CLIENT_CRASHED =
      DistributionConfig.GEMFIRE_PREFIX + "distributedsystem.cache.client.crashed";

  /**
   * Gets the interval in seconds between config refreshes
   *
   * @return the current refresh interval in seconds
   */
  int getRefreshInterval();

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
  void setRefreshInterval(int refreshInterval) throws OperationNotSupportedException;

  /**
   * Sets the refresh interval field. Sets interval in seconds between config refreshes; zero or
   * less turns off auto refreshing. Manual refreshing has no effect on when the next scheduled
   * refresh will occur.
   */
  void _setRefreshInterval(int refreshInterval);

  /**
   * Gets this member's cache.
   *
   * @return <code>ObjectName</code> for this member's cache
   *
   * @throws AdminException If this system member does not host a cache
   */
  ObjectName manageCache() throws AdminException, MalformedObjectNameException;

  /**
   * Gets all active StatisticResources for this manager.
   *
   * @return array of ObjectName instances
   */
  ObjectName[] manageStats() throws AdminException, MalformedObjectNameException;

  /**
   * Gets the active StatisticResources for this manager, based on the typeName as the key
   *
   * @return ObjectName of StatisticResourceJMX instance
   */
  ObjectName[] manageStat(String statisticsTypeName)
      throws AdminException, MalformedObjectNameException;

  /**
   * Handles notification to refresh. Reacts by refreshing the values of this GemFireManager's
   * ConfigurationParamaters. Any other notification is ignored.
   *
   * @param notification the JMX notification being received
   * @param hb handback object is unused
   */
  void handleNotification(Notification notification, Object hb);

  /**
   * Add MBean attribute definitions for each ConfigurationParameter.
   *
   * @param managed the mbean definition to add attributes to
   * @return a new instance of ManagedBean copied from <code>managed</code> but with the new
   *         attributes added
   */
  ManagedBean addDynamicAttributes(ManagedBean managed) throws AdminException;


  /**
   * Implementation should handle creation of cache by extracting the details from the given event
   * object.
   *
   * @param event event object corresponding to the creation of the cache
   */
  void handleCacheCreate(SystemMemberCacheEvent event);

  /**
   * Implementation should handle closure of cache by extracting the details from the given event
   * object.
   *
   * @param event event object corresponding to the closure of the cache
   */
  void handleCacheClose(SystemMemberCacheEvent event);

  /**
   * Implementation should handle creation of region by extracting the details from the given event
   * object.
   *
   * @param event event object corresponding to the creation of a region
   */
  void handleRegionCreate(SystemMemberRegionEvent event);

  /**
   * Implementation should handle loss of region by extracting the details from the given event
   * object.
   *
   * @param event event object corresponding to the loss of a region
   */
  void handleRegionLoss(SystemMemberRegionEvent event);

  /**
   * Implementation should handle client membership changes.
   *
   * @param clientId id of the client for whom membership change happened
   * @param eventType membership change type; one of {@link ClientMembershipMessage#JOINED},
   *        {@link ClientMembershipMessage#LEFT}, {@link ClientMembershipMessage#CRASHED}
   */
  void handleClientMembership(String clientId, int eventType);

  ////////////////////// Inner Classess //////////////////////

  /**
   * A helper class that provides implementation of the <code>SystemMemberJmx</code> interface as
   * static methods.
   */
  class Helper {
    private static final Logger logger = LogService.getLogger();

    private static AtomicInteger notificationSequenceNumber = new AtomicInteger();

    public static int setAndReturnRefreshInterval(SystemMemberJmx member, int refreshInterval) {
      int ret = refreshInterval;

      try {
        MBeanUtil.registerRefreshNotification(member, // NotificationListener
            ((ManagedResource) member).getMBeanName(), // User Data
            RefreshNotificationType.SYSTEM_MEMBER_CONFIG, refreshInterval); // int

      } catch (RuntimeException e) {
        logger.warn(e.getMessage(), e); // dead in water, print, and then ignore
        ret = 0; // zero out to avoid more exceptions

      } catch (VirtualMachineError err) {
        SystemFailure.initiateFailure(err);
        // If this ever returns, rethrow the error. We're poisoned
        // now, so don't let this thread continue.
        throw err;
      } catch (Error e) {
        // Whenever you catch Error or Throwable, you must also
        // catch VirtualMachineError (see above). However, there is
        // _still_ a possibility that you are dealing with a cascading
        // error condition, so you also need to check to see if the JVM
        // is still usable:
        SystemFailure.checkFailure();
        logger.error(e.getMessage(), e); // dead in water, print, and then ignore
        ret = 0; // zero out to avoid more exceptions
      }

      return ret;
    }

    public static ObjectName manageCache(SystemMemberJmx member)
        throws AdminException, MalformedObjectNameException {
      boolean IthrewIt = false;
      try {
        SystemMemberCache cache = member.getCache();
        if (cache == null) {
          IthrewIt = true;
          throw new AdminException(
              "This System Member does not have a Cache.");
        }
        SystemMemberCacheJmxImpl cacheJmx = (SystemMemberCacheJmxImpl) cache;
        return ObjectName.getInstance(cacheJmx.getMBeanName());
      } catch (AdminException e) {
        if (!IthrewIt) {
          logger.warn(e.getMessage(), e);
        }
        throw e;
      } catch (RuntimeException e) {
        logger.warn(e.getMessage(), e);
        throw e;
      } catch (VirtualMachineError err) {
        SystemFailure.initiateFailure(err);
        // If this ever returns, rethrow the error. We're poisoned
        // now, so don't let this thread continue.
        throw err;
      } catch (Error e) {
        // Whenever you catch Error or Throwable, you must also
        // catch VirtualMachineError (see above). However, there is
        // _still_ a possibility that you are dealing with a cascading
        // error condition, so you also need to check to see if the JVM
        // is still usable:
        SystemFailure.checkFailure();
        logger.error(e.getMessage(), e);
        throw e;
      }
    }

    public static ObjectName[] manageStats(SystemMemberJmx member)
        throws AdminException, MalformedObjectNameException {
      try {
        StatisticResource[] stats = member.getStats();
        ObjectName[] onames = new ObjectName[stats.length];
        for (int i = 0; i < stats.length; i++) {
          StatisticResourceJmxImpl stat = (StatisticResourceJmxImpl) stats[i];
          onames[i] = ObjectName.getInstance(stat.getMBeanName());
        }
        return onames;
      } catch (AdminException e) {
        logger.warn(e.getMessage(), e);
        throw e;
      } catch (RuntimeException e) {
        logger.warn(e.getMessage(), e);
        throw e;
      } catch (VirtualMachineError err) {
        SystemFailure.initiateFailure(err);
        // If this ever returns, rethrow the error. We're poisoned
        // now, so don't let this thread continue.
        throw err;
      } catch (Error e) {
        // Whenever you catch Error or Throwable, you must also
        // catch VirtualMachineError (see above). However, there is
        // _still_ a possibility that you are dealing with a cascading
        // error condition, so you also need to check to see if the JVM
        // is still usable:
        SystemFailure.checkFailure();
        logger.error(e.getMessage(), e);
        throw e;
      }
    }

    public static ObjectName[] manageStat(SystemMemberJmx member, String statisticsTypeName)
        throws AdminException, MalformedObjectNameException {
      try {
        StatisticResource[] stats = member.getStat(statisticsTypeName);
        if (stats == null)
          return null;
        else {
          ObjectName[] statNames = new ObjectName[stats.length];
          for (int i = 0; i < stats.length; i++) {
            StatisticResourceJmxImpl statJMX = (StatisticResourceJmxImpl) stats[i];
            statNames[i] = ObjectName.getInstance(statJMX.getMBeanName());
          }
          return statNames;
        }
      } catch (AdminException e) {
        logger.warn(e.getMessage(), e);
        throw e;
      } catch (RuntimeException e) {
        logger.warn(e.getMessage(), e);
        throw e;
      } catch (Error e) {
        logger.error(e.getMessage(), e);
        throw e;
      }
    }

    public static void handleNotification(SystemMemberJmx member, Notification notification,
        Object hb) {
      if (RefreshNotificationType.SYSTEM_MEMBER_CONFIG.getType().equals(notification.getType())
          && ((ManagedResource) member).getMBeanName().equals(notification.getUserData())) {

        try {
          member.refreshConfig();

        } catch (org.apache.geode.admin.AdminException e) {
          logger.warn(e.getMessage(), e);
        } catch (OperationCancelledException e) {
          // underlying resource is no longer reachable by remote admin
          logger.warn(e.getMessage(), e);
          member._setRefreshInterval(0);

        } catch (java.lang.RuntimeException e) {
          logger.warn(e.getMessage(), e); // dead in water, print, and then ignore
          member._setRefreshInterval(0); // zero out to avoid more exceptions

        } catch (VirtualMachineError err) {
          SystemFailure.initiateFailure(err);
          // If this ever returns, rethrow the error. We're poisoned
          // now, so don't let this thread continue.
          throw err;
        } catch (java.lang.Error e) {
          // Whenever you catch Error or Throwable, you must also
          // catch VirtualMachineError (see above). However, there is
          // _still_ a possibility that you are dealing with a cascading
          // error condition, so you also need to check to see if the JVM
          // is still usable:
          SystemFailure.checkFailure();
          logger.error(e.getMessage(), e); // dead in water, print, and then ignore
          member._setRefreshInterval(0); // zero out to avoid more exceptions
        }
      }
    }

    public static ManagedBean addDynamicAttributes(SystemMemberJmx member, ManagedBean managed)
        throws AdminException {

      if (managed == null) {
        throw new IllegalArgumentException(
            "ManagedBean is null");
      }

      member.refreshConfig(); // to get the config parms...

      // need to create a new instance of ManagedBean to clean the "slate"...
      ManagedBean newManagedBean = new DynamicManagedBean(managed);
      ConfigurationParameter[] params = member.getConfiguration();
      for (int i = 0; i < params.length; i++) {
        ConfigurationParameterJmxImpl parm = (ConfigurationParameterJmxImpl) params[i];
        ConfigAttributeInfo attrInfo = new ConfigAttributeInfo(parm);

        attrInfo.setName(parm.getName());
        attrInfo.setDisplayName(parm.getName());
        attrInfo.setDescription(parm.getDescription());
        attrInfo.setType(parm.getJmxValueType().getName());

        attrInfo.setIs(false);
        attrInfo.setReadable(true);
        attrInfo.setWriteable(parm.isModifiable());

        newManagedBean.addAttribute(attrInfo);
      }
      return newManagedBean;
    }

    /**
     * Returns the next notification sequence number.
     *
     * @return the notificationSequenceNumber
     */
    /* default */static int getNextNotificationSequenceNumber() {
      return notificationSequenceNumber.incrementAndGet();
    }

    /**
     * Returns the cache event details extracted from the given SystemMemberCacheEvent
     *
     * @param event SystemMemberCacheEvent instance
     * @return the cache event details extracted from the given SystemMemberCacheEvent
     */
    /* default */static String getCacheEventDetails(SystemMemberCacheEvent event) {
      String memberId = event.getMemberId();
      Operation operation = event.getOperation();

      return "CacheEvent[MemberId: " + memberId + ", operation: " + operation + "]";
    }

    /**
     * Returns the region event details extracted from the given SystemMemberRegionEvent
     *
     * @param event SystemMemberRegionEvent instance
     * @return the cache event details extracted from the given SystemMemberRegionEvent
     */
    /* default */static String getRegionEventDetails(SystemMemberRegionEvent event) {
      String memberId = event.getMemberId();
      Operation operation = event.getOperation();

      return "RegionEvent[MemberId: " + memberId + ", operation: " + operation + ", region:"
          + event.getRegionPath() + "]";
    }

    /**
     * Sends the given notification.
     *
     * @param notif notification to send
     *
     * @throws NullPointerException if resource or ModelMBean for resource is null
     */
    /* default */static void sendNotification(ManagedResource resource, Notification notif) {
      try {
        if (MBeanUtil.isRegistered(resource.getObjectName())) {
          resource.getModelMBean().sendNotification(notif);
          if (logger.isDebugEnabled()) {
            logger.debug("Sent '{}' notification", notif.getType());
          }
        }
      } catch (RuntimeOperationsException e) {
        logger
            .info(String.format("Failed to send %s notification for %s",
                new Object[] {"'" + notif.getType() + "'", "'" + notif.getMessage() + "'"}),
                e);
      } catch (MBeanException e) {
        logger
            .info(String.format("Failed to send %s notification for %s",
                new Object[] {"'" + notif.getType() + "'", "'" + notif.getMessage() + "'"}),
                e);
      }
    }
  }
}
