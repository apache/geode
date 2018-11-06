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

import javax.management.Notification;
import javax.management.ObjectName;
import javax.management.modelmbean.ModelMBean;
import javax.naming.OperationNotSupportedException;

import org.apache.commons.modeler.ManagedBean;
import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.SystemFailure;
import org.apache.geode.admin.AdminException;
import org.apache.geode.admin.Statistic;
import org.apache.geode.internal.admin.StatResource;
import org.apache.geode.internal.logging.LogService;

/**
 * Provides MBean support for the monitoring of a statistic resource.
 *
 * @since GemFire 3.5
 *
 */
public class StatisticResourceJmxImpl extends org.apache.geode.admin.internal.StatisticResourceImpl
    implements javax.management.NotificationListener,
    org.apache.geode.admin.jmx.internal.ManagedResource {

  private static final Logger logger = LogService.getLogger();

  /**
   * Interval in seconds between refreshes. Values less than one results in no refreshing .
   */
  private int refreshInterval = 0;

  /** The JMX object name of this managed resource */
  private ObjectName objectName;

  /** A flag to indicate if time is inited. MBeanUtil lookup is costly */
  private boolean timerInited = false;

  // -------------------------------------------------------------------------
  // Constructor(s)
  // -------------------------------------------------------------------------

  /**
   * Constructor for the StatisticResource object
   *
   * @param statResource the admin StatResource to manage/monitor
   * @param member the SystemMember owning this resource
   * @exception org.apache.geode.admin.AdminException if unable to create this StatisticResource for
   *            administration
   */
  public StatisticResourceJmxImpl(StatResource statResource, SystemMemberJmx member)
      throws org.apache.geode.admin.AdminException {
    super(statResource, member);
    initializeMBean();
  }

  /** Create and register the MBean to manage this resource */
  private void initializeMBean() throws org.apache.geode.admin.AdminException {
    this.mbeanName = new StringBuffer("GemFire.Statistic:").append("source=")
        .append(MBeanUtil.makeCompliantMBeanNameProperty(this.member.getId())).append(",type=")
        .append(MBeanUtil.makeCompliantMBeanNameProperty(getType())).append(",name=")
        .append(MBeanUtil.makeCompliantMBeanNameProperty(getName())).append(",uid=")
        .append(getUniqueId()).toString();

    this.objectName =
        MBeanUtil.createMBean(this, addDynamicAttributes(MBeanUtil.lookupManagedBean(this)));

    // Refresh Interval
    AdminDistributedSystemJmxImpl sysJmx =
        (AdminDistributedSystemJmxImpl) this.member.getDistributedSystem();
    if (sysJmx.getRefreshInterval() > 0)
      this.refreshInterval = sysJmx.getRefreshInterval();
  }

  // -------------------------------------------------------------------------
  // MBean attributes - accessors/mutators
  // -------------------------------------------------------------------------

  /**
   * Gets the interval in seconds between statistics refreshes
   *
   * @return the current refresh interval in seconds
   */
  public int getRefreshInterval() {
    return this.refreshInterval;
  }

  /**
   * Sets interval in seconds between statistic refreshes; zero or less turns off auto refreshing.
   * Manual refreshing has no effect on when the next scheduled refresh will occur.
   *
   * @param refreshInterval the new refresh interval in seconds
   */
  private void _setRefreshInterval(int refreshInterval) {
    boolean isRegistered = MBeanUtil.isRefreshNotificationRegistered(this,
        RefreshNotificationType.STATISTIC_RESOURCE_STATISTICS);

    if (isRegistered && (getRefreshInterval() == refreshInterval))
      return;

    try {
      MBeanUtil.registerRefreshNotification(this, // NotificationListener
          getMBeanName(), // User Data as MBean Name
          RefreshNotificationType.STATISTIC_RESOURCE_STATISTICS, refreshInterval); // int

      this.refreshInterval = refreshInterval;
      timerInited = true;
    } catch (RuntimeException e) {
      logger.warn(e.getMessage(), e); // dead in water, print, and then ignore
      this.refreshInterval = 0; // zero out to avoid more exceptions
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
      this.refreshInterval = 0; // zero out to avoid more exceptions
    }
  }

  /**
   * RefreshInterval is now set only through the AdminDistributedSystem property refreshInterval.
   * Attempt to set refreshInterval on StatisticResourceJmx MBean would result in an
   * OperationNotSupportedException Auto-refresh is enabled on demand when a call to getStatistics
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

  // -------------------------------------------------------------------------
  // JMX Notification listener
  // -------------------------------------------------------------------------

  /**
   * Handles notification to refresh. Reacts by refreshing the values of this SystemMember's
   * ConfigurationParamaters. Any other notification is ignored. Given notification is handled only
   * if there is any JMX client connected to the system.
   * <p>
   * TODO: investigate use of NotificationFilter instead of explicit check...
   *
   * @param notification the JMX notification being received
   * @param hb handback object is unused
   */
  public void handleNotification(Notification notification, Object hb) {
    AdminDistributedSystemJmxImpl adminDSJmx =
        (AdminDistributedSystemJmxImpl) this.member.getDistributedSystem();

    String typeStatResourceStats = RefreshNotificationType.STATISTIC_RESOURCE_STATISTICS.getType();

    if (typeStatResourceStats.equals(notification.getType())
        && getMBeanName().equals(notification.getUserData())
        && !adminDSJmx.isRmiClientCountZero()) {
      try {
        refresh();

      } catch (org.apache.geode.admin.AdminException e) {
        logger.warn(e.getMessage(), e);
      } catch (org.apache.geode.admin.OperationCancelledException e) {
        // underlying resource is no longer reachable by remote admin
        logger.warn(e.getMessage(), e);
        _setRefreshInterval(0);
      } catch (CancelException e) {
        // shutting down - okay to ignore
      } catch (java.lang.RuntimeException e) {
        logger.debug(e.getMessage(), e); // dead in water, print, and then ignore
        _setRefreshInterval(0); // zero out to avoid more exceptions
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
        this.refreshInterval = 0; // zero out to avoid more exceptions
      }
    }
  }

  // -------------------------------------------------------------------------
  // Create MBean attributes for each Statistic
  // -------------------------------------------------------------------------

  /**
   * Add MBean attribute definitions for each Statistic.
   *
   * @param managed the mbean definition to add attributes to
   * @return a new instance of ManagedBean copied from <code>managed</code> but with the new
   *         attributes added
   */
  ManagedBean addDynamicAttributes(ManagedBean managed)
      throws org.apache.geode.admin.AdminException {
    if (managed == null) {
      throw new IllegalArgumentException(
          "ManagedBean is null");
    }

    refresh(); // to get the stats...

    // need to create a new instance of ManagedBean to clean the "slate"...
    ManagedBean newManagedBean = new DynamicManagedBean(managed);
    for (int i = 0; i < this.statistics.length; i++) {
      StatisticAttributeInfo attrInfo = new StatisticAttributeInfo();

      attrInfo.setName(this.statistics[i].getName());
      attrInfo.setDisplayName(this.statistics[i].getName());
      attrInfo.setDescription(this.statistics[i].getDescription());
      attrInfo.setType("java.lang.Number");

      attrInfo.setIs(false);
      attrInfo.setReadable(true);
      attrInfo.setWriteable(false);

      attrInfo.setStat(this.statistics[i]);

      newManagedBean.addAttribute(attrInfo);
    }
    return newManagedBean;
  }

  public Statistic[] getStatistics() {
    if (!timerInited) {
      // 1st call to getStatistics would trigger
      // the auto-refresh if an interval is set
      if (this.refreshInterval > 0) {
        this._setRefreshInterval(this.refreshInterval);
      }
    }

    if (this.statistics == null) {
      try {
        this.refresh();
      } catch (AdminException e) {
        this.statistics = new Statistic[0];
      }
    }

    return this.statistics;
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
    return ManagedResourceType.STATISTIC_RESOURCE;
  }

  public void cleanupResource() {
    this.modelMBean = null;
    this.member = null;
    this.statistics = null;
    this.statResource = null;
  }

  /**
   * Checks equality of the given object with <code>this</code> based on the type (Class) and the
   * MBean Name returned by <code>getMBeanName()</code> methods.
   *
   * @param obj object to check equality with
   * @return true if the given object is if the same type and its MBean Name is same as
   *         <code>this</code> object's MBean Name, false otherwise
   */
  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof StatisticResourceJmxImpl)) {
      return false;
    }

    StatisticResourceJmxImpl other = (StatisticResourceJmxImpl) obj;

    return this.getMBeanName().equals(other.getMBeanName());
  }

  /**
   * Returns hash code for <code>this</code> object which is based on the MBean Name generated.
   *
   * @return hash code for <code>this</code> object
   */
  @Override
  public int hashCode() {
    return this.getMBeanName().hashCode();
  }
}
