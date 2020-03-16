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

package org.apache.geode.internal.admin;

import org.apache.geode.admin.AdminException;
import org.apache.geode.admin.GemFireHealth;
import org.apache.geode.admin.GemFireHealthConfig;
import org.apache.geode.admin.GemFireMemberStatus;
import org.apache.geode.admin.RegionSubRegionSnapshot;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Config;

/**
 * Represents one java vm connected to a GemFire distributed system
 *
 */
public interface GemFireVM {

  /**
   * Constant for lightweight cache inspection. Entry values will be returned as Strings.
   */
  int LIGHTWEIGHT_CACHE_VALUE = 100;

  /**
   * Constant for logical cache inspection. Entry values will be returned as a tree of
   * {@link EntryValueNode}s, with each node containing its logical elements.
   */
  int LOGICAL_CACHE_VALUE = 200;

  /**
   * Constant for physical cache inspection. Entry values will be returned as a tree of
   * {@link EntryValueNode}s, with each node containing its declared fields.
   */
  int PHYSICAL_CACHE_VALUE = 300;

  /**
   * Returns the host the vm is running on.
   */
  java.net.InetAddress getHost();

  /**
   * Returns the name of the remote system connection.
   */
  String getName();

  /**
   * Returns the directory in which the member runs
   *
   * @since GemFire 4.0
   */
  java.io.File getWorkingDirectory();

  /**
   * Returns the product directory (the value of GEODE_HOME env variable)
   */
  java.io.File getGeodeHomeDir();

  /**
   * Returns the time the system was started
   */
  java.util.Date getBirthDate();

  /**
   * Returns a String describing the vm's gemfire version info
   *
   * @since GemFire 3.5
   */
  String getVersionInfo();

  /**
   * Returns all statistic resources except those involving SharedClass
   */
  StatResource[] getStats(String statisticsTypeName);

  /**
   * Returns all statistic resources
   */
  StatResource[] getAllStats();

  /**
   * Returns a snapshot of the distributed lock services
   */
  DLockInfo[] getDistributedLockInfo();

  /**
   * Adds a {@link StatListener} for the given resource and attribute. Changes in value will be
   * streamed back from the vm.
   */
  void addStatListener(StatListener observer, StatResource observedResource, Stat observedStat);

  /**
   * Removes {@link StatListener}
   */
  void removeStatListener(StatListener observer);

  /**
   * Adds a {@link HealthListener} with the given configuration to the vm. If a health listener has
   * already been added it will be removed and a new one added.
   *
   * @param cfg determines how and when the health will be checked.
   * @since GemFire 3.5
   */
  void addHealthListener(HealthListener observer, GemFireHealthConfig cfg);

  /**
   * Removes an added health listener.
   *
   * @since GemFire 3.5
   */
  void removeHealthListener();

  /**
   * Resets the current health status to "good".
   *
   * @since GemFire 3.5
   */
  void resetHealthStatus();

  /**
   * Returns detailed information explaining the current health status. Each array element is a
   * different cause for the current status. An empty array will be returned if the current status
   * is "good".
   *
   * @param healthCode The current health status
   *
   * @since GemFire 3.5
   */
  String[] getHealthDiagnosis(GemFireHealth.Health healthCode);

  /**
   * Returns the runtime {@link Config} from the vm
   */
  Config getConfig();

  /**
   * Returns the runtime {@link org.apache.geode.admin.GemFireMemberStatus} from the vm The idea is
   * this snapshot is similar to stats that represent the current state of a running VM. However,
   * this is a bit higher level than a stat
   *
   * @since GemFire 5.7
   */
  GemFireMemberStatus getSnapshot();

  /**
   * Returns the runtime {@link org.apache.geode.admin.RegionSubRegionSnapshot} from the vm The idea
   * is this snapshot is quickly salvageable to present a cache's region's info
   *
   * @since GemFire 5.7
   */
  RegionSubRegionSnapshot getRegionSnapshot();

  /**
   * Sets the runtime configurable parameters in the gemfire vm's {@link Config}
   */
  void setConfig(Config cfg);

  /**
   * Returns the locally running agent through which we access the remote vm
   */
  GfManagerAgent getManagerAgent();


  /**
   * Returns the the main log and the tail of the currently active child log, or just the tail of
   * the main log if child logging is disabled.
   */
  String[] getSystemLogs();

  /**
   * Sets the additional classpath settings to be used in the remote vm when processing admin
   * messages from the console. It can be changed in between messages.
   */
  void setInspectionClasspath(String classpath);

  /**
   * Returns classpath info set by {@link GemFireVM#setInspectionClasspath}
   */
  String getInspectionClasspath();

  /**
   * Returns the root cache region or null if the root region hasn't been created.
   */
  Region[] getRootRegions();

  /**
   * Return the existing region (or subregion) with the specified path that already exists or is
   * already mapped into the cache. Whether or not the path starts with a forward slash it is
   * interpreted as a full path starting at a root. Does not cause a shared region to be mapped into
   * the cache.
   *
   * @param c the cache to get the region from
   * @param path the path to the region
   * @return the Region or null if not found
   * @throws IllegalArgumentException if path is null, the empty string, or "/"
   * @since GemFire 3.5
   */
  Region getRegion(CacheInfo c, String path);

  /**
   * Creates a new root VM region with the given name and attributes in this remote VM. Information
   * about the region is returned.
   *
   * @throws AdminException If an error occurs while creating the region
   *
   * @since GemFire 4.0
   */
  Region createVMRootRegion(CacheInfo c, String name, RegionAttributes attrs) throws AdminException;

  /**
   * Creates a new root VM region with the given name and attributes in this remote VM. Information
   * about the region is returned.
   *
   * @throws AdminException If an error occurs while creating the region
   *
   * @since GemFire 4.0
   */
  Region createSubregion(CacheInfo c, String parentPath, String name, RegionAttributes attrs)
      throws AdminException;

  /**
   * Sets the cache inspection mode to {@link #LIGHTWEIGHT_CACHE_VALUE},
   * {@link #LOGICAL_CACHE_VALUE}, or {@link #PHYSICAL_CACHE_VALUE}.
   *
   * @throws IllegalArgumentException if the type is not one of the appropriate constants
   */
  void setCacheInspectionMode(int mode);

  /**
   * Returns one of these constants {@link #LIGHTWEIGHT_CACHE_VALUE}, {@link #LOGICAL_CACHE_VALUE},
   * or {@link #PHYSICAL_CACHE_VALUE}.
   */
  int getCacheInspectionMode();

  /**
   * Causes a snapshot of the given region to be taken. Results are streamed back to any
   * SnapshotListeners registered with this <code>GemFireVM</code>'s parent {@link GfManagerAgent}.
   *
   * @param regionName the region to snapshot
   */
  void takeRegionSnapshot(String regionName, int snapshotId);

  /**
   * The distribution ID if this VM. Its used to identify this VM by members if this VM's
   * distributed system.
   */
  InternalDistributedMember getId();

  /**
   * Returns information on this vm's cache. If the vm does not have a cache then <code>null</code>
   * is returned.
   */
  CacheInfo getCacheInfo();

  /**
   * Sets the lockTimeout configuration value for the given cache and then returns the current info
   * for that cache.
   */
  CacheInfo setCacheLockTimeout(CacheInfo c, int v) throws AdminException;

  /**
   * Sets the lockLease configuration value for the given cache and then returns the current info
   * for that cache.
   */
  CacheInfo setCacheLockLease(CacheInfo c, int v) throws AdminException;

  /**
   * Sets the searchTimeout configuration value for the given cache and then returns the current
   * info for that cache.
   */
  CacheInfo setCacheSearchTimeout(CacheInfo c, int v) throws AdminException;

  /**
   * Adds a cache server a cache in this VM
   *
   * @since GemFire 4.0
   */
  AdminBridgeServer addCacheServer(CacheInfo cache) throws AdminException;

  /**
   * Returns information about a cache server that runs in this VM
   *
   * @param id The unique {@link AdminBridgeServer#getId id} of the cache server
   */
  AdminBridgeServer getBridgeInfo(CacheInfo cache, int id) throws AdminException;

  /**
   * Starts a cache server in this VM
   *
   * @since GemFire 4.0
   */
  AdminBridgeServer startBridgeServer(CacheInfo cache, AdminBridgeServer bridge)
      throws AdminException;

  /**
   * Stops a cache server in this VM
   *
   * @since GemFire 4.0
   */
  AdminBridgeServer stopBridgeServer(CacheInfo cache, AdminBridgeServer bridge)
      throws AdminException;

  /**
   * This method should be used to set the Alerts Manager for the member agent. Stat Alerts
   * Aggregator would use this method to set stat Alerts Manager with the available alert
   * definitions and the refresh interval set for each member joining the distributed system.
   *
   * @param alertDefs Stat Alert Definitions to set for the Alerts Manager
   * @param refreshInterval refresh interval to be used by the Alerts Manager
   * @param setRemotely whether to be set on remote VM
   *
   * @since GemFire 5.7
   */
  void setAlertsManager(StatAlertDefinition[] alertDefs, long refreshInterval, boolean setRemotely);

  /**
   * This method would be used to set refresh interval for the GemFireVM. This method would mostly
   * be called on each member after initial set up whenever the refresh interval is changed.
   *
   * @param refreshInterval refresh interval to set (in milliseconds)
   *
   * @since GemFire 5.7
   */
  void setRefreshInterval(long refreshInterval);

  /**
   * This method would be used to set Sta Alert Definitions for the GemFireVM. This method would
   * mostly be called on each member after initial set up whenever one or more Stat Alert
   * Definitions get added/updated/removed.
   *
   * @param alertDefs an array of StaAlertDefinition objects
   * @param actionCode one of UpdateAlertDefinitionRequest.ADD_ALERT_DEFINITION,
   *        UpdateAlertDefinitionRequestUPDATE_ALERT_DEFINITION,
   *        UpdateAlertDefinitionRequest.REMOVE_ALERT_DEFINITION
   *
   * @since GemFire 5.7
   */
  void updateAlertDefinitions(StatAlertDefinition[] alertDefs, int actionCode);
}
