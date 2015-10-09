/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.admin;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.admin.AdminException;
import com.gemstone.gemfire.admin.GemFireHealth;
import com.gemstone.gemfire.admin.GemFireHealthConfig;
import com.gemstone.gemfire.admin.GemFireMemberStatus;
import com.gemstone.gemfire.admin.RegionSubRegionSnapshot;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.Config;

/**
 * Represents one java vm connected to a GemFire distributed system
 * 
 * @author Darrel Schneider
 * @author Kirk Lund
 */
public interface GemFireVM {

  /**
   * Constant for lightweight cache inspection. Entry values will be returned
   * as Strings.
   */
  public static final int LIGHTWEIGHT_CACHE_VALUE = 100;

  /**
   * Constant for logical cache inspection. Entry values will be returned as
   * a tree of {@link EntryValueNode}s, with each node containing its logical elements.
   */
  public static final int LOGICAL_CACHE_VALUE = 200;

  /**
   * Constant for physical cache inspection. Entry values will be returned as
   * a tree of {@link EntryValueNode}s, with each node containing its declared fields.
   */
  public static final int PHYSICAL_CACHE_VALUE = 300;

  /**
   * Returns the host the vm is running on.
   */
  public java.net.InetAddress getHost();

  /**
   * Returns the name of the remote system connection.
   */
  public String getName();
    
  /**
   * Returns the directory in which the member runs
   *
   * @since 4.0
   */
  public java.io.File getWorkingDirectory();

  /**
   * Returns the product directory (the value of GEMFIRE env variable)
   */
  public java.io.File getGemFireDir();
  
  /**
   * Returns the time the system was started
   */
  public java.util.Date getBirthDate();

  /**
   * Returns a String describing the vm's gemfire version info
   * @since 3.5
   */
  public String getVersionInfo();

  /**
   * Returns all statistic resources except those involving SharedClass
   */
  public StatResource[] getStats(String statisticsTypeName);

  /**
   * Returns all statistic resources
   */
  public StatResource[] getAllStats();
   
  /**
   * Returns a snapshot of the distributed lock services
   */
  public DLockInfo[] getDistributedLockInfo();

  /**
   * Adds a {@link StatListener} for the given resource and attribute.
   * Changes in value will be streamed back from the vm.
   */
  public void addStatListener(StatListener observer,
                              StatResource observedResource,
                              Stat observedStat);
  
  /**
   * Removes {@link StatListener}
   */
  public void removeStatListener(StatListener observer);  

  /**
   * Adds a {@link HealthListener} with the given configuration to the vm.
   * If a health listener has already been added it will be removed
   * and a new one added.
   * @param cfg determines how and when the health will be checked.
   * @since 3.5
   */
  public void addHealthListener(HealthListener observer,
                                GemFireHealthConfig cfg);
  
  /**
   * Removes an added health listener.
   * @since 3.5
   */
  public void removeHealthListener();

  /**
   * Resets the current health status to "good".
   * @since 3.5
   */
  public void resetHealthStatus();

  /**
   * Returns detailed information explaining the current health
   * status.  Each array element is a different cause for the current
   * status.  An empty array will be returned if the current status is
   * "good".
   *
   * @param healthCode
   *        The current health status
   *
   * @since 3.5
   */
  public String[] getHealthDiagnosis(GemFireHealth.Health healthCode);

  /**
   * Returns the runtime {@link Config} from the vm
   */
  public Config getConfig();

  /**
   * Returns the runtime {@link com.gemstone.gemfire.admin.GemFireMemberStatus} from the vm
   * The idea is this snapshot is similar to stats that represent the current state of a 
   * running VM. However, this is a bit higher level than a stat 
   * @since 5.7
   */
  public GemFireMemberStatus getSnapshot();
  
  /**
   * Returns the runtime {@link com.gemstone.gemfire.admin.RegionSubRegionSnapshot} from the vm
   * The idea is this snapshot is quickly salvageable to present a cache's region's info 
   * @since 5.7
   */
  public RegionSubRegionSnapshot getRegionSnapshot();
  
  /**
   * Sets the runtime configurable parameters in the gemfire vm's
   * {@link Config} 
   */
  public void setConfig(Config cfg);

  /**
   * Returns the locally running agent through which we access the remote vm
   */
  public GfManagerAgent getManagerAgent();
  

  /**
   * Returns the the main log and the tail of the currently active child log,
   * or just the tail of the main log if child logging is disabled.
   */
  public String[] getSystemLogs();

  /**
   * Sets the additional classpath settings to be used in the remote vm
   * when processing admin messages from the console. It can be changed
   * in between messages.
   */
  public void setInspectionClasspath(String classpath);
  
  /**
   * Returns classpath info set by {@link GemFireVM#setInspectionClasspath}
   */
  public String getInspectionClasspath();
  
  /**
   * Returns the root cache region or null if the root
   * region hasn't been created.
   */
  public Region[] getRootRegions();
  /**
   * Return the existing region (or subregion) with the specified
   * path that already exists or is already mapped into the cache.
   * Whether or not the path starts with a forward slash it is interpreted as a
   * full path starting at a root.
   * Does not cause a shared region to be mapped into the cache.
   *
   * @param c the cache to get the region from
   * @param path the path to the region
   * @return the Region or null if not found
   * @throws IllegalArgumentException if path is null, the empty string, or "/"
   * @since 3.5
   */
  public Region getRegion(CacheInfo c, String path);

  /**
   * Creates a new root VM region with the given name and attributes
   * in this remote VM.  Information about the region is returned.
   *
   * @throws AdminException
   *         If an error occurs while creating the region
   *
   * @since 4.0
   */
  public Region createVMRootRegion(CacheInfo c, String name,
                                   RegionAttributes attrs)
    throws AdminException;

  /**
   * Creates a new root VM region with the given name and attributes
   * in this remote VM.  Information about the region is returned.
   *
   * @throws AdminException
   *         If an error occurs while creating the region
   *
   * @since 4.0
   */
  public Region createSubregion(CacheInfo c, String parentPath,
                                String name, RegionAttributes attrs)
    throws AdminException;

  /**
   * Sets the cache inspection mode to {@link #LIGHTWEIGHT_CACHE_VALUE},
   * {@link #LOGICAL_CACHE_VALUE}, or {@link #PHYSICAL_CACHE_VALUE}.
   * @throws IllegalArgumentException if the type is not one of the appropriate constants
   */
  public void setCacheInspectionMode(int mode);

  /**
   * Returns one of these constants {@link #LIGHTWEIGHT_CACHE_VALUE},
   * {@link #LOGICAL_CACHE_VALUE}, or {@link #PHYSICAL_CACHE_VALUE}.
   */
  public int getCacheInspectionMode();

  /**
   * Causes a snapshot of the given region to be taken. Results are streamed back
   * to any SnapshotListeners registered with this <code>GemFireVM</code>'s
   * parent {@link GfManagerAgent}.
   * @param regionName  the region to snapshot
   */
  public void takeRegionSnapshot(String regionName, int snapshotId);

//   /**
//    * Clears any results of a snapshot request still waiting to be streamed back
//    */
//   public void flushSnapshots();  
  
  
  /**
   * Returns the name given to the {@link com.gemstone.gemfire.GemFireConnection}
   * of this process
   */
  //public String getName();
  
//   /**
//    * Returns true if a cache has been created.
//    */
//   public boolean hasCache();
  /**
   * The distribution ID if this VM. Its used to identify this VM
   * by members if this VM's distributed system.
   */
  public InternalDistributedMember getId();

  /**
   * Returns information on this vm's cache.
   * If the vm does not have a cache then <code>null</code> is returned.
   */
  public CacheInfo getCacheInfo();

  /**
   * Sets the lockTimeout configuration value for the given cache and
   * then returns the current info for that cache.
   */
  public CacheInfo setCacheLockTimeout(CacheInfo c, int v)
    throws AdminException ;
  /**
   * Sets the lockLease configuration value for the given cache and
   * then returns the current info for that cache.
   */
  public CacheInfo setCacheLockLease(CacheInfo c, int v)
    throws AdminException;
  /**
   * Sets the searchTimeout configuration value for the given cache and
   * then returns the current info for that cache.
   */
  public CacheInfo setCacheSearchTimeout(CacheInfo c, int v)
    throws AdminException;

  /**
   * Adds a bridge server a cache in this VM
   *
   * @since 4.0
   */
  public AdminBridgeServer addCacheServer(CacheInfo cache)
    throws AdminException;

  /**
   * Returns information about a bridge server that runs in this VM
   *
   * @param id
   *        The unique {@link AdminBridgeServer#getId id} of the
   *        bridge server
   */
  public AdminBridgeServer getBridgeInfo(CacheInfo cache, 
                                         int id)
    throws AdminException;

  /**
   * Starts a bridge server in this VM
   *
   * @since 4.0
   */
  public AdminBridgeServer startBridgeServer(CacheInfo cache,
                                             AdminBridgeServer bridge)
    throws AdminException;

  /**
   * Stops a bridge server in this VM
   *
   * @since 4.0
   */
  public AdminBridgeServer stopBridgeServer(CacheInfo cache,
                                            AdminBridgeServer bridge)
    throws AdminException;
  
  /**
   * This method should be used to set the Alerts Manager for the member agent. 
   * Stat Alerts Aggregator would use this method to set stat Alerts Manager 
   * with the available alert definitions and the refresh interval set for 
   * each member joining the distributed system. 
   * 
   * @param alertDefs Stat Alert Definitions to set for the Alerts Manager
   * @param refreshInterval refresh interval to be used by the Alerts Manager
   * @param setRemotely whether to be set on remote VM
   * 
   * @since 5.7
   */
  public void setAlertsManager(StatAlertDefinition[] alertDefs, long refreshInterval, boolean setRemotely);
  
  /**
   * This method would be used to set refresh interval for the GemFireVM. This 
   * method would mostly be called on each member after initial set up whenever 
   * the refresh interval is changed.
   * 
   * @param refreshInterval refresh interval to set (in milliseconds)
   * 
   * @since 5.7
   */
  public void setRefreshInterval(long refreshInterval);
  
  /**
   * This method would be used to set Sta Alert Definitions for the GemFireVM. 
   * This method would mostly be called on each member after initial set up 
   * whenever one or more Stat Alert Definitions get added/updated/removed.
   * 
   * @param alertDefs an array of StaAlertDefinition objects
   * @param actionCode one of UpdateAlertDefinitionRequest.ADD_ALERT_DEFINITION, 
   *                   UpdateAlertDefinitionRequestUPDATE_ALERT_DEFINITION, 
   *                   UpdateAlertDefinitionRequest.REMOVE_ALERT_DEFINITION
   *                   
   * @since 5.7
   */
  public void updateAlertDefinitions(StatAlertDefinition[] alertDefs, int actionCode);
}
