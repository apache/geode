/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.admin;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.persistence.PersistentID;
import com.gemstone.gemfire.distributed.DistributedMember;

import java.io.File;
import java.net.InetAddress;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Administrative interface for managing an entire GemFire distributed
 * system.  This interface should not be confused with {@link
 * com.gemstone.gemfire.distributed.DistributedSystem
 * DistributedSystem} that represents a connection to a GemFire
 * distributed system.
 *
 * @see AdminDistributedSystemFactory
 *
 * @author    Kirk Lund
 * @since     3.5
 * @deprecated as of 7.0 use the <code><a href="{@docRoot}/com/gemstone/gemfire/management/package-summary.html">management</a></code> package instead
 */
public interface AdminDistributedSystem {
	
  /**
   * Retrieves the unique id for this system.
   */
  public String getId();
  
  /**
   * Retrieves display friendly name for this system.  If this administrative
   * VM defined an optional name for its connection to the distributed system,
   * that name will be returned.  Otherwise the returned value will be {@link
   * com.gemstone.gemfire.admin.AdminDistributedSystem#getId}.
   */
  public String getName();
  
  /**
   * Retrieves the remote command and formatting this system should use to 
   * access and/or manipulate resources on remote machines.
   */
  public String getRemoteCommand();

  /**
   * Sets the remote command and formatting this system should use to access 
   * and/or manipulate resources on remote machines.
   */
  public void setRemoteCommand(String remoteCommand);

  /**
   * Sets the lowest level of alert that should be delivered to the
   * {@link AlertListener}s registered on this
   * <code>AdminDistributedSystem</code>.  The default level is {@link
   * AlertLevel#WARNING}. 
   */
  public void setAlertLevel(AlertLevel level);

  /**
   * Returns the lowest level of alerts that should be delivered to
   * the {@link AlertListener}s registered on this
   * <code>AdminDistributedSystem</code>.
   *
   * @see #setAlertLevel
   */
  public AlertLevel getAlertLevel();
  
  /**
   * Sets the lowest level of alert that should be delivered to the
   * {@link AlertListener}s registered on this
   * <code>AdminDistributedSystem</code>.  The default level is {@link
   * AlertLevel#WARNING}. 
   */
  public void setAlertLevelAsString(String level);

  /**
   * Returns the lowest level of alerts that should be delivered to
   * the {@link AlertListener}s registered on this
   * <code>AdminDistributedSystem</code>.
   *
   * @see #setAlertLevelAsString
   */
  public String getAlertLevelAsString();

  /**
   * Registers an <code>AlertListener</code> that will receive all
   * alerts that are at or above the {@linkplain #setAlertLevel alert
   * level}. 
   */
  public void addAlertListener(AlertListener listener);

  /**
   * Unregisters an <code>AlertListener</code> 
   */
  public void removeAlertListener(AlertListener listener);

  /**
   * Retrieves the multicast address in use by this system.
   */
  public String getMcastAddress();

  /**
   * Retrieves the multicast port in use by this system.
   */
  public int getMcastPort();

  /**
   * Retrieves comma-delimited list locators to be used if multi-cast port is
   * zero.  Format of each locators must be host[port].
   */
  public String getLocators();

  /** 
   * Returns true if this system has enabled the use of multicast for communications
   */
  public boolean isMcastEnabled();
  
  /**
   * Returns true if any members of this system are currently running.
   */
  public boolean isRunning();

  /** 
   * Returns <code>true</code> if this is currently connected to the
   * system.
   */
  public boolean isConnected();
    
  /**
   * Starts all managed entities that are not currently running.
   *
   * @throws AdminException
   *         If a problem is encountered while starting the managed
   *         entities.
   */
  public void start() throws AdminException;

  /**
   * Stops all managed entities that are currently running.
   *
   * @throws AdminException
   *         If a problem is encountered while starting the managed
   *         entities.
   */
  public void stop() throws AdminException;

  /**
   * Merges and returns all system logs as a single formatted log.
   */
  public String displayMergedLogs();

  /**
   * Retrieves the license information for this installation of GemFire.
   *
   * @deprecated Removed licensing in 8.0.
   */
  public java.util.Properties getLicense();

  /**
   * Creates a new <code>DistributionLocator</code> that is ready to
   * {@linkplain DistributionLocator#getConfig configure} and
   * {@linkplain #start start}.
   *
   * <P>
   *
   * It is presumed that the newly-added locator is used to discover
   * members of the distributed system.  That is, the host/port of the
   * new locator is appended to the {@link #getLocators locators}
   * attribute of this <code>AdminDistributedSystem</code>.
   */
  public DistributionLocator addDistributionLocator();
  
  /** 
   * Returns array of <code>DistributionLocator</code>s administered
   * by this <code>AdminDistributedSystem</code>.
   */ 
  public DistributionLocator[] getDistributionLocators();
  
  /**
   * Retrieves SystemMember instances for every
   * application that is running and currently connection to this
   * system.  Note that this list does not include dedicated
   * {@linkplain #getCacheVms cache server vms}.
   */
  public SystemMember[] getSystemMemberApplications() 
  throws com.gemstone.gemfire.admin.AdminException;

  /**
   * Display in readable format the latest Alert in this distributed system.
   */
  public String getLatestAlert();
  
  /**
   * Returns an object for monitoring the health of GemFire.
   */
  public GemFireHealth getGemFireHealth();

  /**
   * Connects to the distributed system.  This method will return
   * immediately after spawning a background thread that connects to
   * the distributed system.  As a result, a
   * <code>AdminDistributedSystem</code> can be "connected" to before
   * any members of the system have been started or have been seen.
   * The {@link #waitToBeConnected} method will wait for the
   * connection to be made.
   *
   * @see #isConnected
   * @see #isRunning
   * @see #waitToBeConnected
   */
  public void connect();

  /**
   * Wait for up to a given number of milliseconds for the connection
   * to the distributed system to be made.
   *
   * @param timeout
   *        The number of milliseconds to wait for the connection to
   *        to be made.
   *
   * @return Whether or not the connection was made.
   *         <code>false</code>, if the method times out
   *
   * @throws InterruptedException
   *         If the thread invoking this method is interrupted while
   *         waiting. 
   * @throws IllegalStateException
   *         If {@link #connect} has not yet been called.
   */
  public boolean waitToBeConnected(long timeout)
    throws InterruptedException;

  /**
   * Disconnects from the distributed system.
   */
  public void disconnect();

  /** Returns this system's configuration .*/  
  public DistributedSystemConfig getConfig();
  
  /**
   * Registers a listener that receives callbacks when a member joins
   * or leaves the distributed system.
   */
  public void addMembershipListener(SystemMembershipListener listener);

  /**
   * Unregisters a membership listener
   *
   * @see #addMembershipListener
   */
  public void removeMembershipListener(SystemMembershipListener listener);

  /**
   * Registers a cache event listener.
   * Does nothing if the listener is already registered. The listeners are called
   * in the order they are registered.
   * @param listener the listener to register.
    * @since 5.0
   */
   public void addCacheListener(SystemMemberCacheListener listener);

   /**
    * Unregisters a cache listener. Does nothing if the listener is
    * not registered.
    * @param listener the listener to unregister.
    * @since 5.0
    */
   public void removeCacheListener(SystemMemberCacheListener listener);

  /**
   * Creates a new cache server that is ready to {@linkplain
   * CacheServerConfig configure} and {@linkplain #start
   * start}.
   *
   * @since 4.0
   * @deprecated as of 5.7 use {@link #addCacheVm} instead.
   */
  @Deprecated
  public CacheServer addCacheServer() throws AdminException;

  /**
   * Returns all of the dedicated cache server members of the
   * distributed system.  Because they are not managed entities,
   * application VMs that host a server cache are not included in the
   * array.
   *
   * @since 4.0
   * @deprecated as of 5.7 use {@link #getCacheVms} instead.
   */
  @Deprecated
  public CacheServer[] getCacheServers() throws AdminException;

  /**
   * Returns all the cache server members of the distributed system which are
   * hosting a client queue for the particular durable-client having the given
   * durableClientId
   * 
   * @param durableClientId -
   *                durable-id of the client
   * @return array of CacheServer(s) having the queue for the durable client
   * @throws AdminException
   *
   * @since 5.6
   */
  public CacheServer[] getCacheServers(String durableClientId)
      throws AdminException;

  /**
   * Creates a new cache vm that is ready to {@linkplain
   * CacheVmConfig configure} and {@linkplain #start
   * start}.
   *
   * @since 5.7
   */
  public CacheVm addCacheVm() throws AdminException;

  /**
   * Returns all of the dedicated cache server vm members of the
   * distributed system.  Because they are not managed entities,
   * application VMs that host a server cache are not included in the
   * array.
   *
   * @since 5.7
   */
  public CacheVm[] getCacheVms() throws AdminException;

  /**
   * Returns the administrative SystemMember specified by the {@link
   * com.gemstone.gemfire.distributed.DistributedMember}.
   *
   * @param distributedMember the distributed member to lookup
   * @return administrative SystemMember for that distributed member
   * @since 5.0
   */
  public SystemMember lookupSystemMember(DistributedMember distributedMember) 
  throws AdminException;

  /**
   * Indicate to the distributed system that persistent files have been lost.
   * When a member recovers from a set of persistent files, it will wait for
   * other members that were also persisting the same region to start up. If the
   * persistent files for those other members were lost, this method can be used
   * to tell the remaining members to stop waiting for the lost data.
   * 
   * @param host
   *          The host of the member whose files were lost.
   * @param directory
   *          The directory where those files resided.
   * @since 6.5
   * @deprecated use {@link #revokePersistentMember(UUID)} instead
   */
  public void revokePersistentMember(InetAddress host, String directory) throws AdminException;
  
  /**
   * Indicate to the distributed system that persistent files have been lost.
   * When a member recovers from a set of persistent files, it will wait for
   * other members that were also persisting the same region to start up. If the
   * persistent files for those other members were lost, this method can be used
   * to tell the remaining members to stop waiting for the lost data.
   * 
   * @param diskStoreID
   *          The unique id of the disk store which you are revoking. The unique
   *          id can be discovered from {@link #getMissingPersistentMembers()}
   * 
   * @since 7.0
   */
  public void revokePersistentMember(UUID diskStoreID) throws AdminException;
  
  /**
   * Retrieve the set of persistent files that the existing members are waiting
   * for. See {@link AdminDistributedSystem#revokePersistentMember(InetAddress, String)}
   * @return The persistent members that were known to the existing persistent members,
   * when the existing members were last online.
   * @throws AdminException
   * @since 6.5
   * 
   */
  public Set<PersistentID> getMissingPersistentMembers() throws AdminException;

  /**
   * Shuts down all the members of the distributed system with a cache that the admin 
   * member is connected to, excluding the stand-alone locators. Calling this method
   * will ensure that regions with the {@link DataPolicy#PERSISTENT_PARTITION} to
   * be shutdown in a way which allows for a faster recovery when the members are 
   * restarted.
   * 
   * Killing individual members can lead to inconsistencies in the members persistent
   * data, which gemfire repairs on startup. Calling shutDownAllMembers makes sure
   * that the persistent files are consistent on shutdown, which makes recovery faster.
   *  
   * This is equivalent to calling shutDownAllMembers(0);
   * @return The set of members that were shutdown
   * @since 6.5
   */
  public Set<DistributedMember> shutDownAllMembers() throws AdminException;
  
  /**
   * Shuts down all the members of the distributed system with a cache that the
   * admin member is connected to, excluding the stand-alone locators. Calling
   * this method will ensure that regions with the
   * {@link DataPolicy#PERSISTENT_PARTITION} to be shutdown in a way which
   * allows for a faster recovery when the members are restarted.
   * 
   * Killing individual members can lead to inconsistencies in the members
   * persistent data, which gemfire repairs on startup. Calling
   * shutDownAllMembers makes sure that the persistent files are consistent on
   * shutdown, which makes recovery faster.
   * 
   * @param timeout The amount of time to wait (in milliseconds) for the shutdown all to
   *          complete. 
   * @return The set of members that were shutdown, or null if the timeout is exceeded.
   * 
   * @since 6.5
   */
  public Set<DistributedMember> shutDownAllMembers(long timeout) throws AdminException;

  /**
   * Backup the persistent files for all of the members of the distributed
   * system that the admin member is connected to. 
   * 
   * @param targetDir The directory where each member's backup should be placed.
   * 
   * @return The status of the backup, which includes the set of members
   * that were backed up and the set of members that were known to be
   * offline at the time of backup.
   * @since 6.5
   */
  public BackupStatus backupAllMembers(File targetDir) throws AdminException;
  
  /**
   * Incrementally backup the persistent files for all of the members of the distributed
   * system that the admin member is connected to. Only new operation log files since the previous backup will be copied during this backup.
   * The generated restore script will reference and copy operation log files from the previous backup.
   * 
   * @param targetDir The directory where each member's backup should be placed.
   * @param baselineDir The directory of a previous backup.
   * If this parameter is null or the directory does not exist (on a member by member basis)
   * a full backup will be performed for the member. 
   * 
   * @return The status of the backup, which includes the set of members
   * that were backed up and the set of members that were known to be
   * offline at the time of backup.
   * @since 6.5
   */
  public BackupStatus backupAllMembers(File targetDir,File baselineDir) throws AdminException;
  
  /**
   * Compact the persistent files for all of the members of the distributed
   * system that the admin member connected to. 
   * 
   * This is equivalent to calling {DiskStore#forceCompaction} on all members.
   * 
   * @return The set of members that compacted their disk stores.
   * @since 6.5
   */
  public Map<DistributedMember, Set<PersistentID>> compactAllDiskStores() throws AdminException;
}

