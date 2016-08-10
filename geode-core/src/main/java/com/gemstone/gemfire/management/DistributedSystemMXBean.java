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
package com.gemstone.gemfire.management;

import java.util.Map;
import javax.management.ObjectName;

import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

/**
 * MBean that provides access to information and management operations for a
 * {@link DistributedSystem}.
 *
 * It also provides an API for navigating the other MBeans exposed by the
 * GemFire distributed system.
 * 
 * There will be one DistributedSystemMBean per GemFire cluster.
 * 
 * <p> ObjectName : GemFire:service=System,type=Distributed
 * 
 * <p> List of notifications emitted by this MBean.
 * 
 * <p>
 * <table border="1">
 * <tr>
 * <th>Notification Type</th>
 * <th>Notification Source</th>
 * <th>Message</th>
 * </tr>
 * <tr>
 * <td>gemfire.distributedsystem.cache.member.joined</td>
 * <td>Name or ID of member who joined</td>
 * <td>Member Joined &ltMember Name or ID&gt</td>
 * </tr>
 * <tr>
 * <td>gemfire.distributedsystem.cache.member.departed</td>
 * <td>Name or ID of member who departed</td>
 * <td>Member Departed &ltMember Name or ID&gt has crashed = &lttrue/false&gt</td>
 * </tr>
 * <tr>
 * <td>gemfire.distributedsystem.cache.member.suspect</td>
 * <td>Name or ID of member who is suspected</td>
 * <td>Member Suspected &ltMember Name or ID&gt By &ltWho Suspected&gt</td>
 * </tr>
 * <tr>
 * <td>system.alert</td>
 * <td>DistributedSystem("&ltDistributedSystem ID"&gt)</td>
 * <td>Alert Message</td>
 * </tr>
 * </table>
 *
 * @since GemFire 7.0
 *
 */
@ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
public interface DistributedSystemMXBean {

  /**
   * Returns the ID of thie DistributedSystem.
   * allow anyone to access this method
   *
   * @return The DistributedSystem ID or -1 if not set.
   */
  @ResourceOperation()
  public int getDistributedSystemId();

  /**
   * Returns the number of members in the distributed system.
   */
  public int getMemberCount();

  /**
   * Returns a list of names for all members.
   */
  public String[] listMembers();

  /**
   * Returns a list of names for locator members.
   *
   * @param onlyStandAloneLocators
   *          if set to <code>true</code>, includes only stand alone locator
   *          members.
   * @return a list of names for locator members.
   */

  public String[] listLocatorMembers(boolean onlyStandAloneLocators);

  /**
   * Returns a list of names for all groups.
   */
  public String[] listGroups();

  /**
   * Returns the number of locators in the distributed system.
   */
  public int getLocatorCount();

  /**
   * Returns a list of IDs for all locators.
   */
  public String[] listLocators(); //TODO - Abhishek Should be renamed to listLocatorDiscoveryConfigurations? Do we need something for mcast too?

  /**
   * Returns the number of disks stores in the distributed system.
   */
  public int getSystemDiskStoreCount();

  /**
   * Returns a map of all {@link DistributedMember}s and their {@link DiskStore}s.
   */
  public Map<String, String[]> listMemberDiskstore();

  /**
   * Returns a list of IDs for all gateway senders.
   */
  public String[] listGatewaySenders();

  /**
   * Returns a list of IDs for all gateway receivers.
   */
  public String[] listGatewayReceivers();

  /**
   * Returns the minimum level set for alerts to be delivered to listeners.
   */
  public String getAlertLevel();

  /**
   * Sets the minimum level for alerts to be delivered to listeners.
   *
   * @param alertLevel
   *          Minimum level for alerts to be delivered.
   *          Must be one of: WARNING, ERROR, SEVERE or NONE.
   */
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.WRITE)
  public void changeAlertLevel(String alertLevel) throws Exception;

  /**
   * Returns the total available heap (in megabytes) across all distributed
   * members.
   */
  public long getTotalHeapSize();

  /**
   * Returns the total number of entries in all regions.
   */
  public long getTotalRegionEntryCount();

  /**
   * Returns the number of {@link Region}s.
   */

  public int getTotalRegionCount();

  /**
   * Returns the number of times that a cache miss occurred for all regions.
   */
  public int getTotalMissCount();

  /**
   * Returns the number of times that a hit occurred for all regions.
   */
  public int getTotalHitCount();

  /**
   * Returns the number of connected clients.
   */
  public int getNumClients();


  /**
   * Returns the average number of disk reads per second across all distributed
   * members.
   */
  public float getDiskReadsRate();

  /**
   * Returns the average number of disk writes per second across all
   * distributed members.
   */
  public float getDiskWritesRate();

  /**
   * Returns the average disk flush latency time.
   */
  public long getDiskFlushAvgLatency();

  /**
   * Returns the number of backups currently in progress for all disk stores.
   */
  public int getTotalBackupInProgress();

  /**
   * Returns the number of initial images in progress.
   */
  public int getNumInitialImagesInProgress();

  /**
   * Returns the number of active (currently executing) CQs for all cache servers.
   */
  public long getActiveCQCount();

  /**
   * Returns the average number of queries per second across all
   * distributed members.
   */
  public float getQueryRequestRate();

  /**
   * Performs a backup on all members.
   *
   * @param targetDirPath
   *          Directory to which backup files will be written
   * @param baselineDirPath
   *          path of the directory for baseline backup.
   * @return The results of the backup request.
   */
  @ResourceOperation(resource = Resource.DATA, operation = Operation.READ)
  public DiskBackupStatus backupAllMembers(String targetDirPath, String baselineDirPath)
      throws Exception;

  /**
   * Returns the configuration information for a distributed member.
   *
   * @param member
   *          Name or ID of the member.
   * @return The configuration information for a member.
   * @throws Exception
   *           for an invalid member ID.
   */
  public GemFireProperties fetchMemberConfiguration(String member)
      throws Exception;

  /**
   * Returns the total time (in seconds) since a distributed member was started.
   *
   * @param member
   *          Name or ID of the member.
   * @return The total time (in seconds) since a member was started.
   * @throws Exception
   *           for an invalid member ID.
   */
  public long fetchMemberUpTime(String member) throws Exception;

  /**
   * Returns a list of names for all cache servers which are able to serve requests from GemFire clients.
   * 
   */
  public String[] listCacheServers();
  
  
  /**
   * Returns a list of names for all servers where server means
   * any long-running GemFire process that was started with "start server" command from GFSH.
   */
  public String[] listServers();

  /**
   * Returns JVM metrics for a distributed member.
   *
   * @param member
   *          Name or ID of the member.
   * @throws Exception
   *           for an invalid member ID.
   */
  public JVMMetrics showJVMMetrics(String member) throws Exception;

  /**
   * Returns operating system metrics for a distributed member.
   *
   * @param member
   *          Name or ID of the member.
   * @throws Exception
   *           for an invalid member ID.
   */
  public OSMetrics showOSMetrics(String member) throws Exception;

  /**
   * Returns network metrics for a distributed member.
   *
   * @param member
   *          Name or ID of the member.
   * @throws Exception
   *           for an invalid member ID.
   */
  public NetworkMetrics showNetworkMetric(String member) throws Exception;

  /**
   * Returns disk metrics for a distributed member.
   *
   * @param member
   *          Name or ID of the member.
   * @throws Exception
   *           for an invalid member ID.
   */
  public DiskMetrics showDiskMetrics(String member) throws Exception;

  /**
   * Shuts down all members of a distributed system except for the managing member.
   *
   * @return List of names of all distributed members that were shutdown.
   */
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.MANAGE)
  public String[] shutDownAllMembers() throws Exception;

  /**
   * Returns a list of names for all regions.
   */
  public String[] listRegions();

  /**
   * Returns a list of full paths for all regions.
   */
  public String[] listAllRegionPaths();

  /**
   * Removes a disk store from the distributed system.
   *
   * @param diskStoreId
   *          UUID of the disk store to remove
   * @return True if the request is successful, false otherwise.
   */
  @ResourceOperation(resource = Resource.DATA, operation = Operation.MANAGE)
  public boolean revokeMissingDiskStores(String diskStoreId)
      throws Exception;

  /**
   * Returns a list of details for disk stores which have been determined to be
   * unavailable during the recovery of region.
   */
  public PersistentMemberDetails[] listMissingDiskStores();

  /**
   * Returns the object name for a {@link MemberMXBean} used to access
   * this distributed member.
   * allow anyone to access this method
   */
  @ResourceOperation()
  public ObjectName getMemberObjectName();

  /**
   * Returns the object name for a {@link ManagerMXBean} used to access the
   * management service running on this distributed member.
   */
  public ObjectName getManagerObjectName();

  /**
   * Returns a list of object names for the {@link MemberMXBean}s used to
   * access all distributed members.
   */
  public ObjectName[] listMemberObjectNames();

  /**
   * Returns the object name for a {@link MemberMXBean} used to access
   * a distributed member..
   *
   * @param member
   *          Name or ID of the member.
   */
  public ObjectName fetchMemberObjectName(String member) throws Exception;

  /**
   * Returns a list of object names for the {@link RegionMXBean}s used to
   * access all regions on a distributed member.
   *
   * @param memberMBeanName
   *          ObjectName of the member.
   */
  public ObjectName[] fetchRegionObjectNames(ObjectName memberMBeanName)
      throws Exception;

  /**
   * Returns a list of object names for the {@link DistributedRegionMXBean}s
   * used to access all distributed regions.
   *
   * @return An array of object names or an empty array if no distributed regions
   *         are found.
   */
  public ObjectName[] listDistributedRegionObjectNames();

  /**
   * Returns the object name for a {@link DistributedRegionMXBean} used to
   * access a distributed region.
   *
   * @param regionPath
   *          Full path of the region.
   */
  public ObjectName fetchDistributedRegionObjectName(String regionPath)
      throws Exception;

  /**
   * Returns the object name for a {@link RegionMXBean} used to access
   * a region.
   *
   * @param member
   *          Name or ID of the member.
   * @param regionPath
   *          Full path of the region.
   */
  public ObjectName fetchRegionObjectName(String member, String regionPath)
      throws Exception;

  /**
   * Returns the object name for a {@link GatewaySenderMXBean} used to access
   * a gateway sender.
   *
   * @param member
   *          Name or ID of the member.
   * @param senderId
   *          ID of a gateway sender.
   */
  public ObjectName fetchGatewaySenderObjectName(String member,
      String senderId) throws Exception;

  /**
   * Returns the object name for a {@link GatewayReceiverMXBean} used to access
   * a gateway receiver.
   *
   * @param member
   *          Name or ID of the member.
   */
  public ObjectName fetchGatewayReceiverObjectName(String member)
      throws Exception;

  /**
   * Returns a list of object names for the {@link GatewaySenderMXBean}s
   * used to access all gateway senders.
   *
   * @return An array of object names or an empty array if no gateway senders
   *         are found.
   */
  public ObjectName[] listGatewaySenderObjectNames();

  /**
   * Returns a list of object names for the {@link GatewaySenderMXBean}s
   * used to access all gateway senders on a member.
   *
   * @param member
   *          Name or ID of the member.
   */
  public ObjectName[] listGatewaySenderObjectNames(String member)
      throws Exception;

  /**
   * Returns a list of object names for the {@link GatewayReceiverMXBean}s
   * used to access all gateway senders.
   *
   * @return An array of object names or an empty array if no gateway receivers
   *         are found.
   */
  public ObjectName[] listGatewayReceiverObjectNames();

  /**
   * Returns the object name for a {@link DistributedLockServiceMXBean} used to
   * access a distributed lock service.
   *
   * @param lockServiceName
   *          Name of the lock service.
   */
  public ObjectName fetchDistributedLockServiceObjectName(String lockServiceName)
      throws Exception;

  /**
   * Returns the object name for a {@link LockServiceMXBean} used to
   * access a lock service.
   *
   * @param member
   *          Name or Id of the member.
   * @param lockService
   *          Name of the lock service.
   */
  public ObjectName fetchLockServiceObjectName(String member,
      String lockService) throws Exception;

  /**
   * Returns object name of a {@link DiskStoreMXBean} for a given name and member
   *
   * @param member
   *          name or id of the member
   * @param diskStoreName
   *          name of the disk store
   * @return a ObjectName
   * @throws Exception
   */
  public ObjectName fetchDiskStoreObjectName(String member,
      String diskStoreName) throws Exception;

  /**
   * Returns the object name for a {@link CacheServerMXBean} used to access
   * a cache server.
   *
   * @param member
   *          Name or ID of the member.
   * @param port
   *          Port of the server.
   */
  public ObjectName fetchCacheServerObjectName(String member, int port)
      throws Exception;

  /**
   * Returns a list of object names for the {@link CacheServerMXBean}s
   * used to access all cache servers.
   */
  public ObjectName[] listCacheServerObjectNames();

  /**
   * Returns the number of map-reduce jobs currently running on all members
   * in the distributed system.
   */
  public int getNumRunningFunctions();

  /**
   * Returns the number of CQs registers on all members.
   */
  public long getRegisteredCQCount();

  /**
   * Returns the number of bytes used on all disks.
   */
  public long getTotalDiskUsage();

  /**
   * Returns the total heap used on all members.
   */
  public long getUsedHeapSize();

  /**
   * Returns the average number of reads per second for all members.
   */
  public float getAverageReads();

  /**
   * Returns the average writes per second, including both put and putAll operations,
   * for all members.
   */
  public float getAverageWrites();

  /**
   * Returns the number of subscriptions for all members.
   */
  public int getNumSubscriptions();


  /**
   * Returns the number of garbage collection operations for all members.
   */
  public long getGarbageCollectionCount();

  /**
   * Returns a map of remote distributed system IDs and the current connection
   * status for each.
   */
  public Map<String, Boolean> viewRemoteClusterStatus();

  /**
   * Returns the number JVM pauses (which may or may not include full garbage
   * collection pauses) detected by GemFire.
   */
  public long getJVMPauses();

  /**
   * This API is used to query data from GemFire system. This returns a JSON
   * formated String having data and it's type. Type and value of data makes an
   * array , type preceding the value.
   * 
   * e.g. {"result":[["java.lang.String","v"],["java.lang.String","b"]]}
   * 
   * GemFire PDXInstances are also supported. The type of PDXInstance is
   * PDXInstance and value will be key value pair. There is no marker to know
   * the "IdentityField" of the PDXInstance.
   * 
   * If the query is executed on the cluster and no member list is given in
   * input first key of the JSON string will be "result" followed by the result
   * set in JSON format.
   * 
   * If the query is executed on one or more specific members then returned
   * string will have an array of "member" and "result" keys.
   * 
   * For query on replicated region data from a random node which have the
   * region is shown, if no member input is given. For PR regions data from all
   * the nodes are collected and shown. User must be careful to query on a PR if
   * the region is big and hosted on a lot of nodes
   * 
   * Join queries on PR mandates that user provide one member as input. If the
   * member does not host the regions or the regions are not co-located error
   * string will be returned.
   * 
   * 
   * @param queryString
   *          GemFire supported OQL query
   * @param members
   *          comma separated list of members on which the query is to be
   *          executed. It is not mandatory to give this input barring join
   *          queries on PR. If member list is not provided query will be for
   *          the whole cluster.
   * @param limit
   *          result set limit. If not set or 0 is passed default limit of 1000
   *          will be set.
   * @return a JSON formated string containing data and its type
   */
  @ResourceOperation(resource = Resource.DATA, operation = Operation.READ)
  public String queryData(String queryString, String members, int limit) throws Exception;
  
  /**
   * 
   * Functionality is same as queryData() method. Only difference being the
   * resultant JSON string is compressed with Java GZIP with UTF-8 encoding. Any
   * client application can de compress the byte[] using GZIP.
   * 
   * e.g.
   * 
   * GZIPInputStream gis = new GZIPInputStream(new ByteArrayInputStream(bytes));
   * 
   * BufferedReader bf = new BufferedReader(new InputStreamReader(gis,"UTF-8"));
   * 
   * String outStr = ""; String line; while ((line = bf.readLine()) != null) {
   * outStr += line; }
   * 
   * @param queryString
   *          GemFire supported OQL query
   * @param members
   *          comma separated list of members on which the query is to be
   *          executed. It is not mandatory to give this input barring join
   *          queries on PR. If member list is not provided query will be for
   *          the whole cluster.
   * @param limit
   *          result set limit. If not set or 0 is passed default limit of 1000
   *          will be set.
   * @return a byte[] which is a compressed JSON string.
   */
  @ResourceOperation(resource = Resource.DATA, operation = Operation.READ)
  public byte[] queryDataForCompressedResult(String queryString, String members, int limit) throws Exception;
  
  
  /**
   * Returns the number of committed transactions across all members. It gives
   * point in time value i.e. Number of tx committed at the time of reading this
   * value
   */
  public int getTransactionCommitted();

  /**
   * Returns the number of transactions that were rolled back across all
   * members. It gives point in time value i.e. Number of tx rolled back at the
   * time of reading this value
   */
  public int getTransactionRolledBack();
  
  /**
   * Number of rows DistributedSystemMXBean.queryData() operation will return.
   * By default it will be 1000. User can modify this to control number of rows
   * to be shown on Pulse, as Pulse DataBrowser internally uses
   * DistributedSystemMXBean.queryData()
   */
  public int getQueryResultSetLimit();

  @ResourceOperation(resource = Resource.DATA, operation = Operation.MANAGE)
  public void setQueryResultSetLimit(int queryResultSetLimit);

  /**
   * Number of elements in a collection to be shown in queryData operation if
   * query results contain collections like Map, List etc.
   * 
   */
  public int getQueryCollectionsDepth();

  @ResourceOperation(resource = Resource.DATA, operation = Operation.MANAGE)
  public void setQueryCollectionsDepth(int queryCollectionsDepth);
}
