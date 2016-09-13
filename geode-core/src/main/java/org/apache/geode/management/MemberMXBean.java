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

import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

/**
 * MBean that provides access to information and management functionality for a
 * {@link DistributedMember} of the GemFire distributed system.
 * 
 * <p>
 * ObjectName of the MBean
 * :GemFire:type=Member,member=&ltname-or-dist-member-id&gt
 * 
 * <p>
 * There will be one instance of this MBean per GemFire node.
 * 
 * <p>
 * List of notification emitted by MemberMXBean.
 * 
 * <p>
 * <table border="1">
 * <tr>
 * <th>Notification Type</th>
 * <th>Notification Source</th>
 * <th>Message</th>
 * </tr>
 * <tr>
 * <td>gemfire.distributedsystem.cache.region.created</td>
 * <td>Member name or ID</td>
 * <td>Region Created with Name &ltRegion Name&gt</td>
 * </tr>
 * <tr>
 * <td>gemfire.distributedsystem.cache.region.closed</td>
 * <td>Member name or ID</td>
 * <td>Region Destroyed/Closed with Name &ltRegion Name&gt</td>
 * </tr>
 * <tr>
 * <td>gemfire.distributedsystem.cache.disk.created</td>
 * <td>Member name or ID</td>
 * <td>DiskStore Created with Name &ltDiskStore Name&gt</td>
 * </tr>
 * <tr>
 * <td>gemfire.distributedsystem.cache.disk.closed</td>
 * <td>Member name or ID</td>
 * <td>DiskStore Destroyed/Closed with Name &ltDiskStore Name&gt</td>
 * </tr>
 * <tr>
 * <td>gemfire.distributedsystem.cache.lockservice.created</td>
 * <td>Member name or ID</td>
 * <td>LockService Created with Name &ltLockService Name&gt</td>
 * </tr>
 * <tr>
 * <td>gemfire.distributedsystem.cache.lockservice.closed</td>
 * <td>Member name or ID</td>
 * <td>Lockservice Closed with Name &ltLockService Name&gt</td>
 * </tr>
 * <tr>
 * <td>gemfire.distributedsystem.gateway.sender.created</td>
 * <td>Member name or ID</td>
 * <td>GatewaySender Created in the VM</td>
 * </tr>
 * <tr>
 * <td>gemfire.distributedsystem.gateway.sender.started</td>
 * <td>Member name or ID</td>
 * <td>GatewaySender Started in the VM &ltSender Id&gt</td>
 * </tr>
 * <tr>
 * <td>gemfire.distributedsystem.gateway.sender.stopped</td>
 * <td>Member name or ID</td>
 * <td>GatewaySender Stopped in the VM &ltSender Id&gt</td>
 * </tr>
 * <tr>
 * <td>gemfire.distributedsystem.gateway.sender.paused</td>
 * <td>Member name or ID</td>
 * <td>GatewaySender Paused in the VM &ltSender Id&gt</td>
 * </tr>
 * <tr>
 * <td>gemfire.distributedsystem.gateway.sender.resumed</td>
 * <td>Member name or ID</td>
 * <td>GatewaySender Resumed in the VM &ltSender Id&gt</td>
 * </tr>
 * <tr>
 * <td>gemfire.distributedsystem.async.event.queue.created</td>
 * <td>Member name or ID</td>
 * <td>Async Event Queue is Created in the VM</td>
 * </tr>
 * <tr>
 * <td>gemfire.distributedsystem.gateway.receiver.created</td>
 * <td>Member name or ID</td>
 * <td>GatewayReceiver Created in the VM</td>
 * </tr>
 * <tr>
 * <td>gemfire.distributedsystem.gateway.receiver.started</td>
 * <td>Member name or ID</td>
 * <td>GatewayReceiver Started in the VM</td>
 * </tr>
 * <tr>
 * <td>gemfire.distributedsystem.gateway.receiver.stopped</td>
 * <td>Member name or ID</td>
 * <td>GatewayReceiver Stopped in the VM</td>
 * </tr>
 * <tr>
 * <td>gemfire.distributedsystem.cache.server.started</td>
 * <td>Member name or ID</td>
 * <td>Cache Server is Started in the VM</td>
 * </tr>
 * <tr>
 * <td>gemfire.distributedsystem.cache.server.stopped</td>
 * <td>Member name or ID</td>
 * <td>Cache Server is stopped in the VM</td>
 * </tr>
 * <tr>
 * <td>gemfire.distributedsystem.locator.started</td>
 * <td>Member name or ID</td>
 * <td>Locator is Started in the VM</td>
 * </tr>
 * </table>
 *
 * @since GemFire 7.0
 */
@ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
public interface MemberMXBean {

  /**
   * Returns the most recent log entries for the member.
   * 
   * @param numberOfLines
   *          Number of lines to return, up to a maximum of 100.
   */
  public String showLog(int numberOfLines);

  /**
   * Returns the license string for this member.
   *
   * @deprecated Removed licensing in 8.0.
   */
  @Deprecated
  public String viewLicense();

  /**
   * Performs compaction on all of the member's disk stores.
   * 
   * @return A list of names of the disk stores that were compacted.
   */
  @ResourceOperation(resource = Resource.DATA, operation = Operation.MANAGE)
  public String[] compactAllDiskStores();
  
  /**
   * Creates a Manager MBean on this member.
   * 
   * @return True if the Manager MBean was successfully created, false otherwise.
   */
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.MANAGE)
  public boolean createManager();
  
  /**
   * Shuts down the member. This is an asynchronous call and it will 
   * return immediately without waiting for a result.
   */
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.MANAGE)
  public void shutDownMember();
  
  /**
   * Returns JVM metrics.
   */
  public JVMMetrics showJVMMetrics();
  
  /**
   * Returns operating system metrics.
   */
  public OSMetrics showOSMetrics();

  /**
   * Executes a command on the member.
   * 
   * @param commandString
   *          Command to be executed.
   * 
   * @return Result of the execution in JSON format.
   */
  @ResourceOperation()
  String processCommand(String commandString);
  
  /**
   * Executes a command on the member.
   * 
   * @param commandString
   *          Command to be execute.
   * @param env
   *          Environmental properties to use during command execution.
   * @return Result of the execution in JSON format.
   */
  @ResourceOperation()
  String processCommand(String commandString, Map<String, String> env);
  
  /**
   * Executes a command on the member.
   * 
   * @param commandString
   *          Command to be execute.
   * @param env
   *          Environmental properties to use during command execution.
   * @param binaryData
   *          Binary data specific to the command being executed.
   * @return Result of the execution in JSON format.
   */
  @ResourceOperation()
  String processCommand(String commandString, Map<String, String> env, Byte[][] binaryData);

  /**
   * Returns the name of all disk stores in use by this member.
   * 
   * @param includeRegionOwned
   *          Whether to include disk stores owned by a region.
   */
  public String[] listDiskStores(boolean includeRegionOwned);

  /**
   * Returns the GemFire specific properties for this member.
   */
  public GemFireProperties listGemFireProperties();

  /**
   * Returns the name or IP address of the host on which this member is
   * running.
   */
  public String getHost();

  /**
   * Returns the name of this member.
   */
  public String getName();
  
  /**
   * Returns the ID of this member.
   */
  public String getId();
  
  /**
   * Returns the name of the member if it's been set, otherwise the ID.
   */
  public String getMember();
  
  /**
   * Returns the names of the groups this member belongs to.
   */
  public String[] getGroups();

  /**
   * Returns the operating system process ID.
   */
  public int getProcessId();

  /**
   * Returns the status.
   */
  public String status();

  /**
   * Returns the GemFire version.
   */
  public String getVersion();
 
  /**
   * Returns whether this member is attached to at least one Locator.
   * 
   * @return True if this member is attached to a Locator, false otherwise.
   */
  public boolean isLocator();

  /**
   * Returns the number of seconds that this member will wait for a
   * distributed lock.
   */
  public long getLockTimeout();

  /**
   * Returns the number of second that this member will lease a distributed
   * lock.
   */
  public long getLockLease();

  /**
   * Any long-running GemFire process that was started with "start server" command from GFSH.
   * It returns true even if that process has --disable-default-server=true.
   */
  public boolean isServer();

  /**
   * Returns whether this member has at least one GatewaySender.
   * 
   * @return True if this member has at least one GatwaySender, false otherwise.
   */
  public boolean hasGatewaySender();

  /**
   * Returns whether this member is running the Manager service.
   * 
   * @return True if this member is running the Manager service, false otherwise.
   */
  public boolean isManager();

  /**
   * Returns whether this member has created the Manager service (it may be
   * created, but not running).
   * 
   * @return True if this member has created the Manager service, false otherwise.
   */
  public boolean isManagerCreated();

  /**
   * Returns whether this member has at least one GatewayReceiver.
   * 
   * @return True if this member has at least one GatwayReceiver, false otherwise.
   */
  public boolean hasGatewayReceiver();

  /**
   * Returns the ClassPath.
   */
  public String getClassPath();

  /**
   * Returns the current time on the member's host.
   */
  public long getCurrentTime();

  /**
   * Returns the number of seconds that this member has been running.
   */
  public long getMemberUpTime();

  /**
   * Returns the time (as a percentage) that this member's process time with
   * respect to Statistics sample time interval. If process time between two
   * sample time t1 & t2 is p1 and p2 
   * cpuUsage = ((p2-p1) * 100) / ((t2-t1))
   * 
   * ProcessCpuTime is obtained from OperatingSystemMXBean.
   * If process CPU time is not available in the platform it will be shown as -1
   * 
   */
  public float getCpuUsage();

  /**
   * Returns the current size of the heap in megabytes.
   * @deprecated Please use {@link #getUsedMemory()} instead.
   */
  public long getCurrentHeapSize();

  /**
   * Returns the maximum size of the heap in megabytes.
   * @deprecated Please use {@link #getMaxMemory()} instead.
   */
  public long getMaximumHeapSize();

  /**
   * Returns the free heap size in megabytes.
   * @deprecated Please use {@link #getFreeMemory()} instead.
   */
  public long getFreeHeapSize();

  /**
   * Returns the maximum size of the heap in megabytes.
   * 
   */
  public long getMaxMemory();
  
  /**
   * Returns the free heap size in megabytes.
   */
  public long getFreeMemory();
  
  /**
   * Returns the current size of the heap in megabytes.
   */
  public long getUsedMemory();
  
  /**
   * Returns the current threads.
   */
  public String[] fetchJvmThreads();

  /**
   * Returns the maximum number of open file descriptors allowed for the member's
   * host operating system.
   */
  public long getFileDescriptorLimit();

  /**
   * Returns the current number of open file descriptors.
   */
  public long getTotalFileDescriptorOpen();

  /**
   * Returns the number of Regions present in the Cache.
   */
  public int getTotalRegionCount();

  /**
   * Returns the number of Partition Regions present in the Cache.
   */
  public int getPartitionRegionCount();

  /**
   * Returns a list of all Region names.
   */
  public String[] listRegions();
  
  
  /**
   * Returns a list of all disk stores, including those owned by a Region.
   */
  public String[] getDiskStores();
  
  /**
   * Returns a list of all root Region names.
   */
  public String[] getRootRegionNames();

  /**
   * Returns the total number of entries in all regions.
   */
  public int getTotalRegionEntryCount();

  /**
   * Returns the total number of buckets.
   */
  public int getTotalBucketCount();

  /**
   * Returns the number of buckets for which this member is the primary holder.
   */
  public int getTotalPrimaryBucketCount();

  /**
   * Returns the cache get average latency.
   */
  public long getGetsAvgLatency();

  /**
   * Returns the cache put average latency.
   */
  public long getPutsAvgLatency();

  /**
   * Returns the cache putAll average latency.
   */
  public long getPutAllAvgLatency();

  /**
   * Returns the number of times that a cache miss occurred for all regions.
   */
  public int getTotalMissCount();

  /**
   * Returns the number of times that a hit occurred for all regions.
   */
  public int getTotalHitCount();

  /**
   * Returns the number of gets per second.
   */
  public float getGetsRate();

  /**
   * Returns the number of puts per second. Only includes puts done explicitly
   * on this member's cache, not those pushed from another member.
   */
  public float getPutsRate();

  /**
   * Returns the number of putAlls per second.
   */
  public float getPutAllRate();

  /**
   * Returns the number of creates per second.
   */
  public float getCreatesRate();
  
  /**
   * Returns the number of destroys per second.
   */
  public float getDestroysRate();

  /**
   * Returns the average latency of a call to a CacheWriter.
   */
  public long getCacheWriterCallsAvgLatency();

  /**
   * Returns the average latency of a call to a CacheListener.
   */
  public long getCacheListenerCallsAvgLatency();

  /**
   * Returns the total number of times that a load on this cache has completed,
   * as a result of either a local get or a remote net load.
   */
  public int getTotalLoadsCompleted();
  
  /**
   * Returns the average latency of a load.
   */
  public long getLoadsAverageLatency();
  
  /**
   * Returns the total number of times the a network load initiated by this cache
   * has completed.
   */
  public int getTotalNetLoadsCompleted();
  
  /**
   * Returns the net load average latency.
   */
  public long getNetLoadsAverageLatency();
  
  /**
   * Returns the total number of times that a network search initiated by this cache 
   * has completed.
   */
  public int getTotalNetSearchCompleted();

  /**
   * Returns the net search average latency.
   */
  public long getNetSearchAverageLatency();
  
  /**
   * Returns the current number of disk tasks (op-log compaction, asynchronous
   * recovery, etc.) that are waiting for a thread to run.
   */
  public int getTotalDiskTasksWaiting();

  /**
   * Returns the average number of bytes per second sent.
   */
  public float getBytesSentRate();

  /**
   * Returns the average number of bytes per second received.
   */
  public float getBytesReceivedRate();

  /**
   * Returns a list of IDs for all connected gateway receivers.
   */
  public String[] listConnectedGatewayReceivers();

  /**
   * Returns a list of IDs for all gateway senders.
   */
  public String[] listConnectedGatewaySenders();

  /**
   * Returns the number of currently executing functions.
   */
  public int getNumRunningFunctions();

  /**
   * Returns the average function execution rate.
   */
  public float getFunctionExecutionRate();

  /**
   * Returns the number of currently executing functions that will return
   * resutls.
   */
  public int getNumRunningFunctionsHavingResults();

  /**
   * Returns the number of current transactions.
   */
  public int getTotalTransactionsCount();

  /**
   * Returns the average commit latency in nanoseconds .
   */
  public long getTransactionCommitsAvgLatency();

  /**
   * Returns the number of committed transactions.
   */
  public int getTransactionCommittedTotalCount();

  /**
   * Returns the number of transactions that were rolled back.
   */
  public int getTransactionRolledBackTotalCount();

  /**
   * Returns the average number of transactions committed per second.
   */
  public float getTransactionCommitsRate();
  

  /**
   * Returns the number of bytes reads per second from all the disks of the member. 
   */
  public float getDiskReadsRate();

  /**
   * Returns the number of bytes written per second to disk to all the disks of the member. 
   */
  public float getDiskWritesRate();

  /**
   * Returns the average disk flush latency time in nanoseconds. 
   */
  public long getDiskFlushAvgLatency();

  /**
   * Returns the number of backups currently in progress for all disk stores.
   */
  public int getTotalBackupInProgress();
  
  /**
   * Returns the number of backups that have been completed.
   */
  public int getTotalBackupCompleted();
  
  /**
   * Returns the number of threads waiting for a lock.
   */
  public int getLockWaitsInProgress();

  /**
   * Returns the amount of time (in milliseconds) spent waiting for a lock.
   */
  public long getTotalLockWaitTime();

  /**
   * Returns the number of lock services in use.
   */
  public int getTotalNumberOfLockService();
  
  /**
   * Returns the number of locks for which this member is a granter.
   */
  public int getTotalNumberOfGrantors();

  /**
   * Returns the number of lock request queues in use by this member.
   */
  public int getLockRequestQueues();

  /**
   * Returns the entry eviction rate as triggered by the LRU policy.
   */
  public float getLruEvictionRate();

  /**
   * Returns the rate of entries destroyed either by destroy cache operations or
   * eviction.
   */
  public float getLruDestroyRate();

  /**
   * Returns the number of initial images in progress.
   */
  public int getInitialImagesInProgres();

  /**
   * Returns the total amount of time spent performing a "get initial image"
   * operation when creating a region.
   */
  public long getInitialImageTime();

  /**
   * Return the number of keys received while performing a "get initial image"
   * operation when creating a region.
   */
  public int getInitialImageKeysReceived();
  
  /**
   * Returns the average time (in nanoseconds) spent deserializing objects.
   * Includes deserializations that result in a PdxInstance.
   */
  public long getDeserializationAvgLatency();

  /**
   * Returns the average latency (in nanoseconds) spent deserializing objects.
   * Includes deserializations that result in a PdxInstance.
   */
  public long getDeserializationLatency();

  /**
   * Returns the instantaneous rate of deserializing objects.
   * Includes deserializations that result in a PdxInstance.
   */
  public float getDeserializationRate();

  /**
   * Returns the average time (in nanoseconds) spent serializing objects.
   * Includes serializations that result in a PdxInstance.
   */
  public long getSerializationAvgLatency();

  /**
   * Returns the average latency (in nanoseconds) spent serializing objects.
   * Includes serializations that result in a PdxInstance.
   */
  public long getSerializationLatency();

  /**
   * Returns the instantaneous rate of serializing objects.
   * Includes serializations that result in a PdxInstance.
   */
  public float getSerializationRate();

  /**
   * Returns the instantaneous rate of PDX instance deserialization.
   */
  public float getPDXDeserializationRate();

  /**
   * Returns the average time, in seconds, spent deserializing PDX instanced.
   */
  public long getPDXDeserializationAvgLatency();

  /**
   * Returns the total number of bytes used on all disks.
   */
  public long getTotalDiskUsage();
  
  /**
   * Returns the number of threads in use.
   */
  public int getNumThreads();
  
  /**
   * Returns the system load average for the last minute. The system load
   * average is the sum of the number of runnable entities queued to the
   * available processors and the number of runnable entities running on the
   * available processors averaged over a period of time.
   * 
   * Pulse Attribute
   * 
   * @return The load average or a negative value if one is not available.
   */
  public double getLoadAverage();
  
  /**
   * Returns the number of times garbage collection has occurred.
   */
  public long getGarbageCollectionCount();

  /**
   * Returns the amount of time (in milliseconds) spent on garbage collection.
   */
  public long getGarbageCollectionTime();  
  
  /**
   * Returns the average number of reads per second.
   */
  public float getAverageReads();

  /**
   * Returns the average writes per second, including both put and putAll operations.
   */
  public float getAverageWrites();
  
  /**
   * Returns the number JVM pauses (which may or may not include full garbage
   * collection pauses) detected by GemFire.
   */
  public long getJVMPauses();
  
  
  /**
   * Returns the underlying host's current cpuActive percentage 
   */
  public int getHostCpuUsage();
  
  /**
   * 
   * Returns true if a cache server is running on this member and able server requests from GemFire clients
   */
  public boolean isCacheServer();
  
  /**
   * Returns the redundancy-zone of the member;
   */
  public String getRedundancyZone();
  
  /**
   * Returns current number of cache rebalance operations being directed by this process. 
   */
  public int getRebalancesInProgress();
  
  /**
   * Returns current number of threads waiting for a reply.
   */
  public int getReplyWaitsInProgress();
  
  /**
   * Returns total number of times waits for a reply have completed. 
   */
  public int getReplyWaitsCompleted();
  
  /**
   * The current number of nodes in this distributed system visible to this member. 
   */
  public int getVisibleNodes();

  /**
   * Returns the number of off heap objects.
   */
  public int getOffHeapObjects();
  
  /**
   * Returns the size of the maximum configured off-heap memory in bytes.
   */
  public long getOffHeapMaxMemory();
  
  /**
   * Returns the size of available (or unallocated) off-heap memory in bytes.
   */
  public long getOffHeapFreeMemory();
  
  /**
   * Returns the size of utilized off-heap memory in bytes.
   */
  public long getOffHeapUsedMemory();

  /**
   * Returns the percentage of off-heap memory fragmentation.
   */
  public int getOffHeapFragmentation();
  
  /**
   * Returns the total time spent compacting in millseconds.
   */
  public long getOffHeapCompactionTime();
}
