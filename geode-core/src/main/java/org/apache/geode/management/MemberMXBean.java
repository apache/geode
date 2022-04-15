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
package org.apache.geode.management;

import java.util.List;
import java.util.Map;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;
import org.apache.geode.security.ResourcePermission.Target;

/**
 * MBean that provides access to information and management functionality for a
 * {@link DistributedMember} of the GemFire distributed system.
 *
 * <p>
 * ObjectName of the MBean :GemFire:type=Member,member=&lt;name-or-dist-member-id&gt;
 *
 * <p>
 * There will be one instance of this MBean per GemFire node.
 *
 * <p>
 * List of notification emitted by MemberMXBean.
 *
 * <table border="1" summary="">
 * <tr>
 * <th>Notification Type</th>
 * <th>Notification Source</th>
 * <th>Message</th>
 * </tr>
 * <tr>
 * <td>gemfire.distributedsystem.cache.region.created</td>
 * <td>Member name or ID</td>
 * <td>Region Created with Name &lt;Region Name&gt;</td>
 * </tr>
 * <tr>
 * <td>gemfire.distributedsystem.cache.region.closed</td>
 * <td>Member name or ID</td>
 * <td>Region Destroyed/Closed with Name &lt;Region Name&gt;</td>
 * </tr>
 * <tr>
 * <td>gemfire.distributedsystem.cache.disk.created</td>
 * <td>Member name or ID</td>
 * <td>DiskStore Created with Name &lt;DiskStore Name&gt;</td>
 * </tr>
 * <tr>
 * <td>gemfire.distributedsystem.cache.disk.closed</td>
 * <td>Member name or ID</td>
 * <td>DiskStore Destroyed/Closed with Name &lt;DiskStore Name&gt;</td>
 * </tr>
 * <tr>
 * <td>gemfire.distributedsystem.cache.lockservice.created</td>
 * <td>Member name or ID</td>
 * <td>LockService Created with Name &lt;LockService Name&gt;</td>
 * </tr>
 * <tr>
 * <td>gemfire.distributedsystem.cache.lockservice.closed</td>
 * <td>Member name or ID</td>
 * <td>Lockservice Closed with Name &lt;LockService Name&gt;</td>
 * </tr>
 * <tr>
 * <td>gemfire.distributedsystem.gateway.sender.created</td>
 * <td>Member name or ID</td>
 * <td>GatewaySender Created in the VM</td>
 * </tr>
 * <tr>
 * <td>gemfire.distributedsystem.gateway.sender.started</td>
 * <td>Member name or ID</td>
 * <td>GatewaySender Started in the VM &lt;Sender Id&gt;</td>
 * </tr>
 * <tr>
 * <td>gemfire.distributedsystem.gateway.sender.stopped</td>
 * <td>Member name or ID</td>
 * <td>GatewaySender Stopped in the VM &lt;Sender Id&gt;</td>
 * </tr>
 * <tr>
 * <td>gemfire.distributedsystem.gateway.sender.paused</td>
 * <td>Member name or ID</td>
 * <td>GatewaySender Paused in the VM &lt;Sender Id&gt;</td>
 * </tr>
 * <tr>
 * <td>gemfire.distributedsystem.gateway.sender.resumed</td>
 * <td>Member name or ID</td>
 * <td>GatewaySender Resumed in the VM &lt;Sender Id&gt;</td>
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
   * @param numberOfLines Number of lines to return, up to a maximum of 100.
   * @return the most recent log entries for the member
   */
  String showLog(int numberOfLines);

  /**
   * Returns the license string for this member.
   *
   * @return the license string for this member
   *
   * @deprecated Removed licensing in 8.0.
   */
  @Deprecated
  String viewLicense();

  /**
   * Performs compaction on all of the member's disk stores.
   *
   * @return A list of names of the disk stores that were compacted.
   */
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.MANAGE,
      target = Target.DISK)
  String[] compactAllDiskStores();

  /**
   * Creates a Manager MBean on this member.
   *
   * @return True if the Manager MBean was successfully created, false otherwise.
   */
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.MANAGE)
  boolean createManager();

  /**
   * Shuts down the member. This is an asynchronous call and it will return immediately without
   * waiting for a result.
   */
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.MANAGE)
  void shutDownMember();

  /**
   * Returns JVM metrics.
   *
   * @return JVM metrics
   */
  JVMMetrics showJVMMetrics();

  /**
   * Returns operating system metrics.
   *
   * @return operating system metrics
   */
  OSMetrics showOSMetrics();

  /**
   * Executes a command on the member.
   *
   * @param commandString Command to be executed.
   *
   * @return Result of the execution in JSON format.
   */
  @ResourceOperation()
  String processCommand(String commandString);

  /**
   * Executes a command on the member.
   *
   * @param commandString Command to be execute.
   * @param env Environmental properties to use during command execution.
   * @return Result of the execution in JSON format.
   */
  @ResourceOperation()
  String processCommand(String commandString, Map<String, String> env);

  /**
   * Executes a command on the member. this is the method that's used by the HttpOperationInvoker
   * and JmxOperationInvoker
   *
   * @param commandString Command to be execute.
   * @param env Environmental properties to use during command execution.
   * @param stagedFilePaths Local files (as relevant to the command). May be null.
   * @return Result of the execution in JSON format.
   */
  @ResourceOperation()
  String processCommand(String commandString, Map<String, String> env,
      List<String> stagedFilePaths);

  /**
   * Executes a command on the member. this is the method that's used by the HttpOperationInvoker
   * and JmxOperationInvoker
   *
   * @param commandString Command to be execute.
   * @param env Environmental properties to use during command execution.
   * @param binaryData Binary data specific to the command being executed.
   * @return Result of the execution in JSON format.
   *
   * @deprecated since 1.4 use processCommand(String commandString, Map$lt;String, String$gt; env,
   *             List$lt;String$gt; stagedFilePaths) instead
   */
  @Deprecated
  @ResourceOperation()
  String processCommand(String commandString, Map<String, String> env, Byte[][] binaryData);

  /**
   * Returns the name of all disk stores in use by this member.
   *
   * @param includeRegionOwned Whether to include disk stores owned by a region.
   * @return the name of all disk stores in use by this member
   */
  String[] listDiskStores(boolean includeRegionOwned);

  /**
   * Returns the GemFire specific properties for this member.
   *
   * @return the GemFire specific properties for this member
   */
  GemFireProperties listGemFireProperties();

  /**
   * Returns the name or IP address of the host on which this member is running.
   *
   * @return the name or IP address of the host on which this member is running
   */
  String getHost();

  /**
   * Returns the name of this member.
   *
   * @return the name of this member
   */
  String getName();

  /**
   * Returns the ID of this member.
   *
   * @return the ID of this member
   */
  String getId();

  /**
   * Returns the name of the member if it's been set, otherwise the ID.
   *
   * @return the name of the member if it's been set, otherwise the ID
   */
  String getMember();

  /**
   * Returns the names of the groups this member belongs to.
   *
   * @return the names of the groups this member belongs to
   */
  String[] getGroups();

  /**
   * Returns the operating system process ID.
   *
   * @return the operating system process ID
   */
  int getProcessId();

  /**
   * Returns the status.
   *
   * @return the status
   */
  String status();

  /**
   * Returns the GemFire version, including build id, jdk version, product name and release version
   * etc.
   *
   * @return the GemFire version, including build id, jdk version, product name and release version
   */
  @ResourceOperation()
  String getVersion();

  /**
   * returns only the version string
   *
   * @return the version string
   */
  @ResourceOperation()
  String getReleaseVersion();

  /**
   * returns only the Geode version string
   *
   * @return the Geode version string
   */
  @ResourceOperation()
  String getGeodeReleaseVersion();

  /**
   * Returns whether this member is attached to at least one Locator.
   *
   * @return True if this member is attached to a Locator, false otherwise.
   */
  boolean isLocator();

  /**
   * Returns the number of seconds that this member will wait for a distributed lock.
   *
   * @return the number of seconds that this member will wait for a distributed lock
   */
  long getLockTimeout();

  /**
   * Returns the number of second that this member will lease a distributed lock.
   *
   * @return the number of second that this member will lease a distributed lock
   */
  long getLockLease();

  /**
   * Any long-running GemFire process that was started with "start server" command from GFSH. It
   * returns true even if that process has --disable-default-server=true.
   *
   * @return whether the process was started with "start server" command from GFSH
   */
  boolean isServer();

  /**
   * Returns whether this member has at least one GatewaySender.
   *
   * @return True if this member has at least one GatewaySender, false otherwise.
   */
  boolean hasGatewaySender();

  /**
   * Returns whether this member is running the Manager service.
   *
   * @return True if this member is running the Manager service, false otherwise.
   */
  boolean isManager();

  /**
   * Returns whether this member has created the Manager service (it may be created, but not
   * running).
   *
   * @return True if this member has created the Manager service, false otherwise.
   */
  boolean isManagerCreated();

  /**
   * Returns whether this member has at least one GatewayReceiver.
   *
   * @return True if this member has at least one GatewayReceiver, false otherwise.
   */
  boolean hasGatewayReceiver();

  /**
   * Returns the ClassPath.
   *
   * @return the ClassPath
   */
  String getClassPath();

  /**
   * Returns the current time on the member's host.
   *
   * @return the current time on the member's host
   */
  long getCurrentTime();

  /**
   * Returns the number of seconds that this member has been running.
   *
   * @return the number of seconds that this member has been running
   */
  long getMemberUpTime();

  /**
   * Returns the time (as a percentage) that this member's process time with respect to Statistics
   * sample time interval. If process time between two sample time t1 &amp; t2 is p1 and p2 cpuUsage
   * =
   * ((p2-p1) * 100) / ((t2-t1))
   *
   * ProcessCpuTime is obtained from OperatingSystemMXBean. If process CPU time is not available in
   * the platform it will be shown as -1
   *
   * @return the CPU usage as a percentage
   */
  float getCpuUsage();

  /**
   * Returns the current size of the heap in megabytes.
   *
   * @return the current size of the heap in megabytes
   *
   * @deprecated Please use {@link #getUsedMemory()} instead.
   */
  @Deprecated
  long getCurrentHeapSize();

  /**
   * Returns the maximum size of the heap in megabytes.
   *
   * @return the maximum size of the heap in megabytes
   *
   * @deprecated Please use {@link #getMaxMemory()} instead.
   */
  @Deprecated
  long getMaximumHeapSize();

  /**
   * Returns the free heap size in megabytes.
   *
   * @return the free heap size in megabytes
   *
   * @deprecated Please use {@link #getFreeMemory()} instead.
   */
  @Deprecated
  long getFreeHeapSize();

  /**
   * Returns the maximum size of the heap in megabytes.
   *
   * @return the maximum size of the heap in megabytes
   */
  long getMaxMemory();

  /**
   * Returns the free heap size in megabytes.
   *
   * @return the free heap size in megabytes
   */
  long getFreeMemory();

  /**
   * Returns the current size of the heap in megabytes.
   *
   * @return the current size of the heap in megabytes
   */
  long getUsedMemory();

  /**
   * Returns the current threads.
   *
   * @return the current threads
   */
  String[] fetchJvmThreads();

  /**
   * Returns the maximum number of open file descriptors allowed for the member's host operating
   * system.
   *
   * @return the maximum number of open file descriptors allowed for the member's host operating
   *         system
   */
  long getFileDescriptorLimit();

  /**
   * Returns the current number of open file descriptors.
   *
   * @return the current number of open file descriptors
   */
  long getTotalFileDescriptorOpen();

  /**
   * Returns the number of Regions present in the Cache.
   *
   * @return the number of Regions present in the Cache
   */
  int getTotalRegionCount();

  /**
   * Returns the number of Partition Regions present in the Cache.
   *
   * @return the number of Partition Regions present in the Cache
   */
  int getPartitionRegionCount();

  /**
   * Returns a list of all Region names.
   *
   * @return an array of all Region names
   */
  String[] listRegions();


  /**
   * Returns a list of all disk stores, including those owned by a Region.
   *
   * @return an array of all disk stores, including those owned by a Region
   */
  String[] getDiskStores();

  /**
   * Returns a list of all root Region names.
   *
   * @return an array of all root Region names
   */
  String[] getRootRegionNames();

  /**
   * Returns the total number of entries in all regions.
   *
   * @return the total number of entries in all regions
   */
  int getTotalRegionEntryCount();

  /**
   * Returns the total number of buckets.
   *
   * @return the total number of buckets
   */
  int getTotalBucketCount();

  /**
   * Returns the number of buckets for which this member is the primary holder.
   *
   * @return the number of buckets for which this member is the primary holder
   */
  int getTotalPrimaryBucketCount();

  /**
   * Returns the cache get average latency.
   *
   * @return the cache get average latency
   */
  long getGetsAvgLatency();

  /**
   * Returns the cache put average latency.
   *
   * @return the cache put average latency
   */
  long getPutsAvgLatency();

  /**
   * Returns the cache putAll average latency.
   *
   * @return the cache putAll average latency
   */
  long getPutAllAvgLatency();

  /**
   * Returns the number of times that a cache miss occurred for all regions.
   *
   * @return the number of times that a cache miss occurred for all regions
   */
  int getTotalMissCount();

  /**
   * Returns the number of times that a hit occurred for all regions.
   *
   * @return the number of times that a hit occurred for all regions
   */
  int getTotalHitCount();

  /**
   * Returns the number of gets per second.
   *
   * @return the number of gets per second
   */
  float getGetsRate();

  /**
   * Returns the number of puts per second. Only includes puts done explicitly on this member's
   * cache, not those pushed from another member.
   *
   * @return the number of puts per second
   */
  float getPutsRate();

  /**
   * Returns the number of putAlls per second.
   *
   * @return the number of putAlls per second
   */
  float getPutAllRate();

  /**
   * Returns the number of creates per second.
   *
   * @return the number of creates per second
   */
  float getCreatesRate();

  /**
   * Returns the number of destroys per second.
   *
   * @return the number of destroys per second
   */
  float getDestroysRate();

  /**
   * Returns the average latency of a call to a CacheWriter.
   *
   * @return the average latency of a call to a CacheWriter
   */
  long getCacheWriterCallsAvgLatency();

  /**
   * Returns the average latency of a call to a CacheListener.
   *
   * @return the average latency of a call to a CacheListener
   */
  long getCacheListenerCallsAvgLatency();

  /**
   * Returns the total number of times that a load on this cache has completed, as a result of
   * either a local get or a remote net load.
   *
   * @return the total number of times that a load on this cache has completed
   */
  int getTotalLoadsCompleted();

  /**
   * Returns the average latency of a load.
   *
   * @return the average latency of a load
   */
  long getLoadsAverageLatency();

  /**
   * Returns the total number of times that a network load initiated by this cache has completed.
   *
   * @return the total number of times that a network load initiated by this cache has completed
   */
  int getTotalNetLoadsCompleted();

  /**
   * Returns the net load average latency.
   *
   * @return the net load average latency
   */
  long getNetLoadsAverageLatency();

  /**
   * Returns the total number of times that a network search initiated by this cache has completed.
   *
   * @return the total number of times that a network search initiated by this cache has completed
   */
  int getTotalNetSearchCompleted();

  /**
   * Returns the net search average latency.
   *
   * @return the net search average latency
   */
  long getNetSearchAverageLatency();

  /**
   * Returns the current number of disk tasks (op-log compaction, asynchronous recovery, etc.) that
   * are waiting for a thread to run.
   *
   * @return the current number of disk tasks that are waiting for a thread to run
   */
  int getTotalDiskTasksWaiting();

  /**
   * Returns the average number of bytes per second sent.
   *
   * @return the average number of bytes per second sent
   */
  float getBytesSentRate();

  /**
   * Returns the average number of bytes per second received.
   *
   * @return the average number of bytes per second received
   */
  float getBytesReceivedRate();

  /**
   * Returns a list of IDs for all connected gateway receivers.
   *
   * @return a list of IDs for all connected gateway receivers
   */
  String[] listConnectedGatewayReceivers();

  /**
   * Returns a list of IDs for all gateway senders.
   *
   * @return a list of IDs for all gateway senders
   */
  String[] listConnectedGatewaySenders();

  /**
   * Returns the number of currently executing functions.
   *
   * @return the number of currently executing functions
   */
  int getNumRunningFunctions();

  /**
   * Returns the average function execution rate.
   *
   * @return the average function execution rate
   */
  float getFunctionExecutionRate();

  /**
   * Returns the number of currently executing functions that will return results.
   *
   * @return the number of currently executing functions that will return results
   */
  int getNumRunningFunctionsHavingResults();

  /**
   * Returns the number of current transactions.
   *
   * @return the number of current transactions
   */
  int getTotalTransactionsCount();

  /**
   * Returns the average commit latency in nanoseconds.
   *
   * @return the average commit latency in nanoseconds
   */
  long getTransactionCommitsAvgLatency();

  /**
   * Returns the number of committed transactions.
   *
   * @return the number of committed transactions
   */
  int getTransactionCommittedTotalCount();

  /**
   * Returns the number of transactions that were rolled back.
   *
   * @return the number of transactions that were rolled back
   */
  int getTransactionRolledBackTotalCount();

  /**
   * Returns the average number of transactions committed per second.
   *
   * @return the average number of transactions committed per second
   */
  float getTransactionCommitsRate();


  /**
   * Returns the number of bytes reads per second from all the disks of the member.
   *
   * @return the number of bytes reads per second from all the disks of the member
   */
  float getDiskReadsRate();

  /**
   * Returns the number of bytes written per second to disk to all the disks of the member.
   *
   * @return the number of bytes written per second to disk to all the disks of the member
   */
  float getDiskWritesRate();

  /**
   * Returns the average disk flush latency time in nanoseconds.
   *
   * @return the average disk flush latency time in nanoseconds
   */
  long getDiskFlushAvgLatency();

  /**
   * Returns the number of backups currently in progress for all disk stores.
   *
   * @return the number of backups currently in progress for all disk stores
   */
  int getTotalBackupInProgress();

  /**
   * Returns the number of backups that have been completed.
   *
   * @return the number of backups that have been completed
   */
  int getTotalBackupCompleted();

  /**
   * Returns the number of threads waiting for a lock.
   *
   * @return the number of threads waiting for a lock
   */
  int getLockWaitsInProgress();

  /**
   * Returns the amount of time (in milliseconds) spent waiting for a lock.
   *
   * @return the amount of time (in milliseconds) spent waiting for a lock
   */
  long getTotalLockWaitTime();

  /**
   * Returns the number of lock services in use.
   *
   * @return the number of lock services in use
   */
  int getTotalNumberOfLockService();

  /**
   * Returns the number of locks for which this member is a granter.
   *
   * @return the number of locks for which this member is a granter
   */
  int getTotalNumberOfGrantors();

  /**
   * Returns the number of lock request queues in use by this member.
   *
   * @return the number of lock request queues in use by this member
   */
  int getLockRequestQueues();

  /**
   * Returns the entry eviction rate as triggered by the LRU policy.
   *
   * @return the entry eviction rate as triggered by the LRU policy
   */
  float getLruEvictionRate();

  /**
   * Returns the rate of entries destroyed either by destroy cache operations or eviction.
   *
   * @return the rate of entries destroyed either by destroy cache operations or eviction
   */
  float getLruDestroyRate();

  /**
   * Returns tthe number of "get initial image" operations in progress.
   *
   * @return the number of "get initial image" operations in progress
   *
   * @deprecated as typo in name has been corrected: use
   *             {@link MemberMXBean#getInitialImagesInProgress} instead.
   */
  @Deprecated
  int getInitialImagesInProgres();

  /**
   * Returns the number of "get initial image" operations in progress.
   *
   * @return the number of "get initial image" operations in progress
   */
  int getInitialImagesInProgress();

  /**
   * Returns the total amount of time spent performing a "get initial image" operation when creating
   * a region.
   *
   * @return the total amount of time spent performing a "get initial image" operation
   */
  long getInitialImageTime();

  /**
   * Return the number of keys received while performing a "get initial image" operation when
   * creating a region.
   *
   * @return the number of keys received while performing a "get initial image" operation
   */
  int getInitialImageKeysReceived();

  /**
   * Returns the average time (in nanoseconds) spent deserializing objects. Includes
   * deserializations that result in a PdxInstance.
   *
   * @return the average time (in nanoseconds) spent deserializing objects
   */
  long getDeserializationAvgLatency();

  /**
   * Returns the average latency (in nanoseconds) spent deserializing objects. Includes
   * deserializations that result in a PdxInstance.
   *
   * @return the average latency (in nanoseconds) spent deserializing objects
   */
  long getDeserializationLatency();

  /**
   * Returns the instantaneous rate of deserializing objects. Includes deserializations that result
   * in a PdxInstance.
   *
   * @return the instantaneous rate of deserializing objects
   */
  float getDeserializationRate();

  /**
   * Returns the average time (in nanoseconds) spent serializing objects. Includes serializations
   * that result in a PdxInstance.
   *
   * @return he average time (in nanoseconds) spent serializing objects
   */
  long getSerializationAvgLatency();

  /**
   * Returns the average latency (in nanoseconds) spent serializing objects. Includes serializations
   * that result in a PdxInstance.
   *
   * @return the average latency (in nanoseconds) spent serializing objects
   */
  long getSerializationLatency();

  /**
   * Returns the instantaneous rate of serializing objects. Includes serializations that result in a
   * PdxInstance.
   *
   * @return the instantaneous rate of serializing objects
   */
  float getSerializationRate();

  /**
   * Returns the instantaneous rate of PDX instance deserialization.
   *
   * @return the instantaneous rate of PDX instance deserialization
   */
  float getPDXDeserializationRate();

  /**
   * Returns the average time, in seconds, spent deserializing PDX instanced.
   *
   * @return the average time, in seconds, spent deserializing PDX instanced
   */
  long getPDXDeserializationAvgLatency();

  /**
   * Returns the total number of bytes used on all disks.
   *
   * @return the total number of bytes used on all disks
   */
  long getTotalDiskUsage();

  /**
   * Returns the number of threads in use.
   *
   * @return the number of threads in use
   */
  int getNumThreads();

  /**
   * Returns the system load average for the last minute. The system load average is the sum of the
   * number of runnable entities queued to the available processors and the number of runnable
   * entities running on the available processors averaged over a period of time.
   *
   * Pulse Attribute
   *
   * @return The load average or a negative value if one is not available.
   */
  double getLoadAverage();

  /**
   * Returns the number of times garbage collection has occurred.
   *
   * @return the number of times garbage collection has occurred
   */
  long getGarbageCollectionCount();

  /**
   * Returns the amount of time (in milliseconds) spent on garbage collection.
   *
   * @return the amount of time (in milliseconds) spent on garbage collection
   */
  long getGarbageCollectionTime();

  /**
   * Returns the average number of reads per second.
   *
   * @return the average number of reads per second
   */
  float getAverageReads();

  /**
   * Returns the average writes per second, including both put and putAll operations.
   *
   * @return the average writes per second, including both put and putAll operations
   */
  float getAverageWrites();

  /**
   * Returns the number JVM pauses (which may or may not include full garbage collection pauses)
   * detected by GemFire.
   *
   * @return the number JVM pauses detected
   */
  long getJVMPauses();


  /**
   * Returns the underlying host's current cpuActive percentage
   *
   * @return the underlying host's current cpuActive percentage
   */
  int getHostCpuUsage();

  /**
   * Returns true if a cache server is running on this member and able to serve requests from
   * clients
   *
   * @return whether a cache server is running on this member and able to serve requests from
   *         clients
   */
  boolean isCacheServer();

  /**
   * Returns the redundancy-zone of the member.
   *
   * @return the redundancy-zone of the member
   */
  String getRedundancyZone();

  /**
   * Returns current number of cache rebalance operations being directed by this process.
   *
   * @return current number of cache rebalance operations being directed by this process
   */
  int getRebalancesInProgress();

  /**
   * Returns current number of threads waiting for a reply.
   *
   * @return current number of threads waiting for a reply
   */
  int getReplyWaitsInProgress();

  /**
   * Returns total number of times waits for a reply have completed.
   *
   * @return total number of times waits for a reply have completed
   */
  int getReplyWaitsCompleted();

  /**
   * The current number of nodes in this distributed system visible to this member.
   *
   * @return the current number of nodes in this distributed system visible to this member
   */
  int getVisibleNodes();

  /**
   * Returns the number of off heap objects.
   *
   * @return the number of off heap objects
   */
  int getOffHeapObjects();

  /**
   * Returns the size of the maximum configured off-heap memory in bytes.
   *
   * @return the size of the maximum configured off-heap memory in bytes
   */
  long getOffHeapMaxMemory();

  /**
   * Returns the size of available (or unallocated) off-heap memory in bytes.
   *
   * @return the size of available (or unallocated) off-heap memory in bytes
   */
  long getOffHeapFreeMemory();

  /**
   * Returns the size of utilized off-heap memory in bytes.
   *
   * @return the size of utilized off-heap memory in bytes
   */
  long getOffHeapUsedMemory();

  /**
   * Returns the percentage of off-heap memory fragmentation.
   *
   * @return the percentage of off-heap memory fragmentation
   */
  int getOffHeapFragmentation();

  long getOffHeapFragments();

  long getOffHeapFreedChunks();

  int getOffHeapLargestFragment();

  /**
   * Returns the total time spent compacting in milliseconds.
   *
   * @return the total time spent compacting in milliseconds
   */
  long getOffHeapCompactionTime();
}
