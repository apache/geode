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
package org.apache.geode.management.internal.beans;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.management.NotificationBroadcasterSupport;

import org.apache.commons.io.FileUtils;

import org.apache.geode.management.GemFireProperties;
import org.apache.geode.management.JVMMetrics;
import org.apache.geode.management.MemberMXBean;
import org.apache.geode.management.OSMetrics;
import org.apache.geode.management.internal.util.ManagementUtils;

/**
 * This MBean is a gateway to cache and a member
 *
 *
 */
public class MemberMBean extends NotificationBroadcasterSupport implements MemberMXBean {

  private MemberMBeanBridge bridge;

  public MemberMBean(MemberMBeanBridge bridge) {
    this.bridge = bridge;
  }

  @Override
  public String[] compactAllDiskStores() {
    return bridge.compactAllDiskStores();
  }

  @Override
  public String viewLicense() {
    return "";
  }

  @Override
  public String showLog(int numLines) {
    return bridge.fetchLog(numLines);
  }

  @Override
  public float getBytesReceivedRate() {
    return bridge.getBytesReceivedRate();
  }

  @Override
  public float getBytesSentRate() {
    return bridge.getBytesSentRate();
  }

  @Override
  public long getCacheListenerCallsAvgLatency() {
    return bridge.getCacheListenerCallsAvgLatency();
  }

  @Override
  public long getCacheWriterCallsAvgLatency() {
    return bridge.getCacheWriterCallsAvgLatency();
  }

  @Override
  public String[] listConnectedGatewayReceivers() {
    return bridge.listConnectedGatewayReceivers();
  }

  @Override
  public String[] listConnectedGatewaySenders() {
    return bridge.listConnectedGatewaySenders();
  }


  @Override
  public float getCpuUsage() {
    return bridge.getCpuUsage();
  }

  @Override
  public float getCreatesRate() {
    return bridge.getCreatesRate();
  }

  @Override
  public long getCurrentHeapSize() {
    return getUsedMemory();
  }

  @Override
  public float getDiskReadsRate() {
    return bridge.getDiskReadsRate();
  }

  @Override
  public float getDiskWritesRate() {
    return bridge.getDiskWritesRate();
  }

  @Override
  public long getFileDescriptorLimit() {
    return bridge.getFileDescriptorLimit();
  }

  @Override
  public long getDiskFlushAvgLatency() {
    return bridge.getDiskFlushAvgLatency();
  }

  @Override
  public long getFreeHeapSize() {
    return getFreeMemory();
  }


  @Override
  public float getFunctionExecutionRate() {
    return bridge.getFunctionExecutionRate();
  }

  @Override
  public float getGetsRate() {
    return bridge.getGetsRate();
  }

  @Override
  public int getInitialImageKeysReceived() {
    return bridge.getInitialImageKeysReceived();
  }

  @Override
  public long getInitialImageTime() {
    return bridge.getInitialImageTime();
  }

  @Override
  public int getInitialImagesInProgres() {
    return getInitialImagesInProgress();
  }

  @Override
  public int getInitialImagesInProgress() {
    return bridge.getInitialImagesInProgress();
  }


  @Override
  public String[] fetchJvmThreads() {
    return bridge.fetchJvmThreads();
  }

  @Override
  public String[] listRegions() {
    return bridge.getListOfRegions();
  }

  @Override
  public int getLockWaitsInProgress() {
    return bridge.getLockWaitsInProgress();
  }

  @Override
  public float getLruDestroyRate() {
    return bridge.getLruDestroyRate();
  }

  @Override
  public float getLruEvictionRate() {
    return bridge.getLruEvictionRate();
  }

  @Override
  public long getMaximumHeapSize() {
    return getMaxMemory();
  }

  @Override
  public int getNumRunningFunctions() {
    return bridge.getNumRunningFunctions();
  }

  @Override
  public int getNumRunningFunctionsHavingResults() {
    return bridge.getNumRunningFunctionsHavingResults();
  }

  @Override
  public long getPutAllAvgLatency() {
    return bridge.getPutAllAvgLatency();
  }

  @Override
  public float getPutAllRate() {
    return bridge.getPutAllRate();
  }

  @Override
  public long getPutsAvgLatency() {
    return bridge.getPutsAvgLatency();
  }

  @Override
  public float getPutsRate() {
    return bridge.getPutsRate();
  }

  @Override
  public int getLockRequestQueues() {
    return bridge.getLockRequestQueues();
  }

  @Override
  public String[] getRootRegionNames() {
    return bridge.getRootRegionNames();
  }

  @Override
  public int getTotalBackupInProgress() {
    return bridge.getTotalBackupInProgress();
  }

  @Override
  public int getTotalBucketCount() {
    return bridge.getTotalBucketCount();
  }


  @Override
  public long getTotalFileDescriptorOpen() {
    return bridge.getTotalFileDescriptorOpen();
  }

  @Override
  public int getTotalHitCount() {
    return bridge.getTotalHitCount();
  }

  @Override
  public int getTotalLoadsCompleted() {
    return bridge.getTotalLoadsCompleted();
  }

  @Override
  public long getTotalLockWaitTime() {
    return bridge.getTotalLockWaitTime();
  }

  @Override
  public int getTotalMissCount() {
    return bridge.getTotalMissCount();
  }

  @Override
  public int getTotalNumberOfLockService() {
    return bridge.getTotalNumberOfLockService();
  }

  @Override
  public int getTotalPrimaryBucketCount() {
    return bridge.getTotalPrimaryBucketCount();
  }

  @Override
  public int getTotalRegionCount() {
    return bridge.getTotalRegionCount();
  }

  @Override
  public int getTotalRegionEntryCount() {
    return bridge.getTotalRegionEntryCount();
  }

  @Override
  public int getTotalTransactionsCount() {
    return bridge.getTotalTransactionsCount();
  }

  @Override
  public long getTransactionCommitsAvgLatency() {
    return bridge.getTransactionCommitsAvgLatency();
  }

  @Override
  public float getTransactionCommitsRate() {
    return bridge.getTransactionCommitsRate();
  }

  @Override
  public int getTransactionCommittedTotalCount() {
    return bridge.getTransactionCommittedTotalCount();
  }

  @Override
  public int getTransactionRolledBackTotalCount() {
    return bridge.getTransactionRolledBackTotalCount();
  }

  @Override
  public long getMemberUpTime() {
    return bridge.getMemberUpTime();
  }

  @Override
  public String getClassPath() {
    return bridge.getClassPath();
  }

  @Override
  public long getCurrentTime() {
    return bridge.getCurrentTime();
  }

  @Override
  public String getHost() {
    return bridge.getHost();
  }

  @Override
  public long getLockLease() {
    return bridge.getLockLease();
  }

  @Override
  public long getLockTimeout() {
    return bridge.getLockTimeout();
  }

  @Override
  public int getProcessId() {
    return bridge.getProcessId();
  }

  @Override
  public String status() {
    return bridge.status();
  }

  @Override
  public String getVersion() {
    return bridge.getVersion();
  }

  @Override
  public String getReleaseVersion() {
    return bridge.getReleaseVersion();
  }

  @Override
  public String getGeodeReleaseVersion() {
    return bridge.getGeodeReleaseVersion();
  }

  @Override
  public boolean hasGatewayReceiver() {
    return bridge.hasGatewayReceiver();
  }

  @Override
  public boolean hasGatewaySender() {
    return bridge.hasGatewaySender();
  }

  @Override
  public boolean isLocator() {
    return bridge.isLocator();
  }

  @Override
  public boolean isManager() {
    return bridge.isManager();
  }

  @Override
  public boolean isManagerCreated() {
    return bridge.isManagerCreated();
  }

  @Override
  public boolean isServer() {
    return bridge.isServer();
  }

  @Override
  public GemFireProperties listGemFireProperties() {
    return bridge.getGemFireProperty();
  }

  @Override
  public boolean createManager() {
    return bridge.createManager();
  }

  @Override
  public String processCommand(String commandString) {
    Map<String, String> emptyMap = Collections.emptyMap();
    return processCommand(commandString, emptyMap);
  }

  @Override
  public String processCommand(String commandString, Map<String, String> env) {
    return processCommand(commandString, env, (List<String>) null);
  }

  @Override
  public String processCommand(String commandString, Map<String, String> env,
      List<String> stagedFilePaths) {
    return bridge.processCommand(commandString, env, stagedFilePaths);
  }

  @Override
  /*
   * We don't expect any callers to call this code, but just in case, implementation is provided for
   * backward compatibility
   *
   * @deprecated since 1.4 use processCommand(String commandString, Map<String, String> env,
   * List<String> stagedFilePaths)
   */
  public String processCommand(String commandString, Map<String, String> env, Byte[][] binaryData) {
    // save the binaryData into stagedFile first, and then call the new api
    File tempDir = FileUtils.getTempDirectory();
    List<String> filePaths = null;
    try {
      filePaths = ManagementUtils.bytesToFiles(binaryData, tempDir.getAbsolutePath());
      return bridge.processCommand(commandString, env, filePaths);
    } catch (IOException e) {
      throw new RuntimeException(e.getMessage(), e);
    } finally {
      // delete the staged files
      FileUtils.deleteQuietly(tempDir);
    }
  }

  @Override
  public String[] listDiskStores(boolean includeRegionOwned) {
    return bridge.listDiskStores(includeRegionOwned);
  }

  @Override
  public JVMMetrics showJVMMetrics() {
    return bridge.fetchJVMMetrics();
  }

  @Override
  public OSMetrics showOSMetrics() {
    return bridge.fetchOSMetrics();
  }

  @Override
  public void shutDownMember() {
    bridge.shutDownMember();

  }

  @Override
  public String getName() {
    return bridge.getName();
  }

  @Override
  public String getId() {
    return bridge.getId();
  }

  @Override
  public String getMember() {
    return bridge.getMember();
  }

  @Override
  public String[] getGroups() {
    return bridge.getGroups();
  }

  @Override
  public String[] getDiskStores() {
    return bridge.getDiskStores();
  }

  @Override
  public long getGetsAvgLatency() {
    return bridge.getGetsAvgLatency();
  }

  @Override
  public long getLoadsAverageLatency() {
    return bridge.getLoadsAverageLatency();
  }

  @Override
  public long getNetLoadsAverageLatency() {
    return bridge.getNetLoadsAverageLatency();
  }

  @Override
  public long getNetSearchAverageLatency() {
    return bridge.getNetSearchAverageLatency();
  }

  @Override
  public int getTotalDiskTasksWaiting() {
    return bridge.getTotalDiskTasksWaiting();
  }

  @Override
  public int getTotalNetLoadsCompleted() {
    return bridge.getTotalNetLoadsCompleted();
  }

  @Override
  public int getTotalNetSearchCompleted() {
    return bridge.getTotalNetSearchCompleted();
  }

  @Override
  public int getTotalNumberOfGrantors() {
    return bridge.getTotalNumberOfGrantors();
  }

  @Override
  public int getTotalBackupCompleted() {
    return bridge.getTotalBackupCompleted();
  }

  @Override
  public float getDestroysRate() {
    return bridge.getDestroysRate();
  }

  @Override
  public long getDeserializationAvgLatency() {
    return bridge.getDeserializationAvgLatency();
  }

  @Override
  public long getDeserializationLatency() {
    return bridge.getDeserializationLatency();
  }

  @Override
  public float getDeserializationRate() {
    return bridge.getDeserializationRate();
  }

  @Override
  public int getPartitionRegionCount() {
    return bridge.getPartitionRegionCount();
  }

  @Override
  public long getSerializationAvgLatency() {
    return bridge.getSerializationAvgLatency();
  }

  @Override
  public long getSerializationLatency() {
    return bridge.getSerializationLatency();
  }

  @Override
  public float getSerializationRate() {
    return bridge.getSerializationRate();
  }

  @Override
  public long getPDXDeserializationAvgLatency() {
    return bridge.getPDXDeserializationAvgLatency();
  }

  @Override
  public float getPDXDeserializationRate() {
    return bridge.getPDXDeserializationRate();
  }

  public MemberMBeanBridge getBridge() {
    return bridge;
  }

  public void stopMonitor() {
    bridge.stopMonitor();
  }

  @Override
  public long getTotalDiskUsage() {
    return bridge.getTotalDiskUsage();
  }

  @Override
  public float getAverageReads() {
    return bridge.getAverageReads();
  }

  @Override
  public float getAverageWrites() {
    return bridge.getAverageWrites();
  }

  @Override
  public long getGarbageCollectionTime() {
    return bridge.getGarbageCollectionTime();
  }

  @Override
  public long getGarbageCollectionCount() {
    return bridge.getGarbageCollectionCount();
  }

  @Override
  public double getLoadAverage() {
    return bridge.getLoadAverage();
  }

  @Override
  public int getNumThreads() {
    return bridge.getNumThreads();
  }

  @Override
  public long getJVMPauses() {
    return bridge.getJVMPauses();
  }

  @Override
  public int getHostCpuUsage() {
    return bridge.getHostCpuUsage();
  }

  @Override
  public boolean isCacheServer() {
    return bridge.isCacheServer();
  }

  @Override
  public String getRedundancyZone() {
    return bridge.getRedundancyZone();
  }

  @Override
  public int getRebalancesInProgress() {
    return bridge.getRebalancesInProgress();
  }

  @Override
  public int getReplyWaitsInProgress() {
    return bridge.getReplyWaitsInProgress();
  }

  @Override
  public int getReplyWaitsCompleted() {
    return bridge.getReplyWaitsCompleted();
  }

  @Override
  public int getVisibleNodes() {
    return bridge.getVisibleNodes();
  }

  @Override
  public int getOffHeapObjects() {
    return bridge.getOffHeapObjects();
  }

  @Override
  public long getOffHeapMaxMemory() {
    return bridge.getOffHeapMaxMemory();
  }

  @Override
  public long getOffHeapFreeMemory() {
    return bridge.getOffHeapFreeMemory();
  }

  @Override
  public long getOffHeapUsedMemory() {
    return bridge.getOffHeapUsedMemory();
  }

  @Override
  public int getOffHeapFragmentation() {
    return bridge.getOffHeapFragmentation();
  }

  @Override
  public long getOffHeapCompactionTime() {
    return bridge.getOffHeapCompactionTime();
  }

  @Override
  public long getMaxMemory() {
    return bridge.getMaxMemory();
  }

  @Override
  public long getFreeMemory() {
    return bridge.getFreeMemory();
  }

  @Override
  public long getUsedMemory() {
    return bridge.getUsedMemory();
  }
}
