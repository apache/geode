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
package com.gemstone.gemfire.management.internal.web.controllers.support;

import java.util.Map;

import com.gemstone.gemfire.management.GemFireProperties;
import com.gemstone.gemfire.management.JVMMetrics;
import com.gemstone.gemfire.management.MemberMXBean;
import com.gemstone.gemfire.management.OSMetrics;

/**
 * The MemberMXBeanAdapter class is an abstract adapter class to the MemberMXBean interface.
 * <p/>
 * @see com.gemstone.gemfire.management.MemberMXBean
 * @since 8.0
 */
public class MemberMXBeanAdapter implements MemberMXBean {

  @Override
  public String showLog(final int numberOfLines) {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public String viewLicense() { throw new UnsupportedOperationException("Not Implemented!"); }

  @Override
  public String[] compactAllDiskStores() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public boolean createManager() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public void shutDownMember() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public JVMMetrics showJVMMetrics() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public OSMetrics showOSMetrics() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public String processCommand(final String commandString) {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public String processCommand(final String commandString, final Map<String, String> env) {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public String processCommand(final String commandString, final Map<String, String> env, final Byte[][] binaryData) {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public String[] listDiskStores(final boolean includeRegionOwned) {
    throw new UnsupportedOperationException("Not Implemented!");
  }
  
  @Override
  public GemFireProperties listGemFireProperties() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public String getHost() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public String getName() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public String getId() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public String getMember() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public String[] getGroups() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public int getProcessId() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public String status() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public String getVersion() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public boolean isLocator() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public long getLockTimeout() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public long getLockLease() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public boolean isServer() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public boolean hasGatewaySender() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public boolean isManager() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public boolean isManagerCreated() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public boolean hasGatewayReceiver() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public String getClassPath() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public long getCurrentTime() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public long getMemberUpTime() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public float getCpuUsage() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  @Deprecated
  public long getCurrentHeapSize() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  @Deprecated
  public long getMaximumHeapSize() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  @Deprecated
  public long getFreeHeapSize() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public String[] fetchJvmThreads() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public long getFileDescriptorLimit() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public long getTotalFileDescriptorOpen() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public int getTotalRegionCount() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public int getPartitionRegionCount() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public String[] listRegions() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public String[] getDiskStores() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public String[] getRootRegionNames() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public int getTotalRegionEntryCount() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public int getTotalBucketCount() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public int getTotalPrimaryBucketCount() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public long getGetsAvgLatency() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public long getPutsAvgLatency() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public long getPutAllAvgLatency() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public int getTotalMissCount() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public int getTotalHitCount() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public float getGetsRate() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public float getPutsRate() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public float getPutAllRate() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public float getCreatesRate() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public float getDestroysRate() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public long getCacheWriterCallsAvgLatency() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public long getCacheListenerCallsAvgLatency() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public int getTotalLoadsCompleted() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public long getLoadsAverageLatency() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public int getTotalNetLoadsCompleted() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public long getNetLoadsAverageLatency() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public int getTotalNetSearchCompleted() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public long getNetSearchAverageLatency() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public int getTotalDiskTasksWaiting() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public float getBytesSentRate() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public float getBytesReceivedRate() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public String[] listConnectedGatewayReceivers() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public String[] listConnectedGatewaySenders() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public int getNumRunningFunctions() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public float getFunctionExecutionRate() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public int getNumRunningFunctionsHavingResults() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public int getTotalTransactionsCount() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public long getTransactionCommitsAvgLatency() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public int getTransactionCommittedTotalCount() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public int getTransactionRolledBackTotalCount() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public float getTransactionCommitsRate() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public float getDiskReadsRate() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public float getDiskWritesRate() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public long getDiskFlushAvgLatency() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public int getTotalBackupInProgress() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public int getTotalBackupCompleted() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public int getLockWaitsInProgress() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public long getTotalLockWaitTime() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public int getTotalNumberOfLockService() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public int getTotalNumberOfGrantors() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public int getLockRequestQueues() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public float getLruEvictionRate() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public float getLruDestroyRate() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public int getInitialImagesInProgres() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public long getInitialImageTime() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public int getInitialImageKeysReceived() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public long getDeserializationAvgLatency() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public long getDeserializationLatency() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public float getDeserializationRate() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public long getSerializationAvgLatency() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public long getSerializationLatency() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public float getSerializationRate() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public float getPDXDeserializationRate() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public long getPDXDeserializationAvgLatency() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public long getTotalDiskUsage() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public int getNumThreads() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public double getLoadAverage() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public long getGarbageCollectionCount() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public long getGarbageCollectionTime() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public float getAverageReads() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public float getAverageWrites() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public long getJVMPauses() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override 
    public int getHostCpuUsage() { 
       throw new UnsupportedOperationException("Not Implemented!"); 
   }

  @Override
  public boolean isCacheServer() {
    throw new UnsupportedOperationException("Not Implemented!"); 
  }

  @Override
  public String getRedundancyZone() {
    throw new UnsupportedOperationException("Not Implemented!"); 
  }

  @Override
  public int getRebalancesInProgress() {
    throw new UnsupportedOperationException("Not Implemented!"); 
  }

  @Override
  public int getReplyWaitsInProgress() {
    throw new UnsupportedOperationException("Not Implemented!"); 
  }

  @Override
  public int getReplyWaitsCompleted() {
    throw new UnsupportedOperationException("Not Implemented!"); 
  }

  @Override
  public int getVisibleNodes() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public int getOffHeapObjects() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public long getOffHeapMaxMemory() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public long getOffHeapFreeMemory() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public long getOffHeapUsedMemory() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public int getOffHeapFragmentation() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public long getOffHeapCompactionTime() {
    throw new UnsupportedOperationException("Not Implemented!");
  }    

  @Override
  public long getMaxMemory() {
    throw new UnsupportedOperationException("Not Implemented!");
  }
  
  @Override
  public long getFreeMemory() {
    throw new UnsupportedOperationException("Not Implemented!");
  }
  
  @Override
  public long getUsedMemory() {
    throw new UnsupportedOperationException("Not Implemented!");
  }
}
