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

import java.util.Map;

import javax.management.NotificationBroadcasterSupport;
import javax.management.ObjectName;

import org.apache.geode.management.DiskBackupStatus;
import org.apache.geode.management.DiskMetrics;
import org.apache.geode.management.DistributedSystemMXBean;
import org.apache.geode.management.GemFireProperties;
import org.apache.geode.management.JVMMetrics;
import org.apache.geode.management.NetworkMetrics;
import org.apache.geode.management.OSMetrics;
import org.apache.geode.management.PersistentMemberDetails;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;
import org.apache.geode.security.ResourcePermission.Target;

/**
 * Distributed System MBean
 *
 * It is provided with one bridge reference which acts as an intermediate for JMX and GemFire.
 *
 */
public class DistributedSystemMBean extends NotificationBroadcasterSupport
    implements DistributedSystemMXBean {

  /**
   * Injected DistributedSystemBridge
   */
  private final DistributedSystemBridge bridge;

  public DistributedSystemMBean(DistributedSystemBridge bridge) {
    this.bridge = bridge;
  }

  @Override
  public DiskBackupStatus backupAllMembers(String targetDirPath, String baselineDirPath)
      throws Exception {
    bridge.getCache().getSecurityService().authorize(Resource.CLUSTER, Operation.WRITE,
        Target.DISK);
    return bridge.backupAllMembers(targetDirPath, baselineDirPath);
  }

  @Override
  public String getAlertLevel() {
    return bridge.getAlertLevel();
  }

  @Override
  public String[] listCacheServers() {
    return bridge.listCacheServers();
  }


  @Override
  public int getNumClients() {
    return bridge.getNumClients();
  }

  @Override
  public long getActiveCQCount() {
    return bridge.getActiveCQCount();
  }

  @Override
  public DiskMetrics showDiskMetrics(String member) throws Exception {
    return bridge.showDiskMetrics(member);
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
  public long getDiskFlushAvgLatency() {
    return bridge.getDiskFlushAvgLatency();
  }


  @Override
  public String[] listGatewayReceivers() {
    return bridge.listGatewayReceivers();
  }

  @Override
  public String[] listGatewaySenders() {
    return bridge.listGatewaySenders();
  }

  @Override
  public JVMMetrics showJVMMetrics(String member) throws Exception {
    return bridge.showJVMMetrics(member);
  }

  @Override
  public int getLocatorCount() {
    return bridge.getLocatorCount();
  }

  @Override
  public String[] listLocators() {
    return bridge.listLocators();
  }

  @Override
  public GemFireProperties fetchMemberConfiguration(String member) throws Exception {
    return bridge.fetchMemberConfiguration(member);
  }

  @Override
  public int getDistributedSystemId() {
    return bridge.getDistributedSystemId();
  }

  @Override
  public int getMemberCount() {
    return bridge.getMemberCount();
  }

  @Override
  public Map<String, String[]> listMemberDiskstore() {
    return bridge.getMemberDiskstoreMap();
  }

  @Override
  public long fetchMemberUpTime(String member) throws Exception {
    return bridge.getMemberUpTime(member);
  }

  @Override
  public String[] listMembers() {
    return bridge.getMembers();
  }

  @Override
  public String[] listLocatorMembers(boolean onlyStandAloneLocators) {
    return bridge.listLocatorMembers(onlyStandAloneLocators);
  }

  @Override

  public String[] listGroups() {
    return bridge.getGroups();
  }

  @Override
  public NetworkMetrics showNetworkMetric(String member) throws Exception {
    return bridge.showNetworkMetric(member);
  }

  @Override
  public int getNumInitialImagesInProgress() {
    return bridge.getNumInitialImagesInProgress();
  }

  @Override
  public OSMetrics showOSMetrics(String member) throws Exception {
    return bridge.showOSMetrics(member);
  }

  @Override
  public float getQueryRequestRate() {
    return bridge.getQueryRequestRate();
  }

  @Override
  public int getSystemDiskStoreCount() {
    return bridge.getSystemDiskStoreCount();
  }

  @Override
  public int getTotalBackupInProgress() {
    return bridge.getTotalBackupInProgress();
  }

  @Override
  public long getTotalHeapSize() {
    return bridge.getTotalHeapSize();
  }

  @Override
  public int getTotalHitCount() {
    return bridge.getTotalHitCount();
  }

  @Override
  public int getTotalMissCount() {
    return bridge.getTotalMissCount();
  }

  @Override
  public int getTotalRegionCount() {
    return bridge.getTotalRegionCount();
  }

  @Override
  public long getTotalRegionEntryCount() {
    return bridge.getTotalRegionEntryCount();
  }


  @Override
  public void changeAlertLevel(String alertLevel) throws Exception {
    bridge.changeAlertLevel(alertLevel);

  }

  @Override
  public String[] shutDownAllMembers() throws Exception {
    return bridge.shutDownAllMembers();
  }

  @Override
  public String[] listRegions() {
    return bridge.listAllRegions();
  }

  @Override
  public String[] listAllRegionPaths() {
    return bridge.listAllRegionPaths();
  }

  @Override
  public PersistentMemberDetails[] listMissingDiskStores() {
    return bridge.listMissingDiskStores();
  }

  @Override
  public boolean revokeMissingDiskStores(final String diskStoreId) {
    return bridge.revokeMissingDiskStores(diskStoreId);
  }


  @Override
  public ObjectName getMemberObjectName() {
    return bridge.getMemberObjectName();
  }

  @Override
  public ObjectName getManagerObjectName() {
    return bridge.getManagerObjectName();
  }

  @Override
  public ObjectName fetchMemberObjectName(String member) throws Exception {
    return bridge.fetchMemberObjectName(member);
  }

  @Override
  public ObjectName[] listMemberObjectNames() {
    return bridge.listMemberObjectNames();
  }

  @Override
  public ObjectName fetchDistributedRegionObjectName(String regionPath) throws Exception {
    return bridge.fetchDistributedRegionObjectName(regionPath);
  }

  @Override
  public ObjectName fetchRegionObjectName(String member, String regionPath) throws Exception {
    return bridge.fetchRegionObjectName(member, regionPath);
  }

  @Override
  public ObjectName[] fetchRegionObjectNames(ObjectName memberMBeanName) throws Exception {
    return bridge.fetchRegionObjectNames(memberMBeanName);
  }

  @Override
  public ObjectName[] listDistributedRegionObjectNames() {
    return bridge.listDistributedRegionObjectNames();
  }

  @Override
  public ObjectName fetchCacheServerObjectName(String member, int port) throws Exception {
    return bridge.fetchCacheServerObjectName(member, port);
  }

  @Override
  public ObjectName fetchDiskStoreObjectName(String member, String diskStore) throws Exception {
    return bridge.fetchDiskStoreObjectName(member, diskStore);
  }

  @Override
  public ObjectName fetchDistributedLockServiceObjectName(String lockServiceName) throws Exception {
    return bridge.fetchDistributedLockServiceObjectName(lockServiceName);
  }

  @Override
  public ObjectName fetchGatewayReceiverObjectName(String member) throws Exception {
    return bridge.fetchGatewayReceiverObjectName(member);
  }

  @Override
  public ObjectName fetchGatewaySenderObjectName(String member, String senderId) throws Exception {
    return bridge.fetchGatewaySenderObjectName(member, senderId);
  }

  @Override
  public ObjectName fetchLockServiceObjectName(String member, String lockService) throws Exception {
    return bridge.fetchLockServiceObjectName(member, lockService);
  }

  @Override
  public ObjectName[] listCacheServerObjectNames() {
    return bridge.listCacheServerObjectNames();
  }

  @Override
  public ObjectName[] listGatewayReceiverObjectNames() {
    return bridge.listGatewayReceiverObjectNames();
  }

  @Override
  public ObjectName[] listGatewaySenderObjectNames() {
    return bridge.listGatewaySenderObjectNames();
  }

  @Override
  public ObjectName[] listGatewaySenderObjectNames(String member) throws Exception {
    return bridge.listGatewaySenderObjectNames(member);
  }

  @Override
  public int getNumRunningFunctions() {
    return bridge.getNumRunningFunctions();
  }

  @Override
  public long getRegisteredCQCount() {
    return bridge.getRegisteredCQCount();
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
  public long getUsedHeapSize() {
    return bridge.getUsedHeapSize();
  }

  @Override
  public int getNumSubscriptions() {
    return bridge.getNumSubscriptions();
  }


  @Override
  public long getGarbageCollectionCount() {
    return bridge.getGarbageCollectionCount();
  }

  @Override
  public Map<String, Boolean> viewRemoteClusterStatus() {
    return bridge.viewRemoteClusterStatus();
  }

  @Override
  public long getJVMPauses() {
    return bridge.getJVMPauses();
  }

  @Override
  public String queryData(String queryString, String members, int limit) throws Exception {
    return bridge.queryData(queryString, members, limit);
  }

  @Override
  public byte[] queryDataForCompressedResult(String queryString, String members, int limit)
      throws Exception {
    return bridge.queryDataForCompressedResult(queryString, members, limit);
  }


  @Override
  public int getTransactionCommitted() {
    return bridge.getTransactionCommitted();
  }

  @Override
  public int getTransactionRolledBack() {
    return bridge.getTransactionRolledBack();
  }

  @Override
  public String[] listServers() {
    return bridge.listServers();
  }

  @Override
  public int getQueryResultSetLimit() {
    return bridge.getQueryResultSetLimit();
  }

  @Override
  public void setQueryResultSetLimit(int queryResultSetLimit) {
    bridge.setQueryResultSetLimit(queryResultSetLimit);
  }

  @Override
  public int getQueryCollectionsDepth() {
    return bridge.getQueryCollectionsDepth();
  }

  @Override
  public void setQueryCollectionsDepth(int queryCollectionsDepth) {
    bridge.setQueryCollectionsDepth(queryCollectionsDepth);
  }
}
