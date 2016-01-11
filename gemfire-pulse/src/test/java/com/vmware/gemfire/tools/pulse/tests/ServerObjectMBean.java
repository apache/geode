/*=========================================================================
 * Copyright (c) 2012-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.vmware.gemfire.tools.pulse.tests;

import javax.management.openmbean.TabularData;

public interface ServerObjectMBean {
  String OBJECT_NAME = "GemFire:service=System,type=Distributed";

  TabularData viewRemoteClusterStatus();

  int getMemberCount();

  int getNumClients();

  int getDistributedSystemId();

  int getLocatorCount();

  int getTotalRegionCount();

  int getNumRunningFunctions();

  long getRegisteredCQCount();

  int getNumSubscriptions();

  int getTransactionCommitted();

  int getTransactionRolledBack();

  long getTotalHeapSize();

  long getUsedHeapSize();

  long getMaxMemory();

  long getUsedMemory();

  long getTotalRegionEntryCount();

  int getCurrentQueryCount();

  long getTotalDiskUsage();

  float getDiskWritesRate();

  float getAverageWrites();

  float getAverageReads();

  float getQueryRequestRate();

  float getDiskReadsRate();

  long getJVMPauses();

  String[] listCacheServers();

  String[] listServers();

  String queryData(String p0, String p1, int p2);
}
