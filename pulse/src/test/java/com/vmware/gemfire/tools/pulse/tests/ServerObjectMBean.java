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
  public static final String OBJECT_NAME = "GemFire:service=System,type=Distributed";

  public TabularData viewRemoteClusterStatus();

  public int getMemberCount();

  public int getNumClients();

  public int getDistributedSystemId();

  public int getLocatorCount();

  public int getTotalRegionCount();

  public int getNumRunningFunctions();

  public long getRegisteredCQCount();

  public int getNumSubscriptions();

  public int getTransactionCommitted();

  public int getTransactionRolledBack();

  public long getTotalHeapSize();

  public long getUsedHeapSize();

  public long getMaxMemory();

  public long getUsedMemory();

  public long getTotalRegionEntryCount();

  public int getCurrentQueryCount();

  public long getTotalDiskUsage();

  public float getDiskWritesRate();

  public float getAverageWrites();

  public float getAverageReads();

  public float getQueryRequestRate();

  public float getDiskReadsRate();

  public long getJVMPauses();

  public String[] listCacheServers();

  public String queryData(String p0, String p1, int p2);
}
