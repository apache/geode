/*=========================================================================
 * Copyright (c) 2012-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.vmware.gemfire.tools.pulse.tests;

public interface MemberMBean {
  String OBJECT_NAME = "GemFire:type=Member";

  boolean getManager();

  int getTotalRegionCount();

  boolean getLocator();

  long getTotalDiskUsage();

  boolean getServer();

  String[] getGroups();

  //public String[] getRedundancyZone();
  String getRedundancyZone();

  long getTotalFileDescriptorOpen();

  double getLoadAverage();

  float getDiskWritesRate();

  long getJVMPauses();

  long getCurrentHeapSize();

  long getMaximumHeapSize();

  long getUsedMemory();

  long getMaxMemory();

  int getNumThreads();

  long getMemberUpTime();

  String getHost();

  long getTotalBytesOnDisk();

  float getCpuUsage();

  String getMember();

  String getId();

  float getAverageReads();

  float getAverageWrites();

  int getPort();

  long getFoo();

  long getOffHeapFreeSize();

  long getOffHeapUsedSize();

  long getOffHeapFreeMemory();

  long getOffHeapUsedMemory();

  String getVersion();
}
