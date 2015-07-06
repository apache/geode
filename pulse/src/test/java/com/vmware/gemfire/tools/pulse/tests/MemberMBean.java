/*=========================================================================
 * Copyright (c) 2012-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.vmware.gemfire.tools.pulse.tests;

public interface MemberMBean {
  public static final String OBJECT_NAME = "GemFire:type=Member";

  public boolean getManager();

  public int getTotalRegionCount();

  public boolean getLocator();

  public long getTotalDiskUsage();

  public boolean getServer();

  public String[] getGroups();

  //public String[] getRedundancyZone();
  public String getRedundancyZone();

  public long getTotalFileDescriptorOpen();

  public double getLoadAverage();

  public float getDiskWritesRate();

  public long getJVMPauses();

  public long getCurrentHeapSize();

  public long getMaximumHeapSize();

  public long getUsedMemory();

  public long getMaxMemory();

  public int getNumThreads();

  public long getMemberUpTime();

  public String getHost();

  public long getTotalBytesOnDisk();

  public float getCpuUsage();

  public String getMember();

  public String getId();

  public float getAverageReads();

  public float getAverageWrites();

  public int getPort();

  public long getFoo();

  public long getOffHeapFreeSize();

  public long getOffHeapUsedSize();

  public long getOffHeapFreeMemory();

  public long getOffHeapUsedMemory();

  public String getVersion();
}
