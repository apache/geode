/*=========================================================================
 * Copyright (c) 2012-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.vmware.gemfire.tools.pulse.tests;

public class Member extends JMXBaseBean implements MemberMBean {
  private String name = null;

  public Member(String name) {
    this.name = name;
  }

  protected String getKey(String propName) {
    return "member." + name + "." + propName;
  }

  @Override
  public boolean getManager() {
    return getBoolean("manager");
  }

  @Override
  public int getTotalRegionCount() {
    return getInt("totalRegionCount");
  }

  @Override
  public boolean getLocator() {
    return getBoolean("locator");
  }

  @Override
  public long getTotalDiskUsage() {
    return getLong("totalDiskUsage");
  }

  @Override
  public boolean getServer() {
    return getBoolean("sever");
  }

  @Override
  public String[] getGroups() {
    return getStringArray("Groups");
  }

  @Override
  /*public String[] getRedundancyZone() {
    return getStringArray("RedundancyZone");
  }*/
  public String getRedundancyZone() {
    return getString("RedundancyZone");
  }

  @Override
  public long getTotalFileDescriptorOpen() {
    return getLong("totalFileDescriptorOpen");
  }

  @Override
  public double getLoadAverage() {
    return getDouble("loadAverage");
  }

  @Override
  public float getDiskWritesRate() {
    return getFloat("diskWritesRate");
  }

  @Override
  public long getJVMPauses() {
    return getLong("JVMPauses");
  }

  @Override
  public long getCurrentHeapSize() {
    return getLong("currentHeapSize");
  }

  @Override
  public long getMaximumHeapSize() {
    return getLong("maximumHeapSize");
  }

  @Override
  public long getUsedMemory() {
    return getLong("UsedMemory");
  }

  @Override
  public long getMaxMemory() {
    return getLong("MaxMemory");
  }

  @Override
  public int getNumThreads() {
    return getInt("numThreads");
  }

  @Override
  public long getMemberUpTime() {
    return getLong("memberUpTime");
  }

  @Override
  public String getHost() {
    return getString("host");
  }

  @Override
  public long getTotalBytesOnDisk() {
    return getLong("totalBytesOnDisk");
  }

  @Override
  public float getCpuUsage() {
    return getFloat("cpuUsage");
  }

  @Override
  public String getMember() {
    return getString("member");
  }

  @Override
  public String getId() {
    return getString("id");
  }

  @Override
  public float getAverageReads() {
    return getFloat("averageReads");
  }

  @Override
  public float getAverageWrites() {
    return getFloat("averageWrites");
  }

  @Override
  public int getPort() {
    return getInt("port");
  }

  @Override
  public long getFoo() {
    return getLong("foo");
  }

  @Override
  public long getOffHeapFreeSize() {
    return getLong("OffHeapFreeSize");
  }

  @Override
  public long getOffHeapUsedSize() {
    return getLong("OffHeapUsedSize");
  }

  @Override
  public long getOffHeapFreeMemory() {
    return getLong("OffHeapFreeMemory");
  }

  @Override
  public long getOffHeapUsedMemory() {
    return getLong("OffHeapUsedMemory");
  }

  @Override
  public String getVersion() {
    return getString("Version");
  }
}
