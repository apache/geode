/*
 *
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
 *
 */
package org.apache.geode.tools.pulse.tests;

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
// This count is built dynamically in Pulse backend and region count is maintained in Cluster.Member data structure
//    return getInt("totalRegionCount");
    return 0;
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
  public double getDiskWritesRate() {
    return getDouble("diskWritesRate");
  }

  @Override
  public long getJVMPauses() {
    return getLong("JVMPauses");
  }

  @Override
  public long getCurrentHeapSize() {
//    return getLong("currentHeapSize");
    return getLong("UsedMemory");
  }

  @Override
  public long getMaximumHeapSize() {
//    return getLong("maximumHeapSize");
    return getLong("MaxMemory");
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
  public double getCpuUsage() {
    return getDouble("cpuUsage");
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
  public double getAverageReads() {
    return getDouble("averageReads");
  }

  @Override
  public double getAverageWrites() {
    return getDouble("averageWrites");
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
