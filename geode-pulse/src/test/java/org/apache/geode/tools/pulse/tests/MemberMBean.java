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

  double getDiskWritesRate();

  long getJVMPauses();

  long getCurrentHeapSize();

  long getMaximumHeapSize();

  long getUsedMemory();

  long getMaxMemory();

  int getNumThreads();

  long getMemberUpTime();

  String getHost();

  long getTotalBytesOnDisk();

  double getCpuUsage();

  String getMember();

  String getId();

  double getAverageReads();

  double getAverageWrites();

  int getPort();

  long getFoo();

  long getOffHeapFreeSize();

  long getOffHeapUsedSize();

  long getOffHeapFreeMemory();

  long getOffHeapUsedMemory();

  String getVersion();
}
