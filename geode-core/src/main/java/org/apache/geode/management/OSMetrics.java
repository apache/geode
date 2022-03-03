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
package org.apache.geode.management;

import java.beans.ConstructorProperties;
import java.io.Serializable;

/**
 * Composite data type used to distribute metrics for the operating system hosting a member of the
 * distributed system.
 *
 * @since GemFire 7.0
 *
 */
public class OSMetrics implements Serializable {

  /**
   * Maximum number file descriptor which can be opened
   */
  private final long maxFileDescriptorCount;

  /**
   * Current number of open file descriptor count
   */
  private final long openFileDescriptorCount;

  private final long processCpuTime;

  private final long committedVirtualMemorySize;

  private final long totalPhysicalMemorySize;

  private final long freePhysicalMemorySize;

  private final long totalSwapSpaceSize;

  private final long freeSwapSpaceSize;

  /**
   * Returns the operating system name.
   */
  private final String name;

  /**
   * Returns the operating system version.
   */
  private final String version;

  /**
   * Returns the operating system architecture.
   */
  private final String arch;

  /**
   * Returns the number of processors available to the Java virtual machine. This method is
   * equivalent to the {@link Runtime#availableProcessors()} method.
   */
  private final int availableProcessors;

  /**
   * * Returns the system load average for the last minute. The system load average is the sum of
   * the number of runnable entities queued to the {@linkplain #getAvailableProcessors available
   * processors} and the number of runnable entities running on the available processors averaged
   * over a period of time. The way in which the load average is calculated is operating system
   * specific but is typically a damped time-dependent average.
   */
  private final double systemLoadAverage;

  /**
   *
   * This constructor is to be used by internal JMX framework only. User should not try to create an
   * instance of this class.
   */
  @ConstructorProperties({"maxFileDescriptorCount", "openFileDescriptorCount", "processCpuTime",
      "committedVirtualMemorySize", "totalPhysicalMemorySize", "freePhysicalMemorySize",
      "totalSwapSpaceSize", "freeSwapSpaceSize", "name", "version", "arch", "availableProcessors",
      "systemLoadAverage"

  })
  public OSMetrics(long maxFileDescriptorCount, long openFileDescriptorCount, long processCpuTime,
      long committedVirtualMemorySize, long totalPhysicalMemorySize, long freePhysicalMemorySize,
      long totalSwapSpaceSize, long freeSwapSpaceSize, String name, String version, String arch,
      int availableProcessors, double systemLoadAverage) {
    this.maxFileDescriptorCount = maxFileDescriptorCount;
    this.openFileDescriptorCount = openFileDescriptorCount;
    this.processCpuTime = processCpuTime;
    this.committedVirtualMemorySize = committedVirtualMemorySize;
    this.totalPhysicalMemorySize = totalPhysicalMemorySize;
    this.freePhysicalMemorySize = freePhysicalMemorySize;
    this.totalSwapSpaceSize = totalSwapSpaceSize;
    this.freeSwapSpaceSize = freeSwapSpaceSize;
    this.name = name;
    this.version = version;
    this.arch = arch;
    this.availableProcessors = availableProcessors;
    this.systemLoadAverage = systemLoadAverage;

  }

  /**
   * Returns the maximum number of open file descriptors allowed by the operating system.
   */
  public long getMaxFileDescriptorCount() {
    return maxFileDescriptorCount;
  }

  /**
   * Returns the current number of open file descriptors..
   */
  public long getOpenFileDescriptorCount() {
    return openFileDescriptorCount;
  }

  /**
   * Returns the amount of time (in nanoseconds) used by the member's process.
   */
  public long getProcessCpuTime() {
    return processCpuTime;
  }

  /**
   * Returns the current number of megabytes of memory allocated.
   */
  public long getCommittedVirtualMemorySize() {
    return committedVirtualMemorySize;
  }

  /**
   * Returns the number of megabytes of memory available to the operating system.
   */
  public long getTotalPhysicalMemorySize() {
    return totalPhysicalMemorySize;
  }

  /**
   * Returns the number of megabytes of free memory available to the operating system.
   */
  public long getFreePhysicalMemorySize() {
    return freePhysicalMemorySize;
  }

  /**
   * Returns the number of megabytes of swap space allocated.
   */
  public long getTotalSwapSpaceSize() {
    return totalSwapSpaceSize;
  }

  /**
   * Returns the number of megabytes of free swap space.
   */
  public long getFreeSwapSpaceSize() {
    return freeSwapSpaceSize;
  }

  /**
   * Returns the name of the operating system.
   */
  public String getName() {
    return name;
  }

  /**
   * Returns the version of the operating system.
   */
  public String getVersion() {
    return version;
  }

  /**
   * Returns the hardware architecture.
   */
  public String getArch() {
    return arch;
  }

  /**
   * Returns the number of available processors.
   */
  public int getAvailableProcessors() {
    return availableProcessors;
  }

  /**
   * Returns the system load average.
   */
  public double getSystemLoadAverage() {
    return systemLoadAverage;
  }

  /**
   * String representation of OSMetrics
   */
  @Override
  public String toString() {

    return "{OSMetrics : maxFileDescriptorCount = " + maxFileDescriptorCount
        + " openFileDescriptorCount = " + openFileDescriptorCount + " processCpuTime = "
        + processCpuTime + " committedVirtualMemorySize = " + committedVirtualMemorySize
        + " totalPhysicalMemorySize = " + totalPhysicalMemorySize + " freePhysicalMemorySize = "
        + freePhysicalMemorySize + " totalSwapSpaceSize = " + totalSwapSpaceSize
        + " freeSwapSpaceSize = " + freeSwapSpaceSize + " name = " + name + " version = " + version
        + " arch = " + arch + " availableProcessors = " + availableProcessors
        + " systemLoadAverage = " + systemLoadAverage + "}";
  }
}
