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

package com.gemstone.gemfire.internal.statistics.platform;

import com.gemstone.gemfire.*;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.statistics.StatisticsTypeFactoryImpl;

/**
 * <P>This class provides the interface for statistics about the 
 * OS X machine a GemFire system is running on.
 */
public class OSXSystemStats
{

  // shared fields
//  private final static int allocatedSwapINT = 0;
//  private final static int bufferMemoryINT = 1;
//  private final static int contextSwitchesINT = 2;
//  private final static int cpuActiveINT = 3;
//  private final static int cpuIdleINT = 4;
//  private final static int cpuNiceINT = 5;
//  private final static int cpuSystemINT = 6;
//  private final static int cpuUserINT = 7;
//  private final static int cpusINT = 8;
//  private final static int freeMemoryINT = 9;
//  private final static int pagesPagedInINT = 10;
//  private final static int pagesPagedOutINT = 11;
//  private final static int pagesSwappedInINT = 12;
//  private final static int pagesSwappedOutINT = 13;
//  private final static int physicalMemoryINT = 14;
//  private final static int processCreatesINT = 15;
//  private final static int processesINT = 16;
//  private final static int sharedMemoryINT = 17;
//  private final static int unallocatedSwapINT = 18;
//
//  private final static int loopbackPacketsLONG = 0;
//  private final static int loopbackBytesLONG = 1;
//  private final static int recvPacketsLONG = 2;
//  private final static int recvBytesLONG = 3;
//  private final static int recvErrorsLONG = 4;
//  private final static int recvDropsLONG = 5;
//  private final static int xmitPacketsLONG = 6;
//  private final static int xmitBytesLONG = 7;
//  private final static int xmitErrorsLONG = 8;
//  private final static int xmitDropsLONG = 9;
//  private final static int xmitCollisionsLONG = 10;
//
//  private final static int loadAverage1DOUBLE = 0;
//  private final static int loadAverage15DOUBLE = 1;
//  private final static int loadAverage5DOUBLE = 2;

  private final static StatisticsType myType;

  private static void checkOffset(String name, int offset) {
    int id = myType.nameToId(name);
    Assert.assertTrue(offset == id, "Expected the offset for " + name + " to be " + offset + " but it was " + id);
  }

  static {
    StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();
    myType = f.createType("OSXSystemStats",
                          "Statistics on an OS X machine.",
                          new StatisticDescriptor[] {
                            f.createIntGauge("dummyStat",
                                                "Place holder statistic until Stats are implimented for the Mac OS X Platform.",
                                                "megabytes"),
//                            f.createIntGauge("allocatedSwap",
//                                                "The number of megabytes of swap space have actually been written to. Swap space must be reserved before it can be allocated.",
//                                                "megabytes"),
//                            f.createIntGauge("bufferMemory",
//                                                "The number of megabytes of memory allocated to buffers.",
//                                                "megabytes"),
//                            f.createIntCounter("contextSwitches",
//                                                "The total number of context switches from one thread to another on the computer.  Thread switches can occur either inside of a single process or across processes.  A thread switch may be caused either by one thread asking another for information, or by a thread being preempted by another, higher priority thread becoming ready to run.",
//                                                "operations", false),
//                            f.createIntGauge("cpuActive",
//                                                "The percentage of the total available time that has been used in a non-idle state.",
//                                                "%"),
//                            f.createIntGauge("cpuIdle",
//                                                "The percentage of the total available time that has been spent sleeping.",
//                                                "%", true),
//                            f.createIntGauge("cpuNice",
//                                                "The percentage of the total available time that has been used to execute user code in processes with low priority.",
//                                                "%"),
//                            f.createIntGauge("cpuSystem",
//                                                "The percentage of the total available time that has been used to execute system (i.e. kernel) code.",
//                                                "%"),
//                            f.createIntGauge("cpuUser",
//                                                "The percentage of the total available time that has been used to execute user code.",
//                                                "%"),
//                            f.createIntGauge("cpus",
//                                                "The number of online cpus on the local machine.",
//                                                "items"),
//                            f.createIntGauge("freeMemory",
//                                                "The number of megabytes of unused memory on the machine.",
//                                                "megabytes", true),
//                            f.createIntCounter("pagesPagedIn",
//                                                "The total number of pages that have been brought into memory from disk by the operating system's memory manager.",
//                                                "pages", false),
//                            f.createIntCounter("pagesPagedOut",
//                                                "The total number of pages that have been flushed from memory to disk by the operating system's memory manager.",
//                                                "pages", false),
//                            f.createIntCounter("pagesSwappedIn",
//                                                "The total number of swap pages that have been read in from disk by the operating system's memory manager.",
//                                                "pages", false),
//                            f.createIntCounter("pagesSwappedOut",
//                                                "The total number of swap pages that have been written out to disk by the operating system's memory manager.",
//                                                "pages", false),
//                            f.createIntGauge("physicalMemory",
//                                                "The actual amount of total physical memory on the machine.",
//                                                "megabytes", true),
//                            f.createIntCounter("processCreates",
//                                                "The total number of times a process has been created.",
//                                                "operations", false),
//                            f.createIntGauge("processes",
//                                                "The number of processes in the computer at the time of data collection.  Notice that this is an instantaneous count, not an average over the time interval.  Each process represents the running of a program.",
//                                                "processes"),
//                            f.createIntGauge("sharedMemory",
//                                                "The number of megabytes of shared memory on the machine.",
//                                                "megabytes", true),
//                            f.createIntGauge("unallocatedSwap",
//                                                "The number of megabytes of swap space that have not been allocated.",
//                                                "megabytes", true),
//
//                            f.createLongCounter("loopbackPackets",
//                                             "The number of network packets sent (or received) on the loopback interface",
//                                             "packets", false),
//                            f.createLongCounter("loopbackBytes",
//                                             "The number of network bytes sent (or received) on the loopback interface",
//                                             "bytes", false),
//			    f.createLongCounter("recvPackets",
//					     "The total number of network packets received (excluding loopback)",
//					     "packets", false),
//                            f.createLongCounter("recvBytes",
//                                              "The total number of network bytes received (excluding loopback)",
//                                              "bytes", false),
//			    f.createLongCounter("recvErrors",
//					     "The total number of network receive errors",
//					     "errors", false),
//                            f.createLongCounter("recvDrops",
//                                             "The total number network receives dropped",
//                                             "packets", false),
//			    f.createLongCounter("xmitPackets",
//					     "The total number of network packets transmitted (excluding loopback)",
//					     "packets", false),
//                            f.createLongCounter("xmitBytes",
//                                             "The total number of network bytes transmitted (excluding loopback)",
//                                             "bytes", false),
//			    f.createLongCounter("xmitErrors",
//					     "The total number of network transmit errors",
//					     "errors", false),
//			    f.createLongCounter("xmitDrops",
//					     "The total number of network transmits dropped",
//					     "packets", false),
//			    f.createLongCounter("xmitCollisions",
//					    "The total number of network transmit collisions",
//					     "collisions", false),
//
//
//                            f.createDoubleGauge("loadAverage1",
//                                                "The average number of threads in the run queue or waiting for disk I/O over the last minute.",
//                                                "threads"),
//                            f.createDoubleGauge("loadAverage15",
//                                                "The average number of threads in the run queue or waiting for disk I/O over the last fifteen minutes.",
//                                                "threads"),
//                            f.createDoubleGauge("loadAverage5",
//                                                "The average number of threads in the run queue or waiting for disk I/O over the last five minutes.",
//                                                "threads"),
                          });

//    checkOffset("allocatedSwap", allocatedSwapINT);
//    checkOffset("bufferMemory", bufferMemoryINT);
//    checkOffset("contextSwitches", contextSwitchesINT);
//    checkOffset("cpuActive", cpuActiveINT);
//    checkOffset("cpuIdle", cpuIdleINT);
//    checkOffset("cpuNice", cpuNiceINT);
//    checkOffset("cpuSystem", cpuSystemINT);
//    checkOffset("cpuUser", cpuUserINT);
//    checkOffset("cpus", cpusINT);
//    checkOffset("freeMemory", freeMemoryINT);
//    checkOffset("pagesPagedIn", pagesPagedInINT);
//    checkOffset("pagesPagedOut", pagesPagedOutINT);
//    checkOffset("pagesSwappedIn", pagesSwappedInINT);
//    checkOffset("pagesSwappedOut", pagesSwappedOutINT);
//    checkOffset("physicalMemory", physicalMemoryINT);
//    checkOffset("processCreates", processCreatesINT);
//    checkOffset("processes", processesINT);
//    checkOffset("sharedMemory", sharedMemoryINT);
//    checkOffset("unallocatedSwap", unallocatedSwapINT);
//
//    checkOffset("loopbackPackets", loopbackPacketsLONG);
//    checkOffset("loopbackBytes", loopbackBytesLONG);
//    checkOffset("recvPackets", recvPacketsLONG);
//    checkOffset("recvBytes", recvBytesLONG);
//    checkOffset("recvErrors", recvErrorsLONG);
//    checkOffset("recvDrops", recvDropsLONG);
//    checkOffset("xmitPackets", xmitPacketsLONG);
//    checkOffset("xmitBytes", xmitBytesLONG);
//    checkOffset("xmitErrors", xmitErrorsLONG);
//    checkOffset("xmitDrops", xmitDropsLONG);
//    checkOffset("xmitCollisions", xmitCollisionsLONG);
//
//    checkOffset("loadAverage1", loadAverage1DOUBLE);
//    checkOffset("loadAverage15", loadAverage15DOUBLE);
//    checkOffset("loadAverage5", loadAverage5DOUBLE);
  }
    
  private OSXSystemStats() {
    // no instances allowed
  }
  public static StatisticsType getType() {
    return myType;
  }
}
