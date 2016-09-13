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
 * Linux machine a GemFire system is running on.
 */
public class LinuxSystemStats
{

  // shared fields
  final static int allocatedSwapINT = 0;
  final static int bufferMemoryINT = 1;
  final static int sharedMemoryINT = 2;
  final static int cpuActiveINT = 3;
  final static int cpuIdleINT = 4;
  final static int cpuNiceINT = 5;
  final static int cpuSystemINT = 6;
  final static int cpuUserINT = 7;
  final static int iowaitINT = 8;
  final static int irqINT = 9;
  final static int softirqINT = 10;
  final static int cpusINT = 11;
  final static int freeMemoryINT = 12;
  final static int physicalMemoryINT = 13;
  final static int processesINT = 14;
  final static int unallocatedSwapINT = 15;
  final static int cachedMemoryINT = 16;
  final static int dirtyMemoryINT = 17;
  final static int cpuNonUserINT = 18;

  final static int loopbackPacketsLONG = 0;
  final static int loopbackBytesLONG = 1;
  final static int recvPacketsLONG = 2;
  final static int recvBytesLONG = 3;
  final static int recvErrorsLONG = 4;
  final static int recvDropsLONG = 5;
  final static int xmitPacketsLONG = 6;
  final static int xmitBytesLONG = 7;
  final static int xmitErrorsLONG = 8;
  final static int xmitDropsLONG = 9;
  final static int xmitCollisionsLONG = 10;
  final static int contextSwitchesLONG = 11;
  final static int processCreatesLONG = 12;
  final static int pagesPagedInLONG = 13;
  final static int pagesPagedOutLONG = 14;
  final static int pagesSwappedInLONG = 15;
  final static int pagesSwappedOutLONG = 16;
  final static int readsCompletedLONG = 17;
  final static int readsMergedLONG = 18;
  final static int bytesReadLONG = 19;
  final static int timeReadingLONG = 20;
  final static int writesCompletedLONG = 21;
  final static int writesMergedLONG = 22;
  final static int bytesWrittenLONG = 23;
  final static int timeWritingLONG = 24;
  final static int iosInProgressLONG = 25;
  final static int timeIosInProgressLONG = 26;
  final static int ioTimeLONG = 27;

  final static int loadAverage1DOUBLE = 0;
  final static int loadAverage15DOUBLE = 1;
  final static int loadAverage5DOUBLE = 2;

  private final static StatisticsType myType;

  private static void checkOffset(String name, int offset) {
    int id = myType.nameToId(name);
    Assert.assertTrue(offset == id, "Expected the offset for " + name + " to be " + offset + " but it was " + id);
  }

  static {
    StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();
    myType = f.createType("LinuxSystemStats",
                          "Statistics on a Linux machine.",
                          new StatisticDescriptor[] {
                            f.createIntGauge("allocatedSwap",
                                                "The number of megabytes of swap space have actually been written to. Swap space must be reserved before it can be allocated.",
                                                "megabytes"),
                            f.createIntGauge("bufferMemory",
                                                "The number of megabytes of memory allocated to buffers.",
                                                "megabytes"),
                            f.createIntGauge("sharedMemory",
                                                "The number of megabytes of shared memory on the machine.",
                                                "megabytes", true),
                            f.createIntGauge("cpuActive",
                                                "The percentage of the total available time that has been used in a non-idle state.",
                                                "%"),
                            f.createIntGauge("cpuIdle",
                                                "The percentage of the total available time that has been spent sleeping.",
                                                "%", true),
                            f.createIntGauge("cpuNice",
                                                "The percentage of the total available time that has been used to execute user code in processes with low priority.",
                                                "%"),
                            f.createIntGauge("cpuSystem",
                                                "The percentage of the total available time that has been used to execute system (i.e. kernel) code.",
                                                "%"),
                            f.createIntGauge("cpuUser",
                                                "The percentage of the total available time that has been used to execute user code.",
                                                "%"),
                            f.createIntGauge("iowait",
                                                "The percentage of the total available time that has been used to wait for I/O to complete.",
                                                "%"),
                            f.createIntGauge("irq",
                                                "The percentage of the total available time that has been used servicing  interrupts.",
                                                "%"),
                            f.createIntGauge("softirq",
                                                "The percentage of the total available time that has been used servicing softirqs.",
                                                "%"),
                            f.createIntGauge("cpus",
                                                "The number of online cpus on the local machine.",
                                                "items"),
                            f.createIntGauge("freeMemory",
                                                "The number of megabytes of unused memory on the machine.",
                                                "megabytes", true),
                            f.createIntGauge("physicalMemory",
                                                "The actual amount of total physical memory on the machine.",
                                                "megabytes", true),
                            f.createIntGauge("processes",
                                                "The number of processes in the computer at the time of data collection.  Notice that this is an instantaneous count, not an average over the time interval.  Each process represents the running of a program.",
                                                "processes"),
                            f.createIntGauge("unallocatedSwap",
                                                "The number of megabytes of swap space that have not been allocated.",
                                                "megabytes", true),
                            f.createIntGauge("cachedMemory",
                                                "The number of megabytes of memory used for the file system cache.",
                                                "megabytes", true),
                            f.createIntGauge("dirtyMemory",
                                                "The number of megabytes of memory in the file system cache that need to be written.",
                                                "megabytes", true),
                            f.createIntGauge("cpuNonUser",
                                                "The percentage of total available time that has been used to execute non-user code.(includes system, iowait, irq, softirq etc.)",
                                                "%"),


                            f.createLongCounter("loopbackPackets",
                                             "The number of network packets sent (or received) on the loopback interface",
                                             "packets", false),
                            f.createLongCounter("loopbackBytes",
                                             "The number of network bytes sent (or received) on the loopback interface",
                                             "bytes", false),
			    f.createLongCounter("recvPackets",
					     "The total number of network packets received (excluding loopback)",
					     "packets", false),
                            f.createLongCounter("recvBytes",
                                              "The total number of network bytes received (excluding loopback)",
                                              "bytes", false),
			    f.createLongCounter("recvErrors",
					     "The total number of network receive errors",
					     "errors", false),
                            f.createLongCounter("recvDrops",
                                             "The total number network receives dropped",
                                             "packets", false),
			    f.createLongCounter("xmitPackets",
					     "The total number of network packets transmitted (excluding loopback)",
					     "packets", false),
                            f.createLongCounter("xmitBytes",
                                             "The total number of network bytes transmitted (excluding loopback)",
                                             "bytes", false),
			    f.createLongCounter("xmitErrors",
					     "The total number of network transmit errors",
					     "errors", false),
			    f.createLongCounter("xmitDrops",
					     "The total number of network transmits dropped",
					     "packets", false),
			    f.createLongCounter("xmitCollisions",
					    "The total number of network transmit collisions",
					     "collisions", false),
                            f.createLongCounter("contextSwitches",
                                                "The total number of context switches from one thread to another on the computer.  Thread switches can occur either inside of a single process or across processes.  A thread switch may be caused either by one thread asking another for information, or by a thread being preempted by another, higher priority thread becoming ready to run.",
                                                "operations", false),
                            f.createLongCounter("processCreates",
                                                "The total number of times a process has been created.",
                                                "operations", false),
                            f.createLongCounter("pagesPagedIn",
                                                "The total number of pages that have been brought into memory from disk by the operating system's memory manager.",
                                                "pages", false),
                            f.createLongCounter("pagesPagedOut",
                                                "The total number of pages that have been flushed from memory to disk by the operating system's memory manager.",
                                                "pages", false),
                            f.createLongCounter("pagesSwappedIn",
                                                "The total number of swap pages that have been read in from disk by the operating system's memory manager.",
                                                "pages", false),
                            f.createLongCounter("pagesSwappedOut",
                                                "The total number of swap pages that have been written out to disk by the operating system's memory manager.",
                                                "pages", false),
                            f.createLongCounter("diskReadsCompleted",
                                                "The total number disk read operations completed successfully",
                                                "ops"),
                            f.createLongCounter("diskReadsMerged",
                                                "The total number disk read operations that were able to be merge with adjacent reads for efficiency",
                                                "ops"),
                            f.createLongCounter("diskBytesRead",
                                                "The total number bytes read from disk successfully",
                                                "bytes"),
                            f.createLongCounter("diskTimeReading",
                                                "The total number of milliseconds spent reading from disk",
                                                "milliseconds"),
                            f.createLongCounter("diskWritesCompleted",
                                                "The total number disk write operations completed successfully",
                                                "ops"),
                            f.createLongCounter("diskWritesMerged",
                                                "The total number disk write operations that were able to be merge with adjacent reads for efficiency",
                                                "ops"),
                            f.createLongCounter("diskBytesWritten",
                                                "The total number bytes written to disk successfully",
                                                "bytes"),
                            f.createLongCounter("diskTimeWriting",
                                                "The total number of milliseconds spent writing to disk",
                                                "milliseconds"),
                            f.createLongGauge("diskOpsInProgress",
                                                "The current number of disk operations in progress",
                                                "ops"),
                            f.createLongCounter("diskTimeInProgress",
                                                "The total number of milliseconds spent with disk ops in progress",
                                                "milliseconds"),
                            f.createLongCounter("diskTime",
                                                "The total number of milliseconds that measures both completed disk operations and any accumulating backlog of in progress ops.",
                                                "milliseconds"),


                            f.createDoubleGauge("loadAverage1",
                                                "The average number of threads in the run queue or waiting for disk I/O over the last minute.",
                                                "threads"),
                            f.createDoubleGauge("loadAverage15",
                                                "The average number of threads in the run queue or waiting for disk I/O over the last fifteen minutes.",
                                                "threads"),
                            f.createDoubleGauge("loadAverage5",
                                                "The average number of threads in the run queue or waiting for disk I/O over the last five minutes.",
                                                "threads"),
                          });

    checkOffset("allocatedSwap", allocatedSwapINT);
    checkOffset("bufferMemory", bufferMemoryINT);
    checkOffset("sharedMemory", sharedMemoryINT);
    checkOffset("cpuActive", cpuActiveINT);
    checkOffset("cpuIdle", cpuIdleINT);
    checkOffset("cpuNice", cpuNiceINT);
    checkOffset("cpuSystem", cpuSystemINT);
    checkOffset("cpuUser", cpuUserINT);
    checkOffset("iowait", iowaitINT);
    checkOffset("irq", irqINT);
    checkOffset("softirq", softirqINT);
    checkOffset("cpus", cpusINT);
    checkOffset("freeMemory", freeMemoryINT);
    checkOffset("physicalMemory", physicalMemoryINT);
    checkOffset("processes", processesINT);
    checkOffset("unallocatedSwap", unallocatedSwapINT);
    checkOffset("cachedMemory", cachedMemoryINT);
    checkOffset("dirtyMemory", dirtyMemoryINT);
    checkOffset("cpuNonUser", cpuNonUserINT);

    checkOffset("loopbackPackets", loopbackPacketsLONG);
    checkOffset("loopbackBytes", loopbackBytesLONG);
    checkOffset("recvPackets", recvPacketsLONG);
    checkOffset("recvBytes", recvBytesLONG);
    checkOffset("recvErrors", recvErrorsLONG);
    checkOffset("recvDrops", recvDropsLONG);
    checkOffset("xmitPackets", xmitPacketsLONG);
    checkOffset("xmitBytes", xmitBytesLONG);
    checkOffset("xmitErrors", xmitErrorsLONG);
    checkOffset("xmitDrops", xmitDropsLONG);
    checkOffset("xmitCollisions", xmitCollisionsLONG);
    checkOffset("contextSwitches", contextSwitchesLONG);
    checkOffset("processCreates", processCreatesLONG);
    checkOffset("pagesPagedIn", pagesPagedInLONG);
    checkOffset("pagesPagedOut", pagesPagedOutLONG);
    checkOffset("pagesSwappedIn", pagesSwappedInLONG);
    checkOffset("pagesSwappedOut", pagesSwappedOutLONG);
    checkOffset("diskReadsCompleted", readsCompletedLONG);
    checkOffset("diskReadsMerged", readsMergedLONG);
    checkOffset("diskBytesRead", bytesReadLONG);
    checkOffset("diskTimeReading", timeReadingLONG);
    checkOffset("diskWritesCompleted", writesCompletedLONG);
    checkOffset("diskWritesMerged", writesMergedLONG);
    checkOffset("diskBytesWritten", bytesWrittenLONG);
    checkOffset("diskTimeWriting", timeWritingLONG);
    checkOffset("diskOpsInProgress", iosInProgressLONG);
    checkOffset("diskTimeInProgress", timeIosInProgressLONG);
    checkOffset("diskTime", ioTimeLONG);

    checkOffset("loadAverage1", loadAverage1DOUBLE);
    checkOffset("loadAverage15", loadAverage15DOUBLE);
    checkOffset("loadAverage5", loadAverage5DOUBLE);
  }
    
  private LinuxSystemStats() {
    // no instances allowed
  }
  public static StatisticsType getType() {
    return myType;
  }
}
