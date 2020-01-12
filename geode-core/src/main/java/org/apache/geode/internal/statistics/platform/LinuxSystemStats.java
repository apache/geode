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

package org.apache.geode.internal.statistics.platform;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.StatisticsType;
import org.apache.geode.StatisticsTypeFactory;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.internal.statistics.StatisticsTypeFactoryImpl;

/**
 * <P>
 * This class provides the interface for statistics about the Linux machine a GemFire system is
 * running on.
 */
public class LinuxSystemStats {

  // shared fields
  static final int allocatedSwapLONG;
  static final int bufferMemoryLONG;
  static final int sharedMemoryLONG;
  static final int cpuActiveLONG;
  static final int cpuIdleLONG;
  static final int cpuNiceLONG;
  static final int cpuSystemLONG;
  static final int cpuUserLONG;
  static final int iowaitLONG;
  static final int irqLONG;
  static final int softirqLONG;
  static final int cpusLONG;
  static final int freeMemoryLONG;
  static final int physicalMemoryLONG;
  static final int processesLONG;
  static final int unallocatedSwapLONG;
  static final int cachedMemoryLONG;
  static final int dirtyMemoryLONG;
  static final int cpuNonUserLONG;
  static final int cpuStealLONG;
  static final int tcpSOMaxConnLONG;

  static final int loopbackPacketsLONG;
  static final int loopbackBytesLONG;
  static final int recvPacketsLONG;
  static final int recvBytesLONG;
  static final int recvErrorsLONG;
  static final int recvDropsLONG;
  static final int xmitPacketsLONG;
  static final int xmitBytesLONG;
  static final int xmitErrorsLONG;
  static final int xmitDropsLONG;
  static final int xmitCollisionsLONG;
  static final int contextSwitchesLONG;
  static final int processCreatesLONG;
  static final int pagesPagedInLONG;
  static final int pagesPagedOutLONG;
  static final int pagesSwappedInLONG;
  static final int pagesSwappedOutLONG;
  static final int readsCompletedLONG;
  static final int readsMergedLONG;
  static final int bytesReadLONG;
  static final int timeReadingLONG;
  static final int writesCompletedLONG;
  static final int writesMergedLONG;
  static final int bytesWrittenLONG;
  static final int timeWritingLONG;
  static final int iosInProgressLONG;
  static final int timeIosInProgressLONG;
  static final int ioTimeLONG;
  static final int tcpExtSynCookiesRecvLONG;
  static final int tcpExtSynCookiesSentLONG;
  static final int tcpExtListenDropsLONG;
  static final int tcpExtListenOverflowsLONG;


  static final int loadAverage1DOUBLE;
  static final int loadAverage15DOUBLE;
  static final int loadAverage5DOUBLE;

  @Immutable
  private static final StatisticsType myType;

  static {
    StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();
    myType = f.createType("LinuxSystemStats", "Statistics on a Linux machine.",
        new StatisticDescriptor[] {
            f.createLongGauge("allocatedSwap",
                "The number of megabytes of swap space have actually been written to. Swap space must be reserved before it can be allocated.",
                "megabytes"),
            f.createLongGauge("bufferMemory",
                "The number of megabytes of memory allocated to buffers.", "megabytes"),
            f.createLongGauge("sharedMemory",
                "The number of megabytes of shared memory on the machine.", "megabytes", true),
            f.createLongGauge("cpuActive",
                "The percentage of the total available time that has been used in a non-idle state.",
                "%"),
            f.createLongGauge("cpuIdle",
                "The percentage of the total available time that has been spent sleeping.", "%",
                true),
            f.createLongGauge("cpuNice",
                "The percentage of the total available time that has been used to execute user code in processes with low priority.",
                "%"),
            f.createLongGauge("cpuSystem",
                "The percentage of the total available time that has been used to execute system (i.e. kernel) code.",
                "%"),
            f.createLongGauge("cpuUser",
                "The percentage of the total available time that has been used to execute user code.",
                "%"),
            f.createLongGauge("iowait",
                "The percentage of the total available time that has been used to wait for I/O to complete.",
                "%"),
            f.createLongGauge("irq",
                "The percentage of the total available time that has been used servicing  interrupts.",
                "%"),
            f.createLongGauge("softirq",
                "The percentage of the total available time that has been used servicing softirqs.",
                "%"),
            f.createLongGauge("cpus", "The number of online cpus on the local machine.", "items"),
            f.createLongGauge("freeMemory",
                "The number of megabytes of unused memory on the machine.", "megabytes", true),
            f.createLongGauge("physicalMemory",
                "The actual amount of total physical memory on the machine.", "megabytes", true),
            f.createLongGauge("processes",
                "The number of processes in the computer at the time of data collection.  Notice that this is an instantaneous count, not an average over the time interval.  Each process represents the running of a program.",
                "processes"),
            f.createLongGauge("unallocatedSwap",
                "The number of megabytes of swap space that have not been allocated.", "megabytes",
                true),
            f.createLongGauge("cachedMemory",
                "The number of megabytes of memory used for the file system cache.", "megabytes",
                true),
            f.createLongGauge("dirtyMemory",
                "The number of megabytes of memory in the file system cache that need to be written.",
                "megabytes", true),
            f.createLongGauge("cpuNonUser",
                "The percentage of total available time that has been used to execute non-user code.(includes system, iowait, irq, softirq etc.)",
                "%"),
            f.createLongGauge("cpuSteal",
                "Steal time is the amount of time the operating system wanted to execute, but was not allowed to by the hypervisor.",
                "%"),
            f.createLongGauge("soMaxConn",
                "Maximum TCP/IP server socket connection request backlog",
                "connection requests"),

            f.createLongCounter("loopbackPackets",
                "The number of network packets sent (or received) on the loopback interface",
                "packets", false),
            f.createLongCounter("loopbackBytes",
                "The number of network bytes sent (or received) on the loopback interface", "bytes",
                false),
            f.createLongCounter("recvPackets",
                "The total number of network packets received (excluding loopback)", "packets",
                false),
            f.createLongCounter("recvBytes",
                "The total number of network bytes received (excluding loopback)", "bytes", false),
            f.createLongCounter("recvErrors", "The total number of network receive errors",
                "errors", false),
            f.createLongCounter("recvDrops", "The total number network receives dropped", "packets",
                false),
            f.createLongCounter("xmitPackets",
                "The total number of network packets transmitted (excluding loopback)", "packets",
                false),
            f.createLongCounter("xmitBytes",
                "The total number of network bytes transmitted (excluding loopback)", "bytes",
                false),
            f.createLongCounter("xmitErrors", "The total number of network transmit errors",
                "errors", false),
            f.createLongCounter("xmitDrops", "The total number of network transmits dropped",
                "packets", false),
            f.createLongCounter("xmitCollisions", "The total number of network transmit collisions",
                "collisions", false),
            f.createLongCounter("contextSwitches",
                "The total number of context switches from one thread to another on the computer.  Thread switches can occur either inside of a single process or across processes.  A thread switch may be caused either by one thread asking another for information, or by a thread being preempted by another, higher priority thread becoming ready to run.",
                "operations", false),
            f.createLongCounter("processCreates",
                "The total number of times a process has been created.", "operations", false),
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
                "The total number disk read operations completed successfully", "ops"),
            f.createLongCounter("diskReadsMerged",
                "The total number disk read operations that were able to be merge with adjacent reads for efficiency",
                "ops"),
            f.createLongCounter("diskBytesRead",
                "The total number bytes read from disk successfully", "bytes"),
            f.createLongCounter("diskTimeReading",
                "The total number of milliseconds spent reading from disk", "milliseconds"),
            f.createLongCounter("diskWritesCompleted",
                "The total number disk write operations completed successfully", "ops"),
            f.createLongCounter("diskWritesMerged",
                "The total number disk write operations that were able to be merge with adjacent reads for efficiency",
                "ops"),
            f.createLongCounter("diskBytesWritten",
                "The total number bytes written to disk successfully", "bytes"),
            f.createLongCounter("diskTimeWriting",
                "The total number of milliseconds spent writing to disk", "milliseconds"),
            f.createLongGauge("diskOpsInProgress",
                "The current number of disk operations in progress", "ops"),
            f.createLongCounter("diskTimeInProgress",
                "The total number of milliseconds spent with disk ops in progress", "milliseconds"),
            f.createLongCounter("diskTime",
                "The total number of milliseconds that measures both completed disk operations and any accumulating backlog of in progress ops.",
                "milliseconds"),
            f.createLongCounter("tcpExtSynCookiesRecv",
                "The number of TCP/IP SYN cookies received due to a full server socket backlog.  "
                    + "If this is non-zero consider disabling SYN cookies because they form sub-optimal connections.",
                "cookies received"),
            f.createLongCounter("tcpExtSynCookiesSent",
                "The number of TCP/IP SYN cookies sent due to a full server socket backlog.  "
                    + "If this is non-zero consider disabling SYN cookies because they form sub-optimal connections.",
                "cookies sent"),
            f.createLongCounter("tcpExtListenDrops",
                "The number of TCP/IP connection requests that have been dropped due to a full backlog.  "
                    + "If this is large increase the OS SOMAXCONN setting and increase socket backlog settings",
                "requests"),
            f.createLongCounter("tcpExtListenOverflows",
                "The number of TCP/IP connection requests that could not be queued due to a small backlog.  "
                    + "These are either dropped (tcpExtListenDrops) or handled via cookies (tcpSynCookiesSent).  "
                    + "In either case you should consider increasing SOMAXCONN and increasing backlog settings.",
                "requests"),


            f.createDoubleGauge("loadAverage1",
                "The average number of threads in the run queue or waiting for disk I/O over the last minute.",
                "threads"),
            f.createDoubleGauge("loadAverage15",
                "The average number of threads in the run queue or waiting for disk I/O over the last fifteen minutes.",
                "threads"),
            f.createDoubleGauge("loadAverage5",
                "The average number of threads in the run queue or waiting for disk I/O over the last five minutes.",
                "threads"),});

    allocatedSwapLONG = myType.nameToId("allocatedSwap");
    bufferMemoryLONG = myType.nameToId("bufferMemory");
    sharedMemoryLONG = myType.nameToId("sharedMemory");
    cpuActiveLONG = myType.nameToId("cpuActive");
    cpuIdleLONG = myType.nameToId("cpuIdle");
    cpuNiceLONG = myType.nameToId("cpuNice");
    cpuSystemLONG = myType.nameToId("cpuSystem");
    cpuUserLONG = myType.nameToId("cpuUser");
    iowaitLONG = myType.nameToId("iowait");
    irqLONG = myType.nameToId("irq");
    softirqLONG = myType.nameToId("softirq");
    cpusLONG = myType.nameToId("cpus");
    freeMemoryLONG = myType.nameToId("freeMemory");
    physicalMemoryLONG = myType.nameToId("physicalMemory");
    processesLONG = myType.nameToId("processes");
    unallocatedSwapLONG = myType.nameToId("unallocatedSwap");
    cachedMemoryLONG = myType.nameToId("cachedMemory");
    dirtyMemoryLONG = myType.nameToId("dirtyMemory");
    cpuNonUserLONG = myType.nameToId("cpuNonUser");
    cpuStealLONG = myType.nameToId("cpuSteal");
    tcpSOMaxConnLONG = myType.nameToId("soMaxConn");
    loopbackPacketsLONG = myType.nameToId("loopbackPackets");
    loopbackBytesLONG = myType.nameToId("loopbackBytes");
    recvPacketsLONG = myType.nameToId("recvPackets");
    recvBytesLONG = myType.nameToId("recvBytes");
    recvErrorsLONG = myType.nameToId("recvErrors");
    recvDropsLONG = myType.nameToId("recvDrops");
    xmitPacketsLONG = myType.nameToId("xmitPackets");
    xmitBytesLONG = myType.nameToId("xmitBytes");
    xmitErrorsLONG = myType.nameToId("xmitErrors");
    xmitDropsLONG = myType.nameToId("xmitDrops");
    xmitCollisionsLONG = myType.nameToId("xmitCollisions");
    contextSwitchesLONG = myType.nameToId("contextSwitches");
    processCreatesLONG = myType.nameToId("processCreates");
    pagesPagedInLONG = myType.nameToId("pagesPagedIn");
    pagesPagedOutLONG = myType.nameToId("pagesPagedOut");
    pagesSwappedInLONG = myType.nameToId("pagesSwappedIn");
    pagesSwappedOutLONG = myType.nameToId("pagesSwappedOut");
    readsCompletedLONG = myType.nameToId("diskReadsCompleted");
    readsMergedLONG = myType.nameToId("diskReadsMerged");
    bytesReadLONG = myType.nameToId("diskBytesRead");
    timeReadingLONG = myType.nameToId("diskTimeReading");
    writesCompletedLONG = myType.nameToId("diskWritesCompleted");
    writesMergedLONG = myType.nameToId("diskWritesMerged");
    bytesWrittenLONG = myType.nameToId("diskBytesWritten");
    timeWritingLONG = myType.nameToId("diskTimeWriting");
    iosInProgressLONG = myType.nameToId("diskOpsInProgress");
    timeIosInProgressLONG = myType.nameToId("diskTimeInProgress");
    ioTimeLONG = myType.nameToId("diskTime");
    tcpExtSynCookiesRecvLONG = myType.nameToId("tcpExtSynCookiesRecv");
    tcpExtSynCookiesSentLONG = myType.nameToId("tcpExtSynCookiesSent");
    tcpExtListenDropsLONG = myType.nameToId("tcpExtListenDrops");
    tcpExtListenOverflowsLONG = myType.nameToId("tcpExtListenOverflows");

    loadAverage1DOUBLE = myType.nameToId("loadAverage1");
    loadAverage15DOUBLE = myType.nameToId("loadAverage15");
    loadAverage5DOUBLE = myType.nameToId("loadAverage5");
  }

  private LinuxSystemStats() {
    // no instances allowed
  }

  public static StatisticsType getType() {
    return myType;
  }
}
