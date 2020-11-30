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

  public static final String ALLOCATED_SWAP = "allocatedSwap";
  public static final String BUFFER_MEMORY = "bufferMemory";
  public static final String SHARED_MEMORY = "sharedMemory";
  public static final String CPU_ACTIVE = "cpuActive";
  public static final String CPU_IDLE = "cpuIdle";
  public static final String CPU_NICE = "cpuNice";
  public static final String CPU_SYSTEM = "cpuSystem";
  public static final String CPU_USER = "cpuUser";
  public static final String IOWAIT = "iowait";
  public static final String IRQ = "irq";
  public static final String SOFTIRQ = "softirq";
  public static final String CPUS = "cpus";
  public static final String FREE_MEMORY = "freeMemory";
  public static final String PHYSICAL_MEMORY = "physicalMemory";
  public static final String PROCESSES = "processes";
  public static final String UNALLOCATED_SWAP = "unallocatedSwap";
  public static final String CACHED_MEMORY = "cachedMemory";
  public static final String DIRTY_MEMORY = "dirtyMemory";
  public static final String CPU_NON_USER = "cpuNonUser";
  public static final String CPU_STEAL = "cpuSteal";
  public static final String SO_MAX_CONN = "soMaxConn";
  public static final String LOOPBACK_PACKETS = "loopbackPackets";
  public static final String LOOPBACK_BYTES = "loopbackBytes";
  public static final String RECV_PACKETS = "recvPackets";
  public static final String RECV_BYTES = "recvBytes";
  public static final String RECV_ERRORS = "recvErrors";
  public static final String RECV_DROPS = "recvDrops";
  public static final String XMIT_PACKETS = "xmitPackets";
  public static final String XMIT_BYTES = "xmitBytes";
  public static final String XMIT_ERRORS = "xmitErrors";
  public static final String XMIT_DROPS = "xmitDrops";
  public static final String XMIT_COLLISIONS = "xmitCollisions";
  public static final String CONTEXT_SWITCHES = "contextSwitches";
  public static final String PROCESS_CREATES = "processCreates";
  public static final String PAGES_PAGED_IN = "pagesPagedIn";
  public static final String PAGES_PAGED_OUT = "pagesPagedOut";
  public static final String PAGES_SWAPPED_IN = "pagesSwappedIn";
  public static final String PAGES_SWAPPED_OUT = "pagesSwappedOut";
  public static final String DISK_READS_COMPLETED = "diskReadsCompleted";
  public static final String DISK_READS_MERGED = "diskReadsMerged";
  public static final String DISK_BYTES_READ = "diskBytesRead";
  public static final String DISK_TIME_READING = "diskTimeReading";
  public static final String DISK_WRITES_COMPLETED = "diskWritesCompleted";
  public static final String DISK_WRITES_MERGED = "diskWritesMerged";
  public static final String DISK_BYTES_WRITTEN = "diskBytesWritten";
  public static final String DISK_TIME_WRITING = "diskTimeWriting";
  public static final String DISK_OPS_IN_PROGRESS = "diskOpsInProgress";
  public static final String DISK_TIME_IN_PROGRESS = "diskTimeInProgress";
  public static final String DISK_TIME = "diskTime";
  public static final String TCP_EXT_SYN_COOKIES_RECV = "tcpExtSynCookiesRecv";
  public static final String TCP_EXT_SYN_COOKIES_SENT = "tcpExtSynCookiesSent";
  public static final String TCP_EXT_LISTEN_DROPS = "tcpExtListenDrops";
  public static final String TCP_EXT_LISTEN_OVERFLOWS = "tcpExtListenOverflows";

  public static final String LOAD_AVERAGE_1 = "loadAverage1";
  public static final String LOAD_AVERAGE_15 = "loadAverage15";
  public static final String LOAD_AVERAGE_5 = "loadAverage5";

  static {
    StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();
    myType = f.createType("LinuxSystemStats", "Statistics on a Linux machine.",
        new StatisticDescriptor[] {
            f.createLongGauge(ALLOCATED_SWAP,
                "The number of megabytes of swap space have actually been written to. Swap space must be reserved before it can be allocated.",
                "megabytes"),
            f.createLongGauge(BUFFER_MEMORY,
                "The number of megabytes of memory allocated to buffers.", "megabytes"),
            f.createLongGauge(SHARED_MEMORY,
                "The number of megabytes of shared memory on the machine.", "megabytes", true),
            f.createLongGauge(CPU_ACTIVE,
                "The percentage of the total available time that has been used in a non-idle state.",
                "%"),
            f.createLongGauge(CPU_IDLE,
                "The percentage of the total available time that has been spent sleeping.", "%",
                true),
            f.createLongGauge(CPU_NICE,
                "The percentage of the total available time that has been used to execute user code in processes with low priority.",
                "%"),
            f.createLongGauge(CPU_SYSTEM,
                "The percentage of the total available time that has been used to execute system (i.e. kernel) code.",
                "%"),
            f.createLongGauge(CPU_USER,
                "The percentage of the total available time that has been used to execute user code.",
                "%"),
            f.createLongGauge(IOWAIT,
                "The percentage of the total available time that has been used to wait for I/O to complete.",
                "%"),
            f.createLongGauge(IRQ,
                "The percentage of the total available time that has been used servicing  interrupts.",
                "%"),
            f.createLongGauge(SOFTIRQ,
                "The percentage of the total available time that has been used servicing softirqs.",
                "%"),
            f.createLongGauge(CPUS, "The number of online cpus on the local machine.", "items"),
            f.createLongGauge(FREE_MEMORY,
                "The number of megabytes of unused memory on the machine.", "megabytes", true),
            f.createLongGauge(PHYSICAL_MEMORY,
                "The actual amount of total physical memory on the machine.", "megabytes", true),
            f.createLongGauge(PROCESSES,
                "The number of processes in the computer at the time of data collection.  Notice that this is an instantaneous count, not an average over the time interval.  Each process represents the running of a program.",
                "processes"),
            f.createLongGauge(UNALLOCATED_SWAP,
                "The number of megabytes of swap space that have not been allocated.", "megabytes",
                true),
            f.createLongGauge(CACHED_MEMORY,
                "The number of megabytes of memory used for the file system cache.", "megabytes",
                true),
            f.createLongGauge(DIRTY_MEMORY,
                "The number of megabytes of memory in the file system cache that need to be written.",
                "megabytes", true),
            f.createLongGauge(CPU_NON_USER,
                "The percentage of total available time that has been used to execute non-user code.(includes system, iowait, irq, softirq etc.)",
                "%"),
            f.createLongGauge(CPU_STEAL,
                "Steal time is the amount of time the operating system wanted to execute, but was not allowed to by the hypervisor.",
                "%"),
            f.createLongGauge(SO_MAX_CONN,
                "Maximum TCP/IP server socket connection request backlog",
                "connection requests"),

            f.createLongCounter(LOOPBACK_PACKETS,
                "The number of network packets sent (or received) on the loopback interface",
                "packets", false),
            f.createLongCounter(LOOPBACK_BYTES,
                "The number of network bytes sent (or received) on the loopback interface", "bytes",
                false),
            f.createLongCounter(RECV_PACKETS,
                "The total number of network packets received (excluding loopback)", "packets",
                false),
            f.createLongCounter(RECV_BYTES,
                "The total number of network bytes received (excluding loopback)", "bytes", false),
            f.createLongCounter("recvErrors", "The total number of network receive errors",
                "errors", false),
            f.createLongCounter(RECV_DROPS, "The total number network receives dropped", "packets",
                false),
            f.createLongCounter(XMIT_PACKETS,
                "The total number of network packets transmitted (excluding loopback)", "packets",
                false),
            f.createLongCounter(XMIT_BYTES,
                "The total number of network bytes transmitted (excluding loopback)", "bytes",
                false),
            f.createLongCounter(XMIT_ERRORS, "The total number of network transmit errors",
                "errors", false),
            f.createLongCounter(XMIT_DROPS, "The total number of network transmits dropped",
                "packets", false),
            f.createLongCounter(XMIT_COLLISIONS, "The total number of network transmit collisions",
                "collisions", false),
            f.createLongCounter(CONTEXT_SWITCHES,
                "The total number of context switches from one thread to another on the computer.  Thread switches can occur either inside of a single process or across processes.  A thread switch may be caused either by one thread asking another for information, or by a thread being preempted by another, higher priority thread becoming ready to run.",
                "operations", false),
            f.createLongCounter(PROCESS_CREATES,
                "The total number of times a process has been created.", "operations", false),
            f.createLongCounter(PAGES_PAGED_IN,
                "The total number of pages that have been brought into memory from disk by the operating system's memory manager.",
                "pages", false),
            f.createLongCounter(PAGES_PAGED_OUT,
                "The total number of pages that have been flushed from memory to disk by the operating system's memory manager.",
                "pages", false),
            f.createLongCounter(PAGES_SWAPPED_IN,
                "The total number of swap pages that have been read in from disk by the operating system's memory manager.",
                "pages", false),
            f.createLongCounter(PAGES_SWAPPED_OUT,
                "The total number of swap pages that have been written out to disk by the operating system's memory manager.",
                "pages", false),
            f.createLongCounter(DISK_READS_COMPLETED,
                "The total number disk read operations completed successfully", "ops"),
            f.createLongCounter(DISK_READS_MERGED,
                "The total number disk read operations that were able to be merge with adjacent reads for efficiency",
                "ops"),
            f.createLongCounter(DISK_BYTES_READ,
                "The total number bytes read from disk successfully", "bytes"),
            f.createLongCounter(DISK_TIME_READING,
                "The total number of milliseconds spent reading from disk", "milliseconds"),
            f.createLongCounter(DISK_WRITES_COMPLETED,
                "The total number disk write operations completed successfully", "ops"),
            f.createLongCounter(DISK_WRITES_MERGED,
                "The total number disk write operations that were able to be merge with adjacent reads for efficiency",
                "ops"),
            f.createLongCounter(DISK_BYTES_WRITTEN,
                "The total number bytes written to disk successfully", "bytes"),
            f.createLongCounter(DISK_TIME_WRITING,
                "The total number of milliseconds spent writing to disk", "milliseconds"),
            f.createLongGauge(DISK_OPS_IN_PROGRESS,
                "The current number of disk operations in progress", "ops"),
            f.createLongCounter(DISK_TIME_IN_PROGRESS,
                "The total number of milliseconds spent with disk ops in progress", "milliseconds"),
            f.createLongCounter(DISK_TIME,
                "The total number of milliseconds that measures both completed disk operations and any accumulating backlog of in progress ops.",
                "milliseconds"),
            f.createLongCounter(TCP_EXT_SYN_COOKIES_RECV,
                "The number of TCP/IP SYN cookies received due to a full server socket backlog.  "
                    + "If this is non-zero consider disabling SYN cookies because they form sub-optimal connections.",
                "cookies received"),
            f.createLongCounter(TCP_EXT_SYN_COOKIES_SENT,
                "The number of TCP/IP SYN cookies sent due to a full server socket backlog.  "
                    + "If this is non-zero consider disabling SYN cookies because they form sub-optimal connections.",
                "cookies sent"),
            f.createLongCounter(TCP_EXT_LISTEN_DROPS,
                "The number of TCP/IP connection requests that have been dropped due to a full backlog.  "
                    + "If this is large increase the OS SOMAXCONN setting and increase socket backlog settings",
                "requests"),
            f.createLongCounter(TCP_EXT_LISTEN_OVERFLOWS,
                "The number of TCP/IP connection requests that could not be queued due to a small backlog.  "
                    + "These are either dropped (tcpExtListenDrops) or handled via cookies (tcpSynCookiesSent).  "
                    + "In either case you should consider increasing SOMAXCONN and increasing backlog settings.",
                "requests"),


            f.createDoubleGauge(LOAD_AVERAGE_1,
                "The average number of threads in the run queue or waiting for disk I/O over the last minute.",
                "threads"),
            f.createDoubleGauge(LOAD_AVERAGE_15,
                "The average number of threads in the run queue or waiting for disk I/O over the last fifteen minutes.",
                "threads"),
            f.createDoubleGauge(LOAD_AVERAGE_5,
                "The average number of threads in the run queue or waiting for disk I/O over the last five minutes.",
                "threads"),});

    allocatedSwapLONG = myType.nameToId(ALLOCATED_SWAP);
    bufferMemoryLONG = myType.nameToId(BUFFER_MEMORY);
    sharedMemoryLONG = myType.nameToId(SHARED_MEMORY);
    cpuActiveLONG = myType.nameToId(CPU_ACTIVE);
    cpuIdleLONG = myType.nameToId(CPU_IDLE);
    cpuNiceLONG = myType.nameToId(CPU_NICE);
    cpuSystemLONG = myType.nameToId(CPU_SYSTEM);
    cpuUserLONG = myType.nameToId(CPU_USER);
    iowaitLONG = myType.nameToId(IOWAIT);
    irqLONG = myType.nameToId(IRQ);
    softirqLONG = myType.nameToId(SOFTIRQ);
    cpusLONG = myType.nameToId(CPUS);
    freeMemoryLONG = myType.nameToId(FREE_MEMORY);
    physicalMemoryLONG = myType.nameToId(PHYSICAL_MEMORY);
    processesLONG = myType.nameToId(PROCESSES);
    unallocatedSwapLONG = myType.nameToId(UNALLOCATED_SWAP);
    cachedMemoryLONG = myType.nameToId(CACHED_MEMORY);
    dirtyMemoryLONG = myType.nameToId(DIRTY_MEMORY);
    cpuNonUserLONG = myType.nameToId(CPU_NON_USER);
    cpuStealLONG = myType.nameToId(CPU_STEAL);
    tcpSOMaxConnLONG = myType.nameToId(SO_MAX_CONN);
    loopbackPacketsLONG = myType.nameToId(LOOPBACK_PACKETS);
    loopbackBytesLONG = myType.nameToId(LOOPBACK_BYTES);
    recvPacketsLONG = myType.nameToId(RECV_PACKETS);
    recvBytesLONG = myType.nameToId(RECV_BYTES);
    recvErrorsLONG = myType.nameToId(RECV_ERRORS);
    recvDropsLONG = myType.nameToId(RECV_DROPS);
    xmitPacketsLONG = myType.nameToId(XMIT_PACKETS);
    xmitBytesLONG = myType.nameToId(XMIT_BYTES);
    xmitErrorsLONG = myType.nameToId(XMIT_ERRORS);
    xmitDropsLONG = myType.nameToId(XMIT_DROPS);
    xmitCollisionsLONG = myType.nameToId(XMIT_COLLISIONS);
    contextSwitchesLONG = myType.nameToId(CONTEXT_SWITCHES);
    processCreatesLONG = myType.nameToId(PROCESS_CREATES);
    pagesPagedInLONG = myType.nameToId(PAGES_PAGED_IN);
    pagesPagedOutLONG = myType.nameToId(PAGES_PAGED_OUT);
    pagesSwappedInLONG = myType.nameToId(PAGES_SWAPPED_IN);
    pagesSwappedOutLONG = myType.nameToId(PAGES_SWAPPED_OUT);
    readsCompletedLONG = myType.nameToId(DISK_READS_COMPLETED);
    readsMergedLONG = myType.nameToId(DISK_READS_MERGED);
    bytesReadLONG = myType.nameToId(DISK_BYTES_READ);
    timeReadingLONG = myType.nameToId(DISK_TIME_READING);
    writesCompletedLONG = myType.nameToId(DISK_WRITES_COMPLETED);
    writesMergedLONG = myType.nameToId(DISK_WRITES_MERGED);
    bytesWrittenLONG = myType.nameToId(DISK_BYTES_WRITTEN);
    timeWritingLONG = myType.nameToId(DISK_TIME_WRITING);
    iosInProgressLONG = myType.nameToId(DISK_OPS_IN_PROGRESS);
    timeIosInProgressLONG = myType.nameToId(DISK_TIME_IN_PROGRESS);
    ioTimeLONG = myType.nameToId(DISK_TIME);
    tcpExtSynCookiesRecvLONG = myType.nameToId(TCP_EXT_SYN_COOKIES_RECV);
    tcpExtSynCookiesSentLONG = myType.nameToId(TCP_EXT_SYN_COOKIES_SENT);
    tcpExtListenDropsLONG = myType.nameToId(TCP_EXT_LISTEN_DROPS);
    tcpExtListenOverflowsLONG = myType.nameToId(TCP_EXT_LISTEN_OVERFLOWS);

    loadAverage1DOUBLE = myType.nameToId(LOAD_AVERAGE_1);
    loadAverage15DOUBLE = myType.nameToId(LOAD_AVERAGE_15);
    loadAverage5DOUBLE = myType.nameToId(LOAD_AVERAGE_5);
  }

  private LinuxSystemStats() {
    // no instances allowed
  }

  public static StatisticsType getType() {
    return myType;
  }
}
