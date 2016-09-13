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

package org.apache.geode.internal.statistics.platform;

import org.apache.geode.*;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.statistics.StatisticsTypeFactoryImpl;

/**
 * <P>This class provides the interface for statistics about the 
 * Solaris machine a GemFire system is running on.
 */
public class SolarisSystemStats
{
  private final static int allocatedSwapINT = 0;
  private final static int cpuActiveINT = 1;
  private final static int cpuIdleINT = 2;
  private final static int cpuIoWaitINT = 3;
  private final static int cpuSwapWaitINT = 4;
  private final static int cpuSystemINT = 5;
  private final static int cpuUserINT = 6;
  private final static int cpuWaitingINT = 7;
  private final static int cpusINT = 8;
  private final static int freeMemoryINT = 9;
  private final static int physicalMemoryINT = 10;
  private final static int processesINT = 11;
  private final static int reservedSwapINT = 12;
  private final static int schedulerRunCountINT = 13;
  private final static int schedulerSwapCountINT = 14;
  private final static int schedulerWaitCountINT = 15;
  private final static int unreservedSwapINT = 16;
  private final static int unallocatedSwapINT = 17;

  private final static int anonymousPagesFreedLONG = 0;
  private final static int anonymousPagesPagedInLONG = 1;
  private final static int anonymousPagesPagedOutLONG = 2;
  private final static int contextSwitchesLONG = 3;
  private final static int execPagesFreedLONG = 4;
  private final static int execPagesPagedInLONG = 5;
  private final static int execPagesPagedOutLONG = 6;
  private final static int failedMutexEntersLONG = 7;
  private final static int failedReaderLocksLONG = 8;
  private final static int failedWriterLocksLONG = 9;
  private final static int fileSystemPagesFreedLONG = 10;
  private final static int fileSystemPagesPagedInLONG = 11;
  private final static int fileSystemPagesPagedOutLONG = 12;
  private final static int hatMinorFaultsLONG = 13;
  private final static int interruptsLONG = 14;
  private final static int involContextSwitchesLONG = 15;
  private final static int majorPageFaultsLONG = 16;
  private final static int messageCountLONG = 17;
  private final static int pageDaemonCyclesLONG = 18;
  private final static int pageInsLONG = 19;
  private final static int pageOutsLONG = 20;
  private final static int pagerRunsLONG = 21;
  private final static int pagesPagedInLONG = 22;
  private final static int pagesPagedOutLONG = 23;
  private final static int pagesScannedLONG = 24;
  private final static int procsInIoWaitLONG = 25;
  private final static int protectionFaultsLONG = 26;
  private final static int semphoreOpsLONG = 27;
  private final static int softwareLockFaultsLONG = 28;
  private final static int systemCallsLONG = 29;
  private final static int systemMinorFaultsLONG = 30;
  private final static int threadCreatesLONG = 31;
  private final static int trapsLONG = 32;
  private final static int userMinorFaultsLONG = 33;
  private final static int loopbackInputPacketsLONG = 34;
  private final static int loopbackOutputPacketsLONG = 35;
  private final static int inputPacketsLONG = 36;
  private final static int inputErrorsLONG = 37;
  private final static int outputPacketsLONG = 38;
  private final static int outputErrorsLONG = 39;
  private final static int collisionsLONG = 40;
  private final static int inputBytesLONG = 41;
  private final static int outputBytesLONG = 42;
  private final static int multicastInputPacketsLONG = 43;
  private final static int multicastOutputPacketsLONG = 44;
  private final static int broadcastInputPacketsLONG = 45;
  private final static int broadcastOutputPacketsLONG = 46;
  private final static int inputPacketsDiscardedLONG = 47;
  private final static int outputPacketsDiscardedLONG = 48;

  private final static int loadAverage1DOUBLE = 0;
  private final static int loadAverage15DOUBLE = 1;
  private final static int loadAverage5DOUBLE = 2;

  private final static StatisticsType myType;

  private static void checkOffset(String name, int offset) {
    int id = myType.nameToId(name);
    Assert.assertTrue(offset == id, "Expected the offset for " + name + " to be " + offset + " but it was " + id);
  }

  /*
	     "\n  Segments ConnectionsEstablished ConnectionsActive"
	     "\n  ConnectionsPassive ConnectionFailures ConnectionsReset"
	     "\n  SegmentsReceived SegmentsSent SegmentsRetransmitted"
	     "\n  SentTcpBytes RetransmittedTcpBytes AcksSent DelayedAcksSent"
	     "\n  ControlSegmentsSent"
	     "\n  AcksReceived AckedBytes DuplicateAcks AcksForUnsentData "
	     "\n  ReceivedInorderBytes ReceivedOutOfOrderBytes ReceivedDuplicateBytes ReceivedPartialDuplicateBytes "
	     "\n  RetransmitTimeouts RetransmitTimeoutDrops"
	     "\n  KeepAliveTimeouts KeepAliveProbes KeepAliveDrops"
	     "\n  ListenQueueFull HalfOpenQueueFull HalfOpenDrops"
	     "\n) ");
      sprintf(buff, "%d", typeCount);
      TcpTypeCount = typeCount;
      typeCount++;
      strcat(res, buff);
  }

  */

  static {
    StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();
    myType = f.createType("SolarisSystemStats",
                          "Statistics on a Solaris machine.",
                          new StatisticDescriptor[] {
                            f.createIntGauge("allocatedSwap",
                                                "The number of megabytes of swap space have actually been written to. Swap space must be reserved before it can be allocated.",
                                                "megabytes"),
                            f.createIntGauge("cpuActive",
                                                "The percentage of the total available time that has been used to execute user or system code.",
                                                "%"),
                            f.createIntGauge("cpuIdle",
                                                "The percentage of the total available time that has been spent sleeping.",
                                                "%", true),
                            f.createIntGauge("cpuIoWait",
                                                "The percentage of the total available time that has been spent waiting for disk io to complete.",
                                                "%"),
                            f.createIntGauge("cpuSwapWait",
                                                "The percentage of the total available time that has been spent waiting for paging and swapping to complete.",
                                                "%"),
                            f.createIntGauge("cpuSystem",
                                                "The percentage of the total available time that has been used to execute system (i.e. kernel) code.",
                                                "%"),
                            f.createIntGauge("cpuUser",
                                                "The percentage of the total available time that has been used to execute user code.",
                                                "%"),
                            f.createIntGauge("cpuWaiting",
                                                "The percentage of the total available time that has been spent waiting for io, paging, or swapping.",
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
                            f.createIntGauge("reservedSwap",
                                                "The number of megabytes of swap space reserved for allocation by a particular process.",
                                                "megabytes"),
                            f.createIntCounter("schedulerRunCount",
                                                "The total number of times the system scheduler has put a thread in its run queue.",
                                                "operations", false),
                            f.createIntCounter("schedulerSwapCount",
                                                "The total number of times the system scheduler has swapped out an idle process.",
                                                "operations", false),
                            f.createIntCounter("schedulerWaitCount",
                                                "The total number of times the system scheduler has removed a thread from the run queue because it was waiting for a resource.",
                                                "operations", false),
                            f.createIntGauge("unreservedSwap",
                                                "The number of megabytes of swap space that are free. If this value goes to zero new processes can no longer be created.",
                                                "megabytes", true),
                            f.createIntGauge("unallocatedSwap",
                                                "The number of megabytes of swap space that have not been allocated.",
                                                "megabytes", true),


                            f.createLongCounter("anonymousPagesFreed",
                                                "The total number pages that contain heap, stack, or other changeable data that have been removed from memory and added to the free list.",
                                                "pages"),
                            f.createLongCounter("anonymousPagesPagedIn",
                                                "The total number pages that contain heap, stack, or other changeable data that have been allocated in memory and possibly copied from disk.",
                                                "pages", false),
                            f.createLongCounter("anonymousPagesPagedOut",
                                                "The total number pages that contain heap, stack, or other changeable data that have been removed from memory and copied to disk.",
                                                "pages", false),
                            f.createLongCounter("contextSwitches",
                                                "The total number of context switches from one thread to another on the computer.  Thread switches can occur either inside of a single process or across processes.  A thread switch may be caused either by one thread asking another for information, or by a thread being preempted by another, higher priority thread becoming ready to run.",
                                                "operations", false),
                            f.createLongCounter("execPagesFreed",
                                                "The total number readonly pages that contain code or data that have been removed from memory and returned to the free list.",
                                                "pages"),
                            f.createLongCounter("execPagesPagedIn",
                                                "The total number readonly pages that contain code or data that have been copied from disk to memory.",
                                                "pages", false),
                            f.createLongCounter("execPagesPagedOut",
                                                "The total number readonly pages that contain code or data that have been removed from memory and will need to be paged in when used again.",
                                                "pages", false),
                            f.createLongCounter("failedMutexEnters",
                                                "The total number of times a thread entering a mutex had to wait for the mutex to be unlocked.",
                                                "operations", false),
                            f.createLongCounter("failedReaderLocks",
                                                "The total number of times readers failed to obtain a readers/writer locks on their first try. When this happens the reader has to wait for the current writer to release the lock.",
                                                "operations", false),
                            f.createLongCounter("failedWriterLocks",
                                                "The total number of times writers failed to obtain a readers/writer locks on their first try. When this happens the writer has to wait for all the current readers or the single writer to release the lock.",
                                                "operations", false),
                            f.createLongCounter("fileSystemPagesFreed",
                                                "The total number of pages, that contained the contents of a file due to the file being read from a file system, that   have been removed from memory and put on the free list.",
                                                "pages"),
                            f.createLongCounter("fileSystemPagesPagedIn",
                                                "The total number of pages that contain the contents of a file due to the file being read from a file system.",
                                                "pages", false),
                            f.createLongCounter("fileSystemPagesPagedOut",
                                                "The total number of pages, that contained the contents of a file due to the file being read from a file system, that   have been removed from memory and copied to disk.",
                                                "pages", false),
                            f.createLongCounter("hatMinorFaults",
                                                "The total number of hat faults. You only get these on systems with software memory management units.",
                                                "operations", false),
                            f.createLongCounter("interrupts",
                                                "The total number of interrupts that have occurred on the computer.",
                                                "operations", false),
                            f.createLongCounter("involContextSwitches",
                                                "The total number of times a thread was forced to give up the cpu even though it was still ready to run.",
                                                "operations", false),
                            f.createLongCounter("majorPageFaults",
                                                "The total number of times a page fault required disk io to get the page",
                                                "operations", false),
                            f.createLongCounter("messageCount",
                                                "The total number of msgrcv() and msgsnd() system calls.",
                                                "messages"),
                            f.createLongCounter("pageDaemonCycles",
                                                "The total number of revolutions of the page daemon's scan \"clock hand\".",
                                                "operations", false),
                            f.createLongCounter("pageIns",
                                                "The total number of times pages have been brought into memory from disk by the operating system's memory manager.",
                                                "operations", false),
                            f.createLongCounter("pageOuts",
                                                "The total number of times pages have been flushed from memory to disk by the operating system's memory manager.",
                                                "operations", false),
                            f.createLongCounter("pagerRuns",
                                                "The total number of times the pager daemon has been scheduled to run.",
                                                "operations", false),
                            f.createLongCounter("pagesPagedIn",
                                                "The total number of pages that have been brought into memory from disk by the operating system's memory manager.",
                                                "pages", false),
                            f.createLongCounter("pagesPagedOut",
                                                "The total number of pages that have been flushed from memory to disk by the operating system's memory manager.",
                                                "pages", false),
                            f.createLongCounter("pagesScanned",
                                                "The total number pages examined by the pageout daemon. When the amount of free memory gets below a certain size, the daemon start to look for inactive memory pages to steal from processes. So I high scan rate is a good indication of needing more memory.",
                                                "pages", false),
                            f.createLongGauge("procsInIoWait",
                                                "The number of processes waiting for block I/O at this instant in time.",
                                                "processes"),
                            f.createLongCounter("protectionFaults",
                                                "The total number of times memory has been accessed in a way that was not allowed. This results in a segementation violation and in most cases a core dump.",
                                                "operations", false),
                            f.createLongCounter("semphoreOps",
                                                "The total number of semaphore operations.",
                                                "operations"),
                            f.createLongCounter("softwareLockFaults",
                                                "The total number of faults caused by software locks held on memory pages.",
                                                "operations", false),
                            f.createLongCounter("systemCalls",
                                                "The total number system calls.",
                                                "operations"),
                            f.createLongCounter("systemMinorFaults",
                                                "The total number of minor page faults in kernel code. Minor page faults do not require disk access.",
                                                "operations", false),
                            f.createLongCounter("threadCreates",
                                                "The total number of times a thread has been created.",
                                                "operations", false),
                            f.createLongCounter("traps",
                                                "The total number of traps that have occurred on the computer.",
                                                "operations", false),
                            f.createLongCounter("userMinorFaults",
                                                "The total number of minor page faults in non-kernel code. Minor page faults do not require disk access.",
                                                "operatations", false),
                            f.createLongCounter("loopbackInputPackets",
                                                "The total number of input packets received over the loopback network adaptor.",
                                                "packets"),
                            f.createLongCounter("loopbackOutputPackets",
                                                "The total number of output packets sent over the loopback network adaptor.",
                                                "packets"),
                            f.createLongCounter("inputPackets",
                                                "packets received (Solaris kstat 'ipackets')",
                                                "packets"),
                            f.createLongCounter("inputErrors",
                                                "input errors (Solaris kstat 'ierrors')",
                                                "errors", false),
                            f.createLongCounter("outputPackets",
                                                "Solaris kstat 'opackets'",
                                                "packets"),
                            f.createLongCounter("outputErrors",
                                                "output errors (Solaris kstat 'oerrors')",
                                                "errors", false),
                            f.createLongCounter("collisions",
                                                "Solaris kstat 'collisions'",
                                                "collisions", false),
                            f.createLongCounter("inputBytes",
                                                "octets received (Solaris kstat 'rbytes')",
                                                "bytes"),
                            f.createLongCounter("outputBytes",
                                                "octats transmitted (Solaris kstat 'obytes')",
                                                "bytes"),
                            f.createLongCounter("multicastInputPackets",
                                                "multicast received (Solaris kstat 'multircv')",
                                                "packets"),
                            f.createLongCounter("multicastOutputPackets",
                                                "multicast requested to be sent (Solaris kstat 'multixmt')",
                                                "packets"),
                            f.createLongCounter("broadcastInputPackets",
                                                "broadcast received (Solaris kstat 'brdcstrcv')",
                                                "packets"),
                            f.createLongCounter("broadcastOutputPackets",
                                                "broadcast requested to be sent (Solaris kstat 'brdcstxmt')",
                                                "packets"),
                            f.createLongCounter("inputPacketsDiscarded",
                                                "number receive packets discarded (Solaris kstat 'norcvbuf')",
                                                "packets"),
                            f.createLongCounter("outputPacketsDiscarded",
                                               "packets that could not be sent up because the queue was flow controlled (Solaris kstat 'noxmtbuf')",
                                                "packets"),


                            f.createDoubleGauge("loadAverage1",
                                                "The average number of threads ready to run over the last minute.",
                                                "threads"),
                            f.createDoubleGauge("loadAverage15",
                                                "The average number of threads ready to run over the last fifteen minutes.",
                                                "threads"),
                            f.createDoubleGauge("loadAverage5",
                                                "The average number of threads ready to run over the last five minutes.",
                                                "threads")
                          });
    checkOffset("allocatedSwap", allocatedSwapINT);
    checkOffset("cpuActive", cpuActiveINT);
    checkOffset("cpuIdle", cpuIdleINT);
    checkOffset("cpuIoWait", cpuIoWaitINT);
    checkOffset("cpuSwapWait", cpuSwapWaitINT);
    checkOffset("cpuSystem", cpuSystemINT);
    checkOffset("cpuUser", cpuUserINT);
    checkOffset("cpuWaiting", cpuWaitingINT);
    checkOffset("cpus", cpusINT);
    checkOffset("freeMemory", freeMemoryINT);
    checkOffset("physicalMemory", physicalMemoryINT);
    checkOffset("processes", processesINT);
    checkOffset("reservedSwap", reservedSwapINT);
    checkOffset("schedulerRunCount", schedulerRunCountINT);
    checkOffset("schedulerSwapCount", schedulerSwapCountINT);
    checkOffset("schedulerWaitCount", schedulerWaitCountINT);
    checkOffset("unreservedSwap", unreservedSwapINT);
    checkOffset("unallocatedSwap", unallocatedSwapINT);

    checkOffset("anonymousPagesFreed", anonymousPagesFreedLONG);
    checkOffset("anonymousPagesPagedIn", anonymousPagesPagedInLONG);
    checkOffset("anonymousPagesPagedOut", anonymousPagesPagedOutLONG);
    checkOffset("contextSwitches", contextSwitchesLONG);
    checkOffset("execPagesFreed", execPagesFreedLONG);
    checkOffset("execPagesPagedIn", execPagesPagedInLONG);
    checkOffset("execPagesPagedOut", execPagesPagedOutLONG);
    checkOffset("failedMutexEnters", failedMutexEntersLONG);
    checkOffset("failedReaderLocks", failedReaderLocksLONG);
    checkOffset("failedWriterLocks", failedWriterLocksLONG);
    checkOffset("fileSystemPagesFreed", fileSystemPagesFreedLONG);
    checkOffset("fileSystemPagesPagedIn", fileSystemPagesPagedInLONG);
    checkOffset("fileSystemPagesPagedOut", fileSystemPagesPagedOutLONG);
    checkOffset("hatMinorFaults", hatMinorFaultsLONG);
    checkOffset("interrupts", interruptsLONG);
    checkOffset("involContextSwitches", involContextSwitchesLONG);
    checkOffset("majorPageFaults", majorPageFaultsLONG);
    checkOffset("messageCount", messageCountLONG);
    checkOffset("pageDaemonCycles", pageDaemonCyclesLONG);
    checkOffset("pageIns", pageInsLONG);
    checkOffset("pageOuts", pageOutsLONG);
    checkOffset("pagerRuns", pagerRunsLONG);
    checkOffset("pagesPagedIn", pagesPagedInLONG);
    checkOffset("pagesPagedOut", pagesPagedOutLONG);
    checkOffset("pagesScanned", pagesScannedLONG);
    checkOffset("procsInIoWait", procsInIoWaitLONG);
    checkOffset("protectionFaults", protectionFaultsLONG);
    checkOffset("semphoreOps", semphoreOpsLONG);
    checkOffset("softwareLockFaults", softwareLockFaultsLONG);
    checkOffset("systemCalls", systemCallsLONG);
    checkOffset("systemMinorFaults", systemMinorFaultsLONG);
    checkOffset("threadCreates", threadCreatesLONG);
    checkOffset("traps", trapsLONG);
    checkOffset("userMinorFaults", userMinorFaultsLONG);
    checkOffset("loopbackInputPackets", loopbackInputPacketsLONG);
    checkOffset("loopbackOutputPackets", loopbackOutputPacketsLONG);
    checkOffset("inputPackets", inputPacketsLONG);
    checkOffset("inputErrors", inputErrorsLONG);
    checkOffset("outputPackets", outputPacketsLONG); 
    checkOffset("outputErrors", outputErrorsLONG);
    checkOffset("collisions", collisionsLONG);
    checkOffset("inputBytes", inputBytesLONG);
    checkOffset("outputBytes", outputBytesLONG);
    checkOffset("multicastInputPackets", multicastInputPacketsLONG);
    checkOffset("multicastOutputPackets", multicastOutputPacketsLONG);
    checkOffset("broadcastInputPackets", broadcastInputPacketsLONG);
    checkOffset("broadcastOutputPackets", broadcastOutputPacketsLONG);
    checkOffset("inputPacketsDiscarded", inputPacketsDiscardedLONG);
    checkOffset("outputPacketsDiscarded", outputPacketsDiscardedLONG);

    checkOffset("loadAverage1", loadAverage1DOUBLE);
    checkOffset("loadAverage15", loadAverage15DOUBLE);
    checkOffset("loadAverage5", loadAverage5DOUBLE);
  }

  private SolarisSystemStats() {
    // no instances allowed
  }
  public static StatisticsType getType() {
    return myType;
  }
}
