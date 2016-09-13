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
 * Windows machine a GemFire system is running on.
 */
public class WindowsSystemStats
{
  private final static int committedMemoryInUseINT = 0;
  private final static int eventsINT = 1;
  private final static int interruptsINT = 2;
  private final static int mutexesINT = 3;
  private final static int processesINT = 4;
  private final static int processorQueueLengthINT = 5;
  private final static int registryQuotaInUseINT = 6;
  private final static int sharedMemorySectionsINT = 7;
  private final static int semaphoresINT = 8;
  private final static int threadsINT = 9;
  private final static int dgramsReceivedINT = 10;
  private final static int dgramsNoPortINT = 11;
  private final static int dgramsReceivedErrorsINT = 12;
  private final static int dgramsSentINT = 13;
  private final static int loopbackPacketsINT = 14;
  private final static int loopbackBytesINT = 15;
  private final static int netPacketsReceivedINT = 16;
  private final static int netBytesReceivedINT = 17;
  private final static int netPacketsSentINT = 18;
  private final static int netBytesSentINT = 19;


  private final static int availableMemoryLONG = 0;
  private final static int cacheFaultsLONG = 1;
  private final static int cacheSizeLONG = 2;
  private final static int cacheSizePeakLONG = 3;
  private final static int committedMemoryLONG = 4;
  private final static int committedMemoryLimitLONG = 5;
  private final static int contextSwitchesLONG = 6;
  private final static int demandZeroFaultsLONG = 7;
  private final static int pageFaultsLONG = 8;
  private final static int pageReadsLONG = 9;
  private final static int pagesLONG = 10;
  private final static int pageWritesLONG = 11;
  private final static int pagesInputLONG = 12;
  private final static int pagesOutputLONG = 13;
  private final static int systemCallsLONG = 14;

  private final static int cpuActiveDOUBLE = 0;
  private final static int cpuIdleDOUBLE = 1;
  private final static int cpuInterruptDOUBLE = 2;
  private final static int cpuSystemDOUBLE = 3;
  private final static int cpuUserDOUBLE = 4;

  private final static StatisticsType myType;

  private static void checkOffset(String name, int offset) {
    int id = myType.nameToId(name);
    Assert.assertTrue(offset == id, "Expected the offset for " + name + " to be " + offset + " but it was " + id);
  }

  static {
    StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();
    myType = f.createType("WindowsSystemStats",
                          "Statistics on a Microsoft Windows machine.",
                          new StatisticDescriptor[] {
                            f.createIntGauge("committedMemoryInUse",
                                                "This represents the percentage of available virtual memory in use. This is an instantaneous value, not an average.",
                                                "%"),
                            f.createIntGauge("events",
                                                "The number of events in the computer at the time of data collection.  Notice that this is an instantaneous count, not an average over the time interval.  An event is used when two or more threads wish to synchronize execution.",
                                                "items"),
                            f.createIntCounter("interrupts",
                                                "The total number of harware interrupts on the computer. Some devices that may generate interrupts are the system timer, the mouse, data communication lines, network interface cards and other peripheral devices.  This counter provides an indication of how busy these devices are on a computer-wide basis.",
                                                "operations", false),
                            f.createIntGauge("mutexes",
                                                "The number of mutexes in the computer at the time of data collection.  This is an instantaneous count, not an average over the time interval.  Mutexes are used by threads to assure only one thread is executing some section of code.",
                                                "items"),
                            f.createIntGauge("processes",
                                                "The number of processes in the computer at the time of data collection.  Notice that this is an instantaneous count, not an average over the time interval.  Each process represents the running of a program.",
                                                "processes"),
                            f.createIntGauge("processorQueueLength",
                                                "The instantaneous length of the processor queue in units of threads. All processors use a single queue in which threads wait for processor cycles.  This length does not include the threads that are currently executing.  A sustained processor queue length greater than two generally indicates processor congestion.  This is an instantaneous count, not an average over the time interval",
                                                "threads"),
                            f.createIntGauge("registryQuotaInUse",
                                                "The percentage of the Total Registry Quota Allowed currently in use by the system.",
                                                "%"),
                            f.createIntGauge("sharedMemorySections",
                                                "The number of sections in the computer at the time of data collection.  Notice that this is an instantaneous count, not an average over the time interval. A section is a portion of virtual memory created by a process for storing data.  A process may share sections with other processes.",
                                                "items"),
                            f.createIntGauge("semaphores",
                                                "The number of semaphores in the computer at the time of data collection.  Notice that this is an instantaneous count, not an average over the time interval.  Threads use semaphores to obtain exclusive access to data structures that they share with other threads.",
                                                "items"),
                            f.createIntGauge("threads",
                                                "The number of threads in the computer at the time of data collection.  Notice that this is an instantaneous count, not an average over the time interval.  A thread is the basic executable entity that can execute instructions in a processor.",
                                                "threads"),
                            f.createIntCounter(
                                    "dgramsReceived",
                                    "The number of datagrams received on the VM's machine.",
                                    "datagrams/sec"),
                            f.createIntCounter(
                                    "dgramsNoPort",
                                    "The number of incoming datagrams that were discarded due to invalid headers",
                                    "datagrams/sec"),
                            f.createIntCounter(
                                    "dgramsReceivedErrors",
                                    "The number of received UDP datagrams that could not be delivered for reasons other than the lack of an application at the destination port",
                                    "datagrams"),
                            f.createIntCounter(
                                    "dgramsSent",
                                    "The number of datagrams sent on the VM's machine",
                                    "datagrams/sec"),

                            f.createIntCounter(
                                "loopbackPackets",
                                "The number of packets sent/received on the loopback interface",
                                "packets"),
                            f.createIntCounter(
                                "loopbackBytes",
                                "The number of bytes sent/received on the loopback interface",
                                "bytes"),
                            f.createIntCounter("netPacketsReceived",
                                "The number of network packets received (total excluding loopback)",
                                "packets"),
                            f.createIntCounter("netBytesReceived",
                                "The number of network bytes received (total excluding loopback)",
                                "bytes"),
                            f.createIntCounter("netPacketsSent",
                                "The number of network packets sent (total excluding loopback)",
                                "packets"),
                            f.createIntCounter("netBytesSent",
                                "The number of network bytes sent (total excluding loopback)",
                                "bytes"),

                            f.createLongGauge("availableMemory",
                                                "The size, in bytes, of the virtual memory currently on the Zeroed, Free, and Standby lists.  Zeroed and Free memory is ready for use, with Zeroed memory cleared to zeros.  Standby memory is memory removed from a process's Working Set but still available.  Notice that this is an instantaneous count, not an average over the time interval.",
                                                "bytes", true),
                            f.createLongCounter("cacheFaults",
                                                "Incremented whenever the Cache manager does not find a file's page in the immediate Cache and must ask the memory manager to locate the page elsewhere in memory or on the disk so that it can be loaded into the immediate Cache.",
                                                "operations", false),
                            f.createLongGauge("cacheSize",
                                                "Measures the number of bytes currently in use by the system Cache.  The system Cache is used to buffer data retrieved from disk or LAN.  The system Cache uses memory not in use by active processes in the computer.",
                                                "bytes"),
                            f.createLongGauge("cacheSizePeak",
                                                "Measures the maximum number of bytes used by the system Cache.  The system Cache is used to buffer data retrieved from disk or LAN.  The system Cache uses memory not in use by active processes in the computer.",
                                                "bytes"),
                            f.createLongGauge("committedMemory",
                                                "The size of virtual memory, in bytes, that has been Committed (as opposed to simply reserved).  Committed memory must have backing (i.e., disk) storage available, or must be assured never to need disk storage (because main memory is large enough to hold it.)  Notice that this is an instantaneous count, not an average over the time interval.",
                                                "bytes"),
                            f.createLongGauge("committedMemoryLimit",
                                                "The size, in bytes, of virtual memory that can be committed without having to extend the paging file(s).  If the paging file(s) can be extended, this is a soft limit. Note that this value will change if the paging file is extended.",
                                                "bytes"),
                            f.createLongCounter("contextSwitches",
                                                "The number of times this thread has lost the cpu to another thread.  Thread switches can occur either inside of a single process or across processes.  A thread switch may be caused either by one thread asking another for information, or by a thread being preempted by another, higher priority thread becoming ready to run.  Unlike some early operating systems, Windows uses process boundaries for subsystem protection in addition to the traditional protection of User and Privileged modes. These subsystem processes provide additional protection. Therefore, some work done by Windows on behalf of an application may appear in other subsystem processes in addition to the Privileged Time in the application. Switching to the subsystem process causes one Context Switch in the application thread.  Switching back causes another Context Switch in the subsystem thread.",
                                                "operations", false),
                            f.createLongCounter("demandZeroFaults",
                                                "The total number of page faults for pages that must be filled with zeros before the fault is satisfied.  If the Zeroed list is not empty, the fault can be resolved by removing a page from the Zeroed list.",
                                                "operations", false),
                            f.createLongCounter("pageFaults",
                                                "The total number of Page Faults by the threads executing in this process. A page fault occurs when a thread refers to a virtual memory page that is not in its working set in main memory. This will not cause the page to be fetched from disk if it is on the standby list and hence already in main memory, or if it is in use by another process with whom the page is shared.",
                                                "operations", false),
                            f.createLongCounter("pageReads",
                                                "The number of page read operations done by the process since it was last started. These operations involve actual disk reads and not reads from the shared page cache",
                                                "operations", false),
                            f.createLongCounter("pages",
                                                "The total number of pages read from the disk or written to the disk to resolve memory references to pages that were not in memory at the time of the reference. This is the sum of pagesInput and the pagesOutput. This counter includes paging traffic on behalf of the system Cache to access file data for applications.  This value also includes the pages to/from non-cached mapped memory files.  This is the primary counter to observe if you are concerned about excessive memory pressure (that is, thrashing), and the excessive paging that may result.",
                                                "pages", false),
                            f.createLongCounter("pageWrites",
                                                "The number of pages written by the process since it was last started. These page writes are actual disk writes and not just writes into the shared page cache. Unless a large data load is in process, the number should be low for all processes except the Stone's AIO page server process.",
                                                "operations", false),
                            f.createLongCounter("pagesInput",
                                                "The total number of pages read from the disk to resolve memory references to pages that were not in memory at the time of the reference.  This counter includes paging traffic on behalf of the system Cache to access file data for applications.  This is an important counter to observe if you are concerned about excessive memory pressure (that is, thrashing), and the excessive paging that may result",
                                                "pages", false),
                            f.createLongCounter("pagesOutput",
                                                "A count of the total number of pages that are written to disk because the pages have been modified in main memory",
                                                "pages", false),
                            f.createLongCounter("systemCalls",
                                                "The total number of calls to Windows system service routines on the computer.  These routines perform all of the basic scheduling and synchronization of activities on the computer, and provide access to non-graphical devices, memory management, and name space management.",
                                                "operations"),



                            f.createDoubleGauge("cpuActive",
                                                "The percentage of time spent doing useful work by all processors.  On a multi-processor system, if all processors are always busy this is 100%.",
                                                "%"),
                            f.createDoubleGauge("cpuIdle",
                                                "The percentage of time the machine's processors spent idle.",
                                                "%", true),
                            f.createDoubleGauge("cpuInterrupt",
                                                "The percentage of time spent receiving and servicing interrupts on all the processors on the machine. This value is an indirect indicator of the activity of devices that generate interrupts, such as the system clock, the mouse, disk drivers, data communication lines, network interface cards and other peripheral devices. These devices normally interrupt the processor when they have completed a task or require attention.  Normal thread execution is suspended during interrupts.  Most system clocks interrupt the processor every 10 milliseconds, creating a background of interrupt activity. ",
                                                "%"),
                            f.createDoubleGauge("cpuSystem",
                                                "The percentage of time spent in privileged mode by all processors.  On a multi-processor system, if all processors are always in privileged mode this is 100%.  When a Windows system service is called, the service will often run in privileged mode in order to gain access to system-private data.  Such data is protected from access by threads executing in user mode.  Calls to the system may be explicit, or they may be implicit such as when a page fault or an interrupt occurs.  Unlike some early operating systems, Windows uses process boundaries for subsystem protection in addition to the traditional protection of user and privileged modes. These subsystem processes provide additional protection. Therefore, some work done by Windows on behalf of an application may appear in other subsystem processes in addition to the cpuSystem in the application process.",
                                                "%"),
                            f.createDoubleGauge("cpuUser",
                                                "The percentage of time spent executing code in user mode on all the processor's on the machine.",
                                                "%")
                          });
    checkOffset("committedMemoryInUse", committedMemoryInUseINT);
    checkOffset("events", eventsINT);
    checkOffset("interrupts", interruptsINT);
    checkOffset("mutexes", mutexesINT);
    checkOffset("processes", processesINT);
    checkOffset("processorQueueLength", processorQueueLengthINT);
    checkOffset("registryQuotaInUse", registryQuotaInUseINT);
    checkOffset("sharedMemorySections", sharedMemorySectionsINT);
    checkOffset("semaphores", semaphoresINT);
    checkOffset("threads", threadsINT);
    checkOffset("dgramsReceived", dgramsReceivedINT);
    checkOffset("dgramsNoPort", dgramsNoPortINT);
    checkOffset("dgramsReceivedErrors", dgramsReceivedErrorsINT);
    checkOffset("dgramsSent", dgramsSentINT);

    checkOffset("loopbackPackets", loopbackPacketsINT);
    checkOffset("loopbackBytes", loopbackBytesINT);
    checkOffset("netPacketsReceived", netPacketsReceivedINT);
    checkOffset("netBytesReceived", netBytesReceivedINT);
    checkOffset("netPacketsSent", netPacketsSentINT);
    checkOffset("netBytesSent", netBytesSentINT);
    
    checkOffset("availableMemory", availableMemoryLONG);
    checkOffset("cacheFaults", cacheFaultsLONG);
    checkOffset("cacheSize", cacheSizeLONG);
    checkOffset("cacheSizePeak", cacheSizePeakLONG);
    checkOffset("committedMemory", committedMemoryLONG);
    checkOffset("committedMemoryLimit", committedMemoryLimitLONG);
    checkOffset("contextSwitches", contextSwitchesLONG);
    checkOffset("demandZeroFaults", demandZeroFaultsLONG);
    checkOffset("pageFaults", pageFaultsLONG);
    checkOffset("pageReads", pageReadsLONG);
    checkOffset("pages", pagesLONG);
    checkOffset("pageWrites", pageWritesLONG);
    checkOffset("pagesInput", pagesInputLONG);
    checkOffset("pagesOutput", pagesOutputLONG);
    checkOffset("systemCalls", systemCallsLONG);

    checkOffset("cpuActive", cpuActiveDOUBLE);
    checkOffset("cpuIdle", cpuIdleDOUBLE);
    checkOffset("cpuInterrupt", cpuInterruptDOUBLE);
    checkOffset("cpuSystem", cpuSystemDOUBLE);
    checkOffset("cpuUser", cpuUserDOUBLE);
  }

  private WindowsSystemStats() {
    // no instances allowed
  }
  public static StatisticsType getType() {
    return myType;
  }
}
