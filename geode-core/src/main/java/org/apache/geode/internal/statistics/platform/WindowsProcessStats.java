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
import com.gemstone.gemfire.internal.statistics.HostStatHelper;
import com.gemstone.gemfire.internal.statistics.LocalStatisticsImpl;
import com.gemstone.gemfire.internal.statistics.StatisticsTypeFactoryImpl;
import com.gemstone.gemfire.internal.statistics.platform.ProcessStats;

/**
 * <P>This class provides the interface for statistics about a
 * windows operating system process that is using a GemFire system.
 */
public class WindowsProcessStats
{
  private final static int handlesINT = 0;
  private final static int priorityBaseINT = 1;
  private final static int threadsINT = 2;

  private final static int activeTimeLONG = 0;
  private final static int pageFaultsLONG = 1;
  private final static int pageFileSizeLONG = 2;
  private final static int pageFileSizePeakLONG = 3;
  private final static int privateSizeLONG = 4;
  private final static int systemTimeLONG = 5;
  private final static int userTimeLONG = 6;
  private final static int virtualSizeLONG = 7;
  private final static int virtualSizePeakLONG = 8;
  private final static int workingSetSizeLONG = 9;
  private final static int workingSetSizePeakLONG = 10;

  private final static StatisticsType myType;

  private static void checkOffset(String name, int offset) {
    int id = myType.nameToId(name);
    Assert.assertTrue(offset == id, "Expected the offset for " + name + " to be " + offset + " but it was " + id);
  }

  static {
    StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();
    myType = f
        .createType(
            "WindowsProcessStats",
            "Statistics on a Microsoft Window's process.",
            new StatisticDescriptor[] {
                f.createIntGauge(
                        "handles",
                        "The total number of handles currently open by this process. This number is the sum of the handles currently open by each thread in this process.",
                        "items"),
                f.createIntGauge(
                        "priorityBase",
                        "The current base priority of the process. Threads within a process can raise and lower their own base priority relative to the process's base priority",
                        "priority"),
                f.createIntGauge(
                        "threads",
                        "Number of threads currently active in this process. An instruction is the basic unit of execution in a processor, and a thread is the object that executes instructions. Every running process has at least one thread.",
                        "threads"),

                f.createLongCounter(
                        "activeTime",
                        "The elapsed time in milliseconds that all of the threads of this process used the processor to execute instructions. An instruction is the basic unit of execution in a computer, a thread is the object that executes instructions, and a process is the object created when a program is run. Code executed to handle some hardware interrupts and trap conditions are included in this count.",
                        "milliseconds", false),

                f.createLongCounter(
                        "pageFaults",
                        "The total number of Page Faults by the threads executing in this process. A page fault occurs when a thread refers to a virtual memory page that is not in its working set in main memory. This will not cause the page to be fetched from disk if it is on the standby list and hence already in main memory, or if it is in use by another process with whom the page is shared.",
                        "operations", false),
                f.createLongGauge(
                        "pageFileSize",
                        "The current number of bytes this process has used in the paging file(s). Paging files are used to store pages of memory used by the process that are not contained in other files. Paging files are shared by all processes, and lack of space in paging files can prevent other processes from allocating memory.",
                        "bytes"),
                f.createLongGauge(
                        "pageFileSizePeak",
                        "The maximum number of bytes this process has used in the paging file(s). Paging files are used to store pages of memory used by the process that are not contained in other files. Paging files are shared by all processes, and lack of space in paging files can prevent other processes from allocating memory.",
                        "bytes"),
                f.createLongGauge(
                        "privateSize",
                        "The current number of bytes this process has allocated that cannot be shared with other processes.",
                        "bytes"),
                f.createLongCounter(
                        "systemTime",
                        "The elapsed time in milliseconds that the threads of the process have spent executing code in privileged mode. When a Windows system service is called, the service will often run in Privileged Mode to gain access to system-private data. Such data is protected from access by threads executing in user mode. Calls to the system can be explicit or implicit, such as page faults or interrupts. Unlike some early operating systems, Windows uses process boundaries for subsystem protection in addition to the traditional protection of user and privileged modes. These subsystem processes provide additional protection. Therefore, some work done by Windows on behalf of your application might appear in other subsystem processes in addition to the privileged time in your process.",
                        "milliseconds", false),
                f.createLongCounter(
                        "userTime",
                        "The elapsed time in milliseconds that this process's threads have spent executing code in user mode. Applications, environment subsystems, and integral subsystems execute in user mode. Code executing in User Mode cannot damage the integrity of the Windows Executive, Kernel, and device drivers. Unlike some early operating systems, Windows uses process boundaries for subsystem protection in addition to the traditional protection of user and privileged modes. These subsystem processes provide additional protection. Therefore, some work done by Windows on behalf of your application might appear in other subsystem processes in addition to the privileged time in your process.",
                        "milliseconds", false),
                f.createLongGauge(
                        "virtualSize",
                        "Virtual Bytes is the current size in bytes of the virtual address space the process is using. Use of virtual address space does not necessarily imply corresponding use of either disk or main memory pages. Virtual space is finite, and by using too much, the process can limit its ability to load libraries.",
                        "bytes"),
                f.createLongGauge(
                        "virtualSizePeak",
                        "The maximum number of bytes of virtual address space the process has used at any one time. Use of virtual address space does not necessarily imply corresponding use of either disk or main memory pages. Virtual space is however finite, and by using too much, the process might limit its ability to load libraries.",
                        "bytes"),
                f.createLongGauge(
                        "workingSetSize",
                        "The current number of bytes in the Working Set of this process. The Working Set is the set of memory pages touched recently by the threads in the process. If free memory in the computer is above a threshold, pages are left in the Working Set of a process even if they are not in use. When free memory falls below a threshold, pages are trimmed from Working Sets. If they are needed they will then be soft-faulted back into the Working Set before they are paged out out to disk.",
                        "bytes"),
                f.createLongGauge(
                        "workingSetSizePeak",
                        "The maximum number of bytes in the Working Set of this process at any point in time. The Working Set is the set of memory pages touched recently by the threads in the process. If free memory in the computer is above a threshold, pages are left in the Working Set of a process even if they are not in use. When free memory falls below a threshold, pages are trimmed from Working Sets. If they are needed they will then be soft-faulted back into the Working Set before they leave main memory.",
                        "bytes"),

            });
    checkOffset("handles", handlesINT);
    checkOffset("priorityBase", priorityBaseINT);
    checkOffset("threads", threadsINT);

    checkOffset("activeTime", activeTimeLONG);
    checkOffset("pageFaults", pageFaultsLONG);
    checkOffset("pageFileSize", pageFileSizeLONG);
    checkOffset("pageFileSizePeak", pageFileSizePeakLONG);
    checkOffset("privateSize", privateSizeLONG);
    checkOffset("systemTime", systemTimeLONG);
    checkOffset("userTime", userTimeLONG);
    checkOffset("virtualSize", virtualSizeLONG);
    checkOffset("virtualSizePeak", virtualSizePeakLONG);
    checkOffset("workingSetSize", workingSetSizeLONG);
    checkOffset("workingSetSizePeak", workingSetSizePeakLONG);
  }

  private WindowsProcessStats() {
    // no instances allowed
  }
  public static StatisticsType getType() {
    return myType;
  }

  /**
   * Returns a <code>ProcessStats</code> that wraps Windows process
   * <code>Statistics</code>.
   * 
   * @since GemFire 3.5
   */
  public static ProcessStats createProcessStats(final Statistics stats) { // TODO: was package-protected
    if (stats instanceof LocalStatisticsImpl) {
      HostStatHelper.refresh((LocalStatisticsImpl) stats);
    } // otherwise its a Dummy implementation so do nothing
    return new ProcessStats(stats) {
      @Override
        public long getProcessSize() {
          return stats.getLong(workingSetSizeLONG) / (1024*1024);
        }
      };
  }

}
