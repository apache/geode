/*=========================================================================
 * Copyright (c) 2004-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include <gfcpp/gfcpp_globals.hpp>
#include <ace/Thread_Mutex.h>
#include <ace/Singleton.h>
#include "WindowsProcessStats.hpp"
#include "HostStatHelperWin.hpp"
#include "GemfireStatisticsFactory.hpp"
#include "HostStatHelper.hpp"

using namespace gemfire_statistics;

WindowsProcessStats::WindowsProcessStats(int64 pid, const char* name) {
  GemfireStatisticsFactory* statFactory =
      GemfireStatisticsFactory::getExistingInstance();

  // Create for Statistics Type
  createType(statFactory);

  // Create Statistics
  this->stats = statFactory->createOsStatistics(m_statsType, name, pid);
  GF_D_ASSERT(this->stats != NULL);

// Refresh Stats Values
#if defined(_WIN32)
  HostStatHelperWin::initHostStatHelperWin();
  HostStatHelperWin::refreshProcess(this);
#endif
}

void WindowsProcessStats::createType(StatisticsFactory* statFactory) {
  StatisticDescriptor** statDescriptorArr = new StatisticDescriptor*[15];
  statDescriptorArr[0] = statFactory->createIntGauge(
      "handles",
      "The total number of handles currently open by this process. This number "
      "is the sum of the handles currently open by each thread in this "
      "process.",
      "items");
  statDescriptorArr[1] = statFactory->createIntGauge(
      "priorityBase",
      "The current base priority of the process. Threads within a process can "
      "raise and lower their own base priority relative to the process's base "
      "priority",
      "priority");
  statDescriptorArr[2] = statFactory->createIntGauge(
      "threads",
      "Number of threads currently active in this process. An instruction is "
      "the basic unit of execution in a processor, and a thread is the object "
      "that executes instructions. Every running process has at least one "
      "thread.",
      "threads");

  statDescriptorArr[3] = statFactory->createLongCounter(
      "activeTime",
      "The elapsed time in milliseconds that all of the threads of this "
      "process used the processor to execute instructions. An instruction is "
      "the basic unit of execution in a computer, a thread is the object that "
      "executes instructions, and a process is the object created when a "
      "program is run. Code executed to handle some hardware interrupts and "
      "trap conditions are included in this count.",
      "milliseconds", false);

  statDescriptorArr[4] = statFactory->createLongCounter(
      "pageFaults",
      "The total number of Page Faults by the threads executing in this "
      "process. A page fault occurs when a thread refers to a virtual memory "
      "page that is not in its working set in main memory. This will not cause "
      "the page to be fetched from disk if it is on the standby list and hence "
      "already in main memory, or if it is in use by another process with whom "
      "the page is shared.",
      "operations", false);
  statDescriptorArr[5] = statFactory->createLongGauge(
      "pageFileSize",
      "The current number of bytes this process has used in the paging "
      "file(s). Paging files are used to store pages of memory used by the "
      "process that are not contained in other files. Paging files are shared "
      "by all processes, and lack of space in paging files can prevent other "
      "processes from allocating memory.",
      "bytes");
  statDescriptorArr[6] = statFactory->createLongGauge(
      "pageFileSizePeak",
      "The maximum number of bytes this process has used in the paging "
      "file(s). Paging files are used to store pages of memory used by the "
      "process that are not contained in other files. Paging files are shared "
      "by all processes, and lack of space in paging files can prevent other "
      "processes from allocating memory.",
      "bytes");
  statDescriptorArr[7] = statFactory->createLongGauge(
      "privateSize",
      "The current number of bytes this process has allocated that cannot be "
      "shared with other processes.",
      "bytes");
  statDescriptorArr[8] = statFactory->createLongCounter(
      "systemTime",
      "The elapsed time in milliseconds that the threads of the process have "
      "spent executing code in privileged mode. When a Windows system service "
      "is called, the service will often run in Privileged Mode to gain access "
      "to system-private data. Such data is protected from access by threads "
      "executing in user mode. Calls to the system can be explicit or "
      "implicit, such as page faults or interrupts. Unlike some early "
      "operating systems, Windows uses process boundaries for subsystem "
      "protection in addition to the traditional protection of user and "
      "privileged modes. These subsystem processes provide additional "
      "protection. Therefore, some work done by Windows on behalf of your "
      "application might appear in other subsystem processes in addition to "
      "the privileged time in your process.",
      "milliseconds", false);
  statDescriptorArr[9] = statFactory->createLongCounter(
      "userTime",
      "The elapsed time in milliseconds that this process's threads have spent "
      "executing code in user mode. Applications, environment subsystems, and "
      "integral subsystems execute in user mode. Code executing in User Mode "
      "cannot damage the integrity of the Windows Executive, Kernel, and "
      "device drivers. Unlike some early operating systems, Windows uses "
      "process boundaries for subsystem protection in addition to the "
      "traditional protection of user and privileged modes. These subsystem "
      "processes provide additional protection. Therefore, some work done by "
      "Windows on behalf of your application might appear in other subsystem "
      "processes in addition to the privileged time in your process.",
      "milliseconds", false);
  statDescriptorArr[10] = statFactory->createLongGauge(
      "virtualSize",
      "Virtual Bytes is the current size in bytes of the virtual address space "
      "the process is using. Use of virtual address space does not necessarily "
      "imply corresponding use of either disk or main memory pages. Virtual "
      "space is finite, and by using too much, the process can limit its "
      "ability to load libraries.",
      "bytes");
  statDescriptorArr[11] = statFactory->createLongGauge(
      "virtualSizePeak",
      "The maximum number of bytes of virtual address space the process has "
      "used at any one time. Use of virtual address space does not necessarily "
      "imply corresponding use of either disk or main memory pages. Virtual "
      "space is however finite, and by using too much, the process might limit "
      "its ability to load libraries.",
      "bytes");
  statDescriptorArr[12] = statFactory->createLongGauge(
      "workingSetSize",
      "The current number of bytes in the Working Set of this process. The "
      "Working Set is the set of memory pages touched recently by the threads "
      "in the process. If free memory in the computer is above a threshold, "
      "pages are left in the Working Set of a process even if they are not in "
      "use. When free memory falls below a threshold, pages are trimmed from "
      "Working Sets. If they are needed they will then be soft-faulted back "
      "into the Working Set before they are paged out out to disk.",
      "bytes");
  statDescriptorArr[13] = statFactory->createLongGauge(
      "workingSetSizePeak",
      "The maximum number of bytes in the Working Set of this process at any "
      "point in time. The Working Set is the set of memory pages touched "
      "recently by the threads in the process. If free memory in the computer "
      "is above a threshold, pages are left in the Working Set of a process "
      "even if they are not in use. When free memory falls below a threshold, "
      "pages are trimmed from Working Sets. If they are needed they will then "
      "be soft-faulted back into the Working Set before they leave main "
      "memory.",
      "bytes");

  statDescriptorArr[14] = statFactory->createIntGauge(
      "cpuUsage", "Percentage cpu used by this process", "%");

  try {
    m_statsType = statFactory->createType(
        "WindowsProcessStats", "Statistics for a Microsoft Windows process.",
        statDescriptorArr, 15);
  } catch (Exception&) {
    m_statsType = statFactory->findType("WindowsProcessStats");
  }

  if (m_statsType == NULL) {
    throw OutOfMemoryException("WindowsProcessStats::createType: out memory");
  }

  handlesINT = m_statsType->nameToId("handles");
  priorityBaseINT = m_statsType->nameToId("priorityBase");
  threadsINT = m_statsType->nameToId("threads");
  activeTimeLONG = m_statsType->nameToId("activeTime");
  pageFaultsLONG = m_statsType->nameToId("pageFaults");
  pageFileSizeLONG = m_statsType->nameToId("pageFileSize");
  pageFileSizePeakLONG = m_statsType->nameToId("pageFileSizePeak");

  privateSizeLONG = m_statsType->nameToId("privateSize");
  systemTimeLONG = m_statsType->nameToId("systemTime");
  userTimeLONG = m_statsType->nameToId("userTime");
  virtualSizeLONG = m_statsType->nameToId("virtualSize");
  virtualSizePeakLONG = m_statsType->nameToId("virtualSizePeak");
  workingSetSizeLONG = m_statsType->nameToId("workingSetSize");
  workingSetSizePeakLONG = m_statsType->nameToId("workingSetSizePeak");
  cpuUsageINT = m_statsType->nameToId("cpuUsage");
}

int64 WindowsProcessStats::getProcessSize() {
  return stats->getLong(cpuUsageINT);
}

int32 WindowsProcessStats::getCpuUsage() {
  // throw UnsupportedOperationException( "getCpuUsage is unsupported on
  // Windows." );
  return stats->getInt(activeTimeLONG);
}

int64 WindowsProcessStats::getCPUTime() {
  return stats->getLong(activeTimeLONG);
}
int32 WindowsProcessStats::getNumThreads() { return stats->getInt(threadsINT); }
int64 WindowsProcessStats::getAllCpuTime() {
  return ((stats->getLong(userTimeLONG)) + (stats->getLong(systemTimeLONG)));
}

void WindowsProcessStats::close() {
  if (stats != NULL) {
    stats->close();
  }
}

WindowsProcessStats::~WindowsProcessStats() {
  m_statsType = NULL;
  stats = NULL;
}
