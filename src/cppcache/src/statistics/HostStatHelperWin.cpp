/*=========================================================================
 * Copyright (c) 2004-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include <gfcpp/gfcpp_globals.hpp>
#include <ace/OS.h>

#include "HostStatHelperWin.hpp"

#include "ProcessStats.hpp"
#include "WindowsProcessStats.hpp"

/**
 * Windows2000 Implementation of code to fetch operating system stats.
 *
 */

#if defined(_WIN32)
using namespace gemfire_statistics;

PPERF_DATA_BLOCK HostStatHelperWin::PerfData = NULL;
PPERF_OBJECT_TYPE HostStatHelperWin::ProcessObj = NULL;
PPERF_OBJECT_TYPE HostStatHelperWin::ProcessorObj = NULL;
PPERF_OBJECT_TYPE HostStatHelperWin::MemoryObj = NULL;
PPERF_OBJECT_TYPE HostStatHelperWin::SystemObj = NULL;
PPERF_OBJECT_TYPE HostStatHelperWin::ObjectsObj = NULL;
DWORD HostStatHelperWin::BufferSize = 65536;
int32 HostStatHelperWin::pidCtrOffset = -1;

PERF_COUNTER_DEFINITION
HostStatHelperWin::processCtrCache[MAX_PROCESS_CTRS_COLLECTED];

PERF_COUNTER_DEFINITION
HostStatHelperWin::processorCtrCache[MAX_PROCESSOR_CTRS_COLLECTED];

PERF_COUNTER_DEFINITION
HostStatHelperWin::systemCtrCache[MAX_SYSTEM_CTRS_COLLECTED];

PERF_COUNTER_DEFINITION
HostStatHelperWin::memoryCtrCache[MAX_MEMORY_CTRS_COLLECTED];

PERF_COUNTER_DEFINITION
HostStatHelperWin::objectsCtrCache[MAX_OBJECTS_CTRS_COLLECTED];

struct HostStatHelperWin::FetchDataSType HostStatHelperWin::lastFetchData;
struct HostStatHelperWin::FetchDataSType HostStatHelperWin::currentFetchData;

PPERF_OBJECT_TYPE HostStatHelperWin::FirstObject(PPERF_DATA_BLOCK PerfData) {
  return ((PPERF_OBJECT_TYPE)((PBYTE)PerfData + PerfData->HeaderLength));
}

PPERF_OBJECT_TYPE HostStatHelperWin::NextObject(PPERF_OBJECT_TYPE PerfObj) {
  return ((PPERF_OBJECT_TYPE)((PBYTE)PerfObj + PerfObj->TotalByteLength));
}

PPERF_INSTANCE_DEFINITION HostStatHelperWin::FirstInstance(
    PPERF_OBJECT_TYPE PerfObj) {
  return (
      (PPERF_INSTANCE_DEFINITION)((PBYTE)PerfObj + PerfObj->DefinitionLength));
}

PPERF_INSTANCE_DEFINITION HostStatHelperWin::NextInstance(
    PPERF_COUNTER_BLOCK PerfCntrBlk) {
  return ((PPERF_INSTANCE_DEFINITION)((PBYTE)PerfCntrBlk +
                                      PerfCntrBlk->ByteLength));
}

char* HostStatHelperWin::getInstIdStr(PPERF_INSTANCE_DEFINITION PerfInst,
                                      char* prefix) {
  static char resbuff[132];
  if (PerfInst->UniqueID == PERF_NO_UNIQUE_ID) {
    short* unicodePtr = (short*)((PBYTE)PerfInst + PerfInst->NameOffset);
    ACE_OS::snprintf(resbuff, 132, "%S length=%d unicode[0]=%d",
                     (char*)((PBYTE)PerfInst + PerfInst->NameOffset),
                     PerfInst->NameLength, unicodePtr[0]);
  } else {
    ACE_OS::snprintf(resbuff, 132, "%s%d", prefix, PerfInst->UniqueID);
  }
  return resbuff;
}

PPERF_COUNTER_DEFINITION HostStatHelperWin::FirstCounter(
    PPERF_OBJECT_TYPE PerfObj) {
  return ((PPERF_COUNTER_DEFINITION)((PBYTE)PerfObj + PerfObj->HeaderLength));
}

PPERF_COUNTER_DEFINITION HostStatHelperWin::NextCounter(
    PPERF_COUNTER_DEFINITION PerfCntr) {
  return ((PPERF_COUNTER_DEFINITION)((PBYTE)PerfCntr + PerfCntr->ByteLength));
}

void HostStatHelperWin::HostStatsFetchData() {
  DWORD o;
  PPERF_OBJECT_TYPE objPtr = NULL;
  DWORD res;
  PPERF_COUNTER_DEFINITION PerfCntr;
  DWORD oldBufferSize = BufferSize;
  const char* qstr;
  qstr = LEVEL1_QUERY_STRING;

  while ((res = RegQueryValueEx(HKEY_PERFORMANCE_DATA, qstr, NULL, NULL,
                                (LPBYTE)PerfData, &BufferSize)) ==
         ERROR_MORE_DATA) {
    oldBufferSize += 4096;
    BufferSize = oldBufferSize;
    PerfData = (PPERF_DATA_BLOCK)realloc(PerfData, BufferSize);
  }

#ifdef NTDBG
  LOGDEBUG("HostStatHeleperWin: buffersize is %ld\n", BufferSize);
#endif
  ProcessObj = NULL;
  ProcessorObj = NULL;
  MemoryObj = NULL;
  SystemObj = NULL;
  ObjectsObj = NULL;

  if (res != ERROR_SUCCESS) {
    LOGDEBUG(
        "HostStatHeleperWin: Can't get Windows performance data. "
        "RegQueryValueEx returned %ld\n",
        res);
    return;
  }

  objPtr = FirstObject(PerfData);
  for (o = 0; o < PerfData->NumObjectTypes; o++) {
#ifdef NTDBG
    LOGDEBUG("HostStatHeleperWin: Object %ld\n", objPtr->ObjectNameTitleIndex);
#endif
    switch (objPtr->ObjectNameTitleIndex) {
      case PROCESS_OBJ_ID:
        ProcessObj = objPtr;
        break;
      case PROCESSOR_OBJ_ID:
        ProcessorObj = objPtr;
        break;
      case MEMORY_OBJ_ID:
        MemoryObj = objPtr;
        break;
      case SYSTEM_OBJ_ID:
        SystemObj = objPtr;
        break;
      case OBJECTS_OBJ_ID:
        ObjectsObj = objPtr;
        break;
    }
    objPtr = NextObject(objPtr);
  }
  if (pidCtrOffset == -1) {
    if (ProcessObj) {
      DWORD c;
      PerfCntr = FirstCounter(ProcessObj);
      for (c = 0; c < ProcessObj->NumCounters; c++) {
        switch (PerfCntr->CounterNameTitleIndex) {
          case PID_ID:
            pidCtrOffset = PerfCntr->CounterOffset;
            break;
          case PROCESSORTIME_ID:
            processCtrCache[PROCESSORTIME_IDX] = *PerfCntr;
            break;
          case USERTIME_ID:
            processCtrCache[USERTIME_IDX] = *PerfCntr;
            break;
          case PRIVILEGEDTIME_ID:
            processCtrCache[PRIVILEGEDTIME_IDX] = *PerfCntr;
            break;
          case VIRTUALBYTESPEAK_ID:
            processCtrCache[VIRTUALBYTESPEAK_IDX] = *PerfCntr;
            break;
          case VIRTUALBYTES_ID:
            processCtrCache[VIRTUALBYTES_IDX] = *PerfCntr;
            break;
          case PAGEFAULTS_ID:
            processCtrCache[PAGEFAULTS_IDX] = *PerfCntr;
            break;
          case WORKINGSETPEAK_ID:
            processCtrCache[WORKINGSETPEAK_IDX] = *PerfCntr;
            break;
          case WORKINGSET_ID:
            processCtrCache[WORKINGSET_IDX] = *PerfCntr;
            break;
          case PAGEFILEBYTESPEAK_ID:
            processCtrCache[PAGEFILEBYTESPEAK_IDX] = *PerfCntr;
            break;
          case PAGEFILEBYTES_ID:
            processCtrCache[PAGEFILEBYTES_IDX] = *PerfCntr;
            break;
          case PRIVATEBYTES_ID:
            processCtrCache[PRIVATEBYTES_IDX] = *PerfCntr;
            break;
          case THREADCOUNT_ID:
            processCtrCache[THREADCOUNT_IDX] = *PerfCntr;
            break;
          case PRIORITYBASE_ID:
            processCtrCache[PRIORITYBASE_IDX] = *PerfCntr;
            break;
          case POOLPAGEDBYTES_ID:
            processCtrCache[POOLPAGEDBYTES_IDX] = *PerfCntr;
            break;
          case POOLNONPAGEDBYTES_ID:
            processCtrCache[POOLNONPAGEDBYTES_IDX] = *PerfCntr;
            break;
          case HANDLECOUNT_ID:
            processCtrCache[HANDLECOUNT_IDX] = *PerfCntr;
            break;
          default:
            /* unknown counter. just skip it. */
            break;
        }
        PerfCntr = NextCounter(PerfCntr);
      }
#if GF_DEBUG_ASSERTS == 1
      for (c = 0; c < MAX_PROCESS_CTRS_COLLECTED; c++) {
        if (processCtrCache[c].CounterNameTitleIndex == 0) {
          LOGDEBUG("HostStatHeleperWin: bad processCtr at idx=%d\n", c);
        }
      }
#endif
    }
    if (ProcessorObj) {
      DWORD c;
      PerfCntr = FirstCounter(ProcessorObj);
#if 0
      LOGDEBUG("HostStatHeleperWin: ProcessorObj->NumCounters=%d\n", ProcessorObj->NumCounters);
#endif
      for (c = 0; c < ProcessorObj->NumCounters; c++) {
#if 0
        LOGDEBUG("HostStatHeleperWin: PerfCntr->CounterNameTitleIndex=%d\n", PerfCntr->CounterNameTitleIndex);
#endif
        switch (PerfCntr->CounterNameTitleIndex) {
          case PROCESSORTIME_ID:
            processorCtrCache[TOTALPROCESSORTIME_IDX] = *PerfCntr;
            break;
          case USERTIME_ID:
            processorCtrCache[TOTALUSERTIME_IDX] = *PerfCntr;
            break;
          case PRIVILEGEDTIME_ID:
            processorCtrCache[TOTALPRIVILEGEDTIME_IDX] = *PerfCntr;
            break;
          case INTERRUPTTIME_ID:
            processorCtrCache[INTERRUPTTIME_IDX] = *PerfCntr;
            break;
          case INTERRUPTS_ID:
            processorCtrCache[INTERRUPTS_IDX] = *PerfCntr;
            break;
          default:
            /* unknown counter. just skip it. */
            break;
        }
        PerfCntr = NextCounter(PerfCntr);
      }
#ifdef FLG_DEBUG
      for (c = 0; c < MAX_PROCESSOR_CTRS_COLLECTED; c++) {
        if (processorCtrCache[c].CounterNameTitleIndex == 0) {
          LOGDEBUG("HostStatHeleperWin: bad processorCtr at idx=%d\n",
                   c);  // fflush(stderr);
        }
      }
#endif
    }

    if (SystemObj) {
      DWORD c;
      PerfCntr = FirstCounter(SystemObj);
      for (c = 0; c < SystemObj->NumCounters; c++) {
        switch (PerfCntr->CounterNameTitleIndex) {
          case TOTALFILEREADOPS_ID:
            systemCtrCache[TOTALFILEREADOPS_IDX] = *PerfCntr;
            break;
          case TOTALFILEWRITEOPS_ID:
            systemCtrCache[TOTALFILEWRITEOPS_IDX] = *PerfCntr;
            break;
          case TOTALFILECONTROLOPS_ID:
            systemCtrCache[TOTALFILECONTROLOPS_IDX] = *PerfCntr;
            break;
          case TOTALFILEREADKBYTES_ID:
            systemCtrCache[TOTALFILEREADKBYTES_IDX] = *PerfCntr;
            break;
          case TOTALFILEWRITEKBYTES_ID:
            systemCtrCache[TOTALFILEWRITEKBYTES_IDX] = *PerfCntr;
            break;
          case TOTALFILECONTROLKBYTES_ID:
            systemCtrCache[TOTALFILECONTROLKBYTES_IDX] = *PerfCntr;
            break;
          case TOTALCONTEXTSWITCHES_ID:
            systemCtrCache[TOTALCONTEXTSWITCHES_IDX] = *PerfCntr;
            break;
          case TOTALSYSTEMCALLS_ID:
            systemCtrCache[TOTALSYSTEMCALLS_IDX] = *PerfCntr;
            break;
          case TOTALFILEDATAOPS_ID:
            systemCtrCache[TOTALFILEDATAOPS_IDX] = *PerfCntr;
            break;
          case PROCESSORQUEUELENGTH_ID:
            systemCtrCache[PROCESSORQUEUELENGTH_IDX] = *PerfCntr;
            break;
          case ALIGNMENTFIXUPS_ID:
            systemCtrCache[ALIGNMENTFIXUPS_IDX] = *PerfCntr;
            break;
          case EXCEPTIONDISPATCHES_ID:
            systemCtrCache[EXCEPTIONDISPATCHES_IDX] = *PerfCntr;
            break;
          case FLOATINGEMULATIONS_ID:
            systemCtrCache[FLOATINGEMULATIONS_IDX] = *PerfCntr;
            break;
          case REGISTRYQUOTAINUSE_ID:
            systemCtrCache[REGISTRYQUOTAINUSE_IDX] = *PerfCntr;
            break;
          default:
            /* unknown counter. just skip it. */
            break;
        }
        PerfCntr = NextCounter(PerfCntr);
      }
#ifdef FLG_DEBUG
      for (c = 0; c < MAX_SYSTEM_CTRS_COLLECTED; c++) {
        if (systemCtrCache[c].CounterNameTitleIndex == 0) {
          LOGDEBUG("HostStatHeleperWin: bad systemCtr at idx=%d\n", c);
        }
      }
#endif
    }

    if (MemoryObj) {
      DWORD c;
      PerfCntr = FirstCounter(MemoryObj);
      for (c = 0; c < MemoryObj->NumCounters; c++) {
        switch (PerfCntr->CounterNameTitleIndex) {
          case AVAILABLEBYTES_ID:
            memoryCtrCache[AVAILABLEBYTES_IDX] = *PerfCntr;
            break;
          case COMMITTEDBYTES_ID:
            memoryCtrCache[COMMITTEDBYTES_IDX] = *PerfCntr;
            break;
          case COMMITLIMIT_ID:
            memoryCtrCache[COMMITLIMIT_IDX] = *PerfCntr;
            break;
          case TOTALPAGEFAULTS_ID:
            memoryCtrCache[TOTALPAGEFAULTS_IDX] = *PerfCntr;
            break;
          case WRITECOPIES_ID:
            memoryCtrCache[WRITECOPIES_IDX] = *PerfCntr;
            break;
          case TRANSITIONFAULTS_ID:
            memoryCtrCache[TRANSITIONFAULTS_IDX] = *PerfCntr;
            break;
          case CACHEFAULTS_ID:
            memoryCtrCache[CACHEFAULTS_IDX] = *PerfCntr;
            break;
          case DEMANDZEROFAULTS_ID:
            memoryCtrCache[DEMANDZEROFAULTS_IDX] = *PerfCntr;
            break;
          case PAGES_ID:
            memoryCtrCache[PAGES_IDX] = *PerfCntr;
            break;
          case PAGESINPUT_ID:
            memoryCtrCache[PAGESINPUT_IDX] = *PerfCntr;
            break;
          case PAGEREADS_ID:
            memoryCtrCache[PAGEREADS_IDX] = *PerfCntr;
            break;
          case PAGESOUTPUT_ID:
            memoryCtrCache[PAGESOUTPUT_IDX] = *PerfCntr;
            break;
          case PAGEWRITES_ID:
            memoryCtrCache[PAGEWRITES_IDX] = *PerfCntr;
            break;
          case TOTALPOOLPAGEDBYTES_ID:
            memoryCtrCache[TOTALPOOLPAGEDBYTES_IDX] = *PerfCntr;
            break;
          case TOTALPOOLNONPAGEDBYTES_ID:
            memoryCtrCache[TOTALPOOLNONPAGEDBYTES_IDX] = *PerfCntr;
            break;
          case POOLPAGEDALLOCS_ID:
            memoryCtrCache[POOLPAGEDALLOCS_IDX] = *PerfCntr;
            break;
          case POOLNONPAGEDALLOCS_ID:
            memoryCtrCache[POOLNONPAGEDALLOCS_IDX] = *PerfCntr;
            break;
          case FREESYSTEMPAGETABLEENTRIES_ID:
            memoryCtrCache[FREESYSTEMPAGETABLEENTRIES_IDX] = *PerfCntr;
            break;
          case CACHEBYTES_ID:
            memoryCtrCache[CACHEBYTES_IDX] = *PerfCntr;
            break;
          case CACHEBYTESPEAK_ID:
            memoryCtrCache[CACHEBYTESPEAK_IDX] = *PerfCntr;
            break;
          case POOLPAGEDRESIDENTBYTES_ID:
            memoryCtrCache[POOLPAGEDRESIDENTBYTES_IDX] = *PerfCntr;
            break;
          case SYSTEMCODETOTALBYTES_ID:
            memoryCtrCache[SYSTEMCODETOTALBYTES_IDX] = *PerfCntr;
            break;
          case SYSTEMCODERESIDENTBYTES_ID:
            memoryCtrCache[SYSTEMCODERESIDENTBYTES_IDX] = *PerfCntr;
            break;
          case SYSTEMDRIVERTOTALBYTES_ID:
            memoryCtrCache[SYSTEMDRIVERTOTALBYTES_IDX] = *PerfCntr;
            break;
          case SYSTEMDRIVERRESIDENTBYTES_ID:
            memoryCtrCache[SYSTEMDRIVERRESIDENTBYTES_IDX] = *PerfCntr;
            break;
          case SYSTEMCACHERESIDENTBYTES_ID:
            memoryCtrCache[SYSTEMCACHERESIDENTBYTES_IDX] = *PerfCntr;
            break;
          case COMMITTEDBYTESINUSE_ID:
            memoryCtrCache[COMMITTEDBYTESINUSE_IDX] = *PerfCntr;
            break;
          default:
            /* unknown counter. just skip it. */
            break;
        }
        PerfCntr = NextCounter(PerfCntr);
      }
#ifdef FLG_DEBUG
      for (c = 0; c < MAX_MEMORY_CTRS_COLLECTED; c++) {
        if (memoryCtrCache[c].CounterNameTitleIndex == 0) {
          LOGDEBUG("HostStatHeleperWin: bad memoryCtr at idx=%d\n", c);
        }
      }
#endif
    }
    if (ObjectsObj) {
      DWORD c;
      PerfCntr = FirstCounter(ObjectsObj);
      for (c = 0; c < ObjectsObj->NumCounters; c++) {
        switch (PerfCntr->CounterNameTitleIndex) {
          case PROCESSES_ID:
            objectsCtrCache[PROCESSES_IDX] = *PerfCntr;
            break;
          case THREADS_ID:
            objectsCtrCache[THREADS_IDX] = *PerfCntr;
            break;
          case EVENTS_ID:
            objectsCtrCache[EVENTS_IDX] = *PerfCntr;
            break;
          case SEMAPHORES_ID:
            objectsCtrCache[SEMAPHORES_IDX] = *PerfCntr;
            break;
          case MUTEXES_ID:
            objectsCtrCache[MUTEXES_IDX] = *PerfCntr;
            break;
          case SECTIONS_ID:
            objectsCtrCache[SECTIONS_IDX] = *PerfCntr;
            break;
          default:
            /* unknown counter. just skip it. */
            break;
        }
        PerfCntr = NextCounter(PerfCntr);
      }
#ifdef FLG_DEBUG
      for (c = 0; c < MAX_OBJECTS_CTRS_COLLECTED; c++) {
        if (objectsCtrCache[c].CounterNameTitleIndex == 0) {
          LOGDEBUG("HostStatHeleperWin: bad objectsCtr at idx=%d\n", c);
        }
      }
#endif
    }
  }
  lastFetchData = currentFetchData;
  currentFetchData.perfTimeMs =
      (uint32)(((int64_t)PerfData->PerfTime100nSec.QuadPart) / 10000);
  if (ProcessorObj) {
    int32 i;
    PPERF_INSTANCE_DEFINITION PerfInst;

    if (ProcessorObj->NumInstances > 0) {
      bool foundIt = FALSE;
      PerfInst = FirstInstance(ProcessorObj);
      for (i = 0; i < (int32)ProcessorObj->NumInstances; i++) {
        PPERF_COUNTER_BLOCK PerfCntrBlk =
            (PPERF_COUNTER_BLOCK)((PBYTE)PerfInst + PerfInst->ByteLength);
        short* unicodePtr = (short*)((PBYTE)PerfInst + PerfInst->NameOffset);
        if (PerfInst->UniqueID == PERF_NO_UNIQUE_ID && unicodePtr[0] == '_') {
          /* starts with "_" must be "_Total" */

          currentFetchData.usertime =
              getInt64Value(&processorCtrCache[TOTALUSERTIME_IDX], PerfCntrBlk);
          currentFetchData.systime = getInt64Value(
              &processorCtrCache[TOTALPRIVILEGEDTIME_IDX], PerfCntrBlk);
          currentFetchData.idletime = getInt64Value(
              &processorCtrCache[TOTALPROCESSORTIME_IDX], PerfCntrBlk);
          currentFetchData.inttime =
              getInt64Value(&processorCtrCache[INTERRUPTTIME_IDX], PerfCntrBlk);
          currentFetchData.interrupts =
              getInt32Value(&processorCtrCache[INTERRUPTS_IDX], PerfCntrBlk);
          foundIt = TRUE;
          break;
        }
        PerfInst = NextInstance(PerfCntrBlk);
      }
#ifdef FLG_DEBUG
      if (!foundIt) {
        LOGDEBUG("HostStatHeleperWin: did not find processor named _Total!\n");
      }
#endif
    } else {
#ifdef FLG_DEBUG
      LOGDEBUG("HostStatHeleperWin: unexpected 0 instances!\n");
#endif
    }
  }
}

int32 HostStatHelperWin::getPid(int32 pidCtrOffset,
                                PPERF_COUNTER_BLOCK PerfCntrBlk) {
  int32* result = (int32*)((char*)PerfCntrBlk + pidCtrOffset);
  return *result;
}

uint32 HostStatHelperWin::getInt32Value(PPERF_COUNTER_DEFINITION PerfCntr,
                                        PPERF_COUNTER_BLOCK PerfCntrBlk) {
  if (PerfCntr->CounterOffset == 0) {
#ifdef FLG_DEBUG
    LOGDEBUG("HostStatHeleperWin: missing counter id=%d\n",
             PerfCntr->CounterNameTitleIndex);
#endif
    return 0;
  }

  if (PerfCntr->CounterSize == 4) {
    uint32* lptr;
    lptr = (uint32*)((char*)PerfCntrBlk + PerfCntr->CounterOffset);
    if (PerfCntr->CounterType == PERF_RAW_FRACTION) {
      double fraction = (double)*lptr++;
      double base = (double)*lptr;
      if (base != 0) {
        return (uint32)((fraction / base) * 100.0);
      } else {
        return 0;
      }
    } else if (PerfCntr->CounterType == PERF_RAW_BASE) {
      double base = (double)*lptr--;
      double fraction = (double)*lptr;
      if (base != 0) {
        return (uint32)((fraction / base) * 100.0);
      } else {
        return 0;
      }
    } else {
      return *lptr;
    }
  } else {
#ifdef FLG_DEBUG
    LOGDEBUG("HostStatHeleperWin: unexpected CounterSize of %ld\n",
             PerfCntr->CounterSize);  // fflush(stderr);
#endif
    return 0;
  }
}

int64 HostStatHelperWin::getInt64Value(PPERF_COUNTER_DEFINITION PerfCntr,
                                       PPERF_COUNTER_BLOCK PerfCntrBlk,
                                       bool convertMS) {
  if (PerfCntr->CounterOffset == 0) {
#ifdef FLG_DEBUG
    LOGDEBUG("HostStatHeleperWin: missing counter id=%d\n",
             PerfCntr->CounterNameTitleIndex);
#endif
    return 0;
  }
  if (PerfCntr->CounterSize == 8) {
    int64_t* i64ptr;
    int64_t iValue;
    int64 result;

    i64ptr = (int64_t*)((char*)PerfCntrBlk + PerfCntr->CounterOffset);
    iValue = *i64ptr;

/* #define I64_TRACE */
#ifdef I64_TRACE
    LOGDEBUG("HostStatHeleperWin: %x %I64x", PerfCntr->CounterType, iValue);
#endif
    if (iValue != 0) {
      switch (PerfCntr->CounterType) {
        case PERF_COUNTER_LARGE_RAWCOUNT:
        case PERF_COUNTER_BULK_COUNT:
          break;
#if 0
      case PERF_COUNTER_TIMER:
  /* convert to milliseconds */
  /* PerfFreq is number of times per seconds so adjust by 1000 for ms */
  iValue /=  (((int64_t)PerfData->PerfFreq.QuadPart) / 1000);
  break;
#endif
        case PERF_100NSEC_TIMER_INV:
        case PERF_100NSEC_TIMER:
          if (convertMS) {
            /* convert to milliseconds */
            iValue /= 10000;
          }
          break;
        default:
#ifdef FLG_DEBUG
          LOGDEBUG("HostStatHeleperWin: unexpected CounterType %lx\n",
                   PerfCntr->CounterType);
#endif
          break;
      }
    }
    result = iValue;
    return result;
  } else {
#ifdef FLG_DEBUG
    LOGDEBUG(
        "HostStatHeleperWin: unexpected CounterSize of %ld NameOffset=%d\n",
        PerfCntr->CounterSize,
        PerfCntr->CounterNameTitleIndex);  // fflush(stderr);
#endif
    return 0;
  }
}

void HostStatHelperWin::initHostStatHelperWin() {
  /* Initialize registry data structures */
  int32 counter;
  if (!PerfData) {
    PerfData = (PPERF_DATA_BLOCK)malloc(BufferSize);
  }
  for (counter = 0; counter < MAX_PROCESS_CTRS_COLLECTED; counter++) {
    processCtrCache[counter].CounterOffset = 0;
  }
  for (counter = 0; counter < MAX_SYSTEM_CTRS_COLLECTED; counter++) {
    systemCtrCache[counter].CounterOffset = 0;
  }
  for (counter = 0; counter < MAX_MEMORY_CTRS_COLLECTED; counter++) {
    memoryCtrCache[counter].CounterOffset = 0;
  }
  for (counter = 0; counter < MAX_OBJECTS_CTRS_COLLECTED; counter++) {
    objectsCtrCache[counter].CounterOffset = 0;
  }
  lastFetchData.perfTimeMs = 0;
  currentFetchData.perfTimeMs = 0;
  pidCtrOffset = -1;
  HostStatsFetchData();
}

void HostStatHelperWin::refreshProcess(ProcessStats* processStats) {
  // Get pid, WindowsProcessStats
  WindowsProcessStats* winProcessStat =
      dynamic_cast<WindowsProcessStats*>(processStats);
  if (winProcessStat == NULL) {
    LOGFINE("HostStatHelperWin::refreshProcess failed due to null processStat");
    return;
  }
  Statistics* stats = winProcessStat->stats;
  int32 pid = (int32)stats->getNumericId();  // int64 is converted to int

  int32 i;

  // Fetch new data
  HostStatsFetchData();

  if (ProcessObj) {
    PPERF_INSTANCE_DEFINITION PerfInst;

    if (ProcessObj->NumInstances > 0) {
#if 0
      static int32 done = 0;
      if (!done) {
  done = 1;
  PerfInst = FirstInstance(ProcessObj);
  for (i=0; i < (int32)ProcessObj->NumInstances; i++) {
    PPERF_COUNTER_BLOCK PerfCntrBlk;
    PerfCntrBlk = (PPERF_COUNTER_BLOCK)((PBYTE)PerfInst +
                PerfInst->ByteLength);
    LOGDEBUG("HostStatHeleperWin: process: %s\n", getInstIdStr(PerfInst, "proc-"));
    PerfInst = NextInstance(PerfCntrBlk);
  }
      }
#endif
      PerfInst = FirstInstance(ProcessObj);
      for (i = 0; i < (int32)ProcessObj->NumInstances; i++) {
        PPERF_COUNTER_BLOCK PerfCntrBlk;
        PerfCntrBlk =
            (PPERF_COUNTER_BLOCK)((PBYTE)PerfInst + PerfInst->ByteLength);

        if (pid == getPid(pidCtrOffset, PerfCntrBlk)) {
          stats->setInt(
              winProcessStat->handlesINT,
              getInt32Value(&processCtrCache[HANDLECOUNT_IDX], PerfCntrBlk));
          stats->setInt(
              winProcessStat->priorityBaseINT,
              getInt32Value(&processCtrCache[PRIORITYBASE_IDX], PerfCntrBlk));
          stats->setInt(
              winProcessStat->threadsINT,
              getInt32Value(&processCtrCache[THREADCOUNT_IDX], PerfCntrBlk));
          stats->setLong(winProcessStat->activeTimeLONG,
                         getInt64Value(&processCtrCache[PROCESSORTIME_IDX],
                                       PerfCntrBlk, false));
          stats->setLong(
              winProcessStat->pageFaultsLONG,
              getInt32Value(&processCtrCache[PAGEFAULTS_IDX], PerfCntrBlk));
          stats->setLong(
              winProcessStat->pageFileSizeLONG,
              getInt64Value(&processCtrCache[PAGEFILEBYTES_IDX], PerfCntrBlk));
          stats->setLong(winProcessStat->pageFileSizePeakLONG,
                         getInt64Value(&processCtrCache[PAGEFILEBYTESPEAK_IDX],
                                       PerfCntrBlk));
          stats->setLong(
              winProcessStat->privateSizeLONG,
              getInt64Value(&processCtrCache[PRIVATEBYTES_IDX], PerfCntrBlk));
          stats->setLong(
              winProcessStat->systemTimeLONG,
              getInt64Value(&processCtrCache[PRIVILEGEDTIME_IDX], PerfCntrBlk));
          stats->setLong(
              winProcessStat->userTimeLONG,
              getInt64Value(&processCtrCache[USERTIME_IDX], PerfCntrBlk));
          stats->setLong(
              winProcessStat->virtualSizeLONG,
              getInt64Value(&processCtrCache[VIRTUALBYTES_IDX], PerfCntrBlk));
          stats->setLong(winProcessStat->virtualSizePeakLONG,
                         getInt64Value(&processCtrCache[VIRTUALBYTESPEAK_IDX],
                                       PerfCntrBlk));
          stats->setLong(
              winProcessStat->workingSetSizeLONG,
              getInt64Value(&processCtrCache[WORKINGSET_IDX], PerfCntrBlk));
          stats->setLong(
              winProcessStat->workingSetSizePeakLONG,
              getInt64Value(&processCtrCache[WORKINGSETPEAK_IDX], PerfCntrBlk));

          int cpuUsage = calculateCpuUsage(PerfCntrBlk);
          stats->setInt(winProcessStat->cpuUsageINT, cpuUsage);
          return;
        }  // if
        PerfInst = NextInstance(PerfCntrBlk);
      }  // for
    } else {
#ifdef FLG_DEBUG
      LOGDEBUG("HostStatHeleperWin: unexpected 0 instances!\n");
#endif
    }  // else
  }

}  // refreshProcess

static bool firstTime = true;
// Formula for CPUUsage  =  ( X1 - X0 ) / ( Y1- Y0 )
//  X = Value of PROCESSORTIME_ID Counter ( in 100 nsec unit )
//  Y = Value of Sampling time ( in 100n sec unit )
int HostStatHelperWin::calculateCpuUsage(PPERF_COUNTER_BLOCK& ctrBlk) {
  static int64 oldPerfTime100nSec = 0;
  static int64 oldProcessorTime = 0;
  int32 cpuUsage = 0;

  int64 newProcessorTime =
      getInt64Value(&processCtrCache[PROCESSORTIME_IDX], ctrBlk, false);
  int64 newPerfTime100nSec = PerfData->PerfTime100nSec.QuadPart;

  if (firstTime) {
    firstTime = false;
    oldProcessorTime = newProcessorTime;
    oldPerfTime100nSec = newPerfTime100nSec;
  } else {
    int64 pTimeDelta = newProcessorTime - oldProcessorTime;
    int64 delta100Sec = (newPerfTime100nSec - oldPerfTime100nSec);

    oldProcessorTime = newProcessorTime;
    oldPerfTime100nSec = newPerfTime100nSec;

    double a = (double)pTimeDelta / (double)delta100Sec;
    cpuUsage = (int)(a * 100);
    cpuUsage = cpuUsage > 0 ? cpuUsage : 0;
  }
  return cpuUsage;
}

void HostStatHelperWin::closeHostStatHelperWin() {
  if (PerfData) {
    free((char*)PerfData);
    PerfData = NULL;
  }
  RegCloseKey(HKEY_PERFORMANCE_DATA);
  firstTime = true;
}

#endif
