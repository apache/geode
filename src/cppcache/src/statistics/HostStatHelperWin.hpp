#ifndef _GEMFIRE_STATISTICS_HOSTSTATHELPERWIN_HPP_
#define _GEMFIRE_STATISTICS_HOSTSTATHELPERWIN_HPP_
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

#if defined(_WIN32)

#include <gfcpp/gfcpp_globals.hpp>
#include <string>
#include <Windows.h>
#include <WinPerf.h>

#include <stdio.h>
#include <stdlib.h>
#include <malloc.h>
#include <string.h>
#include "ProcessStats.hpp"

/** @file
*/

namespace gemfire_statistics {

/**
 * Windows2000 Implementation of code to fetch operating system stats.
 */

class HostStatHelperWin {
 private:
#define LEVEL1_QUERY_STRING "2 4 230 238 260"
  static PPERF_DATA_BLOCK PerfData;
  static PPERF_OBJECT_TYPE ProcessObj;
  static PPERF_OBJECT_TYPE ProcessorObj;
  static PPERF_OBJECT_TYPE MemoryObj;
  static PPERF_OBJECT_TYPE SystemObj;
  static PPERF_OBJECT_TYPE ObjectsObj;
  static DWORD BufferSize;
  static int32 pidCtrOffset;

  /* #define NTDBG 1 */

  enum {
    SYSTEM_OBJ_ID = 2,
    MEMORY_OBJ_ID = 4,
    PROCESS_OBJ_ID = 230,
    PROCESSOR_OBJ_ID = 238,
    OBJECTS_OBJ_ID = 260
  };

  enum {
    PID_ID = 784,
    PROCESSORTIME_ID = 6,
    USERTIME_ID = 142,
    PRIVILEGEDTIME_ID = 144,
    VIRTUALBYTESPEAK_ID = 172,
    VIRTUALBYTES_ID = 174,
    PAGEFAULTS_ID = 28,
    WORKINGSETPEAK_ID = 178,
    WORKINGSET_ID = 180,
    PAGEFILEBYTESPEAK_ID = 182,
    PAGEFILEBYTES_ID = 184,
    PRIVATEBYTES_ID = 186,
    THREADCOUNT_ID = 680,
    PRIORITYBASE_ID = 682,
    /* ELAPSEDTIME_ID = 684, */
    POOLPAGEDBYTES_ID = 56,
    POOLNONPAGEDBYTES_ID = 58,
    HANDLECOUNT_ID = 952
  };
  enum {
    PROCESSORTIME_IDX = 0,
    USERTIME_IDX,
    PRIVILEGEDTIME_IDX,
    VIRTUALBYTESPEAK_IDX,
    VIRTUALBYTES_IDX,
    PAGEFAULTS_IDX,
    WORKINGSETPEAK_IDX,
    WORKINGSET_IDX,
    PAGEFILEBYTESPEAK_IDX,
    PAGEFILEBYTES_IDX,
    PRIVATEBYTES_IDX,
    THREADCOUNT_IDX,
    PRIORITYBASE_IDX,
    POOLPAGEDBYTES_IDX,
    POOLNONPAGEDBYTES_IDX,
    HANDLECOUNT_IDX,
    MAX_PROCESS_CTRS_COLLECTED
  };
  static PERF_COUNTER_DEFINITION processCtrCache[MAX_PROCESS_CTRS_COLLECTED];

  enum {
    /* PROCESSORTIME_ID = 6, */
    /* USERTIME_ID = 142, */
    /* PRIVILEGEDTIME_ID = 144, */
    INTERRUPTS_ID = 148,
    INTERRUPTTIME_ID = 698
  };

  enum {
    TOTALPROCESSORTIME_IDX = 0,
    TOTALUSERTIME_IDX,
    TOTALPRIVILEGEDTIME_IDX,
    INTERRUPTS_IDX,
    INTERRUPTTIME_IDX,
    MAX_PROCESSOR_CTRS_COLLECTED
  };

  static PERF_COUNTER_DEFINITION
      processorCtrCache[MAX_PROCESSOR_CTRS_COLLECTED];

  enum {
    TOTALFILEREADOPS_ID = 10,
    TOTALFILEWRITEOPS_ID = 12,
    TOTALFILECONTROLOPS_ID = 14,
    TOTALFILEREADKBYTES_ID = 16,
    TOTALFILEWRITEKBYTES_ID = 18,
    TOTALFILECONTROLKBYTES_ID = 20,
    TOTALCONTEXTSWITCHES_ID = 146,
    TOTALSYSTEMCALLS_ID = 150,
    TOTALFILEDATAOPS_ID = 406,
    /* SYSTEMUPTIME_ID = 674, */
    PROCESSORQUEUELENGTH_ID = 44,
    ALIGNMENTFIXUPS_ID = 686,
    EXCEPTIONDISPATCHES_ID = 688,
    FLOATINGEMULATIONS_ID = 690,
    REGISTRYQUOTAINUSE_ID = 1350
  };
  enum {
    TOTALFILEREADOPS_IDX = 0,
    TOTALFILEWRITEOPS_IDX,
    TOTALFILECONTROLOPS_IDX,
    TOTALFILEREADKBYTES_IDX,
    TOTALFILEWRITEKBYTES_IDX,
    TOTALFILECONTROLKBYTES_IDX,
    TOTALCONTEXTSWITCHES_IDX,
    TOTALSYSTEMCALLS_IDX,
    TOTALFILEDATAOPS_IDX,
    PROCESSORQUEUELENGTH_IDX,
    ALIGNMENTFIXUPS_IDX,
    EXCEPTIONDISPATCHES_IDX,
    FLOATINGEMULATIONS_IDX,
    REGISTRYQUOTAINUSE_IDX,
    MAX_SYSTEM_CTRS_COLLECTED
  };
  static PERF_COUNTER_DEFINITION systemCtrCache[MAX_SYSTEM_CTRS_COLLECTED];

  enum {
    AVAILABLEBYTES_ID = 24,
    COMMITTEDBYTES_ID = 26,
    COMMITLIMIT_ID = 30,
    TOTALPAGEFAULTS_ID = 28,
    WRITECOPIES_ID = 32,

    TRANSITIONFAULTS_ID = 34,
    CACHEFAULTS_ID = 36,
    DEMANDZEROFAULTS_ID = 38,
    PAGES_ID = 40,
    PAGESINPUT_ID = 822,
    PAGEREADS_ID = 42,
    PAGESOUTPUT_ID = 48,
    PAGEWRITES_ID = 50,
    TOTALPOOLPAGEDBYTES_ID = 56,
    TOTALPOOLNONPAGEDBYTES_ID = 58,
    POOLPAGEDALLOCS_ID = 60,
    POOLNONPAGEDALLOCS_ID = 64,
    FREESYSTEMPAGETABLEENTRIES_ID = 678,
    CACHEBYTES_ID = 818,
    CACHEBYTESPEAK_ID = 820,
    POOLPAGEDRESIDENTBYTES_ID = 66,
    SYSTEMCODETOTALBYTES_ID = 68,
    SYSTEMCODERESIDENTBYTES_ID = 70,
    SYSTEMDRIVERTOTALBYTES_ID = 72,
    SYSTEMDRIVERRESIDENTBYTES_ID = 74,
    SYSTEMCACHERESIDENTBYTES_ID = 76,
    COMMITTEDBYTESINUSE_ID = 1406
  };
  enum {
    AVAILABLEBYTES_IDX = 0,
    COMMITTEDBYTES_IDX,
    COMMITLIMIT_IDX,
    TOTALPAGEFAULTS_IDX,
    WRITECOPIES_IDX,
    TRANSITIONFAULTS_IDX,
    CACHEFAULTS_IDX,
    DEMANDZEROFAULTS_IDX,
    PAGES_IDX,
    PAGESINPUT_IDX,
    PAGEREADS_IDX,
    PAGESOUTPUT_IDX,
    PAGEWRITES_IDX,
    TOTALPOOLPAGEDBYTES_IDX,
    TOTALPOOLNONPAGEDBYTES_IDX,
    POOLPAGEDALLOCS_IDX,
    POOLNONPAGEDALLOCS_IDX,
    FREESYSTEMPAGETABLEENTRIES_IDX,
    CACHEBYTES_IDX,
    CACHEBYTESPEAK_IDX,
    POOLPAGEDRESIDENTBYTES_IDX,
    SYSTEMCODETOTALBYTES_IDX,
    SYSTEMCODERESIDENTBYTES_IDX,
    SYSTEMDRIVERTOTALBYTES_IDX,
    SYSTEMDRIVERRESIDENTBYTES_IDX,
    SYSTEMCACHERESIDENTBYTES_IDX,
    COMMITTEDBYTESINUSE_IDX,
    MAX_MEMORY_CTRS_COLLECTED
  };
  static PERF_COUNTER_DEFINITION memoryCtrCache[MAX_MEMORY_CTRS_COLLECTED];

  enum {
    PROCESSES_ID = 248,
    THREADS_ID = 250,
    EVENTS_ID = 252,
    SEMAPHORES_ID = 254,
    MUTEXES_ID = 256,
    SECTIONS_ID = 258
  };
  enum {
    PROCESSES_IDX = 0,
    THREADS_IDX,
    EVENTS_IDX,
    SEMAPHORES_IDX,
    MUTEXES_IDX,
    SECTIONS_IDX,
    MAX_OBJECTS_CTRS_COLLECTED
  };
  static PERF_COUNTER_DEFINITION objectsCtrCache[MAX_OBJECTS_CTRS_COLLECTED];

  struct FetchDataSType {
    uint32 perfTimeMs;
    int64 usertime;
    int64 systime;
    int64 idletime;
    int64 inttime;
    uint32 interrupts;
  };

  static FetchDataSType lastFetchData;
  static FetchDataSType currentFetchData;

 private:
  static void HostStatsFetchData();

  static int32 getPid(int32 pidCtrOffset, PPERF_COUNTER_BLOCK PerfCntrBlk);

  static uint32 getInt32Value(PPERF_COUNTER_DEFINITION PerfCntr,
                              PPERF_COUNTER_BLOCK PerfCntrBlk);

  static int64 getInt64Value(PPERF_COUNTER_DEFINITION PerfCntr,
                             PPERF_COUNTER_BLOCK PerfCntrBlk,
                             bool convertMS = true);

  static PPERF_OBJECT_TYPE FirstObject(PPERF_DATA_BLOCK PerfData);

  static PPERF_OBJECT_TYPE NextObject(PPERF_OBJECT_TYPE PerfObj);

  static PPERF_INSTANCE_DEFINITION FirstInstance(PPERF_OBJECT_TYPE PerfObj);

  static PPERF_INSTANCE_DEFINITION NextInstance(
      PPERF_COUNTER_BLOCK PerfCntrBlk);

  static PPERF_COUNTER_DEFINITION FirstCounter(PPERF_OBJECT_TYPE PerfObj);

  static PPERF_COUNTER_DEFINITION NextCounter(
      PPERF_COUNTER_DEFINITION PerfCntr);

  static char* getInstIdStr(PPERF_INSTANCE_DEFINITION PerfInst, char* prefix);

  static int calculateCpuUsage(PPERF_COUNTER_BLOCK& ctrBlk);

 public:
  static void initHostStatHelperWin();

  static void refreshProcess(ProcessStats* processStats);

  static void closeHostStatHelperWin();

  // static refreeshSystem(Statistics* stats);

};  // class

};  // namespace

#endif  // (_WIN32)

#endif  // _GEMFIRE_STATISTICS_HOSTSTATHELPERWIN_HPP_
