#pragma once

#ifndef GEODE_STATISTICS_HOSTSTATHELPERSOLARIS_H_
#define GEODE_STATISTICS_HOSTSTATHELPERSOLARIS_H_

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

#if defined(_SOLARIS)
#include <gfcpp/gfcpp_globals.hpp>
#include <string>
#include <sys/sysinfo.h>
#include "ProcessStats.hpp"
#include <kstat.h>

/*
  * CPU_USAGE_STAT_THRESHOLD sets how much time must pass between samples
  * before a new cpu utilization is calculated.
  * Units are in 100ths of a second.
  * if set too low you will get divide by zero overflows or scewed data
  * due to rounding errors.
  * This is likely unnecesary with our stat sampling interval being 1 second.
*/
#define CPU_USAGE_STAT_THRESHOLD 10

/** @file
*/

namespace apache {
namespace geode {
namespace statistics {

/**
 * Solaris Implementation to fetch operating system stats.
 *
 */

class HostStatHelperSolaris {
 public:
  static void refreshProcess(ProcessStats* processStats);
  static void closeHostStatHelperSolaris();
  // static refreeshSystem(Statistics* stats);

 private:
  static uint8_t m_logStatErrorCountDown;
  static kstat_ctl_t* m_kstat;
  static uint32_t m_cpuUtilPrev[CPU_STATES];
  static bool m_initialized;
  static void getKernelStats(uint32_t*);
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif  // if def(_SOLARIS)

#endif // GEODE_STATISTICS_HOSTSTATHELPERSOLARIS_H_
