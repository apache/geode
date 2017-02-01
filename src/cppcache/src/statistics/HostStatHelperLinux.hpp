#pragma once

#ifndef GEODE_STATISTICS_HOSTSTATHELPERLINUX_H_
#define GEODE_STATISTICS_HOSTSTATHELPERLINUX_H_

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

#if defined(_LINUX)
#include <gfcpp/gfcpp_globals.hpp>
#include <string>
#include <sys/sysinfo.h>
#include "ProcessStats.hpp"

/** @file
*/

namespace apache {
namespace geode {
namespace statistics {

/**
 * Linux Implementation to fetch operating system stats.
 *
 */

class HostStatHelperLinux {
 public:
  static void refreshProcess(ProcessStats* processStats);
  // static refreeshSystem(Statistics* stats);

 private:
  static uint8_t m_logStatErrorCountDown;
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif  // if def(_LINUX)

#endif // GEODE_STATISTICS_HOSTSTATHELPERLINUX_H_
