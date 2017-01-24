#ifndef _GEMFIRE_STATISTICS_HOSTSTATHELPERNULL_HPP_
#define _GEMFIRE_STATISTICS_HOSTSTATHELPERNULL_HPP_
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

#include <gfcpp/gfcpp_globals.hpp>

/** @file
 */

namespace apache {
namespace geode {
namespace statistics {

class HostStatHelperNull {
 public:
  static void refreshProcess(ProcessStats* processStats) {}
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif  // _GEMFIRE_STATISTICS_HOSTSTATHELPERLINUX_HPP_
