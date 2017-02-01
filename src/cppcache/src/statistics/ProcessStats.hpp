#pragma once

#ifndef GEODE_STATISTICS_PROCESSSTATS_H_
#define GEODE_STATISTICS_PROCESSSTATS_H_

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
#include <gfcpp/statistics/Statistics.hpp>
using namespace apache::geode::client;

/** @file
*/

namespace apache {
namespace geode {
namespace statistics {

/**
 * Abstracts the process statistics that are common on all platforms.
 * This is necessary for monitoring the health of GemFire components.
 *
 */
class CPPCACHE_EXPORT ProcessStats {
 public:
  /**
   * Creates a new <code>ProcessStats</code> that wraps the given
   * <code>Statistics</code>.
   */
  ProcessStats();

  /**
   * Returns the CPU Usage
   */
  virtual int32 getCpuUsage() = 0;

  /**
   * Returns Number of threads
   */
  virtual int32 getNumThreads() = 0;

  /**
   * Returns the size of this process (resident set on UNIX or working
   * set on Windows) in megabytes
   */
  virtual int64 getProcessSize() = 0;

  /**
   * Close Underline Statistics
   */
  virtual void close() = 0;
  virtual int64 getCPUTime() = 0;

  /**
   * Returns the CPU time which is sum of userTime and systemTime
   */
  virtual int64 getAllCpuTime() = 0;

  /**
   * Destructor
   */
  virtual ~ProcessStats();
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_STATISTICS_PROCESSSTATS_H_
