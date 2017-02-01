#pragma once

#ifndef GEODE_GFCPP_CQSTATISTICS_H_
#define GEODE_GFCPP_CQSTATISTICS_H_

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

#include "gfcpp_globals.hpp"
#include "gf_types.hpp"

/**
 * @file
 */

namespace apache {
namespace geode {
namespace client {

/**
 * @cacheserver
 * Querying is only supported for native clients.
 * @endcacheserver
 * @class CqStatistics CqStatistics.hpp
 *
 * This class provides methods to get statistical information about a registered
 * Continuous Query (CQ)
 * represented by the CqQuery object.
 *
 */
class CPPCACHE_EXPORT CqStatistics : public SharedBase {
 public:
  /**
   * Get number of Insert events qualified by this CQ.
   * @return number of inserts.
   */
  virtual uint32_t numInserts() const = 0;

  /**
   * Get number of Delete events qualified by this CQ.
   * @return number of deletes.
   */
  virtual uint32_t numDeletes() const = 0;

  /**
   * Get number of Update events qualified by this CQ.
   * @return number of updates.
   */
  virtual uint32_t numUpdates() const = 0;

  /**
   * Get total of all the events qualified by this CQ.
   * @return total number of events.
   */
  virtual uint32_t numEvents() const = 0;
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_GFCPP_CQSTATISTICS_H_
