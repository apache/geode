#pragma once

#ifndef GEODE_GFCPP_CQSERVICESTATISTICS_H_
#define GEODE_GFCPP_CQSERVICESTATISTICS_H_

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
 * @class CqServiceStatistics CqServiceStatistics.hpp
 *
 * This class provides methods to get aggregate statistical information
 * about the CQs of a client.
 */
class CPPCACHE_EXPORT CqServiceStatistics : public SharedBase {
 public:
  /**
   * Get the number of CQs currently active.
   * Active CQs are those which are executing (in running state).
   * @return number of CQs
   */
  virtual uint32_t numCqsActive() const = 0;

  /**
   * Get the total number of CQs created. This is a cumulative number.
   * @return number of CQs created.
   */
  virtual uint32_t numCqsCreated() const = 0;

  /**
   * Get the total number of closed CQs. This is a cumulative number.
   * @return number of CQs closed.
   */
  virtual uint32_t numCqsClosed() const = 0;

  /**
   * Get the number of stopped CQs currently.
   * @return number of CQs stopped.
   */
  virtual uint32_t numCqsStopped() const = 0;

  /**
   * Get number of CQs that are currently active or stopped.
   * The CQs included in this number are either running or stopped (suspended).
   * Closed CQs are not included.
   * @return number of CQs on client.
   */
  virtual uint32_t numCqsOnClient() const = 0;
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_GFCPP_CQSERVICESTATISTICS_H_
