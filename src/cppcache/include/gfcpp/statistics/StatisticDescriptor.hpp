#pragma once

#ifndef GEODE_GFCPP_STATISTICS_STATISTICDESCRIPTOR_H_
#define GEODE_GFCPP_STATISTICS_STATISTICDESCRIPTOR_H_

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

using namespace apache::geode::client;

/** @file
*/

namespace apache {
namespace geode {
namespace statistics {

/**
 * Describes an individual statistic whose value is updated by an
 * application and may be archived by GemFire.  These descriptions are
 * gathered together in a {@link StatisticsType}.
 *
 * <P>
 * To get an instance of this interface use an instance of
 * {@link StatisticsFactory}.
 * <P>
 * StatisticDescriptors are naturally ordered by their name.
 *
 */

class CPPCACHE_EXPORT StatisticDescriptor {
 public:
  /**
    * Returns the id of this statistic in a {@link StatisticsType
    * }. The id is initialized when its statistics
    * type is created.
    */
  virtual int32 getId() = 0;

  /**
   * Returns the name of this statistic
   */
  virtual const char* getName() = 0;

  /**
   * Returns a description of this statistic
   */
  virtual const char* getDescription() = 0;

  /**
   * Returns true if this statistic is a counter; false if its a gauge.
   * Counter statistics have values that always increase.
   * Gauge statistics have unconstrained values.
   */
  virtual int8 isCounter() = 0;

  /**
   *  Returns true if a larger statistic value indicates better performance.
   */
  virtual int8 isLargerBetter() = 0;

  /**
   *  Returns the unit in which this statistic is measured
   */
  virtual const char* getUnit() = 0;

  /*
   * Destructor
   */
  virtual ~StatisticDescriptor() {}

};  // class
}  // namespace statistics
}  // namespace geode
}  // namespace apache

#endif // GEODE_GFCPP_STATISTICS_STATISTICDESCRIPTOR_H_
