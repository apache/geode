#pragma once

#ifndef GEODE_GFCPP_STATISTICS_STATISTICSTYPE_H_
#define GEODE_GFCPP_STATISTICS_STATISTICSTYPE_H_

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
#include <gfcpp/statistics/StatisticDescriptor.hpp>

using namespace apache::geode::client;

/** @file
*/

namespace apache {
namespace geode {
namespace statistics {

/**
 * Used to describe a logical collection of StatisticDescriptors. These
 * descriptions
 * are used to create an instance of {@link Statistics}.
 *
 * <P>
 * To get an instance of this interface use an instance of
 * {@link StatisticsFactory}.
 *
 */

class CPPCACHE_EXPORT StatisticsType {
 public:
  /**
   * Returns the name of this statistics type.
   */
  virtual const char* getName() = 0;

  /**
   * Returns a description of this statistics type.
   */
  virtual const char* getDescription() = 0;

  /**
   * Returns descriptions of the statistics that this statistics type
   * gathers together.
   */
  virtual StatisticDescriptor** getStatistics() = 0;

  /**
   * Returns the id of the statistic with the given name in this
   * statistics instance.
   *
   * @throws IllegalArgumentException
   *         No statistic named <code>name</code> exists in this
   *         statistics instance.
   */
  virtual int32 nameToId(const char* name) = 0;
  /**
   * Returns the descriptor of the statistic with the given name in this
   * statistics instance.
   *
   * @throws IllegalArgumentException
   *         No statistic named <code>name</code> exists in this
   *         statistics instance.
   */
  virtual StatisticDescriptor* nameToDescriptor(const char* name) = 0;

  /**
   * Returns the total number of statistics descriptors in the type.
   */
  virtual int32 getDescriptorsCount() = 0;

  // protected:
  /**
   * Destructor
   */
  virtual ~StatisticsType() {}

};  // class

}  // namespace statistics
}  // namespace geode
}  // namespace apache

#endif // GEODE_GFCPP_STATISTICS_STATISTICSTYPE_H_
