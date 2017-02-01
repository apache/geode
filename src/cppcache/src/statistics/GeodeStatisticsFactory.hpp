#pragma once

#ifndef GEODE_STATISTICS_GEODESTATISTICSFACTORY_H_
#define GEODE_STATISTICS_GEODESTATISTICSFACTORY_H_

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

#include <sys/types.h>
#ifndef WIN32
#include <unistd.h>
#endif
#include <vector>
#include <ace/Recursive_Thread_Mutex.h>
#include <ace/Map_Manager.h>
#include "StatisticsTypeImpl.hpp"
#include <gfcpp/statistics/StatisticsFactory.hpp>
#include "StatisticsManager.hpp"
#include <gfcpp/ExceptionTypes.hpp>

using namespace apache::geode::client;

/** @file
*/

namespace apache {
namespace geode {
namespace statistics {

class StatisticsManager;

/**
 * Gemfire's implementation of {@link StatisticsFactory}.
 *
 */
class GeodeStatisticsFactory : public StatisticsFactory {
 private:
  //--------------------Properties-------------------------------------------------

  const char* m_name;

  int64 m_id;

  StatisticsManager* m_statMngr;

  static GeodeStatisticsFactory* s_singleton;

  //------------------  methods ------------------------------

  GeodeStatisticsFactory(StatisticsManager* statMngr);

  int64 m_statsListUniqueId;  // Creates a unique id for each stats object in
                              // the list

  ACE_Recursive_Thread_Mutex m_statsListUniqueIdLock;

  /* Maps a stat name to its StatisticDescriptor*/
  ACE_Map_Manager<std::string, StatisticsTypeImpl*, ACE_Recursive_Thread_Mutex>
      statsTypeMap;

  StatisticsTypeImpl* addType(StatisticsTypeImpl* t);

  //////////////////////////public member functions///////////////////////////

 public:
  ~GeodeStatisticsFactory();

  static void clean();

  const char* getName();

  int64 getId();

  static GeodeStatisticsFactory* initInstance(StatisticsManager* statMngr);

  static GeodeStatisticsFactory* getExistingInstance();

  //------------ StatisticsFactory methods: Statistics
  //------------------------------
  Statistics* createStatistics(StatisticsType* type);

  Statistics* createStatistics(StatisticsType* type, const char* textId);

  Statistics* createStatistics(StatisticsType* type, const char* textId,
                               int64 numericId);

  Statistics* createOsStatistics(StatisticsType* type, const char* textId,
                                 int64 numericId);

  Statistics* createAtomicStatistics(StatisticsType* type);

  Statistics* createAtomicStatistics(StatisticsType* type, const char* textId);

  Statistics* createAtomicStatistics(StatisticsType* type, const char* textId,
                                     int64 numericId);

  //------------ StatisticsFactory methods: Statistics Type
  //------------------------------
  StatisticsType* createType(const char* name, const char* description,
                             StatisticDescriptor** stats, int32 statsLength);

  StatisticsType* findType(const char* name);

  //------------ StatisticsFactory methods: Statistics Descriptor
  //---------------------
  StatisticDescriptor* createIntCounter(const char* name,
                                        const char* description,
                                        const char* units, int8 largerBetter);

  StatisticDescriptor* createLongCounter(const char* name,
                                         const char* description,
                                         const char* units, int8 largerBetter);

  StatisticDescriptor* createDoubleCounter(const char* name,
                                           const char* description,
                                           const char* units,
                                           int8 largerBetter);

  StatisticDescriptor* createIntGauge(const char* name, const char* description,
                                      const char* units, int8 largerBetter);

  StatisticDescriptor* createLongGauge(const char* name,
                                       const char* description,
                                       const char* units, int8 largerBetter);

  StatisticDescriptor* createDoubleGauge(const char* name,
                                         const char* description,
                                         const char* units, int8 largerBetter);

  /** Return the first instance that matches the type, or NULL */
  Statistics* findFirstStatisticsByType(StatisticsType* type);

};  // class

}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_STATISTICS_GEODESTATISTICSFACTORY_H_
