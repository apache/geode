#pragma once

#ifndef GEODE_STATISTICS_STATISTICSMANAGER_H_
#define GEODE_STATISTICS_STATISTICSMANAGER_H_

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
#include "HostStatSampler.hpp"
#include <gfcpp/ExceptionTypes.hpp>
#include "StatisticsTypeImpl.hpp"
#include <gfcpp/statistics/Statistics.hpp>
#include <AdminRegion.hpp>

/** @file
*/

namespace apache {
namespace geode {
namespace statistics {

/**
 * Head Application Manager for Statistics Module.
 *
 */
class StatisticsManager {
 private:
  //--------------------Properties-------------------------------------------------

  // interval at which the sampler will take a sample of Stats
  int32 m_sampleIntervalMs;

  //----------------Sampler and Stat Lists-----------------------------------

  HostStatSampler* m_sampler;  // Statistics sampler

  std::vector<Statistics*>
      m_statsList;  // Vector containing all the Stats objects

  // Vector containing stats pointers which are not yet sampled.
  std::vector<Statistics*> m_newlyAddedStatsList;

  ACE_Recursive_Thread_Mutex m_statsListLock;  // Mutex to lock the list of
                                               // Stats

  //----------------Admin Region -----------------------------------
  AdminRegionPtr m_adminRegion;

  //////////////////////////private member functions///////////////////////////

  static StatisticsManager* s_singleton;

  StatisticsManager(const char* filePath, int64 sampleIntervalMs, bool enabled,
                    int64 statFileLimit = 0, int64 statDiskSpaceLimit = 0);

  void closeSampler();

  //////////////////////////public member functions///////////////////////////

 public:
  static StatisticsManager* initInstance(const char* filePath,
                                         int64 sampleIntervalMs, bool enabled,
                                         int64 statFileLimit = 0,
                                         int64 statDiskSpaceLimit = 0);

  static StatisticsManager* getExistingInstance();

  void RegisterAdminRegion(AdminRegionPtr adminRegPtr) {
    m_adminRegion = adminRegPtr;
  }

  AdminRegionPtr getAdminRegion() { return m_adminRegion; }

  void forceSample();

  ~StatisticsManager();

  int32 getStatListModCount();

  void addStatisticsToList(Statistics* stat);

  static void clean();

  //--------------------Stat List
  // functions--------------------------------------
  std::vector<Statistics*>& getStatsList();

  std::vector<Statistics*>& getNewlyAddedStatsList();

  ACE_Recursive_Thread_Mutex& getListMutex();

  //------------ Find Statistics ---------------------

  /** Return the first instance that matches the type, or NULL */
  Statistics* findFirstStatisticsByType(StatisticsType* type);

  std::vector<Statistics*> findStatisticsByType(StatisticsType* type);

  std::vector<Statistics*> findStatisticsByTextId(char* textId);

  std::vector<Statistics*> findStatisticsByNumericId(int64 numericId);

  Statistics* findStatisticsByUniqueId(int64 uniqueId);

  static void deleteStatistics(Statistics*& stat);

};  // class

}  // namespace statistics
}  // namespace geode
}  // namespace apache

#endif // GEODE_STATISTICS_STATISTICSMANAGER_H_
