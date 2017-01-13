

#ifndef _GEMFIRE_STATISTICS_STATISTICSAPPMANAGER_HPP_
#define _GEMFIRE_STATISTICS_STATISTICSAPPMANAGER_HPP_
/*=========================================================================
 * Copyright (c) 2004-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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

namespace gemfire_statistics {

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

}  // namespace

#endif  //  _GEMFIRE_STATISTICS_STATISTICSAPPMANAGER_HPP_
