

#ifndef _GEMFIRE_STATISTICS_GEMFIRESTATISTICSFACTORY_HPP_
#define _GEMFIRE_STATISTICS_GEMFIRESTATISTICSFACTORY_HPP_
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
#include <ace/Recursive_Thread_Mutex.h>
#include <ace/Map_Manager.h>
#include "StatisticsTypeImpl.hpp"
#include <gfcpp/statistics/StatisticsFactory.hpp>
#include "StatisticsManager.hpp"
#include <gfcpp/ExceptionTypes.hpp>

using namespace gemfire;

/** @file
*/

namespace gemfire_statistics {

class StatisticsManager;

/**
 * Gemfire's implementation of {@link StatisticsFactory}.
 *
 */
class GemfireStatisticsFactory : public StatisticsFactory {
 private:
  //--------------------Properties-------------------------------------------------

  const char* m_name;

  int64 m_id;

  StatisticsManager* m_statMngr;

  static GemfireStatisticsFactory* s_singleton;

  //------------------  methods ------------------------------

  GemfireStatisticsFactory(StatisticsManager* statMngr);

  int64 m_statsListUniqueId;  // Creates a unique id for each stats object in
                              // the list

  ACE_Recursive_Thread_Mutex m_statsListUniqueIdLock;

  /* Maps a stat name to its StatisticDescriptor*/
  ACE_Map_Manager<std::string, StatisticsTypeImpl*, ACE_Recursive_Thread_Mutex>
      statsTypeMap;

  StatisticsTypeImpl* addType(StatisticsTypeImpl* t);

  //////////////////////////public member functions///////////////////////////

 public:
  ~GemfireStatisticsFactory();

  static void clean();

  const char* getName();

  int64 getId();

  static GemfireStatisticsFactory* initInstance(StatisticsManager* statMngr);

  static GemfireStatisticsFactory* getExistingInstance();

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

}  // namespace

#endif  //  _GEMFIRE_STATISTICS_GEMFIRESTATISTICSFACTORY_HPP_
