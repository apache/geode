
/*=========================================================================
 * Copyright (c) 2004-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include <gfcpp/gfcpp_globals.hpp>

#include <ace/Atomic_Op_T.h>
#include <ace/Recursive_Thread_Mutex.h>
#include <ace/OS.h>
#include <ace/Thread_Mutex.h>
#include <ace/Time_Value.h>
#include <ace/Guard_T.h>
#include <gfcpp/Exception.hpp>
#include "StatisticsManager.hpp"
#include <gfcpp/Log.hpp>
#include "GemfireStatisticsFactory.hpp"
#include <string>
#include "AtomicStatisticsImpl.hpp"
#include "OsStatisticsImpl.hpp"

using namespace gemfire;
using namespace gemfire_statistics;

/**
 * static member initialization
 */
StatisticsManager* StatisticsManager::s_singleton = NULL;

StatisticsManager::StatisticsManager(const char* filePath, int64 sampleInterval,
                                     bool enabled, int64 statFileLimit,
                                     int64 statDiskSpaceLimit)
    : m_sampler(NULL), m_adminRegion(NULLPTR) {
  m_sampleIntervalMs =
      static_cast<int32_t>(sampleInterval) * 1000; /* convert to millis */
  m_newlyAddedStatsList.reserve(16);               // Allocate initial sizes
  GemfireStatisticsFactory::initInstance(this);

  try {
    if (m_sampler == NULL && enabled) {
      m_sampler = new HostStatSampler(filePath, m_sampleIntervalMs, this,
                                      statFileLimit, statDiskSpaceLimit);
      m_sampler->start();
    }
  } catch (...) {
    delete m_sampler;
    throw;
  }
}

StatisticsManager* StatisticsManager::initInstance(const char* filePath,
                                                   int64 sampleIntervalMs,
                                                   bool enabled,
                                                   int64 statsFileLimit,
                                                   int64 statsDiskSpaceLimit) {
  if (!s_singleton) {
    s_singleton = new StatisticsManager(filePath, sampleIntervalMs, enabled,
                                        statsFileLimit, statsDiskSpaceLimit);
  }

  return s_singleton;
}

StatisticsManager* StatisticsManager::getExistingInstance() {
  if (s_singleton) {
    return s_singleton;
  }

  return NULL;
}

void StatisticsManager::forceSample() {
  if (m_sampler) m_sampler->forceSample();
}

/**************************Dtor*******************************************/

StatisticsManager::~StatisticsManager() {
  try {
    // Stop the sampler
    closeSampler();

    // List should be empty if close() is called on each Stats object
    // If this is not done, delete all the pointers
    ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_statsListLock);
    int32 count = static_cast<int32>(m_statsList.size());
    if (count > 0) {
      LOGFINEST("~StatisticsManager has found %d leftover statistics:", count);
      std::vector<Statistics*>::iterator iterFind = m_statsList.begin();
      while (iterFind != m_statsList.end()) {
        if (*iterFind != NULL) {
          std::string temp((*iterFind)->getType()->getName());
          LOGFINEST("Leftover statistic: %s", temp.c_str());
          /* adongre
           * Passing null variable "*iterFind" to function
           * "gemfire_statistics::StatisticsManager::deleteStatistics(gemfire_statistics::Statistics
           * *&)",
           * which dereferences it.
           * FIX : Put the call into the if condition
           */
          deleteStatistics(*iterFind);
          *iterFind = NULL;
        }
        ++iterFind;
      }
      m_statsList.erase(m_statsList.begin(), m_statsList.end());
    }

    // Clean Factory: clean Type map etc.
    GemfireStatisticsFactory::clean();

  } catch (const Exception& ex) {
    Log::warningCatch("~StatisticsManager swallowing GemFire exception", ex);

  } catch (const std::exception& ex) {
    std::string what = "~StatisticsManager swallowing std::exception: ";
    what += ex.what();
    Log::warning(what.c_str());

  } catch (...) {
    Log::error("~StatisticsManager swallowing unknown exception");
  }
}

void StatisticsManager::clean() {
  if (s_singleton != NULL) {
    delete s_singleton;
    s_singleton = NULL;
  }
}

////////////////////// Mutex methods ///////////////////////////

ACE_Recursive_Thread_Mutex& StatisticsManager::getListMutex() {
  return m_statsListLock;
}

void StatisticsManager::closeSampler() {
  if (m_sampler != NULL) {
    m_sampler->stop();
    delete m_sampler;
    m_sampler = NULL;
  }
}
void StatisticsManager::addStatisticsToList(Statistics* stat) {
  if (stat) {
    ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_statsListLock);
    m_statsList.push_back(stat);

    /* Add to m_newlyAddedStatsList also so that a fresh traversal not needed
    before sampling.
    After writing token to sampled file, stats ptrs will be deleted from list.
    */
    m_newlyAddedStatsList.push_back(stat);
  }
}

int32 StatisticsManager::getStatListModCount() {
  ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_statsListLock);
  return static_cast<int32>(m_statsList.size());
}

std::vector<Statistics*>& StatisticsManager::getStatsList() {
  return this->m_statsList;
}

std::vector<Statistics*>& StatisticsManager::getNewlyAddedStatsList() {
  return this->m_newlyAddedStatsList;
}

Statistics* StatisticsManager::findFirstStatisticsByType(StatisticsType* type) {
  ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_statsListLock);
  std::vector<Statistics*>::iterator start = m_statsList.begin();
  while (start != m_statsList.end()) {
    if (!((*start)->isClosed()) && ((*start)->getType() == type)) {
      return *start;
    }
    start++;
  }
  return NULL;
}

std::vector<Statistics*> StatisticsManager::findStatisticsByType(
    StatisticsType* type) {
  std::vector<Statistics*> hits;

  ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_statsListLock);

  std::vector<Statistics*>::iterator start = m_statsList.begin();
  while (start != m_statsList.end()) {
    if (!((*start)->isClosed()) && ((*start)->getType() == type)) {
      hits.push_back(*start);
    }
    start++;
  }
  return hits;
}

std::vector<Statistics*> StatisticsManager::findStatisticsByTextId(
    char* textId) {
  std::vector<Statistics*> hits;

  ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_statsListLock);

  std::vector<Statistics*>::iterator start = m_statsList.begin();
  while (start != m_statsList.end()) {
    if (!((*start)->isClosed()) && ((*start)->getTextId() == textId)) {
      hits.push_back(*start);
    }
    start++;
  }
  return hits;
}

std::vector<Statistics*> StatisticsManager::findStatisticsByNumericId(
    int64 numericId) {
  std::vector<Statistics*> hits;

  ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_statsListLock);

  std::vector<Statistics*>::iterator start = m_statsList.begin();
  while (start != m_statsList.end()) {
    if (!((*start)->isClosed()) && ((*start)->getNumericId() == numericId)) {
      hits.push_back(*start);
    }
    start++;
  }
  return hits;
}

Statistics* StatisticsManager::findStatisticsByUniqueId(int64 uniqueId) {
  ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_statsListLock);

  std::vector<Statistics*>::iterator start = m_statsList.begin();
  while (start != m_statsList.end()) {
    if (!((*start)->isClosed()) && ((*start)->getUniqueId() == uniqueId)) {
      Statistics* ret = *start;
      return ret;
    }
    start++;
  }
  return NULL;
}

void StatisticsManager::deleteStatistics(Statistics*& stat) {
  if (stat->isAtomic()) {
    AtomicStatisticsImpl* ptr = dynamic_cast<AtomicStatisticsImpl*>(stat);
    delete ptr;
  } else {
    OsStatisticsImpl* ptr = dynamic_cast<OsStatisticsImpl*>(stat);
    delete ptr;
  }
  stat = NULL;
}
