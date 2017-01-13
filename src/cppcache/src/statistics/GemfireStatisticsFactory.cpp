
/*=========================================================================
 * Copyright (c) 2004-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include <gfcpp/gfcpp_globals.hpp>

#include <ace/Recursive_Thread_Mutex.h>
#include <ace/OS.h>
#include <ace/Thread_Mutex.h>
#include <ace/Guard_T.h>
#include <gfcpp/Exception.hpp>
#include "GemfireStatisticsFactory.hpp"
#include <gfcpp/Log.hpp>
#include <string>
#include "AtomicStatisticsImpl.hpp"
#include "OsStatisticsImpl.hpp"
#include "HostStatHelper.hpp"

using namespace gemfire;
using namespace gemfire_statistics;

/**
 * static member initialization
 */
GemfireStatisticsFactory* GemfireStatisticsFactory::s_singleton = NULL;

GemfireStatisticsFactory::GemfireStatisticsFactory(
    StatisticsManager* statMngr) {
  m_name = "GemfireStatisticsFactory";
  m_id = ACE_OS::getpid();
  m_statsListUniqueId = 1;

  m_statMngr = statMngr;
}

GemfireStatisticsFactory* GemfireStatisticsFactory::initInstance(
    StatisticsManager* statMngr) {
  if (!s_singleton) {
    s_singleton = new GemfireStatisticsFactory(statMngr);
  }

  return s_singleton;
}

GemfireStatisticsFactory* GemfireStatisticsFactory::getExistingInstance() {
  GF_D_ASSERT(!!s_singleton);

  s_singleton->getId();  // should fault if !s_singleton

  return s_singleton;
}

/**************************Dtor*******************************************/
void GemfireStatisticsFactory::clean() {
  if (s_singleton != NULL) {
    delete s_singleton;
    s_singleton = NULL;
  }
}

GemfireStatisticsFactory::~GemfireStatisticsFactory() {
  try {
    m_statMngr = NULL;

    // Clean Map : Delete all the pointers of StatisticsType from the map.
    if (statsTypeMap.total_size() == 0) return;

    ACE_Map_Manager<std::string, StatisticsTypeImpl*,
                    ACE_Recursive_Thread_Mutex>::iterator iterFind =
        statsTypeMap.begin();
    while (iterFind != statsTypeMap.end()) {
      delete (*iterFind).int_id_;
      (*iterFind).int_id_ = NULL;
      iterFind++;
    }
    statsTypeMap.unbind_all();

  } catch (const Exception& ex) {
    Log::warningCatch("~GemfireStatisticsFactory swallowing GemFire exception",
                      ex);

  } catch (const std::exception& ex) {
    std::string what = "~GemfireStatisticsFactory swallowing std::exception: ";
    what += ex.what();
    LOGWARN(what.c_str());

  } catch (...) {
    LOGERROR("~GemfireStatisticsFactory swallowing unknown exception");
  }
}

const char* GemfireStatisticsFactory::getName() { return m_name; }

int64 GemfireStatisticsFactory::getId() { return m_id; }

Statistics* GemfireStatisticsFactory::createStatistics(StatisticsType* type) {
  return createAtomicStatistics(type, NULL, 0);
}

Statistics* GemfireStatisticsFactory::createStatistics(StatisticsType* type,
                                                       const char* textId) {
  return createAtomicStatistics(type, textId, 0);
}

Statistics* GemfireStatisticsFactory::createStatistics(StatisticsType* type,
                                                       const char* textId,
                                                       int64 numericId) {
  return createAtomicStatistics(type, textId, 0);
}

Statistics* GemfireStatisticsFactory::createOsStatistics(StatisticsType* type,
                                                         const char* textId,
                                                         int64 numericId) {
  // Validate input
  if (type == NULL) {
    throw IllegalArgumentException("StatisticsType* is Null");
  }

  int64 myUniqueId;
  {
    ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_statsListUniqueIdLock);
    myUniqueId = m_statsListUniqueId++;
  }

  Statistics* result =
      new OsStatisticsImpl(type, textId, numericId, myUniqueId, this);
  { m_statMngr->addStatisticsToList(result); }

  return result;
}

Statistics* GemfireStatisticsFactory::createAtomicStatistics(
    StatisticsType* type) {
  return createAtomicStatistics(type, NULL, 0);
}

Statistics* GemfireStatisticsFactory::createAtomicStatistics(
    StatisticsType* type, const char* textId) {
  return createAtomicStatistics(type, textId, 0);
}

Statistics* GemfireStatisticsFactory::createAtomicStatistics(
    StatisticsType* type, const char* textId, int64 numericId) {
  // Validate input
  if (type == NULL) {
    throw IllegalArgumentException("StatisticsType* is Null");
  }
  int64 myUniqueId;

  {
    ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_statsListUniqueIdLock);
    myUniqueId = m_statsListUniqueId++;
  }

  Statistics* result =
      new AtomicStatisticsImpl(type, textId, numericId, myUniqueId, this);

  { m_statMngr->addStatisticsToList(result); }

  return result;
}

Statistics* GemfireStatisticsFactory::findFirstStatisticsByType(
    StatisticsType* type) {
  return (m_statMngr->findFirstStatisticsByType(type));
}

StatisticsTypeImpl* GemfireStatisticsFactory::addType(StatisticsTypeImpl* st) {
  StatisticsTypeImpl* st1;
  std::string temp(st->getName());
  int status;
  try {
    status = statsTypeMap.rebind(temp, st, st1);
  } catch (const std::exception& ex) {
    throw IllegalArgumentException(ex.what());
  } catch (...) {
    throw IllegalArgumentException("addType: unknown exception");
  }
  if (status == 1) {
  } else if (status == -1) {
    throw IllegalArgumentException(
        "GemfireStatisticsFactory::addType: failed "
        "to add new type %s",
        temp.c_str());
  }
  return st;
}

/**
 * Creates  a StatisticType for the given shared class.
 */
StatisticsType* GemfireStatisticsFactory::createType(
    const char* name, const char* description, StatisticDescriptor** stats,
    int32 statsLength) {
  StatisticsTypeImpl* st =
      new StatisticsTypeImpl(name, description, stats, statsLength);

  if (st != NULL) {
    st = addType(st);
  } else {
    throw OutOfMemoryException(
        "GemfireStatisticsFactory::createType :: out memory");
  }
  return st;
}

StatisticsType* GemfireStatisticsFactory::findType(const char* name) {
  std::string statName = name;
  StatisticsTypeImpl* st = NULL;
  int status = statsTypeMap.find(statName, st);
  if (status == -1) {
    std::string temp(name);
    std::string s = "There is no statistic named \"" + temp + "\"";
    // LOGWARN(s.c_str());
    // throw IllegalArgumentException(s.c_str());
    return NULL;
  } else {
    return st;
  }
}

StatisticDescriptor* GemfireStatisticsFactory::createIntCounter(
    const char* name, const char* description, const char* units,
    int8 largerBetter) {
  return StatisticDescriptorImpl::createIntCounter(name, description, units,
                                                   largerBetter);
}

StatisticDescriptor* GemfireStatisticsFactory::createLongCounter(
    const char* name, const char* description, const char* units,
    int8 largerBetter) {
  return StatisticDescriptorImpl::createLongCounter(name, description, units,
                                                    largerBetter);
}

StatisticDescriptor* GemfireStatisticsFactory::createDoubleCounter(
    const char* name, const char* description, const char* units,
    int8 largerBetter) {
  return StatisticDescriptorImpl::createDoubleCounter(name, description, units,
                                                      largerBetter);
}

StatisticDescriptor* GemfireStatisticsFactory::createIntGauge(
    const char* name, const char* description, const char* units,
    int8 largerBetter) {
  return StatisticDescriptorImpl::createIntGauge(name, description, units,
                                                 largerBetter);
}

StatisticDescriptor* GemfireStatisticsFactory::createLongGauge(
    const char* name, const char* description, const char* units,
    int8 largerBetter) {
  return StatisticDescriptorImpl::createLongGauge(name, description, units,
                                                  largerBetter);
}

StatisticDescriptor* GemfireStatisticsFactory::createDoubleGauge(
    const char* name, const char* description, const char* units,
    int8 largerBetter) {
  return StatisticDescriptorImpl::createDoubleGauge(name, description, units,
                                                    largerBetter);
}
