#pragma once

#ifndef GEODE_CQQUERYVSDSTATS_H_
#define GEODE_CQQUERYVSDSTATS_H_

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
#include <gfcpp/statistics/Statistics.hpp>
#include <gfcpp/statistics/StatisticsFactory.hpp>
#include "SpinLock.hpp"

#include <gfcpp/CqStatistics.hpp>

namespace apache {
namespace geode {
namespace client {

using namespace apache::geode::statistics;

class CPPCACHE_EXPORT CqQueryVsdStats : public CqStatistics {
 public:
  /** hold statistics for a cq. */
  CqQueryVsdStats(const char* cqName);

  /** disable stat collection for this item. */
  virtual ~CqQueryVsdStats();

  void close() { m_cqQueryVsdStats->close(); }

  inline void incNumInserts() { m_cqQueryVsdStats->incInt(m_numInsertsId, 1); }

  inline void incNumUpdates() { m_cqQueryVsdStats->incInt(m_numUpdatesId, 1); }

  inline void incNumDeletes() { m_cqQueryVsdStats->incInt(m_numDeletesId, 1); }

  inline void incNumEvents() { m_cqQueryVsdStats->incInt(m_numEventsId, 1); }

  inline uint32_t numInserts() const {
    return m_cqQueryVsdStats->getInt(m_numInsertsId);
  }
  inline uint32_t numUpdates() const {
    return m_cqQueryVsdStats->getInt(m_numUpdatesId);
  }
  inline uint32_t numDeletes() const {
    return m_cqQueryVsdStats->getInt(m_numDeletesId);
  }
  inline uint32_t numEvents() const {
    return m_cqQueryVsdStats->getInt(m_numEventsId);
  }

 private:
  apache::geode::statistics::Statistics* m_cqQueryVsdStats;

  int32_t m_numInsertsId;
  int32_t m_numUpdatesId;
  int32_t m_numDeletesId;
  int32_t m_numEventsId;
};

class CqQueryStatType {
 private:
  static int8 instanceFlag;
  static CqQueryStatType* single;
  static SpinLock m_singletonLock;
  static SpinLock m_statTypeLock;

 public:
  static CqQueryStatType* getInstance();

  StatisticsType* getStatType();

  static void clean();

 private:
  CqQueryStatType();
  StatisticDescriptor* m_stats[4];

  int32_t m_numInsertsId;
  int32_t m_numUpdatesId;
  int32_t m_numDeletesId;
  int32_t m_numEventsId;

 public:
  inline int32_t getNumInsertsId() { return m_numInsertsId; }

  inline int32_t getNumUpdatesId() { return m_numUpdatesId; }

  inline int32_t getNumDeletesId() { return m_numDeletesId; }

  inline int32_t getNumEventsId() { return m_numEventsId; }
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_CQQUERYVSDSTATS_H_
