#pragma once

#ifndef GEODE_CQSERVICEVSDSTATS_H_
#define GEODE_CQSERVICEVSDSTATS_H_

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
#include <gfcpp/CqServiceStatistics.hpp>

namespace apache {
namespace geode {
namespace client {

using namespace apache::geode::statistics;

class CPPCACHE_EXPORT CqServiceVsdStats : public CqServiceStatistics {
 public:
  /** hold statistics for a cq. */
  CqServiceVsdStats(const char* cqName = "CqServiceVsdStats");

  /** disable stat collection for this item. */
  virtual ~CqServiceVsdStats();

  void close() { m_cqServiceVsdStats->close(); }
  inline void decNumCqsActive() {
    m_cqServiceVsdStats->incInt(m_numCqsActiveId, -1);
  }

  inline void incNumCqsActive() const {
    m_cqServiceVsdStats->incInt(m_numCqsActiveId, 1);
  }
  inline uint32_t numCqsActive() const {
    return m_cqServiceVsdStats->getInt(m_numCqsActiveId);
  }

  inline void incNumCqsCreated() {
    m_cqServiceVsdStats->incInt(m_numCqsCreatedId, 1);
  }
  inline uint32_t numCqsCreated() const {
    return m_cqServiceVsdStats->getInt(m_numCqsCreatedId);
  }

  inline uint32_t numCqsOnClient() const {
    return m_cqServiceVsdStats->getInt(m_numCqsOnClientId);
  }

  inline void incNumCqsClosed() {
    m_cqServiceVsdStats->incInt(m_numCqsClosedId, 1);
  }
  inline uint32_t numCqsClosed() const {
    return m_cqServiceVsdStats->getInt(m_numCqsClosedId);
  }

  inline void incNumCqsStopped() {
    m_cqServiceVsdStats->incInt(m_numCqsStoppedId, 1);
  }
  inline void decNumCqsStopped() {
    m_cqServiceVsdStats->incInt(m_numCqsStoppedId, -1);
  }
  inline uint32_t numCqsStopped() const {
    return m_cqServiceVsdStats->getInt(m_numCqsStoppedId);
  }

  inline void setNumCqsActive(uint32_t value) {
    m_cqServiceVsdStats->setInt(m_numCqsActiveId, value);
  }

  inline void setNumCqsOnClient(uint32_t value) {
    m_cqServiceVsdStats->setInt(m_numCqsOnClientId, value);
  }

  inline void setNumCqsClosed(uint32_t value) {
    m_cqServiceVsdStats->setInt(m_numCqsClosedId, value);
  }

  inline void setNumCqsStopped(uint32_t value) {
    m_cqServiceVsdStats->setInt(m_numCqsStoppedId, value);
  }

 private:
  apache::geode::statistics::Statistics* m_cqServiceVsdStats;

  int32_t m_numCqsActiveId;
  int32_t m_numCqsCreatedId;
  int32_t m_numCqsOnClientId;
  int32_t m_numCqsClosedId;
  int32_t m_numCqsStoppedId;
};

class CqServiceStatType {
 private:
  static int8 instanceFlag;
  static CqServiceStatType* single;
  static SpinLock m_singletonLock;
  static SpinLock m_statTypeLock;

 public:
  static CqServiceStatType* getInstance();

  StatisticsType* getStatType();

  static void clean();

 private:
  CqServiceStatType();
  StatisticDescriptor* m_stats[5];

  int32_t m_numCqsActiveId;
  int32_t m_numCqsCreatedId;
  int32_t m_numCqsOnClientId;
  int32_t m_numCqsClosedId;
  int32_t m_numCqsStoppedId;

 public:
  inline int32_t getNumCqsActiveId() { return m_numCqsActiveId; }

  inline int32_t getNumCqsCreatedId() { return m_numCqsCreatedId; }

  inline int32_t getNumCqsOnClientId() { return m_numCqsOnClientId; }

  inline int32_t getNumCqsClosedId() { return m_numCqsClosedId; }

  inline int32_t getNumCqsStoppedId() { return m_numCqsStoppedId; }
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_CQSERVICEVSDSTATS_H_
