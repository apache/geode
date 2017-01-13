/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#ifndef __GEMFIRE_CACHEPERFSTATS_H__
#define __GEMFIRE_CACHEPERFSTATS_H__

#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/statistics/Statistics.hpp>
#include <gfcpp/statistics/StatisticsFactory.hpp>

namespace gemfire {

using namespace gemfire_statistics;

/** hold statistics for cache.. */
class CPPCACHE_EXPORT CachePerfStats {
 public:
  CachePerfStats() {
    StatisticsFactory* factory = StatisticsFactory::getExistingInstance();
    GF_D_ASSERT(!!factory);

    StatisticsType* statsType = factory->findType("CachePerfStats");

    if (statsType == NULL) {
      const bool largerIsBetter = true;
      StatisticDescriptor** statDescArr = new StatisticDescriptor*[24];

      statDescArr[0] = factory->createIntCounter(
          "creates", "The total number of cache creates", "entries",
          largerIsBetter);
      statDescArr[1] = factory->createIntCounter(
          "puts", "The total number of cache puts", "entries", largerIsBetter);
      statDescArr[2] = factory->createIntCounter(
          "gets", "The total number of cache gets", "entries", largerIsBetter);
      statDescArr[3] = factory->createIntCounter(
          "hits", "The total number of cache hits", "entries", largerIsBetter);
      statDescArr[4] = factory->createIntCounter(
          "misses", "The total number of cache misses", "entries",
          !largerIsBetter);
      statDescArr[5] = factory->createIntGauge(
          "entries", "The current number of cache entries", "entries",
          largerIsBetter);
      statDescArr[6] = factory->createIntCounter(
          "destroys", "The total number of cache destroys", "entries",
          largerIsBetter);
      statDescArr[7] = factory->createIntCounter(
          "overflows",
          "The total number of cache overflows to persistence backup",
          "entries", largerIsBetter);
      statDescArr[8] =
          factory->createIntCounter("retrieves",
                                    "The total number of cache entries fetched "
                                    "from persistence backup into the cache",
                                    "entries", largerIsBetter);
      statDescArr[9] = factory->createIntCounter(
          "cacheListenerCallsCompleted",
          "Total number of times a cache listener call has completed",
          "operations", largerIsBetter);
      statDescArr[10] =
          factory->createIntCounter("deltaPuts",
                                    "Total number of puts containing delta "
                                    "that have been sent from client to server",
                                    "entries", largerIsBetter);
      statDescArr[11] = factory->createIntCounter(
          "processedDeltaMessages",
          "Total number of messages containing delta received from server and "
          "processed after reception",
          "entries", largerIsBetter);
      statDescArr[12] = factory->createIntCounter(
          "deltaMessageFailures",
          "Total number of messages containing delta (received from server) "
          "but could not be processed after reception.",
          "entries", largerIsBetter);
      statDescArr[13] = factory->createLongCounter(
          "processedDeltaMessagesTime",
          "Total time spent applying delta (received from server) on existing "
          "values at client",
          "operations", largerIsBetter);
      statDescArr[14] = factory->createIntCounter(
          "tombstoneCount", "The total number of current tombstones", "entries",
          false);
      statDescArr[15] = factory->createLongCounter(
          "nonReplicatedTombstonesSize",
          "The total number of bytes consumed by tombstones for all regions in "
          "client processes",
          "bytes", false);
      statDescArr[16] = factory->createIntCounter(
          "conflatedEvents",
          "The number of conflicting events that have been elided and not "
          "passed on to event listeners",
          "operations", largerIsBetter);
      statDescArr[17] = factory->createIntCounter(
          "pdxInstanceDeserializations",
          "Total number of times getObject has been called on a PdxInstance.",
          "entries", largerIsBetter);
      statDescArr[18] =
          factory->createLongCounter("pdxInstanceDeserializationTime",
                                     "Total amount of time, in nanoseconds, "
                                     "spent deserializing PdxInstances by"
                                     "calling getObject.",
                                     "operations", !largerIsBetter);
      statDescArr[19] = factory->createIntCounter(
          "pdxInstanceCreations",
          "Total number of times a deserialization created a PdxInstance.",
          "entries", largerIsBetter);
      statDescArr[20] = factory->createIntCounter(
          "pdxSerializations", "Total number of pdx serializations.", "entries",
          largerIsBetter);
      statDescArr[21] = factory->createLongCounter(
          "pdxSerializedBytes",
          "Total number of bytes produced by pdx serialization.", "entries",
          !largerIsBetter);
      statDescArr[22] = factory->createIntCounter(
          "pdxDeserializations", "Total number of pdx deserializations.",
          "entries", largerIsBetter);
      statDescArr[23] = factory->createLongCounter(
          "pdxDeserializedBytes",
          "Total number of bytes read by pdx deserialization.", "entries",
          !largerIsBetter);

      statsType = factory->createType("CachePerfStats",
                                      "Statistics about native client cache",
                                      statDescArr, 24);
    }
    GF_D_ASSERT(statsType != NULL);
    // Create Statistics object
    m_cachePerfStats =
        factory->createAtomicStatistics(statsType, "CachePerfStats");

    // get Id of Statistics Descriptors
    m_destroysId = statsType->nameToId("destroys");
    m_createsId = statsType->nameToId("creates");
    m_putsId = statsType->nameToId("puts");
    m_getsId = statsType->nameToId("gets");
    m_hitsId = statsType->nameToId("hits");
    m_missesId = statsType->nameToId("misses");
    m_entriesId = statsType->nameToId("entries");
    m_overflowsId = statsType->nameToId("overflows");
    m_retrievesId = statsType->nameToId("retrieves");
    m_numListeners = statsType->nameToId("cacheListenerCallsCompleted");
    m_deltaPut = statsType->nameToId("deltaPuts");
    m_deltaReceived = statsType->nameToId("processedDeltaMessages");
    m_deltaFailedOnReceive = statsType->nameToId("deltaMessageFailures");
    m_processedDeltaMessagesTime =
        statsType->nameToId("processedDeltaMessagesTime");
    m_tombstoneCount = statsType->nameToId("tombstoneCount");
    m_tombstoneSize = statsType->nameToId("nonReplicatedTombstonesSize");
    m_conflatedEvents = statsType->nameToId("conflatedEvents");
    m_pdxInstanceDeserializationsId =
        statsType->nameToId("pdxInstanceDeserializations");
    m_pdxInstanceDeserializationTimeId =
        statsType->nameToId("pdxInstanceDeserializationTime");
    m_pdxInstanceCreationsId = statsType->nameToId("pdxInstanceCreations");
    m_pdxSerializationsId = statsType->nameToId("pdxSerializations");
    m_pdxSerializedBytesId = statsType->nameToId("pdxSerializedBytes");
    m_pdxDeserializationsId = statsType->nameToId("pdxDeserializations");
    m_pdxDeserializedBytesId = statsType->nameToId("pdxDeserializedBytes");

    // Set initial value
    m_cachePerfStats->setInt(m_destroysId, 0);
    m_cachePerfStats->setInt(m_createsId, 0);
    m_cachePerfStats->setInt(m_putsId, 0);
    m_cachePerfStats->setInt(m_getsId, 0);
    m_cachePerfStats->setInt(m_hitsId, 0);
    m_cachePerfStats->setInt(m_missesId, 0);
    m_cachePerfStats->setInt(m_entriesId, 0);
    m_cachePerfStats->setInt(m_overflowsId, 0);
    m_cachePerfStats->setInt(m_retrievesId, 0);
    m_cachePerfStats->setInt(m_numListeners, 0);
    m_cachePerfStats->setInt(m_deltaPut, 0);
    m_cachePerfStats->setInt(m_deltaReceived, 0);
    m_cachePerfStats->setInt(m_deltaFailedOnReceive, 0);
    m_cachePerfStats->setLong(m_processedDeltaMessagesTime, 0);
    m_cachePerfStats->setInt(m_tombstoneCount, 0);
    m_cachePerfStats->setLong(m_tombstoneSize, 0);
    m_cachePerfStats->setInt(m_conflatedEvents, 0);
    m_cachePerfStats->setInt(m_pdxInstanceDeserializationsId, 0);
    m_cachePerfStats->setLong(m_pdxInstanceDeserializationTimeId, 0);
    m_cachePerfStats->setInt(m_pdxInstanceCreationsId, 0);
    m_cachePerfStats->setInt(m_pdxSerializationsId, 0);
    m_cachePerfStats->setLong(m_pdxSerializedBytesId, 0);
    m_cachePerfStats->setInt(m_pdxDeserializationsId, 0);
    m_cachePerfStats->setLong(m_pdxDeserializedBytesId, 0);
  }

  virtual ~CachePerfStats() { m_cachePerfStats = NULL; }

  void close() {
    /*StatisticDescriptor** statDescArr =
    m_cachePerfStats->getType()->getStatistics();
    for ( int i = 0; i < m_cachePerfStats->getType()->getDescriptorsCount();
    i++) {
        delete statDescArr[i];
    }*/
    m_cachePerfStats->close();
  }

  inline void incDestroys() { m_cachePerfStats->incInt(m_destroysId, 1); }

  inline void incCreates() { m_cachePerfStats->incInt(m_createsId, 1); }

  inline void incPuts() { m_cachePerfStats->incInt(m_putsId, 1); }

  inline void incGets() { m_cachePerfStats->incInt(m_getsId, 1); }

  inline void incHits() { m_cachePerfStats->incInt(m_hitsId, 1); }

  inline void incMisses() { m_cachePerfStats->incInt(m_missesId, 1); }

  inline void incOverflows() { m_cachePerfStats->incInt(m_overflowsId, 1); }

  inline void incRetrieves() { m_cachePerfStats->incInt(m_retrievesId, 1); }

  inline void incEntries(int32_t delta) {
    m_cachePerfStats->incInt(m_entriesId, delta);
  }

  inline void incListenerCalls() {
    m_cachePerfStats->incInt(m_numListeners, 1);
  }

  inline void incDeltaPut() { m_cachePerfStats->incInt(m_deltaPut, 1); }

  inline void incDeltaReceived() {
    m_cachePerfStats->incInt(m_deltaReceived, 1);
  }

  inline void incFailureOnDeltaReceived() {
    m_cachePerfStats->incInt(m_deltaFailedOnReceive, 1);
  }

  inline void incTimeSpentOnDeltaApplication(long time) {
    m_cachePerfStats->incInt(m_processedDeltaMessagesTime, time);
  }

  inline void incTombstoneCount() {
    m_cachePerfStats->incInt(m_tombstoneCount, 1);
  }
  inline void decTombstoneCount() {
    m_cachePerfStats->incInt(m_tombstoneCount, -1);
  }
  inline void incTombstoneSize(int64_t size) {
    m_cachePerfStats->incLong(m_tombstoneSize, size);
  }
  inline void decTombstoneSize(int64_t size) {
    m_cachePerfStats->incLong(m_tombstoneSize, -size);
  }
  inline void incConflatedEvents() {
    m_cachePerfStats->incInt(m_conflatedEvents, 1);
  }
  int64_t getTombstoneSize() {
    return m_cachePerfStats->getLong(m_tombstoneSize);
  }
  int32_t getTombstoneCount() {
    return m_cachePerfStats->getInt(m_tombstoneCount);
  }
  int32_t getConflatedEvents() {
    return m_cachePerfStats->getInt(m_conflatedEvents);
  }

  inline void incPdxInstanceDeserializations() {
    m_cachePerfStats->incInt(m_pdxInstanceDeserializationsId, 1);
  }

  inline gemfire_statistics::Statistics* getStat() { return m_cachePerfStats; }

  inline int32_t getPdxInstanceDeserializationTimeId() {
    return m_pdxInstanceDeserializationTimeId;
  }

  inline void incPdxInstanceCreations() {
    m_cachePerfStats->incInt(m_pdxInstanceCreationsId, 1);
  }

  inline int32_t getPdxInstanceDeserializations() {
    return m_cachePerfStats->getInt(m_pdxInstanceDeserializationsId);
  }

  inline int32_t getPdxInstanceDeserializationTime() {
    return m_cachePerfStats->getInt(m_pdxInstanceDeserializationTimeId);
  }

  inline int32_t getPdxInstanceCreations() {
    return m_cachePerfStats->getInt(m_pdxInstanceCreationsId);
  }

  inline void incPdxSerialization(int32_t bytes) {
    m_cachePerfStats->incInt(m_pdxSerializationsId, 1);
    m_cachePerfStats->incLong(m_pdxSerializedBytesId, bytes);
  }

  inline int32_t getPdxSerializations() {
    return m_cachePerfStats->getInt(m_pdxSerializationsId);
  }

  inline int64_t getPdxSerializationBytes() {
    return m_cachePerfStats->getLong(m_pdxSerializedBytesId);
  }

  inline void incPdxDeSerialization(int32_t bytes) {
    m_cachePerfStats->incInt(m_pdxDeserializationsId, 1);
    m_cachePerfStats->incLong(m_pdxDeserializedBytesId, bytes);
  }

  inline int32_t getPdxDeSerializations() {
    return m_cachePerfStats->getInt(m_pdxDeserializationsId);
  }

  inline int64_t getPdxDeSerializationBytes() {
    return m_cachePerfStats->getLong(m_pdxDeserializedBytesId);
  }

 private:
  Statistics* m_cachePerfStats;

  int32_t m_destroysId;
  int32_t m_createsId;
  int32_t m_putsId;
  int32_t m_getsId;
  int32_t m_hitsId;
  int32_t m_missesId;
  int32_t m_entriesId;
  int32_t m_overflowsId;
  int32_t m_retrievesId;
  int32_t m_numListeners;
  int32_t m_deltaPut;
  int32_t m_deltaReceived;
  int32_t m_deltaFailedOnReceive;
  int32_t m_processedDeltaMessagesTime;
  int32_t m_tombstoneCount;
  int32_t m_tombstoneSize;
  int32_t m_conflatedEvents;
  int32_t m_pdxInstanceDeserializationsId;
  int32_t m_pdxInstanceDeserializationTimeId;
  int32_t m_pdxInstanceCreationsId;
  int32_t m_pdxSerializationsId;
  int32_t m_pdxSerializedBytesId;
  int32_t m_pdxDeserializationsId;
  int32_t m_pdxDeserializedBytesId;
};
}

#endif  // __GEMFIRE_CACHEPERFSTATS_H__
