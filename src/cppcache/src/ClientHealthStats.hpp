/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef _CLIENT_HEALTH_STATS_HPP_INCLUDED_
#define _CLIENT_HEALTH_STATS_HPP_INCLUDED_

#include <gfcpp/gf_types.hpp>
#include <gfcpp/Serializable.hpp>
#include <gfcpp/Log.hpp>
#include <gfcpp/CacheableDate.hpp>

namespace gemfire {

class ClientHealthStats : public Serializable {
 public:
  /**
   *@brief serialize this object
   **/
  virtual void toData(DataOutput& output) const;

  /**
   *@brief deserialize this object
   **/
  virtual Serializable* fromData(DataInput& input);

  /**
   * @brief creation function for dates.
   */
  static Serializable* createDeserializable();

  /**
   *@brief Return the classId of the instance being serialized.
   * This is used by deserialization to determine what instance
   * type to create and deserialize into.
   */
  virtual int32_t classId() const;

  /**
   *@brief return the typeId byte of the instance being serialized.
   * This is used by deserialization to determine what instance
   * type to create and deserialize into.
   */
  virtual int8_t typeId() const;

  virtual int8_t DSFID() const;
  /** @return the size of the object in bytes */
  virtual uint32_t objectSize() const { return sizeof(ClientHealthStats); }
  /**
   * Factory method for creating an instance of ClientHealthStats
   */
  static ClientHealthStatsPtr create(int gets, int puts, int misses,
                                     int listCalls, int numThreads,
                                     int64_t cpuTime = 0, int cpus = 0) {
    return ClientHealthStatsPtr(new ClientHealthStats(
        gets, puts, misses, listCalls, numThreads, cpuTime, cpus));
  }
  ~ClientHealthStats();

 private:
  ClientHealthStats(int gets, int puts, int misses, int listCalls,
                    int numThreads, int64_t cpuTime, int cpus);
  ClientHealthStats();

  int m_numGets;                // CachePerfStats.gets
  int m_numPuts;                // CachePerfStats.puts
  int m_numMisses;              // CachePerfStats.misses
  int m_numCacheListenerCalls;  // CachePerfStats.cacheListenerCallsCompleted
  int m_numThread;              // ProcessStats.threads;
  int64_t m_processCpuTime;     //
  int m_cpus;
  CacheableDatePtr m_updateTime;  // Last updateTime
};

}  // namespace gemfire
#endif  // _CLIENT_HEALTH_STATS_HPP_INCLUDED_
