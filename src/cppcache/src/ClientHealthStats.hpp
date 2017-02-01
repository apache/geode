#pragma once

#ifndef GEODE_CLIENTHEALTHSTATS_H_
#define GEODE_CLIENTHEALTHSTATS_H_

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

#include <gfcpp/gf_types.hpp>
#include <gfcpp/Serializable.hpp>
#include <gfcpp/Log.hpp>
#include <gfcpp/CacheableDate.hpp>

namespace apache {
namespace geode {
namespace client {

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
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_CLIENTHEALTHSTATS_H_
