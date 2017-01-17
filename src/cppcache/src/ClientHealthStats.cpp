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
#include "ClientHealthStats.hpp"
#include "CacheImpl.hpp"

namespace gemfire {

void ClientHealthStats::toData(DataOutput& output) const {
  output.writeInt(static_cast<int32_t>(m_numGets));
  output.writeInt(static_cast<int32_t>(m_numPuts));
  output.writeInt(static_cast<int32_t>(m_numMisses));
  output.writeInt(static_cast<int32_t>(m_numCacheListenerCalls));
  output.writeInt(static_cast<int32_t>(m_numThread));
  output.writeInt(static_cast<int32_t>(m_cpus));
  output.writeInt(static_cast<int64_t>(m_processCpuTime));
  m_updateTime->toData(output);
}

Serializable* ClientHealthStats::fromData(DataInput& input) {
  input.readInt(&m_numGets);
  input.readInt(&m_numPuts);
  input.readInt(&m_numMisses);
  input.readInt(&m_numCacheListenerCalls);
  input.readInt(&m_numThread);
  input.readInt(&m_processCpuTime);
  input.readInt(&m_cpus);
  m_updateTime->fromData(input);
  return this;
}

Serializable* ClientHealthStats::createDeserializable() {
  return new ClientHealthStats();
}

int32_t ClientHealthStats::classId() const { return 0; }

int8_t ClientHealthStats::DSFID() const {
  return static_cast<int8_t>(GemfireTypeIdsImpl::FixedIDByte);
}
int8_t ClientHealthStats::typeId() const {
  return static_cast<int8_t>(GemfireTypeIdsImpl::ClientHealthStats);
}

ClientHealthStats::ClientHealthStats()
    : m_numGets(0),
      m_numPuts(0),
      m_numMisses(0),
      m_numCacheListenerCalls(0),
      m_numThread(0),
      m_processCpuTime(0),
      m_cpus(0) {
  m_updateTime = CacheableDate::create(ACE_OS::time(NULL));
}

ClientHealthStats::ClientHealthStats(int gets, int puts, int misses,
                                     int listCalls, int numThreads,
                                     int64_t cpuTime, int cpus)
    : m_numGets(gets),
      m_numPuts(puts),
      m_numMisses(misses),
      m_numCacheListenerCalls(listCalls),
      m_numThread(numThreads),
      m_processCpuTime(cpuTime),
      m_cpus(cpus) {
  m_updateTime = CacheableDate::create(ACE_OS::time(NULL));
}

ClientHealthStats::~ClientHealthStats() {}
}  // namespace gemfire
