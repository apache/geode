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
#include "EvictionThread.hpp"
#include "EvictionController.hpp"
#include "DistributedSystemImpl.hpp"
using namespace apache::geode::client;

const char* EvictionThread::NC_Evic_Thread = "NC Evic Thread";
EvictionThread::EvictionThread(EvictionController* parent)
    : m_pParent(parent),
      /* adongre
       * CID 28936: Uninitialized scalar field (UNINIT_CTOR)
       */
      m_run(false) {}

int EvictionThread::svc(void) {
  DistributedSystemImpl::setThreadName(NC_Evic_Thread);
  while (m_run) {
    processEvictions();
  }
  int32_t size = m_queue.size();
  for (int i = 0; i < size; i++) {
    processEvictions();
  }
  return 1;
}

void EvictionThread::processEvictions() {
  int32_t percentageToEvict = 0;
  percentageToEvict = static_cast<int32_t>(m_queue.get(1500));
  if (percentageToEvict != 0) {
    m_pParent->evict(percentageToEvict);
  }
}

void EvictionThread::putEvictionInfo(int32_t info) { m_queue.put(info); }
