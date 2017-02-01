#pragma once

#ifndef GEODE_EVICTIONTHREAD_H_
#define GEODE_EVICTIONTHREAD_H_

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

#include <ace/ACE.h>
#include <ace/OS.h>
#include <ace/Singleton.h>
#include <ace/Thread_Mutex.h>
#include <ace/Task.h>
#include <gfcpp/DataOutput.hpp>
#include <gfcpp/Log.hpp>
#include "IntQueue.hpp"
/**
 * This class does the actual evictions
 */
namespace apache {
namespace geode {
namespace client {
class EvictionController;
typedef IntQueue<int64_t> HeapSizeInfoQueue;

class CPPCACHE_EXPORT EvictionThread : public ACE_Task_Base {
 public:
  EvictionThread(EvictionController* parent);

  inline void start() {
    m_run = true;
    this->activate();
    LOGFINE("Eviction Thread started");
  }

  inline void stop() {
    m_run = false;
    this->wait();
    m_queue.clear();
    LOGFINE("Eviction Thread stopped");
  }

  int svc();
  void putEvictionInfo(int32_t info);
  void processEvictions();

 private:
  EvictionController* m_pParent;
  HeapSizeInfoQueue m_queue;
  bool m_run;

  static const char* NC_Evic_Thread;
};
}  // namespace client
}  // namespace geode
}  // namespace apache


#endif // GEODE_EVICTIONTHREAD_H_
