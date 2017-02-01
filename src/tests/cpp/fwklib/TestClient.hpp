#pragma once

#ifndef APACHE_GEODE_GUARD_b1ca2169435caedbb3e0ccb04094fb46
#define APACHE_GEODE_GUARD_b1ca2169435caedbb3e0ccb04094fb46

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


#include "fwklib/PerfFwk.hpp"
#include "fwklib/ClientTask.hpp"

#include "ace/Task.h"

namespace apache {
namespace geode {
namespace client {
namespace testframework {

class TestClient : public ACE_Task_Base {
 private:
  static TestClient* m_instance;

  ACE_DLList<ClientTask> m_TaskQueue;
  int32_t m_ThreadCount;
  perf::Semaphore m_Flag;
  perf::Semaphore m_Ready;
  perf::Semaphore m_Run;
  perf::Semaphore m_Done;
  perf::Semaphore m_Clean;
  //    ACE_TSS<ACE_thread_t> m_MyId;
  ACE_Thread_Mutex m_Mutex;
  int64_t m_TotalMicros;
  int32_t m_taskStatus;

  int32_t svc();

  int32_t runTask(ClientTask* task, ACE_thread_t id);

  inline void putQ(ClientTask* task, int32_t cnt = 1) {
    ACE_Guard<ACE_Thread_Mutex> guard(m_Mutex);
    for (int32_t i = 0; i < cnt; i++) m_TaskQueue.insert_tail(task);
    m_Flag.release(cnt);
  }

  inline ClientTask* getQ() {
    m_Flag.acquire(1);
    ACE_Guard<ACE_Thread_Mutex> guard(m_Mutex);
    return m_TaskQueue.delete_head();
  }

  inline ACE_Time_Value* getUntil(uint32_t seconds) {
    ACE_Time_Value* until = 0;
    if (seconds != 0) {
      until = new ACE_Time_Value(seconds);
      *until += ACE_OS::gettimeofday();
    }
    return until;
  }

  TestClient(int32_t threadCnt, int32_t id);

  inline ~TestClient() {
    stopThreads();
    wait();
  }

 public:
  static TestClient* createTestClient(int32_t threadCnt, int32_t id);
  static TestClient* getTestClient();
  void destroyTestClient();

  inline int64_t getTotalMicros() { return m_TotalMicros; }

  int32_t runThreaded(ClientTask* task, int32_t threads);
  int32_t getTaskStatus() { return m_taskStatus; }

  bool runIterations(ClientTask* task, uint32_t iters, int32_t threads,
                     uint32_t maxMillis);

  bool timeIterations(ClientTask* task, uint32_t iters, int32_t threads,
                      uint32_t maxMillis);

  bool runInterval(ClientTask* task, uint32_t seconds, int32_t threads,
                   uint32_t maxMillis);

  bool timeInterval(ClientTask* task, uint32_t seconds, int32_t threads,
                    uint32_t maxSeconds);

  bool timeMillisInterval(ClientTask* task, uint32_t millis, int32_t threads,
                          uint32_t maxSeconds);

  inline ACE_thread_t getId() {
    return ACE_Thread::self() /**m_MyId.ts_object()*/;
  }
  inline int32_t getThreadCount() { return m_ThreadCount; }

  inline void stopThreads() {
    ExitTask e;
    putQ(&e, m_ThreadCount);
    wait();
  }
};

}  // namespace testframework
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // APACHE_GEODE_GUARD_b1ca2169435caedbb3e0ccb04094fb46
