/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
* All Rights Reserved.
*=========================================================================
*/

#ifndef __TestClient_hpp__
#define __TestClient_hpp__

#include "fwklib/PerfFwk.hpp"
#include "fwklib/ClientTask.hpp"

#include "ace/Task.h"

namespace gemfire {
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

}  // testframework
}  // gemfire
#endif  // __TestClient_hpp__
