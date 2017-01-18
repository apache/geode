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
#include "TestClient.hpp"
#include "fwklib/Timer.hpp"
#include "fwklib/FwkLog.hpp"

using namespace apache::geode::client;
using namespace apache::geode::client::testframework;
using namespace apache::geode::client::testframework::perf;

TestClient* TestClient::m_instance = NULL;

TestClient* TestClient::createTestClient(int32_t threadCnt, int32_t id) {
  if (m_instance == NULL) {
    m_instance = new TestClient(threadCnt, id);
  }
  return m_instance;
}

TestClient* TestClient::getTestClient() {
  if (m_instance == NULL) {
    FWKEXCEPTION(
        "TestClient is NULL, expected it to be initialized in Client.");
  }
  return m_instance;
}

void TestClient::destroyTestClient() {
  if (m_instance != NULL) {
    delete m_instance;
    m_instance = NULL;
  }
}

TestClient::TestClient(int32_t threadCnt, int32_t id)
    : m_ThreadCount(threadCnt),
      m_Flag(0),
      m_Ready(0),
      m_Run(0),
      m_Done(0),
      m_Clean(0),
      m_TotalMicros(0) {
  int32_t forceActive = 1;

  int32_t flags =
      THR_NEW_LWP | THR_JOINABLE | THR_CANCEL_ENABLE | THR_CANCEL_ASYNCHRONOUS;

#ifndef WIN32
  flags |= THR_INHERIT_SCHED;
#endif

  activate(flags, threadCnt, forceActive);
}

enum States { ENTERED, READY, DONE, CLEAN };
int32_t TestClient::runTask(ClientTask* task, ACE_thread_t id) {
  int32_t fwkResult = FWK_SUCCESS;
  States state = ENTERED;
  bool cleanup = false;
  try {
    if (!task->doSetup(id)) {  // problem during setup
      FWKSEVERE("Problem during task setup");
      return FWK_SEVERE;
    }
    m_Ready.release();
    state = READY;
    m_Run.acquire();
    int32_t iters = task->doTask(id);
    task->addIters(iters);
    m_Done.release();
    state = DONE;
    task->doCleanup(id);
    m_Clean.release();
    state = CLEAN;
  } catch (FwkException& ex) {
    cleanup = true;
    FWKSEVERE(
        "FwkException caught in TestClient::runTask: " << ex.getMessage());
    fwkResult = FWK_SEVERE;
  } catch (Exception& ex) {
    cleanup = true;
    FWKSEVERE("Exception caught in TestClient::runTask: " << ex.getMessage());
    fwkResult = FWK_SEVERE;
  } catch (std::exception& ex) {
    cleanup = true;
    FWKSEVERE("std::exception caught in TestClient::runTask: " << ex.what());
    fwkResult = FWK_SEVERE;
  } catch (...) {
    cleanup = true;
    FWKSEVERE("Unknown exception caught in TestClient::runTask");
    fwkResult = FWK_SEVERE;
  }
  if (cleanup) {
    switch (state) {
      case ENTERED:
        m_Ready.release();
      // and fall thru
      case READY:
        m_Done.release();
      // and fall thru
      case DONE:
        m_Clean.release();
        break;
      case CLEAN:  // do nothing
      default:
        break;
    }
  }
  return fwkResult;
}

int32_t TestClient::svc() {
  bool done = false;
  ACE_thread_t id = ACE_Thread::self();
  //  m_MyId->set( id );
  int32_t runTaskStatus;
  while (!done) {
    ClientTask* task = getQ();
    if (task != 0) {
      if (task->mustExit()) {
        done = true;
      } else {
        try {
          runTaskStatus = runTask(task, id);
          if (runTaskStatus == FWK_SEVERE) m_taskStatus = runTaskStatus;
        } catch (...) {
          FWKINFO("Caught exception in svc.");
        }
      }
    }
  }
  return 0;
}

int32_t TestClient::runThreaded(ClientTask* task, int32_t threads) {
  task->initTask();
  if (threads > getThreadCount()) {
    threads = getThreadCount();
  }
  putQ(task, threads);
  m_Ready.acquire(threads);
  m_Run.release(threads);
  m_Done.acquire(threads);
  m_Clean.acquire(threads);
  return task->getPassCount();
}

bool TestClient::runIterations(ClientTask* task, uint32_t iters,
                               int32_t threads, uint32_t maxSecs) {
  task->initTask();
  if (threads > getThreadCount()) {
    threads = getThreadCount();
  }
  ACE_Time_Value* until = getUntil(maxSecs);

  task->setIterations(iters);
  putQ(task, threads);
  if (!m_Ready.acquire(until, threads)) return false;
  m_Run.release(threads);
  if (!m_Done.acquire(until, threads)) {
    task->endRun();
    return false;
  }
  m_TotalMicros = -1;
  return m_Clean.acquire(until, threads);
}

bool TestClient::timeIterations(ClientTask* task, uint32_t iters,
                                int32_t threads, uint32_t maxSecs) {
  task->initTask();
  if (threads > getThreadCount()) {
    threads = getThreadCount();
  }
  ACE_Time_Value* until = getUntil(maxSecs);

  task->setIterations(iters);
  putQ(task, threads);
  if (!m_Ready.acquire(until, threads)) return false;
  m_Run.release(threads);
  HRTimer timer;
  if (!m_Done.acquire(until, threads)) {
    task->endRun();
    return false;
  }
  m_TotalMicros = timer.elapsedMicros();
  return m_Clean.acquire(until, threads);
}

bool TestClient::runInterval(ClientTask* task, uint32_t seconds,
                             int32_t threads, uint32_t maxSecs) {
  task->initTask();
  if (threads > getThreadCount()) {
    threads = getThreadCount();
  }
  ACE_Time_Value* until = getUntil(maxSecs);

  task->setIterations(0);
  putQ(task, threads);
  if (!m_Ready.acquire(until, threads)) return false;
  m_Run.release(threads);
  perf::sleepSeconds(seconds);
  task->endRun();
  if (!m_Done.acquire(until, threads)) return false;
  m_TotalMicros = -1;
  return m_Clean.acquire(until, threads);
}

bool TestClient::timeInterval(ClientTask* task, uint32_t seconds,
                              int32_t threads, uint32_t maxSecs) {
  task->initTask();
  if (threads > getThreadCount()) {
    threads = getThreadCount();
  }
  ACE_Time_Value* until = getUntil(maxSecs);

  task->setIterations(0);
  putQ(task, threads);
  if (!m_Ready.acquire(until, threads)) return false;
  m_Run.release(threads);
  HRTimer timer;
  perf::sleepSeconds(seconds);
  until = getUntil(maxSecs);
  task->endRun();
  if (!m_Done.acquire(until, threads)) return false;
  m_TotalMicros = timer.elapsedMicros();
  return m_Clean.acquire(until, threads);
}

bool TestClient::timeMillisInterval(ClientTask* task, uint32_t millis,
                                    int32_t threads, uint32_t maxSecs) {
  task->initTask();
  if (threads > getThreadCount()) {
    threads = getThreadCount();
  }
  ACE_Time_Value* until = getUntil(maxSecs);

  task->setIterations(0);
  putQ(task, threads);
  if (!m_Ready.acquire(until, threads)) return false;
  m_Run.release(threads);
  HRTimer timer;
  perf::sleepMillis(millis);
  until = getUntil(maxSecs);
  task->endRun();
  if (!m_Done.acquire(until, threads)) return false;
  m_TotalMicros = timer.elapsedMicros();
  return m_Clean.acquire(until, threads);
}
