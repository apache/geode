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
#include "Service.hpp"
#include "fwklib/FwkLog.hpp"

using namespace apache::geode::client;
using namespace apache::geode::client::testframework;

Service::Service(int32_t threadCnt)
    : m_ThreadCount(threadCnt), m_run(true), m_Mutex() {
  int32_t forceActive = 1;

  int32_t flags =
      THR_NEW_LWP | THR_JOINABLE | THR_CANCEL_ENABLE | THR_CANCEL_ASYNCHRONOUS;

#ifndef WIN32
  flags |= THR_INHERIT_SCHED;
#endif

  m_busy.resetValue(0);
  activate(flags, threadCnt, forceActive);
}

int32_t Service::svc() {
  while (m_run) {
    ServiceTask* task = getQ();
    if (task != NULL) {
      try {
        task->initialize();
        task->doTask();
        task->finalize();
      } catch (FwkException& ex) {
        FWKERROR("Service: Caught exception in svc: " << ex.getMessage());
      } catch (...) {
        FWKERROR("Service: Caught exception in svc.");
      }
      m_busy--;
    }
  }
  return 0;
}

int32_t Service::runThreaded(ServiceTask* task, uint32_t threads) {
  uint32_t avail = getIdleCount();
  if (threads > avail) {
    threads = avail;
  }
  task->setRunFlag(&m_run);
  putQ(task, threads);

  return threads;
}
