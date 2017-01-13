/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
* All Rights Reserved.
*=========================================================================
*/
#include "Service.hpp"
#include "fwklib/FwkLog.hpp"

using namespace gemfire;
using namespace gemfire::testframework;

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
