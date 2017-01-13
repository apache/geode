/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "EvictionThread.hpp"
#include "EvictionController.hpp"
#include "DistributedSystemImpl.hpp"
using namespace gemfire;

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
