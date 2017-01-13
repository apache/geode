/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "ace/Timer_Queue.h"
#include "ace/Timer_Heap.h"
#include "ace/Reactor.h"
#include "ace/svc_export.h"
#include "ace/Timer_Heap_T.h"
#include "ace/Timer_Queue_Adapters.h"

#include "CacheImpl.hpp"
#include "ExpiryTaskManager.hpp"
#include "SuspendedTxExpiryHandler.hpp"

using namespace gemfire;

SuspendedTxExpiryHandler::SuspendedTxExpiryHandler(
    CacheTransactionManagerImpl* cacheTxMgr, TransactionIdPtr tid,
    uint32_t duration)
    :  // UNUSED m_duration(duration),
      m_cacheTxMgr(cacheTxMgr),
      m_txid(tid) {}

int SuspendedTxExpiryHandler::handle_timeout(const ACE_Time_Value& current_time,
                                             const void* arg) {
  LOGDEBUG("Entered SuspendedTxExpiryHandler");
  try {
    // resume the transaction and rollback it
    if (m_cacheTxMgr->tryResume(m_txid, false)) m_cacheTxMgr->rollback();
  } catch (...) {
    // Ignore whatever exception comes
    LOGFINE(
        "Error while rollbacking expired suspended transaction. Ignoring the "
        "error");
  }
  return 0;
}

int SuspendedTxExpiryHandler::handle_close(ACE_HANDLE, ACE_Reactor_Mask) {
  return 0;
}
