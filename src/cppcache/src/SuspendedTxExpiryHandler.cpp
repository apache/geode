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
#include "ace/Timer_Queue.h"
#include "ace/Timer_Heap.h"
#include "ace/Reactor.h"
#include "ace/svc_export.h"
#include "ace/Timer_Heap_T.h"
#include "ace/Timer_Queue_Adapters.h"

#include "CacheImpl.hpp"
#include "ExpiryTaskManager.hpp"
#include "SuspendedTxExpiryHandler.hpp"

using namespace apache::geode::client;

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
