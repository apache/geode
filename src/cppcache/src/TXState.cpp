/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * TXState.cpp
 *
 *  Created on: 09-Feb-2011
 *      Author: ankurs
 */

#include "TXState.hpp"
#include "TransactionalOperation.hpp"
#include <gfcpp/Cache.hpp>
#include "TssConnectionWrapper.hpp"
#include "ThinClientPoolDM.hpp"

namespace gemfire {

TXState::TXState(Cache* cache) {
  m_txId = TXIdPtr(new TXId());
  m_closed = false;
  m_modSerialNum = 0;
  m_dirty = false;
  m_replay = false;
  m_prepared = false;
  m_cache = cache;
  /* adongre
   * Non-static class member "m_suspendedExpiryTaskId" is not initialized in
   * this constructor nor in any functions that it calls.
   */
  m_suspendedExpiryTaskId = 0;
  m_pooldm = NULL;
}

TXState::~TXState() {}

TXIdPtr TXState::getTransactionId() { return m_txId; }

int32_t TXState::nextModSerialNum() {
  m_modSerialNum += 1;
  return m_modSerialNum;
}

CacheablePtr TXState::replay(bool isRollback) {
  GfErrTypeThrowException("Replay is unsupported", GF_NOTSUP);
  int retryAttempts = 3;

  CacheablePtr result = NULLPTR;

  ReplayControl replayControl(this);
  m_dirty = false;
  // if retryAttempts < 0 we retry until there are no servers available
  for (int i = 0; (retryAttempts < 0) || (i < retryAttempts); i++) {
    // try {
    if (isRollback) {  // need to roll back these
      try {
        m_cache->getCacheTransactionManager()
            ->rollback();  // this.firstProxy.rollback(proxy.getTxId().getUniqId());
      } catch (const Exception& ex) {
        LOGFINE(
            "caught exception when rolling back before retrying transaction %s",
            ex.getMessage());
      }
    }
    m_txId = TXIdPtr(new TXId());
    // LOGFINE("retrying transaction after loss of state in server.  Attempt #"
    // + (i+1));
    try {
      for (VectorOfSharedBase::Iterator iter = m_operations.begin();
           m_operations.end() != iter; iter++) {
        TransactionalOperationPtr operation =
            staticCast<TransactionalOperationPtr>(*iter);
        result = operation->replay(m_cache);
      }

      return result;
    } catch (const TransactionDataNodeHasDepartedException& ex) {
      LOGDEBUG("Transaction exception:%s", ex.getMessage());
      isRollback = false;
      // try again
    } catch (const TransactionDataRebalancedException& ex) {
      LOGDEBUG("Transaction exception:%s", ex.getMessage());
      isRollback = true;
      // try again
    }
  }

  GfErrTypeThrowException(
      "Unable to reestablish transaction context on servers", GF_EUNDEF);

  return NULLPTR;
}

void TXState::releaseStickyConnection() {
  // Since this is called during cleanup or through destructor, we should not
  // throw exception from here,
  // which can cause undefined cleanup.
  TcrConnection* conn = TssConnectionWrapper::s_gemfireTSSConn->getConnection();
  if (conn != NULL) {
    ThinClientPoolDM* dm = conn->getEndpointObject()->getPoolHADM();
    if (dm != NULL) {
      if (!dm->isSticky()) {
        LOGFINE(
            "Release the sticky connection associated with the transaction");
        dm->releaseThreadLocalConnection();  // this will release connection now
      } else {
        LOGFINE(
            "Pool with sticky connection - don't Release the sticky connection "
            "associated with the transaction");
        conn->setAndGetBeingUsed(false, false);  // just release connection now
      }
    }
  }

  // ThinClientStickyManager manager(dm);
  // manager.releaseThreadLocalConnection();
}

void TXState::recordTXOperation(ServerRegionOperation op,
                                const char* regionName, CacheableKeyPtr key,
                                VectorOfCacheablePtr arguments) {
  m_operations.push_back(TransactionalOperationPtr(
      new TransactionalOperation(op, regionName, key, arguments)));
}
}  // namespace gemfire
