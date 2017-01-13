/*=========================================================================
 * Copyright (c) 2010-2015 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * CacheTransactionManager2PCImpl.cpp
 *
 *  Created on: 13-Nov-2015
 *      Author: sshcherbakov
 */

#include <gfcpp/gf_types.hpp>
#include "InternalCacheTransactionManager2PCImpl.hpp"
#include "CacheTransactionManagerImpl.hpp"
#include <gfcpp/TransactionId.hpp>
#include <gfcpp/ExceptionTypes.hpp>
#include "TcrMessage.hpp"
#include "ThinClientPoolDM.hpp"
#include "CacheRegionHelper.hpp"
#include <gfcpp/PoolManager.hpp>
#include "TXCleaner.hpp"

namespace gemfire {

InternalCacheTransactionManager2PCImpl::InternalCacheTransactionManager2PCImpl(
    Cache* cache)
    : CacheTransactionManagerImpl(cache) {}

InternalCacheTransactionManager2PCImpl::
    ~InternalCacheTransactionManager2PCImpl() {}

void InternalCacheTransactionManager2PCImpl::prepare() {
  try {
    TSSTXStateWrapper* txStateWrapper = TSSTXStateWrapper::s_gemfireTSSTXState;
    TXState* txState = txStateWrapper->getTXState();

    if (txState == NULL) {
      GfErrTypeThrowException(
          "Transaction is null, cannot prepare of a null transaction",
          GF_CACHE_ILLEGAL_STATE_EXCEPTION);
    }

    ThinClientPoolDM* tcr_dm = getDM();
    // This is for the case when no cache operation/s is performed between
    // tx->begin() and tx->commit()/rollback(),
    // simply return without sending COMMIT message to server. tcr_dm is NULL
    // implies no cache operation is performed.
    // Theres no need to call txCleaner.clean(); here, because TXCleaner
    // destructor is called which cleans ThreadLocal.
    if (tcr_dm == NULL) {
      return;
    }

    TcrMessageTxSynchronization requestCommitBefore(
        BEFORE_COMMIT, txState->getTransactionId()->getId(), STATUS_COMMITTED);

    TcrMessageReply replyCommitBefore(true, NULL);
    GfErrType err =
        tcr_dm->sendSyncRequest(requestCommitBefore, replyCommitBefore);
    if (err != GF_NOERR) {
      GfErrTypeThrowException("Error while prepare", err);
    } else {
      switch (replyCommitBefore.getMessageType()) {
        case TcrMessage::REPLY:
          txState->setPrepared();
          break;
        case TcrMessage::EXCEPTION: {
          TXCleaner txCleaner(this);
          const char* exceptionMsg = replyCommitBefore.getException();
          err = ThinClientRegion::handleServerException(
              "CacheTransactionManager::prepare", exceptionMsg);
          GfErrTypeThrowException("Commit Failed in prepare", err);
          break;
        }
        case TcrMessage::REQUEST_DATA_ERROR: {
          TXCleaner txCleaner(this);
          GfErrTypeThrowException("Commit Failed in prepare",
                                  GF_COMMIT_CONFLICT_EXCEPTION);
          break;
        }
        default: {
          TXCleaner txCleaner(this);
          LOGERROR("Unknown message type in prepare reply %d",
                   replyCommitBefore.getMessageType());
          GfErrTypeThrowException("Commit Failed in prepare", GF_MSG);
          break;
        }
      }
    }
  } catch (const Exception& ex) {
    LOGERROR("Unexpected exception during commit in prepare %s",
             ex.getMessage());
    throw ex;
  }
}

void InternalCacheTransactionManager2PCImpl::commit() {
  LOGFINEST("Committing");
  this->afterCompletion(STATUS_COMMITTED);
}

void InternalCacheTransactionManager2PCImpl::rollback() {
  LOGFINEST("Rolling back");
  this->afterCompletion(STATUS_ROLLEDBACK);
}

void InternalCacheTransactionManager2PCImpl::afterCompletion(int32_t status) {
  try {
    TSSTXStateWrapper* txStateWrapper = TSSTXStateWrapper::s_gemfireTSSTXState;
    TXState* txState = txStateWrapper->getTXState();

    if (txState == NULL) {
      GfErrTypeThrowException(
          "Transaction is null, cannot commit a null transaction",
          GF_CACHE_ILLEGAL_STATE_EXCEPTION);
    }

    ThinClientPoolDM* tcr_dm = getDM();
    // This is for the case when no cache operation/s is performed between
    // tx->begin() and tx->commit()/rollback(),
    // simply return without sending COMMIT message to server. tcr_dm is NULL
    // implies no cache operation is performed.
    // Theres no need to call txCleaner.clean(); here, because TXCleaner
    // destructor is called which cleans ThreadLocal.
    if (tcr_dm == NULL) {
      return;
    }

    if (!txState->isPrepared()) {
      // Fallback to deafult 1PC commit
      // The inherited 1PC implmentation clears the transaction state
      switch (status) {
        case STATUS_COMMITTED:
          CacheTransactionManagerImpl::commit();
          break;
        case STATUS_ROLLEDBACK:
          CacheTransactionManagerImpl::rollback();
          break;
        default:
          GfErrTypeThrowException("Unknown command",
                                  GF_CACHE_ILLEGAL_STATE_EXCEPTION);
      }
      return;
    }

    // In 2PC we always clear the transaction state
    TXCleaner txCleaner(this);

    TcrMessageTxSynchronization requestCommitAfter(
        AFTER_COMMIT, txState->getTransactionId()->getId(), status);

    TcrMessageReply replyCommitAfter(true, NULL);
    GfErrType err =
        tcr_dm->sendSyncRequest(requestCommitAfter, replyCommitAfter);

    if (err != GF_NOERR) {
      GfErrTypeThrowException("Error in 2PC commit", err);
    } else {
      switch (replyCommitAfter.getMessageType()) {
        case TcrMessage::RESPONSE: {
          TXCommitMessagePtr commit =
              staticCast<TXCommitMessagePtr>(replyCommitAfter.getValue());
          if (commit.ptr() !=
              NULL)  // e.g. when afterCompletion(STATUS_ROLLEDBACK) called
          {
            txCleaner.clean();
            commit->apply(this->getCache());
          }
          break;
        }
        case TcrMessage::EXCEPTION: {
          const char* exceptionMsg = replyCommitAfter.getException();
          err = ThinClientRegion::handleServerException(
              "CacheTransactionManager::afterCompletion", exceptionMsg);
          GfErrTypeThrowException("2PC Commit Failed", err);
          break;
        }
        case TcrMessage::REQUEST_DATA_ERROR:
          GfErrTypeThrowException("2PC Commit Failed",
                                  GF_COMMIT_CONFLICT_EXCEPTION);
          break;
        default:
          GfErrTypeThrowException("2PC Commit Failed", GF_MSG);
          break;
      }
    }
  } catch (const Exception& ex) {
    LOGERROR("Unexpected exception during completing transaction %s",
             ex.getMessage());
    throw ex;
  }
}
}  // namespace gemfire
