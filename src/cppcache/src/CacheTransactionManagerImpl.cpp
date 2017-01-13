/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * CacheTransactionManagerImpl.cpp
 *
 *  Created on: 04-Feb-2011
 *      Author: ankurs
 */

#include <gfcpp/gf_types.hpp>
#include "CacheTransactionManagerImpl.hpp"
#include <gfcpp/TransactionId.hpp>
#include <gfcpp/ExceptionTypes.hpp>
#include "TSSTXStateWrapper.hpp"
#include "TcrMessage.hpp"
#include "ThinClientBaseDM.hpp"
#include "ThinClientPoolDM.hpp"
#include "CacheRegionHelper.hpp"
#include "TssConnectionWrapper.hpp"
#include <gfcpp/PoolManager.hpp>
#include "TXCleaner.hpp"

namespace gemfire {

CacheTransactionManagerImpl::CacheTransactionManagerImpl(Cache* cache)
    : m_cache(cache), m_txCond(m_suspendedTxLock) {}

CacheTransactionManagerImpl::~CacheTransactionManagerImpl() {}

void CacheTransactionManagerImpl::begin() {
  if (TSSTXStateWrapper::s_gemfireTSSTXState->getTXState() != NULL) {
    GfErrTypeThrowException("Transaction already in progress",
                            GF_CACHE_ILLEGAL_STATE_EXCEPTION);
  }
  TXState* txState = new TXState(m_cache);
  TSSTXStateWrapper::s_gemfireTSSTXState->setTXState(txState);
  addTx(txState->getTransactionId()->getId());
}

void CacheTransactionManagerImpl::commit() {
  TXCleaner txCleaner(this);
  TXState* txState = txCleaner.getTXState();

  if (txState == NULL) {
    GfErrTypeThrowException(
        "Transaction is null, cannot commit a null transaction",
        GF_CACHE_ILLEGAL_STATE_EXCEPTION);
  }

  TcrMessageCommit request;
  TcrMessageReply reply(true, NULL);

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

  GfErrType err = tcr_dm->sendSyncRequest(request, reply);

  if (err != GF_NOERR) {
    // err = rollback(txState, false);
    //		noteCommitFailure(txState, NULLPTR);
    GfErrTypeThrowException("Error while commiting", err);
  } else {
    switch (reply.getMessageType()) {
      case TcrMessage::RESPONSE: {
        break;
      }
      case TcrMessage::EXCEPTION: {
        //			noteCommitFailure(txState, NULLPTR);
        const char* exceptionMsg = reply.getException();
        err = ThinClientRegion::handleServerException(
            "CacheTransactionManager::commit", exceptionMsg);
        GfErrTypeThrowException("Commit Failed", err);
        break;
      }
      case TcrMessage::COMMIT_ERROR: {
        //			noteCommitFailure(txState, NULLPTR);
        GfErrTypeThrowException("Commit Failed", GF_COMMIT_CONFLICT_EXCEPTION);
        break;
      }
      default: {
        //			noteCommitFailure(txState, NULLPTR);
        LOGERROR("Unknown message type in commit reply %d",
                 reply.getMessageType());
        GfErrTypeThrowException("Commit Failed", GF_MSG);
        break;
      }
    }
  }

  TXCommitMessagePtr commit = staticCast<TXCommitMessagePtr>(reply.getValue());
  txCleaner.clean();
  commit->apply(m_cache);

  /*
          if(m_writer != NULLPTR)
          {
                  try
                  {
                          TransactionEventPtr event(new
     TransactionEvent(txState->getTransactionId(), CachePtr(m_cache),
     commit->getEvents(m_cache)));
                          m_writer->beforeCommit(event);
                  } catch(const TransactionWriterException& ex)
                  {
                          noteCommitFailure(txState, commit);
                          GfErrTypeThrowException(ex.getMessage(),
     GF_COMMIT_CONFLICT_EXCEPTION);
                  }
                  catch (const Exception& ex)
                  {
                          noteCommitFailure(txState, commit);
                          LOGERROR("Unexpected exception during writer callback
     %s", ex.getMessage());
                          throw ex;
                  }
                  catch (...)
                  {
                          noteCommitFailure(txState, commit);
                          LOGERROR("Unexpected exception during writer
     callback");
                          throw;
                  }
          }
  */

  /*try
  {
          TcrMessage requestCommitBefore(TcrMessage::TX_SYNCHRONIZATION,
BEFORE_COMMIT, txState->getTransactionId()->getId(), STATUS_COMMITTED);
          TcrMessage replyCommitBefore;
          err = tcr_dm->sendSyncRequest(requestCommitBefore, replyCommitBefore);
          if(err != GF_NOERR)
          {
                  GfErrTypeThrowException("Error while committing", err);
          } else {
                  switch (replyCommitBefore.getMessageType()) {
                  case TcrMessage::REPLY: {
                          break;
                  }
                  case TcrMessage::EXCEPTION: {
                          const char* exceptionMsg =
replyCommitBefore.getException();
                                                        err =
ThinClientRegion::handleServerException("CacheTransactionManager::commit",
                                                            exceptionMsg);
                                                        GfErrTypeThrowException("Commit
Failed", err);
                          break;
                  }
                  case TcrMessage::REQUEST_DATA_ERROR: {
                          GfErrTypeThrowException("Commit Failed",
GF_COMMIT_CONFLICT_EXCEPTION);
                          break;
                  }
                  default: {
                          LOGERROR("Unknown message type in commit reply %d",
reply.getMessageType());
                                                  GfErrTypeThrowException("Commit
Failed", GF_MSG);
                          break;
                  }
                  }
          }
  }
  catch (const Exception& ex)
  {
//		noteCommitFailure(txState, commit);
          LOGERROR("Unexpected exception during commit %s", ex.getMessage());
          throw ex;
  }
  catch (...)
  {
//		noteCommitFailure(txState, commit);
          LOGERROR("Unexpected exception during writer callback");
          throw;
  }

  try{
          TcrMessage requestCommitAfter(TcrMessage::TX_SYNCHRONIZATION,
AFTER_COMMIT, txState->getTransactionId()->getId(), STATUS_COMMITTED);
          TcrMessage replyCommitAfter;
          txCleaner.clean();
          commit->apply(m_cache);
          err = tcr_dm->sendSyncRequest(requestCommitAfter, replyCommitAfter);

          if(err != GF_NOERR)
          {
                  GfErrTypeThrowException("Error while committing", err);
          } else {
                  switch (replyCommitAfter.getMessageType()) {
                  case TcrMessage::RESPONSE: {
                          //commit = replyCommitAfter.getValue();
                          break;
                  }
                  case TcrMessage::EXCEPTION: {
                          const char* exceptionMsg =
replyCommitAfter.getException();
                                                                                        err = ThinClientRegion::handleServerException("CacheTransactionManager::commit",
                                                                                            exceptionMsg);
                                                                                        GfErrTypeThrowException("Commit Failed", err);
                          break;
                  }
                  case TcrMessage::REQUEST_DATA_ERROR: {
                          GfErrTypeThrowException("Commit Failed",
GF_COMMIT_CONFLICT_EXCEPTION);
                          break;
                  }
                  default: {
                          GfErrTypeThrowException("Commit Failed", GF_MSG);
                          break;
                  }
                  }
          }
  }
  catch (const Exception& ex)
  {
//		noteCommitFailure(txState, commit);
          throw ex;
  }
  catch (...)
  {
//		noteCommitFailure(txState, commit);
          throw;
  }*/

  //	noteCommitSuccess(txState, commit);
}

void CacheTransactionManagerImpl::rollback() {
  TXCleaner txCleaner(this);
  TXState* txState = txCleaner.getTXState();

  if (txState == NULL) {
    GfErrTypeThrowException("Thread does not have an active transaction",
                            GF_CACHE_ILLEGAL_STATE_EXCEPTION);
  }

  try {
    GfErrType err = rollback(txState, true);
    if (err != GF_NOERR) {
      GfErrTypeToException("Error while commiting", err);
    }
  } catch (const Exception& ex) {
    // TODO: put a log message
    throw ex;
  } catch (...) {
    // TODO: put a log message
    throw;
  }
}

GfErrType CacheTransactionManagerImpl::rollback(TXState* txState,
                                                bool callListener) {
  TcrMessageRollback request;
  TcrMessageReply reply(true, NULL);
  GfErrType err = GF_NOERR;
  ThinClientPoolDM* tcr_dm = getDM();
  // This is for the case when no cache operation/s is performed between
  // tx->begin() and tx->commit()/rollback(),
  // simply return without sending COMMIT message to server. tcr_dm is NULL
  // implies no cache operation is performed.
  // Theres no need to call txCleaner.clean(); here, because TXCleaner
  // destructor is called which cleans ThreadLocal.
  if (tcr_dm == NULL) {
    return err;
  }
  err = tcr_dm->sendSyncRequest(request, reply);

  if (err == GF_NOERR) {
    switch (reply.getMessageType()) {
      case TcrMessage::REPLY: {
        break;
      }
      case TcrMessage::EXCEPTION: {
        break;
      }
      default: { break; }
    }
  }

  /*	if(err == GF_NOERR && callListener)
          {
  //		TXCommitMessagePtr commit =
  staticCast<TXCommitMessagePtr>(reply.getValue());
                  noteRollbackSuccess(txState, NULLPTR);
          }
  */
  return err;
}

ThinClientPoolDM* CacheTransactionManagerImpl::getDM() {
  TcrConnection* conn = TssConnectionWrapper::s_gemfireTSSConn->getConnection();
  if (conn != NULL) {
    ThinClientPoolDM* dm = conn->getEndpointObject()->getPoolHADM();
    if (dm != NULL) {
      return dm;
    }
  }
  return NULL;
}

Cache* CacheTransactionManagerImpl::getCache() { return m_cache; }

TransactionIdPtr CacheTransactionManagerImpl::suspend() {
  // get the current state of the thread
  TXState* txState = TSSTXStateWrapper::s_gemfireTSSTXState->getTXState();
  if (txState == NULL) {
    LOGFINE("Transaction not in progress. Returning NULL transaction Id.");
    return NULLPTR;
  }

  // get the current connection that this transaction is using
  TcrConnection* conn = TssConnectionWrapper::s_gemfireTSSConn->getConnection();
  if (conn == NULL) {
    LOGFINE("Thread local connection is null. Returning NULL transaction Id.");
    return NULLPTR;
  }

  // get the endpoint info from the connection
  TcrEndpoint* ep = conn->getEndpointObject();

  // store the endpoint info and the pool DM in the transaction state
  // this function setEPStr and setPoolDM is used only while suspending
  // the transaction. The info stored here is used during resume.
  txState->setEPStr(ep->name());
  txState->setPoolDM(ep->getPoolHADM());

  LOGFINE(
      "suspended Release the sticky connection associated with the "
      "transaction");
  txState->releaseStickyConnection();

  // set the expiry handler for the suspended transaction
  SystemProperties* sysProp = DistributedSystem::getSystemProperties();
  SuspendedTxExpiryHandler* handler = new SuspendedTxExpiryHandler(
      this, txState->getTransactionId(), sysProp->suspendedTxTimeout());
  long id = CacheImpl::expiryTaskManager->scheduleExpiryTask(
      handler, sysProp->suspendedTxTimeout() * 60, 0, false);
  txState->setSuspendedExpiryTaskId(id);

  // add the transaction state to the list of suspended transactions
  addSuspendedTx(txState->getTransactionId()->getId(), txState);

  // set the current transaction state as null
  TSSTXStateWrapper::s_gemfireTSSTXState->setTXState(NULL);

  // return the transaction ID
  return txState->getTransactionId();
}

void CacheTransactionManagerImpl::resume(TransactionIdPtr transactionId) {
  // get the current state of the thread
  if (TSSTXStateWrapper::s_gemfireTSSTXState->getTXState() != NULL) {
    GfErrTypeThrowException("A transaction is already in progress",
                            GF_CACHE_ILLEGAL_STATE_EXCEPTION);
  }

  // get the transaction state of the suspended transaction
  TXState* txState =
      removeSuspendedTx((static_cast<TXIdPtr>(transactionId))->getId());
  if (txState == NULL) {
    GfErrTypeThrowException(
        "Could not get transaction state for the transaction id.",
        GF_CACHE_ILLEGAL_STATE_EXCEPTION);
  }

  resumeTxUsingTxState(txState);
}

bool CacheTransactionManagerImpl::isSuspended(TransactionIdPtr transactionId) {
  return isSuspendedTx((static_cast<TXIdPtr>(transactionId))->getId());
}
bool CacheTransactionManagerImpl::tryResume(TransactionIdPtr transactionId) {
  return tryResume(transactionId, true);
}
bool CacheTransactionManagerImpl::tryResume(TransactionIdPtr transactionId,
                                            bool cancelExpiryTask) {
  // get the current state of the thread
  if (TSSTXStateWrapper::s_gemfireTSSTXState->getTXState() != NULL) {
    LOGFINE("A transaction is already in progress. Cannot resume transaction.");
    return false;
  }

  // get the transaction state of the suspended transaction
  TXState* txState =
      removeSuspendedTx((static_cast<TXIdPtr>(transactionId))->getId());
  if (txState == NULL) return false;

  resumeTxUsingTxState(txState, cancelExpiryTask);
  return true;
}

bool CacheTransactionManagerImpl::tryResume(TransactionIdPtr transactionId,
                                            int32_t waitTimeInMillisec) {
  // get the current state of the thread
  if (TSSTXStateWrapper::s_gemfireTSSTXState->getTXState() != NULL) {
    LOGFINE("A transaction is already in progress. Cannot resume transaction.");
    return false;
  }

  if (!exists(transactionId)) return false;

  // get the transaction state of the suspended transaction
  TXState* txState = removeSuspendedTxUntil(
      (static_cast<TXIdPtr>(transactionId))->getId(), waitTimeInMillisec);
  if (txState == NULL) return false;

  resumeTxUsingTxState(txState);
  return true;
}

void CacheTransactionManagerImpl::resumeTxUsingTxState(TXState* txState,
                                                       bool cancelExpiryTask) {
  if (txState == NULL) return;

  TcrConnection* conn;

  LOGDEBUG("Resuming transaction for tid: %d",
           txState->getTransactionId()->getId());

  if (cancelExpiryTask) {
    // cancel the expiry task for the transaction
    CacheImpl::expiryTaskManager->cancelTask(
        txState->getSuspendedExpiryTaskId());
  } else {
    CacheImpl::expiryTaskManager->resetTask(txState->getSuspendedExpiryTaskId(),
                                            0);
  }

  // set the current state as the state of the suspended transaction
  TSSTXStateWrapper::s_gemfireTSSTXState->setTXState(txState);

  LOGFINE("Get connection for transaction id %d",
          txState->getTransactionId()->getId());
  // get connection to the endpoint specified in the transaction state
  GfErrType error = txState->getPoolDM()->getConnectionToAnEndPoint(
      txState->getEPStr(), conn);
  if (conn == NULL || error != GF_NOERR) {
    // throw an exception and set the current state as NULL because
    // the transaction cannot be resumed
    TSSTXStateWrapper::s_gemfireTSSTXState->setTXState(NULL);
    GfErrTypeThrowException(
        "Could not get a connection for the transaction id.",
        GF_CACHE_ILLEGAL_STATE_EXCEPTION);
  } else {
    txState->getPoolDM()->setThreadLocalConnection(conn);
    LOGFINE("Set the thread local connection for transaction id %d",
            txState->getTransactionId()->getId());
  }
}

bool CacheTransactionManagerImpl::exists(TransactionIdPtr transactionId) {
  return findTx((static_cast<TXIdPtr>(transactionId))->getId());
}

bool CacheTransactionManagerImpl::exists() {
  return TSSTXStateWrapper::s_gemfireTSSTXState->getTXState() != NULL;
}

void CacheTransactionManagerImpl::addTx(int32_t txId) {
  ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_txLock);
  m_TXs.push_back(txId);
}

bool CacheTransactionManagerImpl::removeTx(int32_t txId) {
  ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_txLock);

  for (std::vector<int32_t>::iterator iter = m_TXs.begin(); iter != m_TXs.end();
       ++iter) {
    if (*iter == txId) {
      m_TXs.erase(iter);
      return true;
    }
  }
  return false;
}
bool CacheTransactionManagerImpl::findTx(int32_t txId) {
  ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_txLock);

  for (std::vector<int32_t>::iterator iter = m_TXs.begin(); iter != m_TXs.end();
       ++iter) {
    if (*iter == txId) return true;
  }
  return false;
}

void CacheTransactionManagerImpl::addSuspendedTx(int32_t txId,
                                                 TXState* txState) {
  ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_suspendedTxLock);
  m_suspendedTXs[txId] = txState;
  // signal if some thread is waiting for this transaction to suspend
  m_txCond.broadcast();
}

TXState* CacheTransactionManagerImpl::getSuspendedTx(int32_t txId) {
  ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_suspendedTxLock);
  std::map<int32_t, TXState*>::iterator it = m_suspendedTXs.find(txId);
  if (it == m_suspendedTXs.end()) return NULL;
  TXState* rettxState = (*it).second;
  return rettxState;
}

TXState* CacheTransactionManagerImpl::removeSuspendedTx(int32_t txId) {
  ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_suspendedTxLock);
  std::map<int32_t, TXState*>::iterator it = m_suspendedTXs.find(txId);
  if (it == m_suspendedTXs.end()) return NULL;
  TXState* rettxState = (*it).second;
  m_suspendedTXs.erase(it);
  return rettxState;
}
TXState* CacheTransactionManagerImpl::removeSuspendedTxUntil(
    int32_t txId, int32_t waitTimeInMillisec) {
  ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_suspendedTxLock);
  TXState* txState = NULL;
  ACE_Time_Value currTime(ACE_OS::gettimeofday());
  ACE_Time_Value stopAt(currTime);
  ACE_Time_Value waitTime;
  waitTime.msec(waitTimeInMillisec);
  stopAt += waitTime;

  do {
    txState = removeSuspendedTx(txId);
    if (txState == NULL) {
      LOGFINE("Wait for the connection to get suspended, Tid: %d", txId);
      m_txCond.wait(&stopAt);
    }

  } while (txState == NULL && findTx(txId) &&
           (currTime = ACE_OS::gettimeofday()) < stopAt);
  return txState;
}

bool CacheTransactionManagerImpl::isSuspendedTx(int32_t txId) {
  ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_suspendedTxLock);
  std::map<int32_t, TXState*>::iterator it = m_suspendedTXs.find(txId);
  if (it == m_suspendedTXs.end()) {
    return false;
  } else {
    return true;
  }
}
TransactionIdPtr CacheTransactionManagerImpl::getTransactionId() {
  TXState* txState = TSSTXStateWrapper::s_gemfireTSSTXState->getTXState();
  if (txState == NULL) {
    return NULLPTR;
  } else {
    return txState->getTransactionId();
  }
}
/*
void CacheTransactionManagerImpl::setWriter(TransactionWriterPtr writer)
{
        m_writer = writer;
}

TransactionWriterPtr CacheTransactionManagerImpl::getWriter()
{
        return m_writer;
}


void CacheTransactionManagerImpl::addListener(TransactionListenerPtr aListener)
{
        if(aListener == NULLPTR)
        {
                GfErrTypeThrowException("Trying to add null listener.",
GF_CACHE_ILLEGAL_ARGUMENT_EXCEPTION);
        }
        if(!m_listeners.contains(aListener))
        {
                m_listeners.insert(aListener);
        }
}

void CacheTransactionManagerImpl::removeListener(TransactionListenerPtr
aListener)
{
        if(aListener == NULLPTR)
        {
                GfErrTypeThrowException("Trying to remove null listener.",
GF_CACHE_ILLEGAL_ARGUMENT_EXCEPTION);
        }
        if(m_listeners.erase(aListener))
        {
                aListener->close();
        }
}

void CacheTransactionManagerImpl::noteCommitFailure(TXState* txState, const
TXCommitMessagePtr& commitMessage)
{
        VectorOfEntryEvent events;
        if(commitMessage!= NULLPTR)
        {
                events = commitMessage->getEvents(m_cache);
        }
        TransactionEventPtr event(new
TransactionEvent(txState->getTransactionId(), CachePtr(m_cache), events));

        for(HashSetOfSharedBase::Iterator iter = m_listeners.begin();
m_listeners.end() != iter; iter++)
        {
                TransactionListenerPtr listener =
staticCast<TransactionListenerPtr>(*iter);
                listener->afterFailedCommit(event);
        }
}

void CacheTransactionManagerImpl::noteCommitSuccess(TXState* txState, const
TXCommitMessagePtr& commitMessage)
{
        VectorOfEntryEvent events;
                if(commitMessage!= NULLPTR)
                {
                        events = commitMessage->getEvents(m_cache);
                }
                TransactionEventPtr event(new
TransactionEvent(txState->getTransactionId(), CachePtr(m_cache), events));

        for(HashSetOfSharedBase::Iterator iter = m_listeners.begin();
m_listeners.end() != iter; iter++)
        {
                TransactionListenerPtr listener =
staticCast<TransactionListenerPtr>(*iter);
                listener->afterCommit(event);
        }
}

void CacheTransactionManagerImpl::noteRollbackSuccess(TXState* txState, const
TXCommitMessagePtr& commitMessage)
{
        VectorOfEntryEvent events;
        if(commitMessage!= NULLPTR)
        {
                events = commitMessage->getEvents(m_cache);
        }
        TransactionEventPtr event(new
TransactionEvent(txState->getTransactionId(), CachePtr(m_cache), events));

        for(HashSetOfSharedBase::Iterator iter = m_listeners.begin();
m_listeners.end() != iter; iter++)
        {
                TransactionListenerPtr listener =
staticCast<TransactionListenerPtr>(*iter);
                listener->afterRollback(event);
        }
}
*/
}  // namespace gemfire
