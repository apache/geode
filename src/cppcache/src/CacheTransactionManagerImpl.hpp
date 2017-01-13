/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * CacheTransactionManagerImpl.h
 *
 *  Created on: 04-Feb-2011
 *      Author: ankurs
 */

#ifndef CACHETRANSACTIONMANAGERIMPL_H_
#define CACHETRANSACTIONMANAGERIMPL_H_

#include <gfcpp/CacheTransactionManager.hpp>
#include <gfcpp/HashSetOfSharedBase.hpp>
#include "TXCommitMessage.hpp"
#include <gfcpp/Log.hpp>
#include "SuspendedTxExpiryHandler.hpp"

namespace gemfire {

enum status { STATUS_COMMITTED = 3, STATUS_ROLLEDBACK = 4 };
enum commitOp { BEFORE_COMMIT, AFTER_COMMIT };

class CacheTransactionManagerImpl
    : public virtual gemfire::CacheTransactionManager {
 public:
  CacheTransactionManagerImpl(Cache* cache);
  virtual ~CacheTransactionManagerImpl();

  virtual void begin();
  virtual void commit();
  virtual void rollback();
  virtual bool exists();
  virtual TransactionIdPtr suspend();
  virtual void resume(TransactionIdPtr transactionId);
  virtual bool isSuspended(TransactionIdPtr transactionId);
  virtual bool tryResume(TransactionIdPtr transactionId);
  bool tryResume(TransactionIdPtr transactionId, bool cancelExpiryTask);
  virtual bool tryResume(TransactionIdPtr transactionId,
                         int32_t waitTimeInMillisec);
  virtual bool exists(TransactionIdPtr transactionId);

  virtual TransactionIdPtr getTransactionId();

  //    virtual void setWriter(TransactionWriterPtr writer);
  //    virtual TransactionWriterPtr getWriter();

  //    virtual void addListener(TransactionListenerPtr aListener);
  //    virtual void removeListener(TransactionListenerPtr aListener);

  inline static int32_t hasher(const SharedBasePtr& p) {
    return (int32_t) reinterpret_cast<intptr_t>(p.ptr());
  }

  inline static bool equal_to(const SharedBasePtr& x, const SharedBasePtr& y) {
    return x.ptr() == y.ptr();
  }
  TXState* getSuspendedTx(int32_t txId);

 protected:
  ThinClientPoolDM* getDM();
  Cache* getCache();

 private:
  Cache* m_cache;
  // TransactionListenerPtr m_listener;

  //    void noteCommitFailure(TXState* txState, const TXCommitMessagePtr&
  //    commitMessage);
  //    void noteCommitSuccess(TXState* txState, const TXCommitMessagePtr&
  //    commitMessage);
  //    void noteRollbackSuccess(TXState* txState, const TXCommitMessagePtr&
  //    commitMessage);
  void resumeTxUsingTxState(TXState* txState, bool cancelExpiryTask = true);
  GfErrType rollback(TXState* txState, bool callListener);
  void addSuspendedTx(int32_t txId, TXState* txState);
  TXState* removeSuspendedTx(int32_t txId);
  TXState* removeSuspendedTxUntil(int32_t txId, int32_t waitTimeInSec);
  bool isSuspendedTx(int32_t txId);
  void addTx(int32_t txId);
  bool removeTx(int32_t txId);
  bool findTx(int32_t txId);
  std::map<int32_t, TXState*> m_suspendedTXs;
  ACE_Recursive_Thread_Mutex m_suspendedTxLock;
  std::vector<int32_t> m_TXs;
  ACE_Recursive_Thread_Mutex m_txLock;
  ACE_Condition<ACE_Recursive_Thread_Mutex> m_txCond;

  friend class TXCleaner;
};
}

#endif /* CACHETRANSACTIONMANAGERIMPL_H_ */
