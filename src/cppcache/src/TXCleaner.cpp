/*=========================================================================
 * Copyright (c) 2010-2015 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * TxCleaner.cpp
 *
 *  Created on: Nov 13, 2015
 *      Author: sshcherbakov
 */

#include "TXCleaner.hpp"

namespace gemfire {

TXCleaner::TXCleaner(CacheTransactionManagerImpl* cacheTxMgr) {
  m_txStateWrapper = TSSTXStateWrapper::s_gemfireTSSTXState;
  m_txState = m_txStateWrapper->getTXState();
  m_cacheTxMgr = cacheTxMgr;
}

TXCleaner::~TXCleaner() {
  clean();
  if (m_txState != NULL) {
    m_txState->releaseStickyConnection();
    delete m_txState;
    m_txState = NULL;
  }
}
void TXCleaner::clean() {
  if (m_txState != NULL && m_txState->getTransactionId().ptr() != NULL) {
    m_cacheTxMgr->removeTx(m_txState->getTransactionId()->getId());
  }
  if (m_txStateWrapper != NULL && m_txState != NULL) {
    m_txStateWrapper->setTXState(NULL);
  }
}

TXState* TXCleaner::getTXState() {
  return (m_txStateWrapper == NULL) ? NULL : m_txStateWrapper->getTXState();
}
}  // namespace gemfire
