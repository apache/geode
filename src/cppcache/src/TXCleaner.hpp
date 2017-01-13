/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * TxCleaner.hpp
 *
 *  Created on: Nov 13, 2015
 *      Author: sshcherbakov
 */

#ifndef TXCLEANER_HPP_
#define TXCLEANER_HPP_

#include "CacheTransactionManagerImpl.hpp"
#include "TSSTXStateWrapper.hpp"
#include "TXState.hpp"

namespace gemfire {

class TXCleaner {
 public:
  TXCleaner(CacheTransactionManagerImpl* cacheTxMgr);
  ~TXCleaner();

  void clean();
  TXState* getTXState();

 private:
  TSSTXStateWrapper* m_txStateWrapper;
  TXState* m_txState;
  CacheTransactionManagerImpl* m_cacheTxMgr;

  TXCleaner& operator=(const TXCleaner& other);
  TXCleaner(const TXCleaner& other);
};
}

#endif /* TXCLEANER_HPP_ */
