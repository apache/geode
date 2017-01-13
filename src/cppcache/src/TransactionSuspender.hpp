/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * TransactionSuspender.hpp
 *
 *  Created on: 16-Feb-2011
 *      Author: ankurs
 */

#ifndef TRANSACTIONSUSPENDER_HPP_
#define TRANSACTIONSUSPENDER_HPP_

#include "TXState.hpp"

namespace gemfire {

class TransactionSuspender {
 public:
  TransactionSuspender();
  virtual ~TransactionSuspender();

 private:
  TXState* m_TXState;
};
}

#endif /* TRANSACTIONSUSPENDER_HPP_ */
