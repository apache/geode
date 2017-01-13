/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * TransactionSuspender.cpp
 *
 *  Created on: 16-Feb-2011
 *      Author: ankurs
 */

#include "TransactionSuspender.hpp"
#include "TSSTXStateWrapper.hpp"

namespace gemfire {

TransactionSuspender::TransactionSuspender() {
  TSSTXStateWrapper* txStateWrapper = TSSTXStateWrapper::s_gemfireTSSTXState;
  m_TXState = txStateWrapper->getTXState();
  txStateWrapper->setTXState(NULL);
}

TransactionSuspender::~TransactionSuspender() {
  TSSTXStateWrapper::s_gemfireTSSTXState->setTXState(m_TXState);
}
}  // namespace gemfire
