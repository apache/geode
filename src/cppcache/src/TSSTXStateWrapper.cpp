/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * TSSTXStateWrapper.cpp
 *
 *  Created on: 09-Feb-2011
 *      Author: ankurs
 */

#include "TSSTXStateWrapper.hpp"
#include "TXState.hpp"

namespace gemfire {
ACE_TSS<TSSTXStateWrapper> TSSTXStateWrapper::s_gemfireTSSTXState;

TSSTXStateWrapper::TSSTXStateWrapper() { m_txState = NULL; }

TSSTXStateWrapper::~TSSTXStateWrapper() {
  if (m_txState) {
    delete m_txState;
    m_txState = NULL;
  }
}
}  // namespace gemfire
