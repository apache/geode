/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * TXId.cpp
 *
 *  Created on: 07-Feb-2011
 *      Author: ankurs
 */

#include "TXId.hpp"

namespace gemfire {

AtomicInc TXId::m_transactionId = 1;

TXId::TXId() : m_TXId(m_transactionId++) {}

TXId::~TXId() {}

int32_t TXId::getId() { return m_TXId; }
}
