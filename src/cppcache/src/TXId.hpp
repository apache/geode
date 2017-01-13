/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * TXId.h
 *
 *  Created on: 07-Feb-2011
 *      Author: ankurs
 */

#ifndef TXID_H_
#define TXID_H_

#include <gfcpp/gf_types.hpp>
#include <gfcpp/TransactionId.hpp>
#include <gfcpp/DataOutput.hpp>
#include "AtomicInc.hpp"

namespace gemfire {

_GF_PTR_DEF_(TXId, TXIdPtr);

class TXId : public gemfire::TransactionId {
 public:
  TXId();
  virtual ~TXId();

  int32_t getId();

 private:
  const int32_t m_TXId;
  static AtomicInc m_transactionId;
  TXId& operator=(const TXId&);
  TXId(const TXId&);
};
}

#endif /* TXID_H_ */
