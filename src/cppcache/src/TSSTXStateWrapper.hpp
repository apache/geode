/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * TSSTXStateWrapper.hpp
 *
 *  Created on: 09-Feb-2011
 *      Author: ankurs
 */

#ifndef TSSTXSTATEWRAPPER_HPP_
#define TSSTXSTATEWRAPPER_HPP_

#include <ace/TSS_T.h>
#include "TXId.hpp"

namespace gemfire {
class TXState;

class TSSTXStateWrapper {
 public:
  TSSTXStateWrapper();
  virtual ~TSSTXStateWrapper();

  static ACE_TSS<TSSTXStateWrapper> s_gemfireTSSTXState;
  TXState* getTXState() { return m_txState; }
  void setTXState(TXState* conn) { m_txState = conn; }

 private:
  TXState* m_txState;
  TSSTXStateWrapper& operator=(const TSSTXStateWrapper&);
  TSSTXStateWrapper(const TSSTXStateWrapper&);
};
}

#endif /* TSSTXSTATEWRAPPER_HPP_ */
