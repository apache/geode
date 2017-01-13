/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * TXRegionState.hpp
 *
 *  Created on: 16-Feb-2011
 *      Author: ankurs
 */

#ifndef TXREGIONSTATE_HPP_
#define TXREGIONSTATE_HPP_

#include <gfcpp/gf_types.hpp>
#include <gfcpp/HashMapT.hpp>
#include "TXEntryState.hpp"

using namespace gemfire;

namespace gemfire {

class TXRegionState : public gemfire::SharedBase {
 public:
  TXRegionState();
  virtual ~TXRegionState();

 private:
  HashMapT<CacheableKeyPtr, TXEntryStatePtr> m_entryMods;
};

_GF_PTR_DEF_(TXRegionState, TXRegionStatePtr);
}

#endif /* TXREGIONSTATE_HPP_ */
