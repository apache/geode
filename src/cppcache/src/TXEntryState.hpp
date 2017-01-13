/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * TXEntryState.hpp
 *
 *  Created on: 16-Feb-2011
 *      Author: ankurs
 */

#ifndef TXENTRYSTATE_HPP_
#define TXENTRYSTATE_HPP_

#include <gfcpp/gf_types.hpp>

using namespace gemfire;

namespace gemfire {

class TXEntryState : public gemfire::SharedBase {
 public:
  TXEntryState();
  virtual ~TXEntryState();

 private:
  int8_t adviseOp(int8_t requestedOpCode);
  // UNUSED int32_t m_modSerialNum;

  int8_t m_op;
  //	TXRegionStatePtr m_txRegionState;
  // UNUSED bool m_bulkOp;

  // ORDER of the following is important to the implementation!
  static const int8_t DESTROY_NONE = 0;
  static const int8_t DESTROY_LOCAL = 1;
  static const int8_t DESTROY_DISTRIBUTED = 2;

  // ORDER of the following is important to the implementation!
  static const int8_t OP_NULL = 0;
  static const int8_t OP_L_DESTROY = 1;
  static const int8_t OP_CREATE_LD = 2;
  static const int8_t OP_LLOAD_CREATE_LD = 3;
  static const int8_t OP_NLOAD_CREATE_LD = 4;
  static const int8_t OP_PUT_LD = 5;
  static const int8_t OP_LLOAD_PUT_LD = 6;
  static const int8_t OP_NLOAD_PUT_LD = 7;
  static const int8_t OP_D_INVALIDATE_LD = 8;
  static const int8_t OP_D_DESTROY = 9;
  static const int8_t OP_L_INVALIDATE = 10;
  static const int8_t OP_PUT_LI = 11;
  static const int8_t OP_LLOAD_PUT_LI = 12;
  static const int8_t OP_NLOAD_PUT_LI = 13;
  static const int8_t OP_D_INVALIDATE = 14;
  static const int8_t OP_CREATE_LI = 15;
  static const int8_t OP_LLOAD_CREATE_LI = 16;
  static const int8_t OP_NLOAD_CREATE_LI = 17;
  static const int8_t OP_CREATE = 18;
  static const int8_t OP_SEARCH_CREATE = 19;
  static const int8_t OP_LLOAD_CREATE = 20;
  static const int8_t OP_NLOAD_CREATE = 21;
  static const int8_t OP_LOCAL_CREATE = 22;
  static const int8_t OP_PUT = 23;
  static const int8_t OP_SEARCH_PUT = 24;
  static const int8_t OP_LLOAD_PUT = 25;
  static const int8_t OP_NLOAD_PUT = 26;
};

_GF_PTR_DEF_(TXEntryState, TXEntryStatePtr);
}

#endif /* TXENTRYSTATE_HPP_ */
