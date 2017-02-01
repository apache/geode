#pragma once

#ifndef GEODE_TXENTRYSTATE_H_
#define GEODE_TXENTRYSTATE_H_

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * TXEntryState.hpp
 *
 *  Created on: 16-Feb-2011
 *      Author: ankurs
 */


#include <gfcpp/gf_types.hpp>

using namespace apache::geode::client;

namespace apache {
namespace geode {
namespace client {

class TXEntryState : public apache::geode::client::SharedBase {
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
}  // namespace client
}  // namespace geode
}  // namespace apache


#endif // GEODE_TXENTRYSTATE_H_
