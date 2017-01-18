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

namespace apache {
namespace geode {
namespace client {

_GF_PTR_DEF_(TXId, TXIdPtr);

class TXId : public apache::geode::client::TransactionId {
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
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif /* TXID_H_ */
