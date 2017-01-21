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
 * TransactionSuspender.hpp
 *
 *  Created on: 16-Feb-2011
 *      Author: ankurs
 */

#ifndef TRANSACTIONSUSPENDER_HPP_
#define TRANSACTIONSUSPENDER_HPP_

#include "TXState.hpp"

namespace apache {
namespace geode {
namespace client {

class TransactionSuspender {
 public:
  TransactionSuspender();
  virtual ~TransactionSuspender();

 private:
  TXState* m_TXState;
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif /* TRANSACTIONSUSPENDER_HPP_ */
