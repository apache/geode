#pragma once

#ifndef GEODE_TSSTXSTATEWRAPPER_H_
#define GEODE_TSSTXSTATEWRAPPER_H_

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
 * TSSTXStateWrapper.hpp
 *
 *  Created on: 09-Feb-2011
 *      Author: ankurs
 */


#include <ace/TSS_T.h>
#include "TXId.hpp"

namespace apache {
namespace geode {
namespace client {
class TXState;

class TSSTXStateWrapper {
 public:
  TSSTXStateWrapper();
  virtual ~TSSTXStateWrapper();

  static ACE_TSS<TSSTXStateWrapper> s_geodeTSSTXState;
  TXState* getTXState() { return m_txState; }
  void setTXState(TXState* conn) { m_txState = conn; }

 private:
  TXState* m_txState;
  TSSTXStateWrapper& operator=(const TSSTXStateWrapper&);
  TSSTXStateWrapper(const TSSTXStateWrapper&);
};
}  // namespace client
}  // namespace geode
}  // namespace apache


#endif // GEODE_TSSTXSTATEWRAPPER_H_
