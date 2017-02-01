#pragma once

#ifndef GEODE_TXREGIONSTATE_H_
#define GEODE_TXREGIONSTATE_H_

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
 * TXRegionState.hpp
 *
 *  Created on: 16-Feb-2011
 *      Author: ankurs
 */


#include <gfcpp/gf_types.hpp>
#include <gfcpp/HashMapT.hpp>
#include "TXEntryState.hpp"

using namespace apache::geode::client;

namespace apache {
namespace geode {
namespace client {

class TXRegionState : public apache::geode::client::SharedBase {
 public:
  TXRegionState();
  virtual ~TXRegionState();

 private:
  HashMapT<CacheableKeyPtr, TXEntryStatePtr> m_entryMods;
};

_GF_PTR_DEF_(TXRegionState, TXRegionStatePtr);
}  // namespace client
}  // namespace geode
}  // namespace apache


#endif // GEODE_TXREGIONSTATE_H_
