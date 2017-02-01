#pragma once

#ifndef GEODE_FARSIDEENTRYOP_H_
#define GEODE_FARSIDEENTRYOP_H_

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
 * FarSideEntryOp.hpp
 *
 *  Created on: 22-Feb-2011
 *      Author: ankurs
 */


#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/gf_types.hpp>
#include <gfcpp/Cacheable.hpp>
#include <gfcpp/DataOutput.hpp>
#include <gfcpp/DataInput.hpp>
#include <gfcpp/UserData.hpp>
#include "LocalRegion.hpp"

namespace apache {
namespace geode {
namespace client {

enum OPERATION {
  MARKER = 0,
  CREATE,
  PUTALL_CREATE,
  GET,
  GET_ENTRY,
  CONTAINS_KEY,
  CONTAINS_VALUE,
  CONTAINS_VALUE_FOR_KEY,
  FUNCTION_EXECUTION,
  SEARCH_CREATE,
  LOCAL_LOAD_CREATE,
  NET_LOAD_CREATE,
  UPDATE,
  PUTALL_UPDATE,
  SEARCH_UPDATE,
  LOCAL_LOAD_UPDATE,
  NET_LOAD_UPDATE,
  INVALIDATE,
  LOCAL_INVALIDATE,
  DESTROY,
  LOCAL_DESTROY,
  EVICT_DESTROY,
  REGION_LOAD_SNAPSHOT,
  REGION_LOCAL_DESTROY,
  REGION_CREATE,
  REGION_CLOSE,
  REGION_DESTROY,
  EXPIRE_DESTROY,
  EXPIRE_LOCAL_DESTROY,
  EXPIRE_INVALIDATE,
  EXPIRE_LOCAL_INVALIDATE,
  REGION_EXPIRE_DESTROY,
  REGION_EXPIRE_LOCAL_DESTROY,
  REGION_EXPIRE_INVALIDATE,
  REGION_EXPIRE_LOCAL_INVALIDATE,
  REGION_LOCAL_INVALIDATE,
  REGION_INVALIDATE,
  REGION_CLEAR,
  REGION_LOCAL_CLEAR,
  CACHE_CREATE,
  CACHE_CLOSE,
  FORCED_DISCONNECT,
  REGION_REINITIALIZE,
  CACHE_RECONNECT,
  PUT_IF_ABSENT,
  REPLACE,
  REMOVE
};

class RegionCommit;

_GF_PTR_DEF_(FarSideEntryOp, FarSideEntryOpPtr);

class FarSideEntryOp : public apache::geode::client::SharedBase {
 public:
  FarSideEntryOp(RegionCommit* region);
  virtual ~FarSideEntryOp();

  void fromData(DataInput& input, bool largeModCount, uint16_t memId);
  void apply(RegionPtr& region);
  static bool cmp(const FarSideEntryOpPtr& lhs, const FarSideEntryOpPtr& rhs) {
    return lhs->m_modSerialNum > rhs->m_modSerialNum;
  }

  //	EntryEventPtr getEntryEvent(Cache* cache);

 private:
  // UNUSED RegionCommit* m_region;
  int8_t m_op;
  int32_t m_modSerialNum;
  int32_t m_eventOffset;
  CacheableKeyPtr m_key;
  CacheablePtr m_value;
  bool m_didDestroy;
  UserDataPtr m_callbackArg;
  VersionTagPtr m_versionTag;
  // FilterRoutingInfo filterRoutingInfo;
  bool isDestroy(int8_t op);
  bool isInvalidate(int8_t op);
  void skipFilterRoutingInfo(DataInput& input);
};
}  // namespace client
}  // namespace geode
}  // namespace apache


#endif // GEODE_FARSIDEENTRYOP_H_
