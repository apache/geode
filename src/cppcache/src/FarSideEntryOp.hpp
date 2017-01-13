/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * FarSideEntryOp.hpp
 *
 *  Created on: 22-Feb-2011
 *      Author: ankurs
 */

#ifndef FARSIDEENTRYOP_HPP_
#define FARSIDEENTRYOP_HPP_

#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/gf_types.hpp>
#include <gfcpp/Cacheable.hpp>
#include <gfcpp/DataOutput.hpp>
#include <gfcpp/DataInput.hpp>
#include <gfcpp/UserData.hpp>
#include "LocalRegion.hpp"

namespace gemfire {

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

class FarSideEntryOp : public gemfire::SharedBase {
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
}

#endif /* FARSIDEENTRYOP_HPP_ */
