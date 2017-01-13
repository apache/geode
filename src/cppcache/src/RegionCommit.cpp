/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * RegionCommit.cpp
 *
 *  Created on: 23-Feb-2011
 *      Author: ankurs
 */

#include "RegionCommit.hpp"

namespace gemfire {

RegionCommit::RegionCommit() {
  // TODO Auto-generated constructor stub
}

RegionCommit::~RegionCommit() {
  // TODO Auto-generated destructor stub
}

void RegionCommit::fromData(DataInput& input) {
  input.readObject(m_regionPath);
  input.readObject(m_parentRegionPath);
  int32_t size = 0;

  input.readInt(&size);
  if (size > 0) {
    bool largeModCount;
    input.readBoolean(&largeModCount);
    DSMemberForVersionStampPtr dsMember;
    input.readObject(dsMember);
    uint16_t memId = CacheImpl::getMemberListForVersionStamp()->add(dsMember);
    for (int i = 0; i < size; i++) {
      FarSideEntryOpPtr entryOp(new FarSideEntryOp(this));
      entryOp->fromData(input, largeModCount, memId);
      m_farSideEntryOps.push_back(entryOp);
    }
  }
}

void RegionCommit::apply(Cache* cache) {
  for (VectorOfSharedBase::Iterator iter = m_farSideEntryOps.begin();
       m_farSideEntryOps.end() != iter; iter++) {
    FarSideEntryOpPtr entryOp = staticCast<FarSideEntryOpPtr>(*iter);
    RegionPtr region = cache->getRegion(m_regionPath->asChar());
    if (region == NULLPTR && m_parentRegionPath != NULLPTR) {
      region = cache->getRegion(m_parentRegionPath->asChar());
    }
    entryOp->apply(region);
  }
}

void RegionCommit::fillEvents(Cache* cache,
                              std::vector<FarSideEntryOpPtr>& ops) {
  for (VectorOfSharedBase::Iterator iter = m_farSideEntryOps.begin();
       m_farSideEntryOps.end() != iter; iter++) {
    ops.push_back(*iter);
  }
}
}  // namespace gemfire
