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
