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
 * TXCommitMessage.cpp
 *
 *  Created on: 21-Feb-2011
 *      Author: ankurs
 */

#include "TXCommitMessage.hpp"
#include <gfcpp/DataOutput.hpp>
#include <algorithm>

#include "ClientProxyMembershipID.hpp"
#include "FarSideEntryOp.hpp"
#include <vector>

namespace apache {
namespace geode {
namespace client {

TXCommitMessage::TXCommitMessage()
// UNUSED : m_processorId(0)
{}

TXCommitMessage::~TXCommitMessage() {}

bool TXCommitMessage::isAckRequired() { return false; }

Serializable* TXCommitMessage::fromData(DataInput& input) {
  int32_t pId;
  input.readInt(&pId);
  /*
if(isAckRequired()) {
m_processorId = pId;
// ReplyProcessor21.setMessageRPId(m_processorId);
} else {
m_processorId = -1;
}
   */

  int32_t m_txIdent;
  input.readInt(&m_txIdent);
  ClientProxyMembershipID memId;
  memId.fromData(input);

  bool boolVar;
  input.readBoolean(&boolVar);
  if (boolVar) {
    memId.fromData(input);
    int32_t m_lockId;
    input.readInt(&m_lockId);
  }
  int32_t totalMaxSize;
  input.readInt(&totalMaxSize);

  int8_t* m_farsideBaseMembershipId;
  int32_t m_farsideBaseMembershipIdLen;
  input.readBytes(&m_farsideBaseMembershipId, &m_farsideBaseMembershipIdLen);

  if (m_farsideBaseMembershipId != NULL) {
    GF_SAFE_DELETE_ARRAY(m_farsideBaseMembershipId);
    m_farsideBaseMembershipId = NULL;
  }

  int64_t tid;
  input.readInt(&tid);
  int64_t seqId;
  input.readInt(&seqId);

  bool m_needsLargeModCount;
  input.readBoolean(&m_needsLargeModCount);

  int32_t regionSize;
  input.readInt(&regionSize);
  for (int32_t i = 0; i < regionSize; i++) {
    RegionCommitPtr rc(new RegionCommit(/*this*/));
    rc->fromData(input);
    m_regions.push_back(rc);
  }

  int8_t fixedId;
  input.read(&fixedId);
  if (fixedId == GemfireTypeIdsImpl::FixedIDByte) {
    int8_t dfsid;
    input.read(&dfsid);
    if (dfsid == GemfireTypeIdsImpl::ClientProxyMembershipId) {
      ClientProxyMembershipID memId1;
      /* adongre
       * CID 28816: Resource leak (RESOURCE_LEAK)
       * Calling allocation function
       * "apache::geode::client::DataInput::readBytes(signed char
       * **, int *)" on "bytes".
       * no need to read the bytes, just advance curstor
       * more performant solution
       */
      int32_t len;
      input.readArrayLen(&len);
      input.advanceCursor(len);
      // int8_t* bytes;
      // int32_t len;
      // input.readBytes(&bytes, &len);
      // if ( bytes != NULL ) {
      // GF_SAFE_DELETE_ARRAY(bytes);
      // bytes = NULL;
      //}
      input.readInt(&len);
      // memId1.fromData(input);
    } else {
      LOGERROR(
          "TXCommitMessage::fromData Unexpected type id: %d while "
          "desirializing commit response",
          dfsid);
      GfErrTypeThrowException(
          "TXCommitMessage::fromData Unable to handle commit response",
          GF_CACHE_ILLEGAL_STATE_EXCEPTION);
    }
  } else if (fixedId != GemfireTypeIds::NullObj) {
    LOGERROR(
        "TXCommitMessage::fromData Unexpected type id: %d while desirializing "
        "commit response",
        fixedId);
    GfErrTypeThrowException(
        "TXCommitMessage::fromData Unable to handle commit response",
        GF_CACHE_ILLEGAL_STATE_EXCEPTION);
  }

  int32_t len;
  input.readArrayLen(&len);
  for (int j = 0; j < len; j++) {
    CacheablePtr tmp;
    input.readObject(tmp);
  }

  // input.readObject(farSiders);

  return this;
}

void TXCommitMessage::toData(DataOutput& output) const {}

int32_t TXCommitMessage::classId() const { return 0; }

int8_t TXCommitMessage::typeId() const {
  return static_cast<int8_t>(GemfireTypeIdsImpl::TXCommitMessage);
}

Serializable* TXCommitMessage::create() { return new TXCommitMessage(); }

void TXCommitMessage::apply(Cache* cache) {
  for (VectorOfSharedBase::Iterator iter = m_regions.begin();
       m_regions.end() != iter; iter++) {
    RegionCommitPtr regionCommit = staticCast<RegionCommitPtr>(*iter);
    regionCommit->apply(cache);
  }
}
/*
VectorOfEntryEvent TXCommitMessage::getEvents(Cache* cache)
{
        VectorOfEntryEvent events;
        std::vector<FarSideEntryOpPtr> ops;
        for(VectorOfSharedBase::Iterator iter = m_regions.begin();
m_regions.end() != iter; iter++)
        {
                RegionCommitPtr regionCommit =
staticCast<RegionCommitPtr>(*iter);
                regionCommit->fillEvents(cache, ops);
        }

        std::sort(ops.begin(), ops.end(), FarSideEntryOp::cmp);

        for(std::vector<FarSideEntryOpPtr>::iterator iter = ops.begin();
ops.end() != iter; iter++)
        {
                events.push_back((*iter)->getEntryEvent(cache));
        }

        return events;
}
*/
}  // namespace client
}  // namespace geode
}  // namespace apache
