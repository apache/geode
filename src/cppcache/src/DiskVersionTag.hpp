#pragma once

#ifndef GEODE_DISKVERSIONTAG_H_
#define GEODE_DISKVERSIONTAG_H_

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

#include "VersionTag.hpp"
#include "GeodeTypeIdsImpl.hpp"
#include "DiskStoreId.hpp"

namespace apache {
namespace geode {
namespace client {

class RegionInternal;
class CacheImpl;
_GF_PTR_DEF_(VersionTag, VersionTagPtr);

class DiskVersionTag : public VersionTag {
 protected:
  virtual void readMembers(uint16_t flags, DataInput& input) {
    DSMemberForVersionStampPtr previousMemId, internalMemId;
    MemberListForVersionStampPtr memberList =
        CacheImpl::getMemberListForVersionStamp();
    if ((flags & HAS_MEMBER_ID) != 0) {
      DiskStoreId* temp = new DiskStoreId();
      temp->fromData(input);
      internalMemId = DSMemberForVersionStampPtr(temp);
      m_internalMemId = memberList->add(internalMemId);
    }

    if ((flags & HAS_PREVIOUS_MEMBER_ID) != 0) {
      if ((flags & DUPLICATE_MEMBER_IDS) != 0) {
        m_previousMemId = m_internalMemId;
      } else {
        DiskStoreId* temp = new DiskStoreId();
        temp->fromData(input);
        previousMemId = DSMemberForVersionStampPtr(temp);
        m_previousMemId = memberList->add(previousMemId);
      }
    }
  }

 public:
  DiskVersionTag() : VersionTag() {}

  virtual int32_t classId() const { return 0; }

  virtual int8_t typeId() const {
    return static_cast<int8_t>(GeodeTypeIdsImpl::DiskVersionTag);
  }

  static Serializable* createDeserializable() { return new DiskVersionTag(); }

  /**
   * for internal testing
   */
  DiskVersionTag(int32_t entryVersion, int16_t regionVersionHighBytes,
                 int32_t regionVersionLowBytes, uint16_t internalMemId,
                 uint16_t previousMemId)
      : VersionTag(entryVersion, regionVersionHighBytes, regionVersionLowBytes,
                   internalMemId, previousMemId) {}
};
}  // namespace client
}  // namespace geode
}  // namespace apache


#endif // GEODE_DISKVERSIONTAG_H_
