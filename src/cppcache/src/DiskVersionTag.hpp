/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __DISKVERSIONTAG_HPP__
#define __DISKVERSIONTAG_HPP__

#include "VersionTag.hpp"
#include "GemfireTypeIdsImpl.hpp"
#include "DiskStoreId.hpp"

namespace gemfire {

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
      m_internalMemId =
          memberList->add((DSMemberForVersionStampPtr)internalMemId);
    }

    if ((flags & HAS_PREVIOUS_MEMBER_ID) != 0) {
      if ((flags & DUPLICATE_MEMBER_IDS) != 0) {
        m_previousMemId = m_internalMemId;
      } else {
        DiskStoreId* temp = new DiskStoreId();
        temp->fromData(input);
        previousMemId = DSMemberForVersionStampPtr(temp);
        m_previousMemId =
            memberList->add((DSMemberForVersionStampPtr)previousMemId);
      }
    }
  }

 public:
  DiskVersionTag() : VersionTag() {}

  virtual int32_t classId() const { return 0; }

  virtual int8_t typeId() const {
    return (int8_t)GemfireTypeIdsImpl::DiskVersionTag;
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
}

#endif  // __DISKVERSIONTAG_HPP__
