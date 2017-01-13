/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "VersionTag.hpp"
#include "CacheImpl.hpp"
#include "RegionInternal.hpp"
#include "MemberListForVersionStamp.hpp"
#include "ClientProxyMembershipID.hpp"

using namespace gemfire;

VersionTag::VersionTag() {
  m_bits = 0;
  m_entryVersion = 0;
  m_regionVersionHighBytes = 0;
  m_regionVersionLowBytes = 0;
  m_timeStamp = 0;
  m_internalMemId = 0;
  m_previousMemId = 0;
}

VersionTag::VersionTag(int32_t entryVersion, int16_t regionVersionHighBytes,
                       int32_t regionVersionLowBytes, uint16_t internalMemId,
                       uint16_t previousMemId) {
  m_bits = 0;
  m_entryVersion = entryVersion;
  m_regionVersionHighBytes = regionVersionHighBytes;
  m_regionVersionLowBytes = regionVersionLowBytes;
  m_timeStamp = 0;
  m_internalMemId = internalMemId;
  m_previousMemId = previousMemId;
}

VersionTag::~VersionTag() {}

int32_t VersionTag::classId() const { return 0; }

int8_t VersionTag::typeId() const {
  return static_cast<int8_t>(GemfireTypeIdsImpl::VersionTag);
}

void VersionTag::toData(DataOutput& output) const {
  throw IllegalStateException("VersionTag::toData not implemented");
}

Serializable* VersionTag::fromData(DataInput& input) {
  uint16_t flags;
  input.readInt(&flags);
  input.readInt(&m_bits);
  int8_t distributedSystemId;
  input.read(&distributedSystemId);
  if ((flags & VERSION_TWO_BYTES) != 0) {
    int16_t tempVar;
    input.readInt(&tempVar);
    m_entryVersion = tempVar;
    m_entryVersion &= 0xffff;
  } else {
    input.readInt(&m_entryVersion);
    m_entryVersion &= 0xffffffff;
  }
  if ((flags & HAS_RVV_HIGH_BYTE) != 0) {
    input.readInt(&m_regionVersionHighBytes);
  }
  input.readInt(&m_regionVersionLowBytes);
  input.readUnsignedVL(&m_timeStamp);
  readMembers(flags, input);
  return this;
}

Serializable* VersionTag::createDeserializable() { return new VersionTag(); }
void VersionTag::replaceNullMemberId(uint16_t memId) {
  if (m_previousMemId == 0) {
    m_previousMemId = memId;
  }
  if (m_internalMemId == 0) {
    m_internalMemId = memId;
  }
}
void VersionTag::readMembers(uint16_t flags, DataInput& input) {
  ClientProxyMembershipIDPtr previousMemId, internalMemId;
  MemberListForVersionStampPtr memberList =
      CacheImpl::getMemberListForVersionStamp();
  if ((flags & HAS_MEMBER_ID) != 0) {
    internalMemId = ClientProxyMembershipIDPtr(new ClientProxyMembershipID());

    internalMemId->readEssentialData(input);
    m_internalMemId =
        memberList->add((DSMemberForVersionStampPtr)internalMemId);
  }
  if ((flags & HAS_PREVIOUS_MEMBER_ID) != 0) {
    if ((flags & DUPLICATE_MEMBER_IDS) != 0) {
      m_previousMemId = m_internalMemId;
    } else {
      previousMemId = ClientProxyMembershipIDPtr(new ClientProxyMembershipID());
      previousMemId->readEssentialData(input);
      m_previousMemId =
          memberList->add((DSMemberForVersionStampPtr)previousMemId);
    }
  }
}
