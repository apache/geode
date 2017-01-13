/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __VERSIONTAG_HPP__
#define __VERSIONTAG_HPP__

#include <gfcpp/Cacheable.hpp>
#include "GemfireTypeIdsImpl.hpp"
#include "MemberListForVersionStamp.hpp"
namespace gemfire {

class RegionInternal;
class CacheImpl;
_GF_PTR_DEF_(VersionTag, VersionTagPtr);

class VersionTag : public Cacheable {
 protected:
  uint16_t m_bits;
  int32_t m_entryVersion;
  int16_t m_regionVersionHighBytes;
  int32_t m_regionVersionLowBytes;
  uint16_t m_internalMemId;
  uint16_t m_previousMemId;
  int64_t m_timeStamp;

  static const uint8_t HAS_MEMBER_ID = 0x01;
  static const uint8_t HAS_PREVIOUS_MEMBER_ID = 0x02;
  static const uint8_t VERSION_TWO_BYTES = 0x04;
  static const uint8_t DUPLICATE_MEMBER_IDS = 0x08;
  static const uint8_t HAS_RVV_HIGH_BYTE = 0x10;

  static const uint8_t BITS_POSDUP = 0x01;
  static const uint8_t BITS_RECORDED = 0x02;  // has the rvv recorded this?
  static const uint8_t BITS_HAS_PREVIOUS_ID = 0x03;
  virtual void readMembers(uint16_t flags, DataInput& input);

 public:
  VersionTag();

  virtual ~VersionTag();

  virtual int32_t classId() const;

  virtual int8_t typeId() const;

  virtual void toData(DataOutput& output) const;

  virtual Serializable* fromData(DataInput& input);

  static Serializable* createDeserializable();

  int32_t getEntryVersion() const { return m_entryVersion; }
  int16_t getRegionVersionHighBytes() const { return m_regionVersionHighBytes; }
  int32_t getRegionVersionLowBytes() const { return m_regionVersionLowBytes; }
  uint16_t getInternalMemID() const { return m_internalMemId; }
  uint16_t getPreviousMemID() const { return m_previousMemId; }
  void replaceNullMemberId(uint16_t memId);
  // DSMemberForVersionStampPtr getMemberID(uint16_t memID);
  void setInternalMemID(uint16_t internalMemId) {
    m_internalMemId = internalMemId;
  }

  /**
   * for internal testing
   */
  VersionTag(int32_t entryVersion, int16_t regionVersionHighBytes,
             int32_t regionVersionLowBytes, uint16_t internalMemId,
             uint16_t previousMemId);
};
}

#endif  // __VERSIONTAG_HPP__
