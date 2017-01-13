/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __GEMFIRE_IMPL_VERSIONSTAMP_H__
#define __GEMFIRE_IMPL_VERSIONSTAMP_H__

#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/SharedPtr.hpp>
#include "ClientProxyMembershipID.hpp"
#include "VersionTag.hpp"

namespace gemfire {
/**
 * @brief This class encapsulates Version Stamp for map entries.
 */
class CPPCACHE_EXPORT VersionStamp {
 public:
  VersionStamp()
      : m_memberID(0),
        m_entryVersionHighByte(0),
        m_entryVersionLowBytes(0),
        m_regionVersionHighBytes(0),
        m_regionVersionLowBytes(0) {}

  VersionStamp(const VersionStamp& rhs)
      : m_memberID(rhs.m_memberID),
        m_entryVersionHighByte(rhs.m_entryVersionHighByte),
        m_entryVersionLowBytes(rhs.m_entryVersionLowBytes),
        m_regionVersionHighBytes(rhs.m_regionVersionHighBytes),
        m_regionVersionLowBytes(rhs.m_regionVersionLowBytes) {}

  virtual ~VersionStamp() {}
  void setVersions(VersionTagPtr versionTag);
  void setVersions(VersionStamp& versionStamp);
  int32_t getEntryVersion();
  int64_t getRegionVersion();
  uint16_t getMemberId() const;

  VersionStamp& operator=(const VersionStamp& rhs) {
    if (this == &rhs) return *this;
    this->m_memberID = rhs.m_memberID;
    this->m_entryVersionHighByte = rhs.m_entryVersionHighByte;
    this->m_entryVersionLowBytes = rhs.m_entryVersionLowBytes;
    this->m_regionVersionHighBytes = rhs.m_regionVersionHighBytes;
    this->m_regionVersionLowBytes = rhs.m_regionVersionLowBytes;
    return *this;
  }
  GfErrType processVersionTag(RegionInternal* region, CacheableKeyPtr keyPtr,
                              VersionTagPtr tag, bool deltaCheck);

 private:
  uint16_t m_memberID;
  uint8_t m_entryVersionHighByte;
  uint16_t m_entryVersionLowBytes;
  uint16_t m_regionVersionHighBytes;
  uint32_t m_regionVersionLowBytes;
  GfErrType checkForConflict(RegionInternal* region, std::string keystr,
                             VersionTagPtr tag, bool deltaCheck);
  GfErrType checkForDeltaConflict(RegionInternal* region, std::string keystr,
                                  int64_t stampVersion, int64_t tagVersion,
                                  VersionTagPtr tag);
};
typedef SharedPtr<VersionStamp> VersionStampPtr;
}

#endif  // __GEMFIRE_IMPL_VERSIONSTAMP_H__
