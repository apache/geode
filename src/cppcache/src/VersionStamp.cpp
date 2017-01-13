/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "VersionStamp.hpp"
#include <string>
#include "MemberListForVersionStamp.hpp"
#include "CacheImpl.hpp"
#include "RegionInternal.hpp"
#include "ThinClientRegion.hpp"
#include "ThinClientPoolDM.hpp"

using namespace gemfire;

void VersionStamp::setVersions(VersionTagPtr versionTag) {
  int32_t eVersion = versionTag->getEntryVersion();
  m_entryVersionLowBytes = static_cast<uint16_t>(eVersion & 0xffff);
  m_entryVersionHighByte = static_cast<uint8_t>((eVersion & 0xff0000) >> 16);
  m_regionVersionHighBytes = versionTag->getRegionVersionHighBytes();
  m_regionVersionLowBytes = versionTag->getRegionVersionLowBytes();
  m_memberID = versionTag->getInternalMemID();
}

void VersionStamp::setVersions(VersionStamp& versionStamp) {
  m_entryVersionLowBytes = versionStamp.m_entryVersionLowBytes;
  m_entryVersionHighByte = versionStamp.m_entryVersionHighByte;
  m_regionVersionHighBytes = versionStamp.m_regionVersionHighBytes;
  m_regionVersionLowBytes = versionStamp.m_regionVersionLowBytes;
  m_memberID = versionStamp.m_memberID;
}
int32_t VersionStamp::getEntryVersion() {
  return (m_entryVersionHighByte << 16) | m_entryVersionLowBytes;
}

int64_t VersionStamp::getRegionVersion() {
  return ((static_cast<int64_t>(m_regionVersionHighBytes)) << 32) |
         m_regionVersionLowBytes;
}

uint16_t VersionStamp::getMemberId() const { return m_memberID; }

// Processes version tag. Checks if there is a conflict with the existing
// version.
// Also checks if it is a delta update than it is based on the existing version.
// This is based on the basicprocessVersionTag function of
// AbstractRegionEntry.java
// Any change to the java function should be reflected here as well.
GfErrType VersionStamp::processVersionTag(RegionInternal* region,
                                          CacheableKeyPtr keyPtr,
                                          VersionTagPtr tag, bool deltaCheck) {
  char key[256];
  int16_t keyLen = keyPtr->logString(key, 256);
  std::string keystr(key, keyLen);

  if (tag.ptr() == NULL) {
    LOGERROR("Cannot process version tag as it is NULL.");
    return GF_CACHE_ILLEGAL_STATE_EXCEPTION;
  }

  return checkForConflict(region, keystr, tag, deltaCheck);
}

GfErrType VersionStamp::checkForConflict(RegionInternal* region,
                                         std::string keystr, VersionTagPtr tag,
                                         bool deltaCheck) {
  if (getEntryVersion() == 0 && getRegionVersion() == 0 && getMemberId() == 0) {
    LOGDEBUG(
        "Version stamp on existing entry not found. applying change: key=%s",
        keystr.c_str());
    return GF_NOERR;
  }

  if (tag->getEntryVersion() == 0 && tag->getRegionVersionHighBytes() == 0 &&
      tag->getRegionVersionLowBytes() == 0 && tag->getInternalMemID() == 0) {
    LOGDEBUG("Version Tag not available. applying change: key=%s",
             keystr.c_str());
    return GF_NOERR;
  }
  int64_t stampVersion = getEntryVersion() & 0xffffffffL;
  int64_t tagVersion = tag->getEntryVersion() & 0xffffffffL;
  MemberListForVersionStampPtr memberList =
      region->getCacheImpl()->getMemberListForVersionStamp();
  bool apply = false;
  if (stampVersion != 0) {
    // check for int wraparound on the version number
    int64_t difference = tagVersion - stampVersion;
    if (0x10000 < difference || difference < -0x10000) {
      LOGDEBUG("version rollover detected: key=%s tag=%lld stamp=%lld",
               keystr.c_str(), tagVersion, stampVersion);
      int64_t temp = 0x100000000LL;
      if (difference < 0) {
        tagVersion += temp;
      } else {
        stampVersion += temp;
      }
    }
  }

  if (deltaCheck) {
    GfErrType err =
        checkForDeltaConflict(region, keystr, stampVersion, tagVersion, tag);
    if (err != GF_NOERR) return err;
  }

  if (stampVersion == 0 || stampVersion < tagVersion) {
    LOGDEBUG("applying change: key=%s", keystr.c_str());
    apply = true;
  } else if (stampVersion > tagVersion) {
    LOGDEBUG("disallowing change: key=%s", keystr.c_str());
  } else {
    // compare member IDs
    DSMemberForVersionStampPtr stampID = memberList->getDSMember(getMemberId());
    if (stampID == NULLPTR && stampID.ptr() == NULL) {
      // This scenario is not possible. But added for just in case
      LOGERROR(
          "MemberId of the version stamp could not be found. Disallowing a "
          "possible inconsistent change: key=%s",
          keystr.c_str());
      // throw error
      return GF_CACHE_ILLEGAL_STATE_EXCEPTION;
    }
    DSMemberForVersionStampPtr tagID =
        memberList->getDSMember(tag->getInternalMemID());
    if (tagID == NULLPTR && tagID.ptr() == NULL) {
      // This scenario is not possible. But added for just in case
      LOGERROR(
          "MemberId of the version tag could not be found. Disallowing a "
          "possible inconsistent change. key=%s",
          keystr.c_str());
      // throw error
      return GF_CACHE_ILLEGAL_STATE_EXCEPTION;
    }
    if (!apply) {
      LOGDEBUG(
          "comparing tagID %s with stampId %s for version comparison of key %s",
          tagID->getHashKey().c_str(), stampID->getHashKey().c_str(),
          keystr.c_str());
      int compare = stampID->compareTo(tagID);
      if (compare < 0) {
        LOGDEBUG("applying change: key=%s", keystr.c_str());
        apply = true;
      } else if (compare > 0) {
        LOGDEBUG("disallowing change: key=%s", keystr.c_str());
      } else {
        LOGDEBUG(
            "allowing the change as both the version tag and version stamp are "
            "same: key=%s",
            keystr.c_str());
        // This is required for local ops to succeed.
        apply = true;
      }
    }
  }

  if (!apply) {
    region->getCacheImpl()->m_cacheStats->incConflatedEvents();
    return GF_CACHE_CONCURRENT_MODIFICATION_EXCEPTION;
  }
  return GF_NOERR;
}

GfErrType VersionStamp::checkForDeltaConflict(RegionInternal* region,
                                              std::string keystr,
                                              int64_t stampVersion,
                                              int64_t tagVersion,
                                              VersionTagPtr tag) {
  MemberListForVersionStampPtr memberList =
      region->getCacheImpl()->getMemberListForVersionStamp();
  ThinClientRegion* tcRegion = dynamic_cast<ThinClientRegion*>(region);
  ThinClientPoolDM* poolDM = NULL;
  if (tcRegion) {
    poolDM = dynamic_cast<ThinClientPoolDM*>(tcRegion->getDistMgr());
  }

  if (tagVersion != stampVersion + 1) {
    LOGDEBUG(
        "delta requires full value due to version mismatch. key=%s tagVersion "
        "%lld stampVersion %lld ",
        keystr.c_str(), tagVersion, stampVersion);
    if (poolDM) poolDM->updateNotificationStats(false, 0);
    return GF_INVALID_DELTA;

  } else {
    // make sure the tag was based on the value in this entry by checking the
    // tag's previous-changer ID against this stamp's current ID
    DSMemberForVersionStampPtr stampID = memberList->getDSMember(getMemberId());
    if (stampID.ptr() == NULL) {
      LOGERROR(
          "MemberId of the version stamp could not be found. Requesting full "
          "delta value. key=%s",
          keystr.c_str());
      if (poolDM) poolDM->updateNotificationStats(false, 0);
      return GF_INVALID_DELTA;
    }

    DSMemberForVersionStampPtr tagID =
        memberList->getDSMember(tag->getPreviousMemID());
    if (tagID.ptr() == NULL) {
      LOGERROR(
          "Previous MemberId of the version tag could not be found. Requesting "
          "full delta value. key=%s",
          keystr.c_str());
      if (poolDM) poolDM->updateNotificationStats(false, 0);
      return GF_INVALID_DELTA;
    }

    if (tagID->compareTo(stampID) != 0) {
      LOGDEBUG(
          "delta requires full value due to version mismatch. key=%s. \
        tag.previous=%s but stamp.current=%s",
          keystr.c_str(), tagID->getHashKey().c_str(),
          stampID->getHashKey().c_str());

      if (poolDM) poolDM->updateNotificationStats(false, 0);
      return GF_INVALID_DELTA;
    }
    return GF_NOERR;
  }
}
