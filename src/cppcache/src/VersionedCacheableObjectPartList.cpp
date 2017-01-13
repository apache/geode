/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "VersionedCacheableObjectPartList.hpp"
#include <gfcpp/ExceptionTypes.hpp>
#include "GemfireTypeIdsImpl.hpp"
#include <gfcpp/CacheableString.hpp>
#include "ThinClientRegion.hpp"
#include "CacheableToken.hpp"
#include "DiskStoreId.hpp"
#include "DiskVersionTag.hpp"
namespace gemfire {

const uint8_t VersionedCacheableObjectPartList::FLAG_NULL_TAG = 0;
const uint8_t VersionedCacheableObjectPartList::FLAG_FULL_TAG = 1;
const uint8_t VersionedCacheableObjectPartList::FLAG_TAG_WITH_NEW_ID = 2;
const uint8_t VersionedCacheableObjectPartList::FLAG_TAG_WITH_NUMBER_ID = 3;

void VersionedCacheableObjectPartList::toData(DataOutput& output) const {
  // don't really care about toData() and should never get invoked
  throw UnsupportedOperationException(
      "VersionedCacheableObjectPartList::toData not implemented");
}

void VersionedCacheableObjectPartList::readObjectPart(int32_t index,
                                                      DataInput& input,
                                                      CacheableKeyPtr keyPtr) {
  uint8_t objType = 0;
  CacheableStringPtr exMsgPtr;
  ExceptionPtr ex;
  input.read(&objType);
  CacheablePtr value;
  m_byteArray[index] = objType;
  bool isException = (objType == 2 ? 1 : 0);

  if (isException) {  // Exception case
    // Skip the exception that is in java serialized format, we cant read it.
    int32_t skipLen;
    input.readArrayLen(&skipLen);
    input.advanceCursor(skipLen);

    input.readNativeString(exMsgPtr);  ////4.1
    if (m_exceptions != NULLPTR) {
      const char* exMsg = exMsgPtr->asChar();
      if (strstr(exMsg,
                 "org.apache.geode.security."
                 "NotAuthorizedException") != NULL) {
        ex = new NotAuthorizedException("Authorization exception at server:",
                                        exMsg);
      } else {
        ex = new CacheServerException("Exception at remote server:", exMsg);
      }
      m_exceptions->insert(keyPtr, ex);
    }
  } else if (m_serializeValues) {
    // read length
    int32_t skipLen;
    input.readArrayLen(&skipLen);
    uint8_t* bytes = NULL;
    if (skipLen > 0) {
      // readObject
      bytes = new uint8_t[skipLen];
      input.readBytesOnly(bytes, skipLen);
    }
    CacheableBytesPtr c = CacheableBytes::create(bytes, skipLen);
    value = dynCast<CacheablePtr>(c);
    m_values->insert(keyPtr, value);

    /* adongre
     * CID 29377: Resource leak (RESOURCE_LEAK)Calling allocation function
     * "gemfire::DataInput::readBytes(unsigned char **, int *)" on "bytes".
     */
    GF_SAFE_DELETE_ARRAY(bytes);

  } else {
    // set NULLPTR to indicate that there is no exception for the key on this
    // index
    // readObject
    input.readObject(value);
    if (m_values != NULLPTR) m_values->insert(keyPtr, value);
  }
}

Serializable* VersionedCacheableObjectPartList::fromData(DataInput& input) {
  ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_responseLock);
  LOGDEBUG("VersionedCacheableObjectPartList::fromData");
  uint8_t flags = 0;
  input.read(&flags);
  m_hasKeys = (flags & 0x01) == 0x01;
  bool hasObjects = (flags & 0x02) == 0x02;
  m_hasTags = (flags & 0x04) == 0x04;
  m_regionIsVersioned = (flags & 0x08) == 0x08;
  m_serializeValues = (flags & 0x10) == 0x10;
  bool persistent = (flags & 0x20) == 0x20;
  CacheableKeyPtr key;
  CacheableStringPtr exMsgPtr;
  int32_t len = 0;
  bool valuesNULL = false;
  int32_t keysOffset = (m_keysOffset != NULL ? *m_keysOffset : 0);
  // bool readObjLen = false;
  // int32_t lenOfObjects = 0;
  VectorOfCacheableKeyPtr localKeys(new VectorOfCacheableKey());
  if (m_values == NULLPTR) {
    GF_NEW(m_values, HashMapOfCacheable);
    valuesNULL = true;
  }

  if (!m_hasKeys && !hasObjects && !m_hasTags) {
    LOGDEBUG(
        "VersionedCacheableObjectPartList::fromData: Looks like message has no "
        "data. Returning,");
    return NULL;
  }

  if (m_hasKeys) {
    int64_t tempLen;
    input.readUnsignedVL(&tempLen);
    len = static_cast<int32_t>(tempLen);

    for (int32_t index = 0; index < len; ++index) {
      input.readObject(key, true);
      if (m_resultKeys != NULLPTR) {
        m_resultKeys->push_back(key);
      }
      m_tempKeys->push_back(key);
      localKeys->push_back(key);
    }
  } else if (m_keys != NULL) {
    LOGDEBUG("VersionedCacheableObjectPartList::fromData: m_keys NOT NULL");
    /*
       if (m_hasKeys) {
       int64_t tempLen;
       input.readUnsignedVL(&tempLen);
       len = (int32_t)tempLen;
       }else{
       len = m_keys->size();
       }
       lenOfObjects = len;
       readObjLen = true;
       for (int32_t index = keysOffset; index < keysOffset + len; ++index) {
       key = m_keys->at(index);
       if (m_resultKeys != NULLPTR) {
       m_resultKeys->push_back(key);
       }
       }*/
  } else if (hasObjects) {
    if (m_keys == NULL && m_resultKeys == NULLPTR) {
      LOGERROR(
          "VersionedCacheableObjectPartList::fromData: Exception: hasObjects "
          "is true and m_keys and m_resultKeys are also NULL");
      throw FatalInternalException(
          "VersionedCacheableObjectPartList: "
          "hasObjects is true and m_keys is also NULL");
    } else {
      LOGDEBUG(
          "VersionedCacheableObjectPartList::fromData m_keys or m_resultKeys "
          "not null");
    }
  } else {
    LOGDEBUG(
        "VersionedCacheableObjectPartList::fromData m_hasKeys, m_keys, "
        "hasObjects all are NULL");
  }  // m_hasKeys else ends here

  if (hasObjects) {
    int64_t tempLen;
    input.readUnsignedVL(&tempLen);
    len = static_cast<int32_t>(tempLen);
    m_byteArray.resize(len);
    for (int32_t index = 0; index < len; ++index) {
      if (m_keys != NULL && !m_hasKeys) {
        readObjectPart(index, input, m_keys->at(index + keysOffset));
      } else /*if (m_resultKeys != NULLPTR && m_resultKeys->size() > 0)*/ {
        readObjectPart(index, input, localKeys->at(index));
      } /*else{
         LOGERROR("VersionedCacheableObjectPartList::fromData: hasObjects = true
       but m_keys is NULL and m_resultKeys== NULL or m_resultKeys->size=0" );
       }*/
    }
  }  // hasObjects ends here

  if (m_hasTags) {
    int32_t versionTaglen;
    int64_t tempLen;
    input.readUnsignedVL(&tempLen);
    versionTaglen = static_cast<int32_t>(tempLen);
    len = versionTaglen;
    m_versionTags.resize(versionTaglen);
    std::vector<uint16_t> ids;
    for (int32_t index = 0; index < versionTaglen; index++) {
      uint8_t entryType = 0;
      input.read(&entryType);
      VersionTagPtr versionTag;
      switch (entryType) {
        case FLAG_NULL_TAG: {
          break;
        }
        case FLAG_FULL_TAG: {
          if (persistent) {
            versionTag = VersionTagPtr(new DiskVersionTag());
          } else {
            versionTag = VersionTagPtr(new VersionTag());
          }
          versionTag->fromData(input);
          versionTag->replaceNullMemberId(getEndpointMemId());
          break;
        }

        case FLAG_TAG_WITH_NEW_ID: {
          if (persistent) {
            versionTag = VersionTagPtr(new DiskVersionTag());
          } else {
            versionTag = VersionTagPtr(new VersionTag());
          }
          versionTag->fromData(input);
          ids.push_back(versionTag->getInternalMemID());
          break;
        }

        case FLAG_TAG_WITH_NUMBER_ID: {
          if (persistent) {
            versionTag = VersionTagPtr(new DiskVersionTag());
          } else {
            versionTag = VersionTagPtr(new VersionTag());
          }
          versionTag->fromData(input);
          int32_t idNumber;
          int64_t tempLen;
          input.readUnsignedVL(&tempLen);
          idNumber = static_cast<int32_t>(tempLen);
          versionTag->setInternalMemID(ids.at(idNumber));
          break;
        }
        default: { break; }
      }
      m_versionTags[index] = versionTag;
    }
  } else {  // if consistancyEnabled=false, we need to pass empty or NULLPtr
            // m_versionTags
    for (int32_t index = 0; index < len; ++index) {
      VersionTagPtr versionTag;
      m_versionTags[index] = versionTag;
    }
  }

  if (hasObjects) {
    CacheableKeyPtr key;
    VersionTagPtr versionTag;
    CacheablePtr value;

    for (int32_t index = 0; index < len; ++index) {
      if (m_keys != NULL && !m_hasKeys) {
        key = m_keys->at(index + keysOffset);
      } else /*if (m_resultKeys != NULLPTR && m_resultKeys->size() > 0)*/ {
        key = localKeys->at(index);
      } /*else{
         LOGERROR("VersionedCacheableObjectPartList::fromData: hasObjects = true
       but m_keys is NULL AND m_resultKeys=NULLPTR or m_resultKeys->size=0" );
       }*/

      HashMapOfCacheable::Iterator iter = m_values->find(key);
      value = iter == m_values->end() ? NULLPTR : iter.second();
      if (m_byteArray[index] != 3) {  // 3 - key not found on server
        CacheablePtr oldValue;
        if (m_addToLocalCache) {
          int updateCount = -1;
          versionTag = m_versionTags[index];

          GfErrType err =
              m_region->putLocal("getAll", false, key, value, oldValue, true,
                                 updateCount, m_destroyTracker, versionTag);
          if (err == GF_CACHE_CONCURRENT_MODIFICATION_EXCEPTION) {
            LOGDEBUG(
                "VersionedCacheableObjectPartList::fromData putLocal for key [%s] failed because the cache \
                  already contains an entry with higher version.",
                Utils::getCacheableKeyString(key)->asChar());
            // erase the  value
            m_values->erase(key);
            // add the value with higher version tag
            m_values->insert(key, oldValue);
          }
        }       // END::m_addToLocalCache
        else {  // m_addToLocalCache = false
          m_region->getEntry(key, oldValue);
          // if value has already been received via notification or put by
          // another thread, then return that
          if (oldValue != NULLPTR && !CacheableToken::isInvalid(oldValue)) {
            // erase the old value
            m_values->erase(key);
            // add the value with new value
            m_values->insert(key, oldValue);
          }
        }
      }
    }
  }
  if (m_keysOffset != NULL) *m_keysOffset += len;
  if (valuesNULL) m_values = NULLPTR;
  return this;
}

int32_t VersionedCacheableObjectPartList::classId() const { return 0; }

int8_t VersionedCacheableObjectPartList::typeId() const {
  return GemfireTypeIdsImpl::VersionedObjectPartList;
}

int8_t VersionedCacheableObjectPartList::DSFID() const {
  return GemfireTypeIdsImpl::FixedIDByte;
}

uint32_t VersionedCacheableObjectPartList::objectSize() const { return 0; }
}  // namespace gemfire
