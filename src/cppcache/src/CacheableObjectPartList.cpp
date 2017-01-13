/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "CacheableObjectPartList.hpp"
#include <gfcpp/ExceptionTypes.hpp>
#include "GemfireTypeIdsImpl.hpp"
#include <gfcpp/CacheableString.hpp>
#include "ThinClientRegion.hpp"
#include "CacheableToken.hpp"

namespace gemfire {

void CacheableObjectPartList::toData(DataOutput& output) const {
  // don't really care about toData() and should never get invoked
  throw UnsupportedOperationException(
      "CacheableObjectPartList::toData not implemented");
}

Serializable* CacheableObjectPartList::fromData(DataInput& input) {
  bool hasKeys;
  input.readBoolean(&hasKeys);
  int32_t len;
  input.readInt(&len);
  if (len > 0) {
    CacheableKeyPtr key;
    CacheablePtr value;
    CacheableStringPtr exMsgPtr;
    ExceptionPtr ex;
    // bool isException;
    int32_t keysOffset = (m_keysOffset != NULL ? *m_keysOffset : 0);
    for (int32_t index = keysOffset; index < keysOffset + len; ++index) {
      if (hasKeys) {
        input.readObject(key, true);
      } else if (m_keys != NULL) {
        key = m_keys->operator[](index);
      } else {
        throw FatalInternalException(
            "CacheableObjectPartList: "
            "hasKeys is false and m_keys is also NULL");
      }
      if (m_resultKeys != NULLPTR) {
        m_resultKeys->push_back(key);
      }
      // input.readBoolean(&isException);
      uint8_t byte = 0;
      input.read(&byte);

      if (byte == 2 /* for exception*/) {
        int32_t skipLen;
        input.readArrayLen(&skipLen);
        input.advanceCursor(skipLen);
        // input.readObject(exMsgPtr, true);// Changed
        input.readNativeString(exMsgPtr);
        if (m_exceptions != NULLPTR) {
          const char* exMsg = exMsgPtr->asChar();
          if (strstr(exMsg,
                     "org.apache.geode.security."
                     "NotAuthorizedException") != NULL) {
            ex = new NotAuthorizedException(
                "Authorization exception at server:", exMsg);
          } else {
            ex = new CacheServerException("Exception at remote server:", exMsg);
          }
          m_exceptions->insert(key, ex);
        }
      } else {
        input.readObject(value);
        CacheablePtr oldValue;
        if (m_addToLocalCache) {
          // for both  register interest  and getAll it is desired
          // to overwrite an invalidated entry
          // TODO: what about destroyed token? need to handle
          // destroys during  register interest  by not creating them
          // same for invalidates?
          int updateCount = -1;
          MapOfUpdateCounters::iterator pos = m_updateCountMap->find(key);
          if (pos != m_updateCountMap->end()) {
            updateCount = pos->second;
            m_updateCountMap->erase(pos);
          }
          VersionTagPtr versionTag;
          GfErrType err =
              m_region->putLocal("getAll", false, key, value, oldValue, true,
                                 updateCount, m_destroyTracker, versionTag);
          if (err == GF_CACHE_CONCURRENT_MODIFICATION_EXCEPTION) {
            LOGDEBUG(
                "CacheableObjectPartList::fromData putLocal for key [%s] failed because the cache \
                already contains an entry with higher version.",
                Utils::getCacheableKeyString(key)->asChar());
          }
        } else {
          m_region->getEntry(key, oldValue);
        }
        // if value has already been received via notification or put by
        // another thread, then return that
        if (oldValue != NULLPTR && !CacheableToken::isInvalid(oldValue)) {
          value = oldValue;
        }
        if (m_values != NULLPTR) {
          m_values->insert(key, value);
        }
      }
    }
    if (m_keysOffset != NULL) {
      *m_keysOffset += len;
    }
  }
  return this;
}

int32_t CacheableObjectPartList::classId() const { return 0; }

int8_t CacheableObjectPartList::typeId() const {
  return GemfireTypeIdsImpl::CacheableObjectPartList;
}

int8_t CacheableObjectPartList::DSFID() const {
  return GemfireTypeIdsImpl::FixedIDByte;
}

uint32_t CacheableObjectPartList::objectSize() const { return 0; }
}  // namespace gemfire
