#pragma once

#ifndef GEODE_PUTALLPARTIALRESULT_H_
#define GEODE_PUTALLPARTIALRESULT_H_

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

#include <gfcpp/Serializable.hpp>
#include <gfcpp/CacheableString.hpp>
#include "VersionedCacheableObjectPartList.hpp"
#include <ace/Task.h>

namespace apache {
namespace geode {
namespace client {
class PutAllPartialResult;
typedef SharedPtr<PutAllPartialResult> PutAllPartialResultPtr;

class PutAllPartialResult : public Serializable {
 private:
  VersionedCacheableObjectPartListPtr m_succeededKeys;
  CacheableKeyPtr m_firstFailedKey;
  ExceptionPtr m_firstCauseOfFailure;
  int32_t m_totalMapSize;
  ACE_RW_Thread_Mutex g_readerWriterLock;

 public:
  PutAllPartialResult(int totalMapSize,
                      ACE_Recursive_Thread_Mutex& responseLock);

  void setTotalMapSize(int totalMapSize) { m_totalMapSize = totalMapSize; }

  // Add all succeededKeys and firstfailedKey.
  // Before calling this, we must read PutAllPartialResultServerException and
  // formulate obj of type PutAllPartialResult.
  void consolidate(PutAllPartialResultPtr other);

  ExceptionPtr getFailure() { return m_firstCauseOfFailure; }

  void addKeysAndVersions(VersionedCacheableObjectPartListPtr keysAndVersion);

  void addKeys(VectorOfCacheableKeyPtr m_keys);

  void saveFailedKey(CacheableKeyPtr key, ExceptionPtr cause) {
    if (key == NULLPTR) {
      return;
    }
    // TODO:: Do we need to handle server cancelException.
    if (m_firstFailedKey == NULLPTR /*|| cause instanceof CaccelException */) {
      m_firstFailedKey = key;
      m_firstCauseOfFailure = cause;
    }
  }

  VersionedCacheableObjectPartListPtr getSucceededKeysAndVersions();

  // Returns the first key that failed
  CacheableKeyPtr getFirstFailedKey() { return m_firstFailedKey; }

  // Returns there's failedKeys
  bool hasFailure() { return m_firstFailedKey != NULLPTR; }

  // Returns there's saved succeed keys
  bool hasSucceededKeys();

  virtual CacheableStringPtr toString() const {
    char msgStr1[1024];
    if (m_firstFailedKey != NULLPTR) {
      ACE_OS::snprintf(msgStr1, 1024, "[ Key =%s ]",
                       m_firstFailedKey->toString()->asChar());
    }

    char msgStr2[1024];
    if (m_totalMapSize > 0) {
      // TODO:: impl. CacheableObjectPartList.size();
      int failedKeyNum = m_totalMapSize - m_succeededKeys->size();
      if (failedKeyNum > 0) {
        ACE_OS::snprintf(
            msgStr2, 1024,
            "The putAll operation failed to put %d out of %d entries ",
            failedKeyNum, m_totalMapSize);
      } else {
        ACE_OS::snprintf(
            msgStr2, 1024,
            "The putAll operation successfully put %d out of %d entries ",
            m_succeededKeys->size(), m_totalMapSize);
      }
    }

    char stringBuf[7000];
    ACE_OS::snprintf(stringBuf, 7000, "PutAllPartialResult: %s%s", msgStr1,
                     msgStr2);
    return CacheableString::create(stringBuf);
  }

  void toData(DataOutput& output) const {
    throw IllegalStateException(
        "PutAllPartialResult::toData is not intended for use.");
  }

  Serializable* fromData(DataInput& input) {
    throw IllegalStateException(
        "PutAllPartialResult::fromData is not intended for use.");
    return NULL;
  }

  int32_t classId() const {
    throw IllegalStateException(
        "PutAllPartialResult::classId is not intended for use.");
    return 0;
  }

  uint32_t objectSize() const {
    throw IllegalStateException(
        "PutAllPartialResult::objectSize is not intended for use.");
    return 0;
  }

  int8_t typeId() const { return static_cast<int8_t>(0); }
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_PUTALLPARTIALRESULT_H_
