/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "PutAllPartialResult.hpp"

namespace gemfire {

PutAllPartialResult::PutAllPartialResult(
    int totalMapSize, ACE_Recursive_Thread_Mutex& responseLock) {
  m_succeededKeys = new VersionedCacheableObjectPartList(
      new VectorOfCacheableKey(), responseLock);
  m_totalMapSize = totalMapSize;
}

// Add all succeededKeys and firstfailedKey.
// Before calling this, we must read PutAllPartialResultServerException and
// formulate obj of type PutAllPartialResult.
void PutAllPartialResult::consolidate(PutAllPartialResultPtr other) {
  {
    WriteGuard guard(g_readerWriterLock);
    m_succeededKeys->addAll(other->getSucceededKeysAndVersions());
  }
  saveFailedKey(other->m_firstFailedKey, other->m_firstCauseOfFailure);
}

void PutAllPartialResult::addKeysAndVersions(
    VersionedCacheableObjectPartListPtr keysAndVersion) {
  this->m_succeededKeys->addAll(keysAndVersion);
}

void PutAllPartialResult::addKeys(VectorOfCacheableKeyPtr m_keys) {
  {
    WriteGuard guard(g_readerWriterLock);
    if (m_succeededKeys->getVersionedTagsize() > 0) {
      throw IllegalStateException(
          "attempt to store versionless keys when there are already versioned "
          "results");
    }
    this->m_succeededKeys->addAllKeys(m_keys);
  }
}

VersionedCacheableObjectPartListPtr
PutAllPartialResult::getSucceededKeysAndVersions() {
  return m_succeededKeys;
}

bool PutAllPartialResult::hasSucceededKeys() {
  return this->m_succeededKeys->size() > 0;
}
}  // namespace gemfire
