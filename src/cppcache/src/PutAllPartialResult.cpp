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
