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

#include "LRULocalDestroyAction.hpp"
#include "LRUEntriesMap.hpp"
#include "CacheImpl.hpp"

using namespace gemfire;

bool LRULocalDestroyAction::evict(const MapEntryImplPtr& mePtr) {
  CacheableKeyPtr keyPtr;
  mePtr->getKeyI(keyPtr);
  VersionTagPtr versionTag;
  //  we should invoke the destroyNoThrow with appropriate
  // flags to correctly invoke listeners
  LOGDEBUG("LRULocalDestroy: evicting entry with key [%s]",
           Utils::getCacheableKeyString(keyPtr)->asChar());
  GfErrType err = m_regionPtr->destroyNoThrow(
      keyPtr, NULLPTR, -1, CacheEventFlags::EVICTION | CacheEventFlags::LOCAL,
      versionTag);
  return (err == GF_NOERR);
}
