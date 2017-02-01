#pragma once

#ifndef GEODE_LRUACTION_H_
#define GEODE_LRUACTION_H_

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

#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/Cache.hpp>
#include <gfcpp/PersistenceManager.hpp>
#include "MapEntry.hpp"
#include "CacheableToken.hpp"
#include "RegionInternal.hpp"
#include "Utils.hpp"

namespace apache {
namespace geode {
namespace client {

/**
 * @brief abstract behavior for different eviction actions.
 */
class LRUEntriesMap;
class CPPCACHE_EXPORT LRUAction {
 protected:
  bool m_invalidates;
  bool m_destroys;
  bool m_distributes;
  bool m_overflows;

  LRUAction() {
    m_invalidates = false;
    m_destroys = false;
    m_distributes = false;
    m_overflows = false;
  }

 public:
  // types of action

  typedef enum {
    /** When the region or cached object expires, it is invalidated. */
    INVALIDATE = 0,
    /** When expired, invalidated locally only. */
    LOCAL_INVALIDATE,

    /** When the region or cached object expires, it is destroyed. */
    DESTROY,
    /** When expired, destroyed locally only. */
    LOCAL_DESTROY,

    /** invalid type. */
    INVALID_ACTION,

    /** over flow type */
    OVERFLOW_TO_DISK
  } Action;

 public:
  static LRUAction* newLRUAction(const LRUAction::Action& lruAction,
                                 RegionInternal* regionPtr,
                                 LRUEntriesMap* entriesMapPtr);

  virtual ~LRUAction() {}

  virtual bool evict(const MapEntryImplPtr& mePtr) = 0;

  virtual LRUAction::Action getType() = 0;

  inline bool invalidates() { return m_invalidates; }

  inline bool destroys() { return m_destroys; }

  inline bool distributes() { return m_distributes; }

  inline bool overflows() { return m_overflows; }
};

/**
 * @brief LRUAction for destroy (distributed)
 */
class CPPCACHE_EXPORT LRUDestroyAction : public virtual LRUAction {
 private:
  RegionInternal* m_regionPtr;
  // UNUSED LRUEntriesMap* m_entriesMapPtr;

  LRUDestroyAction(RegionInternal* regionPtr, LRUEntriesMap* entriesMapPtr)
      : m_regionPtr(regionPtr)  // UNUSED , m_entriesMapPtr(entriesMapPtr)
  {
    m_destroys = true;
    m_distributes = true;
  }

 public:
  virtual ~LRUDestroyAction() {}

  virtual bool evict(const MapEntryImplPtr& mePtr) {
    CacheableKeyPtr keyPtr;
    mePtr->getKeyI(keyPtr);
    VersionTagPtr versionTag;
    //  we should invoke the destroyNoThrow with appropriate
    // flags to correctly invoke listeners
    LOGDEBUG("LRUDestroy: evicting entry with key [%s]",
             Utils::getCacheableKeyString(keyPtr)->asChar());
    GfErrType err = GF_NOERR;
    if (!m_regionPtr->isDestroyed()) {
      err = m_regionPtr->destroyNoThrow(keyPtr, NULLPTR, -1,
                                        CacheEventFlags::EVICTION, versionTag);
    }
    return (err == GF_NOERR);
  }

  virtual LRUAction::Action getType() { return LRUAction::DESTROY; }

  friend class LRUAction;
};

/**
 * @brief LRUAction for invalidate.
 */
class CPPCACHE_EXPORT LRULocalInvalidateAction : public virtual LRUAction {
 private:
  RegionInternal* m_regionPtr;
  // UNUSED LRUEntriesMap* m_entriesMapPtr;

  LRULocalInvalidateAction(RegionInternal* regionPtr,
                           LRUEntriesMap* entriesMapPtr)
      : m_regionPtr(regionPtr)  // UNUSED , m_entriesMapPtr(entriesMapPtr)
  {
    m_invalidates = true;
  }

 public:
  virtual ~LRULocalInvalidateAction() {}

  virtual bool evict(const MapEntryImplPtr& mePtr);

  virtual LRUAction::Action getType() { return LRUAction::LOCAL_INVALIDATE; }

  friend class LRUAction;
};

/**
 * @brief LRUAction for invalidate.
 */
class CPPCACHE_EXPORT LRUOverFlowToDiskAction : public virtual LRUAction {
 private:
  RegionInternal* m_regionPtr;
  LRUEntriesMap* m_entriesMapPtr;

  LRUOverFlowToDiskAction(RegionInternal* regionPtr,
                          LRUEntriesMap* entriesMapPtr)
      : m_regionPtr(regionPtr), m_entriesMapPtr(entriesMapPtr) {
    m_overflows = true;
  }

 public:
  virtual ~LRUOverFlowToDiskAction() {}

  virtual bool evict(const MapEntryImplPtr& mePtr);

  virtual LRUAction::Action getType() { return LRUAction::OVERFLOW_TO_DISK; }

  friend class LRUAction;
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_LRUACTION_H_
