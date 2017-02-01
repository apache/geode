#pragma once

#ifndef GEODE_LRUEXPMAPENTRY_H_
#define GEODE_LRUEXPMAPENTRY_H_

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
#include "MapEntry.hpp"
#include "LRUList.hpp"
#include "VersionStamp.hpp"

namespace apache {
namespace geode {
namespace client {
/**
 * @brief Hold region mapped entry value and lru information.
 */
class CPPCACHE_EXPORT LRUExpMapEntry : public MapEntryImpl,
                                       public LRUEntryProperties,
                                       public ExpEntryProperties {
 public:
  virtual ~LRUExpMapEntry() {}

  virtual LRUEntryProperties& getLRUProperties() { return *this; }

  virtual ExpEntryProperties& getExpProperties() { return *this; }

  virtual void cleanup(const CacheEventFlags eventFlags) {
    if (!eventFlags.isExpiration()) {
      cancelExpiryTaskId(m_key);
    }
  }

 protected:
  inline explicit LRUExpMapEntry(bool noInit)
      : MapEntryImpl(true),
        LRUEntryProperties(true),
        ExpEntryProperties(true) {}

  inline LRUExpMapEntry(const CacheableKeyPtr& key) : MapEntryImpl(key) {}

 private:
  // disabled
  LRUExpMapEntry(const LRUExpMapEntry&);
  LRUExpMapEntry& operator=(const LRUExpMapEntry&);
};

typedef SharedPtr<LRUExpMapEntry> LRUExpMapEntryPtr;

class CPPCACHE_EXPORT VersionedLRUExpMapEntry : public LRUExpMapEntry,
                                                public VersionStamp {
 public:
  virtual ~VersionedLRUExpMapEntry() {}

  virtual VersionStamp& getVersionStamp() { return *this; }

 protected:
  inline explicit VersionedLRUExpMapEntry(bool noInit) : LRUExpMapEntry(true) {}

  inline VersionedLRUExpMapEntry(const CacheableKeyPtr& key)
      : LRUExpMapEntry(key) {}

 private:
  // disabled
  VersionedLRUExpMapEntry(const VersionedLRUExpMapEntry&);
  VersionedLRUExpMapEntry& operator=(const VersionedLRUExpMapEntry&);
};

typedef SharedPtr<VersionedLRUExpMapEntry> VersionedLRUExpMapEntryPtr;

class CPPCACHE_EXPORT LRUExpEntryFactory : public EntryFactory {
 public:
  static LRUExpEntryFactory* singleton;
  static void init();

  LRUExpEntryFactory() {}

  virtual ~LRUExpEntryFactory() {}

  virtual void newMapEntry(const CacheableKeyPtr& key,
                           MapEntryImplPtr& result) const;
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_LRUEXPMAPENTRY_H_
