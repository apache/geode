/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __GEMFIRE_IMPL_LRUEXPMAPENTRY_H__
#define __GEMFIRE_IMPL_LRUEXPMAPENTRY_H__

#include <gfcpp/gfcpp_globals.hpp>
#include "MapEntry.hpp"
#include "LRUList.hpp"
#include "VersionStamp.hpp"

namespace gemfire {
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
}  // namespace gemfire

#endif  //__GEMFIRE_IMPL_LRUEXPMAPENTRY_H__
