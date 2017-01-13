/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __GEMFIRE_IMPL_LRUMAPENTRY_H__
#define __GEMFIRE_IMPL_LRUMAPENTRY_H__

#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/CacheableKey.hpp>
#include "MapEntry.hpp"
#include "LRUList.hpp"
#include "VersionStamp.hpp"

namespace gemfire {
/**
 * This template class adds the recently used, eviction bits and persistence
 * info to the MapEntry class. The earlier design looked like below:
 *    LRUListNode     MapEntry
 *          \           /
 *           \         /
 *            \       /
 *           LRUMapEntry
 * This kept the implementation of LRUListNode independent from MapEntry
 * and let create LRUMapEntry using virtual MI to work as expected.
 * However, having LRUMapEntry as a list node meant that node properties
 * also went into the EntriesMap causing problems during unbind_all of
 * the MapEntry which would try to recursively destroy its successor
 * (bug #226). The primary problem with this design is that there is
 * no reason for a MapEntry to inherit node like properties of having
 * a successor. So we will like to split out list node functionality
 * to be inside the LRUList class, while just adding LRU bits to the
 * MapEntry. So we will like to have something like:
 *      ------------------------------
 *      |         LRUListNode        |
 *      |                            |
 *      |  LRUEntryProperties        | MapEntryImpl
 *      |       \                    |   /
 *      |        \           --------|---
 *      |         \         /        |
 *      |         LRUMapEntry        |
 *      |----------------------------|
 *
 *
 */
class CPPCACHE_EXPORT LRUMapEntry : public MapEntryImpl,
                                    public LRUEntryProperties {
 public:
  virtual ~LRUMapEntry() {}

  virtual LRUEntryProperties& getLRUProperties() { return *this; }

  virtual void cleanup(const CacheEventFlags eventFlags) {
    if (!eventFlags.isEviction()) {
      // TODO:  this needs an implementation of doubly-linked list
      // to remove from the list; also add this to LRUExpMapEntry since MI
      // has been removed
    }
  }

 protected:
  inline explicit LRUMapEntry(bool noInit)
      : MapEntryImpl(true), LRUEntryProperties(true) {}

  inline LRUMapEntry(const CacheableKeyPtr& key) : MapEntryImpl(key) {}

 private:
  // disabled
  LRUMapEntry(const LRUMapEntry&);
  LRUMapEntry& operator=(const LRUMapEntry&);
};

typedef SharedPtr<LRUMapEntry> LRUMapEntryPtr;

class CPPCACHE_EXPORT VersionedLRUMapEntry : public LRUMapEntry,
                                             public VersionStamp {
 public:
  virtual ~VersionedLRUMapEntry() {}

  virtual VersionStamp& getVersionStamp() { return *this; }

 protected:
  inline explicit VersionedLRUMapEntry(bool noInit) : LRUMapEntry(true) {}

  inline VersionedLRUMapEntry(const CacheableKeyPtr& key) : LRUMapEntry(key) {}

 private:
  // disabled
  VersionedLRUMapEntry(const VersionedLRUMapEntry&);
  VersionedLRUMapEntry& operator=(const VersionedLRUMapEntry&);
};

typedef SharedPtr<VersionedLRUMapEntry> VersionedLRUMapEntryPtr;

class CPPCACHE_EXPORT LRUEntryFactory : public EntryFactory {
 public:
  static LRUEntryFactory* singleton;
  static void init();

  LRUEntryFactory() {}

  virtual ~LRUEntryFactory() {}

  virtual void newMapEntry(const CacheableKeyPtr& key,
                           MapEntryImplPtr& result) const;
};

}  // namespace gemfire

#endif  //__GEMFIRE_IMPL_LRUMAPENTRY_H__
