/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __GEMFIRE_IMPL_EXPMAPENTRY_H__
#define __GEMFIRE_IMPL_EXPMAPENTRY_H__

#include <gfcpp/gfcpp_globals.hpp>
#include "MapEntry.hpp"
#include "VersionStamp.hpp"

namespace gemfire {
/**
 * @brief Hold region mapped entry value.
 * This subclass adds expiration times.
 */
class CPPCACHE_EXPORT ExpMapEntry : public MapEntryImpl,
                                    public ExpEntryProperties {
 public:
  virtual ~ExpMapEntry() {}

  virtual ExpEntryProperties& getExpProperties() { return *this; }

  virtual void cleanup(const CacheEventFlags eventFlags) {
    if (!eventFlags.isExpiration()) {
      cancelExpiryTaskId(m_key);
    }
  }

 protected:
  // this constructor deliberately skips touching or initializing any members
  inline explicit ExpMapEntry(bool noInit)
      : MapEntryImpl(true), ExpEntryProperties(true) {}

  inline ExpMapEntry(const CacheableKeyPtr& key) : MapEntryImpl(key) {}

 private:
  // disabled
  ExpMapEntry(const ExpMapEntry&);
  ExpMapEntry& operator=(const ExpMapEntry&);
};

typedef SharedPtr<ExpMapEntry> ExpMapEntryPtr;

class CPPCACHE_EXPORT VersionedExpMapEntry : public ExpMapEntry,
                                             public VersionStamp {
 public:
  virtual ~VersionedExpMapEntry() {}

  virtual VersionStamp& getVersionStamp() { return *this; }

 protected:
  inline explicit VersionedExpMapEntry(bool noInit) : ExpMapEntry(true) {}

  inline VersionedExpMapEntry(const CacheableKeyPtr& key) : ExpMapEntry(key) {}

 private:
  // disabled
  VersionedExpMapEntry(const VersionedExpMapEntry&);
  VersionedExpMapEntry& operator=(const VersionedExpMapEntry&);
};

typedef SharedPtr<VersionedExpMapEntry> VersionedExpMapEntryPtr;

class CPPCACHE_EXPORT ExpEntryFactory : public EntryFactory {
 public:
  static ExpEntryFactory* singleton;
  static void init();

  ExpEntryFactory() {}

  virtual ~ExpEntryFactory() {}

  virtual void newMapEntry(const CacheableKeyPtr& key,
                           MapEntryImplPtr& result) const;
};
}

#endif  // __GEMFIRE_IMPL_EXPMAPENTRY_H__
