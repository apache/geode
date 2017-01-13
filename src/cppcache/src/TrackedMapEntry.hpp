/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#ifndef __GEMFIRE_IMPL_TRACKEDMAPENTRY_HPP__
#define __GEMFIRE_IMPL_TRACKEDMAPENTRY_HPP__

#include "MapEntry.hpp"

namespace gemfire {

class TrackedMapEntry : public MapEntry {
 public:
  // Constructor should be invoked only when starting the tracking
  // of a MapEntry, so m_trackingNumber is initialized with 1.
  inline TrackedMapEntry(const MapEntryImpl* entry, int trackingNumber,
                         int updateCount)
      : m_entry(entry),
        m_trackingNumber(trackingNumber),
        m_updateCount(updateCount) {}

  virtual ~TrackedMapEntry() {}

  virtual MapEntryImpl* getImplPtr() { return m_entry.ptr(); }

  virtual int addTracker(MapEntryPtr& newEntry) {
    ++m_trackingNumber;
    return m_updateCount;
  }

  virtual std::pair<bool, int> removeTracker() {
    if (m_trackingNumber > 0) {
      --m_trackingNumber;
    }
    if (m_trackingNumber == 0) {
      m_updateCount = 0;
      return std::make_pair(true, 0);
    }
    return std::make_pair(false, m_trackingNumber);
  }

  virtual int incrementUpdateCount(MapEntryPtr& newEntry) {
    return ++m_updateCount;
  }

  virtual int getTrackingNumber() const { return m_trackingNumber; }

  virtual int getUpdateCount() const { return m_updateCount; }

  virtual void getKey(CacheableKeyPtr& result) const;
  virtual void getValue(CacheablePtr& result) const;
  virtual void setValue(const CacheablePtr& value);
  virtual LRUEntryProperties& getLRUProperties();
  virtual ExpEntryProperties& getExpProperties();
  virtual VersionStamp& getVersionStamp();
  virtual void cleanup(const CacheEventFlags eventFlags);

 private:
  MapEntryImplPtr m_entry;
  int m_trackingNumber;
  int m_updateCount;
};
}

#endif /* __GEMFIRE_IMPL_TRACKEDMAPENTRY_HPP__ */
