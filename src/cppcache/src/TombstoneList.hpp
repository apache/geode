/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#ifndef __GEMFIRE_IMPL_TOMBSTONELIST_H__
#define __GEMFIRE_IMPL_TOMBSTONELIST_H__

#include <list>
#include <ace/Recursive_Thread_Mutex.h>
#include "ace/Guard_T.h"
#include <gfcpp/SharedPtr.hpp>
#include <gfcpp/CacheableBuiltins.hpp>
#include <gfcpp/HashMapT.hpp>
#include <gfcpp/HashSetT.hpp>
#include "MapEntry.hpp"

namespace gemfire {
class MapSegment;
class TombstoneExpiryHandler;
class TombstoneEntry : public SharedBase {
 public:
  TombstoneEntry(MapEntryImpl* entry, int64_t tombstoneCreationTime)
      : m_entry(entry),
        m_tombstoneCreationTime(tombstoneCreationTime),
        /* adongre
         * Coverity - II
         * CID 29289: Uninitialized scalar field (UNINIT_CTOR)
         * Non-static class member "m_expiryTaskId" is not initialized in this
         * constructor nor in any functions that it calls.
         * Fix : Initialize the member
         * also change the member initialization to initializer list
         */
        m_expiryTaskId(0),
        m_handler(NULL) {}
  virtual ~TombstoneEntry() {}
  MapEntryImpl* getEntry() { return m_entry; }
  int64_t getTombstoneCreationTime() { return m_tombstoneCreationTime; }
  int64_t getExpiryTaskId() { return m_expiryTaskId; }
  void setExpiryTaskId(int64_t expiryTaskId) { m_expiryTaskId = expiryTaskId; }
  TombstoneExpiryHandler* getHandler() { return m_handler; };
  void setHandler(TombstoneExpiryHandler* handler) { m_handler = handler; };

 private:
  MapEntryImpl* m_entry;
  int64_t m_tombstoneCreationTime;
  int64_t m_expiryTaskId;
  TombstoneExpiryHandler* m_handler;
};
typedef SharedPtr<TombstoneEntry> TombstoneEntryPtr;

class TombstoneList : public SharedBase {
 public:
  TombstoneList(MapSegment* mapSegment) { m_mapSegment = mapSegment; }
  virtual ~TombstoneList() { cleanUp(); }
  void add(RegionInternal* rptr, MapEntryImpl* entry,
           TombstoneExpiryHandler* handler, long taskID);

  // Reaps the tombstones which have been gc'ed on server.
  // A map that has identifier for ClientProxyMembershipID as key
  // and server version of the tombstone with highest version as the
  // value is passed as paramter
  void reapTombstones(std::map<uint16_t, int64_t>& gcVersions);
  void reapTombstones(CacheableHashSetPtr removedKeys);
  void eraseEntryFromTombstoneList(CacheableKeyPtr key, RegionInternal* region,
                                   bool cancelTask = true);
  long eraseEntryFromTombstoneListWithoutCancelTask(
      CacheableKeyPtr key, RegionInternal* region,
      TombstoneExpiryHandler*& handler);
  bool getEntryFromTombstoneList(CacheableKeyPtr key);
  void cleanUp();
  long getExpiryTask(TombstoneExpiryHandler** handler);

 private:
  void removeEntryFromMapSegment(CacheableKeyPtr key);
  void unguardedRemoveEntryFromMapSegment(CacheableKeyPtr key);
  HashMapT<CacheableKeyPtr, TombstoneEntryPtr> m_tombstoneMap;
  ACE_Recursive_Thread_Mutex m_queueLock;
  MapSegment* m_mapSegment;
  friend class TombstoneExpiryHandler;
};
typedef SharedPtr<TombstoneList> TombstoneListPtr;

}  // end namespace

#endif  // !defined (__GEMFIRE_IMPL_TOMBSTONELIST_H__)
