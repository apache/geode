#ifndef __GEMFIRE_IMPL_MAPENTRY_T_HPP__
#define __GEMFIRE_IMPL_MAPENTRY_T_HPP__

/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include <gfcpp/gfcpp_globals.hpp>
#include "MapEntry.hpp"
#include "TrackedMapEntry.hpp"

#define GF_TRACK_MAX 4
#define GF_UPDATE_MAX 8

namespace gemfire {
// this class encapsulates the common functionality of the MapEntryT<>
// generic template class and its specializations
template <typename TBase, int NUM_TRACKERS, int UPDATE_COUNT>
class MapEntryST {
 public:
  static int addTracker(TBase* loc);
  static int removeTracker(TBase* loc);
  static int incUpdateCount(TBase* loc);

 private:
  // disabled
  MapEntryST();
  MapEntryST(const MapEntryST&);
  MapEntryST& operator=(const MapEntryST&);
};

// specialization of MapEntryST for NUM_TRACKERS==0 to terminate
// removeTracker
template <typename TBase, int UPDATE_COUNT>
class MapEntryST<TBase, 0, UPDATE_COUNT> {
 public:
  static int addTracker(TBase* loc);
  static int removeTracker(TBase* loc);
  static int incUpdateCount(TBase* loc);

 private:
  // disabled
  MapEntryST();
  MapEntryST(const MapEntryST&);
  MapEntryST& operator=(const MapEntryST&);
};

/**
 * We use the tracking of entries to deal with updates coming in while
 * another region operation is in progress. Specifically the issue to be
 * tackled is to ensure that updates that come in while remote server part
 * of an operation is in progress should not be overwritten, particularly
 * the notifications. The approach being used is to set a tracking number
 * on the entry before starting the remote operation which will lead to any
 * updates incrementing an "update counter" of the entry. When the operation
 * tries to perform the local cache update part, then it checks the
 * "update counter" and goes ahead only if that has not changed.
 *
 * This template class encapsulates the tracking number and update counter.
 * It completely avoids storing member variables for the above two by
 * having a series of classes each returning a fixed value for these two
 * and using placement new to convert "this" into another type which will
 * return a new tracking number and update counter. This recursive template
 * definition is terminated using specializations so that beyond
 * "GF_TRACK_MAX" and "GF_UPDATE_MAX" we make use of a wrapper
 * <code>TrackedMapEntry</code> class that will store the tracking number
 * and update counter as separate fields in addition to the actual MapEntry.
 * The assumption is that "GF_TRACK_MAX" or "GF_UPDATE_MAX" will be violated
 * very rarely.
 *
 *
 */
template <typename TBase, int NUM_TRACKERS, int UPDATE_COUNT>
class MapEntryT : public TBase {
 public:
  inline explicit MapEntryT(bool noInit) : TBase(true) {}

  virtual ~MapEntryT() {}

  virtual int addTracker(MapEntryPtr& newEntry) {
    return MapEntryST<TBase, NUM_TRACKERS, UPDATE_COUNT>::addTracker(this);
  }

  virtual std::pair<bool, int> removeTracker() {
    return std::make_pair(
        false,
        MapEntryST<TBase, NUM_TRACKERS, UPDATE_COUNT>::removeTracker(this));
  }

  virtual int incrementUpdateCount(MapEntryPtr& newEntry) {
    return MapEntryST<TBase, NUM_TRACKERS, UPDATE_COUNT>::incUpdateCount(this);
  }

  virtual int getTrackingNumber() const { return NUM_TRACKERS; }

  virtual int getUpdateCount() const { return UPDATE_COUNT; }

  inline static MapEntryT* create(const CacheableKeyPtr& key) {
    MapEntryT* entry;
    GF_NEW(entry, MapEntryT(key));
    return entry;
  }

 protected:
  inline MapEntryT(const CacheableKeyPtr& key) : TBase(key) {}

 private:
  // disabled
  MapEntryT(const MapEntryT&);
  MapEntryT& operator=(const MapEntryT&);
};

// specialization of MapEntryT to terminate the recursive template definition
// for update counter == GF_UPDATE_MAX
template <typename TBase, int NUM_TRACKERS>
class MapEntryT<TBase, NUM_TRACKERS, GF_UPDATE_MAX> : public TBase {
 public:
  inline explicit MapEntryT(bool noInit) : TBase(true) {}

  virtual ~MapEntryT() {}

  virtual int addTracker(MapEntryPtr& newEntry) {
    return MapEntryST<TBase, NUM_TRACKERS, GF_UPDATE_MAX>::addTracker(this);
  }

  virtual std::pair<bool, int> removeTracker() {
    return std::make_pair(
        false,
        MapEntryST<TBase, NUM_TRACKERS, GF_UPDATE_MAX>::removeTracker(this));
  }

  virtual int incrementUpdateCount(MapEntryPtr& newEntry) {
    // fallback to TrackedMapEntry
    GF_NEW(newEntry, TrackedMapEntry(this, NUM_TRACKERS, GF_UPDATE_MAX + 1));
    return (GF_UPDATE_MAX + 1);
  }

  virtual int getTrackingNumber() const { return NUM_TRACKERS; }

  virtual int getUpdateCount() const { return GF_UPDATE_MAX; }

 private:
  // disabled
  MapEntryT(const MapEntryT&);
  MapEntryT& operator=(const MapEntryT&);
};

// specialization of MapEntryT to terminate the recursive template definition
// for tracking number == GF_TRACK_MAX
template <typename TBase, int UPDATE_COUNT>
class MapEntryT<TBase, GF_TRACK_MAX, UPDATE_COUNT> : public TBase {
 public:
  inline explicit MapEntryT(bool noInit) : TBase(true) {}

  virtual ~MapEntryT() {}

  virtual int addTracker(MapEntryPtr& newEntry) {
    // fallback to TrackedMapEntry
    GF_NEW(newEntry, TrackedMapEntry(this, GF_TRACK_MAX + 1, UPDATE_COUNT));
    return UPDATE_COUNT;
  }

  virtual std::pair<bool, int> removeTracker() {
    return std::make_pair(
        false,
        MapEntryST<TBase, GF_TRACK_MAX, UPDATE_COUNT>::removeTracker(this));
  }

  virtual int incrementUpdateCount(MapEntryPtr& newEntry) {
    return MapEntryST<TBase, GF_TRACK_MAX, UPDATE_COUNT>::incUpdateCount(this);
  }

  virtual int getTrackingNumber() const { return GF_TRACK_MAX; }

  virtual int getUpdateCount() const { return UPDATE_COUNT; }

 private:
  // disabled
  MapEntryT(const MapEntryT&);
  MapEntryT& operator=(const MapEntryT&);
};

// specialization of MapEntryT to terminate the recursive template definition
// for tracking number == GF_TRACK_MAX and update counter == GF_UPDATE_MAX
template <typename TBase>
class MapEntryT<TBase, GF_TRACK_MAX, GF_UPDATE_MAX> : public TBase {
 public:
  inline explicit MapEntryT(bool noInit) : TBase(true) {}

  virtual ~MapEntryT() {}

  virtual int addTracker(MapEntryPtr& newEntry) {
    // fallback to TrackedMapEntry
    GF_NEW(newEntry, TrackedMapEntry(this, GF_TRACK_MAX + 1, GF_UPDATE_MAX));
    return GF_UPDATE_MAX;
  }

  virtual std::pair<bool, int> removeTracker() {
    return std::make_pair(
        false,
        MapEntryST<TBase, GF_TRACK_MAX, GF_UPDATE_MAX>::removeTracker(this));
  }

  virtual int incrementUpdateCount(MapEntryPtr& newEntry) {
    // fallback to TrackedMapEntry
    GF_NEW(newEntry, TrackedMapEntry(this, GF_TRACK_MAX, GF_UPDATE_MAX + 1));
    return (GF_UPDATE_MAX + 1);
  }

  virtual int getTrackingNumber() const { return GF_TRACK_MAX; }

  virtual int getUpdateCount() const { return GF_UPDATE_MAX; }

 private:
  // disabled
  MapEntryT(const MapEntryT&);
  MapEntryT& operator=(const MapEntryT&);
};

template <typename TBase, int NUM_TRACKERS, int UPDATE_COUNT>
inline int MapEntryST<TBase, NUM_TRACKERS, UPDATE_COUNT>::addTracker(
    TBase* loc) {
  // use placement new to make vptr point to class that shall have
  // tracking number incremented by one
  // invoke the constructor that will avoid initializing *any* data
  (void)new (loc) MapEntryT<TBase, NUM_TRACKERS + 1, UPDATE_COUNT>(true);
  return UPDATE_COUNT;
}

template <typename TBase, int NUM_TRACKERS, int UPDATE_COUNT>
inline int MapEntryST<TBase, NUM_TRACKERS, UPDATE_COUNT>::removeTracker(
    TBase* loc) {
  // use placement new to make vptr point to class that shall have
  // tracking number decremented by one
  // invoke the constructor that will avoid initializing *any* data
  if (NUM_TRACKERS == 1) {
    // if all trackers have gone away then the update counter is no
    // longer required, so reset it to zero
    (void)new (loc) MapEntryT<TBase, 0, 0>(true);
  } else {
    (void)new (loc) MapEntryT<TBase, NUM_TRACKERS - 1, UPDATE_COUNT>(true);
  }
  return (NUM_TRACKERS - 1);
}

template <typename TBase, int NUM_TRACKERS, int UPDATE_COUNT>
inline int MapEntryST<TBase, NUM_TRACKERS, UPDATE_COUNT>::incUpdateCount(
    TBase* loc) {
  // use placement new to make vptr point to class that shall have
  // update number incremented by one
  // invoke the constructor that will avoid initializing *any* data
  (void)new (loc) MapEntryT<TBase, NUM_TRACKERS, UPDATE_COUNT + 1>(true);
  return (UPDATE_COUNT + 1);
}

template <typename TBase, int UPDATE_COUNT>
inline int MapEntryST<TBase, 0, UPDATE_COUNT>::addTracker(TBase* loc) {
  // use placement new to make vptr point to class that shall have
  // tracking number incremented by one
  // invoke the constructor that will avoid initializing *any* data
  (void)new (loc) MapEntryT<TBase, 1, UPDATE_COUNT>(true);
  return UPDATE_COUNT;
}

template <typename TBase, int UPDATE_COUNT>
inline int MapEntryST<TBase, 0, UPDATE_COUNT>::removeTracker(TBase* loc) {
  return 0;
}

template <typename TBase, int UPDATE_COUNT>
inline int MapEntryST<TBase, 0, UPDATE_COUNT>::incUpdateCount(TBase* loc) {
  return UPDATE_COUNT;
}
}

#endif  // __GEMFIRE_IMPL_MAPENTRY_T_HPP__
