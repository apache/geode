#ifndef __GEMFIRE_REGIONENTRY_H__
#define __GEMFIRE_REGIONENTRY_H__
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include "gfcpp_globals.hpp"
#include "gf_types.hpp"
#include "CacheableKey.hpp"
#include "CacheStatistics.hpp"

/**
 * @file
 */

namespace gemfire {

class RegionInternal;

/**
 * @class RegionEntry RegionEntry.hpp
 * An object in a Region that represents an entry, i.e., a key-value pair.
 *
 * This object's
 * operations are not distributed, do not acquire any locks, and do not affect
 * <code>CacheStatistics</code>.
 *<p>
 * Unless otherwise noted, all of these methods throw a
 * <code>CacheClosedException</code> if the Cache is closed at the time of
 * invocation, or an <code>EntryDestroyedException</code> if the entry has been
 * destroyed.
 */
class CPPCACHE_EXPORT RegionEntry : public SharedBase {
 public:
  /** Returns the key for this entry.
   *
   * @return the key for this entry
   */
  CacheableKeyPtr getKey();

  /** Returns the value of this entry in the local cache. Does not invoke
   * a <code>CacheLoader</code>,
   *
   * @return the value or <code>NULLPTR</code> if this entry is invalid
   */
  CacheablePtr getValue();

  /** Returns the region that contains this entry.
   *
   * @return the Region that contains this entry
   */
  void getRegion(RegionPtr& region);

  /** Returns the statistics for this entry.
   *
   * @return the CacheStatistics for this entry
   * @throws StatisticsDisabledException if statistics have been disabled for
   * this region
   */
  void getStatistics(CacheStatisticsPtr& csptr);

  /**
   * Returns whether this entry has been destroyed.
   * <p>Does not throw a <code>EntryDestroyedException</code> if this entry
   * has been destroyed.
   *
   * @return true if this entry has been destroyed
   */
  bool isDestroyed() const;
  /**
   * @brief destructor
   */
  virtual ~RegionEntry();

 private:
  /**
    * @brief constructors
    * created by region
    */
  RegionEntry(const RegionPtr& region, const CacheableKeyPtr& key,
              const CacheablePtr& value);
  RegionPtr m_region;
  CacheableKeyPtr m_key;
  CacheablePtr m_value;
  CacheStatisticsPtr m_statistics;
  bool m_destroyed;
  friend class RegionInternal;
};

}  // namespace gemfire

#endif
