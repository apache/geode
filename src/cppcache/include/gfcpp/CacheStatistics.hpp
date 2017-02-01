#pragma once

#ifndef GEODE_GFCPP_CACHESTATISTICS_H_
#define GEODE_GFCPP_CACHESTATISTICS_H_

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

#include "gfcpp_globals.hpp"
#include "gf_types.hpp"
/**
 * @file
 */

namespace apache {
namespace geode {
namespace client {

class LocalRegion;

/**
*@class CacheStatistics CacheStatistics.hpp
*
*Defines common statistical information
*for both the region and its entries. All of these methods may throw a
*CacheClosedException, RegionDestroyedException, or EntryDestroyedException.
*
*@see Region::getStatistics
*@see RegionEntry::getStatistics
*/
class CPPCACHE_EXPORT CacheStatistics : public SharedBase {
 public:
  CacheStatistics();

  virtual ~CacheStatistics();

  /** For an entry, returns the time that the entry's value was last modified.
   * For a region, returns the last time any of the region's entries' values or
   * the values in subregions' entries were modified. The
   * modification may have been initiated locally, or it may have been an update
   * distributed from another cache. It may also have been a new value provided
   * by a loader. The modification time on a region is propagated upward to
   * parent
   * regions, transitively, to the root region.
   * <p>
   * The number is expressed as the number of milliseconds since January 1,
   * 1970.
   * The granularity may be as coarse as 100ms, so the accuracy may be off by
   * up to 50ms.
   * <p>
   * Entry and subregion creation will update the modification time on a
   * region, but <code>destroy</code>, <code>destroyRegion</code>,
   * <code>invalidate</code>, and <code>invalidateRegion</code>
   * do not update the modification time.
   * @return the last modification time of the region or the entry;
   * returns 0 if the entry is invalid or the modification time is
   * uninitialized.
   * @see Region::put
   * @see Region::get
   * @see Region::create
   * @see Region::createSubregion
   */
  virtual uint32_t getLastModifiedTime() const;

  /**
   * For an entry, returns the last time it was accessed via
   * <code>Region.get</code>.
   * For a region, returns the last time any of its entries or the entries of
   * its subregions were accessed with <code>Region.get</code>.
   * Any modifications will also update the lastAccessedTime, so
   * <code>lastAccessedTime</code> is always <code>>= lastModifiedTime</code>.
   * The <code>lastAccessedTime</code> on a region is propagated upward to
   * parent regions, transitively, to the the root region.
   * <p>
   * The number is expressed as the number of milliseconds
   * since January 1, 1970.
   * The granularity may be as coarse as 100ms, so the accuracy may be off by
   * up to 50ms.
   *
   * @return the last access time of the region or the entry's value;
   * returns 0 if entry is invalid or access time is uninitialized.
   * @see Region::get
   * @see getLastModifiedTime
   */
  virtual uint32_t getLastAccessedTime() const;

 private:
  virtual void setLastAccessedTime(uint32_t lat);
  virtual void setLastModifiedTime(uint32_t lmt);

  volatile uint32_t m_lastAccessTime;
  volatile uint32_t m_lastModifiedTime;

  friend class LocalRegion;
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_GFCPP_CACHESTATISTICS_H_
