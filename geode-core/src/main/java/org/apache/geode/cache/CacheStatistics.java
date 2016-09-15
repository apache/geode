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

package org.apache.geode.cache;

/**
 * Defines common statistics information
 * for both region and entries. All of these methods may throw a
 * CacheClosedException, RegionDestroyedException or an EntryDestroyedException.
 *
 *
 *
 * @see Region#getStatistics
 * @see Region.Entry#getStatistics
 * @since GemFire 2.0
 */

public interface CacheStatistics
{
  /** For an entry, returns the time that the entry's value was last modified;
   * for a region, the last time any of the region's entries' values or the
   * values in subregions' entries were modified. The
   * modification may have been initiated locally or it may have been an update
   * distributed from another cache. It may also have been a new value provided
   * by a loader. The modification time on a region is propagated upward to parent
   * regions, transitively, to the root region.
   * <p>
   * The number is expressed as the number of milliseconds since January 1, 1970.
   * The granularity may be as course as 100ms, so the accuracy may be off by
   * up to 50ms.
   * <p>
   * Entry and subregion creation will update the modification time on a
   * region, but <code>destroy</code>, <code>destroyRegion</code>,
   * <code>invalidate</code>, and <code>invalidateRegion</code>
   * do not update the modification time.
   * @return the last modification time of the region or the entry;
   * returns 0 if entry is invalid or modification time is uninitialized.
   * @see Region#put(Object, Object)
   * @see Region#get(Object)
   * @see Region#create(Object, Object)
   * @see Region#createSubregion
   */
  public long getLastModifiedTime();

  /**
   * For an entry, returns the last time it was accessed via <code>Region.get</code>;
   * for a region, the last time any of its entries or the entries of its
   * subregions were accessed with <code>Region.get</code>.
   * Any modifications will also update the lastAccessedTime, so
   * <code>lastAccessedTime</code> is always <code>>= lastModifiedTime</code>.
   * The <code>lastAccessedTime</code> on a region is propagated upward to
   * parent regions, transitively, to the the root region.
   * <p>
   * The number is expressed as the number of milliseconds
   * since January 1, 1970.
   * The granularity may be as course as 100ms, so the accuracy may be off by
   * up to 50ms.
   *
   * @return the last access time of the region or the entry's value;
   * returns 0 if entry is invalid or access time is uninitialized.
   * @see Region#get(Object)
   * @see #getLastModifiedTime
   * @throws StatisticsDisabledException if statistics are not available
   */
  public long getLastAccessedTime() throws StatisticsDisabledException;

  /**
   * Returns the number of times that {@link Region#get(Object) Region.get} on
   * the region or the entry was called and there was no value found
   * locally.  Unlike <code>lastAccessedTime</code>, the miss count is
   * not propagated to parent regions.  Note that remote operations
   * such as a "net search" do not effect the miss count.
   *
   * @return the number of cache misses on the region or the
   * entry.  
   * @throws StatisticsDisabledException if statistics are not available
   */
  public long getMissCount() throws StatisticsDisabledException;
  
  /**
   * Returns the number of hits for this region or entry.  The number
   * of hits is defined as the number of times when the 
   * {@link Region#get(Object) Region.get} finds a value locally.  Unlike
   * <code>lastAccessedTime</code>, the hit count is not propagated to
   * parent regions.  Note that remote operations such as a "net
   * search" do not effect the hit count.
   *
   * @return the number of hits for this region or entry.
   * @throws StatisticsDisabledException if statistics are not available
   */
  public long getHitCount() throws StatisticsDisabledException;
  
  /** Return the hit ratio, a convenience method defined as the ratio of hits
   *  to the number of calls to {@link Region#get(Object) Region.get}. If there have been zero
   *  calls to <code>Region.get</code>, then zero is returned.
   *  <p>
   *  The hit ratio is equivalent to:
   *  <pre>
   *  long hitCount = getHitCount();
   *  long total = hitCount + getMissCount();
   *  return total == 0L ? 0.0f : ((float)hitCount / total);
   *  </pre>
   *
   *  @throws StatisticsDisabledException if statistics are not available
   *  @return the hit ratio as a float
   */
  public float getHitRatio() throws StatisticsDisabledException;
    
  
  /** Reset the missCount and hitCount to zero for this entry.
   * 
   * @throws StatisticsDisabledException if statistics are not available
   */
  public void resetCounts() throws StatisticsDisabledException;
}
