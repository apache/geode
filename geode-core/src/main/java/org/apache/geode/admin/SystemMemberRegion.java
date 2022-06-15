/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.admin;

import java.io.File;

import org.apache.geode.cache.api.DataPolicy;
import org.apache.geode.cache.DiskWriteAttributes;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.MembershipAttributes;
import org.apache.geode.cache.MirrorType;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.SubscriptionAttributes;

/**
 * Administrative interface that represent's the {@link SystemMember}'s view of one of its cache's
 * {@link org.apache.geode.cache.Region}s. If the region in the remote system member is closed or
 * destroyed, the methods of <code>SystemMemberRegion</code> will throw
 * {@link RegionNotFoundException}.
 *
 * @since GemFire 3.5
 * @deprecated as of 7.0 use the <code><a href=
 *             "{@docRoot}/org/apache/geode/management/package-summary.html">management</a></code>
 *             package instead
 */
@Deprecated
public interface SystemMemberRegion {
  // attributes
  /**
   * Returns the name that identifies this region in its cache.
   *
   * @return the name that identifies this region in its cache
   *
   * @see org.apache.geode.cache.Region#getName
   */
  String getName();

  /**
   * Returns the full path name that identifies this region in its cache.
   *
   * @return the full path name that identifies this region in its cache
   *
   * @see org.apache.geode.cache.Region#getFullPath
   */
  String getFullPath();

  /**
   * Returns the names of all the subregions of this region.
   *
   * @return the names of all the subregions of this region
   */
  java.util.Set getSubregionNames();

  /**
   * Returns the full path of each of the subregions of this region. These paths are suitable for
   * use with {@link SystemMemberCache#getRegion}.
   *
   * @return the full path of each of the subregions of this region
   */
  java.util.Set getSubregionFullPaths();

  /**
   * Returns a description of any user attribute associated with this region. The description
   * includes the classname of the user attribute object as well as its <code>toString</code>
   * representation.
   *
   * @return a description of any user attribute associated with this region
   */
  String getUserAttribute();

  /**
   * Returns a description of any CacheLoader associated with this region.
   *
   * @return a description of any CacheLoader associated with this region
   */
  String getCacheLoader();

  /**
   * Returns a description of any CacheWriter associated with this region.
   *
   * @return a description of any CacheWriter associated with this region
   */
  String getCacheWriter();

  /**
   * Returns the <code>EvictionAttributes</code> that configure how entries in the region are
   * evicted
   *
   * @return the <code>EvictionAttributes</code> that configure how entries in the region are
   *         evicted
   */
  EvictionAttributes getEvictionAttributes();

  /**
   * Returns a description of the CacheListener in this region's attributes. If there is more than 1
   * CacheListener defined for a region this method will return the description of the 1st
   * CacheListener returned from {@link #getCacheListeners}
   *
   * @return a description of the CacheListener in this region's attributes
   *
   * @deprecated as of 6.0 use getCacheListeners() instead
   */
  @Deprecated
  String getCacheListener();

  /**
   * This method will return an empty array if there are no CacheListeners defined on the region. If
   * there are one or more than 1 CacheListeners defined, this method will return an array which has
   * the names of all the CacheListeners
   *
   * @return String[] the region's <code>CacheListeners</code> as a String array
   * @since GemFire 6.0
   */
  String[] getCacheListeners();

  /**
   * Returns the KeyConstraint in this region's attributes.
   *
   * @return the KeyConstraint in this region's attributes
   */
  String getKeyConstraint();

  /**
   * Returns the ValueConstraint in this region's attributes.
   *
   * @return the ValueConstraint in this region's attributes
   */
  String getValueConstraint();

  /**
   * Returns the RegionTimeToLive time limit in this region's attributes.
   *
   * @return the RegionTimeToLive time limit in this region's attributes
   */
  int getRegionTimeToLiveTimeLimit();

  /**
   * Returns the RegionTimeToLive action in this region's attributes.
   *
   * @return the RegionTimeToLive action in this region's attributes
   */
  ExpirationAction getRegionTimeToLiveAction();

  /**
   * Returns the EntryTimeToLive time limit in this region's attributes.
   *
   * @return the EntryTimeToLive time limit in this region's attributes
   */
  int getEntryTimeToLiveTimeLimit();

  /**
   * Returns the EntryTimeToLive action in this region's attributes.
   *
   * @return the EntryTimeToLive action in this region's attributes
   */
  ExpirationAction getEntryTimeToLiveAction();

  /**
   * string describing the CustomExpiry for entry-time-to-live
   *
   * @return the CustomExpiry for entry-time-to-live
   */
  String getCustomEntryTimeToLive();

  /**
   * Returns the RegionIdleTimeout time limit in this region's attributes.
   *
   * @return the RegionIdleTimeout time limit in this region's attributes
   */
  int getRegionIdleTimeoutTimeLimit();

  /**
   * Returns the RegionIdleTimeout action in this region's attributes.
   *
   * @return the RegionIdleTimeout action in this region's attributes
   */
  ExpirationAction getRegionIdleTimeoutAction();

  /**
   * Returns the EntryIdleTimeout time limit in this region's attributes.
   *
   * @return the EntryIdleTimeout time limit in this region's attributes
   */
  int getEntryIdleTimeoutTimeLimit();

  /**
   * Returns the EntryIdleTimeout action in this region's attributes.
   *
   * @return the EntryIdleTimeout action in this region's attributes
   */
  ExpirationAction getEntryIdleTimeoutAction();

  /**
   * string describing the CustomExpiry for entry-idle-timeout
   *
   * @return the CustomExpiry for entry-idle-timeout
   */
  String getCustomEntryIdleTimeout();

  /**
   * Returns the MirrorType in this region's attributes.
   *
   * @return the MirrorType in this region's attributes
   *
   * @deprecated as of 5.0, you should use getDataPolicy instead
   */
  @Deprecated
  MirrorType getMirrorType();

  /**
   * Returns the DataPolicy in this region's attributes.
   *
   * @return the DataPolicy in this region's attributes
   */
  DataPolicy getDataPolicy();

  /**
   * Returns the Scope in this region's attributes.
   *
   * @return the Scope in this region's attributes
   */
  Scope getScope();

  /**
   * Returns the InitialCapacity in this region's attributes.
   *
   * @return the InitialCapacity in this region's attributes
   */
  int getInitialCapacity();

  /**
   * Returns the LoadFactor in this region's attributes.
   *
   * @return the LoadFactor in this region's attributes
   */
  float getLoadFactor();

  /**
   * Returns the ConcurrencyLevel in this region's attributes.
   *
   * @return the ConcurrencyLevel in this region's attributes
   */
  int getConcurrencyLevel();

  /**
   * Returns whether or not conflicting concurrent operations on this region are prevented
   *
   * @return whether conflicting concurrent operations on this region are prevented
   */
  boolean getConcurrencyChecksEnabled();

  /**
   * Returns the StatisticsEnabled in this region's attributes.
   *
   * @return the StatisticsEnabled in this region's attributes
   */
  boolean getStatisticsEnabled();

  /**
   * Returns whether or not a persistent backup should be made of the region (as opposed to just
   * writing the overflow data to disk).
   *
   * @return whether a persistent backup should be made of the region
   */
  boolean getPersistBackup();

  /**
   * Returns the <code>DiskWriteAttributes</code> that configure how the region is written to disk.
   *
   * @return the <code>DiskWriteAttributes</code> that configure how the region is written to disk
   */
  DiskWriteAttributes getDiskWriteAttributes();

  /**
   * Returns the directories to which the region's data are written. If multiple directories are
   * used, GemFire will attempt to distribute the data evenly amongst them.
   *
   * @return the directories to which the region's data are written
   */
  File[] getDiskDirs();

  /**
   * Returns the number of entries currently in this region.
   *
   * @return the number of entries currently in this region
   */
  int getEntryCount();

  /**
   * Returns the number of subregions currently in this region.
   *
   * @return the number of subregions currently in this region
   */
  int getSubregionCount();

  /**
   * Returns the LastModifiedTime obtained from this region's statistics.
   *
   * @return the LastModifiedTime obtained from this region's statistics
   */
  long getLastModifiedTime();

  /**
   * Returns the LastAccessedTime obtained from this region's statistics.
   *
   * @return the LastAccessedTime obtained from this region's statistics
   */
  long getLastAccessedTime();

  /**
   * Returns the HitCount obtained from this region's statistics.
   *
   * @return the HitCount obtained from this region's statistics
   */
  long getHitCount();

  /**
   * Returns the MissCount obtained from this region's statistics.
   *
   * @return the MissCount obtained from this region's statistics
   */
  long getMissCount();

  /**
   * Returns the HitRatio obtained from this region's statistics.
   *
   * @return the HitRatio obtained from this region's statistics
   */
  float getHitRatio();

  /**
   * Returns whether or not acks are sent after an update is processed.
   *
   * @return False if acks are sent after updates are processed; true if acks are sent before
   *         updates are processed.
   *
   * @since GemFire 4.1
   */
  boolean getEarlyAck();

  // operations
  /**
   * Updates the state of this region instance. Note that once a cache instance is closed refresh
   * will never change the state of its regions.
   */
  void refresh();

  /**
   * Creates a subregion of this region.
   *
   * @param name The name of the region to create
   * @param attrs The attributes of the root region
   * @return the created subregion
   *
   * @throws AdminException If the region cannot be created
   *
   * @since GemFire 4.0
   */
  SystemMemberRegion createSubregion(String name, RegionAttributes attrs) throws AdminException;

  /**
   * Returns the <code>MembershipAttributes</code> that configure required roles for reliable access
   * to the region.
   *
   * @return the <code>MembershipAttributes</code> that configure required roles for reliable access
   *         to the region
   *
   * @deprecated this API is scheduled to be removed
   */
  @Deprecated
  MembershipAttributes getMembershipAttributes();

  /**
   * Returns the <code>SubscriptionAttributes</code> for the region.
   *
   * @return the <code>SubscriptionAttributes</code> for the region
   *
   * @since GemFire 5.0
   */
  SubscriptionAttributes getSubscriptionAttributes();

  /**
   * Returns the <code>PartitionAttributes</code> for the region.
   *
   * @return the <code>PartitionAttributes</code> for the region
   *
   * @since GemFire 5.7
   */
  PartitionAttributes getPartitionAttributes();

}
