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
package org.apache.geode.admin;

import org.apache.geode.cache.*;
import java.io.File;

/**
 * Administrative interface that represent's the {@link
 * SystemMember}'s view of one of its cache's {@link
 * org.apache.geode.cache.Region}s.  If the region in the remote
 * system member is closed or destroyed, the methods of
 * <code>SystemMemberRegion</code> will throw {@link
 * RegionNotFoundException}.
 *
 * @since GemFire 3.5
 * @deprecated as of 7.0 use the <code><a href="{@docRoot}/org/apache/geode/management/package-summary.html">management</a></code> package instead
 */
public interface SystemMemberRegion {
  // attributes
  /**
   * Returns the name that identifies this region in its cache.
   *
   * @see org.apache.geode.cache.Region#getName
   */
  public String getName();
  
  /**
   * Returns the full path name that identifies this region in its
   * cache.
   *
   * @see org.apache.geode.cache.Region#getFullPath
   */
  public String getFullPath();

  /**
   * Returns the names of all the subregions of this region.
   */
  public java.util.Set getSubregionNames();

  /**
   * Returns the full path of each of the subregions of this region.
   * These paths are suitable for use with {@link
   * SystemMemberCache#getRegion}.
   */
  public java.util.Set getSubregionFullPaths();

  /**
   * Returns a description of any user attribute associated with this
   * region.  The description includes the classname of the user
   * attribute object as well as its <code>toString</code>
   * representation.
   */
  public String getUserAttribute();

  /**
   * Returns a description of any CacheLoader associated with this region.
   */
  public String getCacheLoader();
  /**
   * Returns a description of any CacheWriter associated with this region.
   */
  public String getCacheWriter();

  /**
   * Returns the <code>EvictionAttributes</code> that configure how
   * entries in the the region are evicted 
   */
  public EvictionAttributes getEvictionAttributes();

  /**
   * Returns a description of the CacheListener in this region's attributes. If
   * there is more than 1 CacheListener defined for a region this method will
   * return the description of the 1st CacheListener returned from
   * {@link #getCacheListeners}
   * 
   * @deprecated as of 6.0 use getCacheListeners() instead
   */
  @Deprecated
  public String getCacheListener();

  /**
   * This method will return an empty array if there are no CacheListeners
   * defined on the region. If there are one or more than 1 CacheListeners
   * defined, this method will return an array which has the names of all the
   * CacheListeners
   * 
   * @return String[] the region's <code>CacheListeners</code> as a String array
   * @since GemFire 6.0
   */
  public String[] getCacheListeners();

  /**
   * Returns the KeyConstraint in this region's attributes.
   */
  public String getKeyConstraint();

  /**
   * Returns the ValueConstraint in this region's attributes.
   */
  public String getValueConstraint();

  /**
   * Returns the RegionTimeToLive time limit in this region's attributes.
   */
  public int getRegionTimeToLiveTimeLimit();

  /**
   * Returns the RegionTimeToLive action in this region's attributes.
   */
  public ExpirationAction getRegionTimeToLiveAction();

  /**
   * Returns the EntryTimeToLive time limit in this region's attributes.
   */
  public int getEntryTimeToLiveTimeLimit();

  /**
   * Returns the EntryTimeToLive action in this region's attributes.
   */
  public ExpirationAction getEntryTimeToLiveAction();

  /**
   * string describing the CustomExpiry for entry-time-to-live
   * @return the CustomExpiry for entry-time-to-live
   */
  public String getCustomEntryTimeToLive();
  
  /**
   * Returns the RegionIdleTimeout time limit in this region's attributes.
   */
  public int getRegionIdleTimeoutTimeLimit();

  /**
   * Returns the RegionIdleTimeout action in this region's attributes.
   */
  public ExpirationAction getRegionIdleTimeoutAction();

  /**
   * Returns the EntryIdleTimeout time limit in this region's attributes.
   */
  public int getEntryIdleTimeoutTimeLimit();

  /**
   * Returns the EntryIdleTimeout action in this region's attributes.
   */
  public ExpirationAction getEntryIdleTimeoutAction();
  
  /**
   * string describing the CustomExpiry for entry-idle-timeout
   * @return the CustomExpiry for entry-idle-timeout
   */
  public String getCustomEntryIdleTimeout();
  
  /**
   * Returns the MirrorType in this region's attributes.
   * @deprecated as of 5.0, you should use getDataPolicy instead
   */
  @Deprecated
  public MirrorType getMirrorType();
  
  /**
   * Returns the DataPolicy in this region's attributes.
   */
  public DataPolicy getDataPolicy();
  
  /**
  
  /**
   * Returns the Scope in this region's attributes.
   */
  public Scope getScope();

  /**
   * Returns the InitialCapacity in this region's attributes.
   */
  public int getInitialCapacity();

  /**
   * Returns the LoadFactor in this region's attributes.
   */
  public float getLoadFactor();

  /**
   * Returns the ConcurrencyLevel in this region's attributes.
   */
  public int getConcurrencyLevel();

  /**
   * Returns whether or not conflicting concurrent operations on this region
   * are prevented 
   */
  public boolean getConcurrencyChecksEnabled();

  /**
   * Returns the StatisticsEnabled in this region's attributes.
   */
  public boolean getStatisticsEnabled();

  /**
   * Returns whether or not a persistent backup should be made of the
   * region (as opposed to just writing the overflow data to disk).
   */
  public boolean getPersistBackup();

  /**
   * Returns the <code>DiskWriteAttributes</code> that configure how
   * the region is written to disk.
   */
  public DiskWriteAttributes getDiskWriteAttributes();

  /**
   * Returns the directories to which the region's data are written.  If
   * multiple directories are used, GemFire will attempt to distribute the
   * data evenly amongst them.
   */
  public File[] getDiskDirs();

  /**
   * Returns the number of entries currently in this region.
   */
  public int getEntryCount();
  
  /**
   * Returns the number of subregions currently in this region.
   */
  public int getSubregionCount();

  /**
   * Returns the LastModifiedTime obtained from this region's statistics.
   */
  public long getLastModifiedTime();

  /**
   * Returns the LastAccessedTime obtained from this region's statistics.
   */
  public long getLastAccessedTime();

  /**
   * Returns the HitCount obtained from this region's statistics.
   */
  public long getHitCount();

  /**
   * Returns the MissCount obtained from this region's statistics.
   */
  public long getMissCount();

  /**
   * Returns the HitRatio obtained from this region's statistics.
   */
  public float getHitRatio();

  /**
   * Returns whether or not acks are sent after an update is processed.
   * @return False if acks are sent after updates are processed;
   *         true if acks are sent before updates are processed.
   *
   * @since GemFire 4.1
   */
  public boolean getEarlyAck();

  // operations
  /**
   * Updates the state of this region instance. Note that once a cache
   * instance is closed refresh will never change the state of its regions.
   */
  public void refresh();

  /**
   * Creates a subregion of this region.
   *
   * @param name
   *        The name of the region to create
   * @param attrs
   *        The attributes of the root region
   *
   * @throws AdminException
   *         If the region cannot be created
   *
   * @since GemFire 4.0
   */
  public SystemMemberRegion createSubregion(String name,
                                            RegionAttributes attrs)
    throws AdminException;

  /**
   * Returns the <code>MembershipAttributes</code> that configure required
   * roles for reliable access to the region.
   * @deprecated this API is scheduled to be removed
   */
  public MembershipAttributes getMembershipAttributes();
  
  /**
   * Returns the <code>SubscriptionAttributes</code> for the region.
   * @since GemFire 5.0
   */
  public SubscriptionAttributes getSubscriptionAttributes();
  
  /**
   * Returns the <code>PartitionAttributes</code> for the region.
   * @since GemFire 5.7
   */
  public PartitionAttributes getPartitionAttributes();

}

