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
package org.apache.geode.admin.internal;

import static org.apache.geode.cache.Region.SEPARATOR_CHAR;

import java.io.File;
import java.util.Set;
import java.util.TreeSet;

import org.apache.geode.admin.AdminException;
import org.apache.geode.admin.SystemMemberRegion;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheStatistics;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskWriteAttributes;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.MembershipAttributes;
import org.apache.geode.cache.MirrorType;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.SubscriptionAttributes;
import org.apache.geode.internal.admin.remote.AdminRegion;

/**
 * View of a region in a GemFire system member's cache.
 *
 * @since GemFire 3.5
 */
public class SystemMemberRegionImpl implements SystemMemberRegion {

  private final AdminRegion r;
  private RegionAttributes ra;
  private CacheStatistics rs;
  private Set subregionNames;
  private Set subregionFullPaths;
  private int entryCount;
  private int subregionCount;

  /** The cache to which this region belongs */
  private final SystemMemberCacheImpl cache;

  // constructors
  public SystemMemberRegionImpl(SystemMemberCacheImpl cache, Region r) {
    this.cache = cache;
    this.r = (AdminRegion) r;
  }

  private void refreshFields() {
    ra = r.getAttributes();
    if (getStatisticsEnabled() && !ra.getDataPolicy().withPartitioning()) {
      rs = r.getStatistics();
    } else {
      rs = null;
    }
    { // set subregionNames
      Set s = r.subregions(false);
      Set names = new TreeSet();
      Set paths = new TreeSet();
      for (final Object o : s) {
        Region r = (Region) o;
        String name = r.getName();
        names.add(name);
        paths.add(getFullPath() + SEPARATOR_CHAR + name);
      }
      subregionNames = names;
      subregionFullPaths = paths;
    }
    try {
      int[] sizes = r.sizes();
      entryCount = sizes[0];
      subregionCount = sizes[1];
    } catch (CacheException ignore) {
      entryCount = 0;
      subregionCount = 0;
    }
  }

  // attributes
  @Override
  public String getName() {
    return r.getName();
  }

  @Override
  public String getFullPath() {
    return r.getFullPath();
  }

  @Override
  public java.util.Set getSubregionNames() {
    return subregionNames;
  }

  @Override
  public java.util.Set getSubregionFullPaths() {
    return subregionFullPaths;
  }

  @Override
  public String getUserAttribute() {
    return (String) r.getUserAttribute();
  }

  @Override
  public String getCacheLoader() {
    Object o = ra.getCacheLoader();
    if (o == null) {
      return "";
    } else {
      return o.toString();
    }
  }

  @Override
  public String getCacheWriter() {
    Object o = ra.getCacheWriter();
    if (o == null) {
      return "";
    } else {
      return o.toString();
    }
  }

  @Override
  public String getKeyConstraint() {
    Class constraint = ra.getKeyConstraint();
    if (constraint == null) {
      return "";
    } else {
      return constraint.getName();
    }
  }

  @Override
  public String getValueConstraint() {
    Class constraint = ra.getValueConstraint();
    if (constraint == null) {
      return "";
    } else {
      return constraint.getName();
    }
  }

  @Override
  public boolean getEarlyAck() {
    return ra.getEarlyAck();
  }

  @Override
  public int getRegionTimeToLiveTimeLimit() {
    return ra.getRegionTimeToLive().getTimeout();
  }

  @Override
  public ExpirationAction getRegionTimeToLiveAction() {
    return ra.getRegionTimeToLive().getAction();
  }

  @Override
  public int getEntryTimeToLiveTimeLimit() {
    return ra.getEntryTimeToLive().getTimeout();
  }

  @Override
  public ExpirationAction getEntryTimeToLiveAction() {
    return ra.getEntryTimeToLive().getAction();
  }

  @Override
  public String getCustomEntryTimeToLive() {
    Object o = ra.getCustomEntryTimeToLive();
    if (o == null) {
      return "";
    } else {
      return o.toString();
    }
  }

  @Override
  public int getRegionIdleTimeoutTimeLimit() {
    return ra.getRegionIdleTimeout().getTimeout();
  }

  @Override
  public ExpirationAction getRegionIdleTimeoutAction() {
    return ra.getRegionIdleTimeout().getAction();
  }

  @Override
  public int getEntryIdleTimeoutTimeLimit() {
    return ra.getEntryIdleTimeout().getTimeout();
  }

  @Override
  public ExpirationAction getEntryIdleTimeoutAction() {
    return ra.getEntryIdleTimeout().getAction();
  }

  @Override
  public String getCustomEntryIdleTimeout() {
    Object o = ra.getCustomEntryIdleTimeout();
    if (o == null) {
      return "";
    } else {
      return o.toString();
    }
  }

  @Override
  public MirrorType getMirrorType() {
    return ra.getMirrorType();
  }

  @Override
  public DataPolicy getDataPolicy() {
    return ra.getDataPolicy();
  }

  @Override
  public Scope getScope() {
    return ra.getScope();
  }

  @Override
  public EvictionAttributes getEvictionAttributes() {
    return ra.getEvictionAttributes();
  }

  /**
   * This method will return an empty string if there are no CacheListeners defined on the region.
   * If there are more than 1 CacheListeners defined, this method will return the description of the
   * 1st CacheListener in the list returned by the getCacheListeners method. If there is only one
   * CacheListener defined this method will return it's description
   *
   * @return String the region's <code>CacheListener</code> description
   * @deprecated as of 6.0, use {@link #getCacheListeners} instead
   */
  @Override
  @Deprecated
  public String getCacheListener() {
    String[] o = getCacheListeners();
    if (o.length == 0) {
      return "";
    } else {
      return o[0];
    }
  }

  /**
   * This method will return an empty array if there are no CacheListeners defined on the region. If
   * there are one or more than 1 CacheListeners defined, this method will return an array which has
   * the description of all the CacheListeners
   *
   * @return String[] the region's <code>CacheListeners</code> descriptions as a String array
   * @since GemFire 6.0
   */
  @Override
  public String[] getCacheListeners() {
    Object[] o = ra.getCacheListeners();
    String[] ret = null;
    if (o == null || o.length == 0) {
      ret = new String[0];
    } else {
      ret = new String[o.length];
      for (int i = 0; i < o.length; i++) {
        ret[i] = o[i].toString();
      }
    }
    return ret;
  }

  @Override
  public int getInitialCapacity() {
    return ra.getInitialCapacity();
  }

  @Override
  public float getLoadFactor() {
    return ra.getLoadFactor();
  }

  @Override
  public int getConcurrencyLevel() {
    return ra.getConcurrencyLevel();
  }

  @Override
  public boolean getConcurrencyChecksEnabled() {
    return ra.getConcurrencyChecksEnabled();
  }

  @Override
  public boolean getStatisticsEnabled() {
    return ra.getStatisticsEnabled();
  }

  @Override
  public boolean getPersistBackup() {
    return ra.getPersistBackup();
  }

  @Override
  public DiskWriteAttributes getDiskWriteAttributes() {
    return ra.getDiskWriteAttributes();
  }

  @Override
  public File[] getDiskDirs() {
    return ra.getDiskDirs();
  }

  @Override
  public int getEntryCount() {
    return entryCount;
  }

  @Override
  public int getSubregionCount() {
    return subregionCount;
  }

  @Override
  public long getLastModifiedTime() {
    if (rs == null) {
      return 0;
    } else {
      return rs.getLastModifiedTime();
    }
  }

  @Override
  public long getLastAccessedTime() {
    if (rs == null) {
      return 0;
    } else {
      return rs.getLastAccessedTime();
    }
  }

  @Override
  public long getHitCount() {
    if (rs == null) {
      return 0;
    } else {
      return rs.getHitCount();
    }
  }

  @Override
  public long getMissCount() {
    if (rs == null) {
      return 0;
    } else {
      return rs.getMissCount();
    }
  }

  @Override
  public float getHitRatio() {
    if (rs == null) {
      return 0;
    } else {
      return rs.getHitRatio();
    }
  }

  // operations
  @Override
  public void refresh() {
    refreshFields();
  }

  /**
   * Returns a string representation of the object.
   *
   * @return a string representation of the object
   */
  @Override
  public String toString() {
    return getName();
  }

  @Override
  public SystemMemberRegion createSubregion(String name, RegionAttributes attrs)
      throws AdminException {

    Region r = cache.getVM().createSubregion(cache.getCacheInfo(), getFullPath(),
        name, attrs);
    if (r == null) {
      return null;

    } else {
      return cache.createSystemMemberRegion(r);
    }

  }

  @Override
  public MembershipAttributes getMembershipAttributes() {
    return ra.getMembershipAttributes();
  }

  @Override
  public SubscriptionAttributes getSubscriptionAttributes() {
    return ra.getSubscriptionAttributes();
  }

  @Override
  public PartitionAttributes getPartitionAttributes() {
    return ra.getPartitionAttributes();
  }

}
