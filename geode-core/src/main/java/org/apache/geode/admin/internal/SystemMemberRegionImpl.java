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
package org.apache.geode.admin.internal;

import org.apache.geode.admin.*;
import org.apache.geode.cache.*;
//import org.apache.geode.internal.Assert;
//import org.apache.geode.internal.admin.*;
import org.apache.geode.internal.admin.remote.*;

import java.io.File;
import java.util.*;

/**
 * View of a region in a GemFire system member's cache.
 *
 * @since GemFire     3.5
 */
public class SystemMemberRegionImpl implements SystemMemberRegion {

  private AdminRegion r;
  private RegionAttributes ra;
  private CacheStatistics rs;
  private Set subregionNames;
  private Set subregionFullPaths;
  private int entryCount;
  private int subregionCount;

  /** The cache to which this region belongs */
  private final SystemMemberCacheImpl cache;

  // constructors
  public SystemMemberRegionImpl(SystemMemberCacheImpl cache, Region r)
  {
    this.cache = cache;
    this.r = (AdminRegion)r;
  }

  private void refreshFields() {
    this.ra = this.r.getAttributes();
    if (getStatisticsEnabled() && !this.ra.getDataPolicy().withPartitioning()) {
      this.rs = this.r.getStatistics();
    } else {
      this.rs = null;
    }
    { // set subregionNames
      Set s = this.r.subregions(false);
      Set names = new TreeSet();
      Set paths = new TreeSet();
      Iterator it = s.iterator();
      while (it.hasNext()) {
        Region r = (Region)it.next();
        String name = r.getName();
        names.add(name);
        paths.add(this.getFullPath() + Region.SEPARATOR_CHAR + name);
      }
      this.subregionNames = names;
      this.subregionFullPaths = paths;
    }
    try {
      int[] sizes = this.r.sizes();
      this.entryCount = sizes[0];
      this.subregionCount = sizes[1];
    } catch (CacheException ignore) {
      this.entryCount = 0;
      this.subregionCount = 0;
    }
  }
  
  // attributes
  public String getName() {
    return this.r.getName();
  }
  
  public String getFullPath() {
    return this.r.getFullPath();
  }

  public java.util.Set getSubregionNames() {
    return this.subregionNames;
  }

  public java.util.Set getSubregionFullPaths() {
    return this.subregionFullPaths;
  }

  public String getUserAttribute() {
    return (String)r.getUserAttribute();
  }

  public String getCacheLoader() {
    Object o = this.ra.getCacheLoader();
    if (o == null) {
      return "";
    } else {
      return o.toString();
    }
  }
  public String getCacheWriter() {
    Object o = this.ra.getCacheWriter();
    if (o == null) {
      return "";
    } else {
      return o.toString();
    }
  }

  public String getKeyConstraint() {
    Class constraint = this.ra.getKeyConstraint();
    if (constraint == null) {
      return "";
    } else {
      return constraint.getName();
    }
  }

  public String getValueConstraint() {
    Class constraint = this.ra.getValueConstraint();
    if (constraint == null) {
      return "";
    } else {
      return constraint.getName();
    }
  }

  public boolean getEarlyAck() {
    return this.ra.getEarlyAck();
  }

  public int getRegionTimeToLiveTimeLimit() {
    return this.ra.getRegionTimeToLive().getTimeout();
  }

  public ExpirationAction getRegionTimeToLiveAction() {
    return this.ra.getRegionTimeToLive().getAction();
  }

  public int getEntryTimeToLiveTimeLimit() {
    return this.ra.getEntryTimeToLive().getTimeout();
  }

  public ExpirationAction getEntryTimeToLiveAction() {
    return this.ra.getEntryTimeToLive().getAction();
  }

  public String getCustomEntryTimeToLive() {
    Object o = this.ra.getCustomEntryTimeToLive();
    if (o == null) {
      return "";
    } else {
      return o.toString();
    }
  }
  
  public int getRegionIdleTimeoutTimeLimit() {
    return this.ra.getRegionIdleTimeout().getTimeout();
  }

  public ExpirationAction getRegionIdleTimeoutAction() {
    return this.ra.getRegionIdleTimeout().getAction();
  }

  public int getEntryIdleTimeoutTimeLimit() {
    return this.ra.getEntryIdleTimeout().getTimeout();
  }

  public ExpirationAction getEntryIdleTimeoutAction() {
    return this.ra.getEntryIdleTimeout().getAction();
  }

  public String getCustomEntryIdleTimeout() {
    Object o = this.ra.getCustomEntryIdleTimeout();
    if (o == null) {
      return "";
    } else {
      return o.toString();
    }
  }
  
  public MirrorType getMirrorType() {
    return this.ra.getMirrorType();
  }
  
  public DataPolicy getDataPolicy() {
    return this.ra.getDataPolicy();
  }
  
  public Scope getScope() {
    return this.ra.getScope();
  }

  public EvictionAttributes getEvictionAttributes() {
    return this.ra.getEvictionAttributes();
  }

  /**
   * This method will return an empty string if there are no CacheListeners
   * defined on the region. If there are more than 1 CacheListeners defined,
   * this method will return the description of the 1st CacheListener in the
   * list returned by the getCacheListeners method. If there is only one
   * CacheListener defined this method will return it's description
   * 
   * @return String the region's <code>CacheListener</code> description
   * @deprecated as of 6.0, use {@link #getCacheListeners} instead
   */
  @Deprecated
  public String getCacheListener() {
    String[] o = this.getCacheListeners();
    if (o.length == 0) {
      return "";
    }
    else {
      return o[0].toString();
    }
  }

  /**
   * This method will return an empty array if there are no CacheListeners
   * defined on the region. If there are one or more than 1 CacheListeners
   * defined, this method will return an array which has the description of all
   * the CacheListeners
   * 
   * @return String[] the region's <code>CacheListeners</code> descriptions as a
   *         String array
   * @since GemFire 6.0
   */
  public String[] getCacheListeners() {
    Object[] o = this.ra.getCacheListeners();
    String[] ret = null;
    if (o == null || o.length == 0) {
      ret = new String[0];
    }
    else {
      ret = new String[o.length];
      for (int i = 0; i < o.length; i++) {
        ret[i] = o[i].toString();
      }
    }
    return ret;
  }

  public int getInitialCapacity() {
    return this.ra.getInitialCapacity();
  }

  public float getLoadFactor() {
    return this.ra.getLoadFactor();
  }

  public int getConcurrencyLevel() {
    return this.ra.getConcurrencyLevel();
  }

  public boolean getConcurrencyChecksEnabled() {
    return this.ra.getConcurrencyChecksEnabled();
  }

  public boolean getStatisticsEnabled() {
    return this.ra.getStatisticsEnabled();
  }

  public boolean getPersistBackup() {
    return this.ra.getPersistBackup();
  }

  public DiskWriteAttributes getDiskWriteAttributes() {
    return this.ra.getDiskWriteAttributes();
  }

  public File[] getDiskDirs() {
    return this.ra.getDiskDirs();
  }

  public int getEntryCount() {
    return this.entryCount;
  }
  
  public int getSubregionCount() {
    return this.subregionCount;
  }

  public long getLastModifiedTime() {
    if (this.rs == null) {
      return 0;
    } else {
      return this.rs.getLastModifiedTime();
    }
  }

  public long getLastAccessedTime() {
    if (this.rs == null) {
      return 0;
    } else {
      return this.rs.getLastAccessedTime();
    }
  }

  public long getHitCount() {
    if (this.rs == null) {
      return 0;
    } else {
      return this.rs.getHitCount();
    }
  }

  public long getMissCount() {
    if (this.rs == null) {
      return 0;
    } else {
      return this.rs.getMissCount();
    }
  }

  public float getHitRatio() {
    if (this.rs == null) {
      return 0;
    } else {
      return this.rs.getHitRatio();
    }
  }
  
  // operations
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

  public SystemMemberRegion createSubregion(String name,
                                            RegionAttributes attrs)
    throws AdminException {

    Region r =
      this.cache.getVM().createSubregion(this.cache.getCacheInfo(),
                                         this.getFullPath(), name, attrs);
    if (r == null) {
      return null;

    } else {
      return this.cache.createSystemMemberRegion(r);
    }

  }

  public MembershipAttributes getMembershipAttributes() {
    return this.ra.getMembershipAttributes();
  }
  
  public SubscriptionAttributes getSubscriptionAttributes() {
    return this.ra.getSubscriptionAttributes();
  }
  
  public PartitionAttributes getPartitionAttributes() {
    return this.ra.getPartitionAttributes();
  }

}

