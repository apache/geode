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

package org.apache.geode.internal.admin;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.RegionAttributes;

/**
 * Presents an amalgam snapshot of all the {@linkplain org.apache.geode.cache.Region regions} in a
 * distributed system.
 */
public class CompoundRegionSnapshot implements RegionSnapshot {
  private static final long serialVersionUID = 6295026394298398004L;
  /** The name of the Region */
  private String name;
  // private String constraintClass;

  private long lastModifiedTime = 0L; // the lates modified time
  private long lastAccessTime = 0L; // the latest access time
  private long numHits = 0L; // sum of all hits
  private long numMisses = 0L; // sum of all misses
  private float hitRatio = 0f; // calculated from all
  private long hitResponders = 0;
  private double hitRatioSum = 0.0;
  // private int entryCount = 0; //largest entryCount
  // private int subregionCount = 0; //largest subregionCount

  // private Map individuals = new HashMap();
  private Set allCapControllers = new HashSet();
  private Set allListeners = new HashSet();
  private Set allDataPolicies = new HashSet();
  private Set allRegionTtl = new HashSet();
  private Set allEntryTtl = new HashSet();
  private HashSet allCustomTtl = new HashSet();
  private Set allRegionIdleTimeout = new HashSet();
  private Set allEntryIdleTimeout = new HashSet();
  private HashSet allCustomIdle = new HashSet();
  private Set allScopes = new HashSet();
  private Set allUserAttributes = new HashSet();
  private Set allCacheLoaders = new HashSet();
  private Set allCacheWriters = new HashSet();
  private Set allLoadFactors = new HashSet();
  private Set allInitialCaps = new HashSet();
  private Set allConcLevels = new HashSet();
  private Set allStatsEnabled = new HashSet();
  private Set allKeyConstraints = new HashSet();
  private Set allValueConstraints = new HashSet();

  /**
   * Creates a new <code>CompoundRegionSnapshot</code> for the region with the given name.
   */
  public CompoundRegionSnapshot(String regionName) {
    this.name = regionName;
  }

  /**
   * Amalgamates a <code>RegionSnapshot</code> into this <code>CompoundRegionSnapshot</code>.
   *
   * @param systemEntity The member of the distributed system that sent the snapshot
   * @param snap The snapshot to be amalgamated
   *
   * @throws IllegalArgumentException If <code>snap</code> is for a <code>Region</code> other than
   *         the one amalgamated by this snapshot.
   */
  public void addCache(GemFireVM systemEntity, RegionSnapshot snap) {
    if (!snap.getName().equals(this.name)) {
      throw new IllegalArgumentException(
          "All snapshots in a compound snapshot must have the same name");
    }
    // individuals.put(systemEntity, snap);

    RegionAttributes ra = snap.getAttributes();
    if (ra != null) {
      CacheListener listener = ra.getCacheListener();
      if (listener != null) {
        allListeners.add(listener.toString());
      }

      CacheWriter writer = ra.getCacheWriter();
      if (writer != null) {
        allCacheWriters.add(writer.toString());
      }

      CacheLoader loader = ra.getCacheLoader();
      if (loader != null) {
        allCacheLoaders.add(loader);
      }

      allDataPolicies.add(ra.getDataPolicy());
      allRegionTtl.add(ra.getRegionTimeToLive());
      allEntryTtl.add(ra.getEntryTimeToLive());
      allCustomTtl.add(ra.getCustomEntryTimeToLive().toString());
      allRegionIdleTimeout.add(ra.getRegionIdleTimeout());
      allEntryIdleTimeout.add(ra.getEntryIdleTimeout());
      allCustomIdle.add(ra.getCustomEntryIdleTimeout().toString());
      allScopes.add(ra.getScope());
      allLoadFactors.add(new Float(ra.getLoadFactor()));
      allInitialCaps.add(Integer.valueOf(ra.getInitialCapacity()));
      allConcLevels.add(Integer.valueOf(ra.getConcurrencyLevel()));
      allStatsEnabled.add(Boolean.valueOf(ra.getStatisticsEnabled()));
      allUserAttributes.add(snap.getUserAttribute());
      allKeyConstraints.add(ra.getKeyConstraint());
      allValueConstraints.add(ra.getValueConstraint());

      // if (constraintClass == null) {
      // Class clazz = ra.getKeyConstraint();
      // if (clazz != null) {
      // constraintClass = clazz.getName();
      // }
      // }
    }

    long modified = snap.getLastModifiedTime();
    if (modified > 0 && modified > this.lastModifiedTime) {
      this.lastModifiedTime = modified;
    }

    long access = snap.getLastAccessTime();
    if (access > 0 && access > this.lastAccessTime) {
      this.lastAccessTime = access;
    }

    long hitCount = snap.getNumberOfHits();
    if (hitCount > 0) {
      this.numHits += hitCount;
    }

    long missCount = snap.getNumberOfMisses();
    if (missCount > 0) {
      this.numMisses += missCount;
    }

    float hitRatio = snap.getHitRatio();
    if (hitRatio >= 0.00) {
      hitResponders++;
      hitRatioSum += hitRatio;
      this.hitRatio = (float) (hitRatioSum / hitResponders);
    }
  }

  /**
   * Returns the name of the <code>Region</code> whose information is amalgamated in this snapshot.
   */
  public Object getName() {
    return this.name;
  }

  /**
   * Since a compound snapshot does not have <code>RegionAttributes</code>, this method returns
   * <code>null</code>.
   */
  public RegionAttributes getAttributes() {
    return null;
  }

  /**
   * Since a compound snapshot does not have a <code>userAttributes</code>, this method returns
   * <code>null</code>.
   */
  public Object getUserAttribute() {
    return null;
  }

  /**
   * Since a compound snapshot does not really represent a <code>Region</code>, this method returns
   * <code>false</code>.
   */
  public boolean isShared() {
    return false;
  }

  /**
   * Returns an {@link java.util.Iterator} of {@link java.lang.String}
   */
  public Iterator getAllCapacityControllers() {
    return allCapControllers.iterator();
  }

  /**
   * Returns an {@link java.util.Iterator} of {@link java.lang.String}
   */
  public Iterator getAllListeners() {
    return allListeners.iterator();
  }

  /**
   * Returns an <code>Iterator</code> containing the <code>toString</code> of the
   * <code>CacheWriter</code>s of each instance of this snapshot's <code>region</code>
   */
  public Iterator getAllCacheWriters() {
    return allCacheWriters.iterator();
  }

  /**
   * Returns an <code>Iterator</code> containing the <code>toString</code> of the
   * <code>CacheLoader</code>s of each instance of this snapshot's <code>region</code>
   */
  public Iterator getAllCacheLoaders() {
    return allCacheLoaders.iterator();
  }

  /**
   * Returns an {@link java.util.Iterator} of {@link org.apache.geode.cache.DataPolicy}
   */
  public Iterator getAllDataPolicies() {
    return allDataPolicies.iterator();
  }

  /**
   * Returns an {@link java.util.Iterator} of {@link org.apache.geode.cache.ExpirationAttributes}
   */
  public Iterator getAllRegionTtl() {
    return allRegionTtl.iterator();
  }

  /**
   * Returns an {@link java.util.Iterator} of {@link org.apache.geode.cache.ExpirationAttributes}
   */
  public Iterator getAllEntryTtl() {
    return allEntryTtl.iterator();
  }

  /**
   * Returns an <code>Iterator</code> containing the <code>toString</code> of the custom TTL
   * CustomExpiry's of each instance of this snapshot's <code>region</code>
   */
  public Iterator getAllCustomTtl() {
    return allCustomTtl.iterator();
  }

  /**
   * Returns an {@link java.util.Iterator} of {@link org.apache.geode.cache.ExpirationAttributes}
   */
  public Iterator getAllRegionIdleTimeout() {
    return allRegionIdleTimeout.iterator();
  }


  /**
   * Returns an {@link java.util.Iterator} of {@link org.apache.geode.cache.ExpirationAttributes}
   */
  public Iterator getAllEntryIdleTimeout() {
    return allEntryIdleTimeout.iterator();
  }

  /**
   * Returns an <code>Iterator</code> containing the <code>toString</code> of the custom idleTimeout
   * CustomExpiry's of each instance of this snapshot's <code>region</code>
   */
  public Iterator getAllCustomIdleTimeout() {
    return allCustomIdle.iterator();
  }

  /**
   * Returns an {@link java.util.Iterator} of {@link org.apache.geode.cache.Scope}
   */
  public Iterator getAllScopes() {
    return allScopes.iterator();
  }

  /**
   * Returns an {@link java.util.Iterator} of float
   */
  public Iterator getAllLoadFactors() {
    return allLoadFactors.iterator();
  }

  /**
   * Returns an {@link java.util.Iterator} of int
   */
  public Iterator getAllInitialCapacities() {
    return allInitialCaps.iterator();
  }

  /**
   * Returns an {@link java.util.Iterator} of int
   */
  public Iterator getAllConcurrencyLevels() {
    return allConcLevels.iterator();
  }

  /**
   * Returns an {@link java.util.Iterator} of <code>Boolean</code> - which may be silly
   */
  public Iterator getAllStatsEnabled() {
    return allStatsEnabled.iterator();
  }

  /**
   * Returns an <code>Iterator</code> of the <code>userAttribute</code>s (as <code>Objects</code>s)
   * over all instances of this snapshot's <code>Region</code>.
   */
  public Iterator getAllUserAttributes() {
    return allUserAttributes.iterator();
  }

  /**
   * Returns an <code>Iterator</code> of the <code>keyConstraint</code>s (as <code>String</code>s)
   * over all instances of this snapshot's <code>Region</code>.
   */
  public Iterator getAllKeyConstraint() {
    return allKeyConstraints.iterator();
  }

  /**
   * Returns an <code>Iterator</code> of the <code>valueConstraint</code>s (as <code>String</code>s)
   * over all instances of this snapshot's <code>Region</code>.
   */
  public Iterator getAllValueConstraint() {
    return allValueConstraints.iterator();
  }

  /**
   * Returns the most recent <code>lastModifiedTime</code> of any instance of this snapshot's
   * <code>Region</code> across the distributed system.
   */
  public long getLastModifiedTime() {
    return this.lastModifiedTime;
  }

  /**
   * Returns the most recent <code>lastAccessTime</code> of any instance of this snapshot's
   * <code>Region</code> across the distributed system.
   */
  public long getLastAccessTime() {
    return this.lastAccessTime;
  }

  /**
   * Returns the cumulative number of hits across all instances of the snapshot's
   * <code>Region</code>.
   */
  public long getNumberOfHits() {
    return this.numHits;
  }

  /**
   * Returns the cumulative number of misses across all instances of this snapshot's
   * <code>Region</code>.
   */
  public long getNumberOfMisses() {
    return this.numMisses;
  }

  /**
   * Returns the aggregate hit ratio across all instances of this snapshot's <code>Region</code>.
   */
  public float getHitRatio() {
    return this.hitRatio;
  }

}
