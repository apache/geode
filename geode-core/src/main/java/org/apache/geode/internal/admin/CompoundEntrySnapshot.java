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


/**
 * Presents an amalgam snapshot of all the {@linkplain org.apache.geode.cache.Region.Entry regions
 * entries} in a distributed system.
 */
public class CompoundEntrySnapshot implements EntrySnapshot {
  private static final long serialVersionUID = 5776382582897895718L;
  /** The key ("name") of the Region entry */
  private final Object name;

  private long lastModifiedTime = 0L; // the latest modified time
  private long lastAccessTime = 0L; // the latest access time
  private long numHits = 0L; // sum of all hits
  private long numMisses = 0L; // sum of all misses
  private float hitRatio = 0f; // calculated from all
  private long hitResponders = 0;
  private double hitRatioSum = 0.0;
  // private Map individuals = new HashMap();
  private final Set allValues = new HashSet();
  private final Set allUserAttributes = new HashSet();


  /**
   * Creates a <code>CompoundEntrySnapshot</code> for the region entry with the given key ("name").
   */
  public CompoundEntrySnapshot(Object entryName) {
    name = entryName;
  }

  /**
   * Amalgamates an <code>EntrySnapshot</code> into this <code>CompoundEntrySnapshot</code>.
   *
   * @param systemEntity The member of the distributed system that sent the snapshot
   * @param snap The snapshot to be amalgamated
   *
   * @throws IllegalArgumentException If <code>snap</code> is for a region entry other than the one
   *         amalgamated by this snapshot.
   */
  public void addCache(GemFireVM systemEntity, EntrySnapshot snap) {
    if (!snap.getName().equals(name)) {
      throw new IllegalArgumentException(
          "All snapshots in a compound snapshot must have the same name");
    }
    // individuals.put(systemEntity, snap);

    Object value = snap.getValue();
    if (value != null) {
      allValues.add(value.toString());
    } else {
      allValues.add("null");
    }
    Object userAttribute = snap.getUserAttribute();
    if (userAttribute != null) {
      allUserAttributes.add(userAttribute.toString());
    } else {
      allUserAttributes.add("null");
    }

    long modified = snap.getLastModifiedTime();
    if (modified > 0 && modified > lastModifiedTime) {
      lastModifiedTime = modified;
    }

    long access = snap.getLastAccessTime();
    if (access > 0 && access > lastAccessTime) {
      lastAccessTime = access;
    }

    long hitCount = snap.getNumberOfHits();
    if (hitCount > 0) {
      numHits += hitCount;
    }

    long missCount = snap.getNumberOfMisses();
    if (missCount > 0) {
      numMisses += missCount;
    }

    float hitRatio = snap.getHitRatio();
    if (hitRatio >= 0.00) {
      hitResponders++;
      hitRatioSum += hitRatio;
      this.hitRatio = (float) (hitRatioSum / hitResponders);
    }

  }

  /**
   * Returns the name ("key") of the region entry amalgamated by this snapshot.
   */
  @Override
  public Object getName() {
    return name;
  }

  /**
   * Since this snapshot does not represent a single region entry, this method returns
   * <code>null</code>.
   */
  @Override
  public Object getValue() {
    return null;
  }

  /**
   * Since this snapshot does not represent a single region entry, this method returns
   * <code>null</code>.
   */
  @Override
  public Object getUserAttribute() {
    return null;
  }

  /**
   * Returns an <code>Iterator</code> containing the value of this region entry across all
   * <code>Region</code> instances in the distributed system.
   */
  public Iterator getAllValues() {
    return allValues.iterator();
  }

  /**
   * Returns an <code>Iterator</code> containing the <code>userAttributes</code> of this region
   * entry across all <code>Region</code> instances in the distributed system.
   */
  public Iterator getAllUserAttributes() {
    return allUserAttributes.iterator();
  }

  /**
   * Returns the most recent <code>lastModifiedTime</code> of any instance of this snapshot's region
   * entry across the distributed system.
   */
  @Override
  public long getLastModifiedTime() {
    return lastModifiedTime;
  }

  /**
   * Returns the most recent <code>lastAccessTime</code> of any instance of this snapshot's region
   * entry across the distributed system.
   */
  @Override
  public long getLastAccessTime() {
    return lastAccessTime;
  }

  /**
   * Returns the cumulative number of hits across all instances of the snapshot's region entry.
   */
  @Override
  public long getNumberOfHits() {
    return numHits;
  }

  /**
   * Returns the cumulative number of misses across all instances of this snapshot's region entry.
   */
  @Override
  public long getNumberOfMisses() {
    return numMisses;
  }

  /**
   * Returns the aggregate hit ratio across all instances of this snapshot's region entry.
   */
  @Override
  public float getHitRatio() {
    return hitRatio;
  }

}
