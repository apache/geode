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
package org.apache.geode.internal.cache;

import org.apache.geode.cache.CacheStatistics;
import org.apache.geode.cache.EntryDestroyedException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.StatisticsDisabledException;

public class NonTXEntry implements Region.Entry {

  private final LocalRegion region;
  private final Object key;

  private volatile boolean entryIsDestroyed;

  @Override
  public boolean isLocal() {
    return true;
  }

  /**
   * Create an Entry given a key. The returned Entry may or may not be destroyed
   */
  public NonTXEntry(LocalRegion region, RegionEntry regionEntry) {
    if (regionEntry == null) {
      throw new IllegalArgumentException(
          "regionEntry should not be null");
    }
    this.region = region;
    // for a soplog region, since the entry may not be in memory,
    // we will have to fetch it from soplog, if the entry is in
    // memory this is a quick lookup, so rather than RegionEntry
    // we keep reference to key
    key = regionEntry.getKey();
  }

  /** Internal method for getting the underlying RegionEntry */
  public RegionEntry getRegionEntry() {
    RegionEntry regionEntry = region.getRegionMap().getEntry(key);
    if (regionEntry == null) {
      throw new EntryDestroyedException(key.toString());
    }
    return regionEntry;
  }

  private RegionEntry basicGetEntry() {
    RegionEntry re = region.basicGetEntry(key);
    if (re == null) {
      throw new EntryDestroyedException(key.toString());
    }
    return re;
  }

  @Override
  public boolean isDestroyed() {
    if (entryIsDestroyed) {
      return true;
    }
    if (region.isThisRegionBeingClosedOrDestroyed() || region.basicGetEntry(key) == null) {
      entryIsDestroyed = true;
      return true;
    }
    return false;
  }

  @Override
  public Object getKey() {
    return basicGetEntry().getKey();
  }

  @Override
  public Object getValue() {
    return getValue(false);
  }

  public Object getValue(boolean ignoreCopyOnRead) {
    Object value =
        region.getDeserialized(basicGetEntry(), false, ignoreCopyOnRead, false, false);
    if (value == null) {
      throw new EntryDestroyedException(getKey().toString());
    } else if (Token.isInvalid(value)) {
      return null;
    }

    return value;
  }

  /**
   * To get the value from region in serialized form
   *
   * @return {@link VMCachedDeserializable}
   */
  public Object getRawValue() {
    Object value = basicGetEntry().getValue((RegionEntryContext) getRegion());
    if (value == null) {
      throw new EntryDestroyedException(getRegionEntry().getKey().toString());
    } else if (Token.isInvalid(value)) {
      return null;
    }

    return value;
  }

  @Override
  public Region getRegion() {
    basicGetEntry();
    return region;
  }

  @Override
  public CacheStatistics getStatistics() {
    // prefer entry destroyed exception over statistics disabled exception
    basicGetEntry();
    if (!region.isStatisticsEnabled()) {
      throw new StatisticsDisabledException(
          String.format("Statistics disabled for region '%s'", region.getFullPath()));
    }
    return new CacheStatisticsImpl(basicGetEntry(), region);
  }

  @Override
  public Object getUserAttribute() {
    basicGetEntry();
    return region.getEntryUserAttributes().get(basicGetEntry().getKey());
  }

  @Override
  public Object setUserAttribute(Object userAttribute) {
    return region.getEntryUserAttributes().put(basicGetEntry().getKey(), userAttribute);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof NonTXEntry)) {
      return false;
    }
    NonTXEntry entry = (NonTXEntry) obj;
    return basicGetEntry().equals(entry.getRegionEntry())
        && getRegion() == entry.getRegion();
  }

  @Override
  public int hashCode() {
    return basicGetEntry().hashCode() ^ getRegion().hashCode();
  }

  @Override
  public String toString() {
    return "NonTXEntry@"
        + Integer.toHexString(System.identityHashCode(this)) + ' '
        + getRegionEntry();
  }

  /**
   * @since GemFire 5.0
   */
  @Override
  public Object setValue(Object value) {
    return region.put(getKey(), value);
  }
}
