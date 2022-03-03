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
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.StatisticsDisabledException;

/**
 * Used to create a cheap Region.Entry that can be passed to the CustomExpiry callback
 */
class ExpiryRegionEntry implements Region.Entry {

  private final LocalRegion region;
  private final RegionEntry regionEntry;

  ExpiryRegionEntry(LocalRegion region, RegionEntry regionEntry) {
    this.region = region;
    this.regionEntry = regionEntry;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (regionEntry == null ? 0 : regionEntry.hashCode());
    result = prime * result + (region == null ? 0 : region.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    ExpiryRegionEntry other = (ExpiryRegionEntry) obj;
    if (regionEntry == null) {
      if (other.regionEntry != null) {
        return false;
      }
    } else if (!regionEntry.equals(other.regionEntry)) {
      return false;
    }
    if (region == null) {
      return other.region == null;
    } else
      return region.equals(other.region);
  }

  @Override
  public String toString() {
    return "region=" + region.getFullPath() + ", key=" + getKey() + " value=" + getValue();
  }

  @Override
  public Region getRegion() {
    return region;
  }

  /**
   * Returns the entry's RegionEntry if it "checks" out. The check is to see if the region entry
   * still exists.
   *
   * @throws EntryNotFoundException if the RegionEntry has been removed.
   */
  private RegionEntry getCheckedRegionEntry() throws EntryNotFoundException {
    if (regionEntry.isDestroyedOrRemoved()) {
      throw new EntryNotFoundException(
          "Entry for key " + regionEntry.getKey() + " no longer exists");
    }
    return regionEntry;
  }

  @Override
  public Object getValue() {
    Object value =
        region.getDeserialized(getCheckedRegionEntry(), false, false, false, false);
    if (value == null) {
      throw new EntryDestroyedException(getKey().toString());
    } else if (Token.isInvalid(value)) {
      return null;
    }
    return value;
  }

  @Override
  public boolean isLocal() {
    // we only create expiry tasks for local entries
    return true;
  }

  @Override
  public CacheStatistics getStatistics() {
    LocalRegion lr = region;
    if (!lr.isStatisticsEnabled()) {
      throw new StatisticsDisabledException(
          String.format("Statistics disabled for region '%s'",
              lr.getFullPath()));
    }
    return new CacheStatisticsImpl(getCheckedRegionEntry(), lr);
  }

  @Override
  public Object getUserAttribute() {
    return region.getEntryUserAttributes().get(getKey());
  }

  @Override
  public Object setUserAttribute(Object userAttribute) {
    return region.getEntryUserAttributes().put(getKey(), userAttribute);
  }

  @Override
  public boolean isDestroyed() {
    return regionEntry.isDestroyedOrRemoved();
  }

  @Override
  public Object setValue(Object value) {
    return region.put(getKey(), value);
  }

  @Override
  public Object getKey() {
    return regionEntry.getKey();
  }
}
