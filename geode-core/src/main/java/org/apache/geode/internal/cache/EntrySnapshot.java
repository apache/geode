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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Map;

import org.apache.geode.DataSerializable;
import org.apache.geode.cache.CacheStatistics;
import org.apache.geode.cache.EntryDestroyedException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.StatisticsDisabledException;
import org.apache.geode.internal.cache.versions.VersionStamp;
import org.apache.geode.internal.cache.versions.VersionTag;

/**
 * A Region.Entry implementation for remote entries and all PR entries
 *
 * @since GemFire 5.1
 */
public class EntrySnapshot implements Region.Entry, DataSerializable {
  private static final long serialVersionUID = -2139749921655693280L;
  /**
   * True if at the time this entry was created it represented a local data store. False if it was
   * fetched remotely. This field is used by unit tests.
   */
  private boolean startedLocal;
  /** whether this entry has been destroyed */
  private boolean entryDestroyed;
  private transient LocalRegion region = null;
  /**
   * the internal entry for this Entry's key
   */
  private transient NonLocalRegionEntry regionEntry; // would be final except for serialization
                                                     // needing default constructor

  /**
   * creates a new Entry that wraps the given RegionEntry object for the given storage Region
   *
   * @param allowTombstones TODO
   */
  public EntrySnapshot(RegionEntry regionEntry, LocalRegion dataRegion, LocalRegion region,
      boolean allowTombstones) {
    this.region = region;
    if (regionEntry instanceof NonLocalRegionEntry) {
      this.regionEntry = (NonLocalRegionEntry) regionEntry;
      this.startedLocal = false;
    } else {
      this.startedLocal = true;
      // note we always make these non-local now to handle PR buckets moving
      // out from under this Region.Entry.
      if (regionEntry.hasStats()) {
        this.regionEntry =
            new NonLocalRegionEntryWithStats(regionEntry, dataRegion, allowTombstones);
      } else {
        this.regionEntry = new NonLocalRegionEntry(regionEntry, dataRegion, allowTombstones);
      }
    }
  }

  /**
   * Used by unit tests. Only available on PR. If, at the time this entry was created, it was
   * initialized from a local data store then this method returns true.
   *
   * @since GemFire 6.0
   */
  public boolean wasInitiallyLocal() {
    return this.startedLocal;
  }

  public Object getKey() {
    checkEntryDestroyed();
    return regionEntry.getKey();
  }

  public VersionTag getVersionTag() {
    VersionStamp stamp = regionEntry.getVersionStamp();
    if (stamp != null) {
      return stamp.asVersionTag();
    }
    return null;
  }

  public Object getRawValue() {
    return getRawValue(false);
  }

  public Object getRawValue(boolean forceCopy) {
    Object v = this.regionEntry.getValue(null);
    if (v == null) {
      return null;
    }
    if (v instanceof CachedDeserializable) {
      if (region.isCopyOnRead() || forceCopy) {
        v = ((CachedDeserializable) v).getDeserializedWritableCopy(null, null);
      } else {
        v = ((CachedDeserializable) v).getDeserializedValue(null, null);
      }
      if (v == Token.INVALID || v == Token.LOCAL_INVALID) {
        v = null;
      }
    } else {
      if (v == Token.INVALID || v == Token.LOCAL_INVALID) {
        v = null;
      } else {
        v = conditionalCopy(v);
      }
    }
    return v;
  }

  public Object getValue() {
    checkEntryDestroyed();
    return getRawValue();
  }

  /**
   * Makes a copy, if copy-on-get is enabled, of the specified object.
   *
   * @since GemFire 4.0
   */
  private Object conditionalCopy(Object o) {
    return o;
  }

  public Object getUserAttribute() {
    checkEntryDestroyed();
    Map userAttr = region.entryUserAttributes;
    if (userAttr == null) {
      return null;
    }
    return userAttr.get(this.regionEntry.getKey());
  }

  public Object setUserAttribute(Object value) {
    checkEntryDestroyed();
    if (region.entryUserAttributes == null) {
      region.entryUserAttributes = new Hashtable();
    }
    return region.entryUserAttributes.put(this.regionEntry.getKey(), value);
  }

  public boolean isDestroyed() {
    if (this.entryDestroyed) {
      return true;
    }
    if (region.isDestroyed()) {
      this.entryDestroyed = true;
    } else if (this.regionEntry.isRemoved()) {
      this.entryDestroyed = true;
    }
    // else the entry is somewhere else and we don't know if it's destroyed
    return this.entryDestroyed;
  }

  public Region getRegion() {
    checkEntryDestroyed();
    return region;
  }

  public CacheStatistics getStatistics() throws StatisticsDisabledException {
    checkEntryDestroyed();
    if (!regionEntry.hasStats() || !region.statisticsEnabled) {
      throw new StatisticsDisabledException(
          String.format("Statistics disabled for region ' %s '",
              region.getFullPath()));
    }
    return new CacheStatisticsImpl(this.regionEntry, region);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof EntrySnapshot)) {
      return false;
    }
    EntrySnapshot ent = (EntrySnapshot) obj;
    return this.regionEntry.getKey().equals(ent.getKey());
  }

  @Override
  public int hashCode() {
    return this.regionEntry.getKey().hashCode();
  }

  public Object setValue(Object arg) {
    Object returnValue = region.put(this.getKey(), arg);
    this.regionEntry.setCachedValue(arg);
    return returnValue;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.cache.Region.Entry#isLocal()
   */
  public boolean isLocal() {
    // pr entries are always non-local to support the bucket being moved out
    // from under an entry
    return false;
  }

  @Override
  public String toString() {
    if (this.isDestroyed()) {
      return "EntrySnapshot(#destroyed#" + regionEntry.getKey() + "; version="
          + this.getVersionTag() + ")";
    } else {
      return "EntrySnapshot(" + this.regionEntry + ")";
    }
  }

  /**
   * get the underlying RegionEntry object, which will not be fully functional if isLocal() returns
   * false
   *
   * @return the underlying RegionEntry for this Entry
   */
  public RegionEntry getRegionEntry() {
    return this.regionEntry;
  }

  // ////////////////////////////////////
  // /////////////////////// P R I V A T E M E T H O D S
  // ////////////////////////////////////

  private void checkEntryDestroyed() throws EntryDestroyedException {
    if (isDestroyed()) {
      throw new EntryDestroyedException(
          "entry destroyed");
    }
  }

  // for deserialization
  public EntrySnapshot() {}

  public EntrySnapshot(DataInput in, LocalRegion region)
      throws IOException, ClassNotFoundException {
    this.fromData(in);
    this.region = region;
  }

  public void setRegion(LocalRegion r) {
    this.region = r;
  }

  public void setRegionEntry(NonLocalRegionEntry re) {
    this.regionEntry = re;
  }

  // when externalized, we write the state of a non-local RegionEntry so it
  // can be reconstituted anywhere
  public void toData(DataOutput out) throws IOException {
    out.writeBoolean(this.regionEntry instanceof NonLocalRegionEntryWithStats);
    this.regionEntry.toData(out);
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.startedLocal = false;
    boolean hasStats = in.readBoolean();
    if (hasStats) {
      this.regionEntry = new NonLocalRegionEntryWithStats();
    } else {
      this.regionEntry = new NonLocalRegionEntry();
    }
    this.regionEntry.fromData(in);
  }

}
