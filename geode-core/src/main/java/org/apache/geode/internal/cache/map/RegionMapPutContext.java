/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.cache.map;

import java.util.Set;

import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.Operation;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.offheap.annotations.Released;

/**
 * This class is used to hold a bunch of immutable items
 * used by AbstractRegionMap when doing a "put" operation.
 * It also has a few mutable items updates while a "put"
 * operation is being done.
 */
public class RegionMapPutContext {
  private final EntryEventImpl event;
  private final boolean ifNew;
  private final boolean ifOld;
  private final boolean overwriteDestroyed;
  private final boolean requireOldValue;
  private final boolean uninitialized;
  private final boolean retrieveOldValueForDelta;
  private final boolean replaceOnClient;
  private final boolean onlyExisting;
  private final boolean cacheWrite;
  private final CacheWriter cacheWriter;
  private final Set netWriteRecipients;
  private final Object expectedOldValue;
  private boolean clearOccured;
  private long lastModifiedTime;
  private RegionEntry regionEntry;
  private boolean create;
  private boolean completed;
  @Released
  private Object oldValueForDelta;

  public RegionMapPutContext(LocalRegion owner, EntryEventImpl event, boolean ifNew, boolean ifOld,
      boolean overwriteDestroyed, boolean requireOldValue,

      Object expectedOldValue) {
    if (owner == null) {
      // "fix" for bug 32440
      Assert.assertTrue(false, "The owner for RegionMap " + this + " is null for event " + event);
    }
    this.event = event;
    this.ifNew = ifNew;
    this.ifOld = ifOld;
    this.overwriteDestroyed = overwriteDestroyed;
    this.requireOldValue = requireOldValue;
    this.uninitialized = !owner.isInitialized();
    this.retrieveOldValueForDelta = event.getDeltaBytes() != null && event.getRawNewValue() == null;
    this.replaceOnClient =
        event.getOperation() == Operation.REPLACE && owner.getServerProxy() != null;
    this.onlyExisting = ifOld && !this.isReplaceOnClient();
    this.cacheWriter = owner.basicGetWriter();
    this.cacheWrite = !event.isOriginRemote() && !event.isNetSearch() && event.isGenerateCallbacks()
        && (this.getCacheWriter() != null || owner.hasServerProxy()
            || owner.getScope().isDistributed());
    this.expectedOldValue = expectedOldValue;
    if (this.isCacheWrite()) {
      if (this.getCacheWriter() == null && owner.getScope().isDistributed()) {
        this.netWriteRecipients =
            ((DistributedRegion) owner).getCacheDistributionAdvisor().adviseNetWrite();
      } else {
        this.netWriteRecipients = null;
      }
    } else {
      this.netWriteRecipients = null;
    }
  }

  public boolean isIfNew() {
    return ifNew;
  }

  public boolean isIfOld() {
    return ifOld;
  }

  public boolean isOverwriteDestroyed() {
    return overwriteDestroyed;
  }

  public boolean isRequireOldValue() {
    return requireOldValue;
  }

  public boolean isUninitialized() {
    return uninitialized;
  }

  public boolean isRetrieveOldValueForDelta() {
    return retrieveOldValueForDelta;
  }

  public boolean isReplaceOnClient() {
    return replaceOnClient;
  }

  public boolean isOnlyExisting() {
    return onlyExisting;
  }

  public boolean isCacheWrite() {
    return cacheWrite;
  }

  public CacheWriter getCacheWriter() {
    return cacheWriter;
  }

  public Set getNetWriteRecipients() {
    return netWriteRecipients;
  }

  public Object getExpectedOldValue() {
    return expectedOldValue;
  }

  public boolean getClearOccured() {
    return clearOccured;
  }

  public void setClearOccured(boolean clearOccured) {
    this.clearOccured = clearOccured;
  }

  public long getLastModifiedTime() {
    return lastModifiedTime;
  }

  public void setLastModifiedTime(long lastModifiedTime) {
    this.lastModifiedTime = lastModifiedTime;
  }

  public RegionEntry getRegionEntry() {
    return regionEntry;
  }

  public void setRegionEntry(RegionEntry regionEntry) {
    this.regionEntry = regionEntry;
  }

  /**
   * @return true if put created a new entry; false if it updated an existing one.
   */
  public boolean isCreate() {
    return create;
  }

  public void setCreate(boolean v) {
    this.create = v;
  }

  public EntryEventImpl getEvent() {
    return event;
  }

  public boolean isCompleted() {
    return this.completed;
  }

  public void setCompleted(boolean b) {
    this.completed = b;
  }

  public Object getOldValueForDelta() {
    return this.oldValueForDelta;
  }

  public void setOldValueForDelta(Object value) {
    this.oldValueForDelta = value;
  }
}
