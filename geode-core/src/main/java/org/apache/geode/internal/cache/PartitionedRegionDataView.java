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

import java.util.Set;

import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Region.Entry;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;


/**
 * @since GemFire 6.0tx
 */
public class PartitionedRegionDataView extends LocalRegionDataView {


  @Override
  public void updateEntryVersion(EntryEventImpl event) throws EntryNotFoundException {
    PartitionedRegion pr = (PartitionedRegion) event.getRegion();
    pr.updateEntryVersionInBucket(event);
  }

  @Override
  public void invalidateExistingEntry(EntryEventImpl event, boolean invokeCallbacks,
      boolean forceNewEntry) {
    PartitionedRegion pr = (PartitionedRegion) event.getRegion();
    pr.invalidateInBucket(event);
  }

  @Override
  public void destroyExistingEntry(EntryEventImpl event, boolean cacheWrite,
      Object expectedOldValue) {
    PartitionedRegion pr = (PartitionedRegion) event.getRegion();
    pr.destroyInBucket(event, expectedOldValue);
  }

  @Override
  public Entry getEntry(KeyInfo keyInfo, LocalRegion localRegion, boolean allowTombstones) {
    TXStateProxy tx = localRegion.cache.getTXMgr().pauseTransaction();
    try {
      PartitionedRegion pr = (PartitionedRegion) localRegion;
      return pr.nonTXGetEntry(keyInfo, false, allowTombstones);
    } finally {
      localRegion.cache.getTXMgr().unpauseTransaction(tx);
    }
  }

  @Override
  public Object findObject(KeyInfo key, LocalRegion r, boolean isCreate, boolean generateCallbacks,
      Object value, boolean disableCopyOnRead, boolean preferCD,
      ClientProxyMembershipID requestingClient, EntryEventImpl clientEvent,
      boolean returnTombstones) {
    TXStateProxy tx = r.cache.getTXMgr().pauseTransaction();
    try {
      return r.findObjectInSystem(key, isCreate, tx, generateCallbacks, value, disableCopyOnRead,
          preferCD, requestingClient, clientEvent, returnTombstones);
    } finally {
      r.cache.getTXMgr().unpauseTransaction(tx);
    }
  }

  @Override
  public boolean containsKey(KeyInfo keyInfo, LocalRegion localRegion) {
    PartitionedRegion pr = (PartitionedRegion) localRegion;
    return pr.nonTXContainsKey(keyInfo);
  }

  @Override
  public Object getSerializedValue(LocalRegion localRegion, KeyInfo keyInfo, boolean doNotLockEntry,
      ClientProxyMembershipID requestingClient, EntryEventImpl clientEvent,
      boolean returnTombstones) throws DataLocationException {
    PartitionedRegion pr = (PartitionedRegion) localRegion;
    return pr.getDataStore().getSerializedLocally(keyInfo, doNotLockEntry, requestingClient,
        clientEvent, returnTombstones);
  }

  @Override
  public boolean putEntryOnRemote(EntryEventImpl event, boolean ifNew, boolean ifOld,
      Object expectedOldValue, boolean requireOldValue, long lastModified,
      boolean overwriteDestroyed) throws DataLocationException {
    PartitionedRegion pr = (PartitionedRegion) event.getRegion();
    return pr.getDataStore().putLocally(event.getKeyInfo().getBucketId(), event, ifNew, ifOld,
        expectedOldValue, requireOldValue, lastModified);
  }

  @Override
  public void destroyOnRemote(EntryEventImpl event, boolean cacheWrite, Object expectedOldValue)
      throws DataLocationException {
    PartitionedRegion pr = (PartitionedRegion) event.getRegion();
    pr.getDataStore().destroyLocally(event.getKeyInfo().getBucketId(), event, expectedOldValue);
    return;
  }

  @Override
  public void invalidateOnRemote(EntryEventImpl event, boolean invokeCallbacks,
      boolean forceNewEntry) throws DataLocationException {
    PartitionedRegion pr = (PartitionedRegion) event.getRegion();
    pr.getDataStore().invalidateLocally(event.getKeyInfo().getBucketId(), event);
  }

  @Override
  public Set getBucketKeys(LocalRegion localRegion, int bucketId, boolean allowTombstones) {
    PartitionedRegion pr = (PartitionedRegion) localRegion;
    return pr.getBucketKeys(bucketId, allowTombstones);
  }

  @Override
  public Entry getEntryOnRemote(KeyInfo keyInfo, LocalRegion localRegion, boolean allowTombstones)
      throws DataLocationException {
    PartitionedRegion pr = (PartitionedRegion) localRegion;
    return pr.getDataStore().getEntryLocally(keyInfo.getBucketId(), keyInfo.getKey(), false,
        allowTombstones);
  }

  @Override
  public Object getKeyForIterator(KeyInfo curr, LocalRegion currRgn, boolean rememberReads,
      boolean allowTombstones) {
    // do not perform a value check here, it will send out an
    // extra message. Also BucketRegion will check to see if
    // the value for this key is a removed token
    return curr.getKey();
  }

  /**
   * @see InternalDataView#getEntryForIterator(KeyInfo, LocalRegion, boolean, boolean)
   */
  @Override
  public Region.Entry<?, ?> getEntryForIterator(final KeyInfo keyInfo, final LocalRegion currRgn,
      boolean rememberRead, boolean allowTombstones) {
    return currRgn.nonTXGetEntry(keyInfo, false, allowTombstones);
  }
}
