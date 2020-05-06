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

import java.util.Collection;
import java.util.Set;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Region.Entry;
import org.apache.geode.cache.UnsupportedOperationInTransactionException;
import org.apache.geode.internal.cache.entries.AbstractRegionEntry;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.VersionedObjectList;
import org.apache.geode.internal.cache.versions.ConcurrentCacheModificationException;

/**
 *
 * @since GemFire 6.0tx
 */
public class LocalRegionDataView implements InternalDataView {
  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.InternalDataView#getDeserializedValue(java.lang.Object,
   * org.apache.geode.internal.cache.LocalRegion, boolean)
   */
  @Override
  public Object getDeserializedValue(KeyInfo keyInfo, LocalRegion localRegion, boolean updateStats,
      boolean disableCopyOnRead, boolean preferCD, EntryEventImpl clientEvent,
      boolean returnTombstones, boolean retainResult, boolean createIfAbsent) {
    return localRegion.getDeserializedValue(null, keyInfo, updateStats, disableCopyOnRead, preferCD,
        clientEvent, returnTombstones, retainResult);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.InternalDataView#txDestroyExistingEntry(org.apache.geode.
   * internal.cache.EntryEventImpl, boolean)
   */
  @Override
  public void destroyExistingEntry(EntryEventImpl event, boolean cacheWrite,
      Object expectedOldValue) {
    InternalRegion ir = event.getRegion();
    ir.mapDestroy(event, cacheWrite, false, // isEviction
        expectedOldValue);
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.geode.internal.cache.InternalDataView#txInvalidateExistingEntry(org.apache.geode.
   * internal.cache.EntryEventImpl, boolean, boolean)
   */
  @Override
  public void invalidateExistingEntry(EntryEventImpl event, boolean invokeCallbacks,
      boolean forceNewEntry) {
    try {
      event.getRegion().getRegionMap().invalidate(event, invokeCallbacks, forceNewEntry, false);
    } catch (ConcurrentCacheModificationException e) {
      // a newer event has already been applied to the cache. this can happen
      // in a client cache if another thread is operating on the same key
    }
  }

  @Override
  public void updateEntryVersion(EntryEventImpl event) throws EntryNotFoundException {
    try {
      event.getRegion().getRegionMap().updateEntryVersion(event);
    } catch (ConcurrentCacheModificationException e) {
      // a later in time event has already been applied to the cache. this can happen
      // in a cache if another thread is operating on the same key
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.geode.internal.cache.InternalDataView#entryCount(org.apache.geode.internal.cache.
   * LocalRegion)
   */
  @Override
  public int entryCount(LocalRegion localRegion) {
    return localRegion.getRegionSize();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.InternalDataView#getValueInVM(java.lang.Object,
   * org.apache.geode.internal.cache.LocalRegion, boolean)
   */
  @Override
  public Object getValueInVM(KeyInfo keyInfo, LocalRegion localRegion, boolean rememberRead) {
    return localRegion.nonTXbasicGetValueInVM(keyInfo);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.InternalDataView#containsKey(java.lang.Object,
   * org.apache.geode.internal.cache.LocalRegion)
   */
  @Override
  public boolean containsKey(KeyInfo keyInfo, LocalRegion localRegion) {
    return localRegion.nonTXContainsKey(keyInfo);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.InternalDataView#containsValueForKey(java.lang.Object,
   * org.apache.geode.internal.cache.LocalRegion)
   */
  @Override
  public boolean containsValueForKey(KeyInfo keyInfo, LocalRegion localRegion) {
    return localRegion.nonTXContainsValueForKey(keyInfo);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.InternalDataView#getEntry(java.lang.Object,
   * org.apache.geode.internal.cache.LocalRegion)
   */
  @Override
  public Entry getEntry(KeyInfo keyInfo, LocalRegion localRegion, boolean allowTombstones) {
    return localRegion.nonTXGetEntry(keyInfo, false, allowTombstones);
  }

  @Override
  public Entry accessEntry(KeyInfo keyInfo, LocalRegion localRegion) {
    return localRegion.nonTXGetEntry(keyInfo, true, false);
  }


  @Override
  public boolean putEntry(EntryEventImpl event, boolean ifNew, boolean ifOld,
      Object expectedOldValue, boolean requireOldValue, long lastModified,
      boolean overwriteDestroyed) {
    return event.getRegion().virtualPut(event, ifNew, ifOld, expectedOldValue, requireOldValue,
        lastModified, overwriteDestroyed);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.InternalDataView#putEntry(org.apache.geode.internal.cache.
   * EntryEventImpl, boolean, boolean, java.lang.Object, boolean, long, boolean)
   */
  @Override
  public boolean putEntry(EntryEventImpl event, boolean ifNew, boolean ifOld,
      Object expectedOldValue, boolean requireOldValue, long lastModified,
      boolean overwriteDestroyed, boolean invokeCallbacks, boolean throwsConcurrentModification) {
    return event.getRegion().virtualPut(event, ifNew, ifOld, expectedOldValue, requireOldValue,
        lastModified, overwriteDestroyed, invokeCallbacks, throwsConcurrentModification);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.InternalDataView#isStatsDeferred()
   */
  @Override
  public boolean isDeferredStats() {
    return false;
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.geode.internal.cache.InternalDataView#findObject(org.apache.geode.internal.cache.
   * LocalRegion, java.lang.Object, java.lang.Object, boolean, boolean, java.lang.Object)
   */
  @Override
  public Object findObject(KeyInfo keyInfo, LocalRegion r, boolean isCreate,
      boolean generateCallbacks, Object value, boolean disableCopyOnRead, boolean preferCD,
      ClientProxyMembershipID requestingClient, EntryEventImpl clientEvent,
      boolean returnTombstones) {
    return r.nonTxnFindObject(keyInfo, isCreate, generateCallbacks, value, disableCopyOnRead,
        preferCD, requestingClient, clientEvent, returnTombstones);
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.geode.internal.cache.InternalDataView#getEntryForIterator(org.apache.geode.internal.
   * cache.LocalRegion, java.lang.Object, boolean)
   */
  @Override
  public Region.Entry<?, ?> getEntryForIterator(final KeyInfo keyInfo, final LocalRegion currRgn,
      boolean rememberReads, boolean allowTombstones) {
    final AbstractRegionEntry re = (AbstractRegionEntry) keyInfo.getKey();
    if (re != null && (!re.isDestroyedOrRemoved()) || (allowTombstones && re.isTombstone())) {
      return new NonTXEntry(currRgn, re);
    }
    return null;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.InternalDataView#getKeyForIterator(java.lang.Object,
   * org.apache.geode.internal.cache.LocalRegion, boolean)
   */
  @Override
  public Object getKeyForIterator(final KeyInfo keyInfo, final LocalRegion currRgn,
      boolean rememberReads, boolean allowTombstones) {
    final Object key = keyInfo.getKey();
    if (key == null) {
      return null;
    }
    // fix for 42182, before returning a key verify that its value
    // is not a removed token
    if (key instanceof RegionEntry) {
      RegionEntry re = (RegionEntry) key;
      if (!re.isDestroyedOrRemoved() || (allowTombstones && re.isTombstone())) {
        return re.getKey();
      }
    } else if (getEntry(keyInfo, currRgn, allowTombstones) != null) {
      return key;
    }
    return null;
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.geode.internal.cache.InternalDataView#getAdditionalKeysForIterator(org.apache.geode.
   * internal.cache.LocalRegion)
   */
  @Override
  public Set getAdditionalKeysForIterator(LocalRegion currRgn) {
    return null;
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.geode.internal.cache.InternalDataView#getSerializedValue(org.apache.geode.internal.
   * cache.BucketRegion, java.lang.Object, java.lang.Object)
   */
  @Override
  public Object getSerializedValue(LocalRegion localRegion, KeyInfo key, boolean doNotLockEntry,
      ClientProxyMembershipID requestingClient, EntryEventImpl clientEvent,
      boolean returnTombstones) throws DataLocationException {
    throw new IllegalStateException();
  }

  @Override
  public boolean putEntryOnRemote(EntryEventImpl event, boolean ifNew, boolean ifOld,
      Object expectedOldValue, boolean requireOldValue, long lastModified,
      boolean overwriteDestroyed) throws DataLocationException {
    throw new IllegalStateException();
  }

  @Override
  public void destroyOnRemote(EntryEventImpl event, boolean cacheWrite, Object expectedOldValue)
      throws DataLocationException {
    destroyExistingEntry(event, cacheWrite, expectedOldValue);
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.geode.internal.cache.InternalDataView#invalidateOnRemote(org.apache.geode.internal.
   * cache.EntryEventImpl, boolean, boolean)
   */
  @Override
  public void invalidateOnRemote(EntryEventImpl event, boolean invokeCallbacks,
      boolean forceNewEntry) throws DataLocationException {
    invalidateExistingEntry(event, invokeCallbacks, forceNewEntry);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.InternalDataView#getBucketKeys(int)
   */
  @Override
  public Set getBucketKeys(LocalRegion localRegion, int bucketId, boolean allowTombstones) {
    throw new IllegalStateException();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.InternalDataView#getEntryOnRemote(java.lang.Object,
   * org.apache.geode.internal.cache.LocalRegion)
   */
  @Override
  public Entry getEntryOnRemote(KeyInfo key, LocalRegion localRegion, boolean allowTombstones)
      throws DataLocationException {
    throw new IllegalStateException();
  }

  @Override
  public void checkSupportsRegionDestroy() throws UnsupportedOperationInTransactionException {
    // do nothing - this view supports it
  }

  @Override
  public void checkSupportsRegionInvalidate() throws UnsupportedOperationInTransactionException {
    // do nothing - this view supports it
  }

  @Override
  public void checkSupportsRegionClear() throws UnsupportedOperationInTransactionException {
    // do nothing - this view supports it
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.geode.internal.cache.InternalDataView#getRegionKeysForIteration(org.apache.geode.
   * internal.cache.LocalRegion)
   */
  @Override
  public Collection<?> getRegionKeysForIteration(LocalRegion currRegion) {
    // return currRegion.getRegionKeysForIteration();
    return currRegion.getRegionMap().regionEntries();
  }

  @Override
  public void postPutAll(DistributedPutAllOperation putallOp, VersionedObjectList successfulPuts,
      InternalRegion reg) {
    if (!reg.getDataPolicy().withStorage() && reg.getConcurrencyChecksEnabled()
        && putallOp.getBaseEvent().isBridgeEvent()) {
      // if there is no local storage we need to transfer version information
      // to the successfulPuts list for transmission back to the client
      successfulPuts.clear();
      putallOp.fillVersionedObjectList(successfulPuts);
    }
    // BR & DR's putAll
    long token = -1;
    try {
      if (reg.getServerProxy() != null || (reg.getDataPolicy() != DataPolicy.NORMAL
          && reg.getDataPolicy() != DataPolicy.PRELOADED)) {
        token = reg.postPutAllSend(putallOp, successfulPuts);
      }
      reg.postPutAllFireEvents(putallOp, successfulPuts);
    } finally {
      if (token != -1 && reg instanceof DistributedRegion) {
        putallOp.endOperation(token);
      }
    }
  }

  @Override
  public void postRemoveAll(DistributedRemoveAllOperation op, VersionedObjectList successfulOps,
      InternalRegion reg) {
    if (!reg.getDataPolicy().withStorage() && reg.getConcurrencyChecksEnabled()
        && op.getBaseEvent().isBridgeEvent()) {
      // if there is no local storage we need to transfer version information
      // to the successfulOps list for transmission back to the client
      successfulOps.clear();
      op.fillVersionedObjectList(successfulOps);
    }
    // BR, DR's removeAll
    long token = -1;
    try {
      if (reg.getServerProxy() != null || (reg.getDataPolicy() != DataPolicy.NORMAL
          && reg.getDataPolicy() != DataPolicy.PRELOADED)) {
        token = reg.postRemoveAllSend(op, successfulOps);
      }
      reg.postRemoveAllFireEvents(op, successfulOps);
    } finally {
      if (token != -1 && reg instanceof DistributedRegion) {
        op.endOperation(token);
      }
    }
  }
}
