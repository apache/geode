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
package com.gemstone.gemfire.internal.cache;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Region.Entry;
import com.gemstone.gemfire.cache.UnsupportedOperationInTransactionException;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.internal.cache.tier.sockets.VersionedObjectList;
import com.gemstone.gemfire.internal.cache.versions.ConcurrentCacheModificationException;

/**
 * 
 * @author mthomas
 * @since 6.0tx
 */
public class LocalRegionDataView implements InternalDataView {
  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.InternalDataView#getDeserializedValue(java.lang.Object, com.gemstone.gemfire.internal.cache.LocalRegion, boolean)
   */
  public Object getDeserializedValue(KeyInfo keyInfo, LocalRegion localRegion,
      boolean updateStats, boolean disableCopyOnRead, boolean preferCD, EntryEventImpl clientEvent, boolean returnTombstones, boolean allowReadsFromHDFS, boolean retainResult) {
    return localRegion.getDeserializedValue(null, keyInfo, updateStats, disableCopyOnRead, preferCD, clientEvent, returnTombstones, allowReadsFromHDFS, retainResult);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.InternalDataView#txDestroyExistingEntry(com.gemstone.gemfire.internal.cache.EntryEventImpl, boolean)
   */
  public void destroyExistingEntry(EntryEventImpl event, boolean cacheWrite, Object expectedOldValue) {
    LocalRegion lr = event.getLocalRegion();
    lr.mapDestroy(event,
        cacheWrite,
        false, // isEviction
        expectedOldValue);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.InternalDataView#txInvalidateExistingEntry(com.gemstone.gemfire.internal.cache.EntryEventImpl, boolean, boolean)
   */
  public void invalidateExistingEntry(EntryEventImpl event,
      boolean invokeCallbacks, boolean forceNewEntry) {
    try {
      event.getLocalRegion().entries.invalidate(event, invokeCallbacks, forceNewEntry,false);    
    } catch (ConcurrentCacheModificationException e) {
      // a newer event has already been applied to the cache.  this can happen
      // in a client cache if another thread is operating on the same key
    }
  }

  @Override
  public void updateEntryVersion(EntryEventImpl event)
      throws EntryNotFoundException {
    try {
      event.getLocalRegion().entries.updateEntryVersion(event);
    } catch (ConcurrentCacheModificationException e) {
      // a later in time event has already been applied to the cache.  this can happen
      // in a cache if another thread is operating on the same key
    }
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.InternalDataView#entryCount(com.gemstone.gemfire.internal.cache.LocalRegion)
   */
  public int entryCount(LocalRegion localRegion) {
    return localRegion.getRegionSize();
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.InternalDataView#getValueInVM(java.lang.Object, com.gemstone.gemfire.internal.cache.LocalRegion, boolean)
   */
  public Object getValueInVM(KeyInfo keyInfo, LocalRegion localRegion,
      boolean rememberRead) {
    return localRegion.nonTXbasicGetValueInVM(keyInfo);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.InternalDataView#containsKey(java.lang.Object, com.gemstone.gemfire.internal.cache.LocalRegion)
   */
  public boolean containsKey(KeyInfo keyInfo, LocalRegion localRegion) {
    return localRegion.nonTXContainsKey(keyInfo);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.InternalDataView#containsValueForKey(java.lang.Object, com.gemstone.gemfire.internal.cache.LocalRegion)
   */
  public boolean containsValueForKey(KeyInfo keyInfo, LocalRegion localRegion) {
    return localRegion.nonTXContainsValueForKey(keyInfo);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.InternalDataView#getEntry(java.lang.Object, com.gemstone.gemfire.internal.cache.LocalRegion)
   */
  public Entry getEntry(KeyInfo keyInfo, LocalRegion localRegion, boolean allowTombstones) {
    return localRegion.nonTXGetEntry(keyInfo, false, allowTombstones);
  }

  public Entry accessEntry(KeyInfo keyInfo, LocalRegion localRegion) {
    return localRegion.nonTXGetEntry(keyInfo, true, false);
  }


  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.InternalDataView#putEntry(com.gemstone.gemfire.internal.cache.EntryEventImpl, boolean, boolean, java.lang.Object, boolean, long, boolean)
   */
  public boolean putEntry(EntryEventImpl event, boolean ifNew, boolean ifOld,
      Object expectedOldValue, boolean requireOldValue, long lastModified,
      boolean overwriteDestroyed) {
    return event.getLocalRegion().virtualPut(event, ifNew, ifOld, expectedOldValue, requireOldValue, lastModified, overwriteDestroyed);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.InternalDataView#isStatsDeferred()
   */
  public boolean isDeferredStats() {
    return false;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.InternalDataView#findObject(com.gemstone.gemfire.internal.cache.LocalRegion, java.lang.Object, java.lang.Object, boolean, boolean, java.lang.Object)
   */
  public Object findObject(KeyInfo keyInfo, LocalRegion r, boolean isCreate,
      boolean generateCallbacks, Object value, boolean disableCopyOnRead, boolean preferCD, ClientProxyMembershipID requestingClient, EntryEventImpl clientEvent, boolean returnTombstones, boolean allowReadFromHDFS) {
   return r.nonTxnFindObject(keyInfo, isCreate, generateCallbacks, value, disableCopyOnRead, preferCD, requestingClient, clientEvent, returnTombstones, allowReadFromHDFS);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.InternalDataView#getEntryForIterator(com.gemstone.gemfire.internal.cache.LocalRegion, java.lang.Object, boolean)
   */
  public Region.Entry<?, ?> getEntryForIterator(final KeyInfo keyInfo,
      final LocalRegion currRgn, boolean rememberReads, boolean allowTombstones) {
    final AbstractRegionEntry re = (AbstractRegionEntry)keyInfo.getKey();
    if (re != null && (!re.isDestroyedOrRemoved())
      ||  (allowTombstones  &&  re.isTombstone()) ) {
      return currRgn.new NonTXEntry(re);
    }
    return null;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.InternalDataView#getKeyForIterator(java.lang.Object, com.gemstone.gemfire.internal.cache.LocalRegion, boolean)
   */
  public Object getKeyForIterator(final KeyInfo keyInfo,
      final LocalRegion currRgn, boolean rememberReads, boolean allowTombstones) {
    final AbstractRegionEntry re = (AbstractRegionEntry)keyInfo.getKey();
    // fix for 42182, before returning a key verify that its value
    // is not a removed token
    if (re != null && (!re.isDestroyedOrRemoved()
      ||  (allowTombstones  &&  re.isTombstone()) )) {
      return re.getKey();
    }
    return null;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.InternalDataView#getAdditionalKeysForIterator(com.gemstone.gemfire.internal.cache.LocalRegion)
   */
  public Set getAdditionalKeysForIterator(LocalRegion currRgn) {
    return null;
  }

  /*
   * (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.InternalDataView#getSerializedValue(com.gemstone.gemfire.internal.cache.BucketRegion, java.lang.Object, java.lang.Object)
   */
  public Object getSerializedValue(LocalRegion localRegion, KeyInfo key, boolean doNotLockEntry, ClientProxyMembershipID requestingClient, EntryEventImpl clientEvent, boolean returnTombstones, boolean allowReadFromHDFS) throws DataLocationException {
    throw new IllegalStateException();
  }

  public boolean putEntryOnRemote(EntryEventImpl event, boolean ifNew,
      boolean ifOld, Object expectedOldValue, boolean requireOldValue,
      long lastModified, boolean overwriteDestroyed)
      throws DataLocationException {
    throw new IllegalStateException();
  }

  public void destroyOnRemote(EntryEventImpl event, boolean cacheWrite,
      Object expectedOldValue) throws DataLocationException {
    destroyExistingEntry(event, cacheWrite, expectedOldValue);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.InternalDataView#invalidateOnRemote(com.gemstone.gemfire.internal.cache.EntryEventImpl, boolean, boolean)
   */
  public void invalidateOnRemote(EntryEventImpl event, boolean invokeCallbacks,
      boolean forceNewEntry) throws DataLocationException {
    invalidateExistingEntry(event, invokeCallbacks, forceNewEntry);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.InternalDataView#getBucketKeys(int)
   */
  public Set getBucketKeys(LocalRegion localRegion, int bucketId, boolean allowTombstones) {
    throw new IllegalStateException();
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.InternalDataView#getEntryOnRemote(java.lang.Object, com.gemstone.gemfire.internal.cache.LocalRegion)
   */
  public Entry getEntryOnRemote(KeyInfo key, LocalRegion localRegion, boolean allowTombstones) throws DataLocationException {
    throw new IllegalStateException();
  }

  public void checkSupportsRegionDestroy()
      throws UnsupportedOperationInTransactionException {
    // do nothing - this view supports it
  }

  public void checkSupportsRegionInvalidate()
      throws UnsupportedOperationInTransactionException {
    // do nothing - this view supports it
  }

  @Override
  public void checkSupportsRegionClear()
      throws UnsupportedOperationInTransactionException {
    // do nothing - this view supports it
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.InternalDataView#getRegionKeysForIteration(com.gemstone.gemfire.internal.cache.LocalRegion)
   */
  public Collection<?> getRegionKeysForIteration(LocalRegion currRegion) {
    //return currRegion.getRegionKeysForIteration();
    return currRegion.getRegionMap().regionEntries();
  }

  @Override
  public void postPutAll(DistributedPutAllOperation putallOp, VersionedObjectList successfulPuts,LocalRegion region)
  {
    if (!region.dataPolicy.withStorage() && region.concurrencyChecksEnabled
        && putallOp.getBaseEvent().isBridgeEvent()) {
      // if there is no local storage we need to transfer version information
      // to the successfulPuts list for transmission back to the client
      successfulPuts.clear();
      putallOp.fillVersionedObjectList(successfulPuts);
    }
    region.postPutAllSend(putallOp, successfulPuts);
    region.postPutAllFireEvents(putallOp, successfulPuts);
  }

  @Override
  public void postRemoveAll(DistributedRemoveAllOperation op,
      VersionedObjectList successfulOps, LocalRegion region) {
    if (!region.dataPolicy.withStorage() && region.concurrencyChecksEnabled
        && op.getBaseEvent().isBridgeEvent()) {
      // if there is no local storage we need to transfer version information
      // to the successfulOps list for transmission back to the client
      successfulOps.clear();
      op.fillVersionedObjectList(successfulOps);
    }
    region.postRemoveAllSend(op, successfulOps);
    region.postRemoveAllFireEvents(op, successfulOps);
  }
}
