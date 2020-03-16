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

import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Region.Entry;
import org.apache.geode.cache.UnsupportedOperationInTransactionException;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.VersionedObjectList;
import org.apache.geode.internal.offheap.annotations.Retained;

/**
 * @since GemFire 6.0tx
 */
public interface InternalDataView {

  /**
   * @param clientEvent TODO
   * @param returnTombstones TODO
   * @param retainResult if true then the result may be a retained off-heap reference
   * @return the object associated with the key
   */
  @Retained
  Object getDeserializedValue(KeyInfo keyInfo, LocalRegion localRegion, boolean updateStats,
      boolean disableCopyOnRead, boolean preferCD, EntryEventImpl clientEvent,
      boolean returnTombstones, boolean retainResult, boolean createIfAbsent);

  /**
   * @param expectedOldValue TODO
   * @throws EntryNotFoundException if the entry is not found in the view
   */
  void destroyExistingEntry(EntryEventImpl event, boolean cacheWrite, Object expectedOldValue)
      throws EntryNotFoundException;


  /**
   * Invalidate the entry
   *
   * @see Region#invalidate(Object)
   */
  void invalidateExistingEntry(EntryEventImpl event, boolean invokeCallbacks,
      boolean forceNewEntry);

  /**
   * get the entry count
   *
   * @return the entry count
   */
  int entryCount(LocalRegion localRegion);

  Object getValueInVM(KeyInfo keyInfo, LocalRegion localRegion, boolean rememberRead);

  /**
   * @return true if key exists, false otherwise
   */
  boolean containsKey(KeyInfo keyInfo, LocalRegion localRegion);

  boolean containsValueForKey(KeyInfo keyInfo, LocalRegion localRegion);

  Entry getEntry(KeyInfo keyInfo, LocalRegion localRegion, boolean allowTombstones);

  /**
   * get entry for the key. Called only on farside.
   *
   * @return the entry on the remote data store
   */
  Entry getEntryOnRemote(KeyInfo key, LocalRegion localRegion, boolean allowTombstones)
      throws DataLocationException;

  boolean putEntry(EntryEventImpl event, boolean ifNew, boolean ifOld, Object expectedOldValue,
      boolean requireOldValue, long lastModified, boolean overwriteDestroyed);

  /**
   * Put or create an entry in the data view.
   *
   * @param event specifies the new or updated value
   * @return true if operation updated existing data, otherwise false
   */
  boolean putEntry(EntryEventImpl event, boolean ifNew, boolean ifOld, Object expectedOldValue,
      boolean requireOldValue, long lastModified, boolean overwriteDestroyed,
      boolean invokeCallbacks, boolean throwConcurrentModification);

  /**
   * Put or create an entry in the data view. Called only on the farside.
   *
   * @param event specifies the new or updated value
   * @return true if operation updated existing data, otherwise false
   */
  boolean putEntryOnRemote(EntryEventImpl event, boolean ifNew, boolean ifOld,
      Object expectedOldValue, boolean requireOldValue, long lastModified,
      boolean overwriteDestroyed) throws DataLocationException;

  /**
   * Destroy an entry in the data view. Called only on the farside.
   *
   * @param cacheWrite TODO
   * @throws DataLocationException TODO
   */
  void destroyOnRemote(EntryEventImpl event, boolean cacheWrite, Object expectedOldValue)
      throws DataLocationException;

  /**
   * Invalidate an entry in the data view. Called only on farside.
   *
   */
  void invalidateOnRemote(EntryEventImpl event, boolean invokeCallbacks, boolean forceNewEntry)
      throws DataLocationException;

  /**
   * @return true if statistics should be updated later in a batch (such as a tx commit)
   */
  boolean isDeferredStats();


  /**
   * @param disableCopyOnRead if true then copy on read is disabled for this call
   * @param preferCD true if the preferred result form is CachedDeserializable
   * @param requestingClient the client making the request, or null if from a server
   * @param clientEvent the client's event, if any
   * @param returnTombstones TODO
   * @return the Object associated with the key
   */
  Object findObject(KeyInfo key, LocalRegion r, boolean isCreate, boolean generateCallbacks,
      Object value, boolean disableCopyOnRead, boolean preferCD,
      ClientProxyMembershipID requestingClient, EntryEventImpl clientEvent,
      boolean returnTombstones);


  /**
   *
   * @return an Entry for the key
   */
  Object getEntryForIterator(KeyInfo key, LocalRegion currRgn, boolean rememberReads,
      boolean allowTombstones);

  /**
   *
   * @return the key for the provided key
   */
  Object getKeyForIterator(KeyInfo keyInfo, LocalRegion currRgn, boolean rememberReads,
      boolean allowTombstones);

  Set getAdditionalKeysForIterator(LocalRegion currRgn);

  /**
   *
   * @return set of keys in the region
   */
  Collection<?> getRegionKeysForIteration(LocalRegion currRegion);

  /**
   *
   * @param requestingClient the client that made the request, or null if not from a client
   * @param clientEvent the client event, if any
   * @param returnTombstones TODO
   * @return the serialized value from the cache
   */
  Object getSerializedValue(LocalRegion localRegion, KeyInfo key, boolean doNotLockEntry,
      ClientProxyMembershipID requestingClient, EntryEventImpl clientEvent,
      boolean returnTombstones) throws DataLocationException;

  void checkSupportsRegionDestroy() throws UnsupportedOperationInTransactionException;

  void checkSupportsRegionInvalidate() throws UnsupportedOperationInTransactionException;

  void checkSupportsRegionClear() throws UnsupportedOperationInTransactionException;

  /**
   * @param allowTombstones whether to include destroyed entries in the result
   * @return Set of keys in the given bucket
   */
  Set getBucketKeys(LocalRegion localRegion, int bucketId, boolean allowTombstones);

  void postPutAll(DistributedPutAllOperation putallOp, VersionedObjectList successfulPuts,
      InternalRegion reg);

  void postRemoveAll(DistributedRemoveAllOperation op, VersionedObjectList successfulOps,
      InternalRegion reg);

  Entry accessEntry(KeyInfo keyInfo, LocalRegion localRegion);

  void updateEntryVersion(EntryEventImpl event) throws EntryNotFoundException;
}
