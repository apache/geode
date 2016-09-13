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
/**
 * File comment
 */
package com.gemstone.gemfire.internal.cache;

import java.util.Collection;
import java.util.Set;

import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Region.Entry;
import com.gemstone.gemfire.cache.UnsupportedOperationInTransactionException;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.internal.cache.tier.sockets.VersionedObjectList;
import com.gemstone.gemfire.internal.offheap.annotations.Retained;

/**
 * @since GemFire 6.0tx
 */
public interface InternalDataView {

  /**
   * @param keyInfo
   * @param localRegion
   * @param updateStats
   * @param disableCopyOnRead
   * @param preferCD
   * @param clientEvent TODO
   * @param returnTombstones TODO
   * @param retainResult if true then the result may be a retained off-heap reference
   * @return the object associated with the key
   */
  @Retained
  Object getDeserializedValue(KeyInfo keyInfo,
                              LocalRegion localRegion,
                              boolean updateStats,
                              boolean disableCopyOnRead,
                              boolean preferCD,
                              EntryEventImpl clientEvent,
                              boolean returnTombstones,
                              boolean retainResult);

  /**
   * @param event
   * @param cacheWrite
   * @param expectedOldValue TODO
   * @throws EntryNotFoundException if the entry is not found in the view
   */
  void destroyExistingEntry(EntryEventImpl event, boolean cacheWrite, Object expectedOldValue) throws EntryNotFoundException;


  /**
   * Invalidate the entry
   * @see Region#invalidate(Object)
   * @param event
   * @param invokeCallbacks
   * @param forceNewEntry
   */
  void invalidateExistingEntry(EntryEventImpl event, boolean invokeCallbacks, boolean forceNewEntry);

  /**
   * get the entry count
   * @param localRegion
   * @return the entry count
   */
  int entryCount(LocalRegion localRegion);

  /**
   * @param keyInfo
   * @param localRegion
   * @param rememberRead
   * @return TODO
   */
  Object getValueInVM(KeyInfo keyInfo, LocalRegion localRegion, boolean rememberRead);

  /**
   * @param keyInfo
   * @param localRegion
   * @return true if key exists, false otherwise
   */
  boolean containsKey(KeyInfo keyInfo, LocalRegion localRegion);

  /**
   * @param keyInfo
   * @param localRegion
   * @return TODO
   */
  boolean containsValueForKey(KeyInfo keyInfo, LocalRegion localRegion);

  /**
   * @param keyInfo 
   * @param localRegion
   * @param allowTombstones
   * @return TODO
   */
  Entry getEntry(KeyInfo keyInfo, LocalRegion localRegion, boolean allowTombstones);

  /**
   * get entry for the key. Called only on farside.
   * @param key
   * @param localRegion
   * @param allowTombstones
   * @return the entry on the remote data store
   */
  Entry getEntryOnRemote(KeyInfo key, LocalRegion localRegion, boolean allowTombstones) throws DataLocationException;

  /**
   * Put or create an entry in the data view.
   * @param event specifies the new or updated value  
   * @param ifNew
   * @param ifOld
   * @param expectedOldValue
   * @param requireOldValue
   * @param overwriteDestroyed 
   * @param lastModified
   * @return true if operation updated existing data, otherwise false
   */
  boolean putEntry(EntryEventImpl event, boolean ifNew, boolean ifOld,
      Object expectedOldValue, boolean requireOldValue, long lastModified, boolean overwriteDestroyed);

  /**
   * Put or create an entry in the data view. Called only on the farside.
   * @param event specifies the new or updated value  
   * @param ifNew
   * @param ifOld
   * @param expectedOldValue
   * @param requireOldValue
   * @param overwriteDestroyed 
   * @param lastModified
   * @return true if operation updated existing data, otherwise false
   */
  boolean putEntryOnRemote(EntryEventImpl event, boolean ifNew, boolean ifOld,
      Object expectedOldValue, boolean requireOldValue, long lastModified, boolean overwriteDestroyed)
      throws DataLocationException;

  /**
   * Destroy an entry in the data view. Called only on the farside.
   * @param event
   * @param cacheWrite TODO
   * @param expectedOldValue
   * @throws DataLocationException TODO
   */
  void destroyOnRemote(EntryEventImpl event, boolean cacheWrite, Object expectedOldValue) throws DataLocationException;

  /**
   * Invalidate an entry in the data view. Called only on farside.
   * @param event
   * @param invokeCallbacks
   * @param forceNewEntry
   * @throws DataLocationException
   */
  void invalidateOnRemote(EntryEventImpl event, boolean invokeCallbacks, boolean forceNewEntry) throws DataLocationException;

  /**
   * @return true if statistics should be updated later in a batch (such as a tx commit)
   */
  boolean isDeferredStats();

  
  /**
   * @param key
   * @param r
   * @param isCreate
   * @param generateCallbacks
   * @param value
   * @param disableCopyOnRead if true then copy on read is disabled for this call
   * @param preferCD true if the preferred result form is CachedDeserializable
   * @param requestingClient the client making the request, or null if from a server
   * @param clientEvent the client's event, if any
   * @param returnTombstones TODO
   * @return the Object associated with the key
   */
  Object findObject(KeyInfo key, LocalRegion r, boolean isCreate, boolean generateCallbacks,
                    Object value, boolean disableCopyOnRead, boolean preferCD, ClientProxyMembershipID requestingClient,
                    EntryEventImpl clientEvent, boolean returnTombstones);


  /**
   * 
   * @param key
   * @param currRgn
   * @param rememberReads
   * @param allowTombstones
   * @return an Entry for the key
   */
  Object getEntryForIterator(KeyInfo key, LocalRegion currRgn,
      boolean rememberReads, boolean allowTombstones);

  /**
   * 
   * @param keyInfo
   * @param currRgn
   * @param rememberReads
   * @param allowTombstones
   * @return the key for the provided key
   */
  Object getKeyForIterator(KeyInfo keyInfo, LocalRegion currRgn,
      boolean rememberReads, boolean allowTombstones);

  /**
   * @param currRgn
   */
  Set getAdditionalKeysForIterator(LocalRegion currRgn);

  /**
   * 
   * @param currRegion
   * @return set of keys in the region
   */
  Collection<?> getRegionKeysForIteration(LocalRegion currRegion);

  /**
   * 
   * @param localRegion
   * @param key
   * @param doNotLockEntry
   * @param requestingClient the client that made the request, or null if not from a client
   * @param clientEvent the client event, if any
   * @param returnTombstones TODO
   * @return the serialized value from the cache
   */
  Object getSerializedValue(LocalRegion localRegion,
                            KeyInfo key,
                            boolean doNotLockEntry,
                            ClientProxyMembershipID requestingClient,
                            EntryEventImpl clientEvent,
                            boolean returnTombstones) throws DataLocationException;

  abstract void checkSupportsRegionDestroy() throws UnsupportedOperationInTransactionException;
  abstract void checkSupportsRegionInvalidate() throws UnsupportedOperationInTransactionException;
  abstract void checkSupportsRegionClear() throws UnsupportedOperationInTransactionException;
  
  /**
   * @param localRegion
   * @param bucketId
   * @param allowTombstones whether to include destroyed entries in the result
   * @return Set of keys in the given bucket
   */
  Set getBucketKeys(LocalRegion localRegion, int bucketId, boolean allowTombstones);
  
  public void postPutAll(DistributedPutAllOperation putallOp, VersionedObjectList successfulPuts,LocalRegion region);
  public void postRemoveAll(DistributedRemoveAllOperation op, VersionedObjectList successfulOps, LocalRegion region);

  Entry accessEntry(KeyInfo keyInfo, LocalRegion localRegion);

  void updateEntryVersion(EntryEventImpl event) throws EntryNotFoundException;
}
