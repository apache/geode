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
package org.apache.geode.cache.query.internal.index;

import java.util.Collection;

import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.persistence.query.CloseableIterator;

/**
 * 
 * @since GemFire 8.0
 */
public interface IndexStore {

  /**
   * Add a mapping to the index store
   * 
   * @param indexKey
   * @param re
   */
  public void addMapping(Object indexKey, RegionEntry re) throws IMQException;

  /**
   * Remove a mapping from the index store
   * If entry at indexKey is not found, we must crawl the index to be sure the region entry does not exist
   * @param indexKey
   * @param re
   */
  public void removeMapping(Object indexKey, RegionEntry re)
      throws IMQException;

  /**
   * Update a mapping in the index store. This method adds a new
   * mapping and removes the old mapping
   * 
   * @param indexKey
   * @param oldKey
   * @param re
   */
  public void updateMapping(Object indexKey, Object oldKey, RegionEntry re, Object oldValue)
      throws IMQException;

  public String printAll();
  
  /**
   * Return all of the IndexStoreEntries that map to a given region key.
   */
  public CloseableIterator<IndexStoreEntry> get(Object indexKey);

  /**
   * Return all of the IndexStoreEntries in the range between start and end.
   */
  public CloseableIterator<IndexStoreEntry> iterator(Object start,
      boolean startInclusive, Object end, boolean endInclusive,
      Collection keysToRemove);

  /**
   * Return all of the IndexStorageEntries that from start to the tail of the
   * map.
   */
  public CloseableIterator<IndexStoreEntry> iterator(Object start,
      boolean startInclusive, Collection keysToRemove);

  /**
   * Return all of the IndexStorageEntries in the map.
   */
  public CloseableIterator<IndexStoreEntry> iterator(Collection keysToRemove);

  /**
   * Return all of the IndexStoreEntries from the end to the head of the map.
   */
  public CloseableIterator<IndexStoreEntry> descendingIterator(Object end,
      boolean endInclusive, Collection keysToRemove);

  /**
   * Return all of the IndexStoreEntries in the map in descending order.
   */
  public CloseableIterator<IndexStoreEntry> descendingIterator(
      Collection keysToRemove);

  /**
   * Return all of the IndexStoreEntries in the range between end and start.
   */
  public CloseableIterator<IndexStoreEntry> descendingIterator(Object start,
      boolean startInclusive, Object end, boolean endInclusive,
      Collection keysToRemove);

  /**
   * Return the number of IndexStoreEntries that map to a given Index key.
   */
  public int size(Object key);

  /**
   * Return the total keys in the Index store
   */
  public int size();

  public boolean clear();

  interface IndexStoreEntry {
    /**
     * returns deserialized key
     */
    Object getDeserializedKey();

    /**
     * returns deserialized Region key or Value based on the whether the index
     * is on Region keys or Region values. Defaults to RegionEntry
     */
    Object getDeserializedValue();

    /**
     * returns deserialized region key
     */
    Object getDeserializedRegionKey();

    /**
     * 
     * @return true if the RegionEntry is under update
     * @see RegionEntry#isUpdateInProgress() isUpdateInProgress
     */
    boolean isUpdateInProgress();
  }

  public boolean isIndexOnRegionKeys();

  public void setIndexOnRegionKeys(boolean indexOnRegionKeys);

  public boolean isIndexOnValues();

  public void setIndexOnValues(boolean indexOnValues);

  public Object getTargetObject(RegionEntry entry);

  public Object getTargetObjectInVM(RegionEntry entry);
}
