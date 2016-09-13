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
package org.apache.geode.internal.cache.persistence.query;

import org.apache.geode.internal.cache.CachedDeserializable;

/**
 * The contract for a sorted map that maps the pair (index key, regionKey) to
 * an optional value. 
 * 
 * All read API take INDEX keys, and return REGION keys. So for example
 * keyIterator takes a range of index keys and returns the associated
 * region keys.
 * 
 * The index key is allowed to be an object, or a CachedDeserializable.
 * 
 * This class is threadsafe. Iterators will reflect all entries added to
 * the map up until the time that the iterator was obtained. After that they
 * may or may not reflect modifications to the map while the iteration is in progress.
 * They will guarantee that entries will be returned in the correct order.
 * 
 * TODO - is the user required to filter out NULL index keys, or should
 * we provide separate NULL iterators.
 * 
 * TODO - Do we need getKey, getValue, 
 *
 * @since GemFire cedar
 */
public interface IndexMap {

  /**
   * Add an entry to the map. If the same indexKey, regionKey pair
   * exists in the map is it replaced with the new value.
   * @param indexKey the index key for the entry. The index key may be NULL.
   * @param regionKey the region key for the entry. The region key cannot be NULL.
   * @param value a value for the entry, or NULL for no value
   */
  void put(Object indexKey, Object regionKey, Object value);

  /**
   * Remove an entry from the map.
   * @param indexKey the index key to remove
   * @param regionKey the region key to remove
   * 
   * This method has no effect if the indexKey, regionKey does not exist
   * in the map.
   */
  void remove(Object indexKey, Object regionKey);

  /**
   * Return all of the IndexEntries that map to a given region key.
   */
  CloseableIterator<IndexEntry> get(Object indexKey);
  
  /**
   * Return the set of index keys for a given region key.
   */
  CloseableIterator<CachedDeserializable> getKey(Object indexKey);

  /**
   * Return all of the IndexEntries in the range between start and end. 
   * If end < start, this will return a descending iterator going from end
   * to start. 
   */
  CloseableIterator<IndexEntry> iterator(Object start, boolean startInclusive, 
      Object end, boolean endInclusive);
  
  /**
   * Return all of the IndexEntries that from start to the tail of the map.
   */
  CloseableIterator<IndexEntry> iterator(Object start, boolean startInclusive);
  
  /**
   * Return all of the IndexEntries in the map.
   */
  CloseableIterator<IndexEntry> iterator();
  
  /**
   * Return all of the IndexEntries from the end to the head of the map.
   */
  CloseableIterator<IndexEntry> descendingIterator(Object end, 
      boolean endInclusive);
  
  /**
   * Return all of the IndexEntries in the map in descending order.
   */
  CloseableIterator<IndexEntry> descendingIterator();

  /**
   * Return all of the region keys from start to end.
   */
  CloseableIterator<CachedDeserializable> keyIterator(Object start, 
      boolean startInclusive, Object end, boolean endInclusive);
  
  /**
   * Return all of the region keys from start to the tail of the map
   */
  CloseableIterator<CachedDeserializable> keyIterator(Object start, 
      boolean startInclusive);
  
  /**
   * Return all of the region keys in the map
   */
  CloseableIterator<CachedDeserializable> keyIterator();
  
  /**
   * Return all of the region keys from the end to the head of the map, in
   * descending order.
   */
  CloseableIterator<CachedDeserializable> descendingKeyIterator(Object end, 
      boolean endInclusive);
  
  /**
   * Return all of the region keys in the map, in
   * descending order.
   */
  CloseableIterator<CachedDeserializable> descendingKeyIterator();


  /**
   * Return the estimate of the size of the map in the given range,
   * inclusive.
   */
  long size(Object start, Object end);

  /**
   * Return an estimate of the size of the map from the given key to get end
   * of the map
   */
  long sizeToEnd(Object start);

  /**
   * Return an estimate of the size of the map from the beginning to the
   * given key
   */
  long sizeFromStart(Object end);


  /**
   * Return an estimate of the size of the map.
   */
  long size();
  
  /**
   * Destroy the index map and remove all data from disk. Once a map is
   * destroyed, it will not be recovered.
   */
  public void destroy();

  /**
   * A single entry in an index
   * @since GemFire cedar
   */
  interface IndexEntry {
    /**
     * Return the index key of the entry. May be NULL.
     */
    CachedDeserializable getKey();
    /**
     * Return the region key for the index
     */
    CachedDeserializable getRegionKey();
    /**
     * Return the value of the entry. May be NULL.
     */
    CachedDeserializable getValue();
  }

  
  
}
