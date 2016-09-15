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
 * The contract for a sorted set of temporary results for a query.
 * This set may be persisted on disk.
 * 
 * This class is threadsafe. Iterators will reflect all entries added to
 * the set up until the time that the iterator was obtained. After that they
 * may or may not reflect modifications to the set while the iteration is in progress.
 * They will guarantee that entries will be returned in the correct order.
 * 
 * The key and value are both allowed to be an object, or a CachedDeserializable.
 * 
 * @since GemFire cedar
 */
public interface ResultMap {

  /**
   * Add an entry to the map. If the same key exists in the 
   * map it is replaced with the new value
   * @param key the key for the entry. The key may be NULL.
   * @param value a value for the entry, or NULL for no value
   */
  void put(Object key, Object value);

  /**
   * Remove an entry from the map.
   * @param key the key to remove
   * 
   * This method has no effect if the key does not exist
   * in the map.
   */
  void remove(Object key);

  /**
   * Return the Entry for a given key
   */
  Entry getEntry(Object key);
  
  /**
   * Return the value for a given key
   */
  CachedDeserializable get(Object key);
  
  /**
   * return true if this map contains the given key 
   */ 
  public boolean containsKey(Object e);
  
  /**
   * Return all of the IndexEntries in the range between start and end. 
   * If end < start, this will return a descending iterator going from end
   * to start. 
   */
  CloseableIterator<Entry> iterator(Object start, boolean startInclusive, 
      Object end, boolean endInclusive);
  
  /**
   * Return all of the IndexEntries that from start to the tail of the map.
   */
  CloseableIterator<Entry> iterator(Object start, boolean startInclusive);
  
  /**
   * Return all of the IndexEntries in the map.
   */
  CloseableIterator<Entry> iterator();
  
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
   * Close the map to free up resources.
   */
  void close();

  /**
   * A single entry in an index
   * @since GemFire cedar
   */
  interface Entry {
    /**
     * Return the index key of the entry. May be NULL.
     */
    CachedDeserializable getKey();
    /**
     * Return the value of the entry. May be NULL.
     */
    CachedDeserializable getValue();
  }
}
