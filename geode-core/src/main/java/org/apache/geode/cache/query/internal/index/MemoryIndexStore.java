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
package org.apache.geode.cache.query.internal.index;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.geode.cache.EntryDestroyedException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.query.IndexMaintenanceException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.internal.CompiledComparison;
import org.apache.geode.cache.query.internal.DefaultQuery;
import org.apache.geode.cache.query.internal.index.AbstractIndex.InternalIndexStatistics;
import org.apache.geode.cache.query.internal.parse.OQLLexerTokenTypes;
import org.apache.geode.cache.query.internal.types.TypeUtils;
import org.apache.geode.internal.cache.CachedDeserializable;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.NonTXEntry;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.internal.cache.persistence.query.CloseableIterator;

/**
 * The in-memory index storage
 *
 * @since GemFire 8.0
 */
public class MemoryIndexStore implements IndexStore {
  /**
   * Map for valueOf(indexedExpression)=>RegionEntries. SortedMap<Object, (RegionEntry |
   * List<RegionEntry>)>. Package access for unit tests.
   */
  final ConcurrentNavigableMap valueToEntriesMap =
      new ConcurrentSkipListMap(TypeUtils.getExtendedNumericComparator());

  // number of keys
  private final AtomicInteger numIndexKeys = new AtomicInteger(0);

  // Map for RegionEntries=>value of indexedExpression (reverse map)
  private ConcurrentMap entryToValuesMap;

  private final InternalIndexStatistics internalIndexStats;

  private final InternalCache cache;

  private final Region region;

  private boolean indexOnRegionKeys;

  private boolean indexOnValues;

  // Used as a place holder for an indexkey collection for when a thread is about to change
  // the collection from index elem to concurrent hash set. Solution for #47475 where
  // we could be missing removes due to different threads grabbing occurrences of the same
  // index elem (or collection), one would transition and addAll to a new collection,
  // while the other would execute a remove on the index elem.
  // both would complete but the remove would have been lost because we had already added it to the
  // new collection
  private final Object TRANSITIONING_TOKEN = new IndexElemArray(1);

  MemoryIndexStore(Region region, InternalIndexStatistics internalIndexStats, InternalCache cache) {
    this.region = region;
    RegionAttributes ra = region.getAttributes();
    // Initialize the reverse-map if in-place modification is set by the
    // application.
    if (IndexManager.isObjectModificationInplace()) {
      this.entryToValuesMap = new ConcurrentHashMap(ra.getInitialCapacity(), ra.getLoadFactor(),
          ra.getConcurrencyLevel());
    }
    this.internalIndexStats = internalIndexStats;
    this.cache = cache;
  }

  @Override
  public void updateMapping(Object indexKey, Object oldKey, RegionEntry re, Object oldValue)
      throws IMQException {
    try {

      if (DefaultQuery.testHook != null) {
        DefaultQuery.testHook.doTestHook(
            DefaultQuery.TestHook.SPOTS.BEFORE_ADD_OR_UPDATE_MAPPING_OR_DESERIALIZING_NTH_STREAMINGOPERATION,
            null, null);
      }

      // Check if reverse-map is present.
      if (IndexManager.isObjectModificationInplace()) {
        // If reverse map get the old index key from reverse map.
        if (this.entryToValuesMap.containsKey(re)) {
          oldKey = this.entryToValuesMap.get(re);
        }
      } else {
        // Check if the old value and new value same.
        // If they are same, that means the value got updated in place.
        // In the absence of reverse-map find the old index key from
        // forward map.
        if (oldValue != null && oldValue == getTargetObjectInVM(re)) {
          oldKey = getOldKey(indexKey, re);
        }
      }

      // No need to update the map if new and old index key are same.
      if (oldKey != null && oldKey.equals(TypeUtils.indexKeyFor(indexKey))) {
        return;
      }

      boolean retry = false;
      indexKey = TypeUtils.indexKeyFor(indexKey);
      if (indexKey.equals(QueryService.UNDEFINED)) {
        Object targetObject = getTargetObjectForUpdate(re);
        if (Token.isInvalidOrRemoved(targetObject)) {
          if (oldKey != null) {
            basicRemoveMapping(oldKey, re, false);
          }
          return;
        }
      }

      do {
        retry = false;
        Object regionEntries = this.valueToEntriesMap.putIfAbsent(indexKey, re);
        if (regionEntries == TRANSITIONING_TOKEN) {
          retry = true;
          continue;
        } else if (regionEntries == null) {
          internalIndexStats.incNumKeys(1);
          numIndexKeys.incrementAndGet();
        } else if (regionEntries instanceof RegionEntry) {
          IndexElemArray elemArray = new IndexElemArray();
          if (DefaultQuery.testHook != null) {
            DefaultQuery.testHook.doTestHook(
                DefaultQuery.TestHook.SPOTS.BEGIN_TRANSITION_FROM_REGION_ENTRY_TO_ELEMARRAY, null,
                null);
          }
          elemArray.add(regionEntries);
          elemArray.add(re);
          if (!this.valueToEntriesMap.replace(indexKey, regionEntries, elemArray)) {
            retry = true;
          }
          if (DefaultQuery.testHook != null) {
            DefaultQuery.testHook
                .doTestHook(DefaultQuery.TestHook.SPOTS.TRANSITIONED_FROM_REGION_ENTRY_TO_ELEMARRAY,
                    null, null);
          }
          if (DefaultQuery.testHook != null) {
            DefaultQuery.testHook.doTestHook(
                DefaultQuery.TestHook.SPOTS.COMPLETE_TRANSITION_FROM_REGION_ENTRY_TO_ELEMARRAY,
                null, null);
          }
        } else if (regionEntries instanceof IndexConcurrentHashSet) {
          // This synchronized is for avoiding conflcts with remove of
          // ConcurrentHashSet when set size becomes zero during
          // basicRemoveMapping();
          synchronized (regionEntries) {
            ((IndexConcurrentHashSet) regionEntries).add(re);
          }
          if (regionEntries != this.valueToEntriesMap.get(indexKey)) {
            retry = true;
          }
        } else {
          IndexElemArray elemArray = (IndexElemArray) regionEntries;
          synchronized (elemArray) {
            if (elemArray.size() >= IndexManager.INDEX_ELEMARRAY_THRESHOLD) {
              IndexConcurrentHashSet set =
                  new IndexConcurrentHashSet(IndexManager.INDEX_ELEMARRAY_THRESHOLD + 20, 0.75f, 1);
              // Replace first so that we are sure that the set is placed in
              // index then we should add old elements in the new set.

              if (DefaultQuery.testHook != null) {
                DefaultQuery.testHook.doTestHook(
                    DefaultQuery.TestHook.SPOTS.BEGIN_TRANSITION_FROM_ELEMARRAY_TO_CONCURRENT_HASH_SET,
                    null, null);
              }
              // on a remove from the elem array, another thread could start and complete its remove
              // at this point, that is why we need to replace before adding the elem array elements
              // once we put this set into the forward map, we know any future removes are either
              // captured
              // by our instance of the elem array, or the remove operations will need to do a
              // retry?
              if (!this.valueToEntriesMap.replace(indexKey, regionEntries, TRANSITIONING_TOKEN)) {
                retry = true;
              } else {
                if (DefaultQuery.testHook != null) {
                  DefaultQuery.testHook
                      .doTestHook(DefaultQuery.TestHook.SPOTS.TRANSITIONED_FROM_ELEMARRAY_TO_TOKEN,
                          null, null);
                }
                set.add(re);
                set.addAll(elemArray);
                if (!this.valueToEntriesMap.replace(indexKey, TRANSITIONING_TOKEN, set)) {
                  // This should never happen. If we see this in the log, then something is wrong
                  // with the TRANSITIONING TOKEN and synchronization of changing collection types
                  // we should then just go from RE to CHS and completely remove the Elem Array.
                  region.getCache().getLogger().warning(
                      "Unable to transition from index elem to concurrent hash set.  Index needs to be recreated");
                  throw new IndexMaintenanceException(
                      "Unable to transition from index elem to concurrent hash set.  Index needs to be recreated");
                }
                if (DefaultQuery.testHook != null) {
                  DefaultQuery.testHook.doTestHook(
                      DefaultQuery.TestHook.SPOTS.COMPLETE_TRANSITION_FROM_ELEMARRAY_TO_CONCURRENT_HASH_SET,
                      null, null);
                }
              }
            } else {
              elemArray.add(re);
              if (regionEntries != this.valueToEntriesMap.get(indexKey)) {
                retry = true;
              }
            }
          }
        }

        // Add to reverse Map with the new value.
        if (!retry) {

          // remove from forward map in case of update
          // oldKey is not null only for an update
          if (oldKey != null) {
            basicRemoveMapping(oldKey, re, false);
          }

          if (IndexManager.isObjectModificationInplace()) {
            this.entryToValuesMap.put(re, indexKey);
          }
        }
      } while (retry);
    } catch (TypeMismatchException ex) {
      throw new IMQException("Could not add object of type " + indexKey.getClass().getName(), ex);
    }
    internalIndexStats.incNumValues(1);
  }

  /**
   * Find the old key by traversing the forward map in case of in-place update modification If not
   * found it means the value object was modified with same value. So oldKey is same as newKey.
   *
   */
  private Object getOldKey(Object newKey, RegionEntry entry) throws TypeMismatchException {
    for (Object mapEntry : valueToEntriesMap.entrySet()) {
      Object regionEntries = ((Entry) mapEntry).getValue();
      Object indexKey = ((Entry) mapEntry).getKey();
      // if more than one index key maps to the same RegionEntry that
      // means there has been an in-place modification
      if (TypeUtils.compare(indexKey, newKey, CompiledComparison.TOK_NE).equals(Boolean.TRUE)) {
        if (regionEntries instanceof RegionEntry && regionEntries.equals(entry)) {
          return indexKey;
        } else if (regionEntries instanceof Collection) {
          Collection coll = (Collection) regionEntries;
          if (coll.contains(entry)) {
            return indexKey;
          }
        }
      }
    }
    return newKey;
  }

  @Override
  public void addMapping(Object indexKey, RegionEntry re) throws IMQException {
    // for add, oldkey is null
    updateMapping(indexKey, null, re, null);
  }

  @Override
  public void removeMapping(Object indexKey, RegionEntry re) throws IMQException {
    // Remove from forward map
    boolean found = basicRemoveMapping(indexKey, re, true);
    // Remove from reverse map.
    // We do NOT need to synchronize here as different RegionEntries will be
    // operating concurrently i.e. different keys in entryToValuesMap which
    // is a concurrent map.
    if (found && IndexManager.isObjectModificationInplace()) {
      this.entryToValuesMap.remove(re);
    }
  }

  private boolean basicRemoveMapping(Object key, RegionEntry entry, boolean findOldKey)
      throws IMQException {
    boolean found = false;
    boolean possiblyAlreadyRemoved = false;
    try {
      Object newKey = convertToIndexKey(key, entry);
      if (DefaultQuery.testHook != null) {
        DefaultQuery.testHook.doTestHook(DefaultQuery.TestHook.SPOTS.ATTEMPT_REMOVE, null, null);
      }
      boolean retry = false;
      do {
        retry = false;
        Object regionEntries = this.valueToEntriesMap.get(newKey);
        if (regionEntries == TRANSITIONING_TOKEN) {
          if (DefaultQuery.testHook != null) {
            DefaultQuery.testHook.doTestHook(DefaultQuery.TestHook.SPOTS.ATTEMPT_RETRY, null, null);
          }
          retry = true;
          continue;
        } else if (regionEntries != null) {
          if (regionEntries instanceof RegionEntry) {
            found = regionEntries == entry;
            if (found) {
              if (this.valueToEntriesMap.remove(newKey, regionEntries)) {
                numIndexKeys.decrementAndGet();
                internalIndexStats.incNumKeys(-1);
              } else {
                // is another thread has since done an add and shifted us into a collection
                retry = true;
              }
            }
          } else {
            Collection entries = (Collection) regionEntries;
            if (DefaultQuery.testHook != null) {
              DefaultQuery.testHook
                  .doTestHook(DefaultQuery.TestHook.SPOTS.BEGIN_REMOVE_FROM_ELEM_ARRAY, null, null);
            }
            found = entries.remove(entry);
            if (DefaultQuery.testHook != null) {
              DefaultQuery.testHook
                  .doTestHook(DefaultQuery.TestHook.SPOTS.REMOVE_CALLED_FROM_ELEM_ARRAY, null,
                      null);
            }
            // This could be IndexElementArray and might be changing to Set
            // If the remove occurred before changing to a set, then next time it will not be
            // "found"
            // However the end effect would be that it was removed
            if (entries instanceof IndexElemArray) {
              if (!this.valueToEntriesMap.replace(newKey, entries, entries)) {
                retry = true;
                possiblyAlreadyRemoved = found;
                continue;
              }
            }

            if (entries.isEmpty()) {
              // because entries collection is empty, remove and decrement
              // value
              synchronized (entries) {
                if (entries.isEmpty()) {
                  if (valueToEntriesMap.remove(newKey, entries)) {
                    numIndexKeys.decrementAndGet();
                    internalIndexStats.incNumKeys(-1);
                  }
                }
              }
            }
            if (DefaultQuery.testHook != null) {
              DefaultQuery.testHook
                  .doTestHook(DefaultQuery.TestHook.SPOTS.COMPLETE_REMOVE_FROM_ELEM_ARRAY, null,
                      null);
            }
          }
        }
      } while (retry);
    } catch (TypeMismatchException ex) {
      throw new IMQException("Could not add object of type " + key.getClass().getName(), ex);
    }
    if (found) {
      // Update stats if entry was actually removed
      internalIndexStats.incNumValues(-1);
    } else if (!found && !possiblyAlreadyRemoved && !IndexManager.isObjectModificationInplace()
        && key != null) {
      // if there is an inplace-modification find old key by iterating
      // over fwd map and then remove the mapping
      if (findOldKey) {
        try {
          Object oldKey = getOldKey(key, entry);
          found = basicRemoveMapping(oldKey, entry, false);
        } catch (TypeMismatchException e) {
          throw new IMQException("Could not find old key: " + key.getClass().getName(), e);
        }
      }
    }
    return found;
  }

  private Object convertToIndexKey(Object key, RegionEntry entry) throws TypeMismatchException {
    Object newKey;
    if (IndexManager.isObjectModificationInplace() && this.entryToValuesMap.containsKey(entry)) {
      newKey = this.entryToValuesMap.get(entry);
    } else {
      newKey = TypeUtils.indexKeyFor(key);
    }
    return newKey;
  }

  @Override
  public CloseableIterator<IndexStoreEntry> get(Object indexKey) {
    return new MemoryIndexStoreIterator(
        this.valueToEntriesMap.subMap(indexKey, true, indexKey, true), indexKey, null);
  }

  @Override
  public CloseableIterator<IndexStoreEntry> iterator(Object start, boolean startInclusive,
      Object end, boolean endInclusive, Collection keysToRemove) {
    if (start == null) {
      return new MemoryIndexStoreIterator(this.valueToEntriesMap.headMap(end, endInclusive), null,
          keysToRemove);
    }
    return new MemoryIndexStoreIterator(
        this.valueToEntriesMap.subMap(start, startInclusive, end, endInclusive), null,
        keysToRemove);
  }

  @Override
  public CloseableIterator<IndexStoreEntry> iterator(Object start, boolean startInclusive,
      Collection keysToRemove) {
    return new MemoryIndexStoreIterator(this.valueToEntriesMap.tailMap(start, startInclusive), null,
        keysToRemove);
  }

  public Iterator<IndexStoreEntry> getKeysIterator() {
    return new MemoryIndexStoreKeyIterator(this.valueToEntriesMap);
  }

  @Override
  public CloseableIterator<IndexStoreEntry> iterator(Collection keysToRemove) {
    return new MemoryIndexStoreIterator(this.valueToEntriesMap, null, keysToRemove);
  }

  @Override
  public CloseableIterator<IndexStoreEntry> descendingIterator(Object start, boolean startInclusive,
      Object end, boolean endInclusive, Collection keysToRemove) {
    if (start == null) {
      return new MemoryIndexStoreIterator(
          this.valueToEntriesMap.headMap(end, endInclusive).descendingMap(), null, keysToRemove);
    }
    return new MemoryIndexStoreIterator(
        this.valueToEntriesMap.subMap(start, startInclusive, end, endInclusive).descendingMap(),
        null, keysToRemove);
  }

  @Override
  public CloseableIterator<IndexStoreEntry> descendingIterator(Object start, boolean startInclusive,
      Collection keysToRemove) {
    return new MemoryIndexStoreIterator(
        this.valueToEntriesMap.tailMap(start, startInclusive).descendingMap(), null, keysToRemove);
  }

  @Override
  public CloseableIterator<IndexStoreEntry> descendingIterator(Collection keysToRemove) {
    return new MemoryIndexStoreIterator(this.valueToEntriesMap.descendingMap(), null, keysToRemove);
  }

  @Override
  public boolean isIndexOnRegionKeys() {
    return indexOnRegionKeys;
  }

  @Override
  public void setIndexOnRegionKeys(boolean indexOnRegionKeys) {
    this.indexOnRegionKeys = indexOnRegionKeys;
  }

  @Override
  public boolean isIndexOnValues() {
    return indexOnValues;
  }

  @Override
  public void setIndexOnValues(boolean indexOnValues) {
    this.indexOnValues = indexOnValues;
  }

  /**
   * Get the object of interest from the region entry. For now it always gets the deserialized
   * value.
   */
  @Override
  public Object getTargetObject(RegionEntry entry) {
    if (indexOnValues) {
      Object o = entry.getValue((LocalRegion) this.region);
      try {
        if (o == Token.INVALID) {
          return null;
        }
        if (o instanceof CachedDeserializable) {
          return ((CachedDeserializable) o).getDeserializedValue(this.region, entry);
        }
      } catch (EntryDestroyedException ignore) {
        return null;
      }
      return o;
    } else if (indexOnRegionKeys) {
      return entry.getKey();
    }
    return new CachedEntryWrapper(new NonTXEntry((LocalRegion) region, entry));
  }

  @Override
  public Object getTargetObjectInVM(RegionEntry entry) {
    if (indexOnValues) {
      Object o = entry.getValueInVM((LocalRegion) this.region);
      try {
        if (o == Token.INVALID) {
          return null;
        }
        if (o instanceof CachedDeserializable) {
          return ((CachedDeserializable) o).getDeserializedValue(this.region, entry);
        }
      } catch (EntryDestroyedException ede) {
        return null;
      }
      return o;
    } else if (indexOnRegionKeys) {
      return entry.getKey();
    }
    return new NonTXEntry((LocalRegion) region, entry);
  }

  private Object getTargetObjectForUpdate(RegionEntry entry) {
    if (indexOnValues) {
      Object o = entry.getValue((LocalRegion) this.region);
      try {
        if (o == Token.INVALID) {
          return Token.INVALID;
        }
        if (o instanceof CachedDeserializable) {
          return ((CachedDeserializable) o).getDeserializedValue(this.region, entry);
        }
      } catch (EntryDestroyedException ede) {
        return Token.INVALID;
      }
      return o;
    } else if (indexOnRegionKeys) {
      return entry.getKey();
    }
    return new NonTXEntry((LocalRegion) region, entry);
  }

  @Override
  public boolean clear() {
    this.valueToEntriesMap.clear();
    if (IndexManager.isObjectModificationInplace()) {
      this.entryToValuesMap.clear();
    }
    numIndexKeys.set(0);
    return true;
  }

  @Override
  public int size(Object key) {
    Object obj = valueToEntriesMap.get(key);
    if (obj != null) {
      return obj instanceof RegionEntry ? 1 : ((Collection) obj).size();
    } else {
      return 0;
    }
  }

  @Override
  public int size() {
    return numIndexKeys.get();
  }

  private class MemoryIndexStoreKeyIterator implements Iterator<IndexStoreEntry> {

    private final Map valuesToEntriesMap;
    private Object currKey;
    private Iterator<Map.Entry> mapIterator;

    public MemoryIndexStoreKeyIterator(Map valuesToEntriesMap) {
      this.valuesToEntriesMap = valuesToEntriesMap;
    }

    @Override
    public boolean hasNext() {
      if (mapIterator == null) {
        mapIterator = this.valuesToEntriesMap.entrySet().iterator();
      }
      if (mapIterator.hasNext()) {
        Map.Entry currentEntry = mapIterator.next();
        currKey = currentEntry.getKey();
        if (currKey == IndexManager.NULL || currKey == QueryService.UNDEFINED) {
          return hasNext();
        }
        return currKey != null;
      }
      return false;
    }

    @Override
    public MemoryIndexStoreKey next() {
      return new MemoryIndexStoreKey(currKey);
    }
  }

  /**
   * A bi-directional iterator over the CSL. Iterates over the entries of CSL where entry is a
   * mapping (value -> Collection) as well as over the Collection.
   */
  private class MemoryIndexStoreIterator implements CloseableIterator<IndexStoreEntry> {
    final Map map;
    Object indexKey;
    Collection keysToRemove;
    Iterator<Map.Entry> mapIterator;
    Iterator valuesIterator;
    Object currKey;
    Object currValue; // RegionEntry
    final long iteratorStartTime;
    MemoryIndexStoreEntry currentEntry;

    MemoryIndexStoreIterator(Map submap, Object indexKey, Collection keysToRemove) {
      this(submap, indexKey, keysToRemove, cache.cacheTimeMillis());
    }

    private MemoryIndexStoreIterator(Map submap, Object indexKey, Collection keysToRemove,
        long iteratorStartTime) {
      this.map = submap;
      this.indexKey = indexKey;
      this.keysToRemove = keysToRemove == null ? null : new HashSet(keysToRemove);
      this.iteratorStartTime = iteratorStartTime;
      currentEntry = new MemoryIndexStoreEntry(iteratorStartTime);
    }

    /**
     * This iterator iterates over the CSL map as well as on the collection of values for each entry
     * in the map. If the map has next element, check if the previous valuesIterator (iterator on
     * the Collection) has been created or has finished iterating. If not created, create an
     * iterator on the Collection. If created and not finished, return, so that the next() call
     * would create a new InMemoryIndexStorageEntry for the current key and next RE from the
     * collection of values.
     */
    @Override
    public boolean hasNext() {
      // return previous collection of values if not over
      if (valuesIterator != null && valuesIterator.hasNext()) {
        return true;
      }
      // sets the next values iterator
      if (mapIterator == null) {
        mapIterator = map.entrySet().iterator();
      }
      if (mapIterator.hasNext()) {
        // set the next entry in the map as current
        Map.Entry currentMapEntry = mapIterator.next();
        // set the index key
        currKey = currentMapEntry.getKey();
        // if the index key in currentIndexEntry is present in the
        // keysToRemove collection or is Undefined or Null
        // skip the current map entry and advance to the
        // next entry in the index map.
        // skipping null & undefined is required so that they do not get
        // into results of range queries.
        // equality and not equality for null/undefined would create
        // valuesIterator so it would not come here
        if (currKey != indexKey
            && (currKey == QueryService.UNDEFINED || currKey == IndexManager.NULL
                || keysToRemove != null && removeFromKeysToRemove(keysToRemove, currKey))) {
          return hasNext();
        }
        // iterator on the collection of values for the index key
        Object values = currentMapEntry.getValue();
        if (values instanceof Collection) {
          this.valuesIterator = ((Collection) values).iterator();
        } else {
          this.valuesIterator = null;
          currValue = values;
        }
        return values != null && (values instanceof RegionEntry || this.valuesIterator.hasNext());
      }

      currKey = null;
      return false;
    }

    /**
     * returns a new InMemoryIndexStorageEntry with current index key and its next value which is
     * the next RE in the collection. Make sure hasNext() has been called before calling this method
     */
    @Override
    public MemoryIndexStoreEntry next() {
      if (valuesIterator == null) {
        currentEntry.setMemoryIndexStoreEntry(currKey, (RegionEntry) currValue);
        return currentEntry;
      }

      RegionEntry re = (RegionEntry) valuesIterator.next();
      if (re == null) {
        throw new NoSuchElementException();
      }

      currentEntry.setMemoryIndexStoreEntry(currKey, re);
      return currentEntry;
    }

    @Override
    public void remove() {
      mapIterator.remove();
    }

    @Override
    public void close() {
      // do nothing
    }

    public boolean removeFromKeysToRemove(Collection keysToRemove, Object key) {
      Iterator iterator = keysToRemove.iterator();
      while (iterator.hasNext()) {
        try {
          if (TypeUtils.compare(key, iterator.next(), OQLLexerTokenTypes.TOK_EQ)
              .equals(Boolean.TRUE)) {
            iterator.remove();
            return true;
          }
        } catch (TypeMismatchException e) {
          // they are not equals, so we just continue iterating
        }
      }
      return false;
    }
  }

  @Override
  public String printAll() {
    StringBuffer sb = new StringBuffer();
    Iterator iterator = this.valueToEntriesMap.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry mapEntry = (Map.Entry) iterator.next();
      sb.append("Key: " + mapEntry.getKey());
      Object value = mapEntry.getValue();
      if (value instanceof Collection) {
        Iterator entriesIterator = ((Collection) value).iterator();
        while (entriesIterator.hasNext()) {
          sb.append(" Value:" + getTargetObject((RegionEntry) entriesIterator.next()));
        }
      } else {
        sb.append(" Value:" + getTargetObject((RegionEntry) value));
      }

      sb.append("\n");

    }
    return sb.toString();
  }

  class MemoryIndexStoreKey implements IndexStoreEntry {
    private Object indexKey;

    public MemoryIndexStoreKey(Object indexKey) {
      this.indexKey = indexKey;
    }

    @Override

    public Object getDeserializedKey() {
      return indexKey;
    }

    @Override
    public Object getDeserializedValue() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Object getDeserializedRegionKey() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isUpdateInProgress() {
      throw new UnsupportedOperationException();
    }
  }
  /**
   * A wrapper over the entry in the CSL index map. It maps IndexKey -> RegionEntry
   */
  class MemoryIndexStoreEntry implements IndexStoreEntry {
    private Object deserializedIndexKey;
    private RegionEntry regionEntry;
    private boolean updateInProgress;
    private Object value;
    private long iteratorStartTime;

    private MemoryIndexStoreEntry(long iteratorStartTime) {
      this.iteratorStartTime = iteratorStartTime;
    }

    public void setMemoryIndexStoreEntry(Object deserializedIndexKey, RegionEntry regionEntry) {
      this.deserializedIndexKey = deserializedIndexKey;
      this.regionEntry = regionEntry;
      this.updateInProgress = regionEntry.isUpdateInProgress();
      this.value = getTargetObject(regionEntry);
    }

    @Override
    public Object getDeserializedKey() {
      return deserializedIndexKey;
    }

    @Override
    public Object getDeserializedValue() {
      return value;
    }

    @Override
    public Object getDeserializedRegionKey() {
      return regionEntry.getKey();
    }

    public RegionEntry getRegionEntry() {
      return regionEntry;
    }

    @Override
    public boolean isUpdateInProgress() {
      return updateInProgress || regionEntry.isUpdateInProgress()
      // The index update could have started just before the iterator was created. The entry still
      // needs to be re-evaluated in this case.
          || IndexManager.needsRecalculation(iteratorStartTime, regionEntry.getLastModified());
    }
  }

  class CachedEntryWrapper {

    private Object key, value;

    public CachedEntryWrapper(NonTXEntry entry) {
      if (IndexManager.testHook != null) {
        IndexManager.testHook.hook(201);
      }
      this.key = entry.getKey();
      this.value = entry.getValue();
    }

    public Object getKey() {
      return this.key;
    }

    public Object getValue() {
      return this.value;
    }

    public String toString() {
      return new StringBuilder("CachedEntryWrapper@")
          .append(Integer.toHexString(System.identityHashCode(this))).append(' ').append(this.key)
          .append(' ').append(this.value).toString();
    }
  }

}
