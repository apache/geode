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
package com.gemstone.gemfire.cache.query.internal.index;

import java.util.Collection;
import java.util.Iterator;

import com.gemstone.gemfire.cache.EntryDestroyedException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.TypeMismatchException;
import com.gemstone.gemfire.cache.query.internal.parse.OQLLexerTokenTypes;
import com.gemstone.gemfire.cache.query.internal.types.TypeUtils;
import com.gemstone.gemfire.internal.cache.CachedDeserializable;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.RegionEntry;
import com.gemstone.gemfire.internal.cache.RegionEntryContext;
import com.gemstone.gemfire.internal.cache.Token;
import com.gemstone.gemfire.internal.cache.LocalRegion.NonTXEntry;
import com.gemstone.gemfire.internal.cache.persistence.query.CloseableIterator;
import com.gemstone.gemfire.internal.cache.persistence.query.IndexMap;

/**
 * Implementation of IndexStorage that is backed by an IndexMap
 *
 */
public class MapIndexStore implements IndexStore {

  IndexMap indexMap;
  Region region;
  private boolean indexOnValues = false;
  private boolean indexOnRegionKeys = false;
  private static boolean needToCallHasNext = true;
  
  public MapIndexStore(IndexMap indexMap, Region region) {
    this.indexMap = indexMap;
    this.region = region;
  }
  
  // @todo replace with null when backing structure supports null values
  public void addMapping(Object indexKey, RegionEntry re) {
    indexMap.put(indexKey, re.getKey(), "STUB");
  }
  
  public void updateMapping(Object indexKey, Object oldKey, RegionEntry re, Object oldValue) {
    addMapping(indexKey, re);
  }

  public void removeMapping(Object indexKey, RegionEntry re) {
    indexMap.remove(indexKey, re.getKey());
  }

  public String printAll() {
    return "";
  }

  public CloseableIterator<IndexStoreEntry> get(Object key) {
    return new MapIndexStoreIterator(indexMap.get(key), indexOnValues, indexOnRegionKeys);
  }


  public CloseableIterator<IndexStoreEntry> iterator(Object start,
      boolean startInclusive, Object end, boolean endInclusive) {
    return iterator(start, startInclusive, end, endInclusive, null);
  }
  
  public CloseableIterator<IndexStoreEntry> iterator(Object start,
      boolean startInclusive, Object end, boolean endInclusive,
      Collection keysToRemove) {
    //REMOVE THESE CHECKS ONCE nulls are supported
    //These checks will help us get past a certain number of tests but not all of them
    //order by and what not will still probably fail
    if (start == null) {
      return this.descendingIterator(end, endInclusive, keysToRemove);
    }
    else if (end == null) {
      return this.iterator(start, startInclusive, keysToRemove);
    }
    return new MapIndexStoreIterator(indexMap.iterator(start, startInclusive, end, endInclusive), keysToRemove, indexOnValues, indexOnRegionKeys);
  }


  public CloseableIterator<IndexStoreEntry> iterator(Object start,
      boolean startInclusive) {
    return iterator(start, startInclusive, null); 
  }
  
  public CloseableIterator<IndexStoreEntry> iterator(Object start,
      boolean startInclusive, Collection keysToRemove) {
    return new MapIndexStoreIterator(indexMap.iterator(start, startInclusive), keysToRemove, indexOnValues, indexOnRegionKeys);
  }

  public CloseableIterator<IndexStoreEntry> iterator() {
    return iterator(null);
  }

  public CloseableIterator<IndexStoreEntry> iterator(Collection keysToRemove) {
    return new MapIndexStoreIterator(indexMap.iterator(), keysToRemove, indexOnValues, indexOnRegionKeys);
  }

  public CloseableIterator<IndexStoreEntry> descendingIterator(Object end,
      boolean endInclusive) {
    return descendingIterator(end, endInclusive, null);
  }

  public CloseableIterator<IndexStoreEntry> descendingIterator(Object end,
      boolean endInclusive, Collection keysToRemove) {
    return new MapIndexStoreIterator(indexMap.descendingIterator(end, endInclusive), keysToRemove, indexOnValues, indexOnRegionKeys);
  }

  public CloseableIterator<IndexStoreEntry> descendingIterator() {
    return descendingIterator(null);
  }

  public CloseableIterator<IndexStoreEntry> descendingIterator(
      Collection keysToRemove) {
   return new MapIndexStoreIterator(indexMap.descendingIterator(), keysToRemove, indexOnValues, indexOnRegionKeys);
  }

  public CloseableIterator<IndexStoreEntry> descendingIterator(Object start,
      boolean startInclusive, Object end, boolean endInclusive,
      Collection keysToRemove) {
    //@todo change to descending once it is supported
    return new MapIndexStoreIterator(indexMap.iterator(start, startInclusive, end, endInclusive), keysToRemove, indexOnValues, indexOnRegionKeys);
  }

  public int size() {
    return 1;//(int)this.indexMap.size();
  }
  
  public int size(Object key) {
    //return Long.valueOf(indexMap.size(key, key)).intValue();
    return 1;
  }
  
  @Override
  public boolean clear() {
    indexMap.destroy();
    return true;
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
    return this.indexOnValues;
  }

  @Override
  public void setIndexOnValues(boolean indexOnValues) {
    this.indexOnValues = indexOnValues;
  }

  @Override
  public Object getTargetObject(RegionEntry entry) {
    if (indexOnValues) {
      Object o = entry.getValue((LocalRegion) region);
      if (o instanceof CachedDeserializable) {
          return ((CachedDeserializable) o).getDeserializedValue(
              region, entry);
      }
      return o;
    } else if (indexOnRegionKeys) {
      return entry.getKey();
    }
    return ((LocalRegion) region).new NonTXEntry(entry);
  }

  @Override
  public Object getTargetObjectInVM(RegionEntry entry) {
    if (indexOnValues) {
      Object o = entry.getValueInVM((LocalRegion) region);
      if (o instanceof CachedDeserializable) {
          return ((CachedDeserializable) o).getDeserializedValue(
              region, entry);
      }
      return o;
    } else if (indexOnRegionKeys) {
      return entry.getKey();
    }
    return ((LocalRegion) region).new NonTXEntry(entry);
  }

  /**
   * Wraps a CloseableIterator<IndexMap.IndexEntry> and returns a IndexStorageEntry when iterating over
   */
  private class MapIndexStoreIterator implements CloseableIterator<IndexStoreEntry> {
    final CloseableIterator<IndexMap.IndexEntry> iterator;
    final MapIndexStoreEntry nextEntry = new MapIndexStoreEntry();
    final Collection keysToRemove;
    final boolean indexOnRegionKeys;
    final boolean indexOnValues;
    
    private MapIndexStoreIterator(CloseableIterator<IndexMap.IndexEntry> iterator, Collection keysToRemove, boolean indexOnValues, boolean indexOnRegionKeys) {
      this.iterator = iterator;
      this.keysToRemove = keysToRemove;
      this.indexOnRegionKeys = indexOnRegionKeys;
      this.indexOnValues = indexOnValues;
    }
    
    private MapIndexStoreIterator(CloseableIterator<IndexMap.IndexEntry> iterator, boolean indexOnValues, boolean indexOnRegionKeys) {
      this(iterator, null, indexOnValues, indexOnRegionKeys);
    }
    
    public boolean hasNext() {
      if (iterator.hasNext()) {
        IndexMap.IndexEntry indexEntry = iterator.next();
        nextEntry.setIndexEntry(indexEntry);
        needToCallHasNext = false;
        if (keysToRemove != null) {
          Iterator keysToRemoveIterator = keysToRemove.iterator();
          while (keysToRemoveIterator.hasNext()) {
            try {
              if (TypeUtils
                  .compare(nextEntry.getDeserializedValue(), keysToRemoveIterator.next(), OQLLexerTokenTypes.TOK_EQ).equals(
                      Boolean.TRUE)) {
                return hasNext();
              }
            } catch (TypeMismatchException e) {
              // they are not equals, so we just continue iterating
            }
          }
        }
        return true;
      }
      return false;
    }

    /** 
     * do not retain a reference to the returned object.
     */
    public IndexStoreEntry next() {
      if (needToCallHasNext) {
        hasNext();
      }
      //we set this again so that we know that has next needs to be called before the next call
      //hasNext will unset it.  This forces us to call hasNext automatically if the user does not
      needToCallHasNext = true;       
      return nextEntry;
    }

    public void remove() {
      iterator.remove();
    }

    public void close() {
      iterator.close();
      nextEntry.setIndexEntry(null);
    }
  }
  
  
  /**
   * A helper class that wraps and deserializes IndexEntry values for indexes
   *
   */
  private class MapIndexStoreEntry implements IndexStoreEntry {

    IndexMap.IndexEntry entry;
    
    /**
     * sets the IndexEntry 
     */
    void setIndexEntry(IndexMap.IndexEntry entry) {
      this.entry = entry;
    }
    
    public Object getDeserializedKey() {
      return entry.getKey().getDeserializedForReading();
    }

    //Since we are not storing the actual value in the index, we need to 
    //retreive the value from the region
    public Object getDeserializedValue() {
      if (indexOnValues) {
        return region.get(entry.getRegionKey().getDeserializedForReading());
      }
      else if (indexOnRegionKeys){
        return getDeserializedRegionKey();
      }
      else {
        return new EntrySet(getDeserializedRegionKey(), region.get(entry.getRegionKey().getDeserializedForReading()));
      }
    }

    public Object getDeserializedRegionKey() {
        return entry.getRegionKey().getDeserializedForReading();
    }
    
    public boolean isUpdateInProgress() {
      return false;
    }
  }
  
  //wrapper class for when the index is being queried with a map query
  private static class EntrySet {
    public Object key;
    public Object value;
    private EntrySet(Object key, Object value) {
      this.key = key;
      this.value = value;
    }
    
    public Object getKey() {
      return key;
    }
    
    public Object getValue() {
      return value;
    }
  }

}
