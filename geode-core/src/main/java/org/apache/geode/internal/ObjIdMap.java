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
package org.apache.geode.internal;

import java.lang.ref.WeakReference;


/**
 * An <code>ObjIdMap</code> maps GemFire object ids to an <code>Object</code>. This is an
 * optimization because using a {@link java.util.HashMap} for this purposed proved to be too slow
 * because of all of the {@link Integer}s that had to be created.
 */
public class ObjIdMap {

  /** The contents of the map */
  protected Entry[] table;

  /** The total number of mappings in the map */
  private int count;

  /**
   * Once the number of mappings in the map exceeds the threshold, the map is rehashed. The
   * threshold is the (capacity * loadFactor). capacity is table.length
   */
  private int threshold;

  /** The load factor of the map */
  private float loadFactor;

  public final Object rehashLock = new Object();

  //////////////////// Constructors ////////////////////

  /**
   * Creates a new, empty map with the given initial capacity (number of buckets) and load factor.
   */
  public ObjIdMap(int initialCapacity, float loadFactor) {
    if (initialCapacity < 0) {
      throw new IllegalArgumentException(String.format("Illegal Initial Capacity: %s",
          Integer.valueOf(initialCapacity)));
    }

    if (loadFactor <= 0 || Float.isNaN(loadFactor)) {
      throw new IllegalArgumentException(
          String.format("Illegal Load factor: %s", new Float(loadFactor)));
    }

    if (initialCapacity == 0) {
      initialCapacity = 1;
    }

    this.loadFactor = loadFactor;
    table = new Entry[initialCapacity];
    threshold = (int) (initialCapacity * loadFactor);
  }

  /**
   * Creates a new, empty map with the default initial capacity (11 buckets) and load factor (0.75).
   */
  public ObjIdMap() {
    this(11, 0.75f);
  }

  /**
   * Create a new map which will contain all the contents of the oldMap and then add the specified
   * key and value.
   */
  public ObjIdMap(ObjIdMap oldMap, int addKey, Object addValue) {
    this.loadFactor = oldMap.loadFactor;
    // we do a +2 to make enough room for one more entry
    // since we think the loadFactory is 0.5
    rehash(oldMap.table, oldMap.count, oldMap.count + 2);
    put(addKey, addValue);
  }

  /**
   * Create a new map which will contain all the contents of the oldMap.
   */
  public ObjIdMap(ObjIdMap oldMap) {
    this.table = new Entry[oldMap.table.length];
    System.arraycopy(oldMap.table, 0, this.table, 0, this.table.length);
    this.count = oldMap.count;
    this.threshold = oldMap.threshold;
    this.loadFactor = oldMap.loadFactor;
  }
  //////////////////// Instance Methods ////////////////////

  /**
   * Returns the number of mappings in this map
   */
  public int size() {
    return this.count;
  }

  /**
   * Returns <code>true</code> if this map contains a mapping for the given key.
   *
   * @throws IllegalArgumentException <code>key</code> is less than zero
   */
  public boolean containsKey(int key) {

    Entry[] table = this.table;
    int bucket = Math.abs(key) % table.length;
    for (Entry e = table[bucket]; e != null; e = e.next) {
      if (e.key == key) {
        return true;
      }
    }

    return false;
  }

  /**
   * Returns the object to which the given key is mapped. If no object is mapped to the given key,
   * <code>null</code> is returned.
   *
   * @throws IllegalArgumentException <code>key</code> is less than zero
   */
  public Object get(int key) {

    Entry[] table = this.table;
    int bucket = Math.abs(key) % table.length;
    for (Entry e = table[bucket]; e != null; e = e.next) {
      if (e.key == key) {
        return e.value;
      }
    }

    return null;
  }

  /**
   * Rehashes this map into a new map with a large number of buckets. It is called when the number
   * of entries in the map exceeds the capacity and load factor.
   */
  private void rehash() {
    rehash(this.table, this.count, this.count * 2 + 1);
  }

  private void rehash(Entry[] oldMap, int newCount, int newCapacity) {
    int oldCapacity = oldMap.length;

    Entry newMap[] = new Entry[newCapacity];

    synchronized (rehashLock) {
      for (int i = oldCapacity; i-- > 0;) {
        for (Entry old = oldMap[i]; old != null;) {
          Entry e = old;
          old = old.next;

          if (e.value != null && e.value instanceof WeakReference) {
            WeakReference r = (WeakReference) e.value;
            if (r.get() == null) {
              // don't copy this one into the new table since its value was gc'd
              newCount--;
              continue;
            }
          }
          int index = Math.abs(e.key) % newCapacity;
          e.next = newMap[index];
          newMap[index] = e;
        }
      }

      threshold = (int) (newCapacity * loadFactor);
      count = newCount;
      table = newMap;
    }
  }

  /**
   * Creates a mapping between the given key (object id) and an object. Returns the previous value,
   * or <code>null</code> if there was none.
   *
   * @throws IllegalArgumentException <code>key</code> is less than zero
   */
  public Object put(int key, Object value) {

    // Is the key already in the table?
    int bucket = Math.abs(key) % table.length;
    for (Entry e = table[bucket]; e != null; e = e.next) {
      if (e.key == key) {
        Object old = e.value;
        e.value = value;
        return old;
      }
    }

    // Adjust the table, if necessary
    if (this.count >= this.threshold) {
      rehash();
      // table = this.table; assignment has no effect
      bucket = Math.abs(key) % table.length;
    }

    Entry e = new Entry();
    e.key = key;
    e.value = value;
    e.next = table[bucket];
    table[bucket] = e;
    count++;
    return null;
  }

  /**
   * Removes the mapping for the given key. Returns the object to which the key was mapped, or
   * <code>null</code> otherwise.
   */
  public Object remove(int key) {
    Entry[] table = this.table;
    int bucket = Math.abs(key) % table.length;

    for (Entry e = table[bucket], prev = null; e != null; prev = e, e = e.next) {
      if (key == e.key) {
        if (prev != null)
          prev.next = e.next;
        else
          table[bucket] = e.next;

        count--;
        Object oldValue = e.value;
        e.value = null;
        return oldValue;
      }
    }

    return null;
  }

  /**
   * Returns all of the objects in the map
   */
  public Object[] values() {
    Object[] values = new Object[this.size()];

    Entry[] table = this.table;
    int i = 0;
    for (int bucket = 0; bucket < table.length; bucket++) {
      for (Entry e = table[bucket]; e != null; e = e.next) {
        values[i++] = e.value;
      }
    }

    return values;
  }

  /**
   * Returns an iterator over the {@link Entry}s of this map. Note that this iterator is <b>not</b>
   * fail-fast. That is, it is the user's responsibility to ensure that the map does not change
   * while it is being iterated over.
   */
  public EntryIterator iterator() {
    return new EntryIterator();
  }

  /////////////////////// Inner Classes ///////////////////////

  /**
   * Inner class that represents an entry in the map
   */
  public static class Entry {
    /** The key of the entry */
    int key;

    /** The value of the entry */
    Object value;

    /** The next entry in the collision chain */
    Entry next;

    public int getKey() {
      return this.key;
    }

    public Object getValue() {
      return this.value;
    }
  }

  /**
   * A class for iterating over the contents of an <code>ObjIdMap</code>
   */
  public class EntryIterator {
    /** The current collision chain we're traversing */
    private int index = 0;

    /** The next Entry we'll iterate over */
    private Entry next = null;

    //////////////////// Instance Methods ////////////////////

    /**
     * Returns the next Entry to visit. Will return <code>null</code> after we have iterated through
     * all of the entries.
     */
    public Entry next() {
      while (this.next == null && this.index < table.length) {
        if (table[index] != null) {
          this.next = table[index];
        }
        this.index++;
      }

      Entry oldNext = this.next;
      if (oldNext != null) {
        this.next = oldNext.next;
      }
      return oldNext;
    }

  }

}
