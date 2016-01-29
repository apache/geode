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
 * ConcurrentHashMap implementation adapted from JSR 166 backport
 * (http://backport-jsr166.sourceforge.net) JDK5 version release 3.1
 * with modifications to use generics where appropriate:
 * backport-util-concurrent-Java60-3.1-src.tar.gz 
 * 
 * Primary change is to allow HashEntry be an interface so that custom HashEntry
 * implementations can be plugged in. These HashEntry objects are now assumed to
 * be immutable in the sense that they cannot and should not be cloned in a
 * rehash, and the rehash mechanism has been recoded using locking for that. For
 * GemFire/SQLFire, this is now used to plugin the RegionEntry implementation
 * directly as a HashEntry instead of having it as a value and then HashEntry as
 * a separate object having references to key/value which reduces the entry
 * overhead substantially. Other change is to add a "create" method that creates
 * a new object using the {@link MapCallback} interface only if required unlike
 * "putIfAbsent" that requires a pre-built object that may ultimately be thrown
 * away. Also added a "removeConditionally" method that allows for evaluation of
 * an arbitrary condition before removal from the map (unlike the normal
 * "remove" that can only check for equality with a provided object). In
 * addition, the segments are now locked using read-write locks. File has been
 * reformatted to conform to GemStone conventions.
 * GemStone additions have been marked with "GemStone addition".
 * GemStone changes have been marked with "GemStone change(s)".
 * 
 * Original license follows below.
 */

/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */

package com.gemstone.gemfire.internal.util.concurrent;

import java.io.IOException;
import java.io.Serializable;
import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderEventImpl;
import com.gemstone.gemfire.internal.cache.OffHeapRegionEntry;
import com.gemstone.gemfire.internal.cache.RegionEntry;
import com.gemstone.gemfire.internal.offheap.OffHeapRegionEntryHelper;
import com.gemstone.gemfire.internal.size.SingleObjectSizer;
import com.gemstone.gemfire.internal.util.ArrayUtils;

/**
 * A hash table supporting full concurrency of retrievals and adjustable
 * expected concurrency for updates. This class obeys the same functional
 * specification as {@link java.util.Hashtable}, and includes versions of
 * methods corresponding to each method of <tt>Hashtable</tt>. However, even
 * though all operations are thread-safe, retrieval operations do <em>not</em>
 * entail locking, and there is <em>not</em> any support for locking the entire
 * table in a way that prevents all access. This class is fully interoperable
 * with <tt>Hashtable</tt> in programs that rely on its thread safety but not on
 * its synchronization details.
 * 
 * <p>
 * Retrieval operations (including <tt>get</tt>) generally do not block, so may
 * overlap with update operations (including <tt>put</tt> and <tt>remove</tt>).
 * Retrievals reflect the results of the most recently <em>completed</em> update
 * operations holding upon their onset. For aggregate operations such as
 * <tt>putAll</tt> and <tt>clear</tt>, concurrent retrievals may reflect
 * insertion or removal of only some entries. Similarly, Iterators and
 * Enumerations return elements reflecting the state of the hash table at some
 * point at or since the creation of the iterator/enumeration. They do
 * <em>not</em> throw {@link java.util.ConcurrentModificationException}.
 * However, iterators are designed to be used by only one thread at a time.
 * 
 * <p>
 * The allowed concurrency among update operations is guided by the optional
 * <tt>concurrencyLevel</tt> constructor argument (default <tt>16</tt>), which
 * is used as a hint for internal sizing. The table is internally partitioned to
 * try to permit the indicated number of concurrent updates without contention.
 * Because placement in hash tables is essentially random, the actual
 * concurrency will vary. Ideally, you should choose a value to accommodate as
 * many threads as will ever concurrently modify the table. Using a
 * significantly higher value than you need can waste space and time, and a
 * significantly lower value can lead to thread contention. But overestimates
 * and underestimates within an order of magnitude do not usually have much
 * noticeable impact. A value of one is appropriate when it is known that only
 * one thread will modify and all others will only read. Also, resizing this or
 * any other kind of hash table is a relatively slow operation, so, when
 * possible, it is a good idea to provide estimates of expected table sizes in
 * constructors.
 * 
 * <p>
 * This class and its views and iterators implement all of the <em>optional</em>
 * methods of the {@link Map} and {@link Iterator} interfaces.
 * 
 * <p>
 * Like {@link java.util.Hashtable} but unlike {@link java.util.HashMap}, this
 * class does <em>not</em> allow <tt>null</tt> to be used as a key or value.
 * 
 * <p>
 * This class is a member of the <a href="{@docRoot}
 * /../technotes/guides/collections/index.html"> Java Collections Framework</a>.
 * 
 * @since 1.5
 * @author Doug Lea
 * @param <K>
 *          the type of keys maintained by this map
 * @param <V>
 *          the type of mapped values
 */
public class CustomEntryConcurrentHashMap<K, V> extends AbstractMap<K, V> implements
    ConcurrentMap<K, V>, Serializable {

  private static final long serialVersionUID = -7056732555635108300L;

  /*
   * The basic strategy is to subdivide the table among Segments,
   * each of which itself is a concurrently readable hash table.
   */

  /* ---------------- Constants -------------- */

  /**
   * The default initial capacity for this table, used when not otherwise
   * specified in a constructor.
   */
  public static final int DEFAULT_INITIAL_CAPACITY = 16;

  /**
   * The default load factor for this table, used when not otherwise specified
   * in a constructor.
   */
  public static final float DEFAULT_LOAD_FACTOR = 0.75f;

  /**
   * The default concurrency level for this table, used when not otherwise
   * specified in a constructor.
   */
  public static final int DEFAULT_CONCURRENCY_LEVEL = 16;

  /**
   * The maximum capacity, used if a higher value is implicitly specified by
   * either of the constructors with arguments. MUST be a power of two <= 1<<30
   * to ensure that entries are indexable using ints.
   */
  static final int MAXIMUM_CAPACITY = 1 << 30;

  /**
   * The maximum number of segments to allow; used to bound constructor
   * arguments.
   */
  static final int MAX_SEGMENTS = 1 << 16; // slightly conservative

  /**
   * Number of unsynchronized retries in size and containsValue methods before
   * resorting to locking. This is used to avoid unbounded retries if tables
   * undergo continuous modification which would make it impossible to obtain an
   * accurate result.
   */
  static final int RETRIES_BEFORE_LOCK = 2;

// GemStone addition
  /**
   * Token object to indicate that {@link #remove(Object)} does not need to
   * compare against provided value before removing from segment.
   */
  private static final Object NO_OBJECT_TOKEN = new Object();

// End GemStone addition

  /* ---------------- Fields -------------- */

  /**
   * Mask value for indexing into segments. The upper bits of a key's hash code
   * are used to choose the segment.
   */
  final int segmentMask;

  /**
   * Shift value for indexing within segments.
   */
  final int segmentShift;

  /**
   * The segments, each of which is a specialized hash table
   */
  final Segment<K, V>[] segments;

  /**
   * {@link HashEntryCreator} for the map to create {@link HashEntry}s.
   */
  final HashEntryCreator<K, V> entryCreator;

  /**
   * If true then use equals for comparing key and value equality else use
   * reference-equality like an {@link IdentityHashMap}.
   */
  final boolean compareValues;

  transient Set<K> keySet;
  transient Set<Map.Entry<K, V>> entrySet;
  transient Set<Map.Entry<K, V>> reusableEntrySet; // GemStone addition
  transient Collection<V> values;

  /* ---------------- Small Utilities -------------- */

  /**
   * Applies a supplemental hash function to a given hashCode, which defends
   * against poor quality hash functions. This is critical because
   * ConcurrentHashMap uses power-of-two length hash tables, that otherwise
   * encounter collisions for hashCodes that do not differ in lower or upper
   * bits.
   */
  public static final int keyHash(final Object o, final boolean compareValues) {
    int h = compareValues ? o.hashCode() : System.identityHashCode(o);
    // Spread bits to regularize both segment and index locations,
    // using variant of single-word Wang/Jenkins hash.
    h += (h << 15) ^ 0xffffcd7d;
    h ^= (h >>> 10);
    h += (h << 3);
    h ^= (h >>> 6);
    h += (h << 2) + (h << 14);
    return h ^ (h >>> 16);
  }

  /**
   * Returns the segment that should be used for key with given hash
   * 
   * @param hash
   *          the hash code for the key
   * @return the segment
   */
  final Segment<K, V> segmentFor(final int hash) {
    if (this.segmentMask == 0) {
      return this.segments[0];
    }
    return this.segments[(hash >>> this.segmentShift) & this.segmentMask];
  }

  /* ---------------- Inner Classes -------------- */

// GemStone addition
// GemStone changed HashEntry to be an interface with original HashEntry
// as the default implementation HashEntryImpl.

  /**
   * [sumedh] Interface for ConcurrentHashMap list entry. Note that this is
   * never exported out as a user-visible Map.Entry.
   * 
   * Made this public so RegionEntries can directly implement this to reduce
   * memory overhead of separate {@link com.gemstone.gemfire.internal.util.concurrent.CustomEntryConcurrentHashMap.HashEntry} objects for each entry in the
   * map.
   */
  public static interface HashEntry<K, V> {

    /**
     * Get the key object for this entry.
     */
    K getKey();
    
    /**
     * Return true if the entry's key is equal to k.
     * GemFire addition to deal with inline keys.
     */
    boolean isKeyEqual(Object k);

    /**
     * Get the value for this entry.
     */
    V getMapValue();

    /**
     * Set the value for this entry.
     */
    void setMapValue(V newValue);

    /**
     * Get the hash value for this entry.
     */
    int getEntryHash();

    /**
     * Get the next entry, if any, in the linear chain.
     */
    HashEntry<K, V> getNextEntry();

    /**
     * Set the next entry in the linear chain.
     */
    void setNextEntry(HashEntry<K, V> n);
  }

  /**
   * ConcurrentHashMap list entry. Note that this is never exported out as a
   * user-visible Map.Entry.
   * 
   * Because the value field is volatile, not final, it is legal wrt the Java
   * Memory Model for an unsynchronized reader to see null instead of initial
   * value when read via a data race. Although a reordering leading to this is
   * not likely to ever actually occur, the Segment.readValueUnderLock method is
   * used as a backup in case a null (pre-initialized) value is ever seen in an
   * unsynchronized access method.
   */
  static final class HashEntryImpl<K, V> implements HashEntry<K, V> {

    protected final K key;

    protected final int hash;

    protected volatile V value;

    protected HashEntry<K, V> next;

    private final HashEntry<K, V> wrappedEntry;

    HashEntryImpl(final K key, final int hash, final HashEntry<K, V> next,
        final V value, final HashEntry<K, V> wrappedEntry) {
      this.key = key;
      this.hash = hash;
      this.next = next;
      this.value = value;
      this.wrappedEntry = wrappedEntry;
    }

    /**
     * @see CustomEntryConcurrentHashMap.HashEntry#getKey()
     */
    public final K getKey() {
      return this.key;
    }

    /**
     * @see CustomEntryConcurrentHashMap.HashEntry#getMapValue()
     */
    public final V getMapValue() {
      return this.value;
    }

    /**
     * @see CustomEntryConcurrentHashMap.HashEntry#setMapValue(Object)
     */
    public final void setMapValue(V newValue) {
      this.value = newValue;
    }

    /**
     * @see CustomEntryConcurrentHashMap.HashEntry#getEntryHash()
     */
    public final int getEntryHash() {
      return this.hash;
    }

    /**
     * @see CustomEntryConcurrentHashMap.HashEntry#getNextEntry()
     */
    public final HashEntry<K, V> getNextEntry() {
      return this.next;
    }

    /**
     * @see CustomEntryConcurrentHashMap.HashEntry#setNextEntry
     */
    public final void setNextEntry(final HashEntry<K, V> n) {
      this.next = n;
    }

    @Override
    public boolean isKeyEqual(Object k) {
      return k.equals(getKey());
    }
  }

  /**
   * Interface to enable creation of new {@link com.gemstone.gemfire.internal.util.concurrent.CustomEntryConcurrentHashMap.HashEntry} objects by caller.
   * This can be used, for example, to return GemFire RegionEntries directly.
   */
  public static interface HashEntryCreator<K, V> {

    /**
     * Create a new {@link com.gemstone.gemfire.internal.util.concurrent.CustomEntryConcurrentHashMap.HashEntry} given the key, hash, value and next
     * element.
     */
    public HashEntry<K, V> newEntry(K key, int hash, HashEntry<K, V> next,
        V value);

    /**
     * Get the hashCode for given key object.
     */
    public int keyHashCode(Object key, boolean compareValues);
  }

// End GemStone addition

  /**
   * Segments are specialized versions of hash tables. This subclasses from
   * ReentrantLock opportunistically, just to simplify some locking and avoid
   * separate construction.
   */
  static class Segment<K, V> extends ReentrantReadWriteLock implements
      Serializable {

    /*
     * Segments maintain a table of entry lists that are ALWAYS
     * kept in a consistent state, so can be read without locking.
     * Next fields of nodes are immutable (final).  All list
     * additions are performed at the front of each bin. This
     * makes it easy to check changes, and also fast to traverse.
     * When nodes would otherwise be changed, new nodes are
     * created to replace them. This works well for hash tables
     * since the bin lists tend to be short. (The average length
     * is less than two for the default load factor threshold.)
     *
     * Read operations can thus proceed without locking, but rely
     * on selected uses of volatiles to ensure that completed
     * write operations performed by other threads are
     * noticed. For most purposes, the "count" field, tracking the
     * number of elements, serves as that volatile variable
     * ensuring visibility.  This is convenient because this field
     * needs to be read in many read operations anyway:
     *
     *   - All (unsynchronized) read operations must first read the
     *     "count" field, and should not look at table entries if
     *     it is 0.
     *
     *   - All (synchronized) write operations should write to
     *     the "count" field after structurally changing any bin.
     *     The operations must not take any action that could even
     *     momentarily cause a concurrent read operation to see
     *     inconsistent data. This is made easier by the nature of
     *     the read operations in Map. For example, no operation
     *     can reveal that the table has grown but the threshold
     *     has not yet been updated, so there are no atomicity
     *     requirements for this with respect to reads.
     *
     * As a guide, all critical volatile reads and writes to the
     * count field are marked in code comments.
     */

    private static final long serialVersionUID = -6972364566212065192L;

    /**
     * The number of elements in this segment's region.
     */
    transient volatile int count;

    /**
     * Number of updates that alter the size of the table. This is used during
     * bulk-read methods to make sure they see a consistent snapshot: If
     * modCounts change during a traversal of segments computing size or
     * checking containsValue, then we might have an inconsistent view of state
     * so (usually) must retry.
     */
    transient int modCount;

    /**
     * The table is rehashed when its size exceeds this threshold. (The value of
     * this field is always <tt>(int)(capacity *
     * loadFactor)</tt>.)
     */
    transient int threshold;

    /**
     * The per-segment table.
     */
    transient volatile HashEntry<K, V>[] table;

    /**
     * The load factor for the hash table. Even though this value is same for
     * all segments, it is replicated to avoid needing links to outer object.
     * 
     * @serial
     */
    final float loadFactor;

// GemStone addition

    /**
     * {@link com.gemstone.gemfire.internal.util.concurrent.CustomEntryConcurrentHashMap.HashEntryCreator} for the map to create {@link com.gemstone.gemfire.internal.util.concurrent.CustomEntryConcurrentHashMap.HashEntry}s.
     */
    final HashEntryCreator<K, V> entryCreator;

    /**
     * Lock used when updating the {@link com.gemstone.gemfire.internal.util.concurrent.CustomEntryConcurrentHashMap.HashEntry#getNextEntry()} link of an
     * entry.
     */
    final ReentrantReadWriteLock listUpdateLock;

// End GemStone addition

    Segment(final int initialCapacity, final float lf,
        final HashEntryCreator<K, V> entryCreator) {
      this.loadFactor = lf;
      this.entryCreator = entryCreator;
      this.listUpdateLock = new ReentrantReadWriteLock();
      setTable(Segment.<K, V> newEntryArray(initialCapacity));
    }

    @SuppressWarnings("unchecked")
    static <K, V> Segment<K, V>[] newArray(final int i) {
      return new Segment[i];
    }

// GemStone added the method below
    @SuppressWarnings("unchecked")
    static <K, V> HashEntry<K, V>[] newEntryArray(final int size) {
      return new HashEntry[size];
    }

    /**
     * Sets table to new HashEntry array. Call only while holding lock or in
     * constructor.
     */
    final void setTable(final HashEntry<K, V>[] newTable) {
      this.threshold = (int)(newTable.length * this.loadFactor);
      this.table = newTable;
    }

    /**
     * Returns properly casted first entry of bin for given hash.
     */
    final HashEntry<K, V> getFirst(final int hash) {
      final HashEntry<K, V>[] tab = this.table;
      return tab[hash & (tab.length - 1)];
    }

    /**
     * Reads value field of an entry under lock. Called if value field ever
     * appears to be null. This is possible only if a compiler happens to
     * reorder a HashEntry initialization with its table assignment, which is
     * legal under memory model but is not known to ever occur.
     */
    final V readValueUnderLock(final HashEntry<K, V> e) {
      final ReentrantReadWriteLock.ReadLock readLock = super.readLock();
      readLock.lock();
      final V v = e.getMapValue();
      readLock.unlock();
      return v;
    }

    /**
     * Added for GemFire since it stores some keys inline.
     */
    protected boolean equalityKeyCompare(final Object key, final HashEntry<K, V> mapEntry) {
      return mapEntry.isKeyEqual(key);
    }
    protected boolean equalityCompare(final Object v1, final Object v2) {
      return v1.equals(v2);
    }

    protected boolean equalityCompareWithNulls(final Object key,
        final Object mapKey) {
      if (key != mapKey) {
        if (key != null) {
          return key.equals(mapKey);
        }
        return false;
      }
      return true;
    }

    /* Specialized implementations of map methods */

    final V get(final Object key, final int hash) {
      if (this.count != 0) { // read-volatile
// GemStone change to acquire the read lock on list updates
        final ReentrantReadWriteLock.ReadLock listLock = this.listUpdateLock
            .readLock();
        listLock.lock();
        boolean lockAcquired = true;
        HashEntry<K, V> e = getFirst(hash);
        try {
          while (e != null) {
            if (e.getEntryHash() == hash && equalityKeyCompare(key, e)) {
              final V v = e.getMapValue();
              if (v != null) {
                return v;
              }
              listLock.unlock();
              lockAcquired = false;
              return readValueUnderLock(e); // recheck
            }
            e = e.getNextEntry();
          }
        } finally {
          if (lockAcquired) {
            listLock.unlock();
          }
        }
      }
      return null;
    }

    final V getNoLock(final Object key, final int hash,
        final boolean lockListForRead) {
      if (this.count != 0) { // read-volatile
// GemStone change to acquire the read lock on list updates
        ReentrantReadWriteLock.ReadLock listLock = null;
        if (lockListForRead) {
          listLock = this.listUpdateLock.readLock();
          listLock.lock();
        }
        HashEntry<K, V> e = getFirst(hash);
        try {
          while (e != null) {
            if (e.getEntryHash() == hash && equalityKeyCompare(key, e)) {
              return e.getMapValue();
            }
            e = e.getNextEntry();
          }
        } finally {
          if (lockListForRead) {
            listLock.unlock();
          }
        }
      }
      return null;
    }

    final boolean containsKey(final Object key, final int hash) {
      if (this.count != 0) { // read-volatile
// GemStone change to acquire the read lock on list updates
        final ReentrantReadWriteLock.ReadLock listLock = this.listUpdateLock
            .readLock();
        listLock.lock();
        HashEntry<K, V> e = getFirst(hash);
        try {
          while (e != null) {
            if (e.getEntryHash() == hash && equalityKeyCompare(key, e)) {
              return true;
            }
            e = e.getNextEntry();
          }
        } finally {
          listLock.unlock();
        }
      }
      return false;
    }

    final boolean containsValue(final Object value) {
      if (this.count != 0) { // read-volatile
// GemStone change to acquire the read lock on list updates
        ReentrantReadWriteLock.ReadLock readLock = this.listUpdateLock
            .readLock();
RETRYLOOP:
        for (;;) {
          readLock.lock();
          final HashEntry<K, V>[] tab = this.table;
          final int len = tab.length;
          for (int i = 0; i < len; i++) {
            for (HashEntry<K, V> e = tab[i]; e != null; e = e.getNextEntry()) {
              V v = e.getMapValue();
              if (v == null) {
                // GemStone changes BEGIN
                // go back and retry from the very start with segment read lock
                readLock.unlock();
                readLock = super.readLock();
                continue RETRYLOOP;
                /* (original code)
                v = readValueUnderLock(e);
                */
                // GemStone changes END
              }
              if (equalityCompare(value, v)) {
                readLock.unlock();
                return true;
              }
            }
          }
          readLock.unlock();
          return false;
        }
      }
      return false;
    }

    final boolean replace(final K key, final int hash, final V oldValue,
        final V newValue) {
      final ReentrantReadWriteLock.WriteLock writeLock = super.writeLock();
      writeLock.lock();
      try {
        HashEntry<K, V> e = getFirst(hash);
        while (e != null && (e.getEntryHash() != hash
            || !equalityKeyCompare(key, e))) {
          e = e.getNextEntry();
        }

        boolean replaced = false;
        if (e != null && equalityCompare(oldValue, e.getMapValue())) {
          replaced = true;
          e.setMapValue(newValue);
        }
        return replaced;
      } finally {
        writeLock.unlock();
      }
    }

    final V replace(final K key, final int hash, final V newValue) {
      final ReentrantReadWriteLock.WriteLock writeLock = super.writeLock();
      writeLock.lock();
      try {
        HashEntry<K, V> e = getFirst(hash);
        while (e != null && (e.getEntryHash() != hash
            || !equalityKeyCompare(key, e))) {
          e = e.getNextEntry();
        }

        V oldValue = null;
        if (e != null) {
          oldValue = e.getMapValue();
          e.setMapValue(newValue);
        }
        return oldValue;
      } finally {
        writeLock.unlock();
      }
    }

    final V put(final K key, final int hash, final V value,
        final boolean onlyIfAbsent) {
      final ReentrantReadWriteLock.WriteLock writeLock = super.writeLock();
      writeLock.lock();
      try {
        int c = this.count;
        if (c++ > this.threshold) {
          rehash();
        }
        final HashEntry<K, V>[] tab = this.table;
        final int index = hash & (tab.length - 1);
        final HashEntry<K, V> first = tab[index];
        HashEntry<K, V> e = first;
        while (e != null && (e.getEntryHash() != hash
            || !equalityKeyCompare(key, e))) {
          e = e.getNextEntry();
        }

        final V oldValue;
        if (e != null) {
          oldValue = e.getMapValue();
          if (!onlyIfAbsent) {
            e.setMapValue(value);
          }
        }
        else {
          oldValue = null;
          ++this.modCount;
          tab[index] = this.entryCreator.newEntry(key, hash, first, value);
          this.count = c; // write-volatile
        }
        return oldValue;
      } finally {
        writeLock.unlock();
      }
    }

// GemStone additions

    final <C, P> V create(final K key, final int hash,
        final MapCallback<K, V, C, P> valueCreator, final C context,
        final P createParams, final boolean lockForRead) {
      // TODO: This can be optimized by having a special lock implementation
      // that will allow upgrade from read to write lock atomically. This can
      // cause a deadlock if two readers try to simultaneously upgrade, so the
      // upgrade should be a tryLock that will fall back to the usual way if
      // unsuccessful. The advantage of that approach is that "equals" calls in
      // the list can be avoided completely if tryLock succeeds (i.e. presumably
      // the common case of no overlap on a segment concurrently). OTOH it will
      // not be as efficient when get succeeds without a read lock (existing
      // entry) that will not need to wait for any writers.
      if (!lockForRead) {
        final V v = getNoLock(key, hash, true);
        if (v != null) {
          return v;
        }
      }
      else {
        final ReentrantReadWriteLock.ReadLock readLock = super.readLock();
        readLock.lock();
        try {
          final V v = getNoLock(key, hash, false);
          if (v != null) {
            // invoke the callback before returning
            valueCreator.oldValueRead(v);
            return v;
          }
        } finally {
          readLock.unlock();
        }
      }

      final ReentrantReadWriteLock.WriteLock writeLock = super.writeLock();
      writeLock.lock();
      try {
        int c = this.count;
        if (c++ > this.threshold) {
          rehash();
        }
        final HashEntry<K, V>[] tab = this.table;
        final int index = hash & (tab.length - 1);
        final HashEntry<K, V> first = tab[index];
        HashEntry<K, V> e = first;
        while (e != null && (e.getEntryHash() != hash
            || !equalityKeyCompare(key, e))) {
          e = e.getNextEntry();
        }

        final V currentValue;
        if (e == null) {
          ++this.modCount;
          currentValue = valueCreator.newValue(key, context, createParams);
          tab[index] = this.entryCreator.newEntry(key, hash, first,
              currentValue);
          this.count = c; // write-volatile
          return currentValue;
        }
        else {
          currentValue = e.getMapValue();
          if (lockForRead) {
            // invoke the callback before returning an existing value
            valueCreator.oldValueRead(currentValue);
          }
          return currentValue;
        }
      } finally {
        writeLock.unlock();
      }
    }

    final V get(final Object key, final int hash,
        final MapCallback<K, V, ?, ?> readCallback) {
      final ReentrantReadWriteLock.ReadLock readLock = super.readLock();
      readLock.lock();
      try {
        if (this.count != 0) { // read-volatile
          HashEntry<K, V> e = getFirst(hash);
          while (e != null) {
            if (e.getEntryHash() == hash && equalityKeyCompare(key, e)) {
              final V v = e.getMapValue();
              if (v != null) {
                if (readCallback != null) {
                  readCallback.oldValueRead(v);
                }
                return v;
              }
            }
            e = e.getNextEntry();
          }
        }
      } finally {
        readLock.unlock();
      }
      return null;
    }

// End GemStone additions

    final void rehash() {
      final HashEntry<K, V>[] oldTable = this.table;
      final int oldCapacity = oldTable.length;
      if (oldCapacity >= MAXIMUM_CAPACITY) {
        return;
      }

      /*
       * Reclassify nodes in each list to new Map.  Because we are
       * using power-of-two expansion, the elements from each bin
       * must either stay at same index, or move with a power of two
       * offset. We eliminate unnecessary node creation by catching
       * cases where old nodes can be reused because their next
       * fields won't change. Statistically, at the default
       * threshold, only about one-sixth of them need cloning when
       * a table doubles. The nodes they replace will be garbage
       * collectable as soon as they are no longer referenced by any
       * reader thread that may be in the midst of traversing table
       * right now.
       */

      final HashEntry<K, V>[] newTable = newEntryArray(oldCapacity << 1);
      this.threshold = (int)(newTable.length * this.loadFactor);
      final int sizeMask = newTable.length - 1;
      for (int i = 0; i < oldCapacity; i++) {
        // We need to guarantee that any existing reads of old Map can
        // proceed. So we cannot yet null out each bin.
        final HashEntry<K, V> e = oldTable[i];

        if (e != null) {
          final HashEntry<K, V> next = e.getNextEntry();
          final int idx = e.getEntryHash() & sizeMask;

          // Single node on list
          if (next == null) {
            newTable[idx] = e;
          }
          else {
            // Reuse trailing consecutive sequence at same slot
            HashEntry<K, V> lastRun = e;
            int lastIdx = idx;
            for (HashEntry<K, V> last = next; last != null; last = last
                .getNextEntry()) {
              final int k = last.getEntryHash() & sizeMask;
              if (k != lastIdx) {
                lastIdx = k;
                lastRun = last;
              }
            }
            newTable[lastIdx] = lastRun;

            // Clone all remaining nodes
// GemStone changes BEGIN
            // update the next entry instead of cloning the nodes in newTable;
            // this is primarily because we don't want to change
            // the underlying RegionEntry that may be used elsewhere;
            // however we create new wrapper entries for old table so that
            // iterators can continue on old table without blocking updates
            // for indefinite periods
            HashEntryImpl<K, V> newe, newp = null, newFirst = null;
            HashEntry<K, V> nextp;
            //Bug 44155 - we need to clone all of the entries, not just
            //the entries leading up to lastRun, because the entries
            //in the last run may have their next pointers changed
            //by a later rehash.
            for (HashEntry<K, V> p = e; p != null; p = nextp) {
              newe = new HashEntryImpl<K, V>(p.getKey(), p.getEntryHash(),
                  (nextp = p.getNextEntry()), p.getMapValue(), p);
              if (newp != null) {
                newp.setNextEntry(newe);
              }
              else {
                newFirst = newe;
              }
              newp = newe;
            }
            // take the listUpdate write lock before updating the next refs
            final ReentrantReadWriteLock.WriteLock listWriteLock =
                this.listUpdateLock.writeLock();
            listWriteLock.lock();
            try {
              if (newFirst != null) {
                this.table[i] = newFirst; // deliberately using volatile write
              }
              for (HashEntry<K, V> p = e; p != lastRun; p = nextp) {
                final int k = p.getEntryHash() & sizeMask;
                final HashEntry<K, V> n = newTable[k];
                nextp = p.getNextEntry();
                p.setNextEntry(n);
                newTable[k] = p;
              }
            } finally {
              listWriteLock.unlock();
            }
            /* (original code)
            for (HashEntry<K, V> p = e; p != lastRun; p = p.next) {
              final int k = p.hash & sizeMask;
              final HashEntry<K, V> n = newTable[k];
              newTable[k] = this.entryCreator.newEntry(p.key, p.hash, n,
                  p.value);
            }
            */
// GemStone changes END
          }
        }
      }
      this.table = newTable;
    }

    /**
     * Remove; match on key only if value null, else match both.
     */
// GemStone change
    // added "condition" and "removeParams" parameters
    final <C, P> V remove(final Object key, final int hash, final Object value,
        final MapCallback<K, V, C, P> condition, final C context,
        final P removeParams) {
// End GemStone change
      final ReentrantReadWriteLock.WriteLock writeLock = super.writeLock();
      writeLock.lock();
      try {
        final int c = this.count - 1;
        final HashEntry<K, V>[] tab = this.table;
        final int index = hash & (tab.length - 1);
        final HashEntry<K, V> first = tab[index];
        HashEntry<K, V> e = first;
// GemStone change
        // the entry previous to the matched one, if any
        HashEntry<K, V> p = null;
        while (e != null && (e.getEntryHash() != hash
            || !equalityKeyCompare(key, e))) {
          e = e.getNextEntry();
          if (p == null) {
            p = first;
          }
          else {
            p = p.getNextEntry();
          }
        }

        V oldValue = null;
        if (e != null) {
          final V v = e.getMapValue();
// GemStone change
          // allow for passing in a null object for comparison during remove;
          // also invoke the provided condition to check for removal
          if ((value == NO_OBJECT_TOKEN || equalityCompareWithNulls(v, value))
              && (condition == null || condition.doRemoveValue(v, context,
                  removeParams))) {
// End GemStone change
            oldValue = v;
            // All entries following removed node can stay in list,
            // but all preceding ones need to be cloned.
            ++this.modCount;
// GemStone changes BEGIN
            // update the next entry instead of cloning the nodes
            // this is primarily because we don't want to change
            // the underlying RegionEntry that may be used elsewhere
            final ReentrantReadWriteLock.WriteLock listWriteLock =
                this.listUpdateLock.writeLock();
            listWriteLock.lock();
            try {
              if (p == null) {
                tab[index] = e.getNextEntry();
              }
              else {
                p.setNextEntry(e.getNextEntry());
              }
            } finally {
              listWriteLock.unlock();
            }
            /* (original code)
            HashEntry<K, V> newFirst = e.next;
            for (HashEntry<K, V> p = first; p != e; p = p.next) {
              newFirst = this.entryCreator.newEntry(p.key, p.hash, newFirst,
                  p.value);
            }
            tab[index] = newFirst;
            */
// GemStone changes END
            this.count = c; // write-volatile
          }
        }
        return oldValue;
      } finally {
        writeLock.unlock();
      }
    }

    /**
     * GemStone added the clearedEntries param and the result
     */
    final ArrayList<HashEntry<?,?>> clear(ArrayList<HashEntry<?,?>> clearedEntries) {
      if (this.count != 0) {
        final ReentrantReadWriteLock.WriteLock writeLock = super.writeLock();
        writeLock.lock();
        try {
          final HashEntry<K, V>[] tab = this.table;
          // GemStone changes BEGIN
          boolean collectEntries = clearedEntries != null;
          if (!collectEntries) {
            // see if we have a map with off-heap region entries
            for (HashEntry<K, V> he : tab) {
              if (he != null) {
                collectEntries = he instanceof OffHeapRegionEntry;
                if (collectEntries) {
                  clearedEntries = new ArrayList<HashEntry<?, ?>>();
                }
                // after the first non-null entry we are done
                break;
              }
            }
          }
          final boolean checkForGatewaySenderEvent = OffHeapRegionEntryHelper.doesClearNeedToCheckForOffHeap();
          final boolean skipProcessOffHeap = !collectEntries && !checkForGatewaySenderEvent;
          if (skipProcessOffHeap) {
            Arrays.fill(tab, null);
          } else {
            for (int i = 0; i < tab.length; i++) {
              HashEntry<K, V> he = tab[i];
              if (he == null) continue;
              tab[i] = null;
              if (collectEntries) {
                clearedEntries.add(he);
              } else {
                for (HashEntry<K, V> p = he; p != null; p = p.getNextEntry()) {
                  if (p instanceof RegionEntry) {
                    // It is ok to call GatewaySenderEventImpl release without being synced
                    // on the region entry. It will not create an orphan.
                    GatewaySenderEventImpl.release(((RegionEntry) p)._getValue()); // OFFHEAP _getValue ok
                  }
                }
              }
            }
            // GemStone changes END
          }
          ++this.modCount;
          this.count = 0; // write-volatile
        } finally {
          writeLock.unlock();
        }
      }
      return clearedEntries; // GemStone change
    }
  }

  /**
   * Extension of {@link com.gemstone.gemfire.internal.util.concurrent.CustomEntryConcurrentHashMap.Segment} using reference-equality comparison for key,
   * value equality instead of equals method.
   * 
   * @author swale
   * @since 7.0
   */
  static final class IdentitySegment<K, V> extends Segment<K, V> implements
      Serializable {

    private static final long serialVersionUID = 3086228147110819882L;

    IdentitySegment(final int initialCapacity, final float lf,
        final HashEntryCreator<K, V> entryCreator) {
      super(initialCapacity, lf, entryCreator);
    }

    @SuppressWarnings("unchecked")
    static final <K, V> IdentitySegment<K, V>[] newArray(final int i) {
      return new IdentitySegment[i];
    }

    @Override
    protected final boolean equalityKeyCompare(final Object key,
        final HashEntry<K, V> mapEntry) {
      return key == mapEntry.getKey();
    }

    @Override
    protected final boolean equalityCompare(final Object key,
        final Object mapKey) {
      return key == mapKey;
    }

    @Override
    protected final boolean equalityCompareWithNulls(final Object key,
        final Object mapKey) {
      return key == mapKey;
    }
  }

  /* ---------------- Public operations -------------- */

  /**
   * Creates a new, empty map with the specified initial capacity, load factor
   * and concurrency level.
   * 
   * @param initialCapacity
   *          the initial capacity. The implementation performs internal sizing
   *          to accommodate this many elements.
   * @param loadFactor
   *          the load factor threshold, used to control resizing. Resizing may
   *          be performed when the average number of elements per bin exceeds
   *          this threshold.
   * @param concurrencyLevel
   *          the estimated number of concurrently updating threads. The
   *          implementation performs internal sizing to try to accommodate this
   *          many threads.
   * @throws IllegalArgumentException
   *           if the initial capacity is negative or the load factor or
   *           concurrencyLevel are nonpositive.
   */
  public CustomEntryConcurrentHashMap(final int initialCapacity, final float loadFactor,
      final int concurrencyLevel) {
    this(initialCapacity, loadFactor, concurrencyLevel, false, null);
  }

// GemStone addition

  /**
   * Creates a new, empty map with the specified initial capacity, load factor
   * and concurrency level.
   * 
   * @param initialCapacity
   *          the initial capacity. The implementation performs internal sizing
   *          to accommodate this many elements.
   * @param loadFactor
   *          the load factor threshold, used to control resizing. Resizing may
   *          be performed when the average number of elements per bin exceeds
   *          this threshold.
   * @param concurrencyLevel
   *          the estimated number of concurrently updating threads. The
   *          implementation performs internal sizing to try to accommodate this
   *          many threads.
   * @param isIdentityMap
   *          if true then this will use reference-equality instead of equals
   *          like an {@link IdentityHashMap}
   * @throws IllegalArgumentException
   *           if the initial capacity is negative or the load factor or
   *           concurrencyLevel are nonpositive.
   */
  public CustomEntryConcurrentHashMap(final int initialCapacity, final float loadFactor,
      final int concurrencyLevel, final boolean isIdentityMap) {
    this(initialCapacity, loadFactor, concurrencyLevel, isIdentityMap, null);
  }

  /**
   * Creates a new, empty map with the specified initial capacity, load factor,
   * concurrency level and custom {@link HashEntryCreator}.
   * 
   * @param initialCapacity
   *          the initial capacity. The implementation performs internal sizing
   *          to accommodate this many elements.
   * @param loadFactor
   *          the load factor threshold, used to control resizing. Resizing may
   *          be performed when the average number of elements per bin exceeds
   *          this threshold.
   * @param concurrencyLevel
   *          the estimated number of concurrently updating threads. The
   *          implementation performs internal sizing to try to accommodate this
   *          many threads.
   * @param isIdentityMap
   *          if true then this will use reference-equality instead of equals
   *          like an {@link IdentityHashMap}
   * @param entryCreator
   *          a custom {@link HashEntryCreator} for creating the map entries
   * 
   * @throws IllegalArgumentException
   *           if the initial capacity is negative or the load factor or
   *           concurrencyLevel are nonpositive.
   */
  public CustomEntryConcurrentHashMap(int initialCapacity, final float loadFactor,
      int concurrencyLevel, final boolean isIdentityMap,
      HashEntryCreator<K, V> entryCreator) {
    if (!(loadFactor > 0) || initialCapacity < 0 || concurrencyLevel <= 0) {
      throw new IllegalArgumentException();
    }

    if (concurrencyLevel > MAX_SEGMENTS) {
      concurrencyLevel = MAX_SEGMENTS;
    }

    // Find power-of-two sizes best matching arguments
    int sshift = 0;
    int ssize = 1;
    if (concurrencyLevel > 1) {
      while (ssize < concurrencyLevel) {
        ++sshift;
        ssize <<= 1;
      }
    }
    this.segmentShift = 32 - sshift;
    this.segmentMask = ssize - 1;

    if (initialCapacity > MAXIMUM_CAPACITY) {
      initialCapacity = MAXIMUM_CAPACITY;
    }
    int c = initialCapacity / ssize;
    if (c * ssize < initialCapacity) {
      ++c;
    }
    int cap = 1;
    while (cap < c) {
      cap <<= 1;
    }

    if (entryCreator == null) {
      entryCreator = new DefaultHashEntryCreator<K, V>();
    }
    if (!isIdentityMap) {
      this.compareValues = true;
      this.segments = Segment.newArray(ssize);
      this.entryCreator = entryCreator;
      for (int i = 0; i < ssize; ++i) {
        this.segments[i] = new Segment<K, V>(cap, loadFactor, entryCreator);
      }
    }
    else {
      this.compareValues = false;
      this.segments = IdentitySegment.newArray(ssize);
      this.entryCreator = entryCreator;
      for (int i = 0; i < ssize; ++i) {
        this.segments[i] = new IdentitySegment<K, V>(cap, loadFactor,
            entryCreator);
      }
    }
  }

  static final class DefaultHashEntryCreator<K, V> implements
      HashEntryCreator<K, V>, Serializable {

    private static final long serialVersionUID = 3765680607280951726L;

    public final HashEntry<K, V> newEntry(final K key, final int hash,
        final HashEntry<K, V> next, final V value) {
      return new HashEntryImpl<K, V>(key, hash, next, value, null);
    }

    public final int keyHashCode(final Object key,
        final boolean compareValues) {
      return keyHash(key, compareValues);
    }
  }

// End GemStone addition

  /**
   * Creates a new, empty map with the specified initial capacity and load
   * factor and with the default concurrencyLevel (16).
   * 
   * @param initialCapacity
   *          The implementation performs internal sizing to accommodate this
   *          many elements.
   * @param loadFactor
   *          the load factor threshold, used to control resizing. Resizing may
   *          be performed when the average number of elements per bin exceeds
   *          this threshold.
   * @throws IllegalArgumentException
   *           if the initial capacity of elements is negative or the load
   *           factor is nonpositive
   * 
   * @since 1.6
   */
  public CustomEntryConcurrentHashMap(final int initialCapacity, final float loadFactor) {
    this(initialCapacity, loadFactor, DEFAULT_CONCURRENCY_LEVEL, false);
  }

  /**
   * Creates a new, empty map with the specified initial capacity, and with
   * default load factor (0.75) and concurrencyLevel (16).
   * 
   * @param initialCapacity
   *          the initial capacity. The implementation performs internal sizing
   *          to accommodate this many elements.
   * @throws IllegalArgumentException
   *           if the initial capacity of elements is negative.
   */
  public CustomEntryConcurrentHashMap(final int initialCapacity) {
    this(initialCapacity, DEFAULT_LOAD_FACTOR, DEFAULT_CONCURRENCY_LEVEL, false);
  }

  /**
   * Creates a new, empty map with a default initial capacity (16), load factor
   * (0.75) and concurrencyLevel (16).
   */
  public CustomEntryConcurrentHashMap() {
    this(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR,
        DEFAULT_CONCURRENCY_LEVEL, false);
  }

  /**
   * Creates a new map with the same mappings as the given map. The map is
   * created with a capacity of 1.5 times the number of mappings in the given
   * map or 16 (whichever is greater), and a default load factor (0.75) and
   * concurrencyLevel (16).
   * 
   * @param m
   *          the map
   */
  public CustomEntryConcurrentHashMap(final Map<? extends K, ? extends V> m) {
    this(Math.max((int)(m.size() / DEFAULT_LOAD_FACTOR) + 1,
        DEFAULT_INITIAL_CAPACITY), DEFAULT_LOAD_FACTOR,
        DEFAULT_CONCURRENCY_LEVEL, false);
    putAll(m);
  }

  /**
   * Returns <tt>true</tt> if this map contains no key-value mappings.
   * 
   * @return <tt>true</tt> if this map contains no key-value mappings
   */
  @Override
  public final boolean isEmpty() {
    final Segment<K, V>[] segments = this.segments;
    /*
     * We keep track of per-segment modCounts to avoid ABA
     * problems in which an element in one segment was added and
     * in another removed during traversal, in which case the
     * table was never actually empty at any point. Note the
     * similar use of modCounts in the size() and containsValue()
     * methods, which are the only other methods also susceptible
     * to ABA problems.
     */
    final int[] mc = new int[segments.length];
    int mcsum = 0;
    for (int i = 0; i < segments.length; ++i) {
      if (segments[i].count != 0) {
        return false;
      }
      else {
        mcsum += mc[i] = segments[i].modCount;
      }
    }
    // If mcsum happens to be zero, then we know we got a snapshot
    // before any modifications at all were made. This is
    // probably common enough to bother tracking.
    if (mcsum != 0) {
      for (int i = 0; i < segments.length; ++i) {
        if (segments[i].count != 0 || mc[i] != segments[i].modCount) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Returns the number of key-value mappings in this map. If the map contains
   * more than <tt>Integer.MAX_VALUE</tt> elements, returns
   * <tt>Integer.MAX_VALUE</tt>.
   * 
   * @return the number of key-value mappings in this map
   */
  @Override
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="UL_UNRELEASED_LOCK", justification="The lock() calls are followed by unlock() calls without finally-block. Leaving this as is because it's lifted from JDK code and we want to minimize changes.") 
  public final int size() {
    final Segment<K, V>[] segments = this.segments;
    long sum = 0;
    long check = 0;
    final int[] mc = new int[segments.length];
    // Try a few times to get accurate count. On failure due to
    // continuous async changes in table, resort to locking.
    for (int k = 0; k < RETRIES_BEFORE_LOCK; ++k) {
      check = 0;
      sum = 0;
      int mcsum = 0;
      for (int i = 0; i < segments.length; ++i) {
        sum += segments[i].count;
        mcsum += mc[i] = segments[i].modCount;
      }
      if (mcsum != 0) {
        for (int i = 0; i < segments.length; ++i) {
          check += segments[i].count;
          if (mc[i] != segments[i].modCount) {
            check = -1; // force retry
            break;
          }
        }
      }
      if (check == sum) {
        break;
      }
    }
    if (check != sum) { // Resort to locking all segments
      sum = 0;
      for (int i = 0; i < segments.length; ++i) {
        segments[i].readLock().lock();
      }
      for (int i = 0; i < segments.length; ++i) {
        sum += segments[i].count;
      }
      for (int i = 0; i < segments.length; ++i) {
        segments[i].readLock().unlock();
      }
    }
    if (sum > Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    }
    else {
      return (int)sum;
    }
  }

  /**
   * Returns the value to which the specified key is mapped, or {@code null} if
   * this map contains no mapping for the key.
   * 
   * <p>
   * More formally, if this map contains a mapping from a key {@code k} to a
   * value {@code v} such that {@code key.equals(k)}, then this method returns
   * {@code v}; otherwise it returns {@code null}. (There can be at most one
   * such mapping.)
   * 
   * @throws NullPointerException
   *           if the specified key is null
   */
  @Override
  public final V get(final Object key) {
    // throws NullPointerException if key null
    final int hash = this.entryCreator.keyHashCode(key, this.compareValues);
    return segmentFor(hash).get(key, hash);
  }

  /**
   * Tests if the specified object is a key in this table.
   * 
   * @param key
   *          possible key
   * @return <tt>true</tt> if and only if the specified object is a key in this
   *         table, as determined by the <tt>equals</tt> method; <tt>false</tt>
   *         otherwise.
   * @throws NullPointerException
   *           if the specified key is null
   */
  @Override
  public final boolean containsKey(final Object key) {
    // throws NullPointerException if key null
    final int hash = this.entryCreator.keyHashCode(key, this.compareValues);
    return segmentFor(hash).containsKey(key, hash);
  }

  /**
   * Returns <tt>true</tt> if this map maps one or more keys to the specified
   * value. Note: This method requires a full internal traversal of the hash
   * table, and so is much slower than method <tt>containsKey</tt>.
   * 
   * @param value
   *          value whose presence in this map is to be tested
   * @return <tt>true</tt> if this map maps one or more keys to the specified
   *         value
   * @throws NullPointerException
   *           if the specified value is null
   */
  @Override
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="UL_UNRELEASED_LOCK", justification="Leaving this as is because it's lifted from JDK code and we want to minimize changes.") 
  public final boolean containsValue(final Object value) {
    if (value == null) {
      throw new NullPointerException();
    }

    // See explanation of modCount use above

    final Segment<K, V>[] segments = this.segments;
    final int[] mc = new int[segments.length];

    // Try a few times without locking
    for (int k = 0; k < RETRIES_BEFORE_LOCK; ++k) {
      int mcsum = 0;
      for (int i = 0; i < segments.length; ++i) {
        mcsum += mc[i] = segments[i].modCount;
        if (segments[i].containsValue(value)) {
          return true;
        }
      }
      boolean cleanSweep = true;
      if (mcsum != 0) {
        for (int i = 0; i < segments.length; ++i) {
          if (mc[i] != segments[i].modCount) {
            cleanSweep = false;
            break;
          }
        }
      }
      if (cleanSweep) {
        return false;
      }
    }
    // Resort to locking all segments
    for (int i = 0; i < segments.length; ++i) {
      segments[i].readLock().lock();
    }
    boolean found = false;
    try {
      for (int i = 0; i < segments.length; ++i) {
        if (segments[i].containsValue(value)) {
          found = true;
          break;
        }
      }
    } finally {
      for (int i = 0; i < segments.length; ++i) {
        segments[i].readLock().unlock();
      }
    }
    return found;
  }

  /**
   * Legacy method testing if some key maps into the specified value in this
   * table. This method is identical in functionality to {@link #containsValue},
   * and exists solely to ensure full compatibility with class
   * {@link java.util.Hashtable}, which supported this method prior to
   * introduction of the Java Collections framework.
   * 
   * @param value
   *          a value to search for
   * @return <tt>true</tt> if and only if some key maps to the <tt>value</tt>
   *         argument in this table as determined by the <tt>equals</tt> method;
   *         <tt>false</tt> otherwise
   * @throws NullPointerException
   *           if the specified value is null
   */
  public final boolean contains(final Object value) {
    return containsValue(value);
  }

  /**
   * Maps the specified key to the specified value in this table. Neither the
   * key nor the value can be null.
   * 
   * <p>
   * The value can be retrieved by calling the <tt>get</tt> method with a key
   * that is equal to the original key.
   * 
   * @param key
   *          key with which the specified value is to be associated
   * @param value
   *          value to be associated with the specified key
   * @return the previous value associated with <tt>key</tt>, or <tt>null</tt>
   *         if there was no mapping for <tt>key</tt>
   * @throws NullPointerException
   *           if the specified key or value is null
   */
  @Override
  public final V put(final K key, final V value) {
    if (value == null) {
      throw new NullPointerException();
    }
    // throws NullPointerException if key null
    final int hash = this.entryCreator.keyHashCode(key, this.compareValues);
    return segmentFor(hash).put(key, hash, value, false);
  }

  /**
   * {@inheritDoc}
   * 
   * @return the previous value associated with the specified key, or
   *         <tt>null</tt> if there was no mapping for the key
   * @throws NullPointerException
   *           if the specified key or value is null
   */
  public final V putIfAbsent(final K key, final V value) {
    if (value == null) {
      throw new NullPointerException();
    }
    // throws NullPointerException if key null
    final int hash = this.entryCreator.keyHashCode(key, this.compareValues);
    return segmentFor(hash).put(key, hash, value, true);
  }

// GemStone addition

  /**
   * Create a given key, value mapping if the key does not exist in the map else
   * do nothing. The difference between this method and
   * {@link #putIfAbsent(Object, Object)} is that latter always acquires a write
   * lock on the segment which this acquires a write lock only if entry was not
   * found. In other words this method is more efficient for the case when
   * number of entries is small and same entries are being updated repeatedly.
   * 
   * @return true if the key was successfully put in the map or false if there
   *         was an existing mapping for the key in the map
   * 
   * @throws NullPointerException
   *           if the specified key is null
   */
  public final boolean create(final K key, final V value) {
    // throws NullPointerException if key null
    final int hash = this.entryCreator.keyHashCode(key, this.compareValues);
    final Segment<K, V> seg = segmentFor(hash);
    if (seg.containsKey(key, hash)) {
      return false;
    }
    return seg.put(key, hash, value, true) == null;
  }

  /**
   * Factory to create a value on demand for
   * {@link #create(Object, MapCallback, Object, Object, boolean)} rather than
   * requiring a pre-built object as in {@link #putIfAbsent(Object, Object)}
   * that may be ultimately thrown away.
   * <p>
   * Also allows invoking a method when removing from map by a call to
   * {@link #removeConditionally(Object, MapCallback, Object, Object)}.
   * 
   * @author swale
   * @since 7.0
   * 
   * @param <K>
   *          the type of key of the map
   * @param <V>
   *          the type of value of the map
   * @param <C>
   *          the type of context parameter passed to the creation/removal
   *          methods
   * @param <P>
   *          the type of extra parameter passed to the creation/removal methods
   */
  public static interface MapCallback<K, V, C, P> {

    /**
     * Create a new instance of the value object given the key and provided
     * parameters for construction.
     * 
     * @param key
     *          the key for which the value is being created
     * @param context
     *          any context in which this method has been invoked
     * @param createParams
     *          parameters, if any, required for construction of a new value
     *          object
     */
    public V newValue(K key, C context, P createParams);

    /**
     * Invoked when an existing value in map is read by the
     * {@link #create(Object, MapCallback, Object, Object, boolean)} method in
     * the segment when segment is locked for read, or by 
     * 
     * @param value
     *          the value read by create that will be returned
     */
    public void oldValueRead(V value);

    /**
     * Check if the existing value should be removed by the
     * {@link CustomEntryConcurrentHashMap#removeConditionally} method.
     * 
     * @param value
     *          the value to be removed from the map
     * @param context
     *          any context in which this method has been invoked
     * @param removeParams
     *          parameters, if any, to be passed for cleanup of the object
     */
    public boolean doRemoveValue(V value, C context, P removeParams);
  }

  /**
   * Simple adapter class providing empty default implementations for
   * {@link com.gemstone.gemfire.internal.util.concurrent.CustomEntryConcurrentHashMap.MapCallback}.
   */
  public static class MapCallbackAdapter<K, V, C, P> implements
      MapCallback<K, V, C, P> {

    /**
     * @see MapCallback#newValue
     */
    public V newValue(K key, C context, P createParams) {
      return null;
    }

    /**
     * @see MapCallback#oldValueRead
     */
    public void oldValueRead(V value) {
    }

    /**
     * @see MapCallback#doRemoveValue
     */
    public boolean doRemoveValue(V value, C context, P removeParams) {
      return true;
    }
  }

  /**
   * Like {@link #putIfAbsent(Object, Object)} but creates the value only if
   * none present rather than requiring a passed in pre-created object that may
   * ultimately be thrown away. Also takes read lock on the segment, if
   * required, to provide better guarantees w.r.t. remove/replace that checks
   * against old value when the value may be changed structurally by reading
   * (e.g. a list as value changed after a call to this method).
   * 
   * @param key
   *          key with which the specified value is to be associated
   * @param valueCreator
   *          factory object to create the value to be associated with the
   *          specified key, if required
   * @param context
   *          the context in which this method has been invoked and passed to
   *          <code>valueCreator</code> {@link MapCallback#newValue} method to
   *          create the new instance
   * @param createParams
   *          parameters to be passed to the <code>valueCreator</code>
   *          {@link MapCallback#newValue} method to create the new instance
   * @param lockForRead
   *          if passed as true, then the read from the map prior to creation is
   *          done under the segment read lock; this provides better guarantees
   *          with respect to other threads that may be manipulating the value
   *          object in place after reading from the map
   * 
   * @return the previous value associated with the specified key, or the new
   *         value obtained by invoking {@link MapCallback#newValue} if there
   *         was no mapping for the key; this is paired with the segment read
   *         lock
   * 
   * @throws NullPointerException
   *           if the specified key or value is null
   */
  public final <C, P> V create(final K key,
      final MapCallback<K, V, C, P> valueCreator, final C context,
      final P createParams, final boolean lockForRead) {
    // throws NullPointerException if key null
    final int hash = this.entryCreator.keyHashCode(key, this.compareValues);
    return segmentFor(hash).create(key, hash, valueCreator, context,
        createParams, lockForRead);
  }

  /**
   * Returns the value to which the specified key is mapped, or {@code null} if
   * this map contains no mapping for the key.
   * 
   * <p>
   * More formally, if this map contains a mapping from a key {@code k} to a
   * value {@code v} such that {@code key.equals(k)}, then this method returns
   * {@code v}; otherwise it returns {@code null}. (There can be at most one
   * such mapping.)
   * 
   * <p>
   * This variant locks the segment for reading and if the given
   * {@link MapCallback} is non-null then its
   * {@link MapCallback#oldValueRead(Object)} method is invoked in the lock.
   * 
   * @throws NullPointerException
   *           if the specified key is null
   */
  public final V get(final Object key,
      final MapCallback<K, V, ?, ?> readCallback) {
    // throws NullPointerException if key null
    final int hash = this.entryCreator.keyHashCode(key, this.compareValues);
    return segmentFor(hash).get(key, hash, readCallback);
  }

  /**
   * Removes the entry for a key only if the given condition (
   * {@link MapCallback#doRemoveValue} evaluates to true. This is equivalent to:
   * 
   * <pre>
   * if (map.containsKey(key)
   *     &amp;&amp; condition.doRemoveInstance(map.get(key), removeParams)) {
   *   map.remove(key);
   *   return true;
   * }
   * else {
   *   return false;
   * }
   * </pre>
   * 
   * except that the action is performed atomically.
   * 
   * @param key
   *          key with which the specified value is associated
   * @param condition
   *          {@link MapCallback#doRemoveValue} is invoked and checked inside
   *          the segment lock if removal should be done
   * @param context
   *          the context in which this method has been invoked and passed to
   *          <code>condition</code> {@link MapCallback#doRemoveValue} method to
   *          create the new instance
   * @param removeParams
   *          parameters to be passed to the <code>onSuccess</code> parameter
   * 
   * @return the previous value associated with <tt>key</tt>, or <tt>null</tt>
   *         if there was no mapping for <tt>key</tt>
   * 
   * @throws UnsupportedOperationException
   *           if the <tt>remove</tt> operation is not supported by this map
   * @throws ClassCastException
   *           if the key or value is of an inappropriate type for this map
   *           (optional)
   * @throws NullPointerException
   *           if the specified key or value is null, and this map does not
   *           permit null keys or values (optional)
   */
  public final <C, P> V removeConditionally(final Object key,
      final MapCallback<K, V, C, P> condition, final C context,
      final P removeParams) {
    // throws NullPointerException if key null
    final int hash = this.entryCreator.keyHashCode(key, this.compareValues);
    return segmentFor(hash).remove(key, hash, NO_OBJECT_TOKEN, condition,
        context, removeParams);
  }

// End GemStone addition

  /**
   * Copies all of the mappings from the specified map to this one. These
   * mappings replace any mappings that this map had for any of the keys
   * currently in the specified map.
   * 
   * @param m
   *          mappings to be stored in this map
   */
  @Override
  public final void putAll(final Map<? extends K, ? extends V> m) {
    for (final Map.Entry<? extends K, ? extends V> e : m.entrySet()) {
      put(e.getKey(), e.getValue());
    }
  }

  /**
   * Removes the key (and its corresponding value) from this map. This method
   * does nothing if the key is not in the map.
   * 
   * @param key
   *          the key that needs to be removed
   * @return the previous value associated with <tt>key</tt>, or <tt>null</tt>
   *         if there was no mapping for <tt>key</tt>
   * @throws NullPointerException
   *           if the specified key is null
   */
  @Override
  public final V remove(final Object key) {
    // throws NullPointerException if key null
    final int hash = this.entryCreator.keyHashCode(key, this.compareValues);
    return segmentFor(hash)
        .remove(key, hash, NO_OBJECT_TOKEN, null, null, null);
  }

  /**
   * {@inheritDoc}
   * 
   * @throws NullPointerException
   *           if the specified key is null
   */
  public final boolean remove(final Object key, final Object value) {
    if (value == null) {
      return false;
    }
    // throws NullPointerException if key null
    final int hash = this.entryCreator.keyHashCode(key, this.compareValues);
    return segmentFor(hash).remove(key, hash, value, null, null, null) != null;
  }

  /**
   * {@inheritDoc}
   * 
   * @throws NullPointerException
   *           if any of the arguments are null
   */
  public final boolean replace(final K key, final V oldValue, final V newValue) {
    if (oldValue == null || newValue == null) {
      throw new NullPointerException();
    }
    // throws NullPointerException if key null
    final int hash = this.entryCreator.keyHashCode(key, this.compareValues);
    return segmentFor(hash).replace(key, hash, oldValue, newValue);
  }

  /**
   * {@inheritDoc}
   * 
   * @return the previous value associated with the specified key, or
   *         <tt>null</tt> if there was no mapping for the key
   * @throws NullPointerException
   *           if the specified key or value is null
   */
  public final V replace(final K key, final V value) {
    if (value == null) {
      throw new NullPointerException();
    }
    // throws NullPointerException if key null
    final int hash = this.entryCreator.keyHashCode(key, this.compareValues);
    return segmentFor(hash).replace(key, hash, value);
  }

  /**
   * Removes all of the mappings from this map.
   */
  @Override
  public final void clear() {
    ArrayList<HashEntry<?,?>> entries = null;
    try {
      for (int i = 0; i < this.segments.length; ++i) {
        entries = this.segments[i].clear(entries);
      }
    } finally {
      if (entries != null) {
        final ArrayList<HashEntry<?,?>> clearedEntries = entries;
        final Runnable runnable = new Runnable() {
          public void run() {
            for (HashEntry<?,?> he: clearedEntries) {
              for (HashEntry<?, ?> p = he; p != null; p = p.getNextEntry()) {
                synchronized (p) {
                  ((OffHeapRegionEntry)p).release();
                }
              }
            }
          }
        };
        boolean submitted = false;
        InternalDistributedSystem ids = InternalDistributedSystem.getConnectedInstance();
        if (ids != null) {
          try {
            ids.getDistributionManager().getWaitingThreadPool().submit(runnable);
            submitted = true;
          } catch (RejectedExecutionException e) {
            // fall through with submitted false
          } catch (CancelException e) {
            // fall through with submitted false
          } catch (NullPointerException e) {
            // fall through with submitted false
          }
        }
        if (!submitted) {
          String name = this.getClass().getSimpleName()+"@"+this.hashCode()+" Clear Thread";
          Thread thread = new Thread(runnable, name);
          thread.setDaemon(true);
          thread.start();
        }
      }
    }
  }

  /**
   * Returns a {@link Set} view of the keys contained in this map. The set is
   * backed by the map, so changes to the map are reflected in the set, and
   * vice-versa. The set supports element removal, which removes the
   * corresponding mapping from this map, via the <tt>Iterator.remove</tt>,
   * <tt>Set.remove</tt>, <tt>removeAll</tt>, <tt>retainAll</tt>, and
   * <tt>clear</tt> operations. It does not support the <tt>add</tt> or
   * <tt>addAll</tt> operations.
   * 
   * <p>
   * The view's <tt>iterator</tt> is a "weakly consistent" iterator that will
   * never throw {@link java.util.ConcurrentModificationException}, and
   * guarantees to traverse elements as they existed upon construction of the
   * iterator, and may (but is not guaranteed to) reflect any modifications
   * subsequent to construction.
   */
  @Override
  public final Set<K> keySet() {
    final Set<K> ks = this.keySet;
    return (ks != null) ? ks : (this.keySet = new KeySet());
  }

  /**
   * Returns a {@link Collection} view of the values contained in this map. The
   * collection is backed by the map, so changes to the map are reflected in the
   * collection, and vice-versa. The collection supports element removal, which
   * removes the corresponding mapping from this map, via the
   * <tt>Iterator.remove</tt>, <tt>Collection.remove</tt>, <tt>removeAll</tt>,
   * <tt>retainAll</tt>, and <tt>clear</tt> operations. It does not support the
   * <tt>add</tt> or <tt>addAll</tt> operations.
   * 
   * <p>
   * The view's <tt>iterator</tt> is a "weakly consistent" iterator that will
   * never throw {@link java.util.ConcurrentModificationException}, and
   * guarantees to traverse elements as they existed upon construction of the
   * iterator, and may (but is not guaranteed to) reflect any modifications
   * subsequent to construction.
   */
  @Override
  public final Collection<V> values() {
    final Collection<V> vs = this.values;
    return (vs != null) ? vs : (this.values = new Values());
  }

  /**
   * Returns a {@link Set} view of the mappings contained in this map. The set
   * is backed by the map, so changes to the map are reflected in the set, and
   * vice-versa. The set supports element removal, which removes the
   * corresponding mapping from the map, via the <tt>Iterator.remove</tt>,
   * <tt>Set.remove</tt>, <tt>removeAll</tt>, <tt>retainAll</tt>, and
   * <tt>clear</tt> operations. It does not support the <tt>add</tt> or
   * <tt>addAll</tt> operations.
   * 
   * <p>
   * The view's <tt>iterator</tt> is a "weakly consistent" iterator that will
   * never throw {@link java.util.ConcurrentModificationException}, and
   * guarantees to traverse elements as they existed upon construction of the
   * iterator, and may (but is not guaranteed to) reflect any modifications
   * subsequent to construction.
   */
  @Override
  public final Set<Map.Entry<K, V>> entrySet() {
    final Set<Map.Entry<K, V>> es = this.entrySet;
    return (es != null) ? es : (this.entrySet = new EntrySet(false));
  }

// GemStone addition

  /**
   * Returns a {@link Set} view of the mappings contained in this map. The set
   * is backed by the map, so changes to the map are reflected in the set, and
   * vice-versa. The set supports element removal, which removes the
   * corresponding mapping from the map, via the <tt>Iterator.remove</tt>,
   * <tt>Set.remove</tt>, <tt>removeAll</tt>, <tt>retainAll</tt>, and
   * <tt>clear</tt> operations. It does not support the <tt>add</tt> or
   * <tt>addAll</tt> operations.
   * 
   * <p>
   * The view's <tt>iterator</tt> is a "weakly consistent" iterator that will
   * never throw {@link java.util.ConcurrentModificationException}, and
   * guarantees to traverse elements as they existed upon construction of the
   * iterator, and may (but is not guaranteed to) reflect any modifications
   * subsequent to construction.
   * 
   * <p>
   * This set provides entries that are reused during iteration so caller cannot
   * store the returned <code>Map.Entry</code> objects.
   */
  public final Set<Map.Entry<K, V>> entrySetWithReusableEntries() {
    final Set<Map.Entry<K, V>> es = this.reusableEntrySet;
    return (es != null) ? es : (this.reusableEntrySet = new EntrySet(true));
  }

// End GemStone addition

  /**
   * Returns an enumeration of the keys in this table.
   * 
   * @return an enumeration of the keys in this table
   * @see #keySet()
   */
  public final Enumeration<K> keys() {
    return new KeyIterator();
  }

  /**
   * Returns an enumeration of the values in this table.
   * 
   * @return an enumeration of the values in this table
   * @see #values()
   */
  public final Enumeration<V> elements() {
    return new ValueIterator();
  }

  /* ---------------- Iterator Support -------------- */

  abstract class HashIterator {

    int currentSegmentIndex;

    int nextTableIndex;

// GemStone changed HashEntry<K, V>[] currentTable to currentSegment
    HashEntry<K, V>[] currentTable;

    HashEntry<K, V> nextEntry;

    HashEntry<K, V> lastReturned;

    final ArrayList<HashEntry<K, V>> currentList;

    int currentListIndex;

    HashIterator() {
      this.currentSegmentIndex = CustomEntryConcurrentHashMap.this.segments.length;
      this.nextTableIndex = -1;
      this.currentList = new ArrayList<HashEntry<K, V>>(5);
      this.currentListIndex = 0;
      advance();
    }

    public final boolean hasMoreElements() {
      return hasNext();
    }

    final void advance() {
// GemStone changes BEGIN
      if (this.currentListIndex < this.currentList.size()) {
        this.nextEntry = this.currentList.get(this.currentListIndex++);
        return;
      }

      this.nextEntry = null;
      if (this.nextTableIndex >= 0) {
        final Segment<K, V> seg = CustomEntryConcurrentHashMap.this
            .segments[this.currentSegmentIndex];
        final ReentrantReadWriteLock.ReadLock listLock = seg.listUpdateLock
            .readLock();
        listLock.lock();
        try {
          do {
            if ((this.nextEntry = currentTable[this.nextTableIndex--]) != null) {
              copyEntriesToList();
              return;
            }
          } while (this.nextTableIndex >= 0);
        } finally {
          listLock.unlock();
        }
      }
      /* (original code)
      if (this.nextEntry != null
          && (this.nextEntry = this.nextEntry.getNextEntry()) != null) {
        return;
      }

      while (this.nextTableIndex >= 0) {
        if ((this.nextEntry = this.currentTable[this.nextTableIndex--])
            != null) {
          return;
        }
      }
      */
// GemStone changes END

      while (this.currentSegmentIndex > 0) {
        final Segment<K, V> seg = CustomEntryConcurrentHashMap.this
            .segments[--this.currentSegmentIndex];
        if (seg.count != 0) {
          this.currentTable = seg.table;
          final ReentrantReadWriteLock.ReadLock listLock = seg.listUpdateLock
              .readLock();
          listLock.lock();
          try {
            for (int j = currentTable.length - 1; j >= 0; --j) {
              if ((this.nextEntry = currentTable[j]) != null) {
                this.nextTableIndex = j - 1;
                copyEntriesToList();
                return;
              }
            }
          } finally {
            listLock.unlock();
          }
        }
      }
    }

// GemStone added the method below
    /**
     * Copy the tail of list of current matched entry ({@link #nextEntry}) to a
     * temporary list, so that the read lock can be released after the copy.
     * 
     * Read lock on {@link #currentSegmentIndex}'s listUpdateLock should already be
     * acquired.
     */
    private final void copyEntriesToList() {
      assert segments[currentSegmentIndex] != null: "unexpected null currentSegment";
      assert segments[currentSegmentIndex].listUpdateLock.getReadLockCount() > 0;

      this.currentList.clear();
      this.currentListIndex = 0;
      for (HashEntry<K, V> p = this.nextEntry.getNextEntry(); p != null; p = p
          .getNextEntry()) {
        this.currentList.add(p);
      }
    }

    public final boolean hasNext() {
      return this.nextEntry != null;
    }

    final HashEntry<K, V> nextEntry() {
      if (this.nextEntry == null) {
        throw new NoSuchElementException();
      }
      this.lastReturned = this.nextEntry;
      advance();
      return this.lastReturned;
    }

    public final void remove() {
      if (this.lastReturned == null) {
        throw new IllegalStateException();
      }
      CustomEntryConcurrentHashMap.this.remove(this.lastReturned.getKey());
      this.lastReturned = null;
    }
  }

  final class KeyIterator extends HashIterator implements Iterator<K>,
      Enumeration<K> {

    public K next() {
      return super.nextEntry().getKey();
    }

    public K nextElement() {
      return super.nextEntry().getKey();
    }
  }

  final class ValueIterator extends HashIterator implements Iterator<V>,
      Enumeration<V> {

    public V next() {
      return super.nextEntry().getMapValue();
    }

    public V nextElement() {
      return super.nextEntry().getMapValue();
    }
  }

  /**
   * An Entry maintaining a key and a value. The value may be changed using the
   * <tt>setValue</tt> method. This class facilitates the process of building
   * custom map implementations. For example, it may be convenient to return
   * arrays of <tt>SimpleEntry</tt> instances in method
   * <tt>Map.entrySet().toArray</tt>.
   */
  public class SimpleReusableEntry implements Map.Entry<K, V>, Serializable {

    private static final long serialVersionUID = 1591026397367910439L;

    protected K key; // GemStone change; made non-final to enable reuse

    private V value;

    /**
     * Creates an entry representing a mapping from the specified key to the
     * specified value.
     * 
     * @param key
     *          the key represented by this entry
     * @param value
     *          the value represented by this entry
     */
    public SimpleReusableEntry(final K key, final V value) {
      this.key = key;
      this.value = value;
    }

    /**
     * Creates an entry representing the same mapping as the specified entry.
     * 
     * @param entry
     *          the entry to copy
     */
    public SimpleReusableEntry(final Entry<? extends K, ? extends V> entry) {
      this.key = entry.getKey();
      this.value = entry.getValue();
    }

    /**
     * Returns the key corresponding to this entry.
     * 
     * @return the key corresponding to this entry
     */
    public final K getKey() {
      return this.key;
    }

    /**
     * Returns the value corresponding to this entry.
     * 
     * @return the value corresponding to this entry
     */
    public final V getValue() {
      return this.value;
    }

    /**
     * Replaces the value corresponding to this entry with the specified value.
     * 
     * @param value
     *          new value to be stored in this entry
     * @return the old value corresponding to the entry
     */
    public V setValue(final V value) {
      final V oldValue = this.value;
      this.value = value;
      return oldValue;
    }

    /**
     * Compares the specified object with this entry for equality. Returns
     * {@code true} if the given object is also a map entry and the two entries
     * represent the same mapping. More formally, two entries {@code e1} and
     * {@code e2} represent the same mapping if
     * 
     * <pre>
     * (e1.getKey() == null ? e2.getKey() == null : e1.getKey().equals(
     *     e2.getKey())) &amp;&amp; (e1.getValue() == null ?
     *         e2.getValue() == null : e1.getValue().equals(e2.getValue()))
     * </pre>
     * 
     * This ensures that the {@code equals} method works properly across
     * different implementations of the {@code Map.Entry} interface.
     * 
     * @param o
     *          object to be compared for equality with this map entry
     * @return {@code true} if the specified object is equal to this map entry
     * @see #hashCode
     */
    @Override
    public boolean equals(final Object o) {
      if (!(o instanceof Map.Entry<?, ?>)) {
        return false;
      }
      final Map.Entry<?, ?> e = (Map.Entry<?, ?>)o;
      if (CustomEntryConcurrentHashMap.this.compareValues) {
        return ArrayUtils.objectEquals(this.key, e.getKey())
            && ArrayUtils.objectEquals(this.value, e.getValue());
      }
      return this.key == e.getKey() && this.value == e.getValue();
    }

    /**
     * Returns the hash code value for this map entry. The hash code of a map
     * entry {@code e} is defined to be:
     * 
     * <pre>
     * (e.getKey() == null ? 0 : e.getKey().hashCode())
     *     &circ; (e.getValue() == null ? 0 : e.getValue().hashCode())
     * </pre>
     * 
     * This ensures that {@code e1.equals(e2)} implies that
     * {@code e1.hashCode()==e2.hashCode()} for any two Entries {@code e1} and
     * {@code e2}, as required by the general contract of
     * {@link Object#hashCode}.
     * 
     * @return the hash code value for this map entry
     * @see #equals
     */
    @Override
    public int hashCode() {
      if (CustomEntryConcurrentHashMap.this.compareValues) {
        return (this.key != null ? this.key.hashCode() : 0)
            ^ (this.value != null ? this.value.hashCode() : 0);
      }
      return System.identityHashCode(this.key)
          ^ System.identityHashCode(this.value);
    }

    /**
     * Returns a String representation of this map entry. This implementation
     * returns the string representation of this entry's key followed by the
     * equals character ("<tt>=</tt>") followed by the string representation of
     * this entry's value.
     * 
     * @return a String representation of this map entry
     */
    @Override
    public String toString() {
      return this.key + "=" + this.value;
    }
  }

  /**
   * Custom Entry class used by EntryIterator.next(), that relays setValue
   * changes to the underlying map.
   */
  final class WriteThroughEntry extends SimpleReusableEntry {

    private static final long serialVersionUID = -6364816773849437756L;

    /**
     * Creates an entry representing a mapping from the specified key to the
     * specified value.
     * 
     * @param key
     *          the key represented by this entry
     * @param value
     *          the value represented by this entry
     */
    WriteThroughEntry(final K key, final V value) {
      super(key, value);
    }

    /**
     * Set our entry's value and write through to the map. The value to return
     * is somewhat arbitrary here. Since a WriteThroughEntry does not
     * necessarily track asynchronous changes, the most recent "previous" value
     * could be different from what we return (or could even have been removed
     * in which case the put will re-establish). We do not and cannot guarantee
     * more.
     */
    @Override
    public V setValue(final V value) {
      if (value == null) {
        throw new NullPointerException();
      }
      final V v = super.setValue(value);
      CustomEntryConcurrentHashMap.this.put(getKey(), value);
      return v;
    }
  }

  final class EntryIterator extends HashIterator implements
      Iterator<Map.Entry<K, V>> {

// GemStone change
    // added possibility to reuse a single Map.Entry for entire iteration
    final WriteThroughEntry reusableEntry;

    EntryIterator(final WriteThroughEntry reusableEntry) {
      this.reusableEntry = reusableEntry;
    }

    public Map.Entry<K, V> next() {
      final HashEntry<K, V> e = super.nextEntry();
      if (this.reusableEntry != null) {
        this.reusableEntry.key = e.getKey();
        this.reusableEntry.setValue(e.getMapValue());
        return this.reusableEntry;
      }
      return new WriteThroughEntry(e.getKey(), e.getMapValue());
    }
// End GemStone change
  }

  final class KeySet extends AbstractSet<K> {

    @Override
    public Iterator<K> iterator() {
      return new KeyIterator();
    }

    @Override
    public int size() {
      return CustomEntryConcurrentHashMap.this.size();
    }

    @Override
    public boolean contains(final Object o) {
      return CustomEntryConcurrentHashMap.this.containsKey(o);
    }

    @Override
    public boolean remove(final Object o) {
      return CustomEntryConcurrentHashMap.this.remove(o) != null;
    }

    @Override
    public void clear() {
      CustomEntryConcurrentHashMap.this.clear();
    }
  }

  final class Values extends AbstractCollection<V> {

    @Override
    public Iterator<V> iterator() {
      return new ValueIterator();
    }

    @Override
    public int size() {
      return CustomEntryConcurrentHashMap.this.size();
    }

    @Override
    public boolean contains(final Object o) {
      return CustomEntryConcurrentHashMap.this.containsValue(o);
    }

    @Override
    public void clear() {
      CustomEntryConcurrentHashMap.this.clear();
    }
  }

  final class EntrySet extends AbstractSet<Map.Entry<K, V>> {

// GemStone change
    // added possibility to reuse a single Map.Entry for entire iteration
    final WriteThroughEntry reusableEntry;

    EntrySet(final boolean useReusableEntry) {
      if (useReusableEntry) {
        this.reusableEntry = new WriteThroughEntry(null, null);
      }
      else {
        this.reusableEntry = null;
      }
    }

    @Override
    public Iterator<Map.Entry<K, V>> iterator() {
      return new EntryIterator(this.reusableEntry);
    }

// End GemStone change

    @Override
    public boolean contains(final Object o) {
      if (!(o instanceof Map.Entry)) {
        return false;
      }
      final Map.Entry<?, ?> e = (Map.Entry<?, ?>)o;
      final V v = CustomEntryConcurrentHashMap.this.get(e.getKey());
      if (CustomEntryConcurrentHashMap.this.compareValues) {
        return v != null && v.equals(e.getValue());
      }
      return v == e.getValue();
    }

    @Override
    public boolean remove(final Object o) {
      if (!(o instanceof Map.Entry)) {
        return false;
      }
      final Map.Entry<?, ?> e = (Map.Entry<?, ?>)o;
      return CustomEntryConcurrentHashMap.this.remove(e.getKey(), e.getValue());
    }

    @Override
    public int size() {
      return CustomEntryConcurrentHashMap.this.size();
    }

    @Override
    public void clear() {
      CustomEntryConcurrentHashMap.this.clear();
    }
  }

  /* ---------------- Serialization Support -------------- */

  /**
   * Save the state of the <tt>ConcurrentHashMap</tt> instance to a stream
   * (i.e., serialize it).
   * 
   * @param s
   *          the stream
   * @serialData the key (Object) and value (Object) for each key-value mapping,
   *             followed by a null pair. The key-value mappings are emitted in
   *             no particular order.
   */
  private void writeObject(final java.io.ObjectOutputStream s)
      throws IOException {
    s.defaultWriteObject();

    for (int k = 0; k < this.segments.length; ++k) {
      final Segment<K, V> seg = this.segments[k];
      final ReentrantReadWriteLock.ReadLock readLock = seg.readLock();
      readLock.lock();
      try {
        final HashEntry<K, V>[] tab = seg.table;
        for (int i = 0; i < tab.length; ++i) {
          for (HashEntry<K, V> e = tab[i]; e != null; e = e.getNextEntry()) {
            s.writeObject(e.getKey());
            s.writeObject(e.getMapValue());
          }
        }
      } finally {
        readLock.unlock();
      }
    }
    s.writeObject(null);
    s.writeObject(null);
  }

  /**
   * Reconstitute the <tt>ConcurrentHashMap</tt> instance from a stream (i.e.,
   * deserialize it).
   * 
   * @param s
   *          the stream
   */
  @SuppressWarnings("unchecked")
  private void readObject(final java.io.ObjectInputStream s)
      throws IOException, ClassNotFoundException {
    s.defaultReadObject();

    // Initialize each segment to be minimally sized, and let grow.
    for (int i = 0; i < this.segments.length; ++i) {
      this.segments[i].setTable(new HashEntry[1]);
    }

    // Read the keys and values, and put the mappings in the table
    for (;;) {
      final K key = (K)s.readObject();
      final V value = (V)s.readObject();
      if (key == null) {
        break;
      }
      put(key, value);
    }
  }

  public long estimateMemoryOverhead(SingleObjectSizer sizer) {

    long totalOverhead = sizer.sizeof(this);
    for (int i = 0; i < this.segments.length; ++i) {
      final Segment<K, V> seg = this.segments[i];
      totalOverhead += sizer.sizeof(seg);
    }

    return totalOverhead;
  }
}
