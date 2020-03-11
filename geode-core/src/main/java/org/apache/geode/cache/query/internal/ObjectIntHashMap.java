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
package org.apache.geode.cache.query.internal;

import java.io.IOException;
import java.io.Serializable;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeMap;

/**
 * This class was derived from <tt>HashMap<tt> implementation of the
 * <tt>Map</tt> interface. This implementation provides all of the optional map operations, and
 * supports ONLY int primitive type values, stored in primitive field of int type instead of
 * promoting them to Integer and the <tt>null</tt> key. The default for a value is 0. (The
 * <tt>ObjectIntHashMap</tt> class is roughly equivalent to <tt>HashMap</tt>, except that it is
 * unsynchronized and permits nulls.) This class makes no guarantees as to the order of the map; in
 * particular, it does not guarantee that the order will remain constant over time.
 *
 * <p>
 * This implementation provides constant-time performance for the basic operations (<tt>get</tt> and
 * <tt>put</tt>), assuming the hash function disperses the elements properly among the buckets.
 * Iteration over collection views requires time proportional to the "capacity" of the
 * <tt>HashMap</tt> instance (the number of buckets) plus its size (the number of key-value
 * mappings). Thus, it's very important not to set the initial capacity too high (or the load factor
 * too low) if iteration performance is important.
 *
 * <p>
 * An instance of <tt>HashMap</tt> has two parameters that affect its performance: <i>initial
 * capacity</i> and <i>load factor</i>. The <i>capacity</i> is the number of buckets in the hash
 * table, and the initial capacity is simply the capacity at the time the hash table is created. The
 * <i>load factor</i> is a measure of how full the hash table is allowed to get before its capacity
 * is automatically increased. When the number of entries in the hash table exceeds the product of
 * the load factor and the current capacity, the hash table is <i>rehashed</i> (that is, internal
 * data structures are rebuilt) so that the hash table has approximately twice the number of
 * buckets.
 *
 * <p>
 * As a general rule, the default load factor (.75) offers a good tradeoff between time and space
 * costs. Higher values decrease the space overhead but increase the lookup cost (reflected in most
 * of the operations of the <tt>HashMap</tt> class, including <tt>get</tt> and <tt>put</tt>). The
 * expected number of entries in the map and its load factor should be taken into account when
 * setting its initial capacity, so as to minimize the number of rehash operations. If the initial
 * capacity is greater than the maximum number of entries divided by the load factor, no rehash
 * operations will ever occur.
 *
 * <p>
 * If many mappings are to be stored in a <tt>HashMap</tt> instance, creating it with a sufficiently
 * large capacity will allow the mappings to be stored more efficiently than letting it perform
 * automatic rehashing as needed to grow the table.
 *
 * <p>
 * <strong>Note that this implementation is not synchronized.</strong> If multiple threads access a
 * hash map concurrently, and at least one of the threads modifies the map structurally, it
 * <i>must</i> be synchronized externally. (A structural modification is any operation that adds or
 * deletes one or more mappings; merely changing the value associated with a key that an instance
 * already contains is not a structural modification.) This is typically accomplished by
 * synchronizing on some object that naturally encapsulates the map.
 *
 * If no such object exists, the map should be "wrapped" using the
 * {@link Collections#synchronizedMap Collections.synchronizedMap} method. This is best done at
 * creation time, to prevent accidental unsynchronized access to the map:
 *
 * <pre>
 *   Map m = Collections.synchronizedMap(new IntHashMap(...));
 * </pre>
 *
 * <p>
 * The iterators returned by all of this class's "collection view methods" are <i>fail-fast</i>: if
 * the map is structurally modified at any time after the iterator is created, in any way except
 * through the iterator's own <tt>remove</tt> method, the iterator will throw a
 * {@link ConcurrentModificationException}. Thus, in the face of concurrent modification, the
 * iterator fails quickly and cleanly, rather than risking arbitrary, non-deterministic behavior at
 * an undetermined time in the future.
 *
 * <p>
 * Note that the fail-fast behavior of an iterator cannot be guaranteed as it is, generally
 * speaking, impossible to make any hard guarantees in the presence of unsynchronized concurrent
 * modification. Fail-fast iterators throw <tt>ConcurrentModificationException</tt> on a best-effort
 * basis. Therefore, it would be wrong to write a program that depended on this exception for its
 * correctness: <i>the fail-fast behavior of iterators should be used only to detect bugs.</i>
 *
 * @author Doug Lea
 * @author Josh Bloch
 * @author Arthur van Hoff
 * @author Neal Gafter
 *
 * @version %I%, %G%
 * @see Object#hashCode()
 * @see Collection
 * @see Map
 * @see TreeMap
 * @see Hashtable
 * @since 1.2
 * @since GemFire 7.1
 */
public class ObjectIntHashMap implements Cloneable, Serializable {

  private static final long serialVersionUID = 7718697444988416372L;

  /**
   * The default initial capacity - MUST be a power of two.
   */
  static final int DEFAULT_INITIAL_CAPACITY = 16;

  /**
   * The maximum capacity, used if a higher value is implicitly specified by either of the
   * constructors with arguments. MUST be a power of two <= 1<<30.
   */
  static final int MAXIMUM_CAPACITY = 1 << 30;

  /**
   * The load factor used when none specified in constructor.
   */
  static final float DEFAULT_LOAD_FACTOR = 0.75f;

  /**
   * The table, resized as necessary. Length MUST Always be a power of two.
   */
  transient Entry[] table;

  /**
   * The number of key-value mappings contained in this map.
   */
  transient int size;

  /**
   * The next size value at which to resize (capacity * load factor).
   *
   * @serial
   */
  int threshold;

  /**
   * The load factor for the hash table.
   *
   * @serial
   */
  final float loadFactor;

  /**
   * The number of times this IntHashMap has been structurally modified Structural modifications are
   * those that change the number of mappings in the IntHashMap or otherwise modify its internal
   * structure (e.g., rehash). This field is used to make iterators on Collection-views of the
   * IntHashMap fail-fast. (See ConcurrentModificationException).
   */
  transient volatile int modCount;

  /**
   * Hashing strategy for key objects.
   *
   */
  final HashingStrategy hashingStrategy; // GemFire addition

  /**
   * Constructs an empty <tt>HashMap</tt> with the specified initial capacity and load factor.
   *
   * @param initialCapacity the initial capacity
   * @param loadFactor the load factor
   * @throws IllegalArgumentException if the initial capacity is negative or the load factor is
   *         nonpositive
   */
  public ObjectIntHashMap(int initialCapacity, float loadFactor, HashingStrategy hs) {
    if (initialCapacity < 0)
      throw new IllegalArgumentException("Illegal initial capacity: " + initialCapacity);
    if (initialCapacity > MAXIMUM_CAPACITY)
      initialCapacity = MAXIMUM_CAPACITY;
    if (loadFactor <= 0 || Float.isNaN(loadFactor))
      throw new IllegalArgumentException("Illegal load factor: " + loadFactor);

    // Find a power of 2 >= initialCapacity
    int capacity = 1;
    while (capacity < initialCapacity)
      capacity <<= 1;

    this.loadFactor = loadFactor;
    threshold = (int) (capacity * loadFactor);
    table = new Entry[capacity];
    hashingStrategy = (hs == null) ? new IntHashMapStrategy() : hs;
    init();
  }

  public ObjectIntHashMap(int initialCapacity, float loadFactor) {
    this(initialCapacity, loadFactor, null);
  }

  /**
   * Constructs an empty <tt>HashMap</tt> with the specified initial capacity and the default load
   * factor (0.75).
   *
   * @param initialCapacity the initial capacity.
   * @throws IllegalArgumentException if the initial capacity is negative.
   */
  public ObjectIntHashMap(int initialCapacity) {
    this(initialCapacity, DEFAULT_LOAD_FACTOR, null);
  }

  public ObjectIntHashMap(int initialCapacity, HashingStrategy hs) {
    this(initialCapacity, DEFAULT_LOAD_FACTOR, hs);
  }

  public ObjectIntHashMap() {
    this(null);
  }

  /**
   * Constructs an empty <tt>HashMap</tt> with the default initial capacity (16) and the default
   * load factor (0.75).
   */
  public ObjectIntHashMap(HashingStrategy hs) {
    this.loadFactor = DEFAULT_LOAD_FACTOR;
    threshold = (int) (DEFAULT_INITIAL_CAPACITY * DEFAULT_LOAD_FACTOR);
    table = new Entry[DEFAULT_INITIAL_CAPACITY];
    this.hashingStrategy = (hs == null) ? new IntHashMapStrategy() : hs;
    init();
  }

  // internal utilities

  /**
   * Initialization hook for subclasses. This method is called in all constructors and
   * pseudo-constructors (clone, readObject) after IntHashMap has been initialized but before any
   * entries have been inserted. (In the absence of this method, readObject would require explicit
   * knowledge of subclasses.)
   */
  void init() {}

  /**
   * Applies a supplemental hash function to a given hashCode, which defends against poor quality
   * hash functions. This is critical because IntHashMap uses power-of-two length hash tables, that
   * otherwise encounter collisions for hashCodes that do not differ in lower bits. Note: Null keys
   * always map to hash 0, thus index 0.
   */
  static int hash(int h) {
    // This function ensures that hashCodes that differ only by
    // constant multiples at each bit position have a bounded
    // number of collisions (approximately 8 at default load factor).
    h ^= (h >>> 20) ^ (h >>> 12);
    return h ^ (h >>> 7) ^ (h >>> 4);
  }

  /**
   * Returns index for hash code h.
   */
  static int indexFor(int h, int length) {
    return h & (length - 1);
  }

  /**
   * Returns the number of key-value mappings in this map.
   *
   * @return the number of key-value mappings in this map
   */
  public int size() {
    return size;
  }

  /**
   * Returns <tt>true</tt> if this map contains no key-value mappings.
   *
   * @return <tt>true</tt> if this map contains no key-value mappings
   */
  public boolean isEmpty() {
    return size == 0;
  }

  /**
   * Returns the value to which the specified key is mapped, or {@code null} if this map contains no
   * mapping for the key.
   *
   * <p>
   * More formally, if this map contains a mapping from a key {@code k} to a value {@code v} such
   * that {@code (key==null ? k==null :
   * key.equals(k))}, then this method returns {@code v}; otherwise it returns {@code null}. (There
   * can be at most one such mapping.)
   *
   * <p>
   * A return value of {@code null} does not <i>necessarily</i> indicate that the map contains no
   * mapping for the key; it's also possible that the map explicitly maps the key to {@code null}.
   * The {@link #containsKey containsKey} operation may be used to distinguish these two cases.
   *
   * @see #put(Object, int)
   */
  public int get(Object key) {
    if (key == null)
      return getForNullKey();
    int hash = hash(hashingStrategy.hashCode(key));
    for (Entry e = table[indexFor(hash, table.length)]; e != null; e = e.next) {
      Object k;
      if (e.hash == hash && ((k = e.key) == key || hashingStrategy.equals(k, key)))
        return e.value;
    }
    return 0;
  }

  /**
   * Offloaded version of get() to look up null keys. Null keys map to index 0. This null case is
   * split out into separate methods for the sake of performance in the two most commonly used
   * operations (get and put), but incorporated with conditionals in others.
   */
  private int getForNullKey() {
    for (Entry e = table[0]; e != null; e = e.next) {
      if (e.key == null)
        return e.value;
    }
    return 0;
  }

  /**
   * Returns <tt>true</tt> if this map contains a mapping for the specified key.
   *
   * @param key The key whose presence in this map is to be tested
   * @return <tt>true</tt> if this map contains a mapping for the specified key.
   */
  public boolean containsKey(Object key) {
    return getEntry(key) != null;
  }

  /**
   * Returns the entry associated with the specified key in the IntHashMap. Returns null if the
   * IntHashMap contains no mapping for the key.
   */
  Entry getEntry(Object key) {
    int hash = (key == null) ? 0 : hash(hashingStrategy.hashCode(key));
    for (Entry e = table[indexFor(hash, table.length)]; e != null; e = e.next) {
      Object k;
      if (e.hash == hash && ((k = e.key) == key || (key != null && hashingStrategy.equals(k, key))))
        return e;
    }
    return null;
  }


  /**
   * Associates the specified value with the specified key in this map. If the map previously
   * contained a mapping for the key, the old value is replaced.
   *
   * @param key key with which the specified value is to be associated
   * @param value value to be associated with the specified key
   * @return the previous value associated with <tt>key</tt>, or <tt>null</tt> if there was no
   *         mapping for <tt>key</tt>. (A <tt>null</tt> return can also indicate that the map
   *         previously associated <tt>null</tt> with <tt>key</tt>.)
   */
  public int put(Object key, int value) {
    if (key == null)
      return putForNullKey(value);
    int hash = hash(hashingStrategy.hashCode(key));
    int i = indexFor(hash, table.length);
    for (Entry e = table[i]; e != null; e = e.next) {
      Object k;
      if (e.hash == hash && ((k = e.key) == key || hashingStrategy.equals(k, key))) {
        int oldValue = e.value;
        e.value = value;
        e.recordAccess(this);
        return oldValue;
      }
    }

    modCount++;
    addEntry(hash, key, value, i);
    return 0;
  }

  /**
   * Offloaded version of put for null keys
   */
  private int putForNullKey(int value) {
    for (Entry e = table[0]; e != null; e = e.next) {
      if (e.key == null) {
        int oldValue = e.value;
        e.value = value;
        e.recordAccess(this);
        return oldValue;
      }
    }
    modCount++;
    addEntry(0, null, value, 0);
    return 0;
  }

  /**
   * This method is used instead of put by constructors and pseudoconstructors (clone, readObject).
   * It does not resize the table, check for comodification, etc. It calls createEntry rather than
   * addEntry.
   */
  private void putForCreate(Object key, int value) {
    int hash = (key == null) ? 0 : hash(hashingStrategy.hashCode(key));
    int i = indexFor(hash, table.length);

    /*
     * Look for preexisting entry for key. This will never happen for clone or deserialize. It will
     * only happen for construction if the input Map is a sorted map whose ordering is inconsistent
     * w/ equals.
     */
    for (Entry e = table[i]; e != null; e = e.next) {
      Object k;
      if (e.hash == hash
          && ((k = e.key) == key || (key != null && hashingStrategy.equals(k, key)))) {
        e.value = value;
        return;
      }
    }

    createEntry(hash, key, value, i);
  }

  private void putAllForCreate(ObjectIntHashMap m) {
    for (Iterator i = m.entrySet().iterator(); i.hasNext();) {
      Entry e = (Entry) i.next();
      putForCreate(e.getKey(), e.getValue());
    }
  }

  /**
   * Rehashes the contents of this map into a new array with a larger capacity. This method is
   * called automatically when the number of keys in this map reaches its threshold.
   *
   * If current capacity is MAXIMUM_CAPACITY, this method does not resize the map, but sets
   * threshold to Integer.MAX_VALUE. This has the effect of preventing future calls.
   *
   * @param newCapacity the new capacity, MUST be a power of two; must be greater than current
   *        capacity unless current capacity is MAXIMUM_CAPACITY (in which case value is
   *        irrelevant).
   */
  void resize(int newCapacity) {
    Entry[] oldTable = table;
    int oldCapacity = oldTable.length;
    if (oldCapacity == MAXIMUM_CAPACITY) {
      threshold = Integer.MAX_VALUE;
      return;
    }

    Entry[] newTable = new Entry[newCapacity];
    transfer(newTable);
    table = newTable;
    threshold = (int) (newCapacity * loadFactor);
  }

  /**
   * Transfers all entries from current table to newTable.
   */
  void transfer(Entry[] newTable) {
    Entry[] src = table;
    int newCapacity = newTable.length;
    for (int j = 0; j < src.length; j++) {
      Entry e = src[j];
      if (e != null) {
        src[j] = null;
        do {
          Entry next = e.next;
          int i = indexFor(e.hash, newCapacity);
          e.next = newTable[i];
          newTable[i] = e;
          e = next;
        } while (e != null);
      }
    }
  }

  /**
   * Copies all of the mappings from the specified map to this map. These mappings will replace any
   * mappings that this map had for any of the keys currently in the specified map.
   *
   * @param m mappings to be stored in this map
   * @throws NullPointerException if the specified map is null
   */
  public void putAll(ObjectIntHashMap m) {
    int numKeysToBeAdded = m.size();
    if (numKeysToBeAdded == 0)
      return;

    /*
     * Expand the map if the map if the number of mappings to be added is greater than or equal to
     * threshold. This is conservative; the obvious condition is (m.size() + size) >= threshold, but
     * this condition could result in a map with twice the appropriate capacity, if the keys to be
     * added overlap with the keys already in this map. By using the conservative calculation, we
     * subject ourself to at most one extra resize.
     */
    if (numKeysToBeAdded > threshold) {
      int targetCapacity = (int) (numKeysToBeAdded / loadFactor + 1);
      if (targetCapacity > MAXIMUM_CAPACITY)
        targetCapacity = MAXIMUM_CAPACITY;
      int newCapacity = table.length;
      while (newCapacity < targetCapacity)
        newCapacity <<= 1;
      if (newCapacity > table.length)
        resize(newCapacity);
    }

    for (Iterator i = m.entrySet().iterator(); i.hasNext();) {
      Entry e = (Entry) i.next();
      put(e.getKey(), e.getValue());
    }
  }

  /**
   * Removes the mapping for the specified key from this map if present.
   *
   * @param key key whose mapping is to be removed from the map
   * @return the previous value associated with <tt>key</tt>, or <tt>null</tt> if there was no
   *         mapping for <tt>key</tt>. (A <tt>null</tt> return can also indicate that the map
   *         previously associated <tt>null</tt> with <tt>key</tt>.)
   */
  public int remove(Object key) {
    Entry e = removeEntryForKey(key);
    return (e == null ? 0 : e.value);
  }

  /**
   * Removes and returns the entry associated with the specified key in the IntHashMap. Returns null
   * if the IntHashMap contains no mapping for this key.
   */
  Entry removeEntryForKey(Object key) {
    int hash = (key == null) ? 0 : hash(hashingStrategy.hashCode(key));
    int i = indexFor(hash, table.length);
    Entry prev = table[i];
    Entry e = prev;

    while (e != null) {
      Entry next = e.next;
      Object k;
      if (e.hash == hash
          && ((k = e.key) == key || (key != null && hashingStrategy.equals(k, key)))) {
        modCount++;
        size--;
        if (prev == e)
          table[i] = next;
        else
          prev.next = next;
        e.recordRemoval(this);
        return e;
      }
      prev = e;
      e = next;
    }

    return e;
  }

  /**
   * Special version of remove for EntrySet.
   */
  Entry removeMapping(Object o) {
    if (!(o instanceof Entry))
      return null;

    Entry entry = (Entry) o;
    Object key = entry.getKey();
    int hash = (key == null) ? 0 : hash(hashingStrategy.hashCode(key));
    int i = indexFor(hash, table.length);
    Entry prev = table[i];
    Entry e = prev;

    while (e != null) {
      Entry next = e.next;
      if (e.hash == hash && e.equals(entry)) {
        modCount++;
        size--;
        if (prev == e)
          table[i] = next;
        else
          prev.next = next;
        e.recordRemoval(this);
        return e;
      }
      prev = e;
      e = next;
    }

    return e;
  }

  /**
   * Removes all of the mappings from this map. The map will be empty after this call returns.
   */
  public void clear() {
    modCount++;
    Entry[] tab = table;
    for (int i = 0; i < tab.length; i++)
      tab[i] = null;
    size = 0;
  }

  /**
   * Returns <tt>true</tt> if this map maps one or more keys to the specified value.
   *
   * @param value value whose presence in this map is to be tested
   * @return <tt>true</tt> if this map maps one or more keys to the specified value
   */
  public boolean containsValue(int value) {

    Entry[] tab = table;
    for (int i = 0; i < tab.length; i++)
      for (Entry e = tab[i]; e != null; e = e.next)
        if (value == e.value)
          return true;
    return false;
  }

  /**
   * Returns a shallow copy of this <tt>HashMap</tt> instance: the keys and values themselves are
   * not cloned.
   *
   * @return a shallow copy of this map
   */
  @Override
  public Object clone() {
    ObjectIntHashMap result = null;
    try {
      result = (ObjectIntHashMap) super.clone();
    } catch (CloneNotSupportedException e) {
      // assert false;
    }
    result.table = new Entry[table.length];
    result.entrySet = null;
    result.modCount = 0;
    result.size = 0;
    result.init();
    result.putAllForCreate(this);

    return result;
  }

  // Comparison and hashing.

  /**
   * Compares the specified object with this map for equality. Returns <tt>true</tt> if the given
   * object is also a map and the two maps represent the same mappings. More formally, two maps
   * <tt>m1</tt> and <tt>m2</tt> represent the same mappings if
   * <tt>m1.entrySet().equals(m2.entrySet())</tt>. This ensures that the <tt>equals</tt> method
   * works properly across different implementations of the <tt>Map</tt> interface.
   *
   * <p>
   * This implementation first checks if the specified object is this map; if so it returns
   * <tt>true</tt>. Then, it checks if the specified object is a map whose size is identical to the
   * size of this map; if not, it returns <tt>false</tt>. If so, it iterates over this map's
   * <tt>entrySet</tt> collection, and checks that the specified map contains each mapping that this
   * map contains. If the specified map fails to contain such a mapping, <tt>false</tt> is returned.
   * If the iteration completes, <tt>true</tt> is returned.
   *
   * @param o object to be compared for equality with this map
   * @return <tt>true</tt> if the specified object is equal to this map
   */
  public boolean equals(Object o) {
    if (o == this)
      return true;

    if (!(o instanceof ObjectIntHashMap))
      return false;
    ObjectIntHashMap m = (ObjectIntHashMap) o;
    if (m.size() != size())
      return false;

    try {
      Iterator<Entry> i = entrySet().iterator();
      while (i.hasNext()) {
        Entry e = i.next();
        Object key = e.getKey();
        int value = e.getValue();
        if (!(m.containsKey(key))) {
          return false;
        } else if (!(value == m.get(key))) {
          return false;
        }
      }
    } catch (ClassCastException unused) {
      return false;
    } catch (NullPointerException unused) {
      return false;
    }

    return true;
  }

  /**
   * Returns the hash code value for this map. The hash code of a map is defined to be the sum of
   * the hash codes of each entry in the map's <tt>entrySet()</tt> view. This ensures that
   * <tt>m1.equals(m2)</tt> implies that <tt>m1.hashCode()==m2.hashCode()</tt> for any two maps
   * <tt>m1</tt> and <tt>m2</tt>, as required by the general contract of {@link Object#hashCode}.
   *
   * <p>
   * This implementation iterates over <tt>entrySet()</tt>, calling
   * {@link java.util.Map.Entry#hashCode() hashCode()} on each element (entry) in the set, and
   * adding up the results.
   *
   * @return the hash code value for this map
   * @see java.util.Map.Entry#hashCode()
   * @see Object#equals(Object)
   * @see Set#equals(Object)
   */
  public int hashCode() {
    int h = 0;
    Iterator<Entry> i = entrySet().iterator();
    while (i.hasNext())
      h += i.next().hashCode();
    return h;
  }

  /**
   * Returns a string representation of this map. The string representation consists of a list of
   * key-value mappings in the order returned by the map's <tt>entrySet</tt> view's iterator,
   * enclosed in braces (<tt>"{}"</tt>). Adjacent mappings are separated by the characters
   * <tt>", "</tt> (comma and space). Each key-value mapping is rendered as the key followed by an
   * equals sign (<tt>"="</tt>) followed by the associated value. Keys and values are converted to
   * strings as by {@link String#valueOf(Object)}.
   *
   * @return a string representation of this map
   */
  public String toString() {
    Iterator<Entry> i = entrySet().iterator();
    if (!i.hasNext())
      return "{}";

    StringBuilder sb = new StringBuilder();
    sb.append('{');
    for (;;) {
      Entry e = i.next();
      Object key = e.getKey();
      int value = e.getValue();
      sb.append(key == this ? "(this Map)" : key);
      sb.append('=');
      sb.append(value);
      if (!i.hasNext())
        return sb.append('}').toString();
      sb.append(", ");
    }
  }

  class Entry {
    final Object key;
    int value; // GemFire Addition.
    Entry next;
    final int hash;

    /**
     * Creates new entry.
     */
    Entry(int h, Object k, int v, Entry n) {
      value = v;
      next = n;
      key = k;
      hash = h;
    }

    public Object getKey() {
      return key;
    }

    public int getValue() {
      return value;
    }

    public int setValue(int newValue) {
      int oldValue = value;
      value = newValue;
      return oldValue;
    }

    public boolean equals(Object o) {
      if (!(o instanceof Entry))
        return false;
      Entry e = (Entry) o;
      Object k1 = getKey();
      Object k2 = e.getKey();
      if (k1 == k2 || (k1 != null && hashingStrategy.equals(k1, k2))) {
        int v1 = getValue();
        int v2 = e.getValue();
        if (v1 == v2)
          return true;
      }
      return false;
    }

    public int hashCode() {
      return this.hash ^ value;
    }

    public String toString() {
      return getKey() + "=" + getValue();
    }

    /**
     * This method is invoked whenever the value in an entry is overwritten by an invocation of
     * put(k,v) for a key k that's already in the IntHashMap.
     */
    void recordAccess(ObjectIntHashMap m) {}

    /**
     * This method is invoked whenever the entry is removed from the table.
     */
    void recordRemoval(ObjectIntHashMap m) {}
  }

  /**
   * Adds a new entry with the specified key, value and hash code to the specified bucket. It is the
   * responsibility of this method to resize the table if appropriate.
   *
   * Subclass overrides this to alter the behavior of put method.
   */
  void addEntry(int hash, Object key, int value, int bucketIndex) {
    Entry e = table[bucketIndex];
    table[bucketIndex] = new Entry(hash, key, value, e);
    if (size++ >= threshold)
      resize(2 * table.length);
  }

  /**
   * Like addEntry except that this version is used when creating entries as part of Map
   * construction or "pseudo-construction" (cloning, deserialization). This version needn't worry
   * about resizing the table.
   *
   * Subclass overrides this to alter the behavior of IntHashMap(Map), clone, and readObject.
   */
  void createEntry(int hash, Object key, int value, int bucketIndex) {
    Entry e = table[bucketIndex];
    table[bucketIndex] = new Entry(hash, key, value, e);
    size++;
  }

  private abstract class HashIterator<E> implements Iterator<E> {
    Entry next; // next entry to return
    int expectedModCount; // For fast-fail
    int index; // current slot
    Entry current; // current entry

    HashIterator() {
      expectedModCount = modCount;
      if (size > 0) { // advance to first entry
        Entry[] t = table;
        while (index < t.length && (next = t[index++]) == null);
      }
    }

    @Override
    public boolean hasNext() {
      return next != null;
    }

    Entry nextEntry() {
      if (modCount != expectedModCount)
        throw new ConcurrentModificationException();
      Entry e = next;
      if (e == null)
        throw new NoSuchElementException();

      if ((next = e.next) == null) {
        Entry[] t = table;
        while (index < t.length && (next = t[index++]) == null);
      }
      current = e;
      return e;
    }

    @Override
    public void remove() {
      if (current == null)
        throw new IllegalStateException();
      if (modCount != expectedModCount)
        throw new ConcurrentModificationException();
      Object k = current.key;
      current = null;
      ObjectIntHashMap.this.removeEntryForKey(k);
      expectedModCount = modCount;
    }

  }

  private class KeyIterator extends HashIterator<Object> {
    @Override
    public Object next() {
      return nextEntry().getKey();
    }
  }

  private class EntryIterator extends HashIterator<Entry> {
    @Override
    public Entry next() {
      return nextEntry();
    }
  }

  // Subclass overrides these to alter behavior of views' iterator() method
  Iterator<Object> newKeyIterator() {
    return new KeyIterator();
  }

  Iterator<Entry> newEntryIterator() {
    return new EntryIterator();
  }


  // Views

  private transient Set<Entry> entrySet = null;
  private transient Set<Object> keySet = null;

  /**
   * Returns a {@link Set} view of the keys contained in this map. The set is backed by the map, so
   * changes to the map are reflected in the set, and vice-versa. If the map is modified while an
   * iteration over the set is in progress (except through the iterator's own <tt>remove</tt>
   * operation), the results of the iteration are undefined. The set supports element removal, which
   * removes the corresponding mapping from the map, via the <tt>Iterator.remove</tt>,
   * <tt>Set.remove</tt>, <tt>removeAll</tt>, <tt>retainAll</tt>, and <tt>clear</tt> operations. It
   * does not support the <tt>add</tt> or <tt>addAll</tt> operations.
   */
  public Set<Object> keySet() {
    Set<Object> ks = keySet;
    return (ks != null ? ks : (keySet = new KeySet()));
  }

  private class KeySet extends AbstractSet<Object> {
    @Override
    public Iterator<Object> iterator() {
      return newKeyIterator();
    }

    @Override
    public int size() {
      return size;
    }

    @Override
    public boolean contains(Object o) {
      return containsKey(o);
    }

    @Override
    public boolean remove(Object o) {
      return ObjectIntHashMap.this.removeEntryForKey(o) != null;
    }

    @Override
    public void clear() {
      ObjectIntHashMap.this.clear();
    }
  }

  /**
   * Returns a {@link Set} view of the mappings contained in this map. The set is backed by the map,
   * so changes to the map are reflected in the set, and vice-versa. If the map is modified while an
   * iteration over the set is in progress (except through the iterator's own <tt>remove</tt>
   * operation, or through the <tt>setValue</tt> operation on a map entry returned by the iterator)
   * the results of the iteration are undefined. The set supports element removal, which removes the
   * corresponding mapping from the map, via the <tt>Iterator.remove</tt>, <tt>Set.remove</tt>,
   * <tt>removeAll</tt>, <tt>retainAll</tt> and <tt>clear</tt> operations. It does not support the
   * <tt>add</tt> or <tt>addAll</tt> operations.
   *
   * @return a set view of the mappings contained in this map
   */
  public Set<Entry> entrySet() {
    return entrySet0();
  }

  private Set<Entry> entrySet0() {
    Set<Entry> es = entrySet;
    return es != null ? es : (entrySet = new EntrySet());
  }

  private class EntrySet extends AbstractSet<Entry> {
    @Override
    public Iterator<Entry> iterator() {
      return newEntryIterator();
    }

    @Override
    public boolean contains(Object o) {
      if (!(o instanceof Entry))
        return false;
      Entry e = (Entry) o;
      Entry candidate = getEntry(e.getKey());
      return candidate != null && candidate.equals(e);
    }

    @Override
    public boolean remove(Object o) {
      return removeMapping(o) != null;
    }

    @Override
    public int size() {
      return size;
    }

    @Override
    public void clear() {
      ObjectIntHashMap.this.clear();
    }
  }

  /**
   * Save the state of the <tt>HashMap</tt> instance to a stream (i.e., serialize it).
   *
   * @serialData The <i>capacity</i> of the IntHashMap (the length of the bucket array) is emitted
   *             (int), followed by the <i>size</i> (an int, the number of key-value mappings),
   *             followed by the key (Object) and value (Object) for each key-value mapping. The
   *             key-value mappings are emitted in no particular order.
   */
  private void writeObject(java.io.ObjectOutputStream s) throws IOException {
    Iterator<Entry> i = (size > 0) ? entrySet0().iterator() : null;

    // Write out the threshold, loadfactor, and any hidden stuff
    s.defaultWriteObject();

    // Write out number of buckets
    s.writeInt(table.length);

    // Write out size (number of Mappings)
    s.writeInt(size);

    // Write out keys and values (alternating)
    if (i != null) {
      while (i.hasNext()) {
        Entry e = i.next();
        s.writeObject(e.getKey());
        s.writeInt(e.getValue());
      }
    }
  }

  /**
   * Reconstitute the <tt>HashMap</tt> instance from a stream (i.e., deserialize it).
   */
  private void readObject(java.io.ObjectInputStream s) throws IOException, ClassNotFoundException {
    // Read in the threshold, loadfactor, and any hidden stuff
    s.defaultReadObject();

    // Read in number of buckets and allocate the bucket array;
    int numBuckets = s.readInt();
    table = new Entry[numBuckets];

    init(); // Give subclass a chance to do its thing.

    // Read in size (number of Mappings)
    int size = s.readInt();

    // Read the keys and values, and put the mappings in the IntHashMap
    for (int i = 0; i < size; i++) {
      Object key = (Object) s.readObject();
      int value = (int) s.readInt();
      putForCreate(key, value);
    }
  }

  // These methods are used when serializing HashSets
  int capacity() {
    return table.length;
  }

  float loadFactor() {
    return loadFactor;
  }

  private class IntHashMapStrategy implements HashingStrategy {

    @Override
    public int hashCode(Object o) {
      return o.hashCode();
    }

    @Override
    public boolean equals(Object o1, Object o2) {
      if (o1 == null && o2 == null)
        return true;
      if (o1 == null || o2 == null)
        return false;
      return o1.equals(o2);
    }
  }
}
