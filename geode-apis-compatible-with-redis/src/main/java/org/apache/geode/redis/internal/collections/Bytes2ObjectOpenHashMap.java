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
package org.apache.geode.redis.internal.collections;

import static it.unimi.dsi.fastutil.HashCommon.arraySize;
import static it.unimi.dsi.fastutil.HashCommon.maxFill;

import java.util.Arrays;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Consumer;
import java.util.function.Predicate;

import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.HashCommon;
import it.unimi.dsi.fastutil.Pair;
import it.unimi.dsi.fastutil.bytes.ByteArrays;
import it.unimi.dsi.fastutil.objects.AbstractObject2ObjectMap;
import it.unimi.dsi.fastutil.objects.AbstractObjectCollection;
import it.unimi.dsi.fastutil.objects.AbstractObjectSet;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectCollection;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import it.unimi.dsi.fastutil.objects.ObjectSet;
import it.unimi.dsi.fastutil.objects.ObjectSpliterator;
import it.unimi.dsi.fastutil.objects.ObjectSpliterators;

/**
 * This class was derived from Object2ObjectOpenCustomHashMap.
 * The differences are:
 * 1. the keys are always a byte[] so no need for "strategy" field
 * 2. the load factor is always the default so no need for the "f" field.
 * 3. no support for the "null" key so no need for the boolean field that signifies a null key
 * exists
 * 4. the fields that cache a singleton instance for entrySet, keySet, and values
 * no longer exist.
 */
public class Bytes2ObjectOpenHashMap<V> extends AbstractObject2ObjectMap<byte[], V>
    implements java.io.Serializable, Cloneable, Hash {
  private static final Strategy<byte[]> strategy = ByteArrays.HASH_STRATEGY;
  private static final long serialVersionUID = 0L;

  /** The array of keys. */
  protected transient byte[][] key;
  /** The array of values. */
  protected transient V[] value;
  /** The mask for wrapping a position counter. */
  protected transient int mask;
  /** The current table size. */
  protected transient int n;
  /** Threshold after which we rehash. It must be the table size times load factor. */
  protected transient int maxFill;
  /** We never resize below this threshold, which is the construction-time {#n}. */
  protected final transient int minN;
  /** Number of entries in the set */
  protected int size;

  /**
   * Creates a new hash map.
   *
   * <p>
   * The actual table size will be the least power of two greater than {@code expected}/{@code f}.
   *
   * @param expected the expected number of elements in the hash map.
   */
  @SuppressWarnings("unchecked")
  public Bytes2ObjectOpenHashMap(final int expected) {
    if (expected < 0) {
      throw new IllegalArgumentException("The expected number of elements must be non-negative");
    }
    minN = n = arraySize(expected, getLoadFactor());
    mask = n - 1;
    maxFill = maxFill(n, getLoadFactor());
    key = new byte[n + 1][];
    value = (V[]) new Object[n + 1];
  }

  /**
   * Creates a new hash map with initial expected {@link Hash#DEFAULT_INITIAL_SIZE} entries
   * and {@link Hash#DEFAULT_LOAD_FACTOR} as load factor.
   */
  public Bytes2ObjectOpenHashMap() {
    this(DEFAULT_INITIAL_SIZE);
  }

  /**
   * Creates a new hash map with {@link Hash#DEFAULT_LOAD_FACTOR} as load factor copying a given
   * one.
   *
   * @param m a {@link Map} to be copied into the new hash map.
   */
  public Bytes2ObjectOpenHashMap(final Map<byte[], ? extends V> m) {
    this(m.size());
    putAll(m);
  }

  /**
   * Returns the hashing strategy.
   *
   * @return the hashing strategy of this custom hash map.
   */
  public Strategy<byte[]> strategy() {
    return strategy;
  }

  private void ensureCapacity(final int capacity) {
    final int needed = arraySize(capacity, getLoadFactor());
    if (needed > n) {
      rehash(needed);
    }
  }

  private void tryCapacity(final long capacity) {
    final int needed = (int) Math.min(1 << 30,
        Math.max(2, HashCommon.nextPowerOfTwo((long) Math.ceil(capacity / getLoadFactor()))));
    if (needed > n) {
      rehash(needed);
    }
  }

  private V removeEntry(final int pos) {
    final V oldValue = value[pos];
    value[pos] = null;
    size--;
    shiftKeys(pos);
    if (n > minN && size < maxFill / 4 && n > DEFAULT_INITIAL_SIZE) {
      rehash(n / 2);
    }
    return oldValue;
  }

  @Override
  public void putAll(Map<? extends byte[], ? extends V> m) {
    if (getLoadFactor() <= .5) {
      ensureCapacity(m.size()); // The resulting map will be sized for m.size() elements
    } else {
      tryCapacity(size() + m.size()); // The resulting map will be tentatively sized for size() +
    }
    // m.size() elements
    super.putAll(m);
  }

  private int find(final byte[] k) {
    byte[] curr;
    final byte[][] key = this.key;
    int pos;
    // The starting point.
    if ((curr =
        key[pos = HashCommon.mix(strategy().hashCode(k)) & mask]) == null) {
      return -(pos + 1);
    }
    if (strategy().equals(k, curr)) {
      return pos;
    }
    // There's always an unused entry.
    while (true) {
      if ((curr = key[pos = pos + 1 & mask]) == null) {
        return -(pos + 1);
      }
      if (strategy().equals(k, curr)) {
        return pos;
      }
    }
  }

  private void insert(final int pos, final byte[] k, final V v) {
    key[pos] = k;
    value[pos] = v;
    if (size++ >= maxFill) {
      rehash(arraySize(size + 1, getLoadFactor()));
    }
  }

  @Override
  public V put(final byte[] k, final V v) {
    final int pos = find(k);
    if (pos < 0) {
      insert(-pos - 1, k, v);
      return defRetValue;
    }
    final V oldValue = value[pos];
    value[pos] = v;
    return oldValue;
  }

  /**
   * Shifts left entries with the specified hash code, starting at the specified position,
   * and empties the resulting free entry.
   *
   * @param pos a starting position.
   */
  protected final void shiftKeys(int pos) {
    // Shift entries with the same hash.
    int last, slot;
    byte[] curr;
    final byte[][] key = this.key;
    while (true) {
      pos = (last = pos) + 1 & mask;
      while (true) {
        if ((curr = key[pos]) == null) {
          key[last] = null;
          value[last] = null;
          return;
        }
        slot = HashCommon.mix(strategy().hashCode(curr)) & mask;
        if (last <= pos ? last >= slot || slot > pos : last >= slot && slot > pos) {
          break;
        }
        pos = pos + 1 & mask;
      }
      key[last] = curr;
      value[last] = value[pos];
    }
  }

  @Override
  public V remove(final Object k) {
    byte[] curr;
    final byte[][] key = this.key;
    int pos;
    // The starting point.
    if ((curr = key[pos =
        HashCommon.mix(strategy().hashCode((byte[]) k)) & mask]) == null) {
      return defRetValue;
    }
    if (strategy().equals((byte[]) k, curr)) {
      return removeEntry(pos);
    }
    while (true) {
      if ((curr = key[pos = pos + 1 & mask]) == null) {
        return defRetValue;
      }
      if (strategy().equals((byte[]) k, curr)) {
        return removeEntry(pos);
      }
    }
  }

  @Override
  public V get(final Object k) {
    byte[] curr;
    final byte[][] key = this.key;
    int pos;
    // The starting point.
    if ((curr = key[pos =
        HashCommon.mix(strategy().hashCode((byte[]) k)) & mask]) == null) {
      return defRetValue;
    }
    if (strategy().equals((byte[]) k, curr)) {
      return value[pos];
    }
    // There's always an unused entry.
    while (true) {
      if ((curr = key[pos = pos + 1 & mask]) == null) {
        return defRetValue;
      }
      if (strategy().equals((byte[]) k, curr)) {
        return value[pos];
      }
    }
  }

  @Override
  public boolean containsKey(final Object k) {
    byte[] curr;
    final byte[][] key = this.key;
    int pos;
    // The starting point.
    if ((curr = key[pos =
        HashCommon.mix(strategy().hashCode((byte[]) k)) & mask]) == null) {
      return false;
    }
    if (strategy().equals((byte[]) k, curr)) {
      return true;
    }
    // There's always an unused entry.
    while (true) {
      if ((curr = key[pos = pos + 1 & mask]) == null) {
        return false;
      }
      if (strategy().equals((byte[]) k, curr)) {
        return true;
      }
    }
  }

  @Override
  public boolean containsValue(final Object v) {
    final V[] value = this.value;
    final byte[][] key = this.key;
    for (int i = n; i-- != 0;) {
      if (!(key[i] == null) && java.util.Objects.equals(value[i], v)) {
        return true;
      }
    }
    return false;
  }

  /*
   * Removes all elements from this map.
   *
   * <p>To increase object reuse, this method does not change the table size.
   * If you want to reduce the table size, you must use {@link #trim()}.
   *
   */
  @Override
  public void clear() {
    if (size == 0) {
      return;
    }
    size = 0;
    Arrays.fill(key, null);
    Arrays.fill(value, null);
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public boolean isEmpty() {
    return size == 0;
  }

  /** The acceptable load factor. */
  protected float getLoadFactor() {
    return DEFAULT_LOAD_FACTOR;
  }

  /**
   * The entry class for a hash map does not record key and value, but
   * rather the position in the hash table of the corresponding entry. This
   * is necessary so that calls to {@link java.util.Map.Entry#setValue(Object)} are reflected in
   * the map
   */
  final class MapEntry
      implements Object2ObjectMap.Entry<byte[], V>, Map.Entry<byte[], V>, Pair<byte[], V> {
    // The table index this entry refers to, or -1 if this entry has been deleted.
    int index;

    MapEntry(final int index) {
      this.index = index;
    }

    MapEntry() {}

    @Override
    public byte[] getKey() {
      return key[index];
    }

    @Override
    public byte[] left() {
      return key[index];
    }

    @Override
    public V getValue() {
      return value[index];
    }

    @Override
    public V right() {
      return value[index];
    }

    @Override
    public V setValue(final V v) {
      final V oldValue = value[index];
      value[index] = v;
      return oldValue;
    }

    @Override
    public Pair<byte[], V> right(final V v) {
      value[index] = v;
      return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean equals(final Object o) {
      if (!(o instanceof Map.Entry)) {
        return false;
      }
      Map.Entry<byte[], V> e = (Map.Entry<byte[], V>) o;
      return strategy().equals(key[index], e.getKey())
          && java.util.Objects.equals(value[index], e.getValue());
    }

    @Override
    public int hashCode() {
      return strategy().hashCode(key[index])
          ^ (value[index] == null ? 0 : value[index].hashCode());
    }

    @Override
    public String toString() {
      return Arrays.toString(key[index]) + "=>" + value[index];
    }
  }
  /** An iterator over a hash map. */
  private abstract class MapIterator<ConsumerType> {
    /**
     * The index of the last entry returned, if positive or zero; initially, {@link #n}. If
     * negative, the last
     * entry returned was that of the key of index {@code - pos - 1} from the {@link #wrapped} list.
     */
    int pos = n;
    /**
     * The index of the last entry that has been returned (more precisely, the value of {@link #pos}
     * if {@link #pos} is positive,
     * or {@link Integer#MIN_VALUE} if {@link #pos} is negative). It is -1 if either
     * we did not return an entry yet, or the last returned entry has been removed.
     */
    int last = -1;
    /** A downward counter measuring how many entries must still be returned. */
    int c = size;
    /**
     * A lazily allocated list containing keys of entries that have wrapped around the table because
     * of removals.
     */
    ObjectArrayList<byte[]> wrapped;

    abstract void acceptOnIndex(final ConsumerType action, final int index);

    public boolean hasNext() {
      return c != 0;
    }

    public int nextEntry() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      c--;
      final byte[][] key = Bytes2ObjectOpenHashMap.this.key;
      while (true) {
        if (--pos < 0) {
          // We are just enumerating elements from the wrapped list.
          last = Integer.MIN_VALUE;
          final byte[] k = wrapped.get(-pos - 1);
          int p = HashCommon.mix(strategy().hashCode(k)) & mask;
          while (!strategy().equals(k, key[p])) {
            p = p + 1 & mask;
          }
          return p;
        }
        if (!(key[pos] == null)) {
          return last = pos;
        }
      }
    }

    public void forEachRemaining(final ConsumerType action) {
      final byte[][] key = Bytes2ObjectOpenHashMap.this.key;
      while (c != 0) {
        if (--pos < 0) {
          // We are just enumerating elements from the wrapped list.
          last = Integer.MIN_VALUE;
          final byte[] k = wrapped.get(-pos - 1);
          int p = HashCommon.mix(strategy().hashCode(k)) & mask;
          while (!strategy().equals(k, key[p])) {
            p = p + 1 & mask;
          }
          acceptOnIndex(action, p);
          c--;
        } else if (!(key[pos] == null)) {
          acceptOnIndex(action, last = pos);
          c--;
        }
      }
    }

    /**
     * Shifts left entries with the specified hash code, starting at the specified position,
     * and empties the resulting free entry.
     *
     * @param pos a starting position.
     */
    private void shiftKeys(int pos) {
      // Shift entries with the same hash.
      int last, slot;
      byte[] curr;
      final byte[][] key = Bytes2ObjectOpenHashMap.this.key;
      while (true) {
        pos = (last = pos) + 1 & mask;
        while (true) {
          if ((curr = key[pos]) == null) {
            key[last] = null;
            value[last] = null;
            return;
          }
          slot = HashCommon.mix(strategy().hashCode(curr)) & mask;
          if (last <= pos ? last >= slot || slot > pos : last >= slot && slot > pos) {
            break;
          }
          pos = pos + 1 & mask;
        }
        if (pos < last) { // Wrapped entry.
          if (wrapped == null) {
            wrapped = new ObjectArrayList<>(2);
          }
          wrapped.add(key[pos]);
        }
        key[last] = curr;
        value[last] = value[pos];
      }
    }

    public void remove() {
      if (last == -1) {
        throw new IllegalStateException();
      }
      if (last == n) {
        key[n] = null;
        value[n] = null;
      } else if (pos >= 0) {
        shiftKeys(last);
      } else {
        // We're removing wrapped entries.
        Bytes2ObjectOpenHashMap.this.remove(wrapped.set(-pos - 1, null));
        last = -1; // Note that we must not decrement size
        return;
      }
      size--;
      last = -1; // You can no longer remove this entry.
    }

    public int skip(final int n) {
      int i = n;
      while (i-- != 0 && hasNext()) {
        nextEntry();
      }
      return n - i - 1;
    }
  }
  private final class EntryIterator extends MapIterator<Consumer<? super Entry<byte[], V>>>
      implements ObjectIterator<Entry<byte[], V>> {
    private MapEntry entry;

    @Override
    public MapEntry next() {
      return entry = new MapEntry(nextEntry());
    }

    // forEachRemaining inherited from MapIterator superclass.
    @Override
    final void acceptOnIndex(final Consumer<? super Object2ObjectMap.Entry<byte[], V>> action,
        final int index) {
      action.accept(entry = new MapEntry(index));
    }

    @Override
    public void remove() {
      super.remove();
      entry.index = -1; // You cannot use a deleted entry.
    }
  }
  private final class FastEntryIterator extends MapIterator<Consumer<? super Entry<byte[], V>>>
      implements ObjectIterator<Object2ObjectMap.Entry<byte[], V>> {
    private final MapEntry entry = new MapEntry();

    @Override
    public MapEntry next() {
      entry.index = nextEntry();
      return entry;
    }

    // forEachRemaining inherited from MapIterator superclass.
    @Override
    final void acceptOnIndex(final Consumer<? super Object2ObjectMap.Entry<byte[], V>> action,
        final int index) {
      entry.index = index;
      action.accept(entry);
    }
  }
  private abstract class MapSpliterator<ConsumerType, SplitType extends MapSpliterator<ConsumerType, SplitType>> {
    /**
     * The index (which bucket) of the next item to give to the action.
     * This counts up instead of down.
     */
    int pos = 0;
    /** The maximum bucket (exclusive) to iterate to */
    int max = n;
    /** An upwards counter counting how many we have given */
    int c = 0;
    boolean hasSplit = false;

    MapSpliterator() {}

    MapSpliterator(int pos, int max, boolean hasSplit) {
      this.pos = pos;
      this.max = max;
      this.hasSplit = hasSplit;
    }

    abstract void acceptOnIndex(final ConsumerType action, final int index);

    abstract SplitType makeForSplit(int pos, int max);

    public boolean tryAdvance(final ConsumerType action) {
      final byte[][] key = Bytes2ObjectOpenHashMap.this.key;
      while (pos < max) {
        if (!(key[pos] == null)) {
          ++c;
          acceptOnIndex(action, pos++);
          return true;
        }
        ++pos;
      }
      return false;
    }

    public void forEachRemaining(final ConsumerType action) {
      final byte[][] key = Bytes2ObjectOpenHashMap.this.key;
      while (pos < max) {
        if (!(key[pos] == null)) {
          acceptOnIndex(action, pos);
          ++c;
        }
        ++pos;
      }
    }

    public long estimateSize() {
      if (!hasSplit) {
        // Root spliterator; we know how many are remaining.
        return size - c;
      } else {
        // After we split, we can no longer know exactly how many we have (or at least not
        // efficiently).
        // (size / n) * (max - pos) aka currentTableDensity * numberOfBucketsLeft seems like a good
        // estimate.
        return Math.min(size - c,
            (long) ((double) size() / n * (max - pos)));
      }
    }

    public SplitType trySplit() {
      if (pos >= max - 1) {
        return null;
      }
      int retLen = max - pos >> 1;
      if (retLen <= 1) {
        return null;
      }
      int myNewPos = pos + retLen;
      int retPos = pos;
      // Since null is returned first, and the convention is that the returned split is the prefix
      // of elements,
      // the split will take care of returning null (if needed), and we won't return it anymore.
      SplitType split = makeForSplit(retPos, myNewPos);
      this.pos = myNewPos;
      this.hasSplit = true;
      return split;
    }

    public long skip(long n) {
      if (n < 0) {
        throw new IllegalArgumentException("Argument must be non-negative: " + n);
      }
      if (n == 0) {
        return 0;
      }
      long skipped = 0;
      final byte[][] key = Bytes2ObjectOpenHashMap.this.key;
      while (pos < max && n > 0) {
        if (!(key[pos++] == null)) {
          ++skipped;
          --n;
        }
      }
      return skipped;
    }
  }
  private final class EntrySpliterator
      extends MapSpliterator<Consumer<? super Entry<byte[], V>>, EntrySpliterator>
      implements ObjectSpliterator<Entry<byte[], V>> {
    private static final int POST_SPLIT_CHARACTERISTICS =
        ObjectSpliterators.SET_SPLITERATOR_CHARACTERISTICS & ~java.util.Spliterator.SIZED;

    EntrySpliterator() {}

    EntrySpliterator(int pos, int max, boolean hasSplit) {
      super(pos, max, hasSplit);
    }

    @Override
    public int characteristics() {
      return hasSplit ? POST_SPLIT_CHARACTERISTICS
          : ObjectSpliterators.SET_SPLITERATOR_CHARACTERISTICS;
    }

    @Override
    final void acceptOnIndex(final Consumer<? super Object2ObjectMap.Entry<byte[], V>> action,
        final int index) {
      action.accept(new MapEntry(index));
    }

    @Override
    final EntrySpliterator makeForSplit(int pos, int max) {
      return new EntrySpliterator(pos, max, true);
    }
  }
  private static class MyBasicEntry<V> extends BasicEntry<byte[], V> {
    public void setKeyAndValue(byte[] k, V v) {
      this.key = k;
      this.value = v;
    }
  }
  private final class MapEntrySet extends AbstractObjectSet<Entry<byte[], V>>
      implements FastEntrySet<byte[], V> {
    @Override
    public ObjectIterator<Object2ObjectMap.Entry<byte[], V>> iterator() {
      return new EntryIterator();
    }

    @Override
    public ObjectIterator<Object2ObjectMap.Entry<byte[], V>> fastIterator() {
      return new FastEntryIterator();
    }

    @Override
    public ObjectSpliterator<Object2ObjectMap.Entry<byte[], V>> spliterator() {
      return new EntrySpliterator();
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean contains(final Object o) {
      if (!(o instanceof Map.Entry)) {
        return false;
      }
      final Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;
      final byte[] k = (byte[]) e.getKey();
      final V v = (V) e.getValue();
      byte[] curr;
      final byte[][] key = Bytes2ObjectOpenHashMap.this.key;
      int pos;
      // The starting point.
      if ((curr =
          key[pos =
              HashCommon.mix(strategy().hashCode(k)) & mask]) == null) {
        return false;
      }
      if (strategy().equals(k, curr)) {
        return java.util.Objects.equals(value[pos], v);
      }
      // There's always an unused entry.
      while (true) {
        if ((curr = key[pos = pos + 1 & mask]) == null) {
          return false;
        }
        if (strategy().equals(k, curr)) {
          return java.util.Objects.equals(value[pos], v);
        }
      }
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean remove(final Object o) {
      if (!(o instanceof Map.Entry)) {
        return false;
      }
      final Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;
      final byte[] k = (byte[]) e.getKey();
      final V v = (V) e.getValue();
      byte[] curr;
      final byte[][] key = Bytes2ObjectOpenHashMap.this.key;
      int pos;
      // The starting point.
      if ((curr =
          key[pos =
              HashCommon.mix(strategy().hashCode(k)) & mask]) == null) {
        return false;
      }
      if (strategy().equals(curr, k)) {
        if (java.util.Objects.equals(value[pos], v)) {
          removeEntry(pos);
          return true;
        }
        return false;
      }
      while (true) {
        if ((curr = key[pos = pos + 1 & mask]) == null) {
          return false;
        }
        if (strategy().equals(curr, k)) {
          if (java.util.Objects.equals(value[pos], v)) {
            removeEntry(pos);
            return true;
          }
        }
      }
    }

    @Override
    public int size() {
      return size;
    }

    @Override
    public void clear() {
      Bytes2ObjectOpenHashMap.this.clear();
    }

    /** {@inheritDoc} */
    @Override
    public void forEach(final Consumer<? super Object2ObjectMap.Entry<byte[], V>> consumer) {
      for (int pos = n; pos-- != 0;) {
        if (!(key[pos] == null)) {
          consumer.accept(new BasicEntry<>(key[pos], value[pos]));
        }
      }
    }

    /** {@inheritDoc} */
    @Override
    public void fastForEach(final Consumer<? super Object2ObjectMap.Entry<byte[], V>> consumer) {
      final MyBasicEntry<V> entry = new MyBasicEntry<>();
      for (int pos = n; pos-- != 0;) {
        if (!(key[pos] == null)) {
          entry.setKeyAndValue(key[pos], value[pos]);
          consumer.accept(entry);
        }
      }
    }
  }

  @Override
  public FastEntrySet<byte[], V> object2ObjectEntrySet() {
    return new MapEntrySet();
  }

  /**
   * An iterator on keys.
   *
   * <p>
   * We simply override the
   * {@link java.util.ListIterator#next()}/{@link java.util.ListIterator#previous()} methods
   * (and possibly their type-specific counterparts) so that they return keys
   * instead of entries.
   */
  private final class KeyIterator extends MapIterator<Consumer<? super byte[]>>
      implements ObjectIterator<byte[]> {
    public KeyIterator() {
      super();
    }

    // forEachRemaining inherited from MapIterator superclass.
    // Despite the superclass declared with generics, the way Java inherits and generates bridge
    // methods avoids the boxing/unboxing
    @Override
    final void acceptOnIndex(final Consumer<? super byte[]> action, final int index) {
      action.accept(key[index]);
    }

    @Override
    public byte[] next() {
      return key[nextEntry()];
    }
  }
  private final class KeySpliterator
      extends MapSpliterator<Consumer<? super byte[]>, KeySpliterator>
      implements ObjectSpliterator<byte[]> {
    private static final int POST_SPLIT_CHARACTERISTICS =
        ObjectSpliterators.SET_SPLITERATOR_CHARACTERISTICS & ~java.util.Spliterator.SIZED;

    KeySpliterator() {}

    KeySpliterator(int pos, int max, boolean hasSplit) {
      super(pos, max, hasSplit);
    }

    @Override
    public int characteristics() {
      return hasSplit ? POST_SPLIT_CHARACTERISTICS
          : ObjectSpliterators.SET_SPLITERATOR_CHARACTERISTICS;
    }

    @Override
    final void acceptOnIndex(final Consumer<? super byte[]> action, final int index) {
      action.accept(key[index]);
    }

    @Override
    final KeySpliterator makeForSplit(int pos, int max) {
      return new KeySpliterator(pos, max, true);
    }
  }
  private final class KeySet extends AbstractObjectSet<byte[]> {
    @Override
    public ObjectIterator<byte[]> iterator() {
      return new KeyIterator();
    }

    @Override
    public ObjectSpliterator<byte[]> spliterator() {
      return new KeySpliterator();
    }

    /** {@inheritDoc} */
    @Override
    public void forEach(final Consumer<? super byte[]> consumer) {
      for (int pos = n; pos-- != 0;) {
        final byte[] k = key[pos];
        if (!(k == null)) {
          consumer.accept(k);
        }
      }
    }

    @Override
    public int size() {
      return size;
    }

    @Override
    public boolean contains(Object k) {
      return containsKey(k);
    }

    @Override
    public boolean remove(Object k) {
      final int oldSize = size;
      Bytes2ObjectOpenHashMap.this.remove(k);
      return size != oldSize;
    }

    @Override
    public void clear() {
      Bytes2ObjectOpenHashMap.this.clear();
    }
  }

  @SuppressWarnings("NullableProblems")
  @Override
  public ObjectSet<byte[]> keySet() {
    return new KeySet();
  }

  /**
   * An iterator on values.
   *
   * <p>
   * We simply override the
   * {@link java.util.ListIterator#next()}/{@link java.util.ListIterator#previous()} methods
   * (and possibly their type-specific counterparts) so that they return values
   * instead of entries.
   */
  private final class ValueIterator extends MapIterator<Consumer<? super V>>
      implements ObjectIterator<V> {
    public ValueIterator() {
      super();
    }

    // forEachRemaining inherited from MapIterator superclass.
    // Despite the superclass declared with generics, the way Java inherits and generates bridge
    // methods avoids the boxing/unboxing
    @Override
    final void acceptOnIndex(final Consumer<? super V> action, final int index) {
      action.accept(value[index]);
    }

    @Override
    public V next() {
      return value[nextEntry()];
    }
  }
  private final class ValueSpliterator extends MapSpliterator<Consumer<? super V>, ValueSpliterator>
      implements ObjectSpliterator<V> {
    private static final int POST_SPLIT_CHARACTERISTICS =
        ObjectSpliterators.COLLECTION_SPLITERATOR_CHARACTERISTICS & ~java.util.Spliterator.SIZED;

    ValueSpliterator() {}

    ValueSpliterator(int pos, int max, boolean hasSplit) {
      super(pos, max, hasSplit);
    }

    @Override
    public int characteristics() {
      return hasSplit ? POST_SPLIT_CHARACTERISTICS
          : ObjectSpliterators.COLLECTION_SPLITERATOR_CHARACTERISTICS;
    }

    @Override
    final void acceptOnIndex(final Consumer<? super V> action, final int index) {
      action.accept(value[index]);
    }

    @Override
    final ValueSpliterator makeForSplit(int pos, int max) {
      return new ValueSpliterator(pos, max, true);
    }
  }

  @SuppressWarnings("NullableProblems")
  @Override
  public ObjectCollection<V> values() {
    return new AbstractObjectCollection<V>() {
      @Override
      public ObjectIterator<V> iterator() {
        return new ValueIterator();
      }

      @Override
      public ObjectSpliterator<V> spliterator() {
        return new ValueSpliterator();
      }

      /** {@inheritDoc} */
      @Override
      public void forEach(final Consumer<? super V> consumer) {
        for (int pos = n; pos-- != 0;) {
          if (!(key[pos] == null)) {
            consumer.accept(value[pos]);
          }
        }
      }

      @Override
      public int size() {
        return size;
      }

      @Override
      public boolean contains(Object v) {
        return containsValue(v);
      }

      @Override
      public void clear() {
        Bytes2ObjectOpenHashMap.this.clear();
      }
    };
  }

  /**
   * Rehashes the map, making the table as small as possible.
   *
   * <p>
   * This method rehashes the table to the smallest size satisfying the
   * load factor. It can be used when the set will not be changed anymore, so
   * to optimize access speed and size.
   *
   * <p>
   * If the table size is already the minimum possible, this method
   * does nothing.
   *
   * @return true if there was enough memory to trim the map.
   * @see #trim(int)
   */
  @SuppressWarnings("unused")
  public boolean trim() {
    return trim(size);
  }

  /**
   * Rehashes this map if the table is too large.
   *
   * <p>
   * Let <var>N</var> be the smallest table size that can hold
   * <code>max(n,{@link #size()})</code> entries, still satisfying the load factor. If the current
   * table size is smaller than or equal to <var>N</var>, this method does
   * nothing. Otherwise, it rehashes this map in a table of size
   * <var>N</var>.
   *
   * <p>
   * This method is useful when reusing maps. {@linkplain #clear() Clearing a
   * map} leaves the table size untouched. If you are reusing a map
   * many times, you can call this method with a typical
   * size to avoid keeping around a very large table just
   * because of a few large transient maps.
   *
   * @param n the threshold for the trimming.
   * @return true if there was enough memory to trim the map.
   * @see #trim()
   */
  public boolean trim(final int n) {
    final int l = HashCommon.nextPowerOfTwo((int) Math.ceil(n / getLoadFactor()));
    if (l >= this.n || size > maxFill(l, getLoadFactor())) {
      return true;
    }
    try {
      rehash(l);
    } catch (OutOfMemoryError cantDoIt) {
      return false;
    }
    return true;
  }

  /**
   * Rehashes the map.
   *
   * <p>
   * This method implements the basic rehashing strategy, and may be
   * overridden by subclasses implementing different rehashing strategies (e.g.,
   * disk-based rehashing). However, you should not override this method
   * unless you understand the internal workings of this class.
   *
   * @param newN the new size
   */
  @SuppressWarnings("unchecked")
  protected void rehash(final int newN) {
    final byte[][] key = this.key;
    final V[] value = this.value;
    final int mask = newN - 1; // Note that this is used by the hashing macro
    final byte[][] newKey = new byte[newN + 1][];
    final V[] newValue = (V[]) new Object[newN + 1];
    int i = n, pos;
    for (int j = size(); j-- != 0;) {
      // noinspection StatementWithEmptyBody
      while (key[--i] == null) {
      }
      if (!(newKey[pos =
          HashCommon.mix(strategy().hashCode(key[i])) & mask] == null)) {
        // noinspection StatementWithEmptyBody
        while (!(newKey[pos = pos + 1 & mask] == null)) {
        }
      }
      newKey[pos] = key[i];
      newValue[pos] = value[i];
    }
    newValue[newN] = value[n];
    n = newN;
    this.mask = mask;
    maxFill = maxFill(n, getLoadFactor());
    this.key = newKey;
    this.value = newValue;
  }

  /**
   * Returns a deep copy of this map.
   *
   * <p>
   * This method performs a deep copy of this hash map; the data stored in the
   * map, however, is not cloned. Note that this makes a difference only for object keys.
   *
   * @return a deep copy of this map.
   */
  @Override
  @SuppressWarnings("unchecked")
  public Bytes2ObjectOpenHashMap<V> clone() {
    Bytes2ObjectOpenHashMap<V> c;
    try {
      c = (Bytes2ObjectOpenHashMap<V>) super.clone();
    } catch (CloneNotSupportedException cantHappen) {
      throw new InternalError();
    }
    c.key = key.clone();
    c.value = value.clone();
    return c;
  }

  /**
   * Returns a hash code for this map.
   *
   * This method overrides the generic method provided by the superclass.
   * Since {@code equals()} is not overridden, it is important
   * that the value returned by this method is the same value as
   * the one returned by the overridden method.
   *
   * @return a hash code for this map.
   */
  @Override
  public int hashCode() {
    int h = 0;
    for (int j = size(), i = 0; j-- != 0;) {
      while (key[i] == null) {
        i++;
      }
      int t = strategy().hashCode(key[i]);
      if (this != value[i]) {
        t ^= value[i] == null ? 0 : value[i].hashCode();
      }
      h += t;
      i++;
    }
    return h;
  }

  @Override
  public boolean equals(Object o) {
    return super.equals(o);
  }

  private void writeObject(java.io.ObjectOutputStream s) throws java.io.IOException {
    final byte[][] key = this.key;
    final V[] value = this.value;
    final EntryIterator i = new EntryIterator();
    s.defaultWriteObject();
    for (int j = size, e; j-- != 0;) {
      e = i.nextEntry();
      s.writeObject(key[e]);
      s.writeObject(value[e]);
    }
  }

  @SuppressWarnings("unchecked")
  private void readObject(java.io.ObjectInputStream s)
      throws java.io.IOException, ClassNotFoundException {
    s.defaultReadObject();
    n = arraySize(size, getLoadFactor());
    maxFill = maxFill(n, getLoadFactor());
    mask = n - 1;
    final byte[][] key = this.key = new byte[n + 1][];
    final V[] value = this.value = (V[]) new Object[n + 1];
    byte[] k;
    V v;
    for (int i = size, pos; i-- != 0;) {
      k = (byte[]) s.readObject();
      v = (V) s.readObject();
      pos = HashCommon.mix(strategy().hashCode(k)) & mask;
      while (!(key[pos] == null)) {
        pos = pos + 1 & mask;
      }
      key[pos] = k;
      value[pos] = v;
    }
  }

  public void fastForEach(final Consumer<? super Map.Entry<byte[], V>> consumer) {
    final MyBasicEntry<V> entry = new MyBasicEntry<>();
    for (int pos = n; pos-- != 0;) {
      if (key[pos] != null) {
        entry.setKeyAndValue(key[pos], value[pos]);
        consumer.accept(entry);
      }
    }
  }

  public void fastForEachKey(final Consumer<byte[]> consumer) {
    for (int pos = n; pos-- != 0;) {
      if (key[pos] != null) {
        consumer.accept(key[pos]);
      }
    }
  }

  public void fastForEachValue(final Consumer<V> consumer) {
    for (int pos = n; pos-- != 0;) {
      if (value[pos] != null) {
        consumer.accept(value[pos]);
      }
    }
  }

  public boolean fastWhileEach(final Predicate<? super Map.Entry<byte[], V>> predicate) {
    final MyBasicEntry<V> entry = new MyBasicEntry<>();
    for (int pos = n; pos-- != 0;) {
      if (key[pos] != null) {
        entry.setKeyAndValue(key[pos], value[pos]);
        if (!predicate.test(entry)) {
          return false;
        }
      }
    }
    return true;
  }

  public boolean fastWhileEachValue(final Predicate<V> predicate) {
    for (int pos = n; pos-- != 0;) {
      if (value[pos] != null) {
        if (!predicate.test(value[pos])) {
          return false;
        }
      }
    }
    return true;
  }

  protected int getMaxIndex() {
    return n;
  }

  protected byte[] getKeyAtIndex(int index) {
    return key[index];
  }

  protected V getValueAtIndex(int index) {
    return value[index];
  }
}
