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
package org.apache.geode.redis.internal.data.collections;


import static it.unimi.dsi.fastutil.HashCommon.mix;
import static org.apache.geode.internal.JvmSizeUtils.memoryOverhead;

import java.util.Collection;

import it.unimi.dsi.fastutil.objects.ObjectOpenCustomHashSet;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.internal.size.Sizeable;

public abstract class SizeableObjectOpenCustomHashSetWithCursor<K>
    extends ObjectOpenCustomHashSet<K>
    implements Sizeable {
  private static final long serialVersionUID = 9174920505089089517L;
  private static final int OPEN_HASH_SET_OVERHEAD =
      memoryOverhead(SizeableObjectOpenCustomHashSetWithCursor.class);

  private int memberOverhead;

  public SizeableObjectOpenCustomHashSetWithCursor(int expected, Strategy<? super K> strategy) {
    super(expected, strategy);
  }

  public SizeableObjectOpenCustomHashSetWithCursor(Strategy<? super K> strategy) {
    super(strategy);
  }

  public SizeableObjectOpenCustomHashSetWithCursor(Collection<? extends K> c,
      Strategy<? super K> strategy) {
    super(c, strategy);
  }

  @Override
  public boolean add(K k) {
    boolean added = super.add(k);
    if (added) {
      memberOverhead += sizeElement(k);
    }
    return added;
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean remove(Object k) {
    boolean removed = super.remove(k);
    if (removed) {
      memberOverhead -= sizeElement((K) k);
    }
    return removed;
  }

  @Override
  public int getSizeInBytes() {
    // The object referenced by the "strategy" field is not sized
    // since it is usually a singleton instance.
    return OPEN_HASH_SET_OVERHEAD + memoryOverhead(key) + memberOverhead;
  }

  /**
   * Scan entries and pass them to the given consumer function, starting at the passed in
   * cursor. This method will scan until at least count entries are returned, or the entire
   * map has been scanned. Once the returned cursor is 0, the entire map is scanned.
   *
   * This method may emit more than *count* number of elements if there are hash collisions.
   *
   * @param cursor The cursor to start from. Should be 0 for the initial scan. Subsequent calls
   *        should use the cursor returned by the previous scan call.
   * @param count The number of elements to scan
   * @param consumer A function to pass the scanned keys and values to
   * @param privateData Some data to pass to the function, for example a map to collect values in.
   *        This
   *        allows the function to be stateless.
   * @param <D> The type of the data passed to the function/
   * @return The next cursor to scan from, or 0 if the scan has touched all elements.
   */
  public <D> int scan(int cursor, int count,
      SizeableObjectOpenCustomHashSetWithCursor.EntryConsumer<K, D> consumer, D privateData) {
    // Implementation notes
    //
    // This stateless scan cursor algorithm is based on the dictScan cursor
    // implementation from dict.c in redis. Please see the comments in that class for the full
    // details. That iteration algorithm was designed by Pieter Noordhuis.
    //
    // There is one wrinkle due to the fact that we are using a different type of hashtable here.
    // The parent class, Object2ObjectOpenHashMap, uses an open addressing with a linear
    // probe. What that means is that when there is a hash collision, instead of putting
    // a linked list of hash entries into a single hash bucket, this implementation simply
    // moves on to the next element to the right in the array and tries to put the inserted
    // object there, continuing until it finds a null slot.
    //
    // So in order to use the redis cursor algorithm, our scan needs to probe ahead to
    // subsequent positions to find any hash entries that match the position we are scanning.
    // This is logically equivalent to iterating over the linked list in a hashtable bucket
    // for a redis style closed addressing hashtable.
    //

    do {
      // Emit all of the entries at the cursor. This means looking forward in the hash
      // table for any non-null entries that might hash to the current cursor and emitting
      // those as well. This may even wrap around to the front of the hashtable.
      int position = cursor;
      while (key[position & mask] != null) {
        K currentKey = key[position & mask];
        if (keyHashesTo(currentKey, position, cursor & mask)) {
          consumer.consume(privateData, currentKey);
          count--;
        }
        position++;
      }

      // Increment the reversed cursor
      cursor |= ~mask;
      cursor = rev(cursor);
      cursor++;
      cursor = rev(cursor);


    } while (count > 0 && cursor != 0);

    return cursor;
  }

  /**
   * reverse the bits in a cursor.
   *
   * Package scope to allow for unit testing to make sure we don't have some silly
   * java signed int issues
   *
   * @param value the value to reverse
   * @return the reversed bits.
   */
  static int rev(int value) {
    // This implementation is also based on dict.c from redis, which was originally from
    // http://graphics.stanford.edu/~seander/bithacks.html#ReverseParallel
    int s = 32;
    int mask = ~0;
    while ((s >>>= 1) > 0) {
      mask ^= (mask << s);
      value = ((value >>> s) & mask) | ((value << s) & ~mask);
    }
    return value;
  }

  public interface EntryConsumer<K, D> {
    void consume(D privateData, K key);
  }

  /**
   * Check to see if given key hashes to the expected hash.
   *
   * @param currentKey The key to key
   * @param currentPosition The position of the key in the key[] array
   * @param expectedHash - the expected hash of the key.
   */
  private boolean keyHashesTo(K currentKey, int currentPosition, int expectedHash) {
    // There is a small optimization here. If the previous element
    // is null, we know that the element at position does hash to the expected
    // hash because it is not here as a result of a collision at some previous position.

    K previousKey = key[(currentPosition - 1) & mask];
    return previousKey == null || hash(currentKey) == expectedHash;
  }

  @VisibleForTesting
  public int hash(K key) {
    return mix(strategy().hashCode(key)) & mask;
  }

  protected abstract int sizeElement(K element);
}
