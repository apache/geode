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

import static org.apache.geode.internal.JvmSizeUtils.memoryOverhead;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.bytes.ByteArrays;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenCustomHashMap;
import org.junit.Test;

import org.apache.geode.cache.util.ObjectSizer;
import org.apache.geode.internal.size.ReflectionObjectSizer;
import org.apache.geode.redis.internal.data.RedisHash;
import org.apache.geode.redis.internal.data.RedisSortedSet;

public class SizeableBytes2ObjectOpenCustomHashMapWithCursorTest {
  private final ObjectSizer sizer = ReflectionObjectSizer.getInstance();

  private static final Hash.Strategy<Integer> NATURAL_HASH = new Hash.Strategy<Integer>() {
    @Override
    public int hashCode(Integer o) {
      return o.hashCode();
    }

    @Override
    public boolean equals(Integer a, Integer b) {
      return a.equals(b);
    }
  };

  private static class Bytes2StringMap
      extends SizeableBytes2ObjectOpenCustomHashMapWithCursor<String> {
    public Bytes2StringMap() {
      super();
    }

    public Bytes2StringMap(int initialSize) {
      super(initialSize);
    }

    @Override
    protected int sizeValue(String value) {
      return 0;
    }
  }

  private static byte[] makeKey(Integer i) {
    return i.toString().getBytes();
  }

  @Test
  public void scanEntireMap_ReturnsExpectedElements() {
    Bytes2StringMap map = new Bytes2StringMap();
    IntStream.range(0, 10).forEach(i -> map.put(makeKey(i), "value-" + i));

    Map<byte[], String> scanned = new Object2ObjectOpenCustomHashMap<>(ByteArrays.HASH_STRATEGY);
    int result = map.scan(0, 10000, Map::put, scanned);
    assertThat(result).isEqualTo(0);
    assertThat(scanned).isEqualTo(map);
  }

  private void fillMapWithUniqueHashKeys(Bytes2StringMap map, int keysToAdd) {
    int keyCounter = 0;
    Set<Integer> hashesAdded = new HashSet<>();
    while (keysToAdd > 0) {
      byte[] key = makeKey(keyCounter);
      int keyHash = map.hash(key);
      if (!hashesAdded.contains(keyHash)) {
        hashesAdded.add(keyHash);
        map.put(key, "value-" + keyCounter);
        keysToAdd--;
      }
      keyCounter++;
    }
  }

  private void fillMapWithCollidingHashKeys(Bytes2StringMap map, int keysToAdd) {
    int keyCounter = 0;
    Set<Integer> hashesAdded = new HashSet<>();
    while (keysToAdd > 0) {
      byte[] key = makeKey(keyCounter);
      int keyHash = map.hash(key);
      if (hashesAdded.isEmpty() || hashesAdded.contains(keyHash)) {
        hashesAdded.add(keyHash);
        map.put(key, "value-" + keyCounter);
        keysToAdd--;
        System.out.println("adding key " + keyCounter);
      }
      keyCounter++;
    }
  }

  @Test
  public void twoScansWithNoModifications_ReturnsExpectedElements() {
    final int MAP_SIZE = 10;
    Bytes2StringMap map = new Bytes2StringMap(MAP_SIZE * 2); // *2 to prevent rehash
    fillMapWithUniqueHashKeys(map, MAP_SIZE);
    Map<byte[], String> scanned = new Object2ObjectOpenCustomHashMap<>(ByteArrays.HASH_STRATEGY);

    int scanSize = 1 + map.size() / 2;
    // Scan part way through the map
    int cursor = map.scan(0, scanSize, Map::put, scanned);
    assertThat(scanned).hasSize(scanSize);

    // Scan past the end of the map
    cursor = map.scan(cursor, scanSize, Map::put, scanned);
    assertThat(scanned).hasSize(map.size());
    assertThat(cursor).isEqualTo(0);

    assertThat(scanned).isEqualTo(map);
  }

  @Test
  public void scanWithConcurrentRemoves_ReturnsExpectedElements() {
    final int MAP_SIZE = 10;
    Bytes2StringMap map = new Bytes2StringMap(MAP_SIZE * 2); // *2 to prevent rehash
    fillMapWithUniqueHashKeys(map, MAP_SIZE);
    Map<byte[], String> scanned = new Object2ObjectOpenCustomHashMap<>(ByteArrays.HASH_STRATEGY);

    int cursor = map.scan(0, MAP_SIZE / 2, Map::put, scanned);
    assertThat(scanned).hasSize(MAP_SIZE / 2);

    // Remove some of the elements
    Iterator<Map.Entry<byte[], String>> iterator = map.entrySet().iterator();
    int removeCount = MAP_SIZE / 2 - 1;
    while (removeCount > 0 && iterator.hasNext()) {
      iterator.next();
      iterator.remove();
      removeCount--;
    }

    cursor = map.scan(cursor, MAP_SIZE / 2, Map::put, scanned);
    assertThat(cursor).isEqualTo(0);

    assertThat(scanned.values()).containsAll(map.values());
  }

  @Test
  public void scanWithHashcodeCollisions_ReturnsExpectedElements() {
    final int MAP_SIZE = 10;
    Bytes2StringMap map = new Bytes2StringMap(MAP_SIZE * 2); // *2 to prevent rehash
    fillMapWithCollidingHashKeys(map, MAP_SIZE);
    Map<byte[], String> scanned = new Object2ObjectOpenCustomHashMap<>(ByteArrays.HASH_STRATEGY);

    int cursor = map.scan(0, 1, Map::put, scanned);

    // The scan had to ignore the count and return all of the elements with the same hash
    assertThat(scanned).hasSize(MAP_SIZE);
    assertThat(scanned).isEqualTo(map);
    cursor = map.scan(cursor, 1, Map::put, scanned);
    assertThat(cursor).isZero();
    assertThat(scanned).hasSize(MAP_SIZE);
    assertThat(scanned).isEqualTo(map);
  }

  @Test
  public void scanWithHashcodeCollisionsAndConcurrentRemoves_ReturnsExpectedElements() {
    final int MAP_SIZE = 10;
    Bytes2StringMap map = new Bytes2StringMap(MAP_SIZE * 2); // *2 to prevent rehash
    fillMapWithCollidingHashKeys(map, MAP_SIZE);
    Map<byte[], String> scanned = new Object2ObjectOpenCustomHashMap<>(ByteArrays.HASH_STRATEGY);

    int cursor = map.scan(0, MAP_SIZE / 2, Map::put, scanned);
    assertThat(scanned).hasSize(MAP_SIZE);

    // Remove some of the elements
    Iterator<Map.Entry<byte[], String>> iterator = map.entrySet().iterator();
    int removeCount = MAP_SIZE / 2 - 1;
    while (removeCount > 0 && iterator.hasNext()) {
      iterator.next();
      iterator.remove();
      removeCount--;
    }

    cursor = map.scan(cursor, MAP_SIZE / 2, Map::put, scanned);

    assertThat(cursor).isEqualTo(0);
    assertThat(scanned).hasSize(MAP_SIZE);
  }

  @Test
  public void scanWithGrowingTable_DoesNotMissElements() {
    final int MAP_SIZE = 10;
    Bytes2StringMap map = new Bytes2StringMap(MAP_SIZE * 2); // *2 to prevent rehash
    fillMapWithUniqueHashKeys(map, MAP_SIZE);
    Map<byte[], String> scanned = new Object2ObjectOpenCustomHashMap<>(ByteArrays.HASH_STRATEGY);
    ArrayList<byte[]> initialKeys = new ArrayList<>(map.keySet());

    int cursor = map.scan(0, MAP_SIZE / 2, Map::put, scanned);
    assertThat(scanned).hasSize(MAP_SIZE / 2);

    // Add a lot of elements to trigger a resize
    IntStream.range(10, 500).forEach(i -> map.put(makeKey(i), "value-" + i));

    cursor = map.scan(cursor, 500, Map::put, scanned);
    assertThat(cursor).isEqualTo(0);

    // We don't know that we will have all of the 500 new elements, only that
    // we should have scanned all of the original elements
    assertThat(scanned.keySet()).containsAll(initialKeys);
  }

  @Test
  public void scanWithShrinkingTable_DoesNotMissElements() {
    final int MAP_SIZE = 500;
    Bytes2StringMap map = new Bytes2StringMap(MAP_SIZE * 2); // *2 to prevent rehash
    fillMapWithUniqueHashKeys(map, MAP_SIZE);
    Map<byte[], String> scanned = new Object2ObjectOpenCustomHashMap<>(ByteArrays.HASH_STRATEGY);

    int cursor = map.scan(0, 50, Map::put, scanned);
    assertThat(scanned).hasSize(50);

    // Remove a lot of elements to trigger a resize
    // Remove some of the elements
    Iterator<Map.Entry<byte[], String>> iterator = map.entrySet().iterator();
    int removeCount = MAP_SIZE - 100;
    while (removeCount > 0 && iterator.hasNext()) {
      iterator.next();
      iterator.remove();
      removeCount--;
    }

    cursor = map.scan(cursor, MAP_SIZE, Map::put, scanned);
    assertThat(cursor).isEqualTo(0);

    // Scan should at least have all of the remaining keys
    assertThat(scanned).containsAllEntriesOf(map);
  }

  @Test
  public void revWorksWhenSignBitIsSet() {
    assertThat(SizeableBytes2ObjectOpenCustomHashMapWithCursor.rev(0xFF000000)).isEqualTo(0xFF);
    assertThat(SizeableBytes2ObjectOpenCustomHashMapWithCursor.rev(0xFF)).isEqualTo(0xFF000000);
  }

  @Test
  public void putUpdatesSizeWhenCreatingNewEntry() {
    RedisHash.Hash hash = new RedisHash.Hash();
    assertThat(hash.getSizeInBytes()).isEqualTo(expectedSize(hash));
    hash.put(new byte[] {(byte) 1}, new byte[] {(byte) 1});
    assertThat(hash.getSizeInBytes()).isEqualTo(expectedSize(hash));
  }

  @Test
  public void putUpdatesSizeWhenUpdatingExistingEntry() {
    RedisHash.Hash hash = new RedisHash.Hash();
    assertThat(hash.getSizeInBytes()).isEqualTo(expectedSize(hash));
    byte[] key = new byte[1];
    byte[] initialValue = new byte[1];

    hash.put(key, initialValue);
    assertThat(hash.getSizeInBytes()).isEqualTo(expectedSize(hash));

    byte[] largerValue = new byte[100];
    hash.put(key, largerValue);
    assertThat(hash.getSizeInBytes()).isEqualTo(expectedSize(hash));

    byte[] smallerValue = new byte[2];
    hash.put(key, smallerValue);
    assertThat(hash.getSizeInBytes()).isEqualTo(expectedSize(hash));
  }

  @Test
  public void removeUpdatesSize() {
    RedisHash.Hash hash = new RedisHash.Hash();
    byte[] key = new byte[1];
    byte[] initialValue = new byte[100];

    hash.put(key, initialValue);
    assertThat(hash.getSizeInBytes()).isEqualTo(expectedSize(hash));

    hash.remove(key);
    assertThat(hash.getSizeInBytes()).isEqualTo(expectedSize(hash));
  }

  private int expectedSize(SizeableBytes2ObjectOpenCustomHashMapWithCursor<?> map) {
    return sizer.sizeof(map);
  }

  @Test
  public void getSizeInBytesIsAccurateForByteArrays() {
    Map<byte[], byte[]> initialElements = new HashMap<>();
    int initialNumberOfElements = 20;
    int elementsToAdd = 100;

    // Create a map with an initial size
    for (int i = 0; i < initialNumberOfElements; ++i) {
      byte[] key = {(byte) i};
      byte[] value = {(byte) (initialNumberOfElements - i)};
      initialElements.put(key, value);
    }
    RedisHash.Hash hash = new RedisHash.Hash(initialElements);

    assertThat(hash.getSizeInBytes()).isEqualTo(expectedSize(hash));

    // Add more elements to force a resizing of the backing arrays and confirm that size changes as
    // expected
    int totalNumberOfElements = initialNumberOfElements + elementsToAdd;
    for (int i = initialNumberOfElements; i < totalNumberOfElements; ++i) {
      byte[] key = {(byte) i};
      byte[] value = {(byte) (totalNumberOfElements - i)};
      hash.put(key, value);
      assertThat(hash.getSizeInBytes()).isEqualTo(expectedSize(hash));
    }

    // Update values and confirm that size changes as expected
    for (int i = initialNumberOfElements; i < totalNumberOfElements; ++i) {
      byte[] key = {(byte) i};
      byte[] value = {(byte) i};
      hash.put(key, value);
      assertThat(hash.getSizeInBytes()).isEqualTo(expectedSize(hash));
    }

    assertThat(hash.size()).isEqualTo(totalNumberOfElements);

    // Remove all elements and confirm that size changes as expected
    for (int i = 0; i < totalNumberOfElements; ++i) {
      hash.remove(new byte[] {(byte) i});
      assertThat(hash.getSizeInBytes()).isEqualTo(expectedSize(hash));
    }

    assertThat(hash.size()).isEqualTo(0);
  }

  @Test
  public void getSizeInBytesIsAccurateForOrderedSetEntryValues() {
    Map<byte[], RedisSortedSet.OrderedSetEntry> initialElements = new HashMap<>();
    int initialNumberOfElements = 20;
    int elementsToAdd = 100;

    // Create a map with an initial size
    for (int i = 0; i < initialNumberOfElements; ++i) {
      byte[] key = {(byte) i};
      byte[] member = key;
      byte[] scoreBytes = String.valueOf(i).getBytes();
      RedisSortedSet.OrderedSetEntry value = new RedisSortedSet.OrderedSetEntry(member, scoreBytes);
      initialElements.put(key, value);
    }
    RedisSortedSet.MemberMap hash = new RedisSortedSet.MemberMap(initialElements);

    assertThat(hash.getSizeInBytes()).isEqualTo(expectedSize(hash));

    // Add more elements to force a resizing of the backing arrays and confirm that size changes as
    // expected
    int totalNumberOfElements = initialNumberOfElements + elementsToAdd;
    for (int i = initialNumberOfElements; i < totalNumberOfElements; ++i) {
      byte[] key = {(byte) i};
      byte[] member = key;
      byte[] scoreBytes = String.valueOf(totalNumberOfElements - i).getBytes();
      RedisSortedSet.OrderedSetEntry value = new RedisSortedSet.OrderedSetEntry(member, scoreBytes);
      hash.put(key, value);
      assertThat(hash.getSizeInBytes()).isEqualTo(expectedSize(hash));
    }

    // Update values and confirm that size changes as expected
    for (int i = initialNumberOfElements; i < totalNumberOfElements; ++i) {
      byte[] key = {(byte) i};
      byte[] member = key;
      byte[] scoreBytes = String.valueOf(i).getBytes();
      RedisSortedSet.OrderedSetEntry value = hash.get(key);
      byte[] oldScoreBytes = value.getScoreBytes();
      int scoreDelta = memoryOverhead(scoreBytes)
          - memoryOverhead(oldScoreBytes);

      int oldSize = hash.getSizeInBytes();
      value.updateScore(scoreBytes);
      int sizeDelta = hash.getSizeInBytes() - oldSize;

      assertThat(sizeDelta).isEqualTo(scoreDelta);

      assertThat(hash.getSizeInBytes()).isEqualTo(expectedSize(hash));
    }

    assertThat(hash.size()).isEqualTo(totalNumberOfElements);

    // Remove all elements and confirm that size changes as expected
    for (int i = 0; i < totalNumberOfElements; ++i) {
      hash.remove(new byte[] {(byte) i});
      assertThat(hash.getSizeInBytes()).isEqualTo(expectedSize(hash));
    }

    assertThat(hash.size()).isEqualTo(0);
  }
}
