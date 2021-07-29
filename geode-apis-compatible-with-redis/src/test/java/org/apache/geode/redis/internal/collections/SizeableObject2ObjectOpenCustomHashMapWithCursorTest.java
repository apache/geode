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

import static org.apache.geode.internal.JvmSizeUtils.sizeByteArray;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.stream.IntStream;

import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.bytes.ByteArrays;
import org.assertj.core.api.SoftAssertions;
import org.junit.Test;

import org.apache.geode.cache.util.ObjectSizer;
import org.apache.geode.internal.size.ReflectionObjectSizer;
import org.apache.geode.redis.internal.data.RedisHash;
import org.apache.geode.redis.internal.data.RedisSortedSet;

public class SizeableObject2ObjectOpenCustomHashMapWithCursorTest {
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

  private static final Hash.Strategy<Integer> COLLIDING_HASH = new Hash.Strategy<Integer>() {
    @Override
    public int hashCode(Integer o) {
      return o % 5;
    }

    @Override
    public boolean equals(Integer a, Integer b) {
      return a.equals(b);
    }
  };

  private static class Integer2StringMap
      extends SizeableObject2ObjectOpenCustomHashMapWithCursor<Integer, String> {
    public Integer2StringMap() {
      super(NATURAL_HASH);
    }

    public Integer2StringMap(Hash.Strategy<Integer> hashStrategy) {
      super(hashStrategy);
    }

    @Override
    protected int sizeKey(Integer key) {
      return 0;
    }

    @Override
    protected int sizeValue(String value) {
      return 0;
    }
  }

  @Test
  public void scanEntireMap_ReturnsExpectedElements() {
    Integer2StringMap map = new Integer2StringMap();
    IntStream.range(0, 10).forEach(i -> map.put(i, "value-" + i));

    Map<Integer, String> scanned = new HashMap<>();
    int result = map.scan(0, 10000, Map::put, scanned);
    assertThat(result).isEqualTo(0);
    assertThat(scanned).isEqualTo(map);
  }

  @Test
  public void twoScansWithNoModifications_ReturnsExpectedElements() {
    Integer2StringMap map = new Integer2StringMap();
    IntStream.range(0, 10).forEach(i -> map.put(i, "value-" + i));

    Map<Integer, String> scanned = new HashMap<>();

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
    Integer2StringMap map = new Integer2StringMap();
    IntStream.range(0, 10).forEach(i -> map.put(i, "value-" + i));

    Map<Integer, String> scanned = new HashMap<>();
    int cursor = map.scan(0, 5, Map::put, scanned);
    assertThat(scanned).hasSize(5);

    // Remove some of the elements
    map.remove(2);
    map.remove(4);
    map.remove(5);
    map.remove(7);

    cursor = map.scan(cursor, 5, Map::put, scanned);
    assertThat(cursor).isEqualTo(0);

    assertThat(scanned).containsKeys(0, 1, 3, 6, 8, 9);
  }

  @Test
  public void scanWithHashcodeCollisions_ReturnsExpectedElements() {
    Integer2StringMap map = new Integer2StringMap(COLLIDING_HASH);
    IntStream.range(0, 10).forEach(i -> map.put(i, "value-" + i));

    // The colliding hash is just key % 5. So 0 and 5 will have the same hashcode, etc.
    Map<Integer, String> scanned = new HashMap<>();
    int cursor = map.scan(0, 1, Map::put, scanned);

    // The scan had to ignore the count and return all of the elements with the same hash
    assertThat(scanned).hasSize(2);

    cursor = map.scan(cursor, 1, Map::put, scanned);
    assertThat(scanned).hasSize(4);
    cursor = map.scan(cursor, 1, Map::put, scanned);
    assertThat(scanned).hasSize(6);
    cursor = map.scan(cursor, 1, Map::put, scanned);
    assertThat(scanned).hasSize(8);
    cursor = map.scan(cursor, 1, Map::put, scanned);
    assertThat(scanned).hasSize(10);
    cursor = map.scan(cursor, 1, Map::put, scanned);
    assertThat(scanned).hasSize(10);

    assertThat(cursor).isEqualTo(0);
    assertThat(scanned).isEqualTo(map);
  }

  @Test
  public void scanWithHashcodeCollisionsAndConcurrentRemoves_ReturnsExpectedElements() {
    Integer2StringMap map = new Integer2StringMap(COLLIDING_HASH);
    IntStream.range(0, 10).forEach(i -> map.put(i, "value-" + i));

    Map<Integer, String> scanned = new HashMap<>();
    int cursor = map.scan(0, 5, Map::put, scanned);
    assertThat(scanned).hasSize(6);

    // Remove some of the elements
    map.remove(2);
    map.remove(4);
    map.remove(5);
    map.remove(7);

    cursor = map.scan(cursor, 5, Map::put, scanned);

    assertThat(cursor).isEqualTo(0);
    assertThat(scanned).containsKeys(0, 1, 3, 6, 8, 9);
  }

  @Test
  public void scanWithGrowingTable_DoesNotMissElements() {
    Integer2StringMap map = new Integer2StringMap();
    IntStream.range(0, 10).forEach(i -> map.put(i, "value-" + i));

    Map<Integer, String> scanned = new HashMap<>();
    int cursor = map.scan(0, 5, Map::put, scanned);
    assertThat(scanned).hasSize(5);


    // Add a lot of elements to trigger a resize
    IntStream.range(10, 500).forEach(i -> map.put(i, "value-" + i));

    cursor = map.scan(cursor, 500, Map::put, scanned);
    assertThat(cursor).isEqualTo(0);

    // We don't know that we will have all of the 500 new elements, only that
    // we should have scanned all of the original elements
    assertThat(scanned).containsKeys(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
  }

  @Test
  public void scanWithShrinkingTable_DoesNotMissElements() {
    Integer2StringMap map = new Integer2StringMap();
    IntStream.range(0, 500).forEach(i -> map.put(i, "value-" + i));

    Map<Integer, String> scanned = new HashMap<>();
    int cursor = map.scan(0, 50, Map::put, scanned);
    assertThat(scanned).hasSize(50);


    // Remove a lot of elements to trigger a resize
    IntStream.range(100, 500).forEach(map::remove);

    cursor = map.scan(cursor, 500, Map::put, scanned);
    assertThat(cursor).isEqualTo(0);

    // Scan should at least have all of the remaining keys
    assertThat(scanned).containsAllEntriesOf(map);
  }

  @Test
  public void revWorksWhenSignBitIsSet() {
    assertThat(SizeableObject2ObjectOpenCustomHashMapWithCursor.rev(0xFF000000)).isEqualTo(0xFF);
    assertThat(SizeableObject2ObjectOpenCustomHashMapWithCursor.rev(0xFF)).isEqualTo(0xFF000000);
  }

  // This test can be used to derive the formula for calculating overhead associated with resizing
  // the backing arrays of the map. If it fails, examine the output of this test and determine if
  // the constant or the formula needs to be adjusted. If all the assertions fail with a constant
  // difference between the expected and actual, adjust the constant. If they fail with inconsistent
  // differences, adjust the formula
  @Test
  public void backingArrayOverheadCalculationTest() {
    RedisHash.Hash map = new RedisHash.Hash(0);
    int arrayContentsOverhead = 0;
    SoftAssertions softly = new SoftAssertions();
    for (int i = 0; i < 250; ++i) {
      byte[] key = new byte[i];
      byte[] value = new byte[250 - i];
      map.put(key, value);
      arrayContentsOverhead += sizer.sizeof(key) + sizer.sizeof(value);

      // Calculate the overhead associated only with the backing array
      int backingArrayOverhead =
          sizer.sizeof(map) - arrayContentsOverhead - sizer.sizeof(ByteArrays.HASH_STRATEGY);
      int expected = map.calculateBackingArraysOverhead();
      softly.assertThat(backingArrayOverhead).isEqualTo(expected);
    }
    softly.assertAll();
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

  @Test
  public void calculateBackingArraysOverheadForDifferentInitialSizes() {
    for (int i = 0; i < 1000; ++i) {
      RedisHash.Hash hash = new RedisHash.Hash(i);
      assertThat(hash.calculateBackingArraysOverhead()).isEqualTo(expectedSize(hash));
    }
  }

  @Test
  public void calculateBackingArraysOverheadForDifferentLoadFactorsAndInitialSizes() {
    Random random = new Random(42);
    for (int i = 0; i < 1000; ++i) {
      float loadFactor = random.nextFloat();
      int initialSize = random.nextInt(1000);

      // Create a map with a random initial size and load factor
      RedisHash.Hash hash = new RedisHash.Hash(initialSize, loadFactor);

      // Confirm that the calculated value matches the actual value
      assertThat(((SizeableObject2ObjectOpenCustomHashMapWithCursor<byte[], byte[]>) hash)
          .calculateBackingArraysOverhead())
              .as("load factor = " + loadFactor + ", initial size = " + initialSize)
              .isEqualTo(expectedSize(hash));
    }
  }

  private int expectedSize(SizeableObject2ObjectOpenCustomHashMapWithCursor<byte[], ?> map) {
    return sizer.sizeof(map) - sizer.sizeof(ByteArrays.HASH_STRATEGY);
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
      int scoreDelta = sizeByteArray(scoreBytes)
          - sizeByteArray(oldScoreBytes);

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
