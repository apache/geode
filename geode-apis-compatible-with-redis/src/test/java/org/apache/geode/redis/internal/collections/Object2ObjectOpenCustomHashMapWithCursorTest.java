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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.stream.IntStream;

import it.unimi.dsi.fastutil.Hash;
import org.junit.Test;

public class Object2ObjectOpenCustomHashMapWithCursorTest {
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

  @Test
  public void scanEntireMap_ReturnsExpectedElements() {
    Object2ObjectOpenCustomHashMapWithCursor<Integer, String> map =
        new Object2ObjectOpenCustomHashMapWithCursor<>(NATURAL_HASH);
    IntStream.range(0, 10).forEach(i -> map.put(i, "value-" + i));

    HashMap<Integer, String> scanned = new HashMap<>();
    int result = map.scan(0, 10000, HashMap::put, scanned);
    assertThat(result).isEqualTo(0);
    assertThat(scanned).isEqualTo(map);
  }

  @Test
  public void twoScansWithNoModifications_ReturnsExpectedElements() {
    Object2ObjectOpenCustomHashMapWithCursor<Integer, String> map =
        new Object2ObjectOpenCustomHashMapWithCursor<>(NATURAL_HASH);
    IntStream.range(0, 10).forEach(i -> map.put(i, "value-" + i));

    HashMap<Integer, String> scanned = new HashMap<>();
    int cursor = map.scan(0, 3, HashMap::put, scanned);
    assertThat(scanned).hasSize(3);
    cursor = map.scan(cursor, 3, HashMap::put, scanned);
    assertThat(scanned).hasSize(6);
    cursor = map.scan(cursor, 4, HashMap::put, scanned);
    assertThat(scanned).hasSize(10);
    cursor = map.scan(cursor, 4, HashMap::put, scanned);
    assertThat(scanned).hasSize(10);
    assertThat(cursor).isEqualTo(0);

    assertThat(scanned).isEqualTo(map);
  }

  @Test
  public void scanWithConcurrentRemoves_ReturnsExpectedElements() {
    Object2ObjectOpenCustomHashMapWithCursor<Integer, String> map =
        new Object2ObjectOpenCustomHashMapWithCursor<>(NATURAL_HASH);
    IntStream.range(0, 10).forEach(i -> map.put(i, "value-" + i));

    HashMap<Integer, String> scanned = new HashMap<>();
    int cursor = map.scan(0, 5, HashMap::put, scanned);
    assertThat(scanned).hasSize(5);

    // Remove some of the elements
    map.remove(2);
    map.remove(4);
    map.remove(5);
    map.remove(7);

    cursor = map.scan(cursor, 5, HashMap::put, scanned);
    assertThat(cursor).isEqualTo(0);

    assertThat(scanned).containsKeys(0, 1, 3, 6, 8, 9);
  }

  @Test
  public void scanWithHashcodeCollisions_ReturnsExpectedElements() {
    Object2ObjectOpenCustomHashMapWithCursor<Integer, String> map =
        new Object2ObjectOpenCustomHashMapWithCursor<>(COLLIDING_HASH);
    IntStream.range(0, 10).forEach(i -> map.put(i, "value-" + i));

    // The colliding hash is just key % 5. So 0 and 5 will have the same hashcode, etc.
    HashMap<Integer, String> scanned = new HashMap<>();
    int cursor = map.scan(0, 1, HashMap::put, scanned);

    // The scan had to ignore the count and return all of the elements with the same hash
    assertThat(scanned).hasSize(2);

    cursor = map.scan(cursor, 1, HashMap::put, scanned);
    assertThat(scanned).hasSize(4);
    cursor = map.scan(cursor, 1, HashMap::put, scanned);
    assertThat(scanned).hasSize(6);
    cursor = map.scan(cursor, 1, HashMap::put, scanned);
    assertThat(scanned).hasSize(8);
    cursor = map.scan(cursor, 1, HashMap::put, scanned);
    assertThat(scanned).hasSize(10);
    cursor = map.scan(cursor, 1, HashMap::put, scanned);
    assertThat(scanned).hasSize(10);

    assertThat(cursor).isEqualTo(0);
    assertThat(scanned).isEqualTo(map);
  }

  @Test
  public void scanWithHashcodeCollisionsAndConcurrentRemoves_ReturnsExpectedElements() {
    Object2ObjectOpenCustomHashMapWithCursor<Integer, String> map =
        new Object2ObjectOpenCustomHashMapWithCursor<>(COLLIDING_HASH);
    IntStream.range(0, 10).forEach(i -> map.put(i, "value-" + i));

    HashMap<Integer, String> scanned = new HashMap<>();
    int cursor = map.scan(0, 5, HashMap::put, scanned);
    assertThat(scanned).hasSize(6);

    // Remove some of the elements
    map.remove(2);
    map.remove(4);
    map.remove(5);
    map.remove(7);

    cursor = map.scan(cursor, 5, HashMap::put, scanned);

    assertThat(cursor).isEqualTo(0);
    assertThat(scanned).containsKeys(0, 1, 3, 6, 8, 9);
  }

  @Test
  public void scanWithGrowingTable_DoesNotMissElements() {
    Object2ObjectOpenCustomHashMapWithCursor<Integer, String> map =
        new Object2ObjectOpenCustomHashMapWithCursor<>(NATURAL_HASH);
    IntStream.range(0, 10).forEach(i -> map.put(i, "value-" + i));

    HashMap<Integer, String> scanned = new HashMap<>();
    int cursor = map.scan(0, 5, HashMap::put, scanned);
    assertThat(scanned).hasSize(5);


    // Add a lot of elements to trigger a resize
    IntStream.range(10, 500).forEach(i -> map.put(i, "value-" + i));

    cursor = map.scan(cursor, 500, HashMap::put, scanned);
    assertThat(cursor).isEqualTo(0);

    // We don't know that we will have all of the 500 new elements, only that
    // we should have scanned all of the original elements
    assertThat(scanned).containsKeys(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
  }

  @Test
  public void scanWithShrinkingTable_DoesNotMissElements() {
    Object2ObjectOpenCustomHashMapWithCursor<Integer, String> map =
        new Object2ObjectOpenCustomHashMapWithCursor<>(NATURAL_HASH);
    IntStream.range(0, 500).forEach(i -> map.put(i, "value-" + i));

    HashMap<Integer, String> scanned = new HashMap<>();
    int cursor = map.scan(0, 50, HashMap::put, scanned);
    assertThat(scanned).hasSize(50);


    // Remove a lot of elements to trigger a resize
    IntStream.range(100, 500).forEach(map::remove);

    cursor = map.scan(cursor, 500, HashMap::put, scanned);
    assertThat(cursor).isEqualTo(0);

    // Scan should at least have all of the remaining keys
    assertThat(scanned).containsAllEntriesOf(map);
  }

  @Test
  public void revWorksWhenSignBitIsSet() {
    assertThat(Object2ObjectOpenCustomHashMapWithCursor.rev(0xFF000000)).isEqualTo(0xFF);
    assertThat(Object2ObjectOpenCustomHashMapWithCursor.rev(0xFF)).isEqualTo(0xFF000000);
  }


}
