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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import it.unimi.dsi.fastutil.bytes.ByteArrays;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.junit.Test;

import org.apache.geode.cache.util.ObjectSizer;
import org.apache.geode.internal.size.ReflectionObjectSizer;
import org.apache.geode.redis.internal.data.RedisSet;

public class SizeableObjectOpenCustomHashSetWithCursorTest {
  private static final int INITIAL_SIZE = 20;
  private static final int SIZE_OF_NEW_ELEMENTS_TO_ADD = 100;
  private final ObjectSizer sizer = ReflectionObjectSizer.getInstance();

  private int expectedSize(RedisSet.MemberSet set) {
    return sizer.sizeof(set) - sizer.sizeof(ByteArrays.HASH_STRATEGY);
  }


  private static class ByteSet
      extends SizeableObjectOpenCustomHashSetWithCursor<byte[]> {
    public ByteSet() {
      super(ByteArrays.HASH_STRATEGY);
    }

    public ByteSet(int initialSize) {
      super(initialSize, ByteArrays.HASH_STRATEGY);
    }

    @Override
    protected int sizeElement(byte[] element) {
      return 0;
    }
  }


  private List<byte[]> initializeMemberSet() {
    List<byte[]> initialElements = new ArrayList<>();
    for (int i = 0; i < INITIAL_SIZE; ++i) {
      initialElements.add(new byte[] {(byte) i});
    }
    return initialElements;
  }

  @Test
  public void getSizeInBytesIsAccurateForByteArrays() {
    RedisSet.MemberSet set = new RedisSet.MemberSet(initializeMemberSet());
    assertThat(set.getSizeInBytes()).isEqualTo(expectedSize(set));
  }

  @Test
  public void addMoreMembers_assertSetSizeIsAccurate() {
    RedisSet.MemberSet set = new RedisSet.MemberSet(initializeMemberSet());
    for (int i = INITIAL_SIZE; i < INITIAL_SIZE + SIZE_OF_NEW_ELEMENTS_TO_ADD; ++i) {
      set.add(new byte[] {(byte) i});
      assertThat(set.getSizeInBytes()).isEqualTo(expectedSize(set));
    }
    assertThat(set.size()).isEqualTo(INITIAL_SIZE + SIZE_OF_NEW_ELEMENTS_TO_ADD);
  }

  @Test
  public void removeAllMembers_assertSetSizeIsAccurate() {
    RedisSet.MemberSet set = new RedisSet.MemberSet(initializeMemberSet());
    for (int i = 0; i < INITIAL_SIZE + SIZE_OF_NEW_ELEMENTS_TO_ADD; ++i) {
      set.remove(new byte[] {(byte) i});
      assertThat(set.getSizeInBytes()).isEqualTo(expectedSize(set));
    }
    assertThat(set.size()).isEqualTo(0);
  }

  @Test
  public void revWorksWhenSignBitIsSet() {
    assertThat(SizeableObjectOpenCustomHashSetWithCursor.rev(0xFF000000)).isEqualTo(0xFF);
    assertThat(SizeableObjectOpenCustomHashSetWithCursor.rev(0xFF)).isEqualTo(0xFF000000);
  }

  @Test
  public void scanEntireSet_ReturnsExpectedElements() {
    ByteSet set = new ByteSet();
    IntStream.range(0, 10).forEach(i -> set.add(makeKey(i)));

    List<byte[]> scanned = new ArrayList<>();
    int result = set.scan(0, 10000, List::add, scanned);
    assertThat(result).isZero();
    assertThat(scanned).containsExactlyInAnyOrderElementsOf(set);
  }

  private static byte[] makeKey(Integer i) {
    return i.toString().getBytes();
  }

  private void fillSetWithUniqueHashKeys(ByteSet set, int keysToAdd) {
    int keyCounter = 0;
    Set<Integer> hashesAdded = new HashSet<>();
    while (keysToAdd > 0) {
      byte[] key = makeKey(keyCounter);
      int keyHash = set.hash(key);
      if (!hashesAdded.contains(keyHash)) {
        hashesAdded.add(keyHash);
        set.add(key);
        keysToAdd--;
      }
      keyCounter++;
    }
  }

  private void fillSetWithCollidingHashKeys(ByteSet set, int keysToAdd) {
    int keyCounter = 0;
    Set<Integer> hashesAdded = new HashSet<>();
    while (keysToAdd > 0) {
      byte[] key = makeKey(keyCounter);
      int keyHash = set.hash(key);
      if (hashesAdded.isEmpty() || hashesAdded.contains(keyHash)) {
        hashesAdded.add(keyHash);
        set.add(key);
        keysToAdd--;
      }
      keyCounter++;
    }
  }

  @Test
  public void twoScansWithNoModifications_ReturnsExpectedElements() {
    final int SET_SIZE = 10;
    ByteSet set = new ByteSet(SET_SIZE * 2); // *2 to prevent rehash
    fillSetWithUniqueHashKeys(set, SET_SIZE);
    List<byte[]> scanned = new ArrayList<>();
    int scanSize = 1 + set.size() / 2;
    // Scan part way through the set
    int cursor = set.scan(0, scanSize, List::add, scanned);
    assertThat(scanned).hasSize(scanSize);

    // Scan past the end of the set
    cursor = set.scan(cursor, scanSize, List::add, scanned);
    assertThat(scanned).hasSize(set.size());
    assertThat(cursor).isEqualTo(0);

    assertThat(scanned).containsExactlyInAnyOrderElementsOf(set);
  }

  @Test
  public void scanWithConcurrentRemoves_ReturnsExpectedElements() {
    final int SET_SIZE = 10;
    ByteSet set = new ByteSet(
        SET_SIZE * 2); // *2 to prevent rehash
    fillSetWithUniqueHashKeys(set, SET_SIZE);
    List<byte[]> scanned = new ArrayList<>();
    int cursor = set.scan(0, SET_SIZE / 2, List::add, scanned);
    assertThat(scanned).hasSize(SET_SIZE / 2);

    // Remove some elements
    ObjectIterator<byte[]> iterator = set.iterator();
    int removeCount = SET_SIZE / 2 - 1;
    while (removeCount > 0 && iterator.hasNext()) {
      iterator.next();
      iterator.remove();
      removeCount--;
    }

    cursor = set.scan(cursor, SET_SIZE / 2, List::add, scanned);
    assertThat(cursor).isZero();

    assertThat(scanned).containsAll(set);
  }

  @Test
  public void scanWithHashcodeCollisions_ReturnsExpectedElements() {
    final int SET_SIZE = 10;
    ByteSet set = new ByteSet(
        SET_SIZE * 2); // *2 to prevent rehash
    fillSetWithCollidingHashKeys(set, SET_SIZE);
    List<byte[]> scanned = new ArrayList<>();
    int cursor = set.scan(0, 1, List::add, scanned);

    // The scan had to ignore the count and return all the elements with the same hash
    assertThat(scanned).hasSize(SET_SIZE);
    assertThat(scanned).containsExactlyInAnyOrderElementsOf(set);
    cursor = set.scan(cursor, 1, List::add, scanned);
    assertThat(cursor).isZero();
    assertThat(scanned).hasSize(SET_SIZE);
    assertThat(scanned).containsExactlyInAnyOrderElementsOf(set);
  }

  @Test
  public void scanWithHashcodeCollisionsAndConcurrentRemoves_ReturnsExpectedElements() {
    final int SET_SIZE = 10;
    ByteSet set = new ByteSet(
        SET_SIZE * 2); // *2 to prevent rehash
    fillSetWithCollidingHashKeys(set, SET_SIZE);
    List<byte[]> scanned = new ArrayList<>();

    int cursor = set.scan(0, SET_SIZE / 2, List::add, scanned);
    assertThat(scanned).hasSize(SET_SIZE);

    // Remove some elements
    ObjectIterator<byte[]> iterator = set.iterator();
    int removeCount = SET_SIZE / 2 - 1;
    while (removeCount > 0 && iterator.hasNext()) {
      iterator.next();
      iterator.remove();
      removeCount--;
    }

    cursor = set.scan(cursor, SET_SIZE / 2, List::add, scanned);

    assertThat(cursor).isZero();
    assertThat(scanned).hasSize(SET_SIZE);
  }

  @Test
  public void scanWithGrowingTable_DoesNotMissElements() {
    final int SET_SIZE = 10;
    ByteSet set =
        new ByteSet(SET_SIZE * 2); // *2 to prevent rehash
    fillSetWithUniqueHashKeys(set, SET_SIZE);
    List<byte[]> scanned = new ArrayList<>();
    List<byte[]> initialKeys = new ArrayList<>(set.size());
    for (byte[] element : set) {
      initialKeys.add(element);
    }

    int cursor = set.scan(0, SET_SIZE / 2, List::add, scanned);
    assertThat(scanned).hasSize(SET_SIZE / 2);

    // Add a lot of elements to trigger a resize
    IntStream.range(10, 500).forEach(i -> set.add(makeKey(i)));

    cursor = set.scan(cursor, 500, List::add, scanned);
    assertThat(cursor).isEqualTo(0);

    // We don't know that we will have all the 500 new elements, only that
    // we should have scanned all the original elements
    assertThat(scanned).containsAll(initialKeys);
  }

  @Test
  public void scanWithShrinkingTable_DoesNotMissElements() {
    final int SET_SIZE = 500;
    ByteSet set = new ByteSet(SET_SIZE * 2); // *2 to prevent rehash
    fillSetWithUniqueHashKeys(set, SET_SIZE);
    List<byte[]> scanned = new ArrayList<>();
    int cursor = set.scan(0, 50, List::add, scanned);
    assertThat(scanned).hasSize(50);

    // Remove a lot of elements to trigger a resize
    // Remove some elements
    ObjectIterator<byte[]> iterator = set.iterator();
    int removeCount = SET_SIZE - 100;
    while (removeCount > 0 && iterator.hasNext()) {
      iterator.next();
      iterator.remove();
      removeCount--;
    }

    cursor = set.scan(cursor, SET_SIZE, List::add, scanned);
    assertThat(cursor).isZero();

    // Scan should at least have all the remaining keys
    assertThat(scanned).containsAll(set);
  }

}
