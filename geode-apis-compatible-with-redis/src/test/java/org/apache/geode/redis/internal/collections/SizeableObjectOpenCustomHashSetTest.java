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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import it.unimi.dsi.fastutil.bytes.ByteArrays;
import org.assertj.core.api.SoftAssertions;
import org.junit.Test;

import org.apache.geode.cache.util.ObjectSizer;
import org.apache.geode.internal.size.ReflectionObjectSizer;
import org.apache.geode.internal.size.ReflectionSingleObjectSizer;
import org.apache.geode.internal.size.SingleObjectSizer;
import org.apache.geode.redis.internal.data.RedisSet;

public class SizeableObjectOpenCustomHashSetTest {
  private final ObjectSizer sizer = ReflectionObjectSizer.getInstance();
  private final SingleObjectSizer elementSizer = new ReflectionSingleObjectSizer();

  // This test can be used to derive the formula for calculating overhead associated with resizing
  // the backing array of the set. If it fails examine the output of this test and determine if the
  // constant or the formula needs to be adjusted. If all the assertions fail with a constant
  // difference between the expected and actual, adjust the constant. If they fail with inconsistent
  // differences, adjust the formula
  @Test
  public void backingArrayOverheadCalculationTest() {
    RedisSet.MemberSet set = new RedisSet.MemberSet(0);
    int backingArrayOverhead;
    int memberOverhead = 0;
    SoftAssertions softly = new SoftAssertions();
    for (int i = 0; i < 250; ++i) {
      byte[] element = new byte[i];
      set.add(element);
      memberOverhead += sizer.sizeof(element);
      backingArrayOverhead = expectedSize(set) - memberOverhead;
      int expected = set.calculateBackingArrayOverhead();
      softly.assertThat(backingArrayOverhead).isEqualTo(expected);
    }
    softly.assertAll();
  }

  private static int getMemberOverhead(SizeableObjectOpenCustomHashSet<byte[]> set) {
    return set.getMemberOverhead();
  }

  @Test
  public void addIncreasesMemberOverheadByCorrectAmount() {
    RedisSet.MemberSet set = new RedisSet.MemberSet();
    int initialSize = getMemberOverhead(set);
    List<byte[]> members = new ArrayList<>();
    for (int i = 0; i < 100; ++i) {
      members.add(new byte[i]);
    }

    // Add a duplicate member to check that member overhead isn't increased if a member isn't added
    // to the set
    members.add(new byte[100]);

    for (byte[] bytes : members) {
      boolean added = set.add(bytes);
      if (added) {
        long expectedOverhead = elementSizer.sizeof(bytes);
        assertThat(expectedOverhead).isEqualTo(getMemberOverhead(set) - initialSize);
        initialSize = getMemberOverhead(set);
      } else {
        assertThat(getMemberOverhead(set) - initialSize).isZero();
      }
    }
  }

  @Test
  public void removeDecreasesMemberOverheadByCorrectAmount() {
    RedisSet.MemberSet set = new RedisSet.MemberSet();
    List<byte[]> members = new ArrayList<>();
    for (int i = 0; i < 100; ++i) {
      members.add(new byte[i]);
    }
    set.addAll(members);

    // Add a byte to the list that isn't present in the set to ensure that member overhead isn't
    // decreased when a member isn't actually removed
    members.add(new byte[101]);

    int initialSize = getMemberOverhead(set);

    for (byte[] bytes : members) {
      boolean removed = set.remove(bytes);
      if (removed) {
        long expectedOverhead = elementSizer.sizeof(bytes);
        assertThat(expectedOverhead).isEqualTo(initialSize - getMemberOverhead(set));
        initialSize = getMemberOverhead(set);
      } else {
        assertThat(getMemberOverhead(set) - initialSize).isZero();
      }
    }
  }

  @Test
  public void calculateBackingArrayOverheadForDifferentInitialSizes() {
    for (int i = 0; i < 1000; ++i) {
      RedisSet.MemberSet set = new RedisSet.MemberSet(i);
      assertThat(set.calculateBackingArrayOverhead()).isEqualTo(expectedSize(set));
    }
  }

  @Test
  public void calculateBackingArrayOverheadForDifferentLoadFactorsAndInitialSizes() {
    Random random = new Random(42);
    for (int i = 0; i < 1000; ++i) {
      float loadFactor = random.nextFloat();
      int initialSize = random.nextInt(1000);
      RedisSet.MemberSet set = new RedisSet.MemberSet(initialSize, loadFactor);
      assertThat(set.calculateBackingArrayOverhead())
          .as("load factor = " + loadFactor + ", initial size = " + initialSize)
          .isEqualTo(expectedSize(set));
    }
  }

  private int expectedSize(RedisSet.MemberSet set) {
    return sizer.sizeof(set) - sizer.sizeof(ByteArrays.HASH_STRATEGY);
  }

  @Test
  public void getSizeInBytesIsAccurateForByteArrays() {
    List<byte[]> initialElements = new ArrayList<>();
    int initialNumberOfElements = 20;
    int elementsToAdd = 100;
    for (int i = 0; i < initialNumberOfElements; ++i) {
      initialElements.add(new byte[] {(byte) i});
    }
    // Create a set with an initial size and confirm that it correctly reports its size
    RedisSet.MemberSet set = new RedisSet.MemberSet(initialElements);
    assertThat(set.getSizeInBytes()).isEqualTo(expectedSize(set));

    // Add enough members to force a resize and assert that the size is correct after each add
    for (int i = initialNumberOfElements; i < initialNumberOfElements + elementsToAdd; ++i) {
      set.add(new byte[] {(byte) i});
      assertThat(set.getSizeInBytes()).isEqualTo(expectedSize(set));
    }
    assertThat(set.size()).isEqualTo(initialNumberOfElements + elementsToAdd);

    // Remove all the members and assert that the size is correct after each remove
    for (int i = 0; i < initialNumberOfElements + elementsToAdd; ++i) {
      set.remove(new byte[] {(byte) i});
      assertThat(set.getSizeInBytes()).isEqualTo(expectedSize(set));
    }
    assertThat(set.size()).isEqualTo(0);
  }
}
