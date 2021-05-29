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

import static java.util.Comparator.comparingLong;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static org.apache.geode.redis.internal.collections.SizeableObjectOpenCustomHashSet.BACKING_ARRAY_LENGTH_COEFFICIENT;
import static org.apache.geode.redis.internal.collections.SizeableObjectOpenCustomHashSet.BACKING_ARRAY_OVERHEAD_CONSTANT;
import static org.apache.geode.redis.internal.collections.SizeableObjectOpenCustomHashSet.MEMBER_OVERHEAD_CONSTANT;
import static org.apache.geode.redis.internal.collections.SizeableObjectOpenCustomHashSet.getElementSize;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;

import it.unimi.dsi.fastutil.bytes.ByteArrays;
import org.assertj.core.api.SoftAssertions;
import org.junit.Test;

import org.apache.geode.internal.size.ReflectionObjectSizer;

public class SizeableObjectOpenCustomHashSetTest {
  private final ReflectionObjectSizer sizer = ReflectionObjectSizer.getInstance();

  // This test can be used to derive the formula for calculating per member overhead for varying
  // byte[] lengths. If it fails, examine the output and determine if the constant or the formula
  // needs to be adjusted. If all the assertions fail with a constant difference between the
  // expected and actual, adjust the constant. If only some fail, or they fail with inconsistent
  // differences, adjust the formula
  @Test
  public void memberOverheadCalculationTest() {
    SoftAssertions softly = new SoftAssertions();
    for (int length = 1; length < 100; ++length) {
      SizeableObjectOpenCustomHashSet<byte[]> set =
          new SizeableObjectOpenCustomHashSet<>(ByteArrays.HASH_STRATEGY);
      int initialSize = sizer.sizeof(set);
      int updatedSize;
      List<Integer> sizeDifferences = new ArrayList<>();
      for (int i = 0; i < Byte.MAX_VALUE; ++i) {
        // Add multiple members of array length 'size' to the set and measure the change in memory
        // overhead associated with the set for each addition
        byte[] bytes = new byte[length];
        bytes[0] = (byte) i;
        set.add(bytes);
        updatedSize = sizer.sizeof(set);
        sizeDifferences.add(updatedSize - initialSize);
        initialSize = updatedSize;
      }

      // Collect size differences into a map of {size difference, number of times seen}
      Map<Integer, Long> sizeChangeFrequency = sizeDifferences.stream()
          .collect(groupingBy(Function.identity(), counting()));

      // Find the most frequently occurring overhead value. Some overhead values will include
      // additional overhead associated with resizing the backing set, but the most frequently
      // occurring value will not
      int perMemberOverhead = Collections
          .max(sizeChangeFrequency.entrySet(), comparingLong(Map.Entry::getValue)).getKey();
      softly.assertThat(perMemberOverhead)
          .as("Expecting per member overhead to = "
              + "base member overhead + (array length rounded up to multiple of 8) = "
              + MEMBER_OVERHEAD_CONSTANT + " + (" + length + " rounded up to multiple of 8)")
          .isEqualTo(MEMBER_OVERHEAD_CONSTANT + ((length + 7) / 8) * 8);
    }
    softly.assertAll();
  }

  // This test can be used to derive the formula for calculating overhead associated with resizing
  // the backing array of the set. If it fails, first ensure that memberOverheadCalculationTest() is
  // passing, as this test depends on the formula derived from that test being correct. If that test
  // is passing, examine the output of this test and determine if the constant or the formula needs
  // to be adjusted. If all the assertions fail with a constant difference between the expected and
  // actual, adjust the constant. If they fail with inconsistent differences, adjust the formula
  @Test
  public void backingArrayOverheadCalculationTest() {
    SizeableObjectOpenCustomHashSet<byte[]> set =
        new SizeableObjectOpenCustomHashSet<>(0, ByteArrays.HASH_STRATEGY);
    int backingArrayOverhead;
    SoftAssertions softly = new SoftAssertions();
    for (int i = 0; i < 250; ++i) {
      set.add(new byte[i]);

      // Calculate the overhead associated only with the backing array
      backingArrayOverhead = sizer.sizeof(set) - set.getMemberOverhead();
      softly.assertThat(backingArrayOverhead)
          .as("Expecting backing array overhead to = "
              + "backing array constant + (backing array length coefficient * backing array length)"
              + " = " + BACKING_ARRAY_OVERHEAD_CONSTANT + " + (" + BACKING_ARRAY_LENGTH_COEFFICIENT
              + " * " + set.getBackingArrayLength() + ")")
          .isEqualTo(BACKING_ARRAY_OVERHEAD_CONSTANT
              + BACKING_ARRAY_LENGTH_COEFFICIENT * set.getBackingArrayLength());
    }
    softly.assertAll();
  }

  @Test
  public void addIncreasesMemberOverheadByCorrectAmount() {
    SizeableObjectOpenCustomHashSet<byte[]> set =
        new SizeableObjectOpenCustomHashSet<>(ByteArrays.HASH_STRATEGY);
    int initialSize = set.getMemberOverhead();
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
        int expectedOverhead = getElementSize(bytes);
        assertThat(expectedOverhead).isEqualTo(set.getMemberOverhead() - initialSize);
        initialSize = set.getMemberOverhead();
      } else {
        assertThat(set.getMemberOverhead() - initialSize).isZero();
      }
    }
  }

  @Test
  public void removeDecreasesMemberOverheadByCorrectAmount() {
    SizeableObjectOpenCustomHashSet<byte[]> set =
        new SizeableObjectOpenCustomHashSet<>(ByteArrays.HASH_STRATEGY);
    List<byte[]> members = new ArrayList<>();
    for (int i = 0; i < 100; ++i) {
      members.add(new byte[i]);
    }
    set.addAll(members);

    // Add a byte to the list that isn't present in the set to ensure that member overhead isn't
    // decreased when a member isn't actually removed
    members.add(new byte[101]);

    int initialSize = set.getMemberOverhead();

    for (byte[] bytes : members) {
      boolean removed = set.remove(bytes);
      if (removed) {
        int expectedOverhead = getElementSize(bytes);
        assertThat(expectedOverhead).isEqualTo(initialSize - set.getMemberOverhead());
        initialSize = set.getMemberOverhead();
      } else {
        assertThat(set.getMemberOverhead() - initialSize).isZero();
      }
    }
  }

  @Test
  public void calculateBackingArrayOverheadForDifferentInitialSizes() {
    for (int i = 0; i < 1000; ++i) {
      SizeableObjectOpenCustomHashSet<byte[]> set =
          new SizeableObjectOpenCustomHashSet<>(i, ByteArrays.HASH_STRATEGY);
      assertThat(set.calculateBackingArrayOverhead()).isEqualTo(sizer.sizeof(set));
    }
  }

  @Test
  public void calculateBackingArrayOverheadForDifferentLoadFactorsAndInitialSizes() {
    Random random = new Random(42);
    for (int i = 0; i < 1000; ++i) {
      float loadFactor = random.nextFloat();
      int initialSize = random.nextInt(1000);
      SizeableObjectOpenCustomHashSet<byte[]> set =
          new SizeableObjectOpenCustomHashSet<>(initialSize, loadFactor, ByteArrays.HASH_STRATEGY);
      assertThat(set.calculateBackingArrayOverhead())
          .as("load factor = " + loadFactor + ", initial size = " + initialSize)
          .isEqualTo(sizer.sizeof(set));
    }
  }

  @Test
  public void getElementSizeForPrimitiveArrays() {
    for (int i = 0; i < 100; ++i) {
      byte[] bytes = new byte[i];
      short[] shorts = new short[i];
      int[] ints = new int[i];
      long[] longs = new long[i];
      float[] floats = new float[i];
      double[] doubles = new double[i];
      char[] chars = new char[i];

      assertThat(getElementSize(bytes)).as("byte[" + i + "]").isEqualTo(sizer.sizeof(bytes));
      assertThat(getElementSize(shorts)).as("short[" + i + "]").isEqualTo(sizer.sizeof(shorts));
      assertThat(getElementSize(ints)).as("int[" + i + "]").isEqualTo(sizer.sizeof(ints));
      assertThat(getElementSize(longs)).as("long[" + i + "]").isEqualTo(sizer.sizeof(longs));
      assertThat(getElementSize(floats)).as("float[" + i + "]").isEqualTo(sizer.sizeof(floats));
      assertThat(getElementSize(doubles)).as("double[" + i + "]").isEqualTo(sizer.sizeof(doubles));
      assertThat(getElementSize(chars)).as("char[" + i + "]").isEqualTo(sizer.sizeof(chars));
    }
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
    SizeableObjectOpenCustomHashSet<byte[]> set =
        new SizeableObjectOpenCustomHashSet<>(initialElements, ByteArrays.HASH_STRATEGY);
    assertThat(set.getSizeInBytes()).isEqualTo(sizer.sizeof(set));

    // Add enough members to force a resize and assert that the size is correct after each add
    for (int i = initialNumberOfElements; i < initialNumberOfElements + elementsToAdd; ++i) {
      set.add(new byte[] {(byte) i});
      assertThat(set.getSizeInBytes()).isEqualTo(sizer.sizeof(set));
    }
    assertThat(set.size()).isEqualTo(initialNumberOfElements + elementsToAdd);

    // Remove all the members and assert that the size is correct after each remove
    for (int i = 0; i < initialNumberOfElements + elementsToAdd; ++i) {
      set.remove(new byte[] {(byte) i});
      assertThat(set.getSizeInBytes()).isEqualTo(sizer.sizeof(set));
    }
    assertThat(set.size()).isEqualTo(0);
  }
}
