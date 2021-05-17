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

import static org.apache.geode.redis.internal.collections.SizeableObjectOpenCustomHashSet.getElementSize;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import it.unimi.dsi.fastutil.bytes.ByteArrays;
import org.junit.Test;

import org.apache.geode.internal.size.ReflectionObjectSizer;

public class SizeableByteArrayOpenCustomHashSetTest {
  private final ReflectionObjectSizer sizer = ReflectionObjectSizer.getInstance();

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
    SizeableObjectOpenCustomHashSet<byte[]> set =
        new SizeableObjectOpenCustomHashSet<>(initialElements, ByteArrays.HASH_STRATEGY);
    assertThat(set.getSizeInBytes()).isEqualTo(sizer.sizeof(set));
    for (int i = initialNumberOfElements; i < initialNumberOfElements + elementsToAdd; ++i) {
      set.add(new byte[] {(byte) i});
      assertThat(set.getSizeInBytes()).isEqualTo(sizer.sizeof(set));
    }
    assertThat(set.size()).isEqualTo(initialNumberOfElements + elementsToAdd);
    for (int i = 0; i < initialNumberOfElements + elementsToAdd; ++i) {
      set.remove(new byte[] {(byte) i});
      assertThat(set.getSizeInBytes()).isEqualTo(sizer.sizeof(set));
    }
    assertThat(set.size()).isEqualTo(0);
  }
}
