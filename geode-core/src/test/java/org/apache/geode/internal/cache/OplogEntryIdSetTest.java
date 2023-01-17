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
package org.apache.geode.internal.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.junit.Test;
import org.junitpioneer.jupiter.ClearSystemProperty;
import org.mockito.stubbing.Answer;

import org.apache.geode.internal.cache.DiskStoreImpl.OplogEntryIdSet;

/**
 * Tests DiskStoreImpl.OplogEntryIdSet
 */
public class OplogEntryIdSetTest {

  @Test
  public void testBasics() {
    OplogEntryIdSet s = new OplogEntryIdSet();

    LongStream.range(1, 777777).forEach(i -> assertThat(s.contains(i)).isFalse());
    LongStream.range(1, 777777).forEach(s::add);
    LongStream.range(1, 777777).forEach(i -> assertThat(s.contains(i)).isTrue());

    assertThatThrownBy(() -> s.add(DiskStoreImpl.INVALID_ID))
        .isInstanceOf(IllegalArgumentException.class);
    assertThat(s.contains(0)).isFalse();

    assertThat(s.contains(0x00000000FFFFFFFFL)).isFalse();
    s.add(0x00000000FFFFFFFFL);
    assertThat(s.contains(0x00000000FFFFFFFFL)).isTrue();

    LongStream.range(0x00000000FFFFFFFFL + 1, 0x00000000FFFFFFFFL + 777777)
        .forEach(i -> assertThat(s.contains(i)).isFalse());
    LongStream.range(0x00000000FFFFFFFFL + 1, 0x00000000FFFFFFFFL + 777777).forEach(s::add);
    LongStream.range(0x00000000FFFFFFFFL + 1, 0x00000000FFFFFFFFL + 777777)
        .forEach(i -> assertThat(s.contains(i)).isTrue());

    LongStream.range(1, 777777).forEach(i -> assertThat(s.contains(i)).isTrue());

    assertThat(s.contains(Long.MAX_VALUE)).isFalse();
    s.add(Long.MAX_VALUE);
    assertThat(s.contains(Long.MAX_VALUE)).isTrue();
    assertThat(s.contains(Long.MIN_VALUE)).isFalse();
    s.add(Long.MIN_VALUE);
    assertThat(s.contains(Long.MIN_VALUE)).isTrue();
  }

  @Test
  @ClearSystemProperty(key = "gemfire.disk.drfHashMapOverflowThreshold")
  public void addMethodOverflowsWhenInternalAddThrowsIllegalArgumentException() {
    int testEntries = 1000;
    int magicInt = testEntries + 1;
    long magicLong = 0x00000000FFFFFFFFL + testEntries + 1;

    Answer<Void> answer = invocationOnMock -> {
      Number value = invocationOnMock.getArgument(0);
      if ((value.intValue() == magicInt) || (value.longValue() == magicLong)) {
        throw new IllegalArgumentException(
            "Too large (XXXXXXXX expected elements with load factor Y.YY)");
      }
      invocationOnMock.callRealMethod();
      return null;
    };

    IntOpenHashSet intOpenHashSet = spy(IntOpenHashSet.class);
    doAnswer(answer).when(intOpenHashSet).add(anyInt());
    LongOpenHashSet longOpenHashSet = spy(LongOpenHashSet.class);
    doAnswer(answer).when(longOpenHashSet).add(anyLong());
    List<IntOpenHashSet> intOpenHashSets =
        new ArrayList<>(Collections.singletonList(intOpenHashSet));
    List<LongOpenHashSet> longOpenHashSets =
        new ArrayList<>(Collections.singletonList(longOpenHashSet));
    OplogEntryIdSet oplogEntryIdSet = new OplogEntryIdSet(intOpenHashSets, longOpenHashSets);

    // Insert some entries.
    assertThat(intOpenHashSets).hasSize(1);
    assertThat(longOpenHashSets).hasSize(1);
    IntStream.range(1, testEntries).forEach(oplogEntryIdSet::add);
    LongStream.range(0x00000000FFFFFFFFL + 1, 0x00000000FFFFFFFFL + testEntries)
        .forEach(oplogEntryIdSet::add);

    // Insert an entry that would cause an overflow for ints and longs.
    oplogEntryIdSet.add(magicInt);
    oplogEntryIdSet.add(magicLong);

    // Entries should exist and no exception should be thrown (even those that caused the exception)
    assertThat(intOpenHashSets).hasSize(2);
    assertThat(longOpenHashSets).hasSize(2);
    IntStream.range(1, testEntries).forEach(i -> assertThat(oplogEntryIdSet.contains(i)).isTrue());
    LongStream.range(0x00000000FFFFFFFFL + 1, 0x00000000FFFFFFFFL + testEntries)
        .forEach(i -> assertThat(oplogEntryIdSet.contains(i)).isTrue());
    assertThat(oplogEntryIdSet.contains(magicInt)).isTrue();
    assertThat(oplogEntryIdSet.contains(magicLong)).isTrue();
  }

  @Test
  public void sizeShouldIncludeOverflownSets() {
    int testEntries = 1000;
    List<IntOpenHashSet> intHashSets = new ArrayList<>();
    List<LongOpenHashSet> longHashSets = new ArrayList<>();

    IntStream.range(1, testEntries + 1).forEach(value -> {
      IntOpenHashSet intOpenHashSet = new IntOpenHashSet();
      intOpenHashSet.add(value);
      intHashSets.add(intOpenHashSet);
    });

    LongStream.range(0x00000000FFFFFFFFL + 1, 0x00000000FFFFFFFFL + testEntries + 1)
        .forEach(value -> {
          LongOpenHashSet longOpenHashSet = new LongOpenHashSet();
          longOpenHashSet.add(value);
          longHashSets.add(longOpenHashSet);
        });

    OplogEntryIdSet oplogEntryIdSet = new OplogEntryIdSet(intHashSets, longHashSets);
    assertThat(oplogEntryIdSet.size()).isEqualTo(testEntries * 2);
  }

  @Test
  public void containsShouldSearchAcrossOverflownSets() {
    int testEntries = 1000;
    List<IntOpenHashSet> intHashSets = new ArrayList<>();
    List<LongOpenHashSet> longHashSets = new ArrayList<>();

    IntStream.range(1, testEntries).forEach(value -> {
      IntOpenHashSet intOpenHashSet = new IntOpenHashSet();
      intOpenHashSet.add(value);
      intHashSets.add(intOpenHashSet);
    });

    LongStream.range(0x00000000FFFFFFFFL + 1, 0x00000000FFFFFFFFL + testEntries).forEach(value -> {
      LongOpenHashSet longOpenHashSet = new LongOpenHashSet();
      longOpenHashSet.add(value);
      longHashSets.add(longOpenHashSet);
    });

    OplogEntryIdSet oplogEntryIdSet = new OplogEntryIdSet(intHashSets, longHashSets);

    // All entries should be searchable across overflown sets
    IntStream.range(1, testEntries).forEach(i -> assertThat(oplogEntryIdSet.contains(i)).isTrue());
    LongStream.range(0x00000000FFFFFFFFL + 1, 0x00000000FFFFFFFFL + testEntries)
        .forEach(i -> assertThat(oplogEntryIdSet.contains(i)).isTrue());
  }
}
