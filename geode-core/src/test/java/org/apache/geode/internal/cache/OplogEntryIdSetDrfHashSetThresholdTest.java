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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetSystemProperty;

import org.apache.geode.internal.cache.DiskStoreImpl.OplogEntryIdSet;

/**
 * Tests DiskStoreImpl.OplogEntryIdSet
 */
public class OplogEntryIdSetDrfHashSetThresholdTest {
  @Test
  @Disabled
  //@SetSystemProperty(key = "gemfire.disk.drfHashMapOverflowThreshold", value = "10")
  public void addMethodOverflowBasedOnDrfOverflowThresholdParameters() {

    int testEntries = 41;
    IntOpenHashSet intOpenHashSet = new IntOpenHashSet();
    LongOpenHashSet longOpenHashSet = new LongOpenHashSet();

    List<IntOpenHashSet> intOpenHashSets =
        new ArrayList<>(Collections.singletonList(intOpenHashSet));
    List<LongOpenHashSet> longOpenHashSets =
        new ArrayList<>(Collections.singletonList(longOpenHashSet));

    OplogEntryIdSet oplogEntryIdSet = new OplogEntryIdSet(intOpenHashSets, longOpenHashSets);
    IntStream.range(1, testEntries).forEach(oplogEntryIdSet::add);
    LongStream.range(0x00000000FFFFFFFFL + 1, 0x00000000FFFFFFFFL + testEntries)
        .forEach(oplogEntryIdSet::add);

    assertThat(intOpenHashSets).hasSize(4);
    assertThat(longOpenHashSets).hasSize(4);

    IntStream.range(1, testEntries).forEach(i -> assertThat(oplogEntryIdSet.contains(i)).isTrue());
    LongStream.range(0x00000000FFFFFFFFL + 1, 0x00000000FFFFFFFFL + testEntries)
        .forEach(i -> assertThat(oplogEntryIdSet.contains(i)).isTrue());

  }
}
