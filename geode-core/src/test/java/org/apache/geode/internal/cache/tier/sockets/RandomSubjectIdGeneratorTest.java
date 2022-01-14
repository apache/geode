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
package org.apache.geode.internal.cache.tier.sockets;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.mockito.quality.Strictness.STRICT_STUBS;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalLong;
import java.util.Random;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoSettings;

@MockitoSettings(strictness = STRICT_STUBS)
class RandomSubjectIdGeneratorTest {
  @Mock
  Random random;

  @Mock
  Consumer<Random> initializer;

  @Test
  void initializesRandomBeforeFirstCallToNextLong() {
    InOrder inOrder = inOrder(initializer, random);
    when(random.nextLong()).thenReturn(3L);
    doNothing().when(initializer).accept(any());

    SubjectIdGenerator generator = new RandomSubjectIdGenerator(random, initializer);

    generator.generateId();

    inOrder.verify(initializer).accept(same(random)); // Must happen before first call to nextLong()
    inOrder.verify(random).nextLong();
  }

  @Test
  void returnsNextRandomLong() {
    List<Long> randomLongs = Arrays.asList(234195L, 28L, -5324L);
    when(random.nextLong())
        .thenReturn(randomLongs.get(0))
        .thenReturn(randomLongs.get(1))
        .thenReturn(randomLongs.get(2));

    SubjectIdGenerator generator = new RandomSubjectIdGenerator(random, initializer);

    List<OptionalLong> generatedIds = new ArrayList<>();
    generatedIds.add(generator.generateId());
    generatedIds.add(generator.generateId());
    generatedIds.add(generator.generateId());

    List<OptionalLong> expectedIds = randomLongs.stream().map(OptionalLong::of).collect(toList());
    assertThat(generatedIds)
        .isEqualTo(expectedIds);
  }

  @Test
  void reportsIdExhaustionIfRandomRepeatsFirstLong() {
    long firstRandomLong = 9934;
    when(random.nextLong())
        .thenReturn(firstRandomLong)
        .thenReturn(firstRandomLong);

    SubjectIdGenerator generator = new RandomSubjectIdGenerator(random, initializer);

    generator.generateId(); // Remembers and returns first random long
    OptionalLong generatedId = generator.generateId(); // Random repeats first random long

    assertThat(generatedId)
        .isEmpty();// Generator reports ID exhaustion by returning an empty OptionalLong
  }

  @Test
  void reInitializesRandomAfterIdExhaustion() {
    InOrder inOrder = inOrder(initializer, random);
    long firstRandomLong = 9934L;
    when(random.nextLong())
        .thenReturn(firstRandomLong)
        .thenReturn(22L)
        .thenReturn(firstRandomLong)
        .thenReturn(128752L);
    doNothing().when(initializer).accept(any());

    SubjectIdGenerator generator = new RandomSubjectIdGenerator(random, initializer);

    generator.generateId(); // Remembers and returns first random long
    generator.generateId(); // Returns next random long
    generator.generateId(); // Triggers re-initialization because Random repeats first random long
    generator.generateId();

    inOrder.verify(initializer).accept(same(random)); // Must happen before first call to nextLong()
    inOrder.verify(random, times(3)).nextLong(); // Third call triggers re-initialization
    inOrder.verify(initializer).accept(same(random)); // Must happen before next call to nextLong()
    inOrder.verify(random).nextLong();
  }

  @Test
  void continuesWithReinitializedRandomAfterIdExhaustion() {
    long firstRandomLong = 9934L;
    List<Long> randomLongsAfterIdExhaustion = Arrays.asList(128752L, -98765L, -2368115L);
    when(random.nextLong())
        .thenReturn(firstRandomLong)
        .thenReturn(firstRandomLong)
        .thenReturn(randomLongsAfterIdExhaustion.get(0))
        .thenReturn(randomLongsAfterIdExhaustion.get(1))
        .thenReturn(randomLongsAfterIdExhaustion.get(2));
    doNothing().when(initializer).accept(any());

    SubjectIdGenerator generator = new RandomSubjectIdGenerator(random, initializer);

    List<OptionalLong> idsGeneratedAfterIdExhaustion = new ArrayList<>();
    generator.generateId(); // Remembers and returns first random long
    generator.generateId(); // Reports ID exhaustion because Random repeats first random long
    idsGeneratedAfterIdExhaustion.add(generator.generateId());
    idsGeneratedAfterIdExhaustion.add(generator.generateId());
    idsGeneratedAfterIdExhaustion.add(generator.generateId());

    List<OptionalLong> idsExpectedAfterExhaustion = randomLongsAfterIdExhaustion.stream()
        .map(OptionalLong::of)
        .collect(toList());
    assertThat(idsGeneratedAfterIdExhaustion)
        .isEqualTo(idsExpectedAfterExhaustion);
  }
}
