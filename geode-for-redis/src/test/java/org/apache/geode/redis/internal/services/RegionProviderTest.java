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
package org.apache.geode.redis.internal.services;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.apache.geode.redis.internal.data.RedisKey;

public class RegionProviderTest {

  @Test
  public void areKeysCrossSlotsReturnsFalseWhenKeysAreSameSlot() {
    RedisKey key1 = mock(RedisKey.class);
    int slot1 = 1;
    when(key1.getSlot()).thenReturn(slot1);
    RedisKey key2 = mock(RedisKey.class);
    when(key2.getSlot()).thenReturn(slot1);

    List<RedisKey> keyList = Arrays.asList(key1, key2);

    assertThat(RegionProvider.areKeysCrossSlots(keyList)).isFalse();
  }

  @Test
  public void areKeysCrossSlotsReturnsTrueWhenKeysAreCrossSlots() {
    RedisKey key1 = mock(RedisKey.class);
    int slot1 = 1;
    when(key1.getSlot()).thenReturn(slot1);
    RedisKey key2 = mock(RedisKey.class);
    int slot2 = 2;
    when(key2.getSlot()).thenReturn(slot2);

    List<RedisKey> keyList = Arrays.asList(key1, key2);

    assertThat(RegionProvider.areKeysCrossSlots(keyList)).isTrue();
  }

  @Test
  public void areKeysCrossSlotsReturnsTrueWhenKeysAreCrossSlotsForManyKeys() {
    List<RedisKey> keyList = new ArrayList<>();
    for (int i = 0; i < 100; ++i) {
      RedisKey key = mock(RedisKey.class);
      int slot1 = 1;
      when(key.getSlot()).thenReturn(slot1);
      keyList.add(key);
    }
    RedisKey finalKey = mock(RedisKey.class);
    int slot2 = 2;
    when(finalKey.getSlot()).thenReturn(slot2);
    keyList.add(finalKey);

    assertThat(RegionProvider.areKeysCrossSlots(keyList)).isTrue();
  }
}
