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

package org.apache.geode.pdx.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Region;

public class PeerTypeRegistrationCachingMapTest {

  private PeerTypeRegistrationCachingMap map;

  @Before
  public void setUp() {
    map = new PeerTypeRegistrationCachingMap();
  }

  @Test
  public void saveToPendingCorrectlyAddsOnlyToPendingMaps() {
    assertPendingMapsSize(0);
    assertLocalMapsSize(0);

    addPdxTypeToPendingMaps();

    assertThat(map.pendingTypeToIdSize()).isEqualTo(1);
    assertThat(map.pendingIdToTypeSize()).isEqualTo(1);
    assertThat(map.pendingEnumToIdSize()).isEqualTo(0);
    assertThat(map.pendingIdToEnumSize()).isEqualTo(0);
    assertLocalMapsSize(0);

    addEnumInfoToPendingMaps();

    assertPendingMapsSize(1);
    assertLocalMapsSize(0);

    Object fakeKey = mock(Object.class);
    Object fakeValue = mock(Object.class);
    map.saveToPending(fakeKey, fakeValue);

    assertPendingMapsSize(1);
    assertLocalMapsSize(0);
  }

  @Test
  public void shouldReloadFromRegionReturnsCorrectly() {
    assertThat(map.shouldReloadFromRegion(null)).isFalse();

    Region<?, ?> region = mock(Region.class);

    when(region.size()).thenReturn(0);
    // When everything is empty, we do not need to reload
    assertThat(map.shouldReloadFromRegion(region)).isFalse();

    when(region.size()).thenReturn(2);
    // Region size is 2, local maps size total is 0, so we should reload
    assertThat(map.shouldReloadFromRegion(region)).isTrue();

    addPdxTypeToMaps();
    addEnumInfoToPendingMaps();
    // Region size is 2, local maps size total is 2, so we do not need to reload
    assertThat(map.shouldReloadFromRegion(region)).isFalse();

    map.flushPendingLocalMaps();
    // Flushing should not change the result of the call
    assertThat(map.shouldReloadFromRegion(region)).isFalse();

    addPdxTypeToMaps();
    addEnumInfoToPendingMaps();
    // Region size is 2, local maps size total is 4, so we should reload
    assertThat(map.shouldReloadFromRegion(region)).isTrue();
  }

  @Test
  public void flushPendingLocalMapCorrectlyPopulatesLocalMaps() {
    int entries = 5;

    IntStream.range(0, entries).forEach(i -> {
      addPdxTypeToPendingMaps();
      addEnumInfoToPendingMaps();
    });

    assertPendingMapsSize(entries);
    assertLocalMapsSize(0);

    map.flushPendingLocalMaps();

    assertPendingMapsSize(0);
    assertLocalMapsSize(entries);
  }

  @Test
  public void clearRemovesAllEntriesFromLocalAndPendingMaps() {
    addPdxTypeToMaps();
    addEnumInfoToMaps();
    addPdxTypeToPendingMaps();
    addEnumInfoToPendingMaps();

    assertPendingMapsSize(1);
    assertLocalMapsSize(1);

    map.clear();

    assertPendingMapsSize(0);
    assertLocalMapsSize(0);
  }

  void assertPendingMapsSize(int expectedSize) {
    assertThat(map.pendingTypeToIdSize()).isEqualTo(expectedSize);
    assertThat(map.pendingIdToTypeSize()).isEqualTo(expectedSize);
    assertThat(map.pendingEnumToIdSize()).isEqualTo(expectedSize);
    assertThat(map.pendingIdToEnumSize()).isEqualTo(expectedSize);
  }

  void assertLocalMapsSize(int expectedSize) {
    assertThat(map.typeToIdSize()).isEqualTo(expectedSize);
    assertThat(map.idToTypeSize()).isEqualTo(expectedSize);
    assertThat(map.enumToIdSize()).isEqualTo(expectedSize);
    assertThat(map.idToEnumSize()).isEqualTo(expectedSize);
  }

  private void addEnumInfoToPendingMaps() {
    EnumId enumId = mock(EnumId.class);
    EnumInfo enumInfo = mock(EnumInfo.class);
    map.saveToPending(enumId, enumInfo);
  }

  private void addPdxTypeToPendingMaps() {
    Integer pdxId = map.pendingTypeToIdSize();
    PdxType pdxType = mock(PdxType.class);
    map.saveToPending(pdxId, pdxType);
  }

  void addEnumInfoToMaps() {
    EnumId enumId = mock(EnumId.class);
    EnumInfo enumInfo = mock(EnumInfo.class);
    map.save(enumId, enumInfo);
  }

  void addPdxTypeToMaps() {
    Integer pdxId = map.typeToIdSize();
    PdxType pdxType = mock(PdxType.class);
    map.save(pdxId, pdxType);
  }
}
