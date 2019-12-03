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

import org.junit.Test;

import org.apache.geode.cache.Region;

public class PeerTypeRegistrationReverseMapTest extends TypeRegistrationReverseMapTest {

  @Test
  public void saveToPendingCorrectlyAddsOnlyToPendingMaps() {
    PeerTypeRegistrationReverseMap map = new PeerTypeRegistrationReverseMap();
    assertThat(map.pendingTypeToIdSize()).isEqualTo(0);
    assertThat(map.pendingEnumToIdSize()).isEqualTo(0);
    assertThat(map.typeToIdSize()).isEqualTo(0);
    assertThat(map.enumToIdSize()).isEqualTo(0);

    addPdxTypeToPendingMap(map);

    assertThat(map.pendingTypeToIdSize()).isEqualTo(1);
    assertThat(map.pendingEnumToIdSize()).isEqualTo(0);
    assertThat(map.typeToIdSize()).isEqualTo(0);
    assertThat(map.enumToIdSize()).isEqualTo(0);

    addEnumInfoToPendingMap(map);

    assertThat(map.pendingTypeToIdSize()).isEqualTo(1);
    assertThat(map.pendingEnumToIdSize()).isEqualTo(1);
    assertThat(map.typeToIdSize()).isEqualTo(0);
    assertThat(map.enumToIdSize()).isEqualTo(0);

    Object fakeKey = mock(Object.class);
    Object fakeValue = mock(Object.class);
    map.saveToPending(fakeKey, fakeValue);

    assertThat(map.pendingTypeToIdSize()).isEqualTo(1);
    assertThat(map.pendingEnumToIdSize()).isEqualTo(1);
    assertThat(map.typeToIdSize()).isEqualTo(0);
    assertThat(map.enumToIdSize()).isEqualTo(0);
  }

  @Test
  public void shouldReloadFromRegionReturnsCorrectly() {
    PeerTypeRegistrationReverseMap map = new PeerTypeRegistrationReverseMap();

    assertThat(map.shouldReloadFromRegion(null)).isFalse();

    Region region = mock(Region.class);

    when(region.size()).thenReturn(0);
    // When everything is empty, we do not need to reload
    assertThat(map.shouldReloadFromRegion(region)).isFalse();

    when(region.size()).thenReturn(2);
    // Region size is 2, reverse maps size total is 0, so we should reload
    assertThat(map.shouldReloadFromRegion(region)).isTrue();

    addPdxTypeToMap(map);
    addEnumInfoToPendingMap(map);
    // Region size is 2, reverse maps size total is 2, so we do not need to reload
    assertThat(map.shouldReloadFromRegion(region)).isFalse();

    map.flushPendingReverseMap();
    // Flushing should not change the result of the call
    assertThat(map.shouldReloadFromRegion(region)).isFalse();

    addPdxTypeToMap(map);
    addEnumInfoToPendingMap(map);
    // Region size is 2, reverse maps size total is 4, so we should reload
    assertThat(map.shouldReloadFromRegion(region)).isTrue();
  }

  @Test
  public void flushPendingReverseMapCorrectlyPopulatesReverseMap() {
    PeerTypeRegistrationReverseMap map = new PeerTypeRegistrationReverseMap();

    addPdxTypeToPendingMap(map);
    addPdxTypeToPendingMap(map);
    addEnumInfoToPendingMap(map);
    addEnumInfoToPendingMap(map);

    assertThat(map.typeToIdSize()).isEqualTo(0);
    assertThat(map.enumToIdSize()).isEqualTo(0);
    assertThat(map.pendingTypeToIdSize()).isEqualTo(2);
    assertThat(map.pendingEnumToIdSize()).isEqualTo(2);

    map.flushPendingReverseMap();

    assertThat(map.typeToIdSize()).isEqualTo(2);
    assertThat(map.enumToIdSize()).isEqualTo(2);
    assertThat(map.pendingTypeToIdSize()).isEqualTo(0);
    assertThat(map.pendingEnumToIdSize()).isEqualTo(0);
  }

  @Test
  public void clearRemovesAllEntriesFromReverseAndPendingMaps() {
    PeerTypeRegistrationReverseMap map = new PeerTypeRegistrationReverseMap();

    addPdxTypeToMap(map);
    addEnumInfoToMap(map);
    addPdxTypeToPendingMap(map);
    addEnumInfoToPendingMap(map);

    assertThat(map.typeToIdSize()).isEqualTo(1);
    assertThat(map.enumToIdSize()).isEqualTo(1);
    assertThat(map.pendingTypeToIdSize()).isEqualTo(1);
    assertThat(map.pendingEnumToIdSize()).isEqualTo(1);

    map.clear();

    assertThat(map.typeToIdSize()).isEqualTo(0);
    assertThat(map.enumToIdSize()).isEqualTo(0);
    assertThat(map.pendingTypeToIdSize()).isEqualTo(0);
    assertThat(map.pendingEnumToIdSize()).isEqualTo(0);
  }

  private void addEnumInfoToPendingMap(PeerTypeRegistrationReverseMap map) {
    EnumId enumId = mock(EnumId.class);
    EnumInfo enumInfo = mock(EnumInfo.class);
    map.saveToPending(enumId, enumInfo);
  }

  private void addPdxTypeToPendingMap(PeerTypeRegistrationReverseMap map) {
    Integer pdxId = map.typeToIdSize();
    PdxType pdxType = mock(PdxType.class);
    map.saveToPending(pdxId, pdxType);
  }
}
