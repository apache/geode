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

import org.junit.Before;
import org.junit.Test;

public class TypeRegistrationCachingMapTest {
  private TypeRegistrationCachingMap map;

  @Before
  public void setUp() {
    map = new TypeRegistrationCachingMap();
  }

  @Test
  public void saveCorrectlyAddsToLocalMaps() {
    assertLocalMapsSize(0);

    addPdxTypeToMaps();

    assertThat(map.typeToIdSize()).isEqualTo(1);
    assertThat(map.idToTypeSize()).isEqualTo(1);
    assertThat(map.enumToIdSize()).isEqualTo(0);
    assertThat(map.idToEnumSize()).isEqualTo(0);

    addEnumInfoToMaps();

    assertLocalMapsSize(1);

    Object fakeKey = mock(Object.class);
    Object fakeValue = mock(Object.class);
    map.save(fakeKey, fakeValue);

    assertLocalMapsSize(1);
  }

  void assertLocalMapsSize(int expectedSize) {
    assertThat(map.typeToIdSize()).isEqualTo(expectedSize);
    assertThat(map.idToTypeSize()).isEqualTo(expectedSize);
    assertThat(map.enumToIdSize()).isEqualTo(expectedSize);
    assertThat(map.idToEnumSize()).isEqualTo(expectedSize);
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
