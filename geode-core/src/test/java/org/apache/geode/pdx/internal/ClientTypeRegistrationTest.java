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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.withSettings;

import java.util.Collection;
import java.util.HashSet;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.internal.ExecutablePool;
import org.apache.geode.internal.cache.InternalCache;

public class ClientTypeRegistrationTest {
  private final int TYPE_ID = 1;
  private final int ENUM_ID = 2;
  private PdxType newType;
  private EnumInfo newEnum;
  private InternalCache mockCache;
  private Collection<Pool> pools;
  private ClientTypeRegistration typeRegistration;

  @Before
  public void setUp() {
    newType = mock(PdxType.class);
    newEnum = mock(EnumInfo.class);
    mockCache = mock(InternalCache.class);
    pools = new HashSet<>();
    Pool mockPool = mock(Pool.class, withSettings().extraInterfaces(ExecutablePool.class));
    pools.add(mockPool);
    typeRegistration = spy(new ClientTypeRegistration(mockCache));
    doReturn(pools).when(typeRegistration).getAllPools();
  }

  @Test
  public void defineTypeReturnsCorrectTypeIdForNewTypeAndStoresItLocally() {
    doReturn(TYPE_ID).when(typeRegistration).getPdxIdFromPool(eq(newType),
        any(ExecutablePool.class));
    assertThat(typeRegistration.defineType(newType)).isEqualTo(TYPE_ID);

    // Confirm that we defined the new type on a server
    verify(typeRegistration, times(1)).getPdxIdFromPool(eq(newType), any(ExecutablePool.class));

    assertThat(typeRegistration.getType(TYPE_ID)).isSameAs(newType);

    // Confirm that the type we just got was retrieved locally
    verify(typeRegistration, times(0)).getPdxTypeFromPool(eq(TYPE_ID), any(ExecutablePool.class));

    assertThat(typeRegistration.getTypeToIdMap().get(newType)).isEqualTo(TYPE_ID);
  }

  @Test
  public void getTypeReturnsCorrectTypeForNewIdAndStoresItLocally() {
    doReturn(newType).when(typeRegistration).getPdxTypeFromPool(eq(TYPE_ID),
        any(ExecutablePool.class));

    assertThat(typeRegistration.getType(TYPE_ID)).isSameAs(newType);

    // Confirm that we got the new type from a server
    verify(typeRegistration, times(1)).getPdxTypeFromPool(eq(TYPE_ID), any(ExecutablePool.class));

    assertThat(typeRegistration.getType(TYPE_ID)).isSameAs(newType);

    // Confirm that the type we just got was retrieved locally
    verify(typeRegistration, times(1)).getPdxTypeFromPool(eq(TYPE_ID), any(ExecutablePool.class));

    assertThat(typeRegistration.getTypeToIdMap().get(newType)).isEqualTo(TYPE_ID);
  }

  @Test
  public void defineEnumReturnsCorrectEnumIdForNewEnumAndStoresItLocally() {
    doReturn(ENUM_ID).when(typeRegistration).getEnumIdFromPool(eq(newEnum),
        any(ExecutablePool.class));
    assertThat(typeRegistration.defineEnum(newEnum)).isEqualTo(ENUM_ID);

    // Confirm that we defined the new enum on a server
    verify(typeRegistration, times(1)).getEnumIdFromPool(eq(newEnum), any(ExecutablePool.class));

    assertThat(typeRegistration.getEnumById(ENUM_ID)).isSameAs(newEnum);

    // Confirm that the enum we just got was retrieved locally
    verify(typeRegistration, times(0)).getEnumFromPool(eq(ENUM_ID), any(ExecutablePool.class));

    assertThat(typeRegistration.getEnumToIdMap().get(newEnum)).isEqualTo(new EnumId(ENUM_ID));
  }

  @Test
  public void getEnumByIdReturnsCorrectEnumInfoForNewIdAndStoresItLocally() {
    doReturn(newEnum).when(typeRegistration).getEnumFromPool(eq(ENUM_ID),
        any(ExecutablePool.class));

    assertThat(typeRegistration.getEnumById(ENUM_ID)).isSameAs(newEnum);

    // Confirm that we got the new enum from a server
    verify(typeRegistration, times(1)).getEnumFromPool(eq(ENUM_ID), any(ExecutablePool.class));

    assertThat(typeRegistration.getEnumById(ENUM_ID)).isSameAs(newEnum);

    // Confirm that the enum we just got was retrieved locally
    verify(typeRegistration, times(1)).getEnumFromPool(eq(ENUM_ID), any(ExecutablePool.class));

    assertThat(typeRegistration.getEnumToIdMap().get(newEnum)).isEqualTo(new EnumId(ENUM_ID));
  }

  @Test
  public void getEnumIdReturnsCorrectEnumIdForNewEnumAndStoresItLocally() {
    Enum<?> mockEnum = mock(Enum.class);
    EnumInfo expectedEnumInfo = new EnumInfo(mockEnum);

    doReturn(ENUM_ID).when(typeRegistration).getEnumIdFromPool(eq(expectedEnumInfo),
        any(ExecutablePool.class));
    assertThat(typeRegistration.getEnumId(mockEnum)).isEqualTo(ENUM_ID);

    // Confirm that we defined the new enum on a server
    verify(typeRegistration, times(1)).getEnumIdFromPool(eq(expectedEnumInfo),
        any(ExecutablePool.class));

    assertThat(typeRegistration.getEnumById(ENUM_ID)).isEqualTo(expectedEnumInfo);

    // Confirm that the enum we just got was retrieved locally
    verify(typeRegistration, times(0)).getEnumFromPool(eq(ENUM_ID), any(ExecutablePool.class));

    assertThat(typeRegistration.getEnumToIdMap().get(expectedEnumInfo))
        .isEqualTo(new EnumId(ENUM_ID));
  }
}
