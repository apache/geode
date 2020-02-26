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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.InternalGemFireError;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.ServerConnectivityException;
import org.apache.geode.cache.client.internal.ExecutablePool;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.internal.cache.InternalCache;

public class ClientTypeRegistrationTest {
  private final int TYPE_ID = 1;
  private final int ENUM_ID = 2;
  private final int numberOfPools = 2;
  private PdxType newType;
  private EnumInfo newEnum;
  private InternalCache mockCache;
  private Collection<Pool> pools = new HashSet<>();
  private ClientTypeRegistration typeRegistration;

  @Before
  public void setUp() {
    newType = mock(PdxType.class);
    newEnum = mock(EnumInfo.class);
    mockCache = mock(InternalCache.class);
    IntStream.range(0, numberOfPools).forEach(i -> pools.add(mock(Pool.class, withSettings().extraInterfaces(ExecutablePool.class))));
    typeRegistration = spy(new ClientTypeRegistration(mockCache));
    doReturn(pools).when(typeRegistration).getAllPools();
  }

  @Test
  public void defineTypeDoesNotThrowExceptionAndUpdatesLocalMapWhenAtLeastOnePoolSucceedsGettingPdxTypeId() {
    assertThat(typeRegistration.getTypeToIdMap().size()).isZero();

    doThrow(new ServerConnectivityException()).doReturn(TYPE_ID).when(typeRegistration).getPdxIdFromPool(eq(newType), any(ExecutablePool.class));

    assertThat(typeRegistration.defineType(newType)).isEqualTo(TYPE_ID);

    // Confirm that we tried to get the typeId on both the pools, since we failed on the first
    verify(typeRegistration, times(numberOfPools)).getPdxIdFromPool(eq(newType), any(ExecutablePool.class));

    // Confirm that we correctly updated the local map
    assertThat(typeRegistration.getTypeToIdMap().size()).isOne();
    assertThat(typeRegistration.getTypeToIdMap()).containsKey(newType);
    assertThat(typeRegistration.getTypeToIdMap()).containsValue(TYPE_ID);

    // Confirm that a second call to defineType() retrieves the type locally and does not attempt to contact the pools
    assertThat(typeRegistration.defineType(newType)).isEqualTo(TYPE_ID);
    verify(typeRegistration, times(numberOfPools)).getPdxIdFromPool(eq(newType), any(ExecutablePool.class));
  }

  @Test
  public void defineTypeThrowsLastExceptionAndDoesNotUpdateLocalMapWhenExceptionsAreEncounteredWhileGettingPdxTypeIdOnEveryPool() {
    String firstExceptionMessage = "firstExceptionMessage";
    String secondExceptionMessage = "secondExceptionMessage";
    doThrow(new ServerConnectivityException(firstExceptionMessage)).doThrow(new ServerConnectivityException(secondExceptionMessage)).when(typeRegistration).getPdxIdFromPool(eq(newType), any(ExecutablePool.class));

    assertThat(typeRegistration.getTypeToIdMap().size()).isZero();
    assertThatThrownBy(() ->typeRegistration.defineType(newType)).isInstanceOf(ServerConnectivityException.class).hasMessageContaining(secondExceptionMessage);

    // Confirm that we did not update the local map
    assertThat(typeRegistration.getTypeToIdMap().size()).isZero();
  }

  @Test
  public void getTypeDoesNotThrowExceptionAndUpdatesLocalMapWhenAtLeastOnePoolSucceedsGettingPdxType() {
    assertThat(typeRegistration.getTypeToIdMap().size()).isZero();

    doThrow(new ServerConnectivityException()).doReturn(newType).when(typeRegistration).getPdxTypeFromPool(eq(TYPE_ID), any(ExecutablePool.class));

    assertThat(typeRegistration.getType(TYPE_ID)).isEqualTo(newType);

    // Confirm that we tried to get the PdxType on both the pools, since we failed on the first
    verify(typeRegistration, times(numberOfPools)).getPdxTypeFromPool(eq(TYPE_ID), any(ExecutablePool.class));

    // Confirm that we correctly updated the local map
    assertThat(typeRegistration.getTypeToIdMap().size()).isOne();
    assertThat(typeRegistration.getTypeToIdMap()).containsKey(newType);
    assertThat(typeRegistration.getTypeToIdMap()).containsValue(TYPE_ID);

    // Confirm that a second call to getType() retrieves the type locally and does not attempt to contact the pools
    assertThat(typeRegistration.getType(TYPE_ID)).isEqualTo(newType);
    verify(typeRegistration, times(numberOfPools)).getPdxTypeFromPool(eq(TYPE_ID), any(ExecutablePool.class));
  }

  @Test
  public void getTypeThrowsLastExceptionAndDoesNotUpdateLocalMapWhenExceptionsAreEncounteredWhileGettingPdxTypeOnEveryPool() {
    String firstExceptionMessage = "firstExceptionMessage";
    String secondExceptionMessage = "secondExceptionMessage";
    doThrow(new ServerConnectivityException(firstExceptionMessage)).doThrow(new ServerConnectivityException(secondExceptionMessage)).when(typeRegistration).getPdxTypeFromPool(eq(TYPE_ID), any(ExecutablePool.class));

    assertThat(typeRegistration.getTypeToIdMap().size()).isZero();
    assertThatThrownBy(() ->typeRegistration.getType(TYPE_ID)).isInstanceOf(ServerConnectivityException.class).hasMessageContaining(secondExceptionMessage);

    // Confirm that we did not update the local map
    assertThat(typeRegistration.getTypeToIdMap().size()).isZero();
  }

  @Test
  public void getTypeThrowsExceptionAndDoesNotUpdateLocalMapWhenNoPoolHasATypeForATypeId() {
    doReturn(null).when(typeRegistration).getPdxTypeFromPool(eq(TYPE_ID), any(ExecutablePool.class));

    assertThat(typeRegistration.getTypeToIdMap().size()).isZero();
    assertThatThrownBy(() ->typeRegistration.getType(TYPE_ID)).isInstanceOf(InternalGemFireError.class);

    // Confirm that we tried to get the PdxType on both the pools
    verify(typeRegistration, times(numberOfPools)).getPdxTypeFromPool(eq(TYPE_ID), any(ExecutablePool.class));
    // Confirm that we did not update the local map
    assertThat(typeRegistration.getTypeToIdMap().size()).isZero();
  }

  @Test
  public void getAllPoolsReturnsAllPoolsNotUsedByGateways() {
    Collection<Pool> pools = new HashSet<>();
    PoolImpl mockGatewayPool = mock(PoolImpl.class);
    when((mockGatewayPool).isUsedByGateway()).thenReturn(true);
    pools.add(mockGatewayPool);
    PoolImpl mockNonGatewayPool = mock(PoolImpl.class);
    when((mockNonGatewayPool).isUsedByGateway()).thenReturn(false);
    pools.add(mockNonGatewayPool);

    doReturn(pools).when(typeRegistration).getPools();

    doCallRealMethod().when(typeRegistration).getAllPools();
    assertThat(typeRegistration.getAllPools()).containsExactly(mockNonGatewayPool);
  }

  @Test
  public void getAllPoolsThrowsExceptionWhenNoPoolsArePresent() {
    doReturn(new HashSet<>()).when(typeRegistration).getPools();

    doCallRealMethod().when(typeRegistration).getAllPools();
    when(mockCache.getCacheClosedException(anyString())).thenReturn(new CacheClosedException());

    assertThatThrownBy(() -> typeRegistration.getAllPools()).isInstanceOf(CacheClosedException.class);
  }

  @Test
  public void getAllPoolsThrowsExceptionWhenAllAvailablePoolsAreUsedByGateways() {
    Collection<Pool> pools = new HashSet<>();
    PoolImpl mockGatewayPoolOne = mock(PoolImpl.class);
    when((mockGatewayPoolOne).isUsedByGateway()).thenReturn(true);
    pools.add(mockGatewayPoolOne);
    PoolImpl mockGatewayPoolTwo = mock(PoolImpl.class);
    when((mockGatewayPoolTwo).isUsedByGateway()).thenReturn(true);
    pools.add(mockGatewayPoolTwo);

    doReturn(pools).when(typeRegistration).getPools();
    doCallRealMethod().when(typeRegistration).getAllPools();
    when(mockCache.getCacheClosedException(anyString())).thenReturn(new CacheClosedException());

    assertThatThrownBy(() -> typeRegistration.getAllPools()).isInstanceOf(CacheClosedException.class);
  }

  @Test
  public void defineEnumDoesNotThrowExceptionAndUpdatesLocalMapWhenAtLeastOnePoolSucceedsGettingEnumId() {
    assertThat(typeRegistration.getEnumToIdMap().size()).isZero();

    doThrow(new ServerConnectivityException()).doReturn(ENUM_ID).when(typeRegistration).getEnumIdFromPool(eq(newEnum), any(ExecutablePool.class));
    doNothing().when(typeRegistration).addPdxEnum(any(EnumInfo.class), anyInt(), any(ExecutablePool.class));

    assertThat(typeRegistration.defineEnum(newEnum)).isEqualTo(ENUM_ID);

    // Confirm that we tried to get the EnumId on both the pools, since we failed on the first
    verify(typeRegistration, times(numberOfPools)).getEnumIdFromPool(eq(newEnum), any(ExecutablePool.class));

    // Confirm that we correctly updated the local map
    assertThat(typeRegistration.getEnumToIdMap().size()).isOne();
    assertThat(typeRegistration.getEnumToIdMap()).containsKey(newEnum);
    assertThat(typeRegistration.getEnumToIdMap().get(newEnum).intValue()).isEqualTo(ENUM_ID);

    // Confirm that a second call to defineEnum() retrieves the EnumId locally and does not attempt to contact the pools
    assertThat(typeRegistration.defineEnum(newEnum)).isEqualTo(ENUM_ID);
    verify(typeRegistration, times(numberOfPools)).getEnumIdFromPool(eq(newEnum), any(ExecutablePool.class));
  }

  @Test
  public void defineEnumThrowsLastExceptionAndDoesNotUpdateLocalMapWhenExceptionsAreEncounteredWhileGettingEnumIdOnEveryPool() {
    String firstExceptionMessage = "firstExceptionMessage";
    String secondExceptionMessage = "secondExceptionMessage";
    doThrow(new ServerConnectivityException(firstExceptionMessage)).doThrow(new ServerConnectivityException(secondExceptionMessage)).when(typeRegistration).getEnumIdFromPool(eq(newEnum), any(ExecutablePool.class));

    assertThat(typeRegistration.getEnumToIdMap().size()).isZero();
    assertThatThrownBy(() -> typeRegistration.defineEnum(newEnum)).isInstanceOf(ServerConnectivityException.class).hasMessageContaining(secondExceptionMessage);

    // Confirm that we did not update the local map
    assertThat(typeRegistration.getEnumToIdMap().size()).isZero();
  }

  @Test
  public void getEnumByIdDoesNotThrowExceptionAndUpdatesLocalMapWhenAtLeastOnePoolSucceedsGettingEnumInfo() {
    assertThat(typeRegistration.getEnumToIdMap().size()).isZero();

    doThrow(new ServerConnectivityException()).doReturn(newEnum).when(typeRegistration).getEnumFromPool(eq(ENUM_ID), any(ExecutablePool.class));

    assertThat(typeRegistration.getEnumById(ENUM_ID)).isEqualTo(newEnum);

    // Confirm that we tried to get the EnumInfo on both the pools, since we failed on the first
    verify(typeRegistration, times(numberOfPools)).getEnumFromPool(eq(ENUM_ID), any(ExecutablePool.class));

    // Confirm that we correctly updated the local map
    assertThat(typeRegistration.getEnumToIdMap().size()).isOne();
    assertThat(typeRegistration.getEnumToIdMap()).containsKey(newEnum);
    assertThat(typeRegistration.getEnumToIdMap().get(newEnum).intValue()).isEqualTo(ENUM_ID);

    // Confirm that a second call to getEnumById() retrieves the EnumInfo locally and does not attempt to contact the pools
    assertThat(typeRegistration.getEnumById(ENUM_ID)).isEqualTo(newEnum);
    verify(typeRegistration, times(numberOfPools)).getEnumFromPool(eq(ENUM_ID), any(ExecutablePool.class));
  }

  @Test
  public void getEnumByIdThrowsLastExceptionAndDoesNotUpdateLocalMapWhenExceptionsAreEncounteredWhileGettingEnumInfoOnEveryPool() {
    String firstExceptionMessage = "firstExceptionMessage";
    String secondExceptionMessage = "secondExceptionMessage";
    doThrow(new ServerConnectivityException(firstExceptionMessage)).doThrow(new ServerConnectivityException(secondExceptionMessage)).when(typeRegistration).getEnumFromPool(eq(ENUM_ID), any(ExecutablePool.class));

    assertThat(typeRegistration.getEnumToIdMap().size()).isZero();
    assertThatThrownBy(() -> typeRegistration.getEnumById(ENUM_ID)).isInstanceOf(ServerConnectivityException.class).hasMessageContaining(secondExceptionMessage);

    // Confirm that we did not update the local map
    assertThat(typeRegistration.getEnumToIdMap().size()).isZero();
  }

  @Test
  public void getEnumByIdThrowsExceptionAndDoesNotUpdateLocalMapWhenNoPoolHasAnEnumInfoForAnEnumId() {
    doReturn(null).when(typeRegistration).getEnumFromPool(eq(ENUM_ID), any(ExecutablePool.class));

    assertThat(typeRegistration.getEnumToIdMap().size()).isZero();
    assertThatThrownBy(() -> typeRegistration.getEnumById(ENUM_ID)).isInstanceOf(InternalGemFireError.class);

    // Confirm that we tried to get the EnumInfo on both the pools
    verify(typeRegistration, times(numberOfPools)).getEnumFromPool(eq(ENUM_ID), any(ExecutablePool.class));
    // Confirm that we did not update the local map
    assertThat(typeRegistration.getEnumToIdMap().size()).isZero();
  }

  @Test
  public void typesAttemptsToGetPdxTypesFromAllPools() {
    Integer firstId = 1;
    PdxType mockFirstType = mock(PdxType.class);
    Map<Integer, PdxType> firstPoolTypes = Collections.singletonMap(firstId, mockFirstType);
    Integer secondId = 2;
    PdxType mockSecondType = mock(PdxType.class);
    Map<Integer, PdxType> secondPoolTypes = Collections.singletonMap(secondId, mockSecondType);

    doReturn(firstPoolTypes).doReturn(secondPoolTypes).when(typeRegistration).getAllPdxTypesFromPool(any(ExecutablePool.class));

    Map<Integer, PdxType> result = typeRegistration.types();

    assertThat(result.size()).isEqualTo(2);
    assertThat(result.get(firstId)).isEqualTo(mockFirstType);
    assertThat(result.get(secondId)).isEqualTo(mockSecondType);

    verify(typeRegistration, times(numberOfPools)).getAllPdxTypesFromPool(any(ExecutablePool.class));
  }

  @Test
  public void enumsAttemptsToGetEnumInfosFromAllPools() {
    Integer firstId = 1;
    EnumInfo mockFirstType = mock(EnumInfo.class);
    Map<Integer, EnumInfo> firstPoolEnums = Collections.singletonMap(firstId, mockFirstType);
    Integer secondId = 2;
    EnumInfo mockSecondType = mock(EnumInfo.class);
    Map<Integer, EnumInfo> secondPoolEnums = Collections.singletonMap(secondId, mockSecondType);

    doReturn(firstPoolEnums).doReturn(secondPoolEnums).when(typeRegistration).getAllEnumsFromPool(any(ExecutablePool.class));

    Map<Integer, EnumInfo> result = typeRegistration.enums();

    assertThat(result.size()).isEqualTo(2);
    assertThat(result.get(firstId)).isEqualTo(mockFirstType);
    assertThat(result.get(secondId)).isEqualTo(mockSecondType);

    verify(typeRegistration, times(numberOfPools)).getAllEnumsFromPool(any(ExecutablePool.class));
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
