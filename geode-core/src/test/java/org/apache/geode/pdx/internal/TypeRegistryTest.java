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
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.InternalGemFireError;
import org.apache.geode.StatisticsType;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.statistics.StatisticsManager;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxSerializationException;
import org.apache.geode.pdx.PdxSerializer;
import org.apache.geode.pdx.ReflectionBasedAutoSerializer;
import org.apache.geode.test.junit.categories.SerializationTest;

@Category({SerializationTest.class})
public class TypeRegistryTest {

  private InternalCache mockInternalCache;
  private TypeRegistry typeRegistry;
  private TypeRegistration mockTypeRegistration;
  private PdxType newType;
  private EnumInfo newEnumInfo;
  private final int TYPE_ID = 1;
  private final int ENUM_ID = 2;

  @Before
  public void setUp() {
    mockInternalCache = mock(InternalCache.class);
    mockTypeRegistration = mock(TypeRegistration.class);
    typeRegistry = new TypeRegistry(mockInternalCache, mockTypeRegistration);
    newType = mock(PdxType.class);
    newEnumInfo = mock(EnumInfo.class);
  }

  @Test
  public void constructorUsesNullTypeRegistrationWhenDisableTypeRegistryArgumentIsTrue() {
    typeRegistry = new TypeRegistry(mockInternalCache, true);
    assertThat(typeRegistry.getTypeRegistration()).isInstanceOf(NullTypeRegistration.class);
  }

  @Test
  public void constructorUsesClientTypeRegistrationWhenCacheHasPool() {
    when(mockInternalCache.hasPool()).thenReturn(true);

    typeRegistry = new TypeRegistry(mockInternalCache, false);
    assertThat(typeRegistry.getTypeRegistration()).isInstanceOf(ClientTypeRegistration.class);
  }

  @Test
  public void constructorUsesLonerTypeRegistrationWhenCacheIsIndeterminateLoner() {
    InternalDistributedSystem mockIDS = mock(InternalDistributedSystem.class);
    when(mockInternalCache.getInternalDistributedSystem()).thenReturn(mockIDS);
    when(mockIDS.isLoner()).thenReturn(true);
    when(mockInternalCache.getPdxPersistent()).thenReturn(false);

    typeRegistry = new TypeRegistry(mockInternalCache, false);
    assertThat(typeRegistry.getTypeRegistration()).isInstanceOf(LonerTypeRegistration.class);
  }

  @Test
  public void constructorUsesPeerTypeRegistrationWhenCacheIsNotClientOrIndeterminateLoner() {
    when(mockInternalCache.hasPool()).thenReturn(false);
    InternalDistributedSystem mockIDS = mock(InternalDistributedSystem.class);
    when(mockInternalCache.getInternalDistributedSystem()).thenReturn(mockIDS);
    when(mockIDS.isLoner()).thenReturn(false);
    DistributionManager mockDM = mock(DistributionManager.class);
    when(mockIDS.getDistributionManager()).thenReturn(mockDM);
    StatisticsManager mockStatManager = mock(StatisticsManager.class);
    when(mockIDS.getStatisticsManager()).thenReturn(mockStatManager);
    StatisticsType mockStatType = mock(StatisticsType.class);
    when(mockStatManager.createType(any(), any(), any())).thenReturn(mockStatType);

    typeRegistry = new TypeRegistry(mockInternalCache, false);
    assertThat(typeRegistry.getTypeRegistration()).isInstanceOf(PeerTypeRegistration.class);
  }

  @Test
  public void defineTypeThrowsWhenOldTypeIsNotEqualToNewTypeWithSameID() {
    when(mockTypeRegistration.defineType(newType)).thenReturn(TYPE_ID);
    PdxType oldType = mock(PdxType.class);
    when(mockTypeRegistration.getType(TYPE_ID)).thenReturn(oldType);

    assertThatThrownBy(() -> typeRegistry.defineType(newType))
        .isInstanceOf(InternalGemFireError.class);
  }

  @Test
  public void addRemoteTypeWithNewTypeCallsDistributedTypeRegistryWhenThereIsNoExistingType() {
    when(mockTypeRegistration.getType(TYPE_ID)).thenReturn(null);

    typeRegistry.addRemoteType(TYPE_ID, newType);

    verify(mockTypeRegistration, times(1)).addRemoteType(TYPE_ID, newType);
  }

  @Test
  public void addRemoteTypeThrowsWhenOldTypeIsNotEqualToNewTypeWithSameID() {
    PdxType oldType = mock(PdxType.class);
    when(mockTypeRegistration.getType(TYPE_ID)).thenReturn(oldType);

    assertThatThrownBy(() -> typeRegistry.addRemoteType(TYPE_ID, newType))
        .isInstanceOf(InternalGemFireError.class);
  }

  @Test
  public void defineLocalTypeGivenNullObjectReturnsExistingTypeAndDoesNotUpdateLocalTypeIds() {
    PdxType existingType = mock(PdxType.class);
    when(mockTypeRegistration.defineType(existingType)).thenReturn(TYPE_ID);
    when(mockTypeRegistration.getType(TYPE_ID)).thenReturn(existingType);
    assertThat(typeRegistry.getLocalTypeIds()).isEmpty();

    assertThat(typeRegistry.defineLocalType(null, existingType)).isSameAs(existingType);
    assertThat(typeRegistry.getLocalTypeIds()).isEmpty();
  }

  @Test
  public void defineLocalTypeGivenNonNullObjectWithExistingTypeReturnsExistingTypeFromLocalTypeIds() {
    PdxType existingType = mock(PdxType.class);
    Object domainInstance = new Object();
    typeRegistry.getLocalTypeIds().put(Object.class, existingType);

    assertThat(typeRegistry.defineLocalType(domainInstance, newType)).isSameAs(existingType);
    verify(mockTypeRegistration, times(0)).defineType(any());
  }

  @Test
  public void defineLocalTypeGivenNewTypeAndNonNullObjectUpdatesLocalTypeIdsAndReturnsNewType() {
    when(mockTypeRegistration.defineType(newType)).thenReturn(TYPE_ID);
    when(mockTypeRegistration.getType(TYPE_ID)).thenReturn(newType);
    Object domainInstance = new Object();

    assertThat(typeRegistry.defineLocalType(domainInstance, newType)).isSameAs(newType);
    assertThat(typeRegistry.getLocalTypeIds())
        .containsExactly(entry(domainInstance.getClass(), newType));
  }

  @Test
  public void setPdxSerializerUpdatesPdxSerializerAndAutoSerializableManager() {
    PdxSerializer mockSerializer = mock(PdxSerializer.class);
    ReflectionBasedAutoSerializer mockReflectionBasedSerializer =
        mock(ReflectionBasedAutoSerializer.class);
    AutoSerializableManager mockReflectionBasedManager = mock(AutoSerializableManager.class);
    when(mockReflectionBasedSerializer.getManager()).thenReturn(mockReflectionBasedManager);

    // Verify that a non-ReflectionBasedAutoSerializer PdxSerializer updates the serializer and does
    // not update the AutoSerializableManager
    TypeRegistry.setPdxSerializer(mockSerializer);
    assertThat(TypeRegistry.getPdxSerializer()).isEqualTo(mockSerializer);
    assertThat(TypeRegistry.getAutoSerializableManager()).isEqualTo(null);

    // Verify that both the PdxSerializer and AutoSerializableManager are updated with a
    // ReflectionBasedAutoSerializer
    TypeRegistry.setPdxSerializer(mockReflectionBasedSerializer);
    assertThat(TypeRegistry.getPdxSerializer()).isEqualTo(mockReflectionBasedSerializer);
    assertThat(TypeRegistry.getAutoSerializableManager()).isEqualTo(mockReflectionBasedManager);

    // Verify that changing from a ReflectionBasedAutoSerializer PdxSerializer to a
    // non-ReflectionBasedAutoSerializer updates the PdxSerializer but does not change the
    // AutoSerializableManager
    TypeRegistry.setPdxSerializer(mockSerializer);
    assertThat(TypeRegistry.getPdxSerializer()).isEqualTo(mockSerializer);
    assertThat(TypeRegistry.getAutoSerializableManager()).isEqualTo(mockReflectionBasedManager);

    // If a PdxSerializer was set but the TypeRegistry.getPdxSerializer() method returns null and
    // the TypeRegistry is marked as closed, a CacheClosedException is thrown
    TypeRegistry.setPdxSerializer(null);
    TypeRegistry.close();
    assertThatThrownBy(TypeRegistry::getPdxSerializer).isInstanceOf(CacheClosedException.class);

    // Call the TypeRegistry.open() method to prevent the CacheClosedException
    TypeRegistry.open();
    // Verify that only the PdxSerializer is set to null when changing from a
    // non-ReflectionBasedAutoSerializer to a null PdxSerializer
    assertThat(TypeRegistry.getPdxSerializer()).isEqualTo(null);
    assertThat(TypeRegistry.getAutoSerializableManager()).isEqualTo(mockReflectionBasedManager);

    // Verify that changing from a ReflectionBasedAutoSerializer to a null PdxSerializer sets both
    // PdxSerializer and AutoSerializableManager to null
    TypeRegistry.setPdxSerializer(mockReflectionBasedSerializer);
    TypeRegistry.setPdxSerializer(null);
    assertThat(TypeRegistry.getPdxSerializer()).isEqualTo(null);
    assertThat(TypeRegistry.getAutoSerializableManager()).isEqualTo(null);
  }

  @Test
  public void getEnumIdReturnsZeroWhenEnumIsNull() {
    assertThat(typeRegistry.getEnumId(null)).isZero();
  }

  @Test
  public void getEnumIdGivenExistingEnumReturnsIdFromLocalEnumIds() {
    Enum<?> existingEnum = mock(Enum.class);
    typeRegistry.getLocalEnumIds().put(existingEnum, ENUM_ID);

    assertThat(typeRegistry.getEnumId(existingEnum)).isEqualTo(ENUM_ID);
    verify(mockTypeRegistration, times(0)).getEnumId(any());
  }

  @Test
  public void getEnumIdGivenNewEnumUpdatesLocalEndumIdsAndReturnsId() {
    Enum<?> newEnum = mock(Enum.class);
    when(mockTypeRegistration.getEnumId(newEnum)).thenReturn(ENUM_ID);

    assertThat(typeRegistry.getEnumId(newEnum)).isEqualTo(ENUM_ID);
    assertThat(typeRegistry.getLocalEnumIds()).containsExactly(entry(newEnum, ENUM_ID));
  }

  @Test
  public void addRemoteEnumWithNewEnumInfoCallsDistributedTypeRegistryWhenThereIsNoExistingEnumInfo() {
    when(mockTypeRegistration.getEnumById(ENUM_ID)).thenReturn(null);

    typeRegistry.addRemoteEnum(ENUM_ID, newEnumInfo);

    verify(mockTypeRegistration, times(1)).addRemoteEnum(ENUM_ID, newEnumInfo);
  }

  @Test
  public void addRemoteEnumThrowsWhenOldEnumInfoIsNotEqualToNewEnumInfoWithSameID() {
    EnumInfo existingEnumInfo = mock(EnumInfo.class);
    when(mockTypeRegistration.getEnumById(ENUM_ID)).thenReturn(existingEnumInfo);

    assertThatThrownBy(() -> typeRegistry.addRemoteEnum(ENUM_ID, newEnumInfo))
        .isInstanceOf(InternalGemFireError.class);
  }

  @Test
  public void defineEnumThrowsWhenOldTypeIsNotEqualToNewTypeWithSameID() {
    EnumInfo existingEnumInfo = mock(EnumInfo.class);
    when(mockTypeRegistration.defineEnum(newEnumInfo)).thenReturn(ENUM_ID);
    when(mockTypeRegistration.getEnumById(ENUM_ID)).thenReturn(existingEnumInfo);

    assertThatThrownBy(() -> typeRegistry.defineEnum(newEnumInfo))
        .isInstanceOf(InternalGemFireError.class);
  }

  @Test
  public void getEnumByIdReturnsNullForEnumIdEqualToZero() {
    assertThat(typeRegistry.getEnumById(0)).isNull();
  }

  @Test
  public void getEnumByIdThrowsPdxSerializationExceptionWhenEnumInfoIsNotFoundInDistributedTypeRegistry() {
    when(mockTypeRegistration.getEnumById(ENUM_ID)).thenReturn(null);

    assertThatThrownBy(() -> typeRegistry.getEnumById(ENUM_ID))
        .isInstanceOf(PdxSerializationException.class);
  }

  @Test
  public void getEnumByIdReturnsPdxInstanceWhenGetPdxReadSerializedByAnyGemFireServicesisTrue() {
    when(mockTypeRegistration.getEnumById(ENUM_ID)).thenReturn(newEnumInfo);
    when(mockInternalCache.getPdxReadSerializedByAnyGemFireServices()).thenReturn(true);
    PdxInstance mockPdxInstance = mock(PdxInstance.class);
    when(newEnumInfo.getPdxInstance(ENUM_ID)).thenReturn(mockPdxInstance);

    assertThat(typeRegistry.getEnumById(ENUM_ID)).isSameAs(mockPdxInstance);
  }

  @Test
  public void getEnumByIdReturnsEnum() {
    EnumInfo testEnumInfo = new EnumInfo(TestEnum.ENUM_VALUE);
    when(mockTypeRegistration.getEnumById(ENUM_ID)).thenReturn(testEnumInfo);
    when(mockInternalCache.getPdxReadSerializedByAnyGemFireServices()).thenReturn(false);

    assertThat(typeRegistry.getEnumById(ENUM_ID)).isSameAs(TestEnum.ENUM_VALUE);
  }

  @Test
  public void getEnumByIdThrowsPdxSerializationExceptionWhenEnumClassCannotBeLoaded()
      throws ClassNotFoundException {
    when(mockTypeRegistration.getEnumById(ENUM_ID)).thenReturn(newEnumInfo);
    when(mockInternalCache.getPdxReadSerializedByAnyGemFireServices()).thenReturn(false);
    doThrow(new ClassNotFoundException()).when(newEnumInfo).getEnum();

    assertThatThrownBy(() -> typeRegistry.getEnumById(ENUM_ID)).isInstanceOf(PdxSerializationException.class);
  }

  @Test
  public void addImportedTypeCallsDistributedTypeRegistryWhenImportedTypeDoesNotExist() {
    when(mockTypeRegistration.getType(TYPE_ID)).thenReturn(null);

    typeRegistry.addImportedType(TYPE_ID, newType);

    verify(mockTypeRegistration, times(1)).addImportedType(TYPE_ID, newType);
  }

  @Test
  public void addImportedTypeThrowsPdxSerializationExceptionWhenImportedTypeDoesNotEqualExistingTypeWithSameId() {
    PdxType existingType = mock(PdxType.class);
    when(mockTypeRegistration.getType(TYPE_ID)).thenReturn(existingType);
    assertThatThrownBy(() -> typeRegistry.addImportedType(TYPE_ID, newType)).isInstanceOf(PdxSerializationException.class);
  }

  @Test
  public void addImportedEnumCallsDistributedTypeRegistryWhenImportedEnumDoesNotExist() {
    when(mockTypeRegistration.getEnumById(TYPE_ID)).thenReturn(null);

    typeRegistry.addImportedEnum(ENUM_ID, newEnumInfo);

    verify(mockTypeRegistration, times(1)).addImportedEnum(ENUM_ID, newEnumInfo);
  }

  @Test
  public void addImportedEnumThrowsPdxSerializationExceptionWhenImportedEnumInfoDoesNotEqualExistingEnumInfoWithSameId() {
    EnumInfo existingEnumInfo = mock(EnumInfo.class);
    when(mockTypeRegistration.getEnumById(ENUM_ID)).thenReturn(existingEnumInfo);
    assertThatThrownBy(() -> typeRegistry.addImportedEnum(ENUM_ID, newEnumInfo)).isInstanceOf(PdxSerializationException.class);
  }

  private enum TestEnum {
    ENUM_VALUE
  }
}
