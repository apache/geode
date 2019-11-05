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
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.junit.categories.SerializationTest;

@Category({SerializationTest.class})
public class TypeRegistryTest {

  private TypeRegistry typeRegistry;

  private PdxType newType;

  @Before
  public void setUp() {
    InternalCache internalCache = mock(InternalCache.class);
    TypeRegistration typeRegistration = mock(TypeRegistration.class);
    typeRegistry = new TypeRegistry(internalCache, typeRegistration);
    newType = mock(PdxType.class);
  }

  // @Test
  // public void defineTypeGivenANewTypeStoresItWithTheCorrectIdAndReturnsIt() {
  // when(typeRegistration.defineType(newType)).thenReturn(TYPE_ID);
  //
  // assertThat(typeRegistry.defineType(newType)).isSameAs(newType);
  //
  // verify(newType).setTypeId(TYPE_ID);
  // assertThat(typeRegistry.getIdToType().get(TYPE_ID)).isSameAs(newType);
  // assertThat(typeRegistry.getTypeToId().get(newType)).isSameAs(TYPE_ID);
  // }
  //
  // @Test
  // public void defineEnumGivenANewEnumStoresItWithTheCorrectIdAndReturnsThatId() {
  // when(typeRegistration.defineEnum(newEnum)).thenReturn(ENUM_ID);
  //
  // assertThat(typeRegistry.defineEnum(newEnum)).isEqualTo(ENUM_ID);
  // assertThat(typeRegistry.getIdToEnum().get(ENUM_ID)).isSameAs(newEnum);
  // assertThat(typeRegistry.getEnumInfoToId().get(newEnum)).isEqualTo(ENUM_ID);
  // }
  //
  // @Test
  // public void
  // defineTypeGivenANewTypeThatIsInTypeToIdButNotIdToTypeStoresItWithTheCorrectIdAndReturnsIt() {
  // when(typeRegistration.defineType(newType)).thenReturn(TYPE_ID);
  // typeRegistry.getTypeToId().put(newType, TYPE_ID);
  //
  // assertThat(typeRegistry.defineType(newType)).isSameAs(newType);
  //
  // verify(newType).setTypeId(TYPE_ID);
  // assertThat(typeRegistry.getIdToType().get(TYPE_ID)).isSameAs(newType);
  // assertThat(typeRegistry.getTypeToId().get(newType)).isSameAs(TYPE_ID);
  // }
  //
  // @Test
  // public void defineTypeGivenATypeEqualToAnExistingTypeReturnsTheExistingType() {
  // PdxType existingType = new PdxType("myClass", true);
  // PdxType equalType = new PdxType("myClass", true);
  // typeRegistry.getTypeToId().put(existingType, TYPE_ID);
  // typeRegistry.getIdToType().put(TYPE_ID, existingType);
  //
  // assertThat(typeRegistry.defineType(equalType)).isSameAs(existingType);
  // }
  //
  // @Test
  // public void defineEnumGivenAnEnumEqualToAnExistingEnumReturnsTheExistingEnum() {
  // EnumInfo existingEnum = new EnumInfo("myClass", "myEnum", 1);
  // EnumInfo equalEnum = new EnumInfo("myClass", "myEnum", 1);
  // typeRegistry.getEnumInfoToId().put(existingEnum, ENUM_ID);
  // typeRegistry.getIdToEnum().put(ENUM_ID, existingEnum);
  //
  // assertThat(typeRegistry.defineEnum(equalEnum)).isEqualTo(ENUM_ID);
  // }
  //
  // @Test
  // public void
  // defineTypeGivenATypeEqualToAnExistingTypeThatIsNotInTypeToIdReturnsTheExistingType() {
  // PdxType existingType = new PdxType("myClass", true);
  // PdxType equalType = new PdxType("myClass", true);
  // typeRegistry.getTypeToId().put(existingType, null);
  // typeRegistry.getIdToType().put(TYPE_ID, existingType);
  // when(typeRegistration.defineType(equalType)).thenReturn(TYPE_ID);
  //
  // assertThat(typeRegistry.defineType(equalType)).isSameAs(existingType);
  // }
  //
  // @Test
  // public void
  // defineEnumGivenAnEnumEqualToAnExistingEnumThatIsNotInEnumInfoToIdReturnsTheIdOfTheExistingEnum()
  // {
  // EnumInfo existingEnum = new EnumInfo("myClass", "myEnum", 1);
  // EnumInfo equalEnum = new EnumInfo("myClass", "myEnum", 1);
  // typeRegistry.getIdToEnum().put(ENUM_ID, existingEnum);
  // when(typeRegistration.defineEnum(equalEnum)).thenReturn(ENUM_ID);
  //
  // assertThat(typeRegistry.defineEnum(equalEnum)).isEqualTo(ENUM_ID);
  // }
  //
  // @Test
  // public void defineTypeGivenATypeNotEqualToAnExistingTypeWithTheSameIdThrows() {
  // PdxType existingType = mock(PdxType.class);
  // typeRegistry.getTypeToId().put(existingType, null);
  // typeRegistry.getIdToType().put(TYPE_ID, existingType);
  // when(typeRegistration.defineType(newType)).thenReturn(TYPE_ID);
  //
  // assertThatThrownBy(() -> typeRegistry.defineType(newType))
  // .hasMessageContaining("Old type does not equal new type for the same id.");
  // }
  //
  // @Test
  // public void defineEnumGivenAnEnumNotEqualToAnExistingEnumWithTheSameIdThrows() {
  // when(typeRegistration.defineEnum(newEnum)).thenReturn(ENUM_ID);
  // EnumInfo existingEnum = mock(EnumInfo.class);
  //
  // typeRegistry.getIdToEnum().put(ENUM_ID, existingEnum);
  //
  // assertThatThrownBy(() -> typeRegistry.defineEnum(newEnum))
  // .hasMessageContaining("Old enum does not equal new enum for the same id.");
  // }

  @Test
  public void defineLocalTypeGivenNullCallsDefineType() {
    TypeRegistry spy = spy(typeRegistry);

    PdxType result = spy.defineLocalType(null, newType);

    assertThat(result).isSameAs(newType);
    verify(spy).defineType(newType);
  }

  @Test
  public void defineLocalTypeGivenNewTypeAddsItToLocalTypeIds() {
    Object domainInstance = new Object();

    PdxType result = typeRegistry.defineLocalType(domainInstance, newType);

    assertThat(result).isSameAs(newType);
    assertThat(typeRegistry.getLocalTypeIds().get(Object.class)).isSameAs(newType);
  }

  @Test
  public void defineLocalTypeGivenExistingTypeReturnsIt() {
    PdxType existingType = new PdxType("myClass", true);
    PdxType equalType = new PdxType("myClass", true);
    Object domainInstance = new Object();
    typeRegistry.getLocalTypeIds().put(Object.class, existingType);

    PdxType result = typeRegistry.defineLocalType(domainInstance, equalType);

    assertThat(result).isSameAs(existingType);
  }
}
