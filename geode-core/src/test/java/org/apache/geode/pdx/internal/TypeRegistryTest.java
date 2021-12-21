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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.junit.categories.SerializationTest;

@Category({SerializationTest.class})
public class TypeRegistryTest {

  private final InternalCache internalCache = mock(InternalCache.class);
  private final TypeRegistration typeRegistration = mock(TypeRegistration.class);
  private final TypeRegistry typeRegistry = new TypeRegistry(internalCache, typeRegistration);

  private final PdxType newType = mock(PdxType.class);

  @Before
  public void setUp() {}

  @Test
  public void defineTypeGivenANewTypeStoresItWithTheCorrectIdAndReturnsIt() {
    when(typeRegistration.defineType(newType)).thenReturn(37);

    PdxType result = typeRegistry.defineType(newType);

    assertThat(result).isSameAs(newType);
    verify(newType).setTypeId(37);
    assertThat(typeRegistry.getIdToType().get(37)).isSameAs(newType);
    assertThat(typeRegistry.getTypeToId().get(newType)).isSameAs(37);
  }

  @Test
  public void defineTypeGivenANewTypeThatIsInTypeToIdButNotIdToTypeStoresItWithTheCorrectIdAndReturnsIt() {
    when(typeRegistration.defineType(newType)).thenReturn(37);
    typeRegistry.getTypeToId().put(newType, 37);
    typeRegistry.getIdToType().put(37, null);

    PdxType result = typeRegistry.defineType(newType);

    assertThat(result).isSameAs(newType);
    verify(newType).setTypeId(37);
    assertThat(typeRegistry.getIdToType().get(37)).isSameAs(newType);
    assertThat(typeRegistry.getTypeToId().get(newType)).isSameAs(37);
  }

  @Test
  public void defineTypeGivenATypeEqualToAnExistingTypeReturnsTheExistingType() {
    PdxType existingType = new PdxType("myClass", true);
    PdxType equalType = new PdxType("myClass", true);
    typeRegistry.getTypeToId().put(existingType, 37);
    typeRegistry.getIdToType().put(37, existingType);

    PdxType result = typeRegistry.defineType(equalType);

    assertThat(result).isSameAs(existingType);
  }

  @Test
  public void defineTypeGivenATypeEqualToAnExistingButNotInTypeToIdTypeReturnsTheExistingType() {
    PdxType existingType = new PdxType("myClass", true);
    PdxType equalType = new PdxType("myClass", true);
    typeRegistry.getTypeToId().put(existingType, null);
    typeRegistry.getIdToType().put(37, existingType);
    when(typeRegistration.defineType(equalType)).thenReturn(37);

    PdxType result = typeRegistry.defineType(equalType);

    assertThat(result).isSameAs(existingType);
  }

  @Test
  public void defineTypeGivenATypeNotEqualToAnExistingButWithTheSameIdThrows() {
    PdxType existingType = mock(PdxType.class);
    typeRegistry.getTypeToId().put(existingType, null);
    typeRegistry.getIdToType().put(37, existingType);
    when(typeRegistration.defineType(newType)).thenReturn(37);

    assertThatThrownBy(() -> typeRegistry.defineType(newType))
        .hasMessageContaining("Old type does not equal new type for the same id.");
  }

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
