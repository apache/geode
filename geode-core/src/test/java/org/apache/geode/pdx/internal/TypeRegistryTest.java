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
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.junit.categories.SerializationTest;

@Category({SerializationTest.class})
public class TypeRegistryTest {
  private final InternalCache cache = mock(InternalCache.class);
  private TypeRegistration distributedTypeRegistry = mock(TypeRegistration.class);
  private final TypeRegistry typeRegistry = new TypeRegistry(cache, distributedTypeRegistry);
  private static final String PDX_CLASS_NAME = "pdxClassName";

  @Test
  public void findFieldThatMatchesNameReturnsEmptyGivenNoTypes() {
    Set<PdxType> pdxTypesForClass = Collections.emptySet();
    when(distributedTypeRegistry.getPdxTypesForClassName(PDX_CLASS_NAME))
        .thenReturn(pdxTypesForClass);

    Set<PdxField> foundFields =
        this.typeRegistry.findFieldThatMatchesName(PDX_CLASS_NAME, "fieldName");

    assertThat(foundFields).isEmpty();
  }

  @Test
  public void findFieldThatMatchesNameReturnsFieldThatExactlyMatches() {
    PdxType exactMatchType = mock(PdxType.class);
    PdxField exactMatchField = mock(PdxField.class);
    when(exactMatchType.getPdxField("fieldName")).thenReturn(exactMatchField);
    Set<PdxType> pdxTypesForClass = new HashSet<>(Arrays.asList(exactMatchType));
    when(distributedTypeRegistry.getPdxTypesForClassName(PDX_CLASS_NAME))
        .thenReturn(pdxTypesForClass);

    Set<PdxField> foundFields =
        this.typeRegistry.findFieldThatMatchesName(PDX_CLASS_NAME, "fieldName");

    assertThat(foundFields).containsExactly(exactMatchField);
  }

  @Test
  public void findFieldThatMatchesNameReturnsTwoFieldsThatExactlyMatch() {
    PdxType exactMatchType = mock(PdxType.class);
    PdxField exactMatchField = mock(PdxField.class);
    when(exactMatchType.getPdxField("fieldName")).thenReturn(exactMatchField);
    PdxType exactMatchType2 = mock(PdxType.class);
    PdxField exactMatchField2 = mock(PdxField.class);
    when(exactMatchType2.getPdxField("fieldName")).thenReturn(exactMatchField2);
    Set<PdxType> pdxTypesForClass = new HashSet<>(Arrays.asList(exactMatchType, exactMatchType2));
    when(distributedTypeRegistry.getPdxTypesForClassName(PDX_CLASS_NAME))
        .thenReturn(pdxTypesForClass);

    Set<PdxField> foundFields =
        this.typeRegistry.findFieldThatMatchesName(PDX_CLASS_NAME, "fieldName");

    assertThat(foundFields).containsExactlyInAnyOrder(exactMatchField, exactMatchField2);
  }

  @Test
  public void findFieldThatMatchesNameReturnsFieldThatInexactlyMatches() {
    PdxType inexactMatchType = mock(PdxType.class);
    PdxField inexactMatchField = mock(PdxField.class);
    when(inexactMatchType.getPdxField("fieldName")).thenReturn(null);
    when(inexactMatchType.getFieldNames()).thenReturn(Arrays.asList("skipThisOne", "FIELDNAME"));
    when(inexactMatchType.getPdxField("FIELDNAME")).thenReturn(inexactMatchField);
    Set<PdxType> pdxTypesForClass = new HashSet<>(Arrays.asList(inexactMatchType));
    when(distributedTypeRegistry.getPdxTypesForClassName(PDX_CLASS_NAME))
        .thenReturn(pdxTypesForClass);

    Set<PdxField> foundFields =
        this.typeRegistry.findFieldThatMatchesName(PDX_CLASS_NAME, "fieldName");

    assertThat(foundFields).containsExactly(inexactMatchField);
  }

  @Test
  public void findFieldThatMatchesNameReturnsEmptyIfFieldExistButNoneMatch() {
    PdxType noMatchType = mock(PdxType.class);
    when(noMatchType.getPdxField("fieldName")).thenReturn(null);
    when(noMatchType.getFieldNames()).thenReturn(Arrays.asList("nomatch1", "nomatch2"));
    Set<PdxType> pdxTypesForClass = new HashSet<>(Arrays.asList(noMatchType));
    when(distributedTypeRegistry.getPdxTypesForClassName(PDX_CLASS_NAME))
        .thenReturn(pdxTypesForClass);

    Set<PdxField> foundFields =
        this.typeRegistry.findFieldThatMatchesName(PDX_CLASS_NAME, "fieldName");

    assertThat(foundFields).isEmpty();
  }

  @Test
  public void findFieldThatMatchesNameThrowsIfMoreThanOneMatch() {
    PdxType inexactMatchType = mock(PdxType.class);
    when(inexactMatchType.getPdxField("fieldName")).thenReturn(null);
    when(inexactMatchType.getFieldNames()).thenReturn(Arrays.asList("fieldname", "FIELDNAME"));
    Set<PdxType> pdxTypesForClass = new HashSet<>(Arrays.asList(inexactMatchType));
    when(distributedTypeRegistry.getPdxTypesForClassName(PDX_CLASS_NAME))
        .thenReturn(pdxTypesForClass);

    Throwable throwable = catchThrowable(
        () -> this.typeRegistry.findFieldThatMatchesName(PDX_CLASS_NAME, "fieldName"));

    assertThat(throwable).isInstanceOf(IllegalStateException.class)
        .hasMessage("the pdx fields fieldname, FIELDNAME all match fieldName");
  }
}
