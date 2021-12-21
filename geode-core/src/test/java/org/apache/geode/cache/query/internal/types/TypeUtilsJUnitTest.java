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
package org.apache.geode.cache.query.internal.types;

import static org.apache.geode.cache.query.internal.parse.OQLLexerTokenTypes.TOK_EQ;
import static org.apache.geode.cache.query.internal.parse.OQLLexerTokenTypes.TOK_GE;
import static org.apache.geode.cache.query.internal.parse.OQLLexerTokenTypes.TOK_GT;
import static org.apache.geode.cache.query.internal.parse.OQLLexerTokenTypes.TOK_LE;
import static org.apache.geode.cache.query.internal.parse.OQLLexerTokenTypes.TOK_LT;
import static org.apache.geode.cache.query.internal.parse.OQLLexerTokenTypes.TOK_NE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.junit.Test;

import org.apache.geode.InternalGemFireError;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.internal.CompiledValue;
import org.apache.geode.cache.query.internal.parse.OQLLexerTokenTypes;
import org.apache.geode.internal.cache.AbstractRegion;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.pdx.internal.PdxInstanceEnum;
import org.apache.geode.pdx.internal.PdxString;

public class TypeUtilsJUnitTest {
  private final List<Integer> equalityOperators =
      Arrays.stream(new int[] {TOK_EQ, TOK_NE}).boxed().collect(Collectors.toList());
  private final List<Integer> comparisonOperators =
      Arrays.stream(new int[] {TOK_EQ, TOK_LT, TOK_LE, TOK_GT, TOK_GE, TOK_NE}).boxed()
          .collect(Collectors.toList());

  @Test
  public void getTemporalComparatorShouldAlwaysReturnAnInstanceOfTemporalComparator() {
    Comparator comparator = TypeUtils.getTemporalComparator();
    assertThat(comparator).isNotNull();
    assertThat(comparator).isExactlyInstanceOf(TemporalComparator.class);
  }

  @Test
  public void getNumericComparatorShouldAlwaysReturnAnInstanceOfNumericComparator() {
    Comparator comparator = TypeUtils.getNumericComparator();
    assertThat(comparator).isNotNull();
    assertThat(comparator).isExactlyInstanceOf(NumericComparator.class);
  }

  @Test
  public void getExtendedNumericComparatorShouldAlwaysReturnAnInstanceOfExtendedNumericComparator() {
    Comparator comparator = TypeUtils.getExtendedNumericComparator();
    assertThat(comparator).isNotNull();
    assertThat(comparator).isExactlyInstanceOf(ExtendedNumericComparator.class);
  }

  @Test
  public void checkCastShouldReturnNullWhenTargetObjectIsNull() {
    Object result = TypeUtils.checkCast(null, CompiledValue.class);
    assertThat(result).isNull();
  }

  @Test
  public void checkCastShouldThrowExceptionWhenTargetObjectCanNotBeTypeCasted() {
    assertThatThrownBy(() -> TypeUtils.checkCast("SomeCharacters", Integer.class))
        .isInstanceOf(InternalGemFireError.class)
        .hasMessageMatching("^expected instance of (.*) but was (.*)$");
  }

  /**
   * Can't test every possible combination, so try a few ones.
   */
  @Test
  public void checkCastShouldReturnCorrectlyWhenTargetObjectIsNotNullAndCanBeTypeCasted() {
    Object stringCastTarget = "SomeCharacters";
    Object stringCastResult = TypeUtils.checkCast(stringCastTarget, String.class);
    assertThat(stringCastResult).isNotNull();
    assertThat(stringCastResult).isInstanceOf(String.class);
    assertThat(stringCastResult).isSameAs(stringCastTarget);

    Object integerCastTarget = 20;
    Object integerCastResult = TypeUtils.checkCast(integerCastTarget, Integer.class);
    assertThat(integerCastResult).isNotNull();
    assertThat(integerCastResult).isInstanceOf(Integer.class);
    assertThat(integerCastResult).isSameAs(integerCastResult);

    Object numberCastResult = TypeUtils.checkCast(integerCastTarget, Number.class);
    assertThat(numberCastResult).isNotNull();
    assertThat(numberCastResult).isInstanceOf(Integer.class);
    assertThat(numberCastResult).isSameAs(numberCastResult);
  }

  @Test
  public void indexKeyForShouldReturnNullWhenTheKeyIsNull() throws TypeMismatchException {
    Object key = TypeUtils.indexKeyFor(null);
    assertThat(key).isNull();
  }

  @Test
  public void indexKeyForShouldThrowExceptionWhenTheKeyTypeCanNotBeUsedAsIndex() {
    assertThatThrownBy(() -> TypeUtils.indexKeyFor(new AtomicInteger(0)))
        .isInstanceOf(TypeMismatchException.class)
        .hasMessageMatching("^Indexes are not supported for type ' (.*) '$");
  }

  @Test
  public void indexKeyForShouldReturnIntegerWhenObjectIsInstanceOfByte()
      throws TypeMismatchException {
    Object keyByte = new Byte("5");
    Object keyByteResult = TypeUtils.indexKeyFor(keyByte);
    assertThat(keyByteResult).isNotNull();
    assertThat(keyByteResult).isInstanceOf(Integer.class);
    assertThat(keyByteResult).isEqualTo(new Integer("5"));
  }

  @Test
  public void indexKeyForShouldReturnIntegerWhenObjectIsInstanceOfShort()
      throws TypeMismatchException {
    Object keyShort = new Short("10");
    Object keyShortResult = TypeUtils.indexKeyFor(keyShort);
    assertThat(keyShortResult).isNotNull();
    assertThat(keyShortResult).isInstanceOf(Integer.class);
    assertThat(keyShortResult).isEqualTo(new Integer("10"));
  }

  @Test
  public void indexKeyForShouldReturnPdxInstanceEnumWhenObjectIsInstanceOfEnum()
      throws TypeMismatchException {
    Object keyEnum = TimeUnit.SECONDS;
    Object keyEnumResult = TypeUtils.indexKeyFor(keyEnum);
    assertThat(keyEnumResult).isNotNull();
    assertThat(keyEnumResult).isInstanceOf(PdxInstanceEnum.class);
    assertThat(((PdxInstanceEnum) keyEnumResult).getName()).isEqualTo(TimeUnit.SECONDS.name());
    assertThat(((PdxInstanceEnum) keyEnumResult).getOrdinal())
        .isEqualTo(TimeUnit.SECONDS.ordinal());
    assertThat(((PdxInstanceEnum) keyEnumResult).getClassName())
        .isEqualTo(TimeUnit.SECONDS.getDeclaringClass().getName());
  }

  @Test
  public void indexKeyForShouldReturnIdentityWhenObjectIsInstanceOfComparable()
      throws TypeMismatchException {
    Object keyComparable = "myKey";
    Object keyComparableResult = TypeUtils.indexKeyFor(keyComparable);
    assertThat(keyComparableResult).isNotNull();
    assertThat(keyComparableResult).isSameAs(keyComparable);

    Object customComparableKey = (Comparable) o -> 0;

    Object customComparableKeyResult = TypeUtils.indexKeyFor(customComparableKey);
    assertThat(customComparableKeyResult).isNotNull();
    assertThat(customComparableKeyResult).isSameAs(customComparableKey);
  }

  /**
   * Can't test every possible combination so try the known, relevant ones.
   */
  @Test
  public void isAssignableFromShouldWorkProperlyForKnownTypes() {
    // Booleans
    assertThat(TypeUtils.isAssignableFrom(Boolean.class, Comparable.class)).isTrue();
    assertThat(TypeUtils.isAssignableFrom(Boolean.class, Comparable.class)).isTrue();
    assertThat(TypeUtils.isAssignableFrom(AtomicBoolean.class, Comparable.class)).isFalse();

    // Dates supported by the TemporalComparator
    assertThat(TypeUtils.isAssignableFrom(Date.class, Date.class)).isTrue();
    assertThat(TypeUtils.isAssignableFrom(Long.class, Date.class)).isFalse();
    assertThat(TypeUtils.isAssignableFrom(java.sql.Date.class, Date.class)).isTrue();
    assertThat(TypeUtils.isAssignableFrom(java.sql.Time.class, Date.class)).isTrue();
    assertThat(TypeUtils.isAssignableFrom(java.sql.Timestamp.class, Date.class)).isTrue();

    // Numbers supported by the NumericComparator
    assertThat(TypeUtils.isAssignableFrom(Short.class, Number.class)).isTrue();
    assertThat(TypeUtils.isAssignableFrom(Long.class, Number.class)).isTrue();
    assertThat(TypeUtils.isAssignableFrom(Float.class, Number.class)).isTrue();
    assertThat(TypeUtils.isAssignableFrom(Double.class, Number.class)).isTrue();
    assertThat(TypeUtils.isAssignableFrom(BigDecimal.class, Number.class)).isTrue();
    assertThat(TypeUtils.isAssignableFrom(Integer.class, Number.class)).isTrue();
    assertThat(TypeUtils.isAssignableFrom(BigInteger.class, Number.class)).isTrue();
    assertThat(TypeUtils.isAssignableFrom(AtomicInteger.class, Number.class)).isTrue();

    // Comparable Interface
    assertThat(TypeUtils.isAssignableFrom(PdxString.class, Comparable.class)).isTrue();
    assertThat(TypeUtils.isAssignableFrom(Long.class, Comparable.class)).isTrue();
    assertThat(TypeUtils.isAssignableFrom(Integer.class, Comparable.class)).isTrue();
  }

  @Test
  public void isTypeConvertibleShouldDelegateToIsAssignableFromMethodForNonWrappedTypesAndNullSourceType() {
    // Special classes (Enum, Object, Class, Interface) and srcType as null.
    assertThat(TypeUtils.isTypeConvertible(null, Enum.class)).isTrue();
    assertThat(TypeUtils.isAssignableFrom(Enum.class, Object.class)).isTrue();

    assertThat(TypeUtils.isTypeConvertible(null, Object.class)).isTrue();
    assertThat(TypeUtils.isAssignableFrom(Object.class, Object.class)).isTrue();

    assertThat(TypeUtils.isTypeConvertible(null, Class.class)).isTrue();
    assertThat(TypeUtils.isAssignableFrom(Class.class, Object.class)).isTrue();

    assertThat(TypeUtils.isTypeConvertible(null, TimeUnit.class)).isTrue();
    assertThat(TypeUtils.isAssignableFrom(TimeUnit.class, Object.class)).isTrue();

    assertThat(TypeUtils.isTypeConvertible(null, Serializable.class)).isTrue();
    assertThat(TypeUtils.isAssignableFrom(Serializable.class, Object.class)).isTrue();

    // Regular, non java wrapped classes.
    assertThat(TypeUtils.isTypeConvertible(AtomicInteger.class, Number.class)).isTrue();
    assertThat(TypeUtils.isAssignableFrom(AtomicInteger.class, Number.class)).isTrue();

    assertThat(TypeUtils.isTypeConvertible(NumericComparator.class, Comparator.class)).isTrue();
    assertThat(TypeUtils.isAssignableFrom(NumericComparator.class, Comparator.class)).isTrue();

    assertThat(TypeUtils.isTypeConvertible(PartitionedRegion.class, LocalRegion.class)).isTrue();
    assertThat(TypeUtils.isAssignableFrom(PartitionedRegion.class, LocalRegion.class)).isTrue();
  }

  @Test
  public void isTypeConvertibleShouldReturnTrueForBooleanPrimitivesAndWrappers() {
    assertThat(TypeUtils.isTypeConvertible(null, Boolean.class)).isTrue();
    assertThat(TypeUtils.isTypeConvertible(Boolean.TYPE, Boolean.TYPE)).isTrue();
    assertThat(TypeUtils.isTypeConvertible(Boolean.TYPE, Boolean.class)).isTrue();
    assertThat(TypeUtils.isTypeConvertible(Boolean.class, Boolean.TYPE)).isTrue();
    assertThat(TypeUtils.isTypeConvertible(Boolean.class, Boolean.class)).isTrue();
  }

  @Test
  public void isTypeConvertibleShouldReturnTrueForNumericPrimitivesAndWrappers() {
    for (int i = 0; i < TypeUtils._numericPrimitiveClasses.size(); i++) {
      Class sourceType = TypeUtils._numericPrimitiveClasses.get(i);

      TypeUtils._numericPrimitiveClasses.stream().skip(i).forEachOrdered(
          destType -> assertThat(TypeUtils.isTypeConvertible(sourceType, destType)).isTrue());
      TypeUtils._numericWrapperClasses.stream().skip(i).limit(1).forEachOrdered(
          destType -> assertThat(TypeUtils.isTypeConvertible(sourceType, destType)).isTrue());
    }

    for (int i = 0; i < TypeUtils._numericWrapperClasses.size(); i++) {
      Class sourceType = TypeUtils._numericWrapperClasses.get(i);

      TypeUtils._numericPrimitiveClasses.stream().skip(i).forEachOrdered(
          destType -> assertThat(TypeUtils.isTypeConvertible(sourceType, destType)).isTrue());
      TypeUtils._numericWrapperClasses.stream().skip(i).limit(1).forEachOrdered(
          destType -> assertThat(TypeUtils.isTypeConvertible(sourceType, destType)).isTrue());
    }
  }

  @Test
  public void isTypeConvertibleShouldReturnTrueForCharacterPrimitivesAndWrappers() {
    assertThat(TypeUtils.isTypeConvertible(null, Character.class)).isTrue();
    assertThat(TypeUtils.isTypeConvertible(Character.TYPE, Character.TYPE)).isTrue();
    assertThat(TypeUtils.isTypeConvertible(Character.TYPE, Character.class)).isTrue();
    assertThat(TypeUtils.isTypeConvertible(Character.class, Character.TYPE)).isTrue();
    assertThat(TypeUtils.isTypeConvertible(Character.class, Character.class)).isTrue();
  }

  @Test
  public void areTypesConvertibleShouldThrowExceptionWhenTheCollectionSizesAreDifferent() {
    Class[] srcTypes = new Class[] {Byte.class};
    Class[] destTypes = new Class[] {Integer.class, Long.class};
    assertThatThrownBy(() -> TypeUtils.areTypesConvertible(srcTypes, destTypes))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Arguments 'srcTypes' and 'destTypes' must be of same length");
  }

  @Test
  public void areTypesConvertibleShouldReturnTrueIfAllTypesWithinTheCollectionsAreConvertible() {
    Class[] srcTypes =
        new Class[] {null, Byte.TYPE, Character.TYPE, Boolean.TYPE, NumericComparator.class};
    Class[] destTypes =
        new Class[] {Object.class, Integer.TYPE, Character.class, Boolean.class, Comparator.class};

    assertThat(TypeUtils.areTypesConvertible(srcTypes, destTypes)).isTrue();
  }

  @Test
  public void areTypesConvertibleShouldReturnFalseIfAtLeastOneTypeWithinTheCollectionsIsNotConvertible() {
    Class[] srcTypes =
        new Class[] {null, Byte.TYPE, Character.TYPE, Boolean.TYPE, NumericComparator.class};
    Class[] destTypes =
        new Class[] {Object.class, Integer.TYPE, Character.class, Object.class, Comparator.class};

    assertThat(TypeUtils.areTypesConvertible(srcTypes, destTypes)).isFalse();
  }

  @Test
  public void getObjectTypeShouldReturnTheProperTypeImplementation() {
    // Object
    ObjectTypeAssert.assertThat(TypeUtils.getObjectType(Object.class)).resolves(Object.class)
        .isObject();

    // Collections
    ObjectTypeAssert.assertThat(TypeUtils.getObjectType(Vector.class)).resolves(Vector.class)
        .isCollectionOf(ObjectTypeImpl.class);
    ObjectTypeAssert.assertThat(TypeUtils.getObjectType(HashSet.class)).resolves(HashSet.class)
        .isCollectionOf(ObjectTypeImpl.class);
    ObjectTypeAssert.assertThat(TypeUtils.getObjectType(ArrayList.class)).resolves(ArrayList.class)
        .isCollectionOf(ObjectTypeImpl.class);
    ObjectTypeAssert.assertThat(TypeUtils.getObjectType(PriorityQueue.class))
        .resolves(PriorityQueue.class).isCollectionOf(ObjectTypeImpl.class);
    ObjectTypeAssert.assertThat(TypeUtils.getObjectType(LinkedBlockingQueue.class))
        .resolves(LinkedBlockingQueue.class).isCollectionOf(ObjectTypeImpl.class);

    // Typed Collections
    Integer[] integers = new Integer[] {};
    ObjectTypeAssert.assertThat(TypeUtils.getObjectType(integers.getClass()))
        .resolves(Integer[].class).isCollectionOf(ObjectTypeImpl.class);

    Vector[] vectors = new Vector[] {};
    ObjectTypeAssert.assertThat(TypeUtils.getObjectType(vectors.getClass()))
        .resolves(Vector[].class).isCollectionOf(CollectionTypeImpl.class);

    // Regions
    ObjectTypeAssert.assertThat(TypeUtils.getObjectType(Region.class)).resolves(Region.class)
        .isCollectionOf(ObjectTypeImpl.class);
    ObjectTypeAssert.assertThat(TypeUtils.getObjectType(BucketRegion.class))
        .resolves(BucketRegion.class).isCollectionOf(ObjectTypeImpl.class);
    ObjectTypeAssert.assertThat(TypeUtils.getObjectType(AbstractRegion.class))
        .resolves(AbstractRegion.class).isCollectionOf(ObjectTypeImpl.class);
    ObjectTypeAssert.assertThat(TypeUtils.getObjectType(PartitionedRegion.class))
        .resolves(PartitionedRegion.class).isCollectionOf(ObjectTypeImpl.class);

    // Regular Maps
    ObjectTypeAssert.assertThat(TypeUtils.getObjectType(Map.class)).resolves(Map.class)
        .isMapOf(ObjectTypeImpl.class, ObjectTypeImpl.class);
    ObjectTypeAssert.assertThat(TypeUtils.getObjectType(HashMap.class)).resolves(HashMap.class)
        .isMapOf(ObjectTypeImpl.class, ObjectTypeImpl.class);
    ObjectTypeAssert.assertThat(TypeUtils.getObjectType(Hashtable.class)).resolves(Hashtable.class)
        .isMapOf(ObjectTypeImpl.class, ObjectTypeImpl.class);
    ObjectTypeAssert.assertThat(TypeUtils.getObjectType(ConcurrentHashMap.class))
        .resolves(ConcurrentHashMap.class).isMapOf(ObjectTypeImpl.class, ObjectTypeImpl.class);

    // Other Classes
    ObjectTypeAssert.assertThat(TypeUtils.getObjectType(String.class)).resolves(String.class)
        .isObject();
    ObjectTypeAssert.assertThat(TypeUtils.getObjectType(Integer.class)).resolves(Integer.class)
        .isObject();
    ObjectTypeAssert.assertThat(TypeUtils.getObjectType(BigDecimal.class))
        .resolves(BigDecimal.class).isObject();
  }

  @Test
  public void getRegionEntryTypeShouldReturnTheProperTypeImplementation() {
    ObjectTypeAssert.assertThat(TypeUtils.getRegionEntryType(mock(Region.class)))
        .resolves(Region.Entry.class).isObject();
    ObjectTypeAssert.assertThat(TypeUtils.getRegionEntryType(mock(BucketRegion.class)))
        .resolves(Region.Entry.class).isObject();
    ObjectTypeAssert.assertThat(TypeUtils.getRegionEntryType(mock(AbstractRegion.class)))
        .resolves(Region.Entry.class).isObject();
    ObjectTypeAssert.assertThat(TypeUtils.getRegionEntryType(mock(PartitionedRegion.class)))
        .resolves(Region.Entry.class).isObject();
  }

  @Test
  public void booleanCompareShouldThrowExceptionIfValuesAreNotInstancesOfBoolean() {
    Integer arbitraryInput = 10;
    assertThatThrownBy(() -> TypeUtils.booleanCompare(true, new Object(), arbitraryInput))
        .isInstanceOf(TypeMismatchException.class)
        .hasMessageMatching("^Booleans can only be compared with booleans$");
    assertThatThrownBy(() -> TypeUtils.booleanCompare(new Object(), false, arbitraryInput))
        .isInstanceOf(TypeMismatchException.class)
        .hasMessageMatching("^Booleans can only be compared with booleans$");
    assertThatThrownBy(() -> TypeUtils.booleanCompare(new Object(), new Object(), arbitraryInput))
        .isInstanceOf(TypeMismatchException.class)
        .hasMessageMatching("^Booleans can only be compared with booleans$");
  }

  @Test
  public void booleanCompareShouldThrowExceptionForNonEqualityComparisonOperators() {
    OQLLexerTokenTypes tempInstance = new OQLLexerTokenTypes() {};
    Field[] fields = OQLLexerTokenTypes.class.getDeclaredFields();

    Arrays.stream(fields).forEach(field -> {
      try {
        int token = field.getInt(tempInstance);
        if (!equalityOperators.contains(token)) {
          assertThatThrownBy(() -> TypeUtils.booleanCompare(true, false, token))
              .isInstanceOf(TypeMismatchException.class)
              .hasMessageMatching("^Boolean values can only be compared with = or <>$");
        }
      } catch (IllegalAccessException exception) {
        throw new RuntimeException(exception);
      }
    });
  }

  @Test
  public void booleanCompareShouldReturnCorrectlyForEqualityComparisonOperators()
      throws TypeMismatchException {
    assertThat(TypeUtils.booleanCompare(true, true, TOK_EQ)).isTrue();
    assertThat(TypeUtils.booleanCompare(false, false, TOK_EQ)).isTrue();
    assertThat(TypeUtils.booleanCompare(true, false, TOK_EQ)).isFalse();
    assertThat(TypeUtils.booleanCompare(false, true, TOK_EQ)).isFalse();

    assertThat(TypeUtils.booleanCompare(Boolean.TRUE, Boolean.TRUE, TOK_EQ)).isTrue();
    assertThat(TypeUtils.booleanCompare(Boolean.FALSE, Boolean.FALSE, TOK_EQ)).isTrue();
    assertThat(TypeUtils.booleanCompare(Boolean.TRUE, Boolean.FALSE, TOK_EQ)).isFalse();
    assertThat(TypeUtils.booleanCompare(Boolean.FALSE, Boolean.TRUE, TOK_EQ)).isFalse();
  }

  @Test
  public void comparingNullValuesShouldReturnBooleanOrUndefined() throws TypeMismatchException {
    assertThat(TypeUtils.compare(null, null, OQLLexerTokenTypes.TOK_EQ)).isNotNull()
        .isEqualTo(Boolean.TRUE);
    assertThat(TypeUtils.compare(null, new Object(), OQLLexerTokenTypes.TOK_EQ)).isNotNull()
        .isEqualTo(Boolean.FALSE);
    assertThat(TypeUtils.compare(new Object(), null, OQLLexerTokenTypes.TOK_EQ)).isNotNull()
        .isEqualTo(Boolean.FALSE);

    assertThat(TypeUtils.compare(null, null, OQLLexerTokenTypes.TOK_NE)).isNotNull()
        .isEqualTo(Boolean.FALSE);
    assertThat(TypeUtils.compare(null, new Object(), OQLLexerTokenTypes.TOK_NE)).isNotNull()
        .isEqualTo(Boolean.TRUE);
    assertThat(TypeUtils.compare(new Object(), null, OQLLexerTokenTypes.TOK_NE)).isNotNull()
        .isEqualTo(Boolean.TRUE);

    OQLLexerTokenTypes tempInstance = new OQLLexerTokenTypes() {};
    Field[] fields = OQLLexerTokenTypes.class.getDeclaredFields();

    Arrays.stream(fields).forEach(field -> {
      try {
        int token = field.getInt(tempInstance);
        if (!equalityOperators.contains(token)) {
          assertThat(TypeUtils.compare(new Object(), null, token))
              .isEqualTo(QueryService.UNDEFINED);
        }
      } catch (IllegalAccessException | TypeMismatchException exception) {
        throw new RuntimeException(exception);
      }
    });
  }

  @Test
  public void comparingUndefinedValuesShouldReturnBooleanOrUndefined()
      throws TypeMismatchException {
    assertThat(TypeUtils.compare(QueryService.UNDEFINED, new Object(), OQLLexerTokenTypes.TOK_NE))
        .isNotNull().isEqualTo(Boolean.TRUE);
    assertThat(TypeUtils.compare(new Object(), QueryService.UNDEFINED, OQLLexerTokenTypes.TOK_NE))
        .isNotNull().isEqualTo(Boolean.TRUE);
    assertThat(TypeUtils.compare(QueryService.UNDEFINED, QueryService.UNDEFINED,
        OQLLexerTokenTypes.TOK_EQ)).isNotNull().isEqualTo(Boolean.TRUE);

    assertThat(TypeUtils.compare(QueryService.UNDEFINED, new Object(), OQLLexerTokenTypes.TOK_EQ))
        .isNotNull().isEqualTo(QueryService.UNDEFINED);
    assertThat(TypeUtils.compare(new Object(), QueryService.UNDEFINED, OQLLexerTokenTypes.TOK_EQ))
        .isNotNull().isEqualTo(QueryService.UNDEFINED);
    assertThat(TypeUtils.compare(QueryService.UNDEFINED, QueryService.UNDEFINED,
        OQLLexerTokenTypes.TOK_NE)).isNotNull().isEqualTo(QueryService.UNDEFINED);
  }

  @Test
  public void comparingEquivalentPdxStringToStringShouldMatchCorrectly() throws Exception {
    String theString = "MyString";
    PdxString pdxString = new PdxString(theString);

    assertThat(TypeUtils.compare(pdxString, theString, OQLLexerTokenTypes.TOK_EQ))
        .isInstanceOf(Boolean.class);
    assertThat((Boolean) TypeUtils.compare(pdxString, theString, OQLLexerTokenTypes.TOK_EQ))
        .isTrue();

    assertThat(TypeUtils.compare(theString, pdxString, OQLLexerTokenTypes.TOK_EQ))
        .isInstanceOf(Boolean.class);
    assertThat((Boolean) TypeUtils.compare(pdxString, theString, OQLLexerTokenTypes.TOK_EQ))
        .isTrue();
  }

  @Test
  public void comparingUnequalPdxStringToStringShouldNotMatch() throws Exception {
    String theString = "MyString";
    PdxString pdxString = new PdxString("AnotherString");

    assertThat(TypeUtils.compare(pdxString, theString, OQLLexerTokenTypes.TOK_EQ))
        .isInstanceOf(Boolean.class);
    assertThat((Boolean) TypeUtils.compare(pdxString, theString, OQLLexerTokenTypes.TOK_EQ))
        .isFalse();

    assertThat(TypeUtils.compare(pdxString, theString, OQLLexerTokenTypes.TOK_EQ))
        .isInstanceOf(Boolean.class);
    assertThat((Boolean) TypeUtils.compare(theString, pdxString, OQLLexerTokenTypes.TOK_EQ))
        .isFalse();
  }

  @Test
  public void comparingTemporalValuesIsEnabled() {
    // Spies to make sure that other comparison methods are not executed.
    Date beginningOfTimeAsDate = spy(new Date(0L));
    Date currentCalendarTimeAsDate = spy(Calendar.getInstance().getTime());
    java.sql.Date beginningOfTimeAsSqlDate =
        spy(new java.sql.Date(beginningOfTimeAsDate.getTime()));
    java.sql.Date currentCalendarTimeAsSqlDate =
        spy(new java.sql.Date(currentCalendarTimeAsDate.getTime()));
    java.sql.Time beginningOfTimeAsSqlTime =
        spy(new java.sql.Time(beginningOfTimeAsDate.getTime()));
    java.sql.Time currentCalendarTimeAsSqlTime =
        spy(new java.sql.Time(currentCalendarTimeAsDate.getTime()));
    java.sql.Timestamp beginningOfTimeAsSqlTimestamp =
        spy(new java.sql.Timestamp(beginningOfTimeAsDate.getTime()));
    java.sql.Timestamp currentCalendarTimeAsSqlTimestamp =
        spy(new java.sql.Timestamp(currentCalendarTimeAsDate.getTime()));

    List<Object> originDates = Arrays.asList(new Object[] {beginningOfTimeAsDate,
        beginningOfTimeAsSqlDate, beginningOfTimeAsSqlTime, beginningOfTimeAsSqlTimestamp});
    List<Object> currentDates =
        Arrays.asList(new Object[] {currentCalendarTimeAsDate, currentCalendarTimeAsSqlDate,
            currentCalendarTimeAsSqlTime, currentCalendarTimeAsSqlTimestamp});

    originDates.forEach(originDate -> originDates.forEach(originDate2 -> {
      try {
        assertThat(TypeUtils.compare(originDate, originDate2, OQLLexerTokenTypes.TOK_EQ))
            .isEqualTo(Boolean.TRUE);
        assertThat(TypeUtils.compare(originDate, originDate2, OQLLexerTokenTypes.TOK_LT))
            .isEqualTo(Boolean.FALSE);
        assertThat(TypeUtils.compare(originDate, originDate2, OQLLexerTokenTypes.TOK_LE))
            .isEqualTo(Boolean.TRUE);
        assertThat(TypeUtils.compare(originDate, originDate2, OQLLexerTokenTypes.TOK_GT))
            .isEqualTo(Boolean.FALSE);
        assertThat(TypeUtils.compare(originDate, originDate2, OQLLexerTokenTypes.TOK_GE))
            .isEqualTo(Boolean.TRUE);
        assertThat(TypeUtils.compare(originDate, originDate2, OQLLexerTokenTypes.TOK_NE))
            .isEqualTo(Boolean.FALSE);
      } catch (TypeMismatchException typeMismatchException) {
        throw new RuntimeException(typeMismatchException);
      }
    }));

    originDates.forEach(originDate -> currentDates.forEach(currentTime -> {
      try {
        assertThat(TypeUtils.compare(originDate, currentTime, OQLLexerTokenTypes.TOK_EQ))
            .isEqualTo(Boolean.FALSE);
        assertThat(TypeUtils.compare(originDate, currentTime, OQLLexerTokenTypes.TOK_LT))
            .isEqualTo(Boolean.TRUE);
        assertThat(TypeUtils.compare(originDate, currentTime, OQLLexerTokenTypes.TOK_LE))
            .isEqualTo(Boolean.TRUE);
        assertThat(TypeUtils.compare(originDate, currentTime, OQLLexerTokenTypes.TOK_GT))
            .isEqualTo(Boolean.FALSE);
        assertThat(TypeUtils.compare(originDate, currentTime, OQLLexerTokenTypes.TOK_GE))
            .isEqualTo(Boolean.FALSE);
        assertThat(TypeUtils.compare(originDate, currentTime, OQLLexerTokenTypes.TOK_NE))
            .isEqualTo(Boolean.TRUE);
      } catch (TypeMismatchException typeMismatchException) {
        throw new RuntimeException(typeMismatchException);
      }
    }));

    currentDates.forEach(currentTime -> originDates.forEach(originDate -> {
      try {
        assertThat(TypeUtils.compare(currentTime, originDate, OQLLexerTokenTypes.TOK_EQ))
            .isEqualTo(Boolean.FALSE);
        assertThat(TypeUtils.compare(currentTime, originDate, OQLLexerTokenTypes.TOK_LT))
            .isEqualTo(Boolean.FALSE);
        assertThat(TypeUtils.compare(currentTime, originDate, OQLLexerTokenTypes.TOK_LE))
            .isEqualTo(Boolean.FALSE);
        assertThat(TypeUtils.compare(currentTime, originDate, OQLLexerTokenTypes.TOK_GT))
            .isEqualTo(Boolean.TRUE);
        assertThat(TypeUtils.compare(currentTime, originDate, OQLLexerTokenTypes.TOK_GE))
            .isEqualTo(Boolean.TRUE);
        assertThat(TypeUtils.compare(currentTime, originDate, OQLLexerTokenTypes.TOK_NE))
            .isEqualTo(Boolean.TRUE);
      } catch (TypeMismatchException typeMismatchException) {
        throw new RuntimeException(typeMismatchException);
      }
    }));
  }

  @Test
  public void comparingTemporalValuesShouldThrowExceptionWhenTheComparisonOperatorIsNotSupported() {
    OQLLexerTokenTypes tempInstance = new OQLLexerTokenTypes() {};
    Field[] fields = OQLLexerTokenTypes.class.getDeclaredFields();

    Arrays.stream(fields).forEach(field -> {
      try {
        int token = field.getInt(tempInstance);
        if (!comparisonOperators.contains(token)) {
          assertThatThrownBy(() -> TypeUtils.compare(new Date(), new Date(), token))
              .isInstanceOf(IllegalArgumentException.class)
              .hasMessageMatching("^Unknown operator: (.*)$");
        }
      } catch (IllegalAccessException exception) {
        throw new RuntimeException(exception);
      }
    });
  }

  @Test
  public void comparingTemporalValuesForWhichTheComparatorThrowsTypeMismatchExceptionShouldReturnBooleanWhenTheComparisonOperatorIsSupported()
      throws TypeMismatchException {
    assertThat(TypeUtils.compare(new Date(12345), new Integer(12345), OQLLexerTokenTypes.TOK_NE))
        .isEqualTo(Boolean.TRUE);
    assertThat(TypeUtils.compare(new Date(12345), new Integer(12345), OQLLexerTokenTypes.TOK_EQ))
        .isEqualTo(Boolean.FALSE);

    OQLLexerTokenTypes tempInstance = new OQLLexerTokenTypes() {};
    Field[] fields = OQLLexerTokenTypes.class.getDeclaredFields();

    Arrays.stream(fields).forEach(field -> {
      try {
        int token = field.getInt(tempInstance);
        if (!equalityOperators.contains(token)) {
          assertThatThrownBy(() -> TypeUtils.compare(new Date(), new Integer(0), token))
              .isInstanceOf(TypeMismatchException.class).hasMessageMatching(
                  "^Unable to compare object of type ' (.*) ' with object of type ' (.*) '$");
        }
      } catch (IllegalAccessException exception) {
        throw new RuntimeException(exception);
      }
    });
  }

  @Test
  public void comparingNumericValuesIsEnabled() {
    // Can't spy final classes, nor primitives.
    long lLong = Short.MIN_VALUE;
    long hLong = Short.MAX_VALUE;
    int lInteger = Short.MIN_VALUE;
    int hInteger = Short.MAX_VALUE;
    float lFloat = Short.MIN_VALUE;
    float hFloat = Short.MAX_VALUE;
    short lShort = Short.MIN_VALUE;
    short hShort = Short.MAX_VALUE;
    double lDouble = Short.MIN_VALUE;
    double hDouble = Short.MAX_VALUE;
    Long lowestLong = (long) Short.MIN_VALUE;
    Long highestLong = (long) Short.MAX_VALUE;
    Float lowestFloat = (float) Short.MIN_VALUE;
    Float highestFloat = (float) Short.MAX_VALUE;
    Short lowestShort = Short.MIN_VALUE;
    Short highestShort = Short.MAX_VALUE;
    Double lowestDouble = (double) Short.MIN_VALUE;
    Double highestDouble = (double) Short.MAX_VALUE;
    Integer lowestInteger = (int) Short.MIN_VALUE;
    Integer highestInteger = (int) Short.MAX_VALUE;
    AtomicLong lowestAtomicLong = new AtomicLong(Short.MIN_VALUE);
    AtomicLong highestAtomicLong = new AtomicLong(Short.MAX_VALUE);
    BigDecimal lowestBigDecimal = BigDecimal.valueOf(Short.MIN_VALUE);
    BigDecimal highestBigDecimal = BigDecimal.valueOf(Short.MAX_VALUE);
    BigInteger lowestBigInteger = BigInteger.valueOf(Short.MIN_VALUE);
    BigInteger highestBigInteger = BigInteger.valueOf(Short.MAX_VALUE);
    AtomicInteger lowestAtomicInteger = new AtomicInteger(Short.MIN_VALUE);
    AtomicInteger highestAtomicInteger = new AtomicInteger(Short.MAX_VALUE);

    List<Number> lowestNumbers = Arrays.asList(lInteger, lLong, lFloat, lShort, lDouble, lowestLong,
        lowestFloat, lowestShort, lowestDouble, lowestInteger, lowestAtomicLong, lowestBigDecimal,
        lowestBigInteger, lowestAtomicInteger);
    List<Number> highestNumbers = Arrays.asList(hInteger, hLong, hFloat, hShort, hDouble,
        highestLong, highestFloat, highestShort, highestDouble, highestInteger, highestAtomicLong,
        highestBigDecimal, highestBigInteger, highestAtomicInteger);

    lowestNumbers.forEach(lowest -> lowestNumbers.stream()
        .filter(number -> number.getClass() != lowest.getClass()).forEach((lowest2) -> {
          try {
            assertThat(TypeUtils.compare(lowest, lowest2, OQLLexerTokenTypes.TOK_EQ))
                .isEqualTo(Boolean.TRUE);
            assertThat(TypeUtils.compare(lowest, lowest2, OQLLexerTokenTypes.TOK_LT))
                .isEqualTo(Boolean.FALSE);
            assertThat(TypeUtils.compare(lowest, lowest2, OQLLexerTokenTypes.TOK_NE))
                .isEqualTo(Boolean.FALSE);
            assertThat(TypeUtils.compare(lowest, lowest2, OQLLexerTokenTypes.TOK_LE))
                .isEqualTo(Boolean.TRUE);
            assertThat(TypeUtils.compare(lowest, lowest2, OQLLexerTokenTypes.TOK_GT))
                .isEqualTo(Boolean.FALSE);
            assertThat(TypeUtils.compare(lowest, lowest2, OQLLexerTokenTypes.TOK_GE))
                .isEqualTo(Boolean.TRUE);
          } catch (TypeMismatchException typeMismatchException) {
            throw new RuntimeException(typeMismatchException);
          }
        }));

    lowestNumbers.forEach(lowestNumber -> highestNumbers.stream()
        .filter(number -> number.getClass() != lowestNumber.getClass()).forEach((highestNumber) -> {
          try {
            assertThat(TypeUtils.compare(lowestNumber, highestNumber, OQLLexerTokenTypes.TOK_EQ))
                .isEqualTo(Boolean.FALSE);
            assertThat(TypeUtils.compare(lowestNumber, highestNumber, OQLLexerTokenTypes.TOK_LT))
                .isEqualTo(Boolean.TRUE);
            assertThat(TypeUtils.compare(lowestNumber, highestNumber, OQLLexerTokenTypes.TOK_GT))
                .isEqualTo(Boolean.FALSE);
            assertThat(TypeUtils.compare(lowestNumber, highestNumber, OQLLexerTokenTypes.TOK_LE))
                .isEqualTo(Boolean.TRUE);
            assertThat(TypeUtils.compare(lowestNumber, highestNumber, OQLLexerTokenTypes.TOK_GE))
                .isEqualTo(Boolean.FALSE);
            assertThat(TypeUtils.compare(lowestNumber, highestNumber, OQLLexerTokenTypes.TOK_NE))
                .isEqualTo(Boolean.TRUE);
          } catch (TypeMismatchException typeMismatchException) {
            throw new RuntimeException(typeMismatchException);
          }
        }));

    highestNumbers.forEach(highestNumber -> lowestNumbers.stream()
        .filter(number -> number.getClass() != highestNumber.getClass()).forEach((lowestNumber) -> {
          try {
            assertThat(TypeUtils.compare(highestNumber, lowestNumber, OQLLexerTokenTypes.TOK_EQ))
                .isEqualTo(Boolean.FALSE);
            assertThat(TypeUtils.compare(highestNumber, lowestNumber, OQLLexerTokenTypes.TOK_LT))
                .isEqualTo(Boolean.FALSE);
            assertThat(TypeUtils.compare(highestNumber, lowestNumber, OQLLexerTokenTypes.TOK_LE))
                .isEqualTo(Boolean.FALSE);
            assertThat(TypeUtils.compare(highestNumber, lowestNumber, OQLLexerTokenTypes.TOK_NE))
                .isEqualTo(Boolean.TRUE);
            assertThat(TypeUtils.compare(highestNumber, lowestNumber, OQLLexerTokenTypes.TOK_GT))
                .isEqualTo(Boolean.TRUE);
            assertThat(TypeUtils.compare(highestNumber, lowestNumber, OQLLexerTokenTypes.TOK_GE))
                .isEqualTo(Boolean.TRUE);
          } catch (TypeMismatchException typeMismatchException) {
            throw new RuntimeException(typeMismatchException);
          }
        }));
  }

  @Test
  public void comparingNumericValuesShouldThrowExceptionWhenTheComparisonOperatorIsNotSupported() {
    OQLLexerTokenTypes tempInstance = new OQLLexerTokenTypes() {};
    Field[] fields = OQLLexerTokenTypes.class.getDeclaredFields();

    Arrays.stream(fields).forEach(field -> {
      try {
        int token = field.getInt(tempInstance);
        if (!comparisonOperators.contains(token)) {
          assertThatThrownBy(() -> TypeUtils.compare(new Integer("20"), new Double("20.12"), token))
              .isInstanceOf(IllegalArgumentException.class)
              .hasMessageMatching("^Unknown operator: (.*)$");
        }
      } catch (IllegalAccessException exception) {
        throw new RuntimeException(exception);
      }
    });
  }

  @Test
  public void comparingNumericValuesForWhichTheComparatorThrowsTypeMismatchExceptionShouldReturnBooleanWhenTheComparisonOperatorIsSupported()
      throws TypeMismatchException {
    assertThat(
        TypeUtils.compare(new Integer("20"), new BigDecimal("100"), OQLLexerTokenTypes.TOK_NE))
            .isEqualTo(Boolean.TRUE);
    assertThat(
        TypeUtils.compare(new Integer("20"), new BigDecimal("100"), OQLLexerTokenTypes.TOK_EQ))
            .isEqualTo(Boolean.FALSE);

    OQLLexerTokenTypes tempInstance = new OQLLexerTokenTypes() {};
    Field[] fields = OQLLexerTokenTypes.class.getDeclaredFields();

    Arrays.stream(fields).forEach(field -> {
      try {
        int token = field.getInt(tempInstance);
        if (!equalityOperators.contains(token)) {
          assertThatThrownBy(
              () -> TypeUtils.compare(new Integer("20"), "100", token))
                  .isInstanceOf(TypeMismatchException.class).hasMessageMatching(
                      "^Unable to compare object of type ' (.*) ' with object of type ' (.*) '$");
        }
      } catch (IllegalAccessException exception) {
        throw new RuntimeException(exception);
      }
    });
  }

  @Test
  public void comparingBooleanValuesShouldDelegateToBooleanCompareImplementation()
      throws TypeMismatchException {
    assertThat(TypeUtils.compare(true, Boolean.TRUE, OQLLexerTokenTypes.TOK_EQ))
        .isEqualTo(Boolean.TRUE);
    assertThat(TypeUtils.booleanCompare(true, Boolean.TRUE, OQLLexerTokenTypes.TOK_EQ)).isTrue();

    assertThat(TypeUtils.compare(Boolean.TRUE, true, OQLLexerTokenTypes.TOK_NE))
        .isEqualTo(Boolean.FALSE);
    assertThat(TypeUtils.booleanCompare(true, Boolean.TRUE, OQLLexerTokenTypes.TOK_NE)).isFalse();

    assertThat(TypeUtils.compare(true, Boolean.FALSE, OQLLexerTokenTypes.TOK_EQ))
        .isEqualTo(Boolean.FALSE);
    assertThat(TypeUtils.booleanCompare(true, Boolean.FALSE, OQLLexerTokenTypes.TOK_EQ)).isFalse();

    assertThat(TypeUtils.compare(Boolean.FALSE, true, OQLLexerTokenTypes.TOK_NE))
        .isEqualTo(Boolean.TRUE);
    assertThat(TypeUtils.booleanCompare(Boolean.FALSE, true, OQLLexerTokenTypes.TOK_NE)).isTrue();
  }

  @Test
  public void comparingComparableInstancesShouldDelegateToDefaultCompareToMethod()
      throws TypeMismatchException {
    Comparable startValue = spy(new ComparableObject(0));
    Comparable finishValue = spy(new ComparableObject(10));
    NumericComparator numericComparator = spy(NumericComparator.class);
    TemporalComparator temporalComparator = spy(TemporalComparator.class);

    assertThat(TypeUtils.compare(startValue, startValue, OQLLexerTokenTypes.TOK_EQ))
        .isEqualTo(Boolean.TRUE);
    assertThat(TypeUtils.compare(startValue, startValue, OQLLexerTokenTypes.TOK_LT))
        .isEqualTo(Boolean.FALSE);
    assertThat(TypeUtils.compare(startValue, startValue, OQLLexerTokenTypes.TOK_LE))
        .isEqualTo(Boolean.TRUE);
    assertThat(TypeUtils.compare(startValue, startValue, OQLLexerTokenTypes.TOK_GT))
        .isEqualTo(Boolean.FALSE);
    assertThat(TypeUtils.compare(startValue, startValue, OQLLexerTokenTypes.TOK_GE))
        .isEqualTo(Boolean.TRUE);
    assertThat(TypeUtils.compare(startValue, startValue, OQLLexerTokenTypes.TOK_NE))
        .isEqualTo(Boolean.FALSE);
    verify(startValue, times(6)).compareTo(startValue);
    reset(startValue);

    assertThat(TypeUtils.compare(finishValue, finishValue, OQLLexerTokenTypes.TOK_EQ))
        .isEqualTo(Boolean.TRUE);
    assertThat(TypeUtils.compare(finishValue, finishValue, OQLLexerTokenTypes.TOK_LT))
        .isEqualTo(Boolean.FALSE);
    assertThat(TypeUtils.compare(finishValue, finishValue, OQLLexerTokenTypes.TOK_LE))
        .isEqualTo(Boolean.TRUE);
    assertThat(TypeUtils.compare(finishValue, finishValue, OQLLexerTokenTypes.TOK_GT))
        .isEqualTo(Boolean.FALSE);
    assertThat(TypeUtils.compare(finishValue, finishValue, OQLLexerTokenTypes.TOK_GE))
        .isEqualTo(Boolean.TRUE);
    assertThat(TypeUtils.compare(finishValue, finishValue, OQLLexerTokenTypes.TOK_NE))
        .isEqualTo(Boolean.FALSE);
    verify(finishValue, times(6)).compareTo(finishValue);
    reset(finishValue);

    assertThat(TypeUtils.compare(startValue, finishValue, OQLLexerTokenTypes.TOK_EQ))
        .isEqualTo(Boolean.FALSE);
    assertThat(TypeUtils.compare(startValue, finishValue, OQLLexerTokenTypes.TOK_LT))
        .isEqualTo(Boolean.TRUE);
    assertThat(TypeUtils.compare(startValue, finishValue, OQLLexerTokenTypes.TOK_LE))
        .isEqualTo(Boolean.TRUE);
    assertThat(TypeUtils.compare(startValue, finishValue, OQLLexerTokenTypes.TOK_GT))
        .isEqualTo(Boolean.FALSE);
    assertThat(TypeUtils.compare(startValue, finishValue, OQLLexerTokenTypes.TOK_GE))
        .isEqualTo(Boolean.FALSE);
    assertThat(TypeUtils.compare(startValue, finishValue, OQLLexerTokenTypes.TOK_NE))
        .isEqualTo(Boolean.TRUE);
    verify(startValue, times(6)).compareTo(finishValue);
    reset(startValue);

    assertThat(TypeUtils.compare(finishValue, startValue, OQLLexerTokenTypes.TOK_EQ))
        .isEqualTo(Boolean.FALSE);
    assertThat(TypeUtils.compare(finishValue, startValue, OQLLexerTokenTypes.TOK_LT))
        .isEqualTo(Boolean.FALSE);
    assertThat(TypeUtils.compare(finishValue, startValue, OQLLexerTokenTypes.TOK_LE))
        .isEqualTo(Boolean.FALSE);
    assertThat(TypeUtils.compare(finishValue, startValue, OQLLexerTokenTypes.TOK_GT))
        .isEqualTo(Boolean.TRUE);
    assertThat(TypeUtils.compare(finishValue, startValue, OQLLexerTokenTypes.TOK_GE))
        .isEqualTo(Boolean.TRUE);
    assertThat(TypeUtils.compare(finishValue, startValue, OQLLexerTokenTypes.TOK_NE))
        .isEqualTo(Boolean.TRUE);
    verify(finishValue, times(6)).compareTo(startValue);
    reset(finishValue);

    // Extra check to verify that no other comparison methods were called.
    verify(numericComparator, times(0)).compare(any(), any());
    verify(temporalComparator, times(0)).compare(any(), any());
  }

  @Test
  public void comparingComparableValuesShouldThrowExceptionWhenTheComparisonOperatorIsNotSupported() {
    OQLLexerTokenTypes tempInstance = new OQLLexerTokenTypes() {};
    Field[] fields = OQLLexerTokenTypes.class.getDeclaredFields();

    Arrays.stream(fields).forEach(field -> {
      try {
        int token = field.getInt(tempInstance);
        if (!comparisonOperators.contains(token)) {
          assertThatThrownBy(
              () -> TypeUtils.compare(mock(Comparable.class), mock(Comparable.class), token))
                  .isInstanceOf(IllegalArgumentException.class)
                  .hasMessageMatching("^Unknown operator: (.*)$");
        }
      } catch (Exception exception) {
        throw new RuntimeException(exception);
      }
    });
  }

  @Test
  public void comparingComparableValuesForWhichTheCompareMethodThrowsClassCastExceptionShouldReturnBooleanWhenTheComparisonOperatorIsSupported()
      throws TypeMismatchException {
    ComparableObject comparableValue = mock(ComparableObject.class);
    when(comparableValue.compareTo(any())).thenThrow(new ClassCastException(""));

    assertThat(
        TypeUtils.compare(comparableValue, mock(Comparable.class), OQLLexerTokenTypes.TOK_NE))
            .isEqualTo(Boolean.TRUE);
    assertThat(
        TypeUtils.compare(comparableValue, mock(Comparable.class), OQLLexerTokenTypes.TOK_EQ))
            .isEqualTo(Boolean.FALSE);

    OQLLexerTokenTypes tempInstance = new OQLLexerTokenTypes() {};
    Field[] fields = OQLLexerTokenTypes.class.getDeclaredFields();

    Arrays.stream(fields).forEach(field -> {
      try {
        int token = field.getInt(tempInstance);
        if (!equalityOperators.contains(token)) {
          assertThatThrownBy(
              () -> TypeUtils.compare(comparableValue, mock(Comparable.class), token))
                  .isInstanceOf(TypeMismatchException.class).hasMessageMatching(
                      "^Unable to compare object of type ' (.*) ' with object of type ' (.*) '$");
        }
      } catch (Exception exception) {
        throw new RuntimeException(exception);
      }
    });
  }

  @Test
  public void comparingArbitraryObjectsShouldDelegateToDefaultEqualsMethod()
      throws TypeMismatchException {
    ArbitraryObject aValue = new ArbitraryObject("0");
    ArbitraryObject anotherValue = new ArbitraryObject("1");
    NumericComparator numericComparator = spy(NumericComparator.class);
    TemporalComparator temporalComparator = spy(TemporalComparator.class);

    assertThat(TypeUtils.compare(aValue, aValue, OQLLexerTokenTypes.TOK_EQ))
        .isEqualTo(Boolean.TRUE);
    assertThat(TypeUtils.compare(aValue, aValue, OQLLexerTokenTypes.TOK_NE))
        .isEqualTo(Boolean.FALSE);
    assertThat(aValue.getInvocationsAmount()).isEqualTo(2);
    aValue.resetInvocationsAmount();

    assertThat(TypeUtils.compare(aValue, anotherValue, OQLLexerTokenTypes.TOK_NE))
        .isEqualTo(Boolean.TRUE);
    assertThat(TypeUtils.compare(anotherValue, aValue, OQLLexerTokenTypes.TOK_NE))
        .isEqualTo(Boolean.TRUE);
    assertThat(TypeUtils.compare(aValue, anotherValue, OQLLexerTokenTypes.TOK_EQ))
        .isEqualTo(Boolean.FALSE);
    assertThat(TypeUtils.compare(anotherValue, aValue, OQLLexerTokenTypes.TOK_EQ))
        .isEqualTo(Boolean.FALSE);
    assertThat(aValue.getInvocationsAmount()).isEqualTo(2);
    assertThat(anotherValue.getInvocationsAmount()).isEqualTo(2);
    aValue.resetInvocationsAmount();
    anotherValue.resetInvocationsAmount();

    // Extra check to verify that no other comparison methods were called.
    verify(numericComparator, times(0)).compare(any(), any());
    verify(temporalComparator, times(0)).compare(any(), any());
  }

  @Test
  public void comparingArbitraryObjectsUsingAnUnsupportedComparisonOperatorShouldThrowException() {
    OQLLexerTokenTypes tempInstance = new OQLLexerTokenTypes() {};
    Field[] fields = OQLLexerTokenTypes.class.getDeclaredFields();

    Arrays.stream(fields).forEach(field -> {
      try {
        int token = field.getInt(tempInstance);
        if (!comparisonOperators.contains(token)) {
          assertThatThrownBy(() -> TypeUtils.compare(new ArbitraryObject("0"),
              new ArbitraryObject("0"),
              token)).isInstanceOf(TypeMismatchException.class).hasMessageMatching(
                  "^Unable to use a relational comparison operator to compare an instance of class ' (.*) ' with an instance of ' (.*) '$");
        }
      } catch (IllegalAccessException exception) {
        throw new RuntimeException(exception);
      }
    });
  }

  /**
   * Test class that implements the Comparable interface.
   */
  public class ComparableObject implements Comparable {
    final Integer id;

    ComparableObject(Integer id) {
      this.id = id;
    }

    @Override
    public int compareTo(Object o) {
      return id.compareTo(((ComparableObject) o).id);
    }
  }

  /**
   * Arbitrary Test class (equals method can not be mocked).
   */
  public class ArbitraryObject {
    final String id;
    final AtomicInteger equalsInvocations;

    ArbitraryObject(String id) {
      this.id = id;
      equalsInvocations = new AtomicInteger(0);
    }

    Integer getInvocationsAmount() {
      return equalsInvocations.get();
    }

    void resetInvocationsAmount() {
      equalsInvocations.set(0);
    }

    @Override
    public boolean equals(Object o) {
      equalsInvocations.incrementAndGet();

      if (this == o) {
        return true;
      }

      if (!(o instanceof ArbitraryObject)) {
        return false;
      }

      ArbitraryObject that = (ArbitraryObject) o;

      return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id);
    }
  }
}
