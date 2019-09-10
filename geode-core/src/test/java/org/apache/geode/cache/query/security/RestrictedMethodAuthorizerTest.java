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
package org.apache.geode.cache.query.security;


import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.query.internal.QRegion;
import org.apache.geode.cache.query.internal.index.DummyQRegion;
import org.apache.geode.internal.cache.EntrySnapshot;
import org.apache.geode.internal.cache.NonTXEntry;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category(SecurityTest.class)
public class RestrictedMethodAuthorizerTest {
  private RestrictedMethodAuthorizer methodInvocationAuthorizer =
      new RestrictedMethodAuthorizer(null);

  @Test
  public void getClassShouldNotBeAuthorized() throws Exception {
    Method method = Integer.class.getMethod("getClass");
    assertThat(methodInvocationAuthorizer.authorize(method, 0)).isFalse();
  }

  @Test
  public void toStringOnAnyObjectShouldBeAuthorized() throws Exception {
    Method stringMethod = Object.class.getMethod("toString");
    assertThat(methodInvocationAuthorizer.authorize(stringMethod, new Object())).isTrue();
  }

  @Test
  public void equalsOnAnyObjectShouldBeAuthorized() throws Exception {
    Method equalsMethod = Object.class.getMethod("equals", Object.class);
    assertThat(methodInvocationAuthorizer.authorize(equalsMethod, new Object())).isTrue();
  }

  @Test
  public void compareToOnAnyObjectShouldBeAuthorized() throws Exception {
    Method equalsMethod = Comparable.class.getMethod("compareTo", Object.class);
    assertThat(methodInvocationAuthorizer.authorize(equalsMethod, new Object())).isTrue();
  }

  @Test
  public void booleanValueOnBooleanShouldBeAuthorized() throws Exception {
    Method booleanValue = Boolean.class.getMethod("booleanValue");
    assertThat(methodInvocationAuthorizer.authorize(booleanValue, Boolean.TRUE)).isTrue();
  }

  @Test
  public void toCharAtOnStringObject() throws Exception {
    Method stringMethod = String.class.getMethod("charAt", int.class);
    assertThat(methodInvocationAuthorizer.isAcceptlisted(stringMethod)).isTrue();
  }

  @Test
  public void codePointAtStringObject() throws Exception {
    Method stringMethod = String.class.getMethod("codePointAt", int.class);
    assertThat(methodInvocationAuthorizer.isAcceptlisted(stringMethod)).isTrue();
  }

  @Test
  public void codePointBeforeStringObject() throws Exception {
    Method stringMethod = String.class.getMethod("codePointBefore", int.class);
    assertThat(methodInvocationAuthorizer.isAcceptlisted(stringMethod)).isTrue();
  }

  @Test
  public void codePointCountStringObject() throws Exception {
    Method stringMethod = String.class.getMethod("codePointCount", int.class, int.class);
    assertThat(methodInvocationAuthorizer.isAcceptlisted(stringMethod)).isTrue();
  }

  @Test
  public void compareToStringObject() throws Exception {
    Method stringMethod = String.class.getMethod("compareTo", String.class);
    assertThat(methodInvocationAuthorizer.isAcceptlisted(stringMethod)).isTrue();
  }

  @Test
  public void compareToIgnoreCaseStringObject() throws Exception {
    Method stringMethod = String.class.getMethod("compareToIgnoreCase", String.class);
    assertThat(methodInvocationAuthorizer.isAcceptlisted(stringMethod)).isTrue();
  }

  @Test
  public void concatStringObject() throws Exception {
    Method stringMethod = String.class.getMethod("compareTo", String.class);
    assertThat(methodInvocationAuthorizer.isAcceptlisted(stringMethod)).isTrue();
  }

  @Test
  public void containsStringObject() throws Exception {
    Method stringMethod = String.class.getMethod("contains", CharSequence.class);
    assertThat(methodInvocationAuthorizer.isAcceptlisted(stringMethod)).isTrue();
  }

  @Test
  public void contentEqualsStringObject() throws Exception {
    Method stringMethod = String.class.getMethod("contentEquals", CharSequence.class);
    assertThat(methodInvocationAuthorizer.isAcceptlisted(stringMethod)).isTrue();
  }

  @Test
  public void contentEqualsWithStringBufferStringObject() throws Exception {
    Method stringMethod = String.class.getMethod("contentEquals", StringBuffer.class);
    assertThat(methodInvocationAuthorizer.isAcceptlisted(stringMethod)).isTrue();
  }

  @Test
  public void endsWithOnStringObject() throws Exception {
    Method stringMethod = String.class.getMethod("endsWith", String.class);
    assertThat(methodInvocationAuthorizer.isAcceptlisted(stringMethod)).isTrue();
  }

  @Test
  public void equalsIgnoreCase() throws Exception {
    Method stringMethod = String.class.getMethod("equalsIgnoreCase", String.class);
    assertThat(methodInvocationAuthorizer.isAcceptlisted(stringMethod)).isTrue();
  }

  @Test
  public void getBytesOnString() throws Exception {
    Method stringMethod = String.class.getMethod("getBytes");
    assertThat(methodInvocationAuthorizer.isAcceptlisted(stringMethod)).isTrue();
  }

  @Test
  public void getBytesWithCharsetOnString() throws Exception {
    Method stringMethod = String.class.getMethod("getBytes", Charset.class);
    assertThat(methodInvocationAuthorizer.isAcceptlisted(stringMethod)).isTrue();
  }

  @Test
  public void hashCodeOnStringObject() throws Exception {
    Method stringMethod = String.class.getMethod("hashCode");
    assertThat(methodInvocationAuthorizer.isAcceptlisted(stringMethod)).isTrue();
  }

  @Test
  public void indexOfOnStringObject() throws Exception {
    Method stringMethod = String.class.getMethod("indexOf", int.class);
    assertThat(methodInvocationAuthorizer.isAcceptlisted(stringMethod)).isTrue();
  }

  @Test
  public void indexOfWithStringOnStringObject() throws Exception {
    Method stringMethod = String.class.getMethod("indexOf", String.class);
    assertThat(methodInvocationAuthorizer.isAcceptlisted(stringMethod)).isTrue();
  }

  @Test
  public void indexOfWithStringAndIntOnStringObject() throws Exception {
    Method stringMethod = String.class.getMethod("indexOf", String.class, int.class);
    assertThat(methodInvocationAuthorizer.isAcceptlisted(stringMethod)).isTrue();
  }

  @Test
  public void internOnStringObject() throws Exception {
    Method stringMethod = String.class.getMethod("intern");
    assertThat(methodInvocationAuthorizer.isAcceptlisted(stringMethod)).isTrue();
  }

  @Test
  public void isEmpty() throws Exception {
    Method stringMethod = String.class.getMethod("isEmpty");
    assertThat(methodInvocationAuthorizer.isAcceptlisted(stringMethod)).isTrue();
  }

  @Test
  public void lastIndexOfWithIntOnString() throws Exception {
    Method stringMethod = String.class.getMethod("lastIndexOf", int.class);
    assertThat(methodInvocationAuthorizer.isAcceptlisted(stringMethod)).isTrue();
  }

  @Test
  public void lastIndexOfWithIntAndFronIndexOnString() throws Exception {
    Method stringMethod = String.class.getMethod("lastIndexOf", int.class, int.class);
    assertThat(methodInvocationAuthorizer.isAcceptlisted(stringMethod)).isTrue();
  }

  @Test
  public void lastIndexOfWithStringOnString() throws Exception {
    Method stringMethod = String.class.getMethod("lastIndexOf", String.class);
    assertThat(methodInvocationAuthorizer.isAcceptlisted(stringMethod)).isTrue();
  }

  @Test
  public void lastIndexOfWithStringAndFromIndexOnString() throws Exception {
    Method stringMethod = String.class.getMethod("lastIndexOf", String.class, int.class);
    assertThat(methodInvocationAuthorizer.isAcceptlisted(stringMethod)).isTrue();
  }

  @Test
  public void lengthOnString() throws Exception {
    Method stringMethod = String.class.getMethod("length");
    assertThat(methodInvocationAuthorizer.isAcceptlisted(stringMethod)).isTrue();
  }

  @Test
  public void matchesOnString() throws Exception {
    Method stringMethod = String.class.getMethod("matches", String.class);
    assertThat(methodInvocationAuthorizer.isAcceptlisted(stringMethod)).isTrue();
  }

  @Test
  public void offsetByCodePointsOnString() throws Exception {
    Method stringMethod = String.class.getMethod("offsetByCodePoints", int.class, int.class);
    assertThat(methodInvocationAuthorizer.isAcceptlisted(stringMethod)).isTrue();
  }


  @Test
  public void regionMatchesWith5ParamsOnString() throws Exception {
    Method stringMethod = String.class.getMethod("regionMatches", boolean.class, int.class,
        String.class, int.class, int.class);
    assertThat(methodInvocationAuthorizer.isAcceptlisted(stringMethod)).isTrue();
  }

  @Test
  public void regionMatchesWith4ParamsOnString() throws Exception {
    Method stringMethod =
        String.class.getMethod("regionMatches", int.class, String.class, int.class, int.class);
    assertThat(methodInvocationAuthorizer.isAcceptlisted(stringMethod)).isTrue();
  }

  @Test
  public void replaceOnString() throws Exception {
    Method stringMethod = String.class.getMethod("replace", char.class, char.class);
    assertThat(methodInvocationAuthorizer.isAcceptlisted(stringMethod)).isTrue();
  }

  @Test
  public void replaceWithCharSequenceOnString() throws Exception {
    Method stringMethod = String.class.getMethod("replace", CharSequence.class, CharSequence.class);
    assertThat(methodInvocationAuthorizer.isAcceptlisted(stringMethod)).isTrue();
  }

  @Test
  public void replaceAllOnString() throws Exception {
    Method stringMethod = String.class.getMethod("replaceAll", String.class, String.class);
    assertThat(methodInvocationAuthorizer.isAcceptlisted(stringMethod)).isTrue();
  }

  @Test
  public void replaceFirstOnString() throws Exception {
    Method stringMethod = String.class.getMethod("replaceFirst", String.class, String.class);
    assertThat(methodInvocationAuthorizer.isAcceptlisted(stringMethod)).isTrue();
  }

  @Test
  public void splitOnString() throws Exception {
    Method stringMethod = String.class.getMethod("split", String.class);
    assertThat(methodInvocationAuthorizer.isAcceptlisted(stringMethod)).isTrue();
  }

  @Test
  public void splitWithLimitOnString() throws Exception {
    Method stringMethod = String.class.getMethod("split", String.class, int.class);
    assertThat(methodInvocationAuthorizer.isAcceptlisted(stringMethod)).isTrue();
  }

  @Test
  public void startsOnString() throws Exception {
    Method stringMethod = String.class.getMethod("startsWith", String.class);
    assertThat(methodInvocationAuthorizer.isAcceptlisted(stringMethod)).isTrue();
  }

  @Test
  public void startsWithOffsetOnString() throws Exception {
    Method stringMethod = String.class.getMethod("startsWith", String.class, int.class);
    assertThat(methodInvocationAuthorizer.isAcceptlisted(stringMethod)).isTrue();
  }

  @Test
  public void substringOnString() throws Exception {
    Method stringMethod = String.class.getMethod("substring", int.class);
    assertThat(methodInvocationAuthorizer.isAcceptlisted(stringMethod)).isTrue();
  }

  @Test
  public void substringWithEndIndexOnString() throws Exception {
    Method stringMethod = String.class.getMethod("substring", int.class, int.class);
    assertThat(methodInvocationAuthorizer.isAcceptlisted(stringMethod)).isTrue();
  }

  @Test
  public void toCharArrayOnString() throws Exception {
    Method stringMethod = String.class.getMethod("toCharArray");
    assertThat(methodInvocationAuthorizer.isAcceptlisted(stringMethod)).isTrue();
  }

  @Test
  public void toLowerCaseOnStringObject() throws Exception {
    Method stringMethod = String.class.getMethod("toLowerCase");
    assertThat(methodInvocationAuthorizer.isAcceptlisted(stringMethod)).isTrue();
  }

  @Test
  public void toUpperCaseOnStringObject() throws Exception {
    Method stringMethod = String.class.getMethod("toUpperCase");
    assertThat(methodInvocationAuthorizer.isAcceptlisted(stringMethod)).isTrue();
  }

  @Test
  public void trimOnString() throws Exception {
    Method stringMethod = String.class.getMethod("trim");
    assertThat(methodInvocationAuthorizer.isAcceptlisted(stringMethod)).isTrue();
  }

  @Test
  public void utilDateAfterMethodIsAcceptListed() throws Exception {
    Method method = Date.class.getMethod("after", Date.class);
    assertThat(methodInvocationAuthorizer.isAcceptlisted(method)).isTrue();
  }

  @Test
  public void sqlDateAfterMethodIsAcceptListed() throws Exception {
    Method method = java.sql.Date.class.getMethod("after", Date.class);
    assertThat(methodInvocationAuthorizer.isAcceptlisted(method)).isTrue();
  }

  @Test
  public void utilDateBeforeMethodIsAcceptListed() throws Exception {
    Method method = Date.class.getMethod("before", Date.class);
    assertThat(methodInvocationAuthorizer.isAcceptlisted(method)).isTrue();
  }

  @Test
  public void sqlDateBeforeMethodIsAcceptListed() throws Exception {
    Method method = java.sql.Date.class.getMethod("before", Date.class);
    assertThat(methodInvocationAuthorizer.isAcceptlisted(method)).isTrue();
  }

  @Test
  public void timestampAfterMethodIsAcceptListed() throws Exception {
    Method method = Timestamp.class.getMethod("after", Timestamp.class);
    assertThat(methodInvocationAuthorizer.isAcceptlisted(method)).isTrue();
  }

  @Test
  public void sqlTimestampBeforeMethodIsAcceptListed() throws Exception {
    Method method = Timestamp.class.getMethod("before", Timestamp.class);
    assertThat(methodInvocationAuthorizer.isAcceptlisted(method)).isTrue();
  }

  @Test
  public void sqlTimestampGetNanosIsAcceptListed() throws Exception {
    Method method = Timestamp.class.getMethod("getNanos");
    assertThat(methodInvocationAuthorizer.isAcceptlisted(method)).isTrue();
  }

  @Test
  public void sqlTimestampGetTimeIsAcceptListed() throws Exception {
    Method method = Timestamp.class.getMethod("getTime");
    assertThat(methodInvocationAuthorizer.isAcceptlisted(method)).isTrue();
  }


  @Test
  public void getKeyForMapEntryIsAcceptListed() throws Exception {
    Method getKeyMethod = Map.Entry.class.getMethod("getKey");
    assertThat(methodInvocationAuthorizer.isAcceptlisted(getKeyMethod)).isTrue();
  }

  @Test
  public void getValueForMapEntryIsAcceptListed() throws Exception {
    Method getValueMethod = Map.Entry.class.getMethod("getValue");
    assertThat(methodInvocationAuthorizer.isAcceptlisted(getValueMethod)).isTrue();
  }

  @Test
  public void getKeyForMapEntrySnapShotIsAcceptListed() throws Exception {
    Method getKeyMethod = EntrySnapshot.class.getMethod("getKey");
    assertThat(methodInvocationAuthorizer.isAcceptlisted(getKeyMethod)).isTrue();
  }

  @Test
  public void getValueForMapEntrySnapShotIsAcceptListed() throws Exception {
    Method getValueMethod = EntrySnapshot.class.getMethod("getValue");
    assertThat(methodInvocationAuthorizer.isAcceptlisted(getValueMethod)).isTrue();
  }

  @Test
  public void getKeyForNonTXEntryIsAcceptListed() throws Exception {
    Method getKeyMethod = NonTXEntry.class.getMethod("getKey");
    assertThat(methodInvocationAuthorizer.isAcceptlisted(getKeyMethod)).isTrue();
  }

  @Test
  public void getValueForNonTXEntryIsAcceptListed() throws Exception {
    Method getValueMethod = NonTXEntry.class.getMethod("getValue");
    assertThat(methodInvocationAuthorizer.isAcceptlisted(getValueMethod)).isTrue();
  }

  @Test
  public void mapMethodsForQRegionAreAcceptListed() throws Exception {
    testMapMethods(QRegion.class);
  }

  @Test
  public void mapMethodsForDummyQRegionAreAcceptListed() throws Exception {
    testMapMethods(DummyQRegion.class);
  }

  @Test
  public void mapMethodsForPartitionedRegionAreAcceptListed() throws Exception {
    Class<PartitionedRegion> clazz = PartitionedRegion.class;
    Method get = clazz.getMethod("get", Object.class);
    Method entrySet = clazz.getMethod("entrySet");
    Method keySet = clazz.getMethod("keySet");
    Method values = clazz.getMethod("values");
    Method containsKey = clazz.getMethod("containsKey", Object.class);
    assertThat(methodInvocationAuthorizer.isAcceptlisted(get)).isTrue();
    assertThat(methodInvocationAuthorizer.isAcceptlisted(entrySet)).isTrue();
    assertThat(methodInvocationAuthorizer.isAcceptlisted(keySet)).isTrue();
    assertThat(methodInvocationAuthorizer.isAcceptlisted(values)).isTrue();
    assertThat(methodInvocationAuthorizer.isAcceptlisted(containsKey)).isTrue();
  }

  @Test
  public void numberMethodsForByteAreAcceptListed() throws Exception {
    testNumberMethods(Byte.class);
  }

  @Test
  public void numberMethodsForDoubleAreAcceptListed() throws Exception {
    testNumberMethods(Double.class);
  }

  @Test
  public void numberMethodsForFloatAreAcceptListed() throws Exception {
    testNumberMethods(Float.class);
  }

  @Test
  public void numberMethodsForIntegerAreAcceptListed() throws Exception {
    testNumberMethods(Integer.class);
  }

  @Test
  public void numberMethodsForShortAreAcceptListed() throws Exception {
    testNumberMethods(Short.class);
  }

  @Test
  public void numberMethodsForBigDecimalAreAcceptListed() throws Exception {
    testNumberMethods(BigDecimal.class);
  }

  @Test
  public void numberMethodsForNumberAreAcceptListed() throws Exception {
    testNumberMethods(BigInteger.class);
  }

  @Test
  public void numberMethodsForAtomicIntegerAreAcceptListed() throws Exception {
    testNumberMethods(AtomicInteger.class);
  }

  @Test
  public void numberMethodsForAtomicLongAreAcceptListed() throws Exception {
    testNumberMethods(AtomicLong.class);
  }

  @Test
  public void verifyAuthorizersUseDefaultAcceptList() {
    RestrictedMethodAuthorizer authorizer1 =
        new RestrictedMethodAuthorizer(null);
    RestrictedMethodAuthorizer authorizer2 =
        new RestrictedMethodAuthorizer(null);
    assertThat(authorizer1.getAcceptList()).isSameAs(authorizer2.getAcceptList());
    assertThat(authorizer1.getAcceptList())
        .isSameAs(RestrictedMethodAuthorizer.DEFAULT_ACCEPTLIST);
    assertThat(authorizer2.getAcceptList())
        .isSameAs(RestrictedMethodAuthorizer.DEFAULT_ACCEPTLIST);
  }

  private void testNumberMethods(Class<?> clazz) throws NoSuchMethodException {
    Method byteValue = clazz.getMethod("byteValue");
    Method doubleValue = clazz.getMethod("doubleValue");
    Method intValue = clazz.getMethod("intValue");
    Method floatValue = clazz.getMethod("longValue");
    Method longValue = clazz.getMethod("floatValue");
    Method shortValue = clazz.getMethod("shortValue");
    assertThat(methodInvocationAuthorizer.isAcceptlisted(byteValue)).isTrue();
    assertThat(methodInvocationAuthorizer.isAcceptlisted(doubleValue)).isTrue();
    assertThat(methodInvocationAuthorizer.isAcceptlisted(intValue)).isTrue();
    assertThat(methodInvocationAuthorizer.isAcceptlisted(floatValue)).isTrue();
    assertThat(methodInvocationAuthorizer.isAcceptlisted(longValue)).isTrue();
    assertThat(methodInvocationAuthorizer.isAcceptlisted(shortValue)).isTrue();
  }

  private void testMapMethods(Class<?> clazz) throws NoSuchMethodException {
    Method get = clazz.getMethod("get", Object.class);
    Method entrySet = clazz.getMethod("entrySet");
    Method keySet = clazz.getMethod("keySet");
    Method values = clazz.getMethod("values");
    Method getEntries = clazz.getMethod("getEntries");
    Method getValues = clazz.getMethod("getValues");
    Method containsKey = clazz.getMethod("containsKey", Object.class);
    assertThat(methodInvocationAuthorizer.isAcceptlisted(get)).isTrue();
    assertThat(methodInvocationAuthorizer.isAcceptlisted(entrySet)).isTrue();
    assertThat(methodInvocationAuthorizer.isAcceptlisted(keySet)).isTrue();
    assertThat(methodInvocationAuthorizer.isAcceptlisted(values)).isTrue();
    assertThat(methodInvocationAuthorizer.isAcceptlisted(getEntries)).isTrue();
    assertThat(methodInvocationAuthorizer.isAcceptlisted(getValues)).isTrue();
    assertThat(methodInvocationAuthorizer.isAcceptlisted(containsKey)).isTrue();
  }

}
