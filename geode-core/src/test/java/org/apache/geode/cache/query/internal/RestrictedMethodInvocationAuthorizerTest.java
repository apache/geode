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
package org.apache.geode.cache.query.internal;


import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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

import org.apache.geode.cache.query.internal.index.DummyQRegion;
import org.apache.geode.internal.cache.EntrySnapshot;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.categories.UnitTest;

@Category({UnitTest.class, SecurityTest.class})
public class RestrictedMethodInvocationAuthorizerTest {
  RestrictedMethodInvocationAuthorizer methodInvocationAuthorizer =
      new RestrictedMethodInvocationAuthorizer(null);

  @Test
  public void getClassShouldFail() throws Exception {
    Method method = Integer.class.getMethod("getClass");
    assertFalse(methodInvocationAuthorizer.isWhitelisted(method));
  }

  @Test
  public void toStringOnAnyObject() throws Exception {
    Method stringMethod = Integer.class.getMethod("toString");
    assertTrue(methodInvocationAuthorizer.isWhitelisted(stringMethod));
  }

  @Test
  public void equalsOnAnyObject() throws Exception {
    Method equalsMethod = Integer.class.getMethod("equals", Object.class);
    assertTrue(methodInvocationAuthorizer.isWhitelisted(equalsMethod));
  }

  @Test
  public void booleanMethodsAreWhiteListed() throws Exception {
    Method booleanValue = Boolean.class.getMethod("booleanValue");
    assertTrue(methodInvocationAuthorizer.isWhitelisted(booleanValue));
  }

  @Test
  public void toCharAtOnStringObject() throws Exception {
    Method stringMethod = String.class.getMethod("charAt", int.class);
    assertTrue(methodInvocationAuthorizer.isWhitelisted(stringMethod));
  }

  @Test
  public void codePointAtStringObject() throws Exception {
    Method stringMethod = String.class.getMethod("codePointAt", int.class);
    assertTrue(methodInvocationAuthorizer.isWhitelisted(stringMethod));
  }

  @Test
  public void codePointBeforeStringObject() throws Exception {
    Method stringMethod = String.class.getMethod("codePointBefore", int.class);
    assertTrue(methodInvocationAuthorizer.isWhitelisted(stringMethod));
  }

  @Test
  public void codePointCountStringObject() throws Exception {
    Method stringMethod = String.class.getMethod("codePointCount", int.class, int.class);
    assertTrue(methodInvocationAuthorizer.isWhitelisted(stringMethod));
  }

  @Test
  public void compareToStringObject() throws Exception {
    Method stringMethod = String.class.getMethod("compareTo", String.class);
    assertTrue(methodInvocationAuthorizer.isWhitelisted(stringMethod));
  }

  @Test
  public void compareToIgnoreCaseStringObject() throws Exception {
    Method stringMethod = String.class.getMethod("compareToIgnoreCase", String.class);
    assertTrue(methodInvocationAuthorizer.isWhitelisted(stringMethod));
  }

  @Test
  public void concatStringObject() throws Exception {
    Method stringMethod = String.class.getMethod("compareTo", String.class);
    assertTrue(methodInvocationAuthorizer.isWhitelisted(stringMethod));
  }

  @Test
  public void containsStringObject() throws Exception {
    Method stringMethod = String.class.getMethod("contains", CharSequence.class);
    assertTrue(methodInvocationAuthorizer.isWhitelisted(stringMethod));
  }

  @Test
  public void contentEqualsStringObject() throws Exception {
    Method stringMethod = String.class.getMethod("contentEquals", CharSequence.class);
    assertTrue(methodInvocationAuthorizer.isWhitelisted(stringMethod));
  }

  @Test
  public void contentEqualsWithStringBufferStringObject() throws Exception {
    Method stringMethod = String.class.getMethod("contentEquals", StringBuffer.class);
    assertTrue(methodInvocationAuthorizer.isWhitelisted(stringMethod));
  }

  @Test
  public void endsWithOnStringObject() throws Exception {
    Method stringMethod = String.class.getMethod("endsWith", String.class);
    assertTrue(methodInvocationAuthorizer.isWhitelisted(stringMethod));
  }

  @Test
  public void equalsIgnoreCase() throws Exception {
    Method stringMethod = String.class.getMethod("equalsIgnoreCase", String.class);
    assertTrue(methodInvocationAuthorizer.isWhitelisted(stringMethod));
  }

  @Test
  public void getBytesOnString() throws Exception {
    Method stringMethod = String.class.getMethod("getBytes");
    assertTrue(methodInvocationAuthorizer.isWhitelisted(stringMethod));
  }

  @Test
  public void getBytesWithCharsetOnString() throws Exception {
    Method stringMethod = String.class.getMethod("getBytes", Charset.class);
    assertTrue(methodInvocationAuthorizer.isWhitelisted(stringMethod));
  }

  @Test
  public void hashCodeOnStringObject() throws Exception {
    Method stringMethod = String.class.getMethod("hashCode");
    assertTrue(methodInvocationAuthorizer.isWhitelisted(stringMethod));
  }

  @Test
  public void indexOfOnStringObject() throws Exception {
    Method stringMethod = String.class.getMethod("indexOf", int.class);
    assertTrue(methodInvocationAuthorizer.isWhitelisted(stringMethod));
  }

  @Test
  public void indexOfWithStringOnStringObject() throws Exception {
    Method stringMethod = String.class.getMethod("indexOf", String.class);
    assertTrue(methodInvocationAuthorizer.isWhitelisted(stringMethod));
  }

  @Test
  public void indexOfWithStringAndIntOnStringObject() throws Exception {
    Method stringMethod = String.class.getMethod("indexOf", String.class, int.class);
    assertTrue(methodInvocationAuthorizer.isWhitelisted(stringMethod));
  }

  @Test
  public void internOnStringObject() throws Exception {
    Method stringMethod = String.class.getMethod("intern");
    assertTrue(methodInvocationAuthorizer.isWhitelisted(stringMethod));
  }

  @Test
  public void isEmpty() throws Exception {
    Method stringMethod = String.class.getMethod("isEmpty");
    assertTrue(methodInvocationAuthorizer.isWhitelisted(stringMethod));
  }

  @Test
  public void lastIndexOfWithIntOnString() throws Exception {
    Method stringMethod = String.class.getMethod("lastIndexOf", int.class);
    assertTrue(methodInvocationAuthorizer.isWhitelisted(stringMethod));
  }

  @Test
  public void lastIndexOfWithIntAndFronIndexOnString() throws Exception {
    Method stringMethod = String.class.getMethod("lastIndexOf", int.class, int.class);
    assertTrue(methodInvocationAuthorizer.isWhitelisted(stringMethod));
  }

  @Test
  public void lastIndexOfWithStringOnString() throws Exception {
    Method stringMethod = String.class.getMethod("lastIndexOf", String.class);
    assertTrue(methodInvocationAuthorizer.isWhitelisted(stringMethod));
  }

  @Test
  public void lastIndexOfWithStringAndFromIndexOnString() throws Exception {
    Method stringMethod = String.class.getMethod("lastIndexOf", String.class, int.class);
    assertTrue(methodInvocationAuthorizer.isWhitelisted(stringMethod));
  }

  @Test
  public void lengthOnString() throws Exception {
    Method stringMethod = String.class.getMethod("length");
    assertTrue(methodInvocationAuthorizer.isWhitelisted(stringMethod));
  }

  @Test
  public void matchesOnString() throws Exception {
    Method stringMethod = String.class.getMethod("matches", String.class);
    assertTrue(methodInvocationAuthorizer.isWhitelisted(stringMethod));
  }

  @Test
  public void offsetByCodePointsOnString() throws Exception {
    Method stringMethod = String.class.getMethod("offsetByCodePoints", int.class, int.class);
    assertTrue(methodInvocationAuthorizer.isWhitelisted(stringMethod));
  }


  @Test
  public void regionMatchesWith5ParamsOnString() throws Exception {
    Method stringMethod = String.class.getMethod("regionMatches", boolean.class, int.class,
        String.class, int.class, int.class);
    assertTrue(methodInvocationAuthorizer.isWhitelisted(stringMethod));
  }

  @Test
  public void regionMatchesWith4ParamsOnString() throws Exception {
    Method stringMethod =
        String.class.getMethod("regionMatches", int.class, String.class, int.class, int.class);
    assertTrue(methodInvocationAuthorizer.isWhitelisted(stringMethod));
  }

  @Test
  public void replaceOnString() throws Exception {
    Method stringMethod = String.class.getMethod("replace", char.class, char.class);
    assertTrue(methodInvocationAuthorizer.isWhitelisted(stringMethod));
  }

  @Test
  public void replaceWithCharSequenceOnString() throws Exception {
    Method stringMethod = String.class.getMethod("replace", CharSequence.class, CharSequence.class);
    assertTrue(methodInvocationAuthorizer.isWhitelisted(stringMethod));
  }

  @Test
  public void replaceAllOnString() throws Exception {
    Method stringMethod = String.class.getMethod("replaceAll", String.class, String.class);
    assertTrue(methodInvocationAuthorizer.isWhitelisted(stringMethod));
  }

  @Test
  public void replaceFirstOnString() throws Exception {
    Method stringMethod = String.class.getMethod("replaceFirst", String.class, String.class);
    assertTrue(methodInvocationAuthorizer.isWhitelisted(stringMethod));
  }

  @Test
  public void splitOnString() throws Exception {
    Method stringMethod = String.class.getMethod("split", String.class);
    assertTrue(methodInvocationAuthorizer.isWhitelisted(stringMethod));
  }

  @Test
  public void splitWithLimitOnString() throws Exception {
    Method stringMethod = String.class.getMethod("split", String.class, int.class);
    assertTrue(methodInvocationAuthorizer.isWhitelisted(stringMethod));
  }

  @Test
  public void startsOnString() throws Exception {
    Method stringMethod = String.class.getMethod("startsWith", String.class);
    assertTrue(methodInvocationAuthorizer.isWhitelisted(stringMethod));
  }

  @Test
  public void startsWithOffsetOnString() throws Exception {
    Method stringMethod = String.class.getMethod("startsWith", String.class, int.class);
    assertTrue(methodInvocationAuthorizer.isWhitelisted(stringMethod));
  }

  @Test
  public void substringOnString() throws Exception {
    Method stringMethod = String.class.getMethod("substring", int.class);
    assertTrue(methodInvocationAuthorizer.isWhitelisted(stringMethod));
  }

  @Test
  public void substringWithEndIndexOnString() throws Exception {
    Method stringMethod = String.class.getMethod("substring", int.class, int.class);
    assertTrue(methodInvocationAuthorizer.isWhitelisted(stringMethod));
  }

  @Test
  public void toCharArrayOnString() throws Exception {
    Method stringMethod = String.class.getMethod("toCharArray");
    assertTrue(methodInvocationAuthorizer.isWhitelisted(stringMethod));
  }

  @Test
  public void toLowerCaseOnStringObject() throws Exception {
    Method stringMethod = String.class.getMethod("toLowerCase");
    assertTrue(methodInvocationAuthorizer.isWhitelisted(stringMethod));
  }

  @Test
  public void toUpperCaseOnStringObject() throws Exception {
    Method stringMethod = String.class.getMethod("toUpperCase");
    assertTrue(methodInvocationAuthorizer.isWhitelisted(stringMethod));
  }

  @Test
  public void trimOnString() throws Exception {
    Method stringMethod = String.class.getMethod("trim");
    assertTrue(methodInvocationAuthorizer.isWhitelisted(stringMethod));
  }

  @Test
  public void utilDateAfterMethodIsWhiteListed() throws Exception {
    Method method = Date.class.getMethod("after", Date.class);
    assertTrue(methodInvocationAuthorizer.isWhitelisted(method));
  }

  @Test
  public void sqlDateAfterMethodIsWhiteListed() throws Exception {
    Method method = java.sql.Date.class.getMethod("after", Date.class);
    assertTrue(methodInvocationAuthorizer.isWhitelisted(method));
  }

  @Test
  public void utilDateBeforeMethodIsWhiteListed() throws Exception {
    Method method = Date.class.getMethod("before", Date.class);
    assertTrue(methodInvocationAuthorizer.isWhitelisted(method));
  }

  @Test
  public void sqlDateBeforeMethodIsWhiteListed() throws Exception {
    Method method = java.sql.Date.class.getMethod("before", Date.class);
    assertTrue(methodInvocationAuthorizer.isWhitelisted(method));
  }

  @Test
  public void timestampAfterMethodIsWhiteListed() throws Exception {
    Method method = Timestamp.class.getMethod("after", Timestamp.class);
    assertTrue(methodInvocationAuthorizer.isWhitelisted(method));
  }

  @Test
  public void sqlTimestampBeforeMethodIsWhiteListed() throws Exception {
    Method method = Timestamp.class.getMethod("before", Timestamp.class);
    assertTrue(methodInvocationAuthorizer.isWhitelisted(method));
  }

  @Test
  public void sqlTimestampGetNanosIsWhiteListed() throws Exception {
    Method method = Timestamp.class.getMethod("getNanos");
    assertTrue(methodInvocationAuthorizer.isWhitelisted(method));
  }

  @Test
  public void sqlTimestampGetTimeIsWhiteListed() throws Exception {
    Method method = Timestamp.class.getMethod("getTime");
    assertTrue(methodInvocationAuthorizer.isWhitelisted(method));
  }


  @Test
  public void getKeyForMapEntryIsWhiteListed() throws Exception {
    Method getKeyMethod = Map.Entry.class.getMethod("getKey");
    assertTrue(methodInvocationAuthorizer.isWhitelisted(getKeyMethod));
  }

  @Test
  public void getValueForMapEntryIsWhiteListed() throws Exception {
    Method getValueMethod = Map.Entry.class.getMethod("getValue");
    assertTrue(methodInvocationAuthorizer.isWhitelisted(getValueMethod));
  }

  @Test
  public void getKeyForMapEntrySnapShotIsWhiteListed() throws Exception {
    Method getKeyMethod = EntrySnapshot.class.getMethod("getKey");
    assertTrue(methodInvocationAuthorizer.isWhitelisted(getKeyMethod));
  }

  @Test
  public void getValueForMapEntrySnapShotIsWhiteListed() throws Exception {
    Method getValueMethod = EntrySnapshot.class.getMethod("getValue");
    assertTrue(methodInvocationAuthorizer.isWhitelisted(getValueMethod));
  }

  @Test
  public void getKeyForNonTXEntryIsWhiteListed() throws Exception {
    Method getKeyMethod = LocalRegion.NonTXEntry.class.getMethod("getKey");
    assertTrue(methodInvocationAuthorizer.isWhitelisted(getKeyMethod));
  }

  @Test
  public void getValueForNonTXEntryIsWhiteListed() throws Exception {
    Method getValueMethod = LocalRegion.NonTXEntry.class.getMethod("getValue");
    assertTrue(methodInvocationAuthorizer.isWhitelisted(getValueMethod));
  }

  @Test
  public void mapMethodsForQRegionAreWhiteListed() throws Exception {
    testMapMethods(QRegion.class);
  }

  @Test
  public void mapMethodsForDummyQRegionAreWhiteListed() throws Exception {
    testMapMethods(DummyQRegion.class);
  }

  @Test
  public void mapMethodsForPartitionedRegionAreWhiteListed() throws Exception {
    Class<PartitionedRegion> clazz = PartitionedRegion.class;
    Method get = clazz.getMethod("get", Object.class);
    Method entrySet = clazz.getMethod("entrySet");
    Method keySet = clazz.getMethod("keySet");
    Method values = clazz.getMethod("values");
    Method containsKey = clazz.getMethod("containsKey", Object.class);
    assertTrue(methodInvocationAuthorizer.isWhitelisted(get));
    assertTrue(methodInvocationAuthorizer.isWhitelisted(entrySet));
    assertTrue(methodInvocationAuthorizer.isWhitelisted(keySet));
    assertTrue(methodInvocationAuthorizer.isWhitelisted(values));
    assertTrue(methodInvocationAuthorizer.isWhitelisted(containsKey));
  }

  @Test
  public void numberMethodsForByteAreWhiteListed() throws Exception {
    testNumberMethods(Byte.class);
  }

  @Test
  public void numberMethodsForDoubleAreWhiteListed() throws Exception {
    testNumberMethods(Double.class);
  }

  @Test
  public void numberMethodsForFloatAreWhiteListed() throws Exception {
    testNumberMethods(Float.class);
  }

  @Test
  public void numberMethodsForIntegerAreWhiteListed() throws Exception {
    testNumberMethods(Integer.class);
  }

  @Test
  public void numberMethodsForShortAreWhiteListed() throws Exception {
    testNumberMethods(Short.class);
  }

  @Test
  public void numberMethodsForBigDecimalAreWhiteListed() throws Exception {
    testNumberMethods(BigDecimal.class);
  }

  @Test
  public void numberMethodsForNumberAreWhiteListed() throws Exception {
    testNumberMethods(BigInteger.class);
  }

  @Test
  public void numberMethodsForAtomicIntegerAreWhiteListed() throws Exception {
    testNumberMethods(AtomicInteger.class);
  }

  @Test
  public void numberMethodsForAtomicLongAreWhiteListed() throws Exception {
    testNumberMethods(AtomicLong.class);
  }

  @Test
  public void verifyAuthorizersUseDefaultWhiteList() {
    RestrictedMethodInvocationAuthorizer authorizer1 =
        new RestrictedMethodInvocationAuthorizer(null);
    RestrictedMethodInvocationAuthorizer authorizer2 =
        new RestrictedMethodInvocationAuthorizer(null);
    assertThat(authorizer1.getWhiteList()).isSameAs(authorizer2.getWhiteList());
    assertThat(authorizer1.getWhiteList())
        .isSameAs(RestrictedMethodInvocationAuthorizer.DEFAULT_WHITELIST);
    assertThat(authorizer2.getWhiteList())
        .isSameAs(RestrictedMethodInvocationAuthorizer.DEFAULT_WHITELIST);
  }

  private void testNumberMethods(Class<?> clazz) throws NoSuchMethodException {
    Method byteValue = clazz.getMethod("byteValue");
    Method doubleValue = clazz.getMethod("doubleValue");
    Method intValue = clazz.getMethod("intValue");
    Method floatValue = clazz.getMethod("longValue");
    Method longValue = clazz.getMethod("floatValue");
    Method shortValue = clazz.getMethod("shortValue");
    assertTrue(methodInvocationAuthorizer.isWhitelisted(byteValue));
    assertTrue(methodInvocationAuthorizer.isWhitelisted(doubleValue));
    assertTrue(methodInvocationAuthorizer.isWhitelisted(intValue));
    assertTrue(methodInvocationAuthorizer.isWhitelisted(floatValue));
    assertTrue(methodInvocationAuthorizer.isWhitelisted(longValue));
    assertTrue(methodInvocationAuthorizer.isWhitelisted(shortValue));
  }

  private void testMapMethods(Class<?> clazz) throws NoSuchMethodException {
    Method get = clazz.getMethod("get", Object.class);
    Method entrySet = clazz.getMethod("entrySet");
    Method keySet = clazz.getMethod("keySet");
    Method values = clazz.getMethod("values");
    Method getEntries = clazz.getMethod("getEntries");
    Method getValues = clazz.getMethod("getValues");
    Method containsKey = clazz.getMethod("containsKey", Object.class);
    assertTrue(methodInvocationAuthorizer.isWhitelisted(get));
    assertTrue(methodInvocationAuthorizer.isWhitelisted(entrySet));
    assertTrue(methodInvocationAuthorizer.isWhitelisted(keySet));
    assertTrue(methodInvocationAuthorizer.isWhitelisted(values));
    assertTrue(methodInvocationAuthorizer.isWhitelisted(getEntries));
    assertTrue(methodInvocationAuthorizer.isWhitelisted(getValues));
    assertTrue(methodInvocationAuthorizer.isWhitelisted(containsKey));
  }

}
