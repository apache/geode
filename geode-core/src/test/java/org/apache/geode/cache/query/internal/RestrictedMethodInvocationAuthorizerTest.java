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


import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
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
    Class clazz = PartitionedRegion.class;
    Method entrySet = clazz.getMethod("entrySet");
    Method keySet = clazz.getMethod("keySet");
    Method values = clazz.getMethod("values");
    Method containsKey = clazz.getMethod("containsKey", Object.class);
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

  private void testNumberMethods(Class clazz) throws NoSuchMethodException {
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

  private void testMapMethods(Class clazz) throws NoSuchMethodException {
    Method entrySet = clazz.getMethod("entrySet");
    Method keySet = clazz.getMethod("keySet");
    Method values = clazz.getMethod("values");
    Method getEntries = clazz.getMethod("getEntries");
    Method getValues = clazz.getMethod("getValues");
    Method containsKey = clazz.getMethod("containsKey", Object.class);
    assertTrue(methodInvocationAuthorizer.isWhitelisted(entrySet));
    assertTrue(methodInvocationAuthorizer.isWhitelisted(keySet));
    assertTrue(methodInvocationAuthorizer.isWhitelisted(values));
    assertTrue(methodInvocationAuthorizer.isWhitelisted(getEntries));
    assertTrue(methodInvocationAuthorizer.isWhitelisted(getValues));
    assertTrue(methodInvocationAuthorizer.isWhitelisted(containsKey));
  }

}
