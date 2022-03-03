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
package org.apache.geode.internal.lang;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;


/**
 * The ObjectUtilsJUnitTest class is a test suite of test cases for testing the contract and
 * functionality of the ObjectUtils class.
 * <p/>
 *
 * @see org.apache.geode.internal.lang.ObjectUtils
 * @see org.junit.Assert
 * @see org.junit.Test
 */
public class ObjectUtilsJUnitTest {

  @Test
  public void testEqualsWithUnequalObjects() {
    assertFalse(ObjectUtils.equals(null, null));
    assertFalse(ObjectUtils.equals(null, "null"));
    assertFalse(ObjectUtils.equals("nil", null));
    assertFalse(ObjectUtils.equals("nil", "null"));
    assertFalse(ObjectUtils.equals("true", true));
    assertFalse(ObjectUtils.equals(false, true));
    assertFalse(ObjectUtils.equals('c', 'C'));
    assertFalse(ObjectUtils.equals(0.0d, -0.0d));
    assertFalse(ObjectUtils.equals(3.14159d, Math.PI));
    assertFalse(ObjectUtils.equals(Integer.MIN_VALUE, Integer.MAX_VALUE));
    assertFalse(ObjectUtils.equals("test", "TEST"));
  }

  @Test
  public void testEqualsWithEqualObjects() {
    assertTrue(ObjectUtils.equals(true, Boolean.TRUE));
    assertTrue(ObjectUtils.equals('c', 'c'));
    assertTrue(ObjectUtils.equals(Double.MIN_VALUE, Double.MIN_VALUE));
    assertTrue(ObjectUtils.equals(Integer.MAX_VALUE, Integer.MAX_VALUE));
    assertTrue(ObjectUtils.equals("null", "null"));
    assertTrue(ObjectUtils.equals("test", "test"));
  }

  @Test
  public void testEqualsIgnoreNullWithUnequalObjects() {
    assertFalse(ObjectUtils.equalsIgnoreNull(null, "null"));
    assertFalse(ObjectUtils.equalsIgnoreNull("nil", null));
    assertFalse(ObjectUtils.equalsIgnoreNull("nil", "null"));
    assertFalse(ObjectUtils.equalsIgnoreNull("test", "testing"));
  }

  @Test
  public void testEqualsIgnoreNullWithEqualObjects() {
    assertTrue(ObjectUtils.equalsIgnoreNull(null, null));
    assertTrue(ObjectUtils.equalsIgnoreNull("nil", "nil"));
    assertTrue(ObjectUtils.equalsIgnoreNull("null", "null"));
    assertTrue(ObjectUtils.equalsIgnoreNull("test", "test"));
  }

  @Test
  public void testHashCode() {
    assertEquals(0, ObjectUtils.hashCode(null));
    assertEquals(Character.valueOf('c').hashCode(), ObjectUtils.hashCode('c'));
    assertEquals(Boolean.TRUE.hashCode(), ObjectUtils.hashCode(true));
    assertEquals(Double.valueOf(Math.PI).hashCode(), ObjectUtils.hashCode(Math.PI));
    assertEquals(Integer.valueOf(0).hashCode(), ObjectUtils.hashCode(0));
    assertEquals("test".hashCode(), ObjectUtils.hashCode("test"));
  }

  @Test
  public void testToString() {
    assertNull(ObjectUtils.toString(null));
    assertEquals("", ObjectUtils.toString(""));
    assertEquals(" ", ObjectUtils.toString(" "));
    assertEquals("null", ObjectUtils.toString("null"));
    assertEquals("test", ObjectUtils.toString("test"));
    assertEquals("J", ObjectUtils.toString('J'));
    assertEquals("2", ObjectUtils.toString(2));
    assertEquals(String.valueOf(Math.PI), ObjectUtils.toString(Math.PI));
    assertEquals("true", ObjectUtils.toString(Boolean.TRUE));
  }
}
