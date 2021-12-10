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
package org.apache.geode.internal.lang.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Calendar;
import java.util.Date;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

/**
 * The ClassUtilsJUnitTest class is a test suite with test cases to test the contract and
 * functionality of the ClassUtils class.
 * <p/>
 *
 * @see ClassUtils
 * @see org.junit.Assert
 * @see org.junit.Test
 * @since GemFire 7.0
 */
public class ClassUtilsTest {

  @Test
  public void testForNameWithExistingClass() {
    assertEquals(Object.class,
        ClassUtils.forName("java.lang.Object", new RuntimeException("unexpected")));
  }

  @Test(expected = RuntimeException.class)
  public void testForNameWithNonExistingClass() {
    try {
      ClassUtils.forName("com.mycompany.non.existing.Class", new RuntimeException("expected"));
    } catch (RuntimeException expected) {
      assertEquals("expected", expected.getMessage());
      throw expected;
    }
  }

  @Test(expected = NullPointerException.class)
  public void testForNameWithNullClassName() {
    ClassUtils.forName(null, new IllegalArgumentException("Null Class!"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testForNameWithEmptyClassName() {
    try {
      ClassUtils.forName(StringUtils.EMPTY, new IllegalArgumentException("Empty Class Name!"));
    } catch (IllegalArgumentException expected) {
      assertEquals("Empty Class Name!", expected.getMessage());
      throw expected;
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testForNameWithBlankClassName() {
    try {
      ClassUtils.forName("  ", new IllegalArgumentException("Blank Class Name!"));
    } catch (IllegalArgumentException expected) {
      assertEquals("Blank Class Name!", expected.getMessage());
      throw expected;
    }
  }

  @Test(expected = NullPointerException.class)
  public void testForNameThrowsNullPointerExceptionForNullRuntimeExceptionArgument() {
    ClassUtils.forName("com.mycompany.non.existing.Class", null);
  }

  @Test
  public void testGetClassWithNull() {
    assertNull(ClassUtils.getClass(null));
  }

  @Test
  public void testGetClassWithObject() {
    assertEquals(java.lang.String.class, ClassUtils.getClass("test"));
  }

  @Test
  public void testGetClassNameWithNull() {
    assertNull(ClassUtils.getClassName(null));
  }

  @Test
  public void testGetClassNameWithObject() {
    assertEquals(java.lang.String.class.getName(), ClassUtils.getClassName("null"));
  }

  @Test
  public void testIsClassAvailableWithExistingClass() {
    assertTrue(ClassUtils.isClassAvailable("java.lang.Object"));
  }

  @Test
  public void testIsClassAvailableWithNonExistingClass() {
    assertFalse(ClassUtils.isClassAvailable("com.mycompany.non.existing.Class"));
  }

  @Test
  public void testIsAnInstanceOf() {
    assertTrue(ClassUtils.isInstanceOf(Object.class, "null"));
    assertTrue(ClassUtils.isInstanceOf(String.class, "C"));
    assertTrue(ClassUtils.isInstanceOf(Number.class, Math.PI));
    assertTrue(ClassUtils.isInstanceOf(Number.class, 3.14f));
    assertTrue(ClassUtils.isInstanceOf(Number.class, 0l));
    assertTrue(ClassUtils.isInstanceOf(Number.class, 0));
    assertTrue(ClassUtils.isInstanceOf(Boolean.class, true));
    assertTrue(ClassUtils.isInstanceOf(Boolean.class, false));
  }

  @Test
  public void testIsNotAnInstanceOf() {
    assertFalse(ClassUtils.isInstanceOf(null, null));
    assertFalse(ClassUtils.isInstanceOf(null, new Object()));
    assertFalse(ClassUtils.isInstanceOf(Object.class, null));
    assertFalse(ClassUtils.isInstanceOf(String.class, 'C'));
    assertFalse(ClassUtils.isInstanceOf(Long.class, Math.PI));
    assertFalse(ClassUtils.isInstanceOf(Double.class, 3.14f));
    assertFalse(ClassUtils.isInstanceOf(Date.class, Calendar.getInstance()));
    assertFalse(ClassUtils.isInstanceOf(Boolean.class, 1));
    assertFalse(ClassUtils.isInstanceOf(Boolean.class, "false"));
  }

  @Test
  public void testIsNotInstanceOfWithNoTypes() {
    assertTrue(ClassUtils.isNotInstanceOf("test"));
  }

  @Test
  public void testIsNotInstanceOfWithMultipleTypes() {
    assertTrue(ClassUtils.isNotInstanceOf("test", Boolean.class, Character.class, Integer.class,
        Double.class));
  }

  @Test
  public void testIsNotInstanceOfWithMultipleNumberTypes() {
    assertFalse(ClassUtils.isNotInstanceOf(1, Double.class, Long.class, Number.class));
  }

  @Test
  public void testIsNotInstanceOfWithMultipleCompatibleTypes() {
    assertFalse(ClassUtils.isNotInstanceOf(1, Double.class, Float.class, Integer.class, Long.class,
        Number.class));
  }

}
