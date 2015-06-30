/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * =========================================================================
 */

package com.gemstone.gemfire.internal.lang;

import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * The InOutParameterJUnitTest class is a test suite with test cases to test the contract and functionality of the
 * InOutParameter class.
 * <p/>
 * @author John Blum
 * @see com.gemstone.gemfire.internal.lang.InOutParameter
 * @see org.junit.Test
 * @since 6.8
 */
@Category(UnitTest.class)
public class InOutParameterJUnitTest {

  @Test
  public void testEquals() {
    assertEquals(new InOutParameter<Object>(null), new InOutParameter<Object>(null));
    assertEquals(new InOutParameter<Object>(null), null);
    assertEquals(new InOutParameter<Object>("test"), new InOutParameter<Object>("test"));
    assertEquals(new InOutParameter<Object>("test"), "test");
    assertEquals(new InOutParameter<Object>(Math.PI), new InOutParameter<Object>(Math.PI));
    assertEquals(new InOutParameter<Object>(Math.PI), Math.PI);
    assertEquals(new InOutParameter<Object>(true), new InOutParameter<Object>(true));
    assertEquals(new InOutParameter<Object>(true), true);
  }

  @Test
  public void testNotEqual() {
    assertFalse(new InOutParameter<Object>("null").equals(new InOutParameter<Object>(null)));
    assertFalse(new InOutParameter<Object>(Math.PI).equals(3.14159d));
    assertFalse(new InOutParameter<Object>("test").equals("TEST"));
  }

  @Test
  public void testHashCode() {
    assertEquals(0, new InOutParameter<Object>(null).hashCode());
    assertEquals("test".hashCode(), new InOutParameter<Object>("test").hashCode());
    assertEquals(new Double(Math.PI).hashCode(), new InOutParameter<Object>(Math.PI).hashCode());
    assertEquals(Boolean.TRUE.hashCode(), new InOutParameter<Object>(true).hashCode());
  }

  @Test
  public void testToString() {
    assertEquals("null", new InOutParameter<Object>(null).toString());
    assertEquals("null", new InOutParameter<Object>("null").toString());
    assertEquals("test", new InOutParameter<Object>("test").toString());
    assertEquals(String.valueOf(Math.PI), new InOutParameter<Object>(Math.PI).toString());
    assertEquals("true", new InOutParameter<Object>(Boolean.TRUE).toString());
  }

}
