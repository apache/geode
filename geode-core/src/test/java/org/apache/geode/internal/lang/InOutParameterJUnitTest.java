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

import org.junit.Test;


/**
 * The InOutParameterJUnitTest class is a test suite with test cases to test the contract and
 * functionality of the InOutParameter class.
 * <p/>
 *
 * @see org.apache.geode.internal.lang.InOutParameter
 * @see org.junit.Test
 * @since GemFire 6.8
 */
public class InOutParameterJUnitTest {

  @Test
  public void testEquals() {
    assertEquals(new InOutParameter<>(null), new InOutParameter<>(null));
    assertEquals(new InOutParameter<>(null), null);
    assertEquals(new InOutParameter<>("test"), new InOutParameter<>("test"));
    assertEquals(new InOutParameter<>("test"), "test");
    assertEquals(new InOutParameter<>(Math.PI), new InOutParameter<>(Math.PI));
    assertEquals(new InOutParameter<>(Math.PI), Math.PI);
    assertEquals(new InOutParameter<>(true), new InOutParameter<>(true));
    assertEquals(new InOutParameter<>(true), true);
  }

  @Test
  public void testNotEqual() {
    assertFalse(new InOutParameter<>("null").equals(new InOutParameter<>(null)));
    assertFalse(new InOutParameter<>(Math.PI).equals(3.14159d));
    assertFalse(new InOutParameter<>("test").equals("TEST"));
  }

  @Test
  public void testHashCode() {
    assertEquals(0, new InOutParameter<>(null).hashCode());
    assertEquals("test".hashCode(), new InOutParameter<>("test").hashCode());
    assertEquals(new Double(Math.PI).hashCode(), new InOutParameter<>(Math.PI).hashCode());
    assertEquals(Boolean.TRUE.hashCode(), new InOutParameter<>(true).hashCode());
  }

  @Test
  public void testToString() {
    assertEquals("null", new InOutParameter<>(null).toString());
    assertEquals("null", new InOutParameter<>("null").toString());
    assertEquals("test", new InOutParameter<>("test").toString());
    assertEquals(String.valueOf(Math.PI), new InOutParameter<>(Math.PI).toString());
    assertEquals("true", new InOutParameter<>(Boolean.TRUE).toString());
  }

}
