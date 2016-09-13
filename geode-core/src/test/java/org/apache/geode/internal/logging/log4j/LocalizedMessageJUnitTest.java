/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.logging.log4j;

import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.i18n.StringId;
import org.apache.geode.test.junit.categories.UnitTest;

/**
 * Tests for LocalizedMessage which bridges our StringId LocalizedStrings for 
 * Log4J2 usage.
 */
@Category(UnitTest.class)
public class LocalizedMessageJUnitTest {

  @Test
  public void testZeroParams() {
    final StringId stringId = new StringId(100, "This is a message for testZeroParams");
    final LocalizedMessage message = LocalizedMessage.create(stringId);
    assertNull(message.getParameters());
  }
  
  @Test
  public void testEmptyParams() {
    final StringId stringId = new StringId(100, "This is a message for testEmptyParams");
    final LocalizedMessage message = LocalizedMessage.create(stringId, new Object[] {});
    final Object[] object = message.getParameters();
    assertNotNull(object);
    assertEquals(0, object.length);
  }
  
  @Test
  public void testGetThrowable() {
    final Throwable t = new Throwable();
    final StringId stringId = new StringId(100, "This is a message for testGetThrowable");
    final LocalizedMessage message = LocalizedMessage.create(stringId, t);
    assertNotNull(message.getThrowable());
    assertEquals(t, message.getThrowable());
    assertTrue(t == message.getThrowable());
  }
}
