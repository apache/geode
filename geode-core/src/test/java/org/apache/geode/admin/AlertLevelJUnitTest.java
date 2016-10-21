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
package org.apache.geode.admin;

import static org.apache.geode.internal.Assert.assertTrue;
import static org.junit.Assert.*;

import java.lang.reflect.Constructor;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.UnitTest;

/**
 * AlertLevel Tester.
 */
@Category(UnitTest.class)
public class AlertLevelJUnitTest {

  /**
   * Method: equals(Object other)
   */

  private AlertLevel alertLevel1 = AlertLevel.WARNING;
  private AlertLevel alertLevel2 = AlertLevel.ERROR;
  private AlertLevel alertLevel3 = AlertLevel.WARNING;


  @Test
  public void testEquals() throws Exception {
    // TODO: Test goes here...
    assertTrue(alertLevel1.equals(alertLevel3));
    assertFalse(alertLevel1.equals(alertLevel2));
    assertFalse(alertLevel1.equals(null));

    Constructor<AlertLevel> constructor;
    constructor = AlertLevel.class.getDeclaredConstructor(int.class, String.class);
    constructor.setAccessible(true);
    AlertLevel level = constructor.newInstance(AlertLevel.ERROR.getSeverity(), "ERROR");
    assertEquals(level.getSeverity(), AlertLevel.ERROR.getSeverity());


    AlertLevel level1 =
        constructor.newInstance(AlertLevel.ERROR.getSeverity(), new String("ERROR"));
    assertEquals(level1.getName(), alertLevel2.getName());
    assertTrue(level1.equals(alertLevel2));

  }

}
