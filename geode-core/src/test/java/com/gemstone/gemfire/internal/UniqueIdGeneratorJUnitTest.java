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
package com.gemstone.gemfire.internal;

import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * Tests UniqueIdGenerator.
 * @since GemFire 5.0.2 (cbb5x_PerfScale)
 */
@Category(UnitTest.class)
public class UniqueIdGeneratorJUnitTest {
  
  @Test
  public void testBasics() throws Exception {
    UniqueIdGenerator uig = new UniqueIdGenerator(1);
    assertEquals(0, uig.obtain());
    try {
      uig.obtain();
      fail("expected IllegalStateException");
    } catch (IllegalStateException expected) {
    }
    uig.release(0);
    assertEquals(0, uig.obtain());

    uig = new UniqueIdGenerator(32768);
    for (int i=0; i < 32768; i++) {
      assertEquals(i, uig.obtain());
    }
    try {
      uig.obtain();
      fail("expected IllegalStateException");
    } catch (IllegalStateException expected) {
    }
    for (int i=32767; i >= 0; i--) {
      uig.release(i);
      assertEquals(i, uig.obtain());
    }
  }
}
