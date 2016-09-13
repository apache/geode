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
package org.apache.geode.internal.cache.xmlcache;

import static org.apache.geode.internal.cache.xmlcache.CacheXmlVersion.*;
import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.UnitTest;

/**
 * @since GemFire 8.1
 */
@Category(UnitTest.class)
public class CacheXmlVersionJUnitTest {

  /**
   * Previous strings based version just check ordinal comparison of strings. It
   * failed test for "8_0".compareTo("8.1") < 0. It also would have failed for
   * "9.0".compareTo("10.0") < 0. Testing that ENUM based solution is ordinal
   * correct for comparisons.
   * 
   * @since GemFire 8.1
   */
  @Test
  public void testOrdinal() {
    assertTrue(GEMFIRE_3_0.compareTo(GEMFIRE_4_0) < 0);
    assertTrue(GEMFIRE_4_0.compareTo(GEMFIRE_4_1) < 0);
    assertTrue(GEMFIRE_4_1.compareTo(GEMFIRE_5_0) < 0);
    assertTrue(GEMFIRE_5_0.compareTo(GEMFIRE_5_1) < 0);
    assertTrue(GEMFIRE_5_1.compareTo(GEMFIRE_5_5) < 0);
    assertTrue(GEMFIRE_5_5.compareTo(GEMFIRE_5_7) < 0);
    assertTrue(GEMFIRE_5_7.compareTo(GEMFIRE_5_8) < 0);
    assertTrue(GEMFIRE_5_8.compareTo(GEMFIRE_6_0) < 0);
    assertTrue(GEMFIRE_6_0.compareTo(GEMFIRE_6_1) < 0);
    assertTrue(GEMFIRE_6_1.compareTo(GEMFIRE_6_5) < 0);
    assertTrue(GEMFIRE_6_5.compareTo(GEMFIRE_6_6) < 0);
    assertTrue(GEMFIRE_6_6.compareTo(GEMFIRE_7_0) < 0);
    assertTrue(GEMFIRE_7_0.compareTo(GEMFIRE_8_0) < 0);
    assertTrue(GEMFIRE_8_0.compareTo(GEMFIRE_8_1) < 0);
    assertTrue(GEMFIRE_8_1.compareTo(GEODE_1_0) < 0);
  }

  /**
   * Test that {@link CacheXmlVersion#valueForVersion(String)} matches the same
   * {@link CacheXmlVersion} via {@link CacheXmlVersion#getVersion()}.
   * 
   * @since GemFire 8.1
   */
  @Test
  public void testValueForVersion() {
    for (final CacheXmlVersion cacheXmlVersion : CacheXmlVersion.values()) {
      assertSame(cacheXmlVersion, CacheXmlVersion.valueForVersion(cacheXmlVersion.getVersion()));
    }
  }

}
