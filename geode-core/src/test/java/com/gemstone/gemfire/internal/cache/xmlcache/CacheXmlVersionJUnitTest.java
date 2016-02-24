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
package com.gemstone.gemfire.internal.cache.xmlcache;

import static com.gemstone.gemfire.internal.cache.xmlcache.CacheXmlVersion.*;
import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 *
 * @since 8.1
 */
@Category(UnitTest.class)
public class CacheXmlVersionJUnitTest {

  /**
   * Previous strings based version just check ordinal comparison of strings. It
   * failed test for "8_0".compareTo("8.1") < 0. It also would have failed for
   * "9.0".compareTo("10.0") < 0. Testing that ENUM based solution is ordinal
   * correct for comparisons.
   * 
   * @since 8.1
   */
  @Test
  public void testOrdinal() {
    assertTrue(VERSION_3_0.compareTo(VERSION_4_0) < 0);
    assertTrue(VERSION_4_0.compareTo(VERSION_4_1) < 0);
    assertTrue(VERSION_4_1.compareTo(VERSION_5_0) < 0);
    assertTrue(VERSION_5_0.compareTo(VERSION_5_1) < 0);
    assertTrue(VERSION_5_1.compareTo(VERSION_5_5) < 0);
    assertTrue(VERSION_5_5.compareTo(VERSION_5_7) < 0);
    assertTrue(VERSION_5_7.compareTo(VERSION_5_8) < 0);
    assertTrue(VERSION_5_8.compareTo(VERSION_6_0) < 0);
    assertTrue(VERSION_6_0.compareTo(VERSION_6_1) < 0);
    assertTrue(VERSION_6_1.compareTo(VERSION_6_5) < 0);
    assertTrue(VERSION_6_5.compareTo(VERSION_6_6) < 0);
    assertTrue(VERSION_6_6.compareTo(VERSION_7_0) < 0);
    assertTrue(VERSION_7_0.compareTo(VERSION_8_0) < 0);
    assertTrue(VERSION_8_0.compareTo(VERSION_8_1) < 0);
    assertTrue(VERSION_8_1.compareTo(VERSION_9_0) < 0);
  }

  /**
   * Test that {@link CacheXmlVersion#valueForVersion(String)} matches the same
   * {@link CacheXmlVersion} via {@link CacheXmlVersion#getVersion()}.
   * 
   * @since 8.1
   */
  @Test
  public void testValueForVersion() {
    for (final CacheXmlVersion cacheXmlVersion : CacheXmlVersion.values()) {
      assertSame(cacheXmlVersion, CacheXmlVersion.valueForVersion(cacheXmlVersion.getVersion()));
    }
  }

}
