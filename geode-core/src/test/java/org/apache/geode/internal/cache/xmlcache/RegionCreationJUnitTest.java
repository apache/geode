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

import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.cache.extension.ExtensionPoint;
import com.gemstone.gemfire.internal.cache.extension.SimpleExtensionPoint;
import com.gemstone.gemfire.internal.cache.extension.SimpleExtensionPointJUnitTest;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * Unit test for {@link RegionCreation}.
 * 
 * @since GemFire 8.1
 */
@Category(UnitTest.class)
public class RegionCreationJUnitTest {

  /**
   * Test method for {@link RegionCreation#getExtensionPoint()}.
   * 
   * Assert that method returns a {@link SimpleExtensionPoint} instance and
   * assume that {@link SimpleExtensionPointJUnitTest} has covered the rest.
   */
  @Test
  public void testGetExtensionPoint() {
    final CacheCreation cache = new CacheCreation();
    final RegionCreation region = new RegionCreation(cache, "test");
    final ExtensionPoint<Region<?, ?>> extensionPoint = region.getExtensionPoint();
    assertNotNull(extensionPoint);
    assertEquals(extensionPoint.getClass(), SimpleExtensionPoint.class);
  }

}
