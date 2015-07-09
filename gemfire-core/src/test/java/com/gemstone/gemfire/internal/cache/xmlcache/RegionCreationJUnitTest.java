/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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
 * @author jbarrett@pivotal.io
 *
 * @since 8.1
 */
@Category(UnitTest.class)
public class RegionCreationJUnitTest {

  /**
   * Test method for {@link RegionCreation#getExtensionPoint()}.
   * 
   * Assert that method returns a {@link SimpleExtensionPoint} instance and
   * assume that {@link SimpleExtensionPointJUnitTest} has covered the rest.
   * 
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
