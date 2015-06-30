/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache30;

import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXml;

/**
 * Tests the declarative caching functionality introduced in the GemFire
 * 5.0 (i.e. congo1). Don't be confused by the 45 in my name :-)
 * 
 * @author Mitch Thomas
 * @since 5.0
 */

public class CacheXml55DUnitTest extends CacheXml51DUnitTest
{

  // ////// Constructors

  public CacheXml55DUnitTest(String name) {
    super(name);
  }

  // ////// Helper methods

  protected String getGemFireVersion()
  {
    return CacheXml.VERSION_5_5;
  }

  /**
   * Tests that a region created with a named attributes has the correct
   * attributes.
   */
  public void testEmpty() throws CacheException
  {}

}
