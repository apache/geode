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
package com.gemstone.gemfire.cache30;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXml;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;

/**
 * Tests the declarative caching functionality introduced in the GemFire
 * 5.0 (i.e. congo1). Don't be confused by the 45 in my name :-)
 * 
 * @since GemFire 5.0
 */

@Category(DistributedTest.class)
public class CacheXml55DUnitTest extends CacheXml51DUnitTest
{

  // ////// Constructors

  public CacheXml55DUnitTest() {
    super();
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
  @Test
  public void testEmpty() throws CacheException
  {}

}
