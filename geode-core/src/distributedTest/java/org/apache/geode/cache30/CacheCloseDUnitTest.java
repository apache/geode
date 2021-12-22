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
package org.apache.geode.cache30;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.LoaderHelper;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;

/**
 * Test to make sure cache close is working.
 *
 * @since GemFire 3.0
 */

public class CacheCloseDUnitTest extends JUnit4CacheTestCase {

  public CacheCloseDUnitTest() {
    super();
  }

  private RegionAttributes createAtts(List callbacks) {
    AttributesFactory factory = new AttributesFactory();

    {
      TestCacheListener listener = new TestCacheListener() {};
      callbacks.add(listener);
      factory.setCacheListener(listener);
    }
    {
      TestCacheWriter writer = new TestCacheWriter() {};
      callbacks.add(writer);
      factory.setCacheWriter(writer);
    }
    {
      TestCacheLoader loader = new TestCacheLoader() {
        @Override
        public Object load2(LoaderHelper helper) throws CacheLoaderException {
          fail("load2 should not be called by this test");
          return null;
        }

        @Override
        public void close2() {
          throw new RuntimeException(
              "make sure exceptions from close callbacks are logged and ignored");
        }
      };
      callbacks.add(loader);
      factory.setCacheLoader(loader);
    }

    return factory.create();
  }

  ////////////////////// Test Methods //////////////////////

  @Test
  public void testCallbacksClosed() throws CacheException {
    {
      List callbacks = new ArrayList();

      // create a root region with callbacks
      Region r = createRootRegion(createAtts(callbacks));

      // create a sub region with callbacks
      r.createSubregion("testCallbacksClosed", createAtts(callbacks));

      closeCache();

      // make sure all callbacks called
      for (final Object callback : callbacks) {
        TestCacheCallback listener = (TestCacheCallback) callback;
        assertTrue("listener not invoked: " + listener, listener.isClosed());
      }
    }

    // now recreate the cache and root region so they can be destroyed
    // during teardown
    {
      AttributesFactory factory = new AttributesFactory();
      createRootRegion(factory.create());
    }
  }
}
