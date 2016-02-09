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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheLoaderException;
import com.gemstone.gemfire.cache.LoaderHelper;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;

/**
 * Test to make sure cache close is working.
 *
 * @author darrel
 * @since 3.0
 */
public class CacheCloseDUnitTest extends CacheTestCase {

  public CacheCloseDUnitTest(String name) {
    super(name);
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
          public Object load2(LoaderHelper helper)
            throws CacheLoaderException
          {
            fail("load2 should not be called by this test");
            return null;
          }
          public void close2() {
            throw new RuntimeException("make sure exceptions from close callbacks are logged and ignored");
          }
        };
      callbacks.add(loader);
      factory.setCacheLoader(loader);
    }
    
    return factory.create();
  }
  
  //////////////////////  Test Methods  //////////////////////

  public void testCallbacksClosed() throws CacheException {
    {
      List callbacks = new ArrayList();

      // create a root region with callbacks
      Region r = createRootRegion(createAtts(callbacks));

      // create a sub region with callbacks
      r.createSubregion("testCallbacksClosed", createAtts(callbacks));

      closeCache();

      // make sure all callbacks called
      Iterator it = callbacks.iterator();
      while (it.hasNext()) {
        TestCacheCallback listener = (TestCacheCallback)it.next();
        assertTrue("listener not invoked: " + listener,
            listener.isClosed());
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
