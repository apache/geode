/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache30;

//import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.*;
//import com.gemstone.gemfire.cache.util.*;
//import com.gemstone.gemfire.distributed.DistributedSystem;
//import dunit.*;
import java.util.*;

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
