/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import static org.junit.Assert.assertEquals;

import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Confirm that bug 34583 is fixed. Cause of bug is recursion is
 * entries iterator that causes stack overflow.
 * @author darrel
 */
@Category(IntegrationTest.class)
public class Bug34583JUnitTest {
  
  public Bug34583JUnitTest() {
  }
  
  public void setup() {
  }
  
  @After
  public void tearDown() {
  }
  
  
  
  @Test
  public void testBunchOfInvalidEntries() throws Exception {
    Properties props = new Properties();
    props.setProperty("mcast-port", "0");
    DistributedSystem ds = DistributedSystem.connect(props);
    try {
      AttributesFactory factory = new AttributesFactory();
      Cache cache = null;
      cache = CacheFactory.create(ds);
     
      Region r = cache.createRegion("testRegion", factory.create());
      final int ENTRY_COUNT = 25000;
      {
        for (int i=1; i <= ENTRY_COUNT; i++) {
          r.put("key" + i, "value" + i);
        }
      }
      { // make sure iterator works while values are valid
        Collection c = r.values();
        assertEquals(ENTRY_COUNT, c.size());
        Iterator it = c.iterator();
        int count = 0;
        while (it.hasNext()) {
          it.next();
          count++;
        }
        assertEquals(ENTRY_COUNT, count);
      }
      r.localInvalidateRegion();
      // now we expect iterator to stackOverflow if bug 34583
      {
        Collection c = r.values();
        assertEquals(0, c.size());
        Iterator it = c.iterator();
        int count = 0;
        while (it.hasNext()) {
          it.next();
          count++;
        }
        assertEquals(0, count);
      }
    } finally {
      ds.disconnect();
    }
  }
}
