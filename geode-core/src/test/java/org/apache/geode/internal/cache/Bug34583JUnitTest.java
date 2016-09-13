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
package org.apache.geode.internal.cache;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertEquals;

/**
 * Confirm that bug 34583 is fixed. Cause of bug is recursion is
 * entries iterator that causes stack overflow.
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
    props.setProperty(MCAST_PORT, "0");
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
