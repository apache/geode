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
/**
 * 
 */
package com.gemstone.gemfire.cache30;

import static org.junit.Assert.fail;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CustomExpiry;
import com.gemstone.gemfire.cache.ExpirationAttributes;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Region.Entry;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.test.junit.categories.FlakyTest;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Test for Bug 44418.
 * 
 * @since 7.0
 */
@Category(IntegrationTest.class)
@SuppressWarnings({ "unchecked", "rawtypes" })
public class Bug44418JUnitTest { // TODO: rename this test to non-ticket descriptive name

  DistributedSystem ds;
  Cache cache;

  @Category(FlakyTest.class) // GEODE-1139: time sensitive, thread sleep, expiration
  @Test
  public void testPut() throws Exception {

    System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
    try {
      final Region r = this.cache.createRegionFactory(RegionShortcut.LOCAL)
      .setStatisticsEnabled(true)
      .setCustomEntryTimeToLive(new CustomExpiry() {
        @Override
        public void close() {
        }
        @Override
        public ExpirationAttributes getExpiry(Entry entry) {
          ExpirationAttributes result;
          if (entry.getValue().equals("longExpire")) {
            result = new ExpirationAttributes(5000);
          } else {
            result = new ExpirationAttributes(1);
          }
          //Bug44418JUnitTest.this.cache.getLogger().info("in getExpiry result=" + result, new RuntimeException("STACK"));
          return result;
        }
      })
      .create("bug44418");
      r.put("key", "longExpire");
      // should take 5000 ms to expire.
      // Now update it with a short expire time
      r.put("key", "quickExpire");
      // now wait to see it expire. We only wait
      // 1000 ms. If we need to wait that long
      // for a 1 ms expire then the expiration
      // is probably still set at 5000 ms.
      long giveup = System.currentTimeMillis() + 10000;
      boolean done = false;
      do {
        Thread.sleep(10);
        done = r.containsValueForKey("key");
      } while (!done && System.currentTimeMillis() < giveup);

      if (r.containsValueForKey("key")) {
        fail("1 ms expire did not happen after waiting 1000 ms");
      }
    } finally {
      System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
    }
  }

  @Category(FlakyTest.class) // GEODE-924: expiration, time sensitive, expects action in 1 second
  @Test
  public void testGet() throws Exception {

    System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
    try {
      final Region r = this.cache.createRegionFactory(RegionShortcut.LOCAL)
      .setStatisticsEnabled(true)
      .setCustomEntryIdleTimeout(new CustomExpiry() {
        private boolean secondTime;
        @Override
        public void close() {
        }
        @Override
        public ExpirationAttributes getExpiry(Entry entry) {
          ExpirationAttributes result;
          if (!this.secondTime) {
            result = new ExpirationAttributes(5000);
            this.secondTime = true;
          } else {
            result = new ExpirationAttributes(1);
          }
          Bug44418JUnitTest.this.cache.getLogger().info("in getExpiry result=" + result, new RuntimeException("STACK"));
          return result;
        }
      })
      .create("bug44418");
      r.put("key", "longExpire");
      // should take 5000 ms to expire.
      r.get("key");
      // now wait to see it expire. We only wait
      // 1000 ms. If we need to wait that long
      // for a 1 ms expire then the expiration
      // is probably still set at 5000 ms.
      long giveup = System.currentTimeMillis() + 1000;
      boolean done = false;
      do {
        Thread.sleep(10);
        // If the value is gone then we expired and are done
        done = r.containsValueForKey("key");
      } while (!done && System.currentTimeMillis() < giveup);

      if (r.containsValueForKey("key")) {
        fail("1 ms expire did not happen after waiting 1000 ms");
      }
    } finally {
      System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
    }
  }

  @After
  public void tearDown() throws Exception {
    if (this.cache != null) {
      this.cache.close();
      this.cache = null;
    }
    if (this.ds != null) {
      this.ds.disconnect();
      this.ds = null;
    }
  }

  @Before
  public void setUp() throws Exception {
    Properties props = new Properties();
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    this.ds = DistributedSystem.connect(props);
    this.cache = CacheFactory.create(this.ds);
  }

}
