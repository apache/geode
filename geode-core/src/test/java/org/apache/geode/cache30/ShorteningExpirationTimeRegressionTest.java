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
/**
 *
 */
package org.apache.geode.cache30;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.awaitility.Awaitility.with;
import static org.junit.Assert.fail;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.awaitility.core.ConditionTimeoutException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CustomExpiry;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Region.Entry;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.test.junit.categories.FlakyTest;
import org.apache.geode.test.junit.categories.IntegrationTest;

/**
 * If a new expiration time is specified that is shorter than an existing one, ensure the new
 * shorter time is honored.
 *
 * <p>
 * TRAC #44418: Serious limits to CustomExpiry functionality
 *
 * @since GemFire 7.0
 */
@Category(IntegrationTest.class)
public class ShorteningExpirationTimeRegressionTest {

  /**
   * Initial expiration time for entry
   */
  private static final int LONG_WAIT_MS = 1000 * 60 * 3;

  /**
   * How long to wait for entry to expire
   */
  private static final int TEST_WAIT_MS = 1000 * 60 * 1;

  /**
   * New short expiration time for entry
   */
  private static final int SHORT_WAIT_MS = 1;

  /**
   * How often to check for expiration
   */
  private static final int POLL_INTERVAL_MS = 1;

  private static final String TEST_KEY = "key";

  private DistributedSystem ds;
  private Cache cache;

  @Before
  public void setUp() throws Exception {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    ds = DistributedSystem.connect(props);
    cache = CacheFactory.create(ds);
  }

  @After
  public void tearDown() throws Exception {
    if (cache != null) {
      cache.close();
      cache = null;
    }
    if (ds != null) {
      ds.disconnect();
      ds = null;
    }
  }

  @Category(FlakyTest.class) // GEODE-1139: time sensitive, thread sleep, expiration
  @Test
  public void testPut() throws Exception {

    System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
    try {
      final Region r = cache.createRegionFactory(RegionShortcut.LOCAL).setStatisticsEnabled(true)
          .setCustomEntryTimeToLive(new CustomExpiryTestClass()).create("bug44418");

      r.put(TEST_KEY, "longExpire");
      // should take LONG_WAIT_MS to expire.

      // Now update it with a short time to live
      r.put(TEST_KEY, "quickExpire");

      if (!awaitExpiration(r, TEST_KEY)) {
        fail(SHORT_WAIT_MS + " ms expire did not happen after waiting " + TEST_WAIT_MS + " ms");
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
      final Region r = cache.createRegionFactory(RegionShortcut.LOCAL).setStatisticsEnabled(true)
          .setCustomEntryIdleTimeout(new CustomExpiryTestClass()).create("bug44418");

      r.put(TEST_KEY, "longExpire");
      // should take LONG_WAIT_MS to expire.

      // Now set a short idle time
      r.get(TEST_KEY);

      if (!awaitExpiration(r, TEST_KEY)) {
        fail(SHORT_WAIT_MS + " ms expire did not happen after waiting " + TEST_WAIT_MS + " ms");
      }
    } finally {
      System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
    }
  }

  private boolean awaitExpiration(Region r, Object key) {
    // Return true if entry expires. We only wait
    // TEST_WAIT_MS. If we need to wait that long for
    // a SHORT_WAIT_MS to expire then the expiration
    // is probably still set at LONG_WAIT_MS.
    try {
      with().pollInterval(POLL_INTERVAL_MS, TimeUnit.MILLISECONDS).await()
          .atMost(TEST_WAIT_MS, TimeUnit.MILLISECONDS).until(() -> !r.containsValueForKey(key));
    } catch (ConditionTimeoutException toe) {
      return false;
    }
    return true;
  }

  private class CustomExpiryTestClass implements CustomExpiry {

    private boolean secondTime;

    @Override
    public void close() {
      // nothing
    }

    @Override
    public ExpirationAttributes getExpiry(Entry entry) {
      ExpirationAttributes result;
      if (!secondTime) {
        // Set long expiration first time entry referenced
        result = new ExpirationAttributes(LONG_WAIT_MS);
        secondTime = true;
      } else {
        // Set short expiration second time entry referenced
        result = new ExpirationAttributes(SHORT_WAIT_MS);
      }
      return result;
    }
  }
}
