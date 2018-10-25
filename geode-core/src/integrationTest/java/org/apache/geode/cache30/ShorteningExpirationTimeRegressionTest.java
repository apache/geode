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

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.rules.TestName;

import org.apache.geode.cache.CustomExpiry;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Region.Entry;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.test.junit.rules.ServerStarterRule;

/**
 * If a new expiration time is specified that is shorter than an existing one, ensure the new
 * shorter time is honored.
 *
 * <p>
 * TRAC #44418: Serious limits to CustomExpiry functionality
 *
 * @since GemFire 7.0
 */
public class ShorteningExpirationTimeRegressionTest {

  private static final int LONG_WAIT_MS = 2 * 60 * 1000;
  private static final int SHORT_WAIT_MS = 1;
  private static final String KEY = "key";

  @ClassRule
  public static ServerStarterRule server =
      new ServerStarterRule().withNoCacheServer().withAutoStart();

  @ClassRule
  public static RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Rule
  public TestName testName = new TestName();

  @BeforeClass
  public static void setUp() {
    System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
  }

  @Test
  public void customEntryTimeToLiveCanBeShortened() {
    RegionFactory<String, String> rf = server.getCache().createRegionFactory(RegionShortcut.LOCAL);
    rf.setCustomEntryTimeToLive(new CustomExpiryTestClass<>());
    rf.setStatisticsEnabled(true);

    Region<String, String> region = rf.create(testName.getMethodName());

    // this sets the expiration timeout to LONG_WAIT_MS
    region.put(KEY, "longExpire");
    // this sets the expiration timeout to SHORT_WAIT_MS
    region.put(KEY, "quickExpire");

    // make sure the entry is invalidated before LONG_WAIT_MS (timeout shortened)
    await().until(() -> !region.containsValueForKey(KEY));
  }

  @Test
  public void customEntryIdleTimeoutCanBeShortened() throws Exception {
    RegionFactory<String, String> rf = server.getCache().createRegionFactory(RegionShortcut.LOCAL);
    rf.setCustomEntryIdleTimeout(new CustomExpiryTestClass<>());
    rf.setStatisticsEnabled(true);

    Region<String, String> region = rf.create(testName.getMethodName());

    // this sets the expiration timeout to LONG_WAIT_MS
    region.put(KEY, "longExpire");
    // this sets the expiration timeout to SHORT_WAIT_MS
    region.get(KEY);

    // make sure the entry is invalidated before LONG_WAIT_MS (timeout shortened)
    await().until(() -> !region.containsValueForKey(KEY));
  }

  private class CustomExpiryTestClass<K, V> implements CustomExpiry<K, V> {

    private volatile boolean useShortExpiration;

    @Override
    public void close() {
      // nothing
    }

    @Override
    public ExpirationAttributes getExpiry(Entry entry) {
      ExpirationAttributes result;
      if (!useShortExpiration) {
        // Set long expiration first time entry referenced
        result = new ExpirationAttributes(LONG_WAIT_MS);
        useShortExpiration = true;
      } else {
        // Set short expiration second time entry referenced
        result = new ExpirationAttributes(SHORT_WAIT_MS);
      }
      return result;
    }
  }
}
