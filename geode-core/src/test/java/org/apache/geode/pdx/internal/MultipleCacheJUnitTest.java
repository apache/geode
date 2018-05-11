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
package org.apache.geode.pdx.internal;

import static org.apache.geode.internal.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.test.junit.categories.ClientServerTest;
import org.apache.geode.test.junit.categories.IntegrationTest;

/**
 * Test that we can create multiple caches in the same JVM
 */
@Category({IntegrationTest.class, ClientServerTest.class})
public class MultipleCacheJUnitTest {

  @Rule
  public TemporaryFolder locatorFolder = new TemporaryFolder();

  private List<Cache> caches = new ArrayList<>();
  private Properties props;
  private Locator locator;

  @Before
  public void startLocator() throws IOException {
    InternalDistributedSystem.ALLOW_MULTIPLE_SYSTEMS = true;
    locator = Locator.startLocatorAndDS(0, locatorFolder.newFile("locator.log"), null);
    props = new Properties();
    props.setProperty(ConfigurationProperties.LOCATORS, "locahost[" + locator.getPort() + "]");
  }

  @After
  public void closeSystemsAndLocator() {
    try {
      caches.forEach(Cache::close);
      locator.stop();
    } finally {
      InternalDistributedSystem.ALLOW_MULTIPLE_SYSTEMS = false;
    }
  }

  @Test
  public void cacheCreateTwoPeerCaches() throws IOException {
    Cache cache1 = createCache("cache1");
    Cache cache2 = createCache("cache2");

    assertNotEquals(cache1, cache2);

    DistributedMember member1 = cache1.getDistributedSystem().getDistributedMember();

    DistributedMember member2 = cache2.getDistributedSystem().getDistributedMember();

    assertNotEquals(member1, member2);
    assertTrue(cache2.getDistributedSystem().getAllOtherMembers().contains(member1));
    assertTrue(cache1.getDistributedSystem().getAllOtherMembers().contains(member2));
  }


  @Test
  public void canCreateTwoPeerCachesWithReplicatedRegion() throws IOException {
    Cache cache1 = createCache("cache1");
    Cache cache2 = createCache("cache2");

    Region<String, String> region1 =
        cache1.<String, String>createRegionFactory(RegionShortcut.REPLICATE).create("region");
    Region<String, String> region2 =
        cache2.<String, String>createRegionFactory(RegionShortcut.REPLICATE).create("region");

    assertNotEquals(region1, region2);

    String value = "value";
    region1.put("key", value);

    String region2Value = region2.get("key");
    assertEquals(value, region2Value);
    // Make sure the value was actually serialized/deserialized between the two caches
    assertFalse(value == region2Value);
  }

  private Cache createCache(String memberName) {
    Cache cache = new CacheFactory(props).set(ConfigurationProperties.NAME, memberName).create();
    caches.add(cache);
    return cache;
  }

}
