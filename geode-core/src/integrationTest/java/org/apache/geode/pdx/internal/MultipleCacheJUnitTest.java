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

import static org.apache.geode.cache.RegionShortcut.REPLICATE;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.distributed.internal.InternalDistributedSystem.ALLOW_MULTIPLE_SYSTEMS_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.InternalDistributedSystem.ConnectListener;
import org.apache.geode.test.junit.categories.ClientServerTest;

/**
 * Test that we can create multiple caches in the same JVM
 */
@Category(ClientServerTest.class)
public class MultipleCacheJUnitTest {

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Rule
  public TemporaryFolder locatorFolder = new TemporaryFolder();

  private final List<Cache> caches = new ArrayList<>();
  private Properties configProperties;
  private Locator locator;
  private ConnectListener connectListener;

  @Before
  public void startLocator() throws IOException {
    System.setProperty(ALLOW_MULTIPLE_SYSTEMS_PROPERTY, "true");

    connectListener = mock(ConnectListener.class);
    InternalDistributedSystem.addConnectListener(connectListener);

    locator = Locator.startLocatorAndDS(0, locatorFolder.newFile("locator.log"), null);
    configProperties = new Properties();
    configProperties.setProperty(LOCATORS, "localhost[" + locator.getPort() + "]");
  }

  @After
  public void closeSystemsAndLocator() {
    InternalDistributedSystem.removeConnectListener(connectListener);
    caches.forEach(Cache::close);
    locator.stop();
  }

  @Test
  public void locatorHasOneDistributedSystem() {
    verify(connectListener, times(1)).onConnect(any());
  }

  @Test
  public void cacheCreateTwoPeerCaches() {
    Cache cache1 = createCache("cache1");
    Cache cache2 = createCache("cache2");

    assertThat(cache2).isNotSameAs(cache1);

    DistributedMember member1 = cache1.getDistributedSystem().getDistributedMember();
    DistributedMember member2 = cache2.getDistributedSystem().getDistributedMember();

    assertThat(member2).isNotEqualTo(member1);
    assertThat(cache2.getDistributedSystem().getAllOtherMembers()).contains(member1);
    assertThat(cache1.getDistributedSystem().getAllOtherMembers()).contains(member2);
  }


  @Test
  public void canCreateTwoPeerCachesWithReplicatedRegion() {
    Cache cache1 = createCache("cache1");
    Cache cache2 = createCache("cache2");

    Region<String, String> region1 =
        cache1.<String, String>createRegionFactory(REPLICATE).create("region");
    Region<String, String> region2 =
        cache2.<String, String>createRegionFactory(REPLICATE).create("region");

    assertThat(region2).isNotSameAs(region1);

    String value = "value";
    region1.put("key", value);

    String region2Value = region2.get("key");
    assertThat(value).isEqualTo(region2Value);

    // Make sure the value was actually serialized/deserialized between the two caches
    assertThat(value).isNotSameAs(region2Value);
  }

  private Cache createCache(String memberName) {
    Cache cache = new CacheFactory(configProperties)
        .set(NAME, memberName)
        .create();
    caches.add(cache);
    return cache;
  }
}
