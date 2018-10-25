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
package org.apache.geode.internal.cache;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.CoreMatchers.is;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.util.CacheListenerAdapter;

/**
 * AFTER_REGION_CREATE was being sent before region initialization (bug 33726). Test to verify that
 * that is no longer the case.
 *
 * <p>
 * TRAC #33726: afterRegionCreate event delivered before region initialization occurs
 */
public class AfterRegionCreateNotBeforeRegionInitRegressionTest {

  private Cache cache;
  private TestCacheListener cacheListener;

  @Rule
  public ErrorCollector errorCollector = new ErrorCollector();

  @Before
  public void setUp() {
    cache = new CacheFactory().set(LOCATORS, "").set(MCAST_PORT, "0").create();
    cacheListener = new TestCacheListener();
  }

  @After
  public void tearDown() {
    cache.close();
  }

  @Test
  public void testAfterRegionCreate() throws Exception {
    AttributesFactory factory = new AttributesFactory();
    factory.setCacheListener(cacheListener);

    Region region = cache.createRegion("testRegion", factory.create());
    region.createSubregion("testSubRegion", factory.create());

    await()
        .untilAsserted(() -> assertThat(cacheListener.afterRegionCreateCount.get()).isEqualTo(2));
  }

  private class TestCacheListener extends CacheListenerAdapter {

    final AtomicInteger afterRegionCreateCount = new AtomicInteger();

    @Override
    public void afterRegionCreate(RegionEvent event) {
      InternalRegion region = (InternalRegion) event.getRegion();
      String regionPath = event.getRegion().getFullPath();
      if (regionPath.contains("/testRegion/testSubRegion") || regionPath.contains("/testRegion")) {
        afterRegionCreateCount.incrementAndGet();
        errorCollector.checkThat(region.isInitialized(), is(true));
      }
    }
  }
}
