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
package org.apache.geode.internal.cache.map;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.OFF_HEAP_MEMORY_SIZE;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Properties;

import junitparams.Parameters;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

/**
 * TestCase that emulates the conditions that entry destroy with concurrent destroy region or cache
 * close event will get expected Exception.
 */
@RunWith(GeodeParamsRunner.class)
public class DestroyEntryDuringCloseIntegrationTest {

  private static final String KEY = "KEY";
  private static final String VALUE = "value";
  public static final String REGION = "region1";

  private Cache cache;

  @Before
  public void setUp() throws Exception {
    Properties props = new Properties();
    props.setProperty(LOCATORS, "");
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(OFF_HEAP_MEMORY_SIZE, "2m");
    cache = new CacheFactory(props).create();
  }

  @After
  public void tearDown() throws Exception {
    RegionMapDestroy.testHookRunnableForConcurrentOperation = null;
    cache.close();
  }

  private Region<Object, Object> createRegion(boolean isOffHeap) {
    return cache.createRegionFactory(RegionShortcut.REPLICATE).setOffHeap(isOffHeap)
        .setConcurrencyChecksEnabled(true).create(REGION);
  }

  @Test
  @Parameters({"true", "false"})
  public void testEntryDestroyWithCacheClose(boolean offheap) throws Exception {
    RegionMapDestroy.testHookRunnableForConcurrentOperation = () -> cache.close();

    Region<Object, Object> region = createRegion(offheap);
    region.put(KEY, VALUE);

    assertThatThrownBy(() -> region.destroy(KEY)).isInstanceOf(CacheClosedException.class);
  }

  @Test
  @Parameters({"true", "false"})
  public void testEntryDestroyWithRegionDestroy(boolean offheap) throws Exception {
    RegionMapDestroy.testHookRunnableForConcurrentOperation =
        () -> cache.getRegion(REGION).destroyRegion();

    Region<Object, Object> region = createRegion(offheap);
    region.put(KEY, VALUE);

    assertThatThrownBy(() -> region.destroy(KEY)).isInstanceOf(RegionDestroyedException.class);
  }

}
