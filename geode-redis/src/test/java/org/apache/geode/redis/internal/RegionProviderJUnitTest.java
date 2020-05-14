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
 *
 */
package org.apache.geode.redis.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.internal.hll.HyperLogLogPlus;


/**
 * Test cases for the RegionProvider object
 *
 *
 */
public class RegionProviderJUnitTest {

  private String NEW_REGION_NM = "NEW_REGION";

  private RegionProvider regionProvider;

  private ExecutionHandlerContext context;

  private Region<ByteArrayWrapper, Map<ByteArrayWrapper, ByteArrayWrapper>> hashRegion;
  private Region<ByteArrayWrapper, Set<ByteArrayWrapper>> setRegion;

  /**
   * Setup data, objects mocks for the test case
   */
  @SuppressWarnings("unchecked")
  @Before
  public void setUp() {
    Region<ByteArrayWrapper, ByteArrayWrapper> stringsRegion = Mockito.mock(Region.class);

    Region<ByteArrayWrapper, HyperLogLogPlus> hLLRegion = Mockito.mock(Region.class);
    KeyRegistrar keyRegistrar = Mockito.mock(KeyRegistrar.class);

    ConcurrentMap<ByteArrayWrapper, ScheduledFuture<?>> expirationsMap =
        Mockito.mock(ConcurrentMap.class);
    ScheduledExecutorService expirationExecutor = Mockito.mock(ScheduledExecutorService.class);
    Cache cache = Mockito.mock(Cache.class);
    Region<Object, Object> newRegion = org.mockito.Mockito.mock(Region.class);

    setRegion = Mockito.mock(Region.class);

    Mockito.when(cache.getRegion(NEW_REGION_NM)).thenReturn(newRegion);

    RegionShortcut defaultShortcut = RegionShortcut.PARTITION;

    hashRegion = Mockito.mock(Region.class);

    regionProvider = new RegionProvider(stringsRegion, hLLRegion, keyRegistrar, expirationsMap,
        expirationExecutor, defaultShortcut, hashRegion, setRegion, cache);
    context = Mockito.mock(ExecutionHandlerContext.class);

  }

  /**
   * Test the RegionProvide.getRegion
   */
  @Test
  public void testGetRegion() {
    assertThat(regionProvider.getRegion(null)).isNull();

    assertThat(regionProvider.getRegion(Coder.stringToByteArrayWrapper("invalid"))).isNull();
  }

  /**
   * Test RegionProvider get or create region method
   */
  @Test
  public void testGetOrCreateRegion() {
    Region<?, ?> region = regionProvider.getOrCreateRegion(
        Coder.stringToByteArrayWrapper(NEW_REGION_NM), RedisDataType.REDIS_HASH, context);
    assertThat(region).isNotNull();

    Region<?, ?> sameregion =
        regionProvider.getRegion(Coder.stringToByteArrayWrapper(NEW_REGION_NM));
    assertThat(region).isNotNull();

    assertThat(sameregion).isSameAs(region);
  }
}
