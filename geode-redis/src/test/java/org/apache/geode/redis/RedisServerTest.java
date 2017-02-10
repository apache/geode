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
package org.apache.geode.redis;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;


@Category(IntegrationTest.class)
public class RedisServerTest extends RedisTestBase {
  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Test
  public void initializeRedisCreatesThreeRegions() {
    Assert.assertEquals(2, cache.rootRegions().size());
    Assert.assertTrue(cache.getRegion(GeodeRedisServiceImpl.REDIS_META_DATA_REGION) != null);
  }

  @Test
  public void initializeRedisCreatesPartitionedRegionByDefault() {
    Region redisStringRegion = cache.getRegion(GeodeRedisServiceImpl.STRING_REGION);
    Assert.assertEquals(DataPolicy.PARTITION, redisStringRegion.getAttributes().getDataPolicy());
  }

  @Test
  public void initializeRedisCreatesRegionsUsingSystemProperty() {
    shutdownCache();
    System.setProperty("gemfireredis.regiontype", "REPLICATE");
    cache = (GemFireCacheImpl) createCacheInstance(getDefaultRedisCacheProperties());
    Region redisStringRegion = cache.getRegion(GeodeRedisServiceImpl.STRING_REGION);
    Assert.assertEquals(DataPolicy.REPLICATE, redisStringRegion.getAttributes().getDataPolicy());
  }

}
