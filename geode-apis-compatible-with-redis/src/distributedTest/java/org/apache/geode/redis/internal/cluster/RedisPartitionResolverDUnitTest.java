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

package org.apache.geode.redis.internal.cluster;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.internal.cache.LocalDataSet;
import org.apache.geode.redis.internal.RegionProvider;
import org.apache.geode.redis.internal.data.RedisData;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.SerializableCallableIF;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;

public class RedisPartitionResolverDUnitTest {

  @ClassRule
  public static RedisClusterStartupRule cluster = new RedisClusterStartupRule(4);

  private static final String LOCAL_HOST = "127.0.0.1";
  private static final int JEDIS_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());
  private static Jedis jedis1;

  private static MemberVM server1;
  private static MemberVM server2;
  private static MemberVM server3;
  private static int redisServerPort1;


  @BeforeClass
  public static void classSetup() {
    MemberVM locator = cluster.startLocatorVM(0);
    server1 = cluster.startRedisVM(1, locator.getPort());
    server2 = cluster.startRedisVM(2, locator.getPort());
    server3 = cluster.startRedisVM(3, locator.getPort());

    redisServerPort1 = cluster.getRedisPort(1);
    jedis1 = new Jedis(LOCAL_HOST, redisServerPort1, JEDIS_TIMEOUT);
  }

  @Before
  public void testSetup() {
    cluster.flushAll(redisServerPort1);
  }

  @Test
  public void testRedisHashesMapToCorrectBuckets() {
    int numKeys = 1000;
    for (int i = 0; i < numKeys; i++) {
      String key = "key-" + i;
      jedis1.set(key, "value-" + i);
    }

    Map<String, Integer> keyToBucketMap1 = getKeyToBucketMap(server1);
    Map<String, Integer> keyToBucketMap2 = getKeyToBucketMap(server2);
    Map<String, Integer> keyToBucketMap3 = getKeyToBucketMap(server3);

    Set<Integer> buckets1 = new HashSet<>(keyToBucketMap1.values());
    Set<Integer> buckets2 = new HashSet<>(keyToBucketMap2.values());
    Set<Integer> buckets3 = new HashSet<>(keyToBucketMap3.values());

    assertThat(buckets1).doesNotContainAnyElementsOf(buckets2);
    assertThat(buckets1).doesNotContainAnyElementsOf(buckets3);
    assertThat(buckets2).doesNotContainAnyElementsOf(buckets3);

    assertThat(buckets1.size() + buckets2.size() + buckets3.size())
        .isEqualTo(RegionProvider.REDIS_REGION_BUCKETS);
  }

  private Map<String, Integer> getKeyToBucketMap(MemberVM vm) {
    return vm.invoke((SerializableCallableIF<Map<String, Integer>>) () -> {
      Region<RedisKey, RedisData> region =
          RedisClusterStartupRule.getCache().getRegion(RegionProvider.REDIS_DATA_REGION);

      LocalDataSet local = (LocalDataSet) PartitionRegionHelper.getLocalPrimaryData(region);
      Map<String, Integer> keyMap = new HashMap<>();

      for (Object key : local.localKeys()) {
        int id = local.getProxy().getKeyInfo(key).getBucketId();
        keyMap.put(key.toString(), id);
      }

      return keyMap;
    });
  }
}
