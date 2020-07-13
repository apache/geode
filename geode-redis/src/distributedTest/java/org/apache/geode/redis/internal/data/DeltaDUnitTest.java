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

package org.apache.geode.redis.internal.data;

import static org.apache.geode.distributed.ConfigurationProperties.MAX_WAIT_TIME_RECONNECT;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.util.BlobHelper;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;

@SuppressWarnings("unchecked")
public class DeltaDUnitTest {

  @ClassRule
  public static RedisClusterStartupRule clusterStartUp = new RedisClusterStartupRule(4);

  private static final String LOCAL_HOST = "127.0.0.1";
  private static final int ITERATION_COUNT = 1000;
  private static final int JEDIS_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());
  private static Jedis jedis1;
  private static Jedis jedis2;

  private static Properties locatorProperties;

  private static MemberVM locator;
  private static MemberVM server1;
  private static MemberVM server2;

  private static int redisServerPort1;
  private static int redisServerPort2;

  @BeforeClass
  public static void classSetup() {
    locatorProperties = new Properties();
    locatorProperties.setProperty(MAX_WAIT_TIME_RECONNECT, "15000");

    locator = clusterStartUp.startLocatorVM(0, locatorProperties);
    server1 = clusterStartUp.startRedisVM(1, locator.getPort());
    server2 = clusterStartUp.startRedisVM(2, locator.getPort());

    redisServerPort1 = clusterStartUp.getRedisPort(1);
    redisServerPort2 = clusterStartUp.getRedisPort(2);

    jedis1 = new Jedis(LOCAL_HOST, redisServerPort1, JEDIS_TIMEOUT);
    jedis2 = new Jedis(LOCAL_HOST, redisServerPort2, JEDIS_TIMEOUT);
  }

  @Before
  public void testSetup() {
    jedis1.flushAll();
  }

  @AfterClass
  public static void tearDown() {
    jedis1.disconnect();
    jedis2.disconnect();

    server1.stop();
    server2.stop();
  }

  @Test
  public void shouldCorrectlyPropagateDeltaToSecondaryServer_whenAppending() {
    String key = "key";
    String baseValue = "value-";
    jedis1.set(key, baseValue);
    for (int i = 0; i < ITERATION_COUNT; i++) {
      jedis1.append(key, String.valueOf(i));

      byte[] server1LocalValue = server1.invoke(() -> (byte[]) getLocalData(key, r -> {
        RedisData localValue = r.get(new ByteArrayWrapper(key.getBytes()));

        try {
          return BlobHelper.serializeToBlob(localValue);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }));

      byte[] server2LocalValue = server2.invoke(() -> (byte[]) getLocalData(key, r -> {
        RedisData localValue = r.get(new ByteArrayWrapper(key.getBytes()));

        try {
          return BlobHelper.serializeToBlob(localValue);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }));

      assertThat(Arrays.equals(server1LocalValue, server2LocalValue));
    }
  }

  @Test
  public void shouldCorrectlyPropagateDeltaToSecondaryServer_whenAddingToSet() {
    String key = "key";

    List<String> members = makeMemberList(ITERATION_COUNT, "member-");
    for (String member : members) {
      jedis1.sadd(key, member);
      Set<ByteArrayWrapper> server1LocalSet =
          server1.invoke(() -> (Set<ByteArrayWrapper>) getLocalData(key, r -> {
            RedisSet localSet = (RedisSet) r.get(new ByteArrayWrapper(key.getBytes()));
            if (localSet == null) {
              return null;
            }
            return localSet.smembers();
          }));

      Set<ByteArrayWrapper> server2LocalSet =
          server2.invoke(() -> (Set<ByteArrayWrapper>) getLocalData(key, r -> {
            RedisSet localSet = (RedisSet) r.get(new ByteArrayWrapper(key.getBytes()));
            if (localSet == null) {
              return null;
            }
            return localSet.smembers();
          }));

      assertThat(server1LocalSet).containsExactlyInAnyOrder(server2LocalSet.toArray(
          new ByteArrayWrapper[] {}));
    }
  }

  @Test
  public void shouldCorrectlyPropagateDeltaToSecondaryServer_whenRemovingFromSet() {
    String key = "key";

    List<String> members = makeMemberList(ITERATION_COUNT, "member-");
    jedis1.sadd(key, members.toArray(new String[] {}));

    for (String member : members) {
      jedis1.srem(key, member);
      Set<ByteArrayWrapper> server1LocalSet =
          server1.invoke(() -> (Set<ByteArrayWrapper>) getLocalData(key, r -> {
            RedisSet localSet = (RedisSet) r.get(new ByteArrayWrapper(key.getBytes()));
            if (localSet == null) {
              return null;
            }
            return localSet.smembers();
          }));

      Set<ByteArrayWrapper> server2LocalSet =
          server2.invoke(() -> (Set<ByteArrayWrapper>) getLocalData(key, r -> {
            RedisSet localSet = (RedisSet) r.get(new ByteArrayWrapper(key.getBytes()));
            if (localSet == null) {
              return null;
            }
            return localSet.smembers();
          }));

      if (server1LocalSet == null || server2LocalSet == null) {
        assertThat(server1LocalSet).isEqualTo(server2LocalSet);
      } else {
        assertThat(server1LocalSet).containsExactlyInAnyOrder(server2LocalSet.toArray(
            new ByteArrayWrapper[] {}));
      }
    }
  }

  @Test
  public void shouldCorrectlyPropagateDeltaToSecondaryServer_whenAddingToHash() {
    String key = "key";

    Map<String, String> testMap = makeHashMap(ITERATION_COUNT, "field-", "value-");

    for (String field : testMap.keySet()) {
      jedis1.hset(key, field, testMap.get(field));

      Collection<ByteArrayWrapper> server1LocalHash =
          server1.invoke(() -> (Collection<ByteArrayWrapper>) getLocalData(key, r -> {
            RedisHash localSet = (RedisHash) r.get(new ByteArrayWrapper(key.getBytes()));
            return localSet.hgetall();
          }));

      Collection<ByteArrayWrapper> server2LocalHash =
          server2.invoke(() -> (Collection<ByteArrayWrapper>) getLocalData(key, r -> {
            RedisHash localSet = (RedisHash) r.get(new ByteArrayWrapper(key.getBytes()));
            return localSet.hgetall();
          }));

      assertThat(server1LocalHash).containsExactlyInAnyOrder(server2LocalHash.toArray(
          new ByteArrayWrapper[] {}));
    }
  }

  @Test
  public void shouldCorrectlyPropagateDeltaToSecondaryServer_whenRemovingFromHash() {
    String key = "key";

    Map<String, String> testMap = makeHashMap(ITERATION_COUNT, "field-", "value-");
    jedis1.hset(key, testMap);

    for (String field : testMap.keySet()) {
      jedis1.hdel(key, field, testMap.get(field));

      Collection<ByteArrayWrapper> server1LocalHash =
          server1.invoke(() -> (Collection<ByteArrayWrapper>) getLocalData(key, r -> {
            RedisHash localSet = (RedisHash) r.get(new ByteArrayWrapper(key.getBytes()));
            if (localSet == null) {
              return null;
            }
            return localSet.hgetall();
          }));

      Collection<ByteArrayWrapper> server2LocalHash =
          server2.invoke(() -> (Collection<ByteArrayWrapper>) getLocalData(key, r -> {
            RedisHash localSet = (RedisHash) r.get(new ByteArrayWrapper(key.getBytes()));
            if (localSet == null) {
              return null;
            }
            return localSet.hgetall();
          }));

      if (server1LocalHash == null || server2LocalHash == null) {
        assertThat(server1LocalHash).isEqualTo(server2LocalHash);
      } else {
        assertThat(server1LocalHash).containsExactlyInAnyOrder(server2LocalHash.toArray(
            new ByteArrayWrapper[] {}));
      }
    }
  }

  @Test
  public void shouldCorrectlyPropagateDeltaToSecondaryServer_whenExpiring() {
    String baseKey = "key-";

    for (int i = 0; i < ITERATION_COUNT; i++) {
      String key = baseKey + i;
      jedis1.set(key, "value");
      jedis1.expire(key, 20);
      long server1LocalExpirtionTimestamp = server1.invoke(() -> (long) getLocalData(key, r -> {
        RedisData localSet = r.get(new ByteArrayWrapper(key.getBytes()));
        if (localSet == null) {
          return null;
        }
        return localSet.getExpirationTimestamp();
      }));

      long server2LocalExpirationTimestamp = server2.invoke(() -> (long) getLocalData(key, r -> {
        RedisData localSet = r.get(new ByteArrayWrapper(key.getBytes()));
        if (localSet == null) {
          return null;
        }
        return localSet.getExpirationTimestamp();
      }));

      assertThat(server1LocalExpirtionTimestamp).isEqualTo(server2LocalExpirationTimestamp);
    }
  }

  private static Object getLocalData(String key,
      Function<Region<ByteArrayWrapper, RedisData>, Object> func) {
    InternalCache cache = ClusterStartupRule.getCache();
    Region<ByteArrayWrapper, RedisData> region = cache.getRegion("__REDIS_DATA");
    Region<ByteArrayWrapper, RedisData> localRegion =
        PartitionRegionHelper.getLocalData(region);

    return func.apply(localRegion);
  }

  private Map<String, String> makeHashMap(int hashSize, String baseFieldName,
      String baseValueName) {
    Map<String, String> map = new HashMap<>();
    for (int i = 0; i < hashSize; i++) {
      map.put(baseFieldName + i, baseValueName + i);
    }
    return map;
  }

  private List<String> makeMemberList(int setSize, String baseString) {
    List<String> members = new ArrayList<>();
    for (int i = 0; i < setSize; i++) {
      members.add(baseString + i);
    }
    return members;
  }
}
