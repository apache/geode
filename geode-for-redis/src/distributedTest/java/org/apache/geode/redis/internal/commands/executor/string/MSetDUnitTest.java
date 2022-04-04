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

package org.apache.geode.redis.internal.commands.executor.string;

import static org.apache.geode.redis.internal.SystemPropertyBasedRedisConfiguration.GEODE_FOR_REDIS_PORT;
import static org.apache.geode.redis.internal.RedisConstants.SERVER_ERROR_MESSAGE;
import static org.apache.geode.redis.internal.services.RegionProvider.DEFAULT_REDIS_REGION_NAME;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.control.RebalanceFactory;
import org.apache.geode.cache.control.ResourceManager;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.internal.data.RedisData;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class MSetDUnitTest {

  private static final Logger logger = LogService.getLogger();

  @ClassRule
  public static RedisClusterStartupRule clusterStartUp = new RedisClusterStartupRule(4);

  @ClassRule
  public static ExecutorServiceRule executor = new ExecutorServiceRule();

  public static final String THROWING_CACHE_WRITER_EXCEPTION = "to be ignored";

  private static final String HASHTAG = "{tag}";
  private static JedisCluster jedis;
  private static MemberVM server1;
  private static int locatorPort;
  private static int server3Port;

  @BeforeClass
  public static void classSetup() {
    MemberVM locator = clusterStartUp.startLocatorVM(0);
    locatorPort = locator.getPort();
    server1 = clusterStartUp.startRedisVM(1, locatorPort);
    clusterStartUp.startRedisVM(2, locatorPort);

    server3Port = AvailablePortHelper.getRandomAvailableTCPPort();
    String finalRedisPort = Integer.toString(server3Port);
    int finalLocatorPort = locatorPort;
    clusterStartUp.startRedisVM(3, x -> x
        .withSystemProperty(GEODE_FOR_REDIS_PORT, finalRedisPort)
        .withConnectionToLocator(finalLocatorPort));

    clusterStartUp.enableDebugLogging(1);
    clusterStartUp.enableDebugLogging(2);
    clusterStartUp.enableDebugLogging(3);

    int redisServerPort1 = clusterStartUp.getRedisPort(1);
    jedis = new JedisCluster(new HostAndPort(BIND_ADDRESS, redisServerPort1), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void after() {
    clusterStartUp.flushAll();
  }

  @AfterClass
  public static void tearDown() {
    jedis.close();
  }

  @Test
  public void testMSet_concurrentInstancesHandleBucketMovement() {
    int KEY_COUNT = 5000;
    String[] keys = new String[KEY_COUNT];

    for (int i = 0; i < keys.length; i++) {
      keys[i] = HASHTAG + "key" + i;
    }
    String[] keysAndValues1 = makeKeysAndValues(keys, "valueOne");
    String[] keysAndValues2 = makeKeysAndValues(keys, "valueTwo");

    new ConcurrentLoopingThreads(100,
        i -> jedis.mset(keysAndValues1),
        i -> jedis.mset(keysAndValues2),
        i -> clusterStartUp.moveBucketForKey(keys[0]))
            .runWithAction(() -> assertThat(jedis.mget(keys)).satisfiesAnyOf(
                values -> assertThat(values)
                    .allSatisfy(value -> assertThat(value).startsWith("valueOne")),
                values -> assertThat(values)
                    .allSatisfy(value -> assertThat(value).startsWith("valueTwo"))));
  }

  @Test
  public void testMSet_crashDoesNotLeaveInconsistencies() throws Exception {
    int KEY_COUNT = 1000;
    String[] keys = new String[KEY_COUNT];

    for (int i = 0; i < keys.length; i++) {
      keys[i] = HASHTAG + "key" + i;
    }
    String[] keysAndValues1 = makeKeysAndValues(keys, "valueOne");
    String[] keysAndValues2 = makeKeysAndValues(keys, "valueTwo");
    AtomicBoolean running = new AtomicBoolean(true);

    String finalRedisPort = Integer.toString(server3Port);
    int finalLocatorPort = locatorPort;
    Future<?> future = executor.submit(() -> {
      for (int i = 0; i < 20 && running.get(); i++) {
        clusterStartUp.moveBucketForKey(keys[0], "server-3");
        // Sleep for a bit so that MSETs can execute
        Thread.sleep(2000);
        clusterStartUp.crashVM(3);
        clusterStartUp.startRedisVM(3, x -> x
            .withSystemProperty(GEODE_FOR_REDIS_PORT, finalRedisPort)
            .withConnectionToLocator(finalLocatorPort));
        clusterStartUp.enableDebugLogging(3);
        rebalanceAllRegions(server1);
      }
      running.set(false);
    });

    try {
      AtomicInteger loopCounter = new AtomicInteger(0);
      new ConcurrentLoopingThreads(running,
          i -> jedis.mset(keysAndValues1),
          i -> jedis.mset(keysAndValues2))
              .runWithAction(() -> {
                logger.info("--->>> Validation STARTING: Loop count = {}  {}",
                    loopCounter.incrementAndGet(), running.get());
                int count = 0;
                List<String> values = jedis.mget(keys);
                for (String v : values) {
                  if (v == null) {
                    continue;
                  }
                  count += v.startsWith("valueOne") ? 1 : -1;
                }
                assertThat(Math.abs(count)).isEqualTo(KEY_COUNT);
                logger.info("--->>> Validation DONE");
              });
    } finally {
      running.set(false);
      future.get();
    }
  }

  @Test
  public void mset_isTransactional() {
    IgnoredException.addIgnoredException(THROWING_CACHE_WRITER_EXCEPTION);
    String hashTag = "{" + clusterStartUp.getKeyOnServer("tag", 1) + "}";

    String key1 = hashTag + "key1";
    String value1 = "value1";
    jedis.set(key1, value1);

    String listKey = hashTag + "listKey";
    jedis.lpush(listKey, "1", "2", "3");

    String nonExistent = hashTag + "nonExistentKey";

    String throwingKey = hashTag + "ThrowingRedisString";
    String throwingKeyValue = "ThrowingRedisStringValue";

    jedis.set(throwingKey, throwingKeyValue);

    // Install a cache writer that will throw an exception if a key with a name equal to throwingKey
    // is updated or created
    clusterStartUp.getMember(1).invoke(() -> {
      RedisClusterStartupRule.getCache()
          .<RedisKey, RedisData>getRegion(DEFAULT_REDIS_REGION_NAME)
          .getAttributesMutator()
          .setCacheWriter(new ThrowingCacheWriter(throwingKey));
    });

    String newValue = "should_not_be_set";

    assertThatThrownBy(
        () -> jedis.mset(key1, newValue, nonExistent, newValue, throwingKey, newValue))
            .hasMessage(SERVER_ERROR_MESSAGE);

    assertThat(jedis.get(key1)).isEqualTo(value1);
    assertThat(jedis.type(listKey)).isEqualTo("list");
    assertThat(jedis.exists(nonExistent)).isFalse();
    assertThat(jedis.get(throwingKey)).isEqualTo(throwingKeyValue);

    IgnoredException.removeAllExpectedExceptions();
  }

  private static class ThrowingCacheWriter implements CacheWriter<RedisKey, RedisData> {
    private final byte[] keyBytes;

    ThrowingCacheWriter(String key) {
      keyBytes = key.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void beforeUpdate(EntryEvent<RedisKey, RedisData> event) throws CacheWriterException {
      if (Arrays.equals(event.getKey().toBytes(), keyBytes)) {
        throw new CacheWriterException(THROWING_CACHE_WRITER_EXCEPTION);
      }
    }

    @Override
    public void beforeCreate(EntryEvent<RedisKey, RedisData> event) throws CacheWriterException {
      if (Arrays.equals(event.getKey().toBytes(), keyBytes)) {
        throw new CacheWriterException(THROWING_CACHE_WRITER_EXCEPTION);
      }
    }

    @Override
    public void beforeDestroy(EntryEvent<RedisKey, RedisData> event) throws CacheWriterException {

    }

    @Override
    public void beforeRegionDestroy(RegionEvent<RedisKey, RedisData> event)
        throws CacheWriterException {

    }

    @Override
    public void beforeRegionClear(RegionEvent<RedisKey, RedisData> event)
        throws CacheWriterException {}
  }

  private String[] makeKeysAndValues(String[] keys, String valueBase) {
    String[] keysValues = new String[keys.length * 2];
    for (int i = 0; i < keys.length * 2; i += 2) {
      keysValues[i] = keys[i / 2];
      keysValues[i + 1] = valueBase + i;
    }

    return keysValues;
  }

  private static void rebalanceAllRegions(MemberVM vm) {
    vm.invoke("Running rebalance", () -> {
      ResourceManager manager = ClusterStartupRule.getCache().getResourceManager();
      RebalanceFactory factory = manager.createRebalanceFactory();
      try {
        factory.start().getResults();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });
  }

}
