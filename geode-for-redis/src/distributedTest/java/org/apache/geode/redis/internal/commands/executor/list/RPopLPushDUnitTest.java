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
package org.apache.geode.redis.internal.commands.executor.list;

import static org.apache.geode.redis.internal.RedisConstants.SERVER_ERROR_MESSAGE;
import static org.apache.geode.redis.internal.services.RegionProvider.DEFAULT_REDIS_REGION_NAME;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.redis.internal.data.RedisData;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class RPopLPushDUnitTest {
  public static final String KEY_1 = "key1";
  public static final String KEY_2 = "key2";
  public static final String THROWING_CACHE_WRITER_EXCEPTION = "to be ignored";

  @Rule
  public RedisClusterStartupRule clusterStartUp = new RedisClusterStartupRule();

  @Rule
  public ExecutorServiceRule executor = new ExecutorServiceRule();

  private static JedisCluster jedis;

  @Before
  public void testSetup() {
    MemberVM locator = clusterStartUp.startLocatorVM(0);
    clusterStartUp.startRedisVM(1, locator.getPort());
    clusterStartUp.startRedisVM(2, locator.getPort());
    clusterStartUp.startRedisVM(3, locator.getPort());
    int redisServerPort = clusterStartUp.getRedisPort(1);
    jedis = new JedisCluster(new HostAndPort(BIND_ADDRESS, redisServerPort), 20_000);
    clusterStartUp.flushAll();
  }

  @After
  public void tearDown() {
    jedis.close();
  }

  @Test
  public void shouldDistributeDataAmongCluster_andRetainDataAfterServerCrash() {
    int primaryVMIndex = 1;
    final String tag = "{" + clusterStartUp.getKeyOnServer("tag", primaryVMIndex) + "}";
    final String sourceKey = tag + KEY_1;
    final String destinationKey = tag + KEY_2;

    final int elementsToMove = 5;
    final int initialElementCount = elementsToMove * 2;

    List<String> initialElements = makeInitialElementsList(initialElementCount);

    jedis.lpush(sourceKey, initialElements.toArray(new String[0]));

    // Move half the elements from the source list to the destination
    for (int i = 0; i < elementsToMove; ++i) {
      assertThat(jedis.rpoplpush(sourceKey, destinationKey)).isEqualTo(initialElements.get(i));
    }

    clusterStartUp.crashVM(primaryVMIndex); // kill primary server

    // For easier validation
    List<String> reversedInitialElements = new ArrayList<>(initialElements);
    Collections.reverse(reversedInitialElements);

    assertThat(jedis.lrange(sourceKey, 0, -1))
        .containsExactlyElementsOf(reversedInitialElements.subList(0, elementsToMove));
    assertThat(jedis.lrange(destinationKey, 0, -1)).containsExactlyElementsOf(
        reversedInitialElements.subList(elementsToMove, initialElementCount));
  }

  @Test
  public void givenBucketsMovedDuringRPopLPush_thenOperationsAreNotLostOrDuplicated()
      throws InterruptedException, ExecutionException {
    final AtomicBoolean continueRunning = new AtomicBoolean(true);
    final List<String> hashTags = getHashTagsForEachServer();
    final int initialElementCount = 1000;

    List<String> initialElements = makeInitialElementsList(initialElementCount);

    for (String hashTag : hashTags) {
      jedis.lpush(hashTag + KEY_1, initialElements.toArray(new String[0]));
    }

    Future<Void> future1 = executor.runAsync(() -> repeatRPopLPush(hashTags.get(0),
        initialElements, continueRunning));
    Future<Void> future2 = executor.runAsync(() -> repeatRPopLPush(hashTags.get(1),
        initialElements, continueRunning));
    Future<Void> future3 =
        executor.runAsync(() -> repeatRPopLPushWithSameSourceAndDest(hashTags.get(2),
            initialElements, continueRunning));

    for (int i = 0; i < 25 && continueRunning.get(); i++) {
      clusterStartUp.moveBucketForKey(hashTags.get(i % hashTags.size()));
      Thread.sleep(200);
    }

    continueRunning.set(false);

    future1.get();
    future2.get();
    future3.get();
  }

  @Test
  public void rpoplpush_isTransactional() {
    IgnoredException.addIgnoredException(THROWING_CACHE_WRITER_EXCEPTION);

    int primaryVMIndex = 1;
    final String tag = "{" + clusterStartUp.getKeyOnServer("tag", primaryVMIndex) + "}";

    final String sourceKey = tag + KEY_1;
    int sourceSize = 2;
    List<String> initialElements = makeInitialElementsList(sourceSize);
    jedis.lpush(sourceKey, initialElements.toArray(new String[0]));

    String throwingKey = tag + "ThrowingRedisString";
    int destinationSize = 2;
    List<String> elementsForThrowingKey = makeInitialElementsList(sourceSize);
    jedis.lpush(throwingKey, elementsForThrowingKey.toArray(new String[0]));

    // Install a cache writer that will throw an exception if a key with a name equal to throwingKey
    // is updated or created
    clusterStartUp.getMember(1).invoke(() -> {
      RedisClusterStartupRule.getCache()
          .<RedisKey, RedisData>getRegion(DEFAULT_REDIS_REGION_NAME)
          .getAttributesMutator()
          .setCacheWriter(new RPopLPushDUnitTest.ThrowingCacheWriter(throwingKey));
    });

    assertThatThrownBy(
        () -> jedis.rpoplpush(sourceKey, throwingKey))
            .hasMessage(SERVER_ERROR_MESSAGE);

    List<String> reversedInitialElements = new ArrayList<>(initialElements);
    Collections.reverse(reversedInitialElements);

    List<String> reversedElementsForThrowingKey = new ArrayList<>(elementsForThrowingKey);
    Collections.reverse(reversedElementsForThrowingKey);

    // Assert rpoplpush has not happened
    assertThat(jedis.lrange(sourceKey, 0, -1)).containsExactlyElementsOf(reversedInitialElements);
    assertThat(jedis.lrange(throwingKey, 0, -1))
        .containsExactlyElementsOf(reversedElementsForThrowingKey);

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

  private List<String> getHashTagsForEachServer() {
    List<String> hashTags = new ArrayList<>();
    hashTags.add("{" + clusterStartUp.getKeyOnServer("tag", 1) + "}");
    hashTags.add("{" + clusterStartUp.getKeyOnServer("tag", 2) + "}");
    hashTags.add("{" + clusterStartUp.getKeyOnServer("tag", 3) + "}");
    return hashTags;
  }

  private List<String> makeInitialElementsList(int size) {
    return IntStream.range(0, size)
        .mapToObj(String::valueOf)
        .collect(Collectors.toList());
  }

  private void repeatRPopLPush(String hashTag, List<String> initialElements,
      AtomicBoolean continueRunning) {
    String source = hashTag + KEY_1;
    String destination = hashTag + KEY_2;

    // For easier validation
    List<String> reversedInitialElements = new ArrayList<>(initialElements);
    Collections.reverse(reversedInitialElements);

    while (continueRunning.get()) {
      for (int i = 0; i < initialElements.size(); i++) {
        assertThat(jedis.rpoplpush(source, destination)).isEqualTo(initialElements.get(i));

        int movedIndex = (reversedInitialElements.size() - 1) - i;
        // Confirm we moved the correct element
        assertThat(jedis.lrange(destination, 0, -1)).containsExactlyElementsOf(
            reversedInitialElements.subList(movedIndex, reversedInitialElements.size()));
        assertThat(jedis.lrange(source, 0, -1)).containsExactlyElementsOf(
            reversedInitialElements.subList(0, movedIndex));
      }

      // All elements have been moved
      assertThat(jedis.exists(source)).isFalse();

      // Swap the source and destination keys
      String tmp = source;
      source = destination;
      destination = tmp;
    }
  }

  private void repeatRPopLPushWithSameSourceAndDest(String hashTag, List<String> initialElements,
      AtomicBoolean continueRunning) {
    String key = hashTag + KEY_1;

    // For easier validation
    List<String> expectedElements = new ArrayList<>(initialElements);
    Collections.reverse(expectedElements);

    while (continueRunning.get()) {
      for (String element : initialElements) {
        assertThat(jedis.rpoplpush(key, key)).isEqualTo(element);
        Collections.rotate(expectedElements, 1);
        assertThat(jedis.lrange(key, 0, -1)).containsExactlyElementsOf(expectedElements);
      }
    }
  }
}
