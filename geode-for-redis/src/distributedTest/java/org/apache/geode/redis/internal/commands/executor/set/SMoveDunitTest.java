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
package org.apache.geode.redis.internal.commands.executor.set;

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

import org.junit.AfterClass;
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

public class SMoveDunitTest {

  public static final String THROWING_CACHE_WRITER_EXCEPTION = "to be ignored";

  @Rule
  public RedisClusterStartupRule clusterStartUp = new RedisClusterStartupRule();

  @Rule
  public ExecutorServiceRule executor = new ExecutorServiceRule();

  public static final String KEY_1 = "key1";
  public static final String KEY_2 = "key2";
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

  @AfterClass
  public static void tearDown() {
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

    List<String> members = makeMemberList(initialElementCount, "member1-");

    jedis.sadd(sourceKey, members.toArray(new String[] {}));

    // Move half the elements from the source set to the destination
    for (int i = 0; i < elementsToMove; ++i) {
      assertThat(jedis.smove(sourceKey, destinationKey, members.get(i))).isEqualTo(1);
    }

    clusterStartUp.crashVM(primaryVMIndex); // kill primary server

    assertThat(jedis.smembers(sourceKey))
        .containsExactlyInAnyOrderElementsOf(members.subList(elementsToMove, initialElementCount));
    assertThat(jedis.smembers(destinationKey))
        .containsExactlyInAnyOrderElementsOf(members.subList(0, elementsToMove));
  }

  @Test
  public void givenBucketsMovedDuringSMove_thenOperationsAreNotLostOrDuplicated()
      throws InterruptedException, ExecutionException {

    final AtomicBoolean continueRunning = new AtomicBoolean(true);
    final List<String> hashTags = getHashTagsForEachServer();

    List<String> members1 = makeMemberList(10, "member1-");
    List<String> members2 = makeMemberList(10, "member2-");
    List<String> members3 = makeMemberList(10, "member3-");

    jedis.sadd(hashTags.get(0) + KEY_1, members1.toArray(new String[] {}));
    jedis.sadd(hashTags.get(1) + KEY_1, members2.toArray(new String[] {}));
    jedis.sadd(hashTags.get(2) + KEY_1, members3.toArray(new String[] {}));

    Future<Void> future1 = executor.runAsync(() -> repeatSMove(hashTags.get(0),
        members1, continueRunning));
    Future<Void> future2 = executor.runAsync(() -> repeatSMove(hashTags.get(1),
        members2, continueRunning));
    Future<Void> future3 =
        executor.runAsync(() -> repeatSMoveWithSameSourceAndDest(hashTags.get(2),
            members3, continueRunning));

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
  public void smove_isTransactional() {
    IgnoredException.addIgnoredException(THROWING_CACHE_WRITER_EXCEPTION);

    int primaryVMIndex = 1;
    final String tag = "{" + clusterStartUp.getKeyOnServer("tag", primaryVMIndex) + "}";

    final String sourceKey = tag + KEY_1;
    int sourceSize = 2;
    List<String> members = makeMemberList(sourceSize, "member1-");
    jedis.sadd(sourceKey, members.toArray(new String[] {}));

    String throwingKey = tag + "ThrowingRedisString";
    int destinationSize = 2;
    List<String> membersForThrowingKey = makeMemberList(2, "ThrowingRedisStringValue");
    jedis.sadd(throwingKey, membersForThrowingKey.toArray(new String[] {}));

    // Install a cache writer that will throw an exception if a key with a name equal to throwingKey
    // is updated or created
    clusterStartUp.getMember(1).invoke(() -> {
      RedisClusterStartupRule.getCache()
          .<RedisKey, RedisData>getRegion(DEFAULT_REDIS_REGION_NAME)
          .getAttributesMutator()
          .setCacheWriter(new SMoveDunitTest.ThrowingCacheWriter(throwingKey));
    });

    String memberToRemove = "member1-0";

    assertThatThrownBy(
        () -> jedis.smove(sourceKey, throwingKey, memberToRemove))
            .hasMessage(SERVER_ERROR_MESSAGE);

    // Assert smove has not happened
    assertThat(jedis.scard(sourceKey)).isEqualTo(sourceSize);
    assertThat(jedis.smembers(sourceKey)).contains(memberToRemove);
    assertThat(jedis.scard(throwingKey)).isEqualTo(destinationSize);
    assertThat(jedis.smembers(throwingKey)).doesNotContain(memberToRemove);

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

  private void repeatSMove(String hashTag, List<String> initialElements,
      AtomicBoolean continueRunning) {
    String source = hashTag + KEY_1;
    String destination = hashTag + KEY_2;

    while (continueRunning.get()) {
      for (int i = 0; i < initialElements.size(); i++) {
        int movedIndex = (initialElements.size() - 1) - i;
        assertThat(jedis.smove(source, destination, initialElements.get(movedIndex))).isEqualTo(1);

        // Confirm we moved the correct element
        assertThat(jedis.smembers(source))
            .containsExactlyInAnyOrderElementsOf(initialElements.subList(0, movedIndex));
        assertThat(jedis.smembers(destination)).containsExactlyInAnyOrderElementsOf(
            initialElements.subList(movedIndex, initialElements.size()));
      }

      // All elements have been moved
      assertThat(jedis.exists(source)).isFalse();

      // Swap the source and destination keys
      String tmp = source;
      source = destination;
      destination = tmp;
    }
  }

  private void repeatSMoveWithSameSourceAndDest(String hashTag, List<String> initialElements,
      AtomicBoolean continueRunning) {
    String key = hashTag + KEY_1;

    while (continueRunning.get()) {
      for (String element : initialElements) {
        assertThat(jedis.smove(key, key, element)).isEqualTo(1);
        Collections.rotate(initialElements, 1);
        assertThat(jedis.smembers(key)).containsExactlyInAnyOrderElementsOf(initialElements);
      }
    }
  }

  private List<String> getHashTagsForEachServer() {
    List<String> hashTags = new ArrayList<>();
    hashTags.add("{" + clusterStartUp.getKeyOnServer("tag", 1) + "}");
    hashTags.add("{" + clusterStartUp.getKeyOnServer("tag", 2) + "}");
    hashTags.add("{" + clusterStartUp.getKeyOnServer("tag", 3) + "}");
    return hashTags;
  }

  private List<String> makeMemberList(int setSize, String baseString) {
    List<String> members = new ArrayList<>();
    for (int i = 0; i < setSize; i++) {
      members.add(baseString + i);
    }
    return members;
  }
}
