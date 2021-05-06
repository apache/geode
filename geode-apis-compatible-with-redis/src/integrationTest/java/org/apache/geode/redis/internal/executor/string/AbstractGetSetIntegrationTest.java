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
package org.apache.geode.redis.internal.executor.string;

import static java.lang.Integer.parseInt;
import static org.apache.geode.redis.RedisCommandArgumentsTestHelper.assertExactNumberOfArgs;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisDataException;

import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public abstract class AbstractGetSetIntegrationTest implements RedisIntegrationTest {

  private JedisCluster jedis;
  private JedisCluster jedis2;
  private static final int ITERATION_COUNT = 4000;
  private static final int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

  @Before
  public void setUp() {
    jedis = new JedisCluster(new HostAndPort("localhost", getPort()), REDIS_CLIENT_TIMEOUT);
    jedis2 = new JedisCluster(new HostAndPort("localhost", getPort()), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void tearDown() {
    flushAll();
    jedis.close();
    jedis2.close();
  }

  @Test
  public void errors_givenWrongNumberOfParameters() {
    assertExactNumberOfArgs(jedis, Protocol.Command.GETSET, 2);
  }

  @Test
  public void testGetSet_updatesKeyWithNewValue_returnsOldValue() {
    String key = randString();
    String contents = randString();
    jedis.set(key, contents);

    String newContents = randString();
    String oldContents = jedis.getSet(key, newContents);
    assertThat(oldContents).isEqualTo(contents);

    contents = newContents;
    newContents = jedis.get(key);
    assertThat(newContents).isEqualTo(contents);
  }

  @Test
  public void testGetSet_setsNonexistentKeyToNewValue_returnsNull() {
    String key = randString();
    String newContents = randString();

    String oldContents = jedis.getSet(key, newContents);
    assertThat(oldContents).isNull();

    String contents = jedis.get(key);
    assertThat(newContents).isEqualTo(contents);
  }

  @Test
  public void testGetSet_shouldWorkWith_INCR_Command() {
    String key = "key";
    Long resultLong;
    String resultString;

    jedis.set(key, "0");

    resultLong = jedis.incr(key);
    assertThat(resultLong).isEqualTo(1);

    resultString = jedis.getSet(key, "0");
    assertThat(parseInt(resultString)).isEqualTo(1);

    resultString = jedis.get(key);
    assertThat(parseInt(resultString)).isEqualTo(0);

    resultLong = jedis.incr(key);
    assertThat(resultLong).isEqualTo(1);
  }

  @Test
  public void testGetSet_whenWrongType_shouldReturnError() {
    String key = "key";
    jedis.hset(key, "field", "some hash value");

    assertThatThrownBy(() -> jedis.getSet(key, "this value doesn't matter"))
        .isInstanceOf(JedisDataException.class)
        .hasMessageContaining(RedisConstants.ERROR_WRONG_TYPE);
  }

  @Test
  public void testGetSet_shouldBeAtomic()
      throws ExecutionException, InterruptedException {
    jedis.set("contestedKey", "0");
    assertThat(jedis.get("contestedKey")).isEqualTo("0");
    CountDownLatch latch = new CountDownLatch(1);
    ExecutorService pool = Executors.newFixedThreadPool(2);
    Callable<Integer> callable1 = () -> doABunchOfIncrs(jedis, latch);
    Callable<Integer> callable2 = () -> doABunchOfGetSets(jedis2, latch);
    Future<Integer> future1 = pool.submit(callable1);
    Future<Integer> future2 = pool.submit(callable2);

    latch.countDown();

    GeodeAwaitility.await().untilAsserted(() -> assertThat(future2.get()).isEqualTo(future1.get()));
    assertThat(future1.get() + future2.get()).isEqualTo(2 * ITERATION_COUNT);
  }

  private Integer doABunchOfIncrs(JedisCluster jedis, CountDownLatch latch)
      throws InterruptedException {
    latch.await();
    for (int i = 0; i < ITERATION_COUNT; i++) {
      jedis.incr("contestedKey");
    }
    return ITERATION_COUNT;
  }

  private Integer doABunchOfGetSets(JedisCluster jedis, CountDownLatch latch)
      throws InterruptedException {
    int sum = 0;
    latch.await();

    while (sum < ITERATION_COUNT) {
      sum += Integer.parseInt(jedis.getSet("contestedKey", "0"));
    }
    return sum;
  }

  private String randString() {
    return Long.toHexString(Double.doubleToLongBits(Math.random()));
  }
}
