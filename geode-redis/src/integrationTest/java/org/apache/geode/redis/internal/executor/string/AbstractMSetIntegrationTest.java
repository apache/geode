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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.RedisPortSupplier;

public abstract class AbstractMSetIntegrationTest implements RedisPortSupplier {

  private Jedis jedis;
  private Jedis jedis2;
  private static final int ITERATION_COUNT = 4000;
  private static final int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

  @Before
  public void setUp() {
    jedis = new Jedis("localhost", getPort(), REDIS_CLIENT_TIMEOUT);
    jedis2 = new Jedis("localhost", getPort(), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void tearDown() {
    jedis.flushAll();
    jedis.close();
    jedis2.close();
  }

  @Test
  public void givenKeyNotProvided_returnsWrongNumberOfArgumentsError() {
    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.MSET))
        .hasMessageContaining("ERR wrong number of arguments for 'mset' command");
  }

  @Test
  public void givenValueNotProvided_returnsWrongNumberOfArgumentsError() {
    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.MSET, "key"))
        .hasMessageContaining("ERR wrong number of arguments for 'mset' command");
  }

  @Test
  public void givenEvenNumberOfArgumentsProvided_returnsWrongNumberOfArgumentsError() {
    // Redis returns this message in this scenario: "ERR wrong number of arguments for MSET"
    assertThatThrownBy(
        () -> jedis.sendCommand(Protocol.Command.MSET, "key1", "value1", "key2", "value2", "key3"))
            .hasMessageContaining("ERR wrong number of arguments");
  }

  @Test
  public void testMSet_setsKeysAndReturnsCorrectValues() {
    int keyCount = 5;
    String[] keyvals = new String[(keyCount * 2)];
    String[] keys = new String[keyCount];
    String[] vals = new String[keyCount];
    for (int i = 0; i < keyCount; i++) {
      String key = randString();
      String val = randString();
      keyvals[2 * i] = key;
      keyvals[2 * i + 1] = val;
      keys[i] = key;
      vals[i] = val;
    }

    String resultString = jedis.mset(keyvals);
    assertThat(resultString).isEqualTo("OK");

    assertThat(jedis.mget(keys)).containsExactly(vals);
  }

  @Test
  @Ignore("GEODE-8192")
  public void testMSet_concurrentInstances_mustBeAtomic()
      throws InterruptedException, ExecutionException {
    String keyBaseName = "MSETBASE";
    String val1BaseName = "FIRSTVALBASE";
    String val2BaseName = "SECONDVALBASE";
    String[] keysAndVals1 = new String[(ITERATION_COUNT * 2)];
    String[] keysAndVals2 = new String[(ITERATION_COUNT * 2)];
    String[] keys = new String[ITERATION_COUNT];
    String[] vals1 = new String[ITERATION_COUNT];
    String[] vals2 = new String[ITERATION_COUNT];
    String[] expectedVals;

    SetUpArraysForConcurrentMSet(keyBaseName,
        val1BaseName, val2BaseName,
        keysAndVals1, keysAndVals2,
        keys,
        vals1, vals2);

    RunTwoMSetsInParallelThreadsAndVerifyReturnValue(keysAndVals1, keysAndVals2);

    List<String> actualVals = jedis.mget(keys);
    expectedVals = DetermineWhichMSetWonTheRace(vals1, vals2, actualVals);

    assertThat(actualVals.toArray(new String[] {})).contains(expectedVals);
  }

  private void SetUpArraysForConcurrentMSet(String keyBaseName, String val1BaseName,
      String val2BaseName, String[] keysAndVals1,
      String[] keysAndVals2, String[] keys, String[] vals1,
      String[] vals2) {
    for (int i = 0; i < ITERATION_COUNT; i++) {
      String key = keyBaseName + i;
      String value1 = val1BaseName + i;
      String value2 = val2BaseName + i;
      keysAndVals1[2 * i] = key;
      keysAndVals1[2 * i + 1] = value1;
      keysAndVals2[2 * i] = key;
      keysAndVals2[2 * i + 1] = value2;
      keys[i] = key;
      vals1[i] = value1;
      vals2[i] = value2;
    }
  }

  private void RunTwoMSetsInParallelThreadsAndVerifyReturnValue(String[] keysAndVals1,
      String[] keysAndVals2)
      throws InterruptedException, ExecutionException {
    CountDownLatch latch = new CountDownLatch(1);
    ExecutorService pool = Executors.newFixedThreadPool(2);
    Callable<String> callable1 = () -> jedis.mset(keysAndVals1);
    Callable<String> callable2 = () -> jedis2.mset(keysAndVals2);
    Future<String> future1 = pool.submit(callable1);
    Future<String> future2 = pool.submit(callable2);

    latch.countDown();

    assertThat(future1.get()).isEqualTo("OK");
    assertThat(future2.get()).isEqualTo("OK");
  }

  private String[] DetermineWhichMSetWonTheRace(String[] vals1, String[] vals2,
      List<String> actualVals) {
    String[] expectedVals;
    if (actualVals.get(0).equals("FIRSTVALBASE0")) {
      expectedVals = vals1;
    } else {
      expectedVals = vals2;
    }
    return expectedVals;
  }

  private String randString() {
    return Long.toHexString(Double.doubleToLongBits(Math.random()));
  }
}
