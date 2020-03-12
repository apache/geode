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

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import redis.clients.jedis.Jedis;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.junit.categories.RedisTest;

@Category({RedisTest.class})
public class StringsJunitTest {

  private static Jedis jedis;
  private static GeodeRedisServer server;
  private static GemFireCache cache;
  private static Random rand;
  private static int port = 6379;
  private static int ITERATION_COUNT = 4000;

  @BeforeClass
  public static void setUp() throws IOException {
    rand = new Random();
    CacheFactory cf = new CacheFactory();
    cf.set(LOG_LEVEL, "error");
    cf.set(MCAST_PORT, "0");
    cf.set(LOCATORS, "");
    cache = cf.create();
    port = AvailablePortHelper.getRandomAvailableTCPPort();
    server = new GeodeRedisServer("localhost", port);

    server.start();
    jedis = new Jedis("localhost", port, 10000000);
  }

  @Test
  public void testAppendAndStrlen() {
    String key = randString();
    int len = key.length();
    String full = key;
    jedis.set(key, key);
    for (int i = 0; i < 15; i++) {
      String rand = randString();
      jedis.append(key, rand);
      len += rand.length();
      full += rand;
    }
    String ret = jedis.get(key);
    assertThat(ret.length()).isEqualTo(len);
    assertThat(full.equals(ret));
    assertThat(new Long(full.length())).isEqualTo(jedis.strlen(key));
  }

  @Test
  public void testDecr() {
    String key1 = randString();
    String key2 = randString();
    String key3 = randString();
    int num1 = 100;
    int num2 = -100;
    jedis.set(key1, "" + num1);
    // jedis.set(key3, "-100");
    jedis.set(key2, "" + num2);

    jedis.decr(key1);
    jedis.decr(key3);
    jedis.decr(key2);
    assertThat(jedis.get(key1)).isEqualTo("" + (num1 - 1));
    assertThat(jedis.get(key2)).isEqualTo("" + (num2 - 1));
    assertThat(jedis.get(key3)).isEqualTo("" + (-1));
  }

  @Test
  public void testIncr() {
    String key1 = randString();
    String key2 = randString();
    String key3 = randString();
    int num1 = 100;
    int num2 = -100;
    jedis.set(key1, "" + num1);
    // jedis.set(key3, "-100");
    jedis.set(key2, "" + num2);

    jedis.incr(key1);
    jedis.incr(key3);
    jedis.incr(key2);

    assertThat(jedis.get(key1)).isEqualTo("" + (num1 + 1));
    assertThat(jedis.get(key2)).isEqualTo("" + (num2 + 1));
    assertThat(jedis.get(key3)).isEqualTo("" + (+1));
  }

  @Test
  public void testDecrBy() {
    String key1 = randString();
    String key2 = randString();
    String key3 = randString();
    int decr1 = rand.nextInt(100);
    int decr2 = rand.nextInt(100);
    Long decr3 = Long.MAX_VALUE / 2;
    int num1 = 100;
    int num2 = -100;
    jedis.set(key1, "" + num1);
    jedis.set(key2, "" + num2);
    jedis.set(key3, "" + Long.MIN_VALUE);

    jedis.decrBy(key1, decr1);
    jedis.decrBy(key2, decr2);

    assertThat(jedis.get(key1)).isEqualTo("" + (num1 - decr1 * 1));
    assertThat(jedis.get(key2)).isEqualTo("" + (num2 - decr2 * 1));

    Exception ex = null;
    try {
      jedis.decrBy(key3, decr3);
    } catch (Exception e) {
      ex = e;
    }
    assertThat(ex).isNotNull();

  }

  @Test
  public void testIncrBy() {
    String key1 = randString();
    String key2 = randString();
    String key3 = randString();
    int incr1 = rand.nextInt(100);
    int incr2 = rand.nextInt(100);
    Long incr3 = Long.MAX_VALUE / 2;
    int num1 = 100;
    int num2 = -100;
    jedis.set(key1, "" + num1);
    jedis.set(key2, "" + num2);
    jedis.set(key3, "" + Long.MAX_VALUE);

    jedis.incrBy(key1, incr1);
    jedis.incrBy(key2, incr2);
    assertThat(jedis.get(key1)).isEqualTo("" + (num1 + incr1 * 1));
    assertThat(jedis.get(key2)).isEqualTo("" + (num2 + incr2 * 1));

    Exception ex = null;
    try {
      jedis.incrBy(key3, incr3);
    } catch (Exception e) {
      ex = e;
    }
    assertThat(ex).isNotNull();
  }

  @Test
  public void testGetRange() {
    String sent = randString();
    String contents = randString();
    jedis.set(sent, contents);
    for (int i = 0; i < sent.length(); i++) {
      String range = jedis.getrange(sent, i, -1);
      assertThat(contents.substring(i)).isEqualTo(range);
    }
    assertThat(jedis.getrange(sent, 2, 0)).isNull();
  }

  @Test
  public void testGetSet() {
    String key = randString();
    String contents = randString();
    jedis.set(key, contents);
    String newContents = randString();
    String oldContents = jedis.getSet(key, newContents);
    assertThat(oldContents).isEqualTo(contents);
    contents = newContents;
  }

  @Test
  public void testDel() {
    String key1 = "firstKey";
    String key2 = "secondKey";
    String key3 = "thirdKey";
    jedis.set(key1, randString());

    assertThat(jedis.del(key1)).isEqualTo(1L);
    assertThat(jedis.get(key1)).isNull();

    jedis.set(key1, randString());
    jedis.set(key2, randString());

    assertThat(jedis.del(key1, key2, key3)).isEqualTo(2L);
    assertThat(jedis.get(key1)).isNull();
    assertThat(jedis.get(key2)).isNull();

    assertThat(jedis.del("ceci nest pas un clavier")).isEqualTo(0L);
  }

  @Test
  public void testMSetAndGet() {
    int r = 5;
    String[] keyvals = new String[(r * 2)];
    String[] keys = new String[r];
    String[] vals = new String[r];
    for (int i = 0; i < r; i++) {
      String key = randString();
      String val = randString();
      keyvals[2 * i] = key;
      keyvals[2 * i + 1] = val;
      keys[i] = key;
      vals[i] = val;
    }

    jedis.mset(keyvals);

    List<String> ret = jedis.mget(keys);
    Object[] retArray = ret.toArray();

    assertThat(Arrays.equals(vals, retArray));
  }

  @Test
  public void testConcurrentDel_differentClients() throws InterruptedException, ExecutionException {
    String keyBaseName = "DELBASE";

    Jedis jedis2 = new Jedis("localhost", port, 10000000);

    doABunchOfSets(keyBaseName, jedis);

    CountDownLatch latch = new CountDownLatch(1);
    ExecutorService pool = Executors.newFixedThreadPool(2);
    Callable<Integer> callable1 = () -> doABunchOfDels(keyBaseName, 0, jedis);
    Callable<Integer> callable2 = () -> doABunchOfDels(keyBaseName, 1, jedis2);
    Future<Integer> future1 = pool.submit(callable1);
    Future<Integer> future2 = pool.submit(callable2);

    latch.countDown();
    assertThat(future1.get() + future2.get()).isEqualTo(ITERATION_COUNT);

    for (int i = 0; i < ITERATION_COUNT; i++) {
      assertThat(jedis.get(keyBaseName + i)).isNull();
    }

    pool.shutdown();
  }

  private void doABunchOfSets(String keyBaseName, Jedis jedis) {
    for (int i = 0; i < ITERATION_COUNT; i++) {
      jedis.set(keyBaseName + i, "value" + i);
    }
  }

  private int doABunchOfDels(String keyBaseName, int start, Jedis jedis) {
    int delCount = 0;
    for (int i = start; i < ITERATION_COUNT; i += 2) {
      delCount += jedis.del(keyBaseName + i);
      Thread.yield();
    }
    return delCount;
  }

  @Test
  public void testMSetNX() {
    Set<String> strings = new HashSet<String>();
    for (int i = 0; i < 2 * 5; i++)
      strings.add(randString());
    String[] array = strings.toArray(new String[0]);
    long response = jedis.msetnx(array);

    assertThat(response).isEqualTo(1);

    long response2 = jedis.msetnx(array[0], randString());

    assertThat(response2).isEqualTo(0);
    assertThat(array[1]).isEqualTo(jedis.get(array[0]));
  }

  @Test
  public void testSetNX() {
    String key1 = randString();
    String key2;
    do {
      key2 = randString();
    } while (key2.equals(key1));

    long response1 = jedis.setnx(key1, key1);
    long response2 = jedis.setnx(key2, key2);
    long response3 = jedis.setnx(key1, key2);

    assertThat(response1).isEqualTo(1);
    assertThat(response2).isEqualTo(1);
    assertThat(response3).isEqualTo(0);
  }

  @Test
  public void testPAndSetex() {
    Random r = new Random();
    int setex = r.nextInt(5);
    if (setex == 0)
      setex = 1;
    String key = randString();
    jedis.setex(key, setex, randString());
    try {
      Thread.sleep((setex + 5) * 1000);
    } catch (InterruptedException e) {
      return;
    }
    String result = jedis.get(key);
    // System.out.println(result);
    assertThat(result).isNull();

    int psetex = r.nextInt(5000);
    if (psetex == 0)
      psetex = 1;
    key = randString();
    jedis.psetex(key, psetex, randString());
    long start = System.currentTimeMillis();
    try {
      Thread.sleep(psetex + 5000);
    } catch (InterruptedException e) {
      return;
    }
    long stop = System.currentTimeMillis();
    result = jedis.get(key);
    assertThat(stop - start).isGreaterThanOrEqualTo(psetex);
    assertThat(result).isNull();
  }

  private String randString() {
    return Long.toHexString(Double.doubleToLongBits(Math.random()));
  }

  @After
  public void flushAll() {
    jedis.flushAll();
  }

  @AfterClass
  public static void tearDown() {
    jedis.close();
    cache.close();
    server.shutdown();
  }
}
