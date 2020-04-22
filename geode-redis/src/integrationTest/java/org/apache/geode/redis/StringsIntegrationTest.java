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

import static java.lang.Integer.parseInt;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.redis.GeodeRedisServer.REDIS_META_DATA_REGION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.params.SetParams;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.redis.general.ConcurrentLoopingThreads;
import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.junit.categories.RedisTest;

@Category({RedisTest.class})
public class StringsIntegrationTest {

  static Jedis jedis;
  static Jedis jedis2;
  static Random rand;
  private static GeodeRedisServer server;
  private static GemFireCache cache;
  private static int port = 6379;
  private static int ITERATION_COUNT = 4000;

  @BeforeClass
  public static void setUp() {
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
    jedis2 = new Jedis("localhost", port, 10000000);
  }

  @After
  public void flushAll() {
    jedis.flushAll();
  }

  @AfterClass
  public static void tearDown() {
    jedis.close();
    jedis2.close();
    cache.close();
    server.shutdown();
  }

  @Test
  public void testSET_shouldSetStringValueToKey_givenEmptyKey() {

    String key = "key";
    String value = "value";

    String result = jedis.get(key);
    assertThat(result).isNull();

    jedis.set(key, value);
    result = jedis.get(key);
    assertThat(result).isEqualTo(value);
  }

  @Test
  public void testSET_shouldSetStringValueToKey_givenKeyIsOfDataTypeSet() {
    String key = "key";
    String stringValue = "value";

    jedis.sadd(key, "member1", "member2");

    jedis.set(key, stringValue);
    String result = jedis.get(key);

    assertThat(result).isEqualTo(stringValue);
  }

  @Test
  public void testSET_shouldSetStringValueToKey_givenKeyIsOfDataTypeHash() {
    String key = "key";
    String stringValue = "value";

    jedis.hset(key, "field", "something else");

    String result = jedis.set(key, stringValue);
    assertThat(result).isEqualTo("OK");

    assertThat(stringValue).isEqualTo(jedis.get(key));
  }

  @Test
  public void testSET_shouldSetNX_evenIfKeyContainsOtherDataType() {
    String key = "key";
    String stringValue = "value";

    jedis.sadd(key, "member1", "member2");
    SetParams setParams = new SetParams();
    setParams.nx();

    String result = jedis.set(key, stringValue, setParams);
    assertThat(result).isNull();
  }

  @Test
  public void testSET_shouldSetXX_evenIfKeyContainsOtherDataType() {
    String key = "key";
    String stringValue = "value";

    jedis.sadd(key, "member1", "member2");
    SetParams setParams = new SetParams();
    setParams.xx();

    jedis.set(key, stringValue, setParams);
    String result = jedis.get(key);

    assertThat(result).isEqualTo(stringValue);
  }

  @Test
  public void testSET_withEXargument_shouldSetExpireTime() {
    String key = "key";
    String value = "value";
    int secondsUntilExpiration = 20;

    SetParams setParams = new SetParams();
    setParams.ex(secondsUntilExpiration);

    jedis.set(key, value, setParams);

    Long result = jedis.ttl(key);

    assertThat(result).isGreaterThan(15l);
  }

  @Test
  public void testSET_withNegative_EX_time_shouldReturnError() {
    String key = "key";
    String value = "value";
    int millisecondsUntilExpiration = -1;

    SetParams setParams = new SetParams();
    setParams.ex(millisecondsUntilExpiration);

    assertThatThrownBy(() -> jedis.set(key, value, setParams))
        .isInstanceOf(JedisDataException.class)
        .hasMessageContaining(RedisConstants.ERROR_INVALID_EXPIRE_TIME);
  }

  @Test
  public void testSET_withPXargument_shouldSetExpireTime() {
    String key = "key";
    String value = "value";
    int millisecondsUntilExpiration = 20000;

    SetParams setParams = new SetParams();
    setParams.px(millisecondsUntilExpiration);

    jedis.set(key, value, setParams);

    Long result = jedis.ttl(key);

    assertThat(result).isGreaterThan(15l);
  }

  @Test
  public void testSET_with_Negative_PX_time_shouldReturnError() {
    String key = "key";
    String value = "value";
    int millisecondsUntilExpiration = -1;

    SetParams setParams = new SetParams();
    setParams.px(millisecondsUntilExpiration);

    assertThatThrownBy(() -> jedis.set(key, value, setParams))
        .isInstanceOf(JedisDataException.class)
        .hasMessageContaining(RedisConstants.ERROR_INVALID_EXPIRE_TIME);
  }

  @Test
  public void testSET_shouldClearPreviousTTL_onSuccess() {
    String key = "key";
    String value = "value";
    int secondsUntilExpiration = 20;

    SetParams setParams = new SetParams();
    setParams.ex(secondsUntilExpiration);

    jedis.set(key, value, setParams);

    jedis.set(key, "other value");

    Long result = jedis.ttl(key);

    assertThat(result).isEqualTo(-1L);
  }

  @Test
  public void testSET_withXXArgument_shouldClearPreviousTTL_Success() {
    String key = "xx_key";
    String value = "did exist";
    int secondsUntilExpiration = 20;
    SetParams setParamsXX = new SetParams();
    setParamsXX.xx();
    SetParams setParamsEX = new SetParams();
    setParamsEX.ex(secondsUntilExpiration);
    String result_EX = jedis.set(key, value, setParamsEX);
    assertThat(result_EX).isEqualTo("OK");
    assertThat(jedis.ttl(key)).isGreaterThan(15L);

    String result_XX = jedis.set(key, value, setParamsXX);

    assertThat(result_XX).isEqualTo("OK");
    Long result = jedis.ttl(key);
    assertThat(result).isEqualTo(-1L);
  }

  @Test
  public void testSET_should_not_clearPreviousTTL_onFailure() {
    String key_NX = "nx_key";
    String value_NX = "set only if key did not exist";
    int secondsUntilExpiration = 20;

    SetParams setParamsEX = new SetParams();
    setParamsEX.ex(secondsUntilExpiration);

    SetParams setParamsNX = new SetParams();
    setParamsNX.nx();

    jedis.set(key_NX, value_NX, setParamsEX);
    String result_NX = jedis.set(key_NX, value_NX, setParamsNX);
    assertThat(result_NX).isNull();

    Long result = jedis.ttl(key_NX);
    assertThat(result).isGreaterThan(15L);
  }

  @Test
  @Ignore
  public void testSET_with_KEEPTTL_shouldRetainPreviousTTL_OnSuccess() {
    String key = "key";
    String value = "value";
    int secondsToExpire = 30;

    SetParams setParamsEx = new SetParams();
    setParamsEx.ex(secondsToExpire);

    jedis.set(key, value, setParamsEx);

    SetParams setParamsKeepTTL = new SetParams();
    // setParamsKeepTTL.keepTtl();
    // Jedis Doesn't support KEEPTTL yet.

    jedis.set(key, "newValue", setParamsKeepTTL);

    Long result = jedis.ttl(key);
    assertThat(result).isGreaterThan(15L);
  }

  @Test
  public void testSET_withNXargument_shouldOnlySetKeyIfKeyDoesNotExist() {
    String key1 = "key_1";
    String key2 = "key_2";
    String value1 = "value_1";
    String value2 = "value_2";

    jedis.set(key1, value1);

    SetParams setParams = new SetParams();
    setParams.nx();

    jedis.set(key1, value2, setParams);
    String result1 = jedis.get(key1);

    assertThat(result1).isEqualTo(value1);

    jedis.set(key2, value2, setParams);
    String result2 = jedis.get(key2);

    assertThat(result2).isEqualTo(value2);
  }

  @Test
  public void testSET_withXXargument_shouldOnlySetKeyIfKeyExists() {
    String key1 = "key_1";
    String key2 = "key_2";
    String value1 = "value_1";
    String value2 = "value_2";

    jedis.set(key1, value1);

    SetParams setParams = new SetParams();
    setParams.xx();

    jedis.set(key1, value2, setParams);
    String result1 = jedis.get(key1);

    assertThat(result1).isEqualTo(value2);

    jedis.set(key2, value2, setParams);
    String result2 = jedis.get(key2);

    assertThat(result2).isNull();
  }

  @Test
  public void testSET_XX_NX_arguments_should_return_OK_if_Successful() {
    String key_NX = "nx_key";
    String key_XX = "xx_key";
    String value_NX = "did not exist";
    String value_XX = "did exist";

    SetParams setParamsXX = new SetParams();
    setParamsXX.xx();

    SetParams setParamsNX = new SetParams();
    setParamsNX.nx();

    String result_NX = jedis.set(key_NX, value_NX, setParamsNX);
    assertThat(result_NX).isEqualTo("OK");

    jedis.set(key_XX, value_XX);
    String result_XX = jedis.set(key_NX, value_NX, setParamsXX);
    assertThat(result_XX).isEqualTo("OK");
  }

  @Test
  public void testSET_XX_NX_arguments_should_return_NULL_if_Not_Successful() {
    String key_NX = "nx_key";
    String key_XX = "xx_key";
    String value_NX = "set only if key did not exist";
    String value_XX = "set only if key did exist";

    SetParams setParamsXX = new SetParams();
    setParamsXX.xx();

    SetParams setParamsNX = new SetParams();
    setParamsNX.nx();

    jedis.set(key_NX, value_NX);
    String result_NX = jedis.set(key_NX, value_NX, setParamsNX);
    assertThat(result_NX).isNull();

    String result_XX = jedis.set(key_XX, value_XX, setParamsXX);
    assertThat(result_XX).isNull();
  }

  @Test
  public void testGET_shouldReturnValueOfKey_givenValueIsAString() {
    String key = "key";
    String value = "value";

    String result = jedis.get(key);
    assertThat(result).isNull();

    jedis.set(key, value);
    result = jedis.get(key);
    assertThat(result).isEqualTo(value);
  }

  @Test
  public void testGET_shouldReturnNil_givenKeyIsEmpty() {
    String key = "this key does not exist";

    String result = jedis.get(key);
    assertThat(result).isNull();
  }

  @Test(expected = JedisDataException.class)
  public void testGET_shouldThrow_JedisDataExceptiondError_givenValueIs_Not_A_String() {
    String key = "key";
    String field = "field";
    String member = "member";

    jedis.sadd(key, field, member);

    jedis.get(key);
  }

  @Test
  public void testAppend_shouldAppendValueWithInputStringAndReturnResultingLength() {
    String key = "key";
    String value = randString();
    int originalValueLength = value.length();

    boolean result = jedis.exists(key);
    assertThat(result).isFalse();

    Long output = jedis.append(key, value);
    assertThat(output).isEqualTo(originalValueLength);

    String randomString = randString();

    output = jedis.append(key, randomString);
    assertThat(output).isEqualTo(originalValueLength + randomString.length());

    String finalValue = jedis.get(key);
    assertThat(finalValue).isEqualTo(value.concat(randomString));
  }


  @Test
  public void testGetRange_whenWholeRangeSpecified_returnsEntireValue() {
    String key = "key";
    String valueWith19Characters = "abc123babyyouknowme";

    jedis.set(key, valueWith19Characters);

    String everything = jedis.getrange(key, 0, -1);
    assertThat(everything).isEqualTo(valueWith19Characters);

    String alsoEverything = jedis.getrange(key, 0, 18);
    assertThat(alsoEverything).isEqualTo(valueWith19Characters);

  }

  @Test
  public void testGetRange_whenMoreThanWholeRangeSpecified_returnsEntireValue() {
    String key = "key";
    String valueWith19Characters = "abc123babyyouknowme";

    jedis.set(key, valueWith19Characters);

    String fromStartToWayPastEnd = jedis.getrange(key, 0, 5000);
    assertThat(fromStartToWayPastEnd).isEqualTo(valueWith19Characters);

    String wayBeforeStartAndJustToEnd = jedis.getrange(key, -50000, -1);
    assertThat(wayBeforeStartAndJustToEnd).isEqualTo(valueWith19Characters);

    String wayBeforeStartAndWayAfterEnd = jedis.getrange(key, -50000, 5000);
    assertThat(wayBeforeStartAndWayAfterEnd).isEqualTo(valueWith19Characters);
  }

  @Test
  public void testGetRange_whenValidSubrangeSpecified_returnsAppropriateSubstring() {
    String key = "key";
    String valueWith19Characters = "abc123babyyouknowme";

    jedis.set(key, valueWith19Characters);

    String fromStartToBeforeEnd = jedis.getrange(key, 0, 16);
    assertThat(fromStartToBeforeEnd).isEqualTo("abc123babyyouknow");

    String fromStartByNegativeOffsetToBeforeEnd = jedis.getrange(key, -19, 16);
    assertThat(fromStartByNegativeOffsetToBeforeEnd).isEqualTo("abc123babyyouknow");

    String fromStartToBeforeEndByNegativeOffset = jedis.getrange(key, 0, -3);
    assertThat(fromStartToBeforeEndByNegativeOffset).isEqualTo("abc123babyyouknow");

    String fromAfterStartToBeforeEnd = jedis.getrange(key, 2, 16);
    assertThat(fromAfterStartToBeforeEnd).isEqualTo("c123babyyouknow");

    String fromAfterStartByNegativeOffsetToBeforeEndByNegativeOffset = jedis.getrange(key, -16, -2);
    assertThat(fromAfterStartByNegativeOffsetToBeforeEndByNegativeOffset)
        .isEqualTo("123babyyouknowm");

    String fromAfterStartToEnd = jedis.getrange(key, 2, 18);
    assertThat(fromAfterStartToEnd).isEqualTo("c123babyyouknowme");

    String fromAfterStartToEndByNegativeOffset = jedis.getrange(key, 2, -1);
    assertThat(fromAfterStartToEndByNegativeOffset).isEqualTo("c123babyyouknowme");
  }

  @Test
  public void testGetRange_rangeIsInvalid_returnsEmptyString() {
    String key = "key";
    String valueWith19Characters = "abc123babyyouknowme";

    jedis.set(key, valueWith19Characters);

    String range1 = jedis.getrange(key, -2, -16);
    assertThat(range1).isEqualTo("");

    String range2 = jedis.getrange(key, 2, 0);
    assertThat(range2).isEqualTo("");
  }

  @Test
  public void testGetRange_nonexistentKey_returnsEmptyString() {
    String key = "nonexistent";

    String range = jedis.getrange(key, 0, -1);
    assertThat(range).isEqualTo("");
  }

  @Test
  public void testGetRange_rangePastEndOfValue_returnsEmptyString() {
    String key = "key";
    String value = "value";

    jedis.set(key, value);

    String range = jedis.getrange(key, 7, 14);
    assertThat(range).isEqualTo("");
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
  public void testSet_protectedRedisDataType_throwsRedisDataTypeMismatchException() {
    assertThatThrownBy(
        () -> jedis.set(REDIS_META_DATA_REGION, "something else"))
            .isInstanceOf(JedisDataException.class)
            .hasMessageContaining("protected");
  }

  @Test
  public void testGetSet_shouldBeAtomic()
      throws ExecutionException, InterruptedException, TimeoutException {
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

  private Integer doABunchOfIncrs(Jedis jedis, CountDownLatch latch) throws InterruptedException {
    latch.await();
    for (int i = 0; i < ITERATION_COUNT; i++) {
      jedis.incr("contestedKey");
    }
    return ITERATION_COUNT;
  }

  private Integer doABunchOfGetSets(Jedis jedis, CountDownLatch latch) throws InterruptedException {
    int sum = 0;
    latch.await();

    while (sum < ITERATION_COUNT) {
      sum += Integer.parseInt(jedis.getSet("contestedKey", "0"));
    }
    return sum;
  }

  @Test
  public void testDel_deletingOneKey_removesKeyAndReturnsOne() {
    String key1 = "firstKey";
    jedis.set(key1, randString());

    Long deletedCount = jedis.del(key1);

    assertThat(deletedCount).isEqualTo(1L);
    assertThat(jedis.get(key1)).isNull();
  }

  @Test
  public void testDel_deletingNonexistentKey_returnsZero() {
    assertThat(jedis.del("ceci nest pas un clavier")).isEqualTo(0L);
  }

  @Test
  public void testDel_deletingMultipleKeys_returnsCountOfOnlyDeletedKeys() {
    String key1 = "firstKey";
    String key2 = "secondKey";
    String key3 = "thirdKey";

    jedis.set(key1, randString());
    jedis.set(key2, randString());

    assertThat(jedis.del(key1, key2, key3)).isEqualTo(2L);
    assertThat(jedis.get(key1)).isNull();
    assertThat(jedis.get(key2)).isNull();
  }

  @Test
  public void testMSetAndMGet_forHappyPath_setsKeysAndReturnsCorrectValues() {
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

    List<String> ret = jedis.mget(keys);
    Object[] retArray = ret.toArray();

    assertThat(Arrays.equals(vals, retArray)).isTrue();
  }

  @Test
  public void testMGet_requestNonexistentKey_respondsWithNil() {
    String key1 = "existingKey";
    String key2 = "notReallyAKey";
    String value1 = "theRealValue";
    String[] keys = new String[2];
    String[] expectedVals = new String[2];
    keys[0] = key1;
    keys[1] = key2;
    expectedVals[0] = value1;
    expectedVals[1] = null;

    jedis.set(key1, value1);

    List<String> ret = jedis.mget(keys);
    Object[] retArray = ret.toArray();

    assertThat(Arrays.equals(expectedVals, retArray)).isTrue();
  }

  @Test
  public void testMGet_concurrentInstances_mustBeAtomic()
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

  @Test
  public void testConcurrentDel_differentClients() {
    String keyBaseName = "DELBASE";

    new ConcurrentLoopingThreads(
        ITERATION_COUNT,
        (i) -> jedis.set(keyBaseName + i, "value" + i))
            .run();

    AtomicLong deletedCount = new AtomicLong();
    new ConcurrentLoopingThreads(ITERATION_COUNT,
        (i) -> deletedCount.addAndGet(jedis.del(keyBaseName + i)),
        (i) -> deletedCount.addAndGet(jedis2.del(keyBaseName + i)))
            .run();


    assertThat(deletedCount.get()).isEqualTo(ITERATION_COUNT);

    for (int i = 0; i < ITERATION_COUNT; i++) {
      assertThat(jedis.get(keyBaseName + i)).isNull();
    }

  }

  @Test
  public void testMSetNX() {
    Set<String> keysAndVals = new HashSet<String>();
    for (int i = 0; i < 2 * 5; i++) {
      keysAndVals.add(randString());
    }
    String[] keysAndValsArray = keysAndVals.toArray(new String[0]);
    long response = jedis.msetnx(keysAndValsArray);

    assertThat(response).isEqualTo(1);

    long response2 = jedis.msetnx(keysAndValsArray[0], randString());

    assertThat(response2).isEqualTo(0);
    assertThat(keysAndValsArray[1]).isEqualTo(jedis.get(keysAndValsArray[0]));
  }

  @Test
  public void testDecr() {
    String oneHundredKey = randString();
    String negativeOneHundredKey = randString();
    String unsetKey = randString();
    final int oneHundredValue = 100;
    final int negativeOneHundredValue = -100;
    jedis.set(oneHundredKey, Integer.toString(oneHundredValue));
    jedis.set(negativeOneHundredKey, Integer.toString(negativeOneHundredValue));

    jedis.decr(oneHundredKey);
    jedis.decr(negativeOneHundredKey);
    jedis.decr(unsetKey);

    assertThat(jedis.get(oneHundredKey)).isEqualTo(Integer.toString(oneHundredValue - 1));
    assertThat(jedis.get(negativeOneHundredKey))
        .isEqualTo(Integer.toString(negativeOneHundredValue - 1));
    assertThat(jedis.get(unsetKey)).isEqualTo(Integer.toString(-1));
  }

  @Test
  public void testDecr_shouldBeAtomic() throws ExecutionException, InterruptedException {
    jedis.set("contestedKey", "0");

    new ConcurrentLoopingThreads(
        ITERATION_COUNT,
        (i) -> jedis.decr("contestedKey"),
        (i) -> jedis2.decr("contestedKey"))
            .run();

    assertThat(jedis.get("contestedKey")).isEqualTo(Integer.toString(-2 * ITERATION_COUNT));
  }

  @Test
  public void testIncr() {
    String oneHundredKey = randString();
    String negativeOneHundredKey = randString();
    String unsetKey = randString();
    final int oneHundredValue = 100;
    final int negativeOneHundredValue = -100;
    jedis.set(oneHundredKey, Integer.toString(oneHundredValue));
    jedis.set(negativeOneHundredKey, Integer.toString(negativeOneHundredValue));

    jedis.incr(oneHundredKey);
    jedis.incr(negativeOneHundredKey);
    jedis.incr(unsetKey);

    assertThat(jedis.get(oneHundredKey)).isEqualTo(Integer.toString(oneHundredValue + 1));
    assertThat(jedis.get(negativeOneHundredKey))
        .isEqualTo(Integer.toString(negativeOneHundredValue + 1));
    assertThat(jedis.get(unsetKey)).isEqualTo(Integer.toString(1));
  }

  @Test
  public void testIncr_whenOverflow_shouldReturnError() {
    String key = "key";
    String max64BitIntegerValue = "9223372036854775807";
    jedis.set(key, max64BitIntegerValue);

    try {
      jedis.incr(key);
    } catch (JedisDataException e) {
      assertThat(e.getMessage()).contains(RedisConstants.ERROR_OVERFLOW);
    }
    assertThat(jedis.get(key)).isEqualTo(max64BitIntegerValue);
  }

  @Test
  public void testIncr_whenWrongType_shouldReturnError() {
    String key = "key";
    String nonIntegerValue = "I am not a number! I am a free man!";
    jedis.set(key, nonIntegerValue);

    try {
      jedis.incr(key);
    } catch (JedisDataException e) {
      assertThat(e.getMessage()).contains(RedisConstants.ERROR_NOT_INTEGER);
    }
    assertThat(jedis.get(key)).isEqualTo(nonIntegerValue);
  }

  @Test
  public void testIncr_shouldBeAtomic() throws ExecutionException, InterruptedException {
    jedis.set("contestedKey", "0");

    new ConcurrentLoopingThreads(
        ITERATION_COUNT,
        (i) -> jedis.incr("contestedKey"),
        (i) -> jedis2.incr("contestedKey"))
            .run();


    assertThat(jedis.get("contestedKey")).isEqualTo(Integer.toString(2 * ITERATION_COUNT));
  }

  @Test
  // @todo: not looked over for current release
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

  // @todo: not looked over for current release
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

  // @todo: not looked over for current release
  @Test
  public void testPAndSetex() {
    Random r = new Random();
    int setex = r.nextInt(5);
    if (setex == 0) {
      setex = 1;
    }
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
    if (psetex == 0) {
      psetex = 1;
    }
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

  @Test
  // @todo: not looked over for current release
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
  public void testStrlen_requestNonexistentKey_returnsZero() {
    Long result = jedis.strlen("Nohbdy");
    assertThat(result).isEqualTo(0);
  }

  @Test
  public void testStrlen_requestKey_returnsLengthOfStringValue() {
    String value = "byGoogle";

    jedis.set("golang", value);

    Long result = jedis.strlen("golang");
    assertThat(result).isEqualTo(value.length());
  }

  @Test
  public void testStrlen_requestWrongType_shouldReturnError() {
    String key = "hashKey";
    jedis.hset(key, "field", "this value doesn't matter");

    assertThatThrownBy(() -> jedis.strlen(key))
        .isInstanceOf(JedisDataException.class)
        .hasMessageContaining(RedisConstants.ERROR_WRONG_TYPE);
  }

  private String randString() {
    return Long.toHexString(Double.doubleToLongBits(Math.random()));
  }
}
