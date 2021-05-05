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
package org.apache.geode.redis.internal.executor.hash;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.EnumMap;

import org.assertj.core.data.Offset;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.RedisPortSupplier;

/**
 * Contains tests that measure the used memory of redis or radish and assert that the memory
 * overhead is equal to an expected memory overhead.
 *
 * @see MemoryOverheadIntegrationTest
 * @see #measureAndCheckPerEntryOverhead(AddEntryFunction, Measurement) (IntToLongFunction,
 *      Measurement)
 */
public abstract class AbstractMemoryOverheadIntegrationTest implements RedisPortSupplier {

  private static final int WARM_UP_ENTRY_COUNT = 1000;
  private static final int TOTAL_ENTRY_COUNT = 5000;
  private static final int SAMPLE_INTERVAL = 100;
  private static final int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

  /**
   * A value that force native redis to not use an optimized data structure.
   * Native redis uses an optimized data structure (a zip list) for very small hashes and sets. This
   * will allow us to force redis not to use a ziplist.
   */
  public static final String LARGE_STRING =
      "value_that_will_force_redis_to_not_use_a_ziplist______________________________________________________________";

  /**
   * Allowed variance in our measurements before the test fails. This allows us to be up to
   * 7 bytes off from the original measurement.
   */
  public static final long ALLOWED_BYTE_DIFFERENCE = 7L;
  protected Jedis jedis;

  protected enum Measurement {
    STRING,
    HASH,
    HASH_ENTRY,
    SET,
    SET_ENTRY
  }

  @Before
  public void setUp() {
    jedis = new Jedis("localhost", getPort(), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void tearDown() {
    jedis.flushAll();
    jedis.close();
  }

  /**
   * Subclasses should use this to return the expected per entry overhead for each measurement.
   *
   * @return A map with the expected overhead for each measurement type.
   */
  abstract EnumMap<Measurement, Integer> expectedPerEntryOverhead();

  /**
   * Return the total used memory on the server.
   */
  abstract long getUsedMemory();

  /**
   * Measure the overhead for each redis string that is added to the server.
   */
  @Test
  public void measureOverheadPerString() {
    // Function that adds a new redis string to the server
    final AddEntryFunction addStringFunction = uniqueString -> {
      String response = jedis.set(uniqueString, uniqueString);
      assertThat(response).isEqualTo("OK");

      // Note - jedis convert strings to bytes with the UTF-8 charset
      // Since the strings above are all ASCII, the length == the number of bytes
      return uniqueString.length() + uniqueString.length();
    };

    measureAndCheckPerEntryOverhead(addStringFunction, Measurement.STRING);
  }

  /**
   * Measure the overhead for each redis hash that is added to the server.
   */
  @Test
  public void measureOverheadPerHash() {
    // Function that adds a new redis hash to the server
    final AddEntryFunction addHashFunction = uniqueString -> {
      String mapKey = "key";
      Long response = jedis.hset(uniqueString, mapKey, LARGE_STRING);
      assertThat(response).isEqualTo(1);
      return uniqueString.length() + mapKey.length() + LARGE_STRING.length();
    };

    measureAndCheckPerEntryOverhead(addHashFunction, Measurement.HASH);
  }

  /**
   * Measure the overhead for each entry that is added to a redis hash. This
   * uses a single hash and adds additional fields to the hash and measures the overhead
   * of the additional fields.
   */
  @Test
  public void measureOverheadPerHashEntry() {
    // Function that adds an additional hash entry to a single redis hash
    final AddEntryFunction addHashEntryFunction = uniqueString -> {

      String valueString = String.format("%s value-%s", LARGE_STRING, uniqueString);
      Long response = jedis.hset("TestSet", uniqueString, valueString);
      assertThat(response).isEqualTo(1);

      return uniqueString.length() + valueString.length();
    };

    measureAndCheckPerEntryOverhead(addHashEntryFunction, Measurement.HASH_ENTRY);
  }

  /**
   * Measure the overhead for each redis set that is added to the server.
   */
  @Test
  public void measureOverheadPerSet() {
    // Function that adds a new redis set to the server
    final AddEntryFunction addSetFunction = uniqueString -> {
      Long response = jedis.sadd(uniqueString, LARGE_STRING);
      assertThat(response).isEqualTo(1);
      return uniqueString.length() + LARGE_STRING.length();
    };

    measureAndCheckPerEntryOverhead(addSetFunction, Measurement.SET);
  }

  /**
   * Measure the overhead for each entry that is added to a redis set. This
   * uses a single sets and adds additional fields to the hash and measures the overhead
   * of the additional fields.
   */
  @Test
  public void measureOverheadPerSetEntry() {
    // Function that adds a new entry to a single redis set
    final AddEntryFunction addSetEntryFunction = uniqueString -> {

      String valueString = String.format("%s value-%s", LARGE_STRING, uniqueString);
      Long response = jedis.sadd("TestSet", valueString);
      assertThat(response).isEqualTo(1);

      return valueString.length();
    };

    measureAndCheckPerEntryOverhead(addSetEntryFunction, Measurement.SET_ENTRY);
  }

  /**
   * Measures the per entry overhead of a particular operation. The passed in function
   * should add a single value to the server using the given uniqueString. This method
   * will call that function repeatedly and measure the memory use.
   *
   * This method measures the total memory use of the server (radish or redis)
   * before and after adding a certain number of entries, and computes the per entry overhead.
   * It asserts that the overhead matches the result of {@link #expectedPerEntryOverhead()} for
   * the given measurement
   *
   * @param addEntry A function that adds an entry to the server through some redis operation. The
   *        function should return the amount of actual user data added. This user data
   *        size will be subtracted out to compute the pure overhead of redis or radish
   *        data structures.
   * @param measurement Indicates what data structure we are measuring. Used to look up the expected
   *        memory usage.
   */
  private void measureAndCheckPerEntryOverhead(AddEntryFunction addEntry, Measurement measurement) {

    long expectedOverhead = expectedPerEntryOverhead().get(measurement);

    // Put some entries to make sure we initialize any constant size data structures. We are
    // just trying to measure the cost of each add entry operation.
    for (int i = 0; i < WARM_UP_ENTRY_COUNT; i++) {
      String uniqueString = String.format("warmup-%10d", i);
      addEntry.addEntryAndReturnDataSize(uniqueString);
    }

    // Perform measurements
    long baseline = getUsedMemory();
    long totalDataSize = 0;
    System.out.println("Measuring the per entry overhead for each " + measurement);
    System.out.printf("%20s, %20s, %20s", "Used Memory", "Total Mem Per Entry",
        "Overhead Per Entry\n");
    long perEntryOverhead = 0;
    for (int i = 0; i < TOTAL_ENTRY_COUNT; i++) {
      String uniqueString = String.format("%10d", i);
      totalDataSize += addEntry.addEntryAndReturnDataSize(uniqueString);
      if (i % SAMPLE_INTERVAL == (SAMPLE_INTERVAL - 1)) {
        long currentMemory = getUsedMemory() - baseline;
        long perEntryMemory = currentMemory / i;
        perEntryOverhead = (currentMemory - totalDataSize) / i;
        System.out.printf("%20d, %20d, %20d\n", currentMemory, perEntryMemory, perEntryOverhead);
      }
    }

    // These assertions compare the computed per entry overhead against result of the
    // expectedPerEntryOverhead function. Please look at that function for the expected values.
    // We are allow these values to be off by 1 byte due to rounding issues
    assertThat(perEntryOverhead).withFailMessage(
        "The overhead per %s has increased from %s to %s. Please see if you can avoid introducing additional memory overhead.",
        measurement, expectedOverhead, perEntryOverhead)
        .isLessThanOrEqualTo(expectedOverhead + ALLOWED_BYTE_DIFFERENCE);

    assertThat(perEntryOverhead).withFailMessage(
        "The overhead per %s has decreased from %s to %s. Great job! Please update the expected value in this test.",
        measurement, expectedOverhead, perEntryOverhead)
        .isCloseTo(expectedOverhead, Offset.offset(ALLOWED_BYTE_DIFFERENCE));
  }

  @FunctionalInterface
  public interface AddEntryFunction {
    /**
     * Add an entry to the server.
     *
     * @param uniqueString A unique string than can be used to generate a unique key
     *        for the entry.
     * @return the size of the user data added by the operation
     */
    long addEntryAndReturnDataSize(String uniqueString);

  }
}
