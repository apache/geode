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

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.GeodeRedisServerRule;

public class AppendIntegrationTest {

  static Jedis jedis;
  static Jedis jedis2;

  @ClassRule
  public static GeodeRedisServerRule server = new GeodeRedisServerRule();

  @BeforeClass
  public static void setUp() {
    jedis = new Jedis("localhost", server.getPort(), 10000000);
    jedis2 = new Jedis("localhost", server.getPort(), 10000000);
  }

  @After
  public void flushAll() {
    jedis.flushAll();
  }

  @AfterClass
  public static void tearDown() {
    jedis.close();
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
  public void testAppend_concurrent() {
    int listSize = 1000;
    String key = "key";

    List<String> values1 = makeStringList(listSize, "values1-");
    List<String> values2 = makeStringList(listSize, "values2-");

    new ConcurrentLoopingThreads(listSize,
        (i) -> jedis.append(key, values1.get(i)),
        (i) -> jedis2.append(key, values2.get(i))).run();

    for (int i = 0; i < listSize; i++) {
      assertThat(jedis.get(key)).contains(values1.get(i));
      assertThat(jedis.get(key)).contains(values2.get(i));
    }
  }

  @Test
  public void testAppend_withBinaryKeyAndValue() {
    byte[] blob = new byte[256];
    byte[] doubleBlob = new byte[512];
    for (int i = 0; i < 256; i++) {
      blob[i] = (byte) i;
      doubleBlob[i] = (byte) i;
      doubleBlob[i + 256] = (byte) i;
    }

    jedis.set(blob, blob);
    jedis.append(blob, blob);
    byte[] result = jedis.get(blob);

    assertThat(result).isEqualTo(doubleBlob);
  }

  private String randString() {
    return Long.toHexString(Double.doubleToLongBits(Math.random()));
  }

  private List<String> makeStringList(int setSize, String baseString) {
    List<String> strings = new ArrayList<>();
    for (int i = 0; i < setSize; i++) {
      strings.add(baseString + i);
    }
    return strings;
  }
}
