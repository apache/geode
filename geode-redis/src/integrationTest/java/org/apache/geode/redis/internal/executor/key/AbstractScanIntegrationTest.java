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

package org.apache.geode.redis.internal.executor.key;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.RedisPortSupplier;

public abstract class AbstractScanIntegrationTest implements RedisPortSupplier {

  private Jedis jedis;
  private static final int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

  @Before
  public void setUp() {
    jedis = new Jedis("localhost", getPort(), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void tearDown() {
    jedis.flushAll();
    jedis.close();
  }

  @Test
  public void givenOneKey_scanReturnsKey() {
    jedis.set("a", "1");
    ScanResult<String> result = jedis.scan("0");

    assertThat(result.isCompleteIteration()).isTrue();
    assertThat(result.getResult()).containsExactly("a");
  }

  @Test
  public void givenEmptyDatabase_scanReturnsEmptyResult() {
    ScanResult<String> result = jedis.scan("0");

    assertThat(result.isCompleteIteration()).isTrue();
    assertThat(result.getResult()).isEmpty();
  }

  @Test
  public void givenMultipleKeys_scanReturnsAllKeys() {
    jedis.set("a", "1");
    jedis.sadd("b", "green", "orange");
    jedis.hset("c", "potato", "sweet");
    ScanResult<String> result = jedis.scan("0");

    assertThat(result.isCompleteIteration()).isTrue();
    assertThat(result.getResult()).containsExactlyInAnyOrder("a", "b", "c");
  }

  @Test
  public void givenMultipleKeysWithCount_scanReturnsAllKeysWithoutDuplicates() {
    jedis.set("a", "1");
    jedis.sadd("b", "green", "orange");
    jedis.hset("c", "potato", "sweet");
    ScanParams scanParams = new ScanParams();
    scanParams.count(1);

    ScanResult<String> result = jedis.scan("0", scanParams);
    List<String> allKeysFromScan = new ArrayList<>();
    allKeysFromScan.addAll(result.getResult());

    while (!result.isCompleteIteration()) {
      result = jedis.scan(result.getCursor(), scanParams);
      allKeysFromScan.addAll(result.getResult());
    }

    assertThat(result.isCompleteIteration()).isTrue();
    assertThat(allKeysFromScan).containsExactlyInAnyOrder("a", "b", "c");
  }

  @Test
  public void givenMultipleKeysWithMatch_scanReturnsAllMatchingKeysWithoutDuplicates() {
    jedis.set("a", "1");
    jedis.sadd("b", "green", "orange");
    jedis.hset("c", "potato", "sweet");
    ScanParams scanParams = new ScanParams();
    scanParams.match("a*");

    ScanResult<String> result = jedis.scan("0", scanParams);

    assertThat(result.isCompleteIteration()).isTrue();
    assertThat(result.getResult()).containsExactly("a");
  }
}
