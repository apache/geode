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

package org.apache.geode.redis.internal.commands.executor.hash;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class HSetNXDunitTest {
  @ClassRule
  public static RedisClusterStartupRule cluster = new RedisClusterStartupRule();

  @ClassRule
  public static ExecutorServiceRule executor = new ExecutorServiceRule();

  private static RedisAdvancedClusterCommands<String, String> lettuce;
  private static RedisClusterClient clusterClient;

  @BeforeClass
  public static void classSetup() {
    MemberVM locator = cluster.startLocatorVM(0);

    cluster.startRedisVM(1, locator.getPort());
    cluster.startRedisVM(2, locator.getPort());

    int redisServerPort1 = cluster.getRedisPort(1);
    clusterClient = RedisClusterClient.create("redis://localhost:" + redisServerPort1);

    lettuce = clusterClient.connect().sync();
  }

  @AfterClass
  public static void cleanup() {
    clusterClient.shutdown();
  }

  @Before
  public void testSetup() {
    cluster.flushAll();
  }

  @Test
  public void testHSETNXReturnsOneWhenKeyDoesNotExistAndZeroWhenItDoes()
      throws ExecutionException, InterruptedException {
    String key = "HSETNX";

    for (int i = 0; i < 1000; i++) {
      int local_i = i;

      Future<Boolean> server_1_counter = executor.submit(
          () -> lettuce.hsetnx(key, "field" + local_i, "value" + local_i));
      Future<Boolean> server_2_counter = executor.submit(
          () -> lettuce.hsetnx(key, "field" + local_i, "value" + local_i));

      assertThat(server_1_counter.get() ^ server_2_counter.get()).isTrue();
    }
  }

}
