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

import static org.apache.geode.distributed.ConfigurationProperties.REDIS_PORT;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import io.lettuce.core.resource.ClientResources;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.redis.session.springRedisTestApplication.config.DUnitSocketAddressResolver;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class HSetNXDunitTest {
  @ClassRule
  public static RedisClusterStartupRule cluster = new RedisClusterStartupRule();

  @ClassRule
  public static ExecutorServiceRule executor = new ExecutorServiceRule();

  private static MemberVM locator;
  private static MemberVM server1;
  private static MemberVM server2;
  private static RedisAdvancedClusterCommands<String, String> lettuce;
  private static StatefulRedisClusterConnection<String, String> connection;
  private static ClientResources resources;

  @BeforeClass
  public static void classSetup() {
    int[] redisPorts = AvailablePortHelper.getRandomAvailableTCPPorts(3);

    String redisPort1 = "" + redisPorts[0];
    String redisPort2 = "" + redisPorts[1];

    locator = cluster.startLocatorVM(0);

    server1 = startRedisVM(1, redisPorts[0]);
    server2 = startRedisVM(2, redisPorts[1]);

    DUnitSocketAddressResolver dnsResolver =
        new DUnitSocketAddressResolver(new String[] {redisPort2, redisPort1});

    resources = ClientResources.builder()
        .socketAddressResolver(dnsResolver)
        .build();

    RedisClusterClient redisClient = RedisClusterClient.create(resources, "redis://localhost");
    redisClient.setOptions(ClusterClientOptions.builder()
        .autoReconnect(true)
        .build());
    connection = redisClient.connect();
    lettuce = connection.sync();
  }

  @Before
  public void testSetup() {
    lettuce.flushall();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    resources.shutdown().get();
    connection.close();
    server1.stop();
    server2.stop();
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

  private static MemberVM startRedisVM(int vmID, int redisPort) {
    int locatorPort = locator.getPort();

    return cluster.startRedisVM(vmID, x -> x
        .withConnectionToLocator(locatorPort)
        .withProperty(REDIS_PORT, "" + redisPort));
  }
}
