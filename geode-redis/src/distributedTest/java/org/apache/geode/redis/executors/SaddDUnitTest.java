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

package org.apache.geode.redis.executors;

import static org.apache.geode.distributed.ConfigurationProperties.MAX_WAIT_TIME_RECONNECT;
import static org.apache.geode.distributed.ConfigurationProperties.REDIS_BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.REDIS_PORT;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

public class SaddDUnitTest {

  @ClassRule
  public static ClusterStartupRule clusterStartUp = new ClusterStartupRule(4);

  static final String LOCAL_HOST = "127.0.0.1";
  static final int SET_SIZE = 1000;
  static int[] availablePorts;
  private static final int JEDIS_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());
  static Jedis jedis1;
  static Jedis jedis2;
  static Jedis jedis3;

  static Properties locatorProperties;
  static Properties serverProperties1;
  static Properties serverProperties2;
  static Properties serverProperties3;

  static MemberVM locator;
  static MemberVM server1;
  static MemberVM server2;
  static MemberVM server3;

  @BeforeClass
  public static void classSetup() {

    availablePorts = AvailablePortHelper.getRandomAvailableTCPPorts(3);

    locatorProperties = new Properties();
    serverProperties1 = new Properties();
    serverProperties2 = new Properties();
    serverProperties3 = new Properties();

    locatorProperties.setProperty(MAX_WAIT_TIME_RECONNECT, "15000");

    serverProperties1.setProperty(REDIS_PORT, Integer.toString(availablePorts[0]));
    serverProperties1.setProperty(REDIS_BIND_ADDRESS, LOCAL_HOST);

    serverProperties2.setProperty(REDIS_PORT, Integer.toString(availablePorts[1]));
    serverProperties2.setProperty(REDIS_BIND_ADDRESS, LOCAL_HOST);

    serverProperties3.setProperty(REDIS_PORT, Integer.toString(availablePorts[2]));
    serverProperties3.setProperty(REDIS_BIND_ADDRESS, LOCAL_HOST);

    locator = clusterStartUp.startLocatorVM(0, locatorProperties);
    server1 = clusterStartUp.startServerVM(1, serverProperties1, locator.getPort());
    server2 = clusterStartUp.startServerVM(2, serverProperties2, locator.getPort());
    server3 = clusterStartUp.startServerVM(3, serverProperties3, locator.getPort());

    jedis1 = new Jedis(LOCAL_HOST, availablePorts[0], JEDIS_TIMEOUT);
    jedis2 = new Jedis(LOCAL_HOST, availablePorts[1], JEDIS_TIMEOUT);
    jedis3 = new Jedis(LOCAL_HOST, availablePorts[2], JEDIS_TIMEOUT);
  }

  @Before
  public void testSetup() {
    jedis1.flushAll();
  }

  @AfterClass
  public static void tearDown() {
    jedis1.disconnect();
    jedis2.disconnect();
    jedis3.disconnect();

    server1.stop();
    server2.stop();
    server3.stop();
  }

  @Test
  public void should_distributeDataAmongMultipleServers_givenMultipleClients() {

    String key = "key";

    List<String> members = makeMemberList(SET_SIZE, "member1-");

    jedis1.sadd(key, members.toArray(new String[] {}));

    Set<String> result = jedis2.smembers(key);

    assertThat(result.toArray()).containsExactlyInAnyOrder(members.toArray());

  }

  @Test
  public void should_distributeDataAmongMultipleServers_givenMultipleClients_AddingDifferentDataToSameSetConcurrently() {

    String key = "key";

    List<String> members1 = makeMemberList(SET_SIZE, "member1-");
    List<String> members2 = makeMemberList(SET_SIZE, "member2-");

    List<String> allMembers = new ArrayList<>();
    allMembers.addAll(members1);
    allMembers.addAll(members2);

    new ConcurrentLoopingThreads(SET_SIZE,
        (i) -> jedis1.sadd(key, members1.get(i)),
        (i) -> jedis2.sadd(key, members2.get(i))).run();

    Set<String> results = jedis3.smembers(key);

    assertThat(results.toArray()).containsExactlyInAnyOrder(allMembers.toArray());
  }

  @Test
  public void should_distributeDataAmongMultipleServers_givenMultipleClients_AddingSameDataToSameSetConcurrently() {

    String key = "key";

    List<String> members = makeMemberList(SET_SIZE, "member-");

    new ConcurrentLoopingThreads(SET_SIZE,
        (i) -> jedis1.sadd(key, members.get(i)),
        (i) -> jedis2.sadd(key, members.get(i))).run();

    Set<String> results = jedis3.smembers(key);

    assertThat(results.toArray()).containsExactlyInAnyOrder(members.toArray());

  }

  @Test
  public void should_distributeDataAmongMultipleServers_givenMultipleClients_AddingDifferentSetsConcurrently() {

    String key1 = "key1";
    String key2 = "key2";

    List<String> members1 = makeMemberList(SET_SIZE, "member1-");
    List<String> members2 = makeMemberList(SET_SIZE, "member2-");

    new ConcurrentLoopingThreads(SET_SIZE,
        (i) -> jedis1.sadd(key1, members1.get(i)),
        (i) -> jedis2.sadd(key2, members2.get(i))).run();

    Set<String> results1 = jedis3.smembers(key1);
    Set<String> results2 = jedis3.smembers(key2);

    assertThat(results1.toArray()).containsExactlyInAnyOrder(members1.toArray());
    assertThat(results2.toArray()).containsExactlyInAnyOrder(members2.toArray());

  }

  @Test
  public void should_distributeDataAmongMultipleServers_givenTwoSetsOfClients_OperatingOnTheSameSetConcurrently() {

    Jedis jedis1B = new Jedis(LOCAL_HOST, availablePorts[0]);
    Jedis jedis2B = new Jedis(LOCAL_HOST, availablePorts[1]);

    String key = "key";

    List<String> members = makeMemberList(SET_SIZE, "member1-");

    new ConcurrentLoopingThreads(SET_SIZE,
        (i) -> jedis1.sadd(key, members.get(i)),
        (i) -> jedis1B.sadd(key, members.get(i)),
        (i) -> jedis2.sadd(key, members.get(i)),
        (i) -> jedis2B.sadd(key, members.get(i))).run();

    Set<String> results = jedis3.smembers(key);

    assertThat(results.toArray()).containsExactlyInAnyOrder(members.toArray());

    jedis1B.disconnect();
    jedis2B.disconnect();
  }

  @Test
  public void should_distributeDataAmongMultipleServers_givenTwoSetsOfClients_OperatingOnTheSameSet_withDifferentData_Concurrently() {

    Jedis jedis1B = new Jedis(LOCAL_HOST, availablePorts[0]);
    Jedis jedis2B = new Jedis(LOCAL_HOST, availablePorts[1]);

    String key = "key1";

    List<String> members1 = makeMemberList(SET_SIZE, "member1-");
    List<String> members2 = makeMemberList(SET_SIZE, "member2-");

    List<String> allMembers = new ArrayList<>();
    allMembers.addAll(members1);
    allMembers.addAll(members2);

    new ConcurrentLoopingThreads(SET_SIZE,
        (i) -> jedis1.sadd(key, members1.get(i)),
        (i) -> jedis1B.sadd(key, members1.get(i)),
        (i) -> jedis2.sadd(key, members2.get(i)),
        (i) -> jedis2B.sadd(key, members2.get(i))).run();

    Set<String> results = jedis3.smembers(key);

    assertThat(results.toArray()).containsExactlyInAnyOrder(allMembers.toArray());

    jedis1B.disconnect();
    jedis2B.disconnect();
  }

  private List<String> makeMemberList(int setSize, String baseString) {
    List<String> members = new ArrayList<>();
    for (int i = 0; i < setSize; i++) {
      members.add(baseString + i);
    }
    return members;
  }
}
