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
package org.apache.geode.redis.internal.executor.set;

import static org.apache.geode.redis.RedisCommandArgumentsTestHelper.assertExactNumberOfArgs;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;

import org.apache.geode.management.internal.cli.util.ThreePhraseGenerator;
import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public abstract class AbstractSIsMemberIntegrationTest implements RedisIntegrationTest {
  private JedisCluster jedis;
  private static final ThreePhraseGenerator generator = new ThreePhraseGenerator();
  private static final int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

  @Before
  public void setUp() {
    jedis = new JedisCluster(new HostAndPort("localhost", getPort()), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void tearDown() {
    flushAll();
    jedis.close();
  }

  @Test
  public void errors_givenWrongNumberOfArguments() {
    assertExactNumberOfArgs(jedis, Protocol.Command.SISMEMBER, 2);
  }

  @Test
  public void testSMembersSIsMember() {
    int elements = 10;
    String key = generator.generate('x');
    String[] strings = generateStrings(elements, 'y');
    jedis.sadd(key, strings);

    for (String entry : strings) {
      assertThat(jedis.sismember(key, entry)).isTrue();
    }
  }

  @Test
  public void testSIsMemberWithNonexistentKey_returnsFalse() {
    assertThat(jedis.sismember("nonExistentKey", "nonExistentMember")).isFalse();
  }

  @Test
  public void testSIsMemberWithNonexistentMember_returnsFalse() {
    String key = "key";

    jedis.sadd("key", "member1", "member2");
    assertThat(jedis.sismember(key, "nonExistentMember")).isFalse();
  }

  private String[] generateStrings(int elements, char uniqueElement) {
    Set<String> strings = new HashSet<>();
    for (int i = 0; i < elements; i++) {
      String elem = generator.generate(uniqueElement);
      strings.add(elem);
    }
    return strings.toArray(new String[strings.size()]);
  }

}
