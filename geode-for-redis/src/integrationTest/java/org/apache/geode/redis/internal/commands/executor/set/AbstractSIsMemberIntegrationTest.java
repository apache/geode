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
package org.apache.geode.redis.internal.commands.executor.set;

import static org.apache.geode.redis.RedisCommandArgumentsTestHelper.assertExactNumberOfArgs;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_WRONG_TYPE;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.RedisIntegrationTest;

public abstract class AbstractSIsMemberIntegrationTest implements RedisIntegrationTest {
  private JedisCluster jedis;
  private static final String setKey = "{user1}setkey";
  private static final String[] setMembers = {"one", "two", "three", "four", "five"};

  @Before
  public void setUp() {
    jedis = new JedisCluster(new HostAndPort(BIND_ADDRESS, getPort()), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void tearDown() {
    flushAll();
    jedis.close();
  }

  @Test
  public void sismemberWrongNumberOfArguments_returnsError() {
    assertExactNumberOfArgs(jedis, Protocol.Command.SISMEMBER, 2);
  }

  @Test
  public void sismemberValidKeyValidMember_returnTrue() {
    jedis.sadd(setKey, setMembers);

    for (String member : setMembers) {
      assertThat(jedis.sismember(setKey, member)).isTrue();
    }
  }

  @Test
  public void sismemberValidKeyNonExistingMember_returnFalse() {
    jedis.sadd(setKey, setMembers);
    assertThat(jedis.sismember(setKey, "nonExistentMember")).isFalse();
  }

  @Test
  public void sismemberNonExistingKeyNonExistingMember_returnFalse() {
    assertThat(jedis.sismember("nonExistentKey", "nonExistentMember")).isFalse();
  }

  @Test
  public void sismemberMemberInAnotherSet_returnFalse() {
    String member = "elephant";
    jedis.sadd("diffSet", member);
    jedis.sadd(setKey, setMembers);

    assertThat(jedis.sismember(setKey, member)).isFalse();
  }

  @Test
  public void sismemberNonExistingKeyAndMemberInAnotherSet_returnFalse() {
    jedis.sadd(setKey, setMembers);

    for (String member : setMembers) {
      assertThat(jedis.sismember("nonExistentKey", member)).isFalse();
    }
  }


  @Test
  public void sismemberAfterSadd_returnsTrue() {
    String newMember = "chicken";
    jedis.sadd(setKey, setMembers);
    assertThat(jedis.sismember(setKey, newMember)).isFalse();
    jedis.sadd(setKey, newMember);
    assertThat(jedis.sismember(setKey, newMember)).isTrue();
  }

  @Test
  public void scardWithWrongKeyType_returnsWrongTypeError() {
    String keyString = "keys";
    String valueString = "alicia";
    jedis.set(keyString, valueString);
    assertThatThrownBy(() -> jedis.sismember(keyString, valueString))
        .hasMessageContaining(ERROR_WRONG_TYPE);
  }
}
