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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public abstract class AbstractMSetNXIntegrationTest implements RedisIntegrationTest {

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
  public void givenKeyNotProvided_returnsWrongNumberOfArgumentsError() {
    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.MSETNX))
        .hasMessageContaining("ERR wrong number of arguments for 'msetnx' command");
  }

  @Test
  public void givenValueNotProvided_returnsWrongNumberOfArgumentsError() {
    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.MSETNX, "key"))
        .hasMessageContaining("ERR wrong number of arguments for 'msetnx' command");
  }

  @Test
  public void givenEvenNumberOfArgumentsProvided_returnsWrongNumberOfArgumentsError() {
    // Redis returns this message in this scenario: "ERR wrong number of arguments for MSET"
    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.MSETNX, "key1", "value1", "key2",
        "value2", "key3"))
            .hasMessageContaining("ERR wrong number of arguments");
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

  private String randString() {
    return Long.toHexString(Double.doubleToLongBits(Math.random()));
  }
}
