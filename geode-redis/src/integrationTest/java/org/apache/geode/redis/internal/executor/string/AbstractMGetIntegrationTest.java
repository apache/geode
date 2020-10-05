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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import org.apache.geode.test.dunit.rules.RedisPortSupplier;

public abstract class AbstractMGetIntegrationTest implements RedisPortSupplier {

  private Jedis jedis;

  @Before
  public void setUp() {
    jedis = new Jedis("localhost", getPort(), 10000000);
  }

  @After
  public void tearDown() {
    jedis.flushAll();
    jedis.close();
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

    assertThat(jedis.mget(keys)).containsExactly(expectedVals);
  }
}
