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

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import org.apache.geode.redis.GeodeRedisServerRule;

public class SetRangeIntegrationTest {

  static Jedis jedis;

  @ClassRule
  public static GeodeRedisServerRule server = new GeodeRedisServerRule();

  @BeforeClass
  public static void setUp() {
    jedis = new Jedis("localhost", server.getPort(), 10000000);
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
  public void setRange_replacesStart() {
    jedis.set("key", "0123456789");
    assertThat(jedis.setrange("key", 0, "abcd")).isEqualTo(10);
    assertThat(jedis.get("key")).isEqualTo("abcd456789");
  }

  @Test
  public void setRange_replacesMiddle() {
    jedis.set("key", "0123456789");
    assertThat(jedis.setrange("key", 3, "abc")).isEqualTo(10);
    assertThat(jedis.get("key")).isEqualTo("012abc6789");
  }

  @Test
  public void setRange_replacesEnd() {
    jedis.set("key", "0123456789");
    assertThat(jedis.setrange("key", 7, "abc")).isEqualTo(10);
    assertThat(jedis.get("key")).isEqualTo("0123456abc");
  }

  @Test
  public void setRange_extendsEnd() {
    jedis.set("key", "0123456789");
    assertThat(jedis.setrange("key", 10, "abc")).isEqualTo(13);
    assertThat(jedis.get("key")).isEqualTo("0123456789abc");
  }

  @Test
  public void setRange_extendsAndPadsWithZero() {
    jedis.set("key", "0123456789");
    assertThat(jedis.setrange("key", 11, "abc")).isEqualTo(14);
    assertThat((int) (jedis.get("key").charAt(10))).isEqualTo(0);
  }

  @Test
  public void setRange_createsKey() {
    assertThat(jedis.setrange("key", 0, "abcd")).isEqualTo(4);
    assertThat(jedis.get("key")).isEqualTo("abcd");
  }

  @Test
  public void setRange_givenSetFails() {
    jedis.sadd("key", "m1");
    assertThatThrownBy(() -> jedis.setrange("key", 0, "abc")).hasMessageContaining("WRONGTYPE");
  }


  @Test
  public void setRange_onNonExistentKey_padsBeginning() {
    assertThat(jedis.setrange("key", 2, "abc")).isEqualTo(5);
    byte[] result = jedis.get(new byte[] {'k', 'e', 'y'});
    byte[] expected = new byte[] {0, 0, 'a', 'b', 'c'};
    assertThat(result).isEqualTo(expected);
  }

}
