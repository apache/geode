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

package org.apache.geode.redis.internal.commands.executor.cluster;

import static org.apache.geode.redis.internal.netty.Coder.stringToBytes;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;
import redis.clients.jedis.util.JedisClusterCRC16;


public class CRC16JUnitTest {

  @Test
  public void testBasicCRC16_sameAsRedis() {
    byte[] data = new byte[] {0};
    assertThat(CRC16.calculate(data, 0, 1))
        .isEqualTo((short) JedisClusterCRC16.getCRC16(data));

    data = new byte[] {1};
    assertThat(CRC16.calculate(new byte[] {1}, 0, 1))
        .isEqualTo((short) JedisClusterCRC16.getCRC16(data));

    data = stringToBytes("123456789");
    assertThat(CRC16.calculate(data, 0, data.length))
        .isEqualTo((short) JedisClusterCRC16.getCRC16(data));

    data = stringToBytes("---123456789---");
    assertThat(CRC16.calculate(data, 3, 12))
        .isEqualTo((short) JedisClusterCRC16.getCRC16(data, 3, 12));

    data = stringToBytes("abcdefghijklmnopqrstuvwxyz");
    assertThat(CRC16.calculate(data, 0, data.length))
        .isEqualTo((short) JedisClusterCRC16.getCRC16(data));

    data = stringToBytes("user1000");
    assertThat(CRC16.calculate(data, 0, data.length))
        .isEqualTo((short) JedisClusterCRC16.getCRC16(data));
  }

}
