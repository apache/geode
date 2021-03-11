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

package org.apache.geode.redis.internal.executor.cluster;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class CRC16JUnitTest {

  @Test
  public void testBasicCRC16_sameAsRedis() {
    assertThat(CRC16.calculate(new byte[] {0}, 0, 1)).isEqualTo(0);
    assertThat(CRC16.calculate(new byte[] {1}, 0, 1)).isEqualTo(0x1021);
    assertThat(CRC16.calculate("123456789".getBytes(), 0, 9)).isEqualTo(0x31c3);
    assertThat(CRC16.calculate("---123456789---".getBytes(), 3, 12)).isEqualTo(0x31c3);
    assertThat(CRC16.calculate("abcdefghijklmnopqrstuvwxyz".getBytes(), 0, 26)).isEqualTo(0x63ac);
    assertThat(CRC16.calculate("user1000".getBytes(), 0, 8)).isEqualTo(0x4d73);
  }

}
