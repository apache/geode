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

package org.apache.geode.redis.internal.data;

import static org.apache.geode.redis.internal.RegionProvider.REDIS_SLOTS;
import static org.apache.geode.redis.internal.RegionProvider.REDIS_SLOTS_PER_BUCKET;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import org.apache.geode.redis.internal.executor.cluster.CRC16;

public class ByteArrayWrapperJUnitTest {

  @Test
  public void testRoutingId_withHashtags() {
    ByteArrayWrapper baw = new ByteArrayWrapper("name{user1000}".getBytes());
    assertThat(baw.getRoutingId()).isEqualTo(calculateRoutingId("user1000"));

    baw = new ByteArrayWrapper("{user1000".getBytes());
    assertThat(baw.getRoutingId()).isEqualTo(calculateRoutingId("{user1000"));

    baw = new ByteArrayWrapper("}user1000{".getBytes());
    assertThat(baw.getRoutingId()).isEqualTo(calculateRoutingId("}user1000{"));

    baw = new ByteArrayWrapper("user{}1000".getBytes());
    assertThat(baw.getRoutingId()).isEqualTo(calculateRoutingId("user{}1000"));

    baw = new ByteArrayWrapper("user}{1000".getBytes());
    assertThat(baw.getRoutingId()).isEqualTo(calculateRoutingId("user}{1000"));

    baw = new ByteArrayWrapper("{user1000}}bar".getBytes());
    assertThat(baw.getRoutingId()).isEqualTo(calculateRoutingId("user1000"));

    baw = new ByteArrayWrapper("foo{user1000}{bar}".getBytes());
    assertThat(baw.getRoutingId()).isEqualTo(calculateRoutingId("user1000"));

    baw = new ByteArrayWrapper("foo{}{user1000}".getBytes());
    assertThat(baw.getRoutingId()).isEqualTo(calculateRoutingId("foo{}{user1000}"));

    baw = new ByteArrayWrapper("{}{user1000}".getBytes());
    assertThat(baw.getRoutingId()).isEqualTo(calculateRoutingId("{}{user1000}"));

    baw = new ByteArrayWrapper("foo{{user1000}}bar".getBytes());
    assertThat(baw.getRoutingId()).isEqualTo(calculateRoutingId("{user1000"));
  }

  private int calculateRoutingId(String data) {
    return (CRC16.calculate(data.getBytes(), 0, data.length()) % REDIS_SLOTS)
        / REDIS_SLOTS_PER_BUCKET;
  }
}
