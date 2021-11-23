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
package org.apache.geode.redis.internal.services.cluster;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.io.IOException;

import org.junit.Test;

import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.serialization.ByteArrayDataInput;

public class RedisMemberInfoTest {

  @Test
  public void constructor_setsValues() {
    DistributedMember mockDistributedMember = mock(DistributedMember.class);
    String hostAddress = "127.0.0.1";
    int redisPort = 12345;

    RedisMemberInfo info = new RedisMemberInfo(mockDistributedMember, hostAddress, redisPort);

    assertThat(info.getMember()).isEqualTo(mockDistributedMember);
    assertThat(info.getHostAddress()).isEqualTo(hostAddress);
    assertThat(info.getRedisPort()).isEqualTo(redisPort);
  }

  @Test
  public void serialization_isStable() throws IOException, ClassNotFoundException {
    DistributedMember distributedMember = new InternalDistributedMember("hostName", 123);
    String hostAddress = "127.0.0.1";
    int redisPort = 12345;

    RedisMemberInfo info1 = new RedisMemberInfo(distributedMember, hostAddress, redisPort);

    HeapDataOutputStream out = new HeapDataOutputStream(100);
    DataSerializer.writeObject(info1, out);
    ByteArrayDataInput in = new ByteArrayDataInput(out.toByteArray());

    RedisMemberInfo info2 = DataSerializer.readObject(in);
    assertThat(info2.getMember()).isEqualTo(info1.getMember());
    assertThat(info2.getHostAddress()).isEqualTo(info1.getHostAddress());
    assertThat(info2.getRedisPort()).isEqualTo(info1.getRedisPort());
  }

  @Test
  public void equals_returnsTrue_givenEquivalentInfo() {
    DistributedMember mockDistributedMember = mock(DistributedMember.class);
    String hostAddress = "127.0.0.1";
    int redisPort = 12345;

    RedisMemberInfo info1 = new RedisMemberInfo(mockDistributedMember, hostAddress, redisPort);
    RedisMemberInfo info2 = new RedisMemberInfo(mockDistributedMember, hostAddress, redisPort);

    assertThat(info1).isNotSameAs(info2);
    assertThat(info1.equals(info2)).isTrue();
    assertThat(info2.equals(info1)).isTrue();
  }

  @Test
  public void equals_returnsFalse_givenNonEquivalentInfo() {
    DistributedMember mockDistributedMember1 = mock(DistributedMember.class);
    String hostAddress1 = "127.0.0.1";
    int redisPort1 = 12345;

    DistributedMember mockDistributedMember2 = mock(DistributedMember.class);
    String hostAddress2 = "1.1.1.0";
    int redisPort2 = 54321;

    RedisMemberInfo info1 = new RedisMemberInfo(mockDistributedMember1, hostAddress1, redisPort1);
    RedisMemberInfo info2 = new RedisMemberInfo(mockDistributedMember2, hostAddress2, redisPort2);

    assertThat(info1.equals(info2)).isFalse();
    assertThat(info2.equals(info1)).isFalse();
  }
}
