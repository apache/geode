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

package org.apache.geode.redis.internal.data.delta;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.redis.internal.data.RedisSortedSet;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@RunWith(GeodeParamsRunner.class)
public class ReplaceByteArrayDoublePairsDeltaUnitTest {
  @Test
  public void testReplaceByteArrayDoublePairsDelta() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);

    RedisSortedSet.MemberMap newMemberMap = new RedisSortedSet.MemberMap(1);
    byte[] deltaBytes = "delta".getBytes();
    RedisSortedSet.OrderedSetEntry entry = new RedisSortedSet.OrderedSetEntry(deltaBytes, 2.0);
    newMemberMap.put(deltaBytes, entry);
    ReplaceByteArrayDoublePairs source = new ReplaceByteArrayDoublePairs(newMemberMap);

    source.serializeTo(dos);

    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    RedisSortedSet redisSortedSet = makeRedisSortedSet();
    redisSortedSet.fromDelta(dis);

    assertThat(redisSortedSet.zcard()).isEqualTo(1);
    assertThat(redisSortedSet.zrank(deltaBytes)).isEqualTo(0L);
  }

  private RedisSortedSet makeRedisSortedSet() {
    RedisSortedSet redisSortedSet = new RedisSortedSet(3);
    redisSortedSet.memberAdd("alpha".getBytes(), 1.0d);
    redisSortedSet.memberAdd("beta".getBytes(), 2.0d);
    redisSortedSet.memberAdd("gamma".getBytes(), 4.0d);
    return redisSortedSet;
  }
}
