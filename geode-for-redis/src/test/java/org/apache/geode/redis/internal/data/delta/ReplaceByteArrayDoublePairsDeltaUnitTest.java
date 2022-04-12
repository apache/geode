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

import static org.apache.geode.redis.internal.data.delta.DeltaType.REPLACE_BYTE_ARRAY_DOUBLE_PAIRS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.redis.internal.data.RedisData;
import org.apache.geode.redis.internal.data.RedisSortedSet;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@RunWith(GeodeParamsRunner.class)
public class ReplaceByteArrayDoublePairsDeltaUnitTest extends AbstractRedisDeltaUnitTest {
  private final byte[] deltaBytes = "delta".getBytes();
  private final double expectedScore = 2.2;

  @Test
  public void testReplaceByteArrayDoublePairsDelta_forRedisSortedSet() throws Exception {
    DataInputStream dis = getDataInputStream();
    RedisSortedSet redisSortedSet = makeRedisSortedSet();

    redisSortedSet.fromDelta(dis);

    assertThat(redisSortedSet.zcard()).isEqualTo(1);
    assertThat(redisSortedSet.zrank(deltaBytes)).isEqualTo(0L);
    assertThat(redisSortedSet.zscore("delta".getBytes()))
        .isEqualTo(String.valueOf(expectedScore).getBytes());
  }

  @Test
  @Parameters(method = "getUnsupportedDataTypeInstancesForDelta")
  @TestCaseName("{method}: redisDataType:{0}")
  public void unsupportedDataTypesThrowException(RedisData redisData)
      throws IOException {
    final DataInputStream dis = getDataInputStream();

    assertThatThrownBy(() -> redisData.fromDelta(dis)).isInstanceOf(
        IllegalStateException.class)
        .hasMessage("unexpected " + REPLACE_BYTE_ARRAY_DOUBLE_PAIRS);
  }

  private DataInputStream getDataInputStream() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);

    RedisSortedSet.MemberMap newMemberMap = new RedisSortedSet.MemberMap(1);
    RedisSortedSet.OrderedSetEntry entry = new RedisSortedSet.OrderedSetEntry(deltaBytes,
        expectedScore);
    newMemberMap.put(deltaBytes, entry);
    ReplaceByteArrayDoublePairs source = new ReplaceByteArrayDoublePairs(newMemberMap);

    source.serializeTo(dos);

    return new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
  }

  @SuppressWarnings("unused")
  private Object[] getUnsupportedDataTypeInstancesForDelta() {
    return new Object[] {
        new Object[] {makeRedisHash()},
        new Object[] {makeRedisList()},
        new Object[] {makeRedisSet()},
        new Object[] {makeRedisString()}
    };
  }
}
