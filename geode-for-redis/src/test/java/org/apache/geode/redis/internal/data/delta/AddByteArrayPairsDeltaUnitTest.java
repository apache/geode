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

import static org.apache.geode.redis.internal.data.delta.DeltaType.ADD_BYTE_ARRAY_PAIRS;
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
import org.apache.geode.redis.internal.data.RedisHash;
import org.apache.geode.redis.internal.data.RedisString;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@RunWith(GeodeParamsRunner.class)
public class AddByteArrayPairsDeltaUnitTest extends AbstractRedisDeltaUnitTest {
  private final byte[] original = "0123456789".getBytes();
  private final byte[] payload = "something amazing I guess".getBytes();

  @Test
  public void testAddByteArrayPairsDelta() throws Exception {
    DataInputStream dis = getDataInputStream();
    RedisHash redisHash = makeRedisHash();
    redisHash.fromDelta(dis);

    assertThat(redisHash.hlen()).isEqualTo(4);
    assertThat(redisHash.hget(original)).isEqualTo(payload);
  }

  @Test
  @Parameters(method = "getDataTypeInstances")
  @TestCaseName("{method}: redisDataType:{0}")
  public void unsupportedDataTypesThrowException_forAddByteArrayDoublePairsDelta(
      RedisData redisData)
      throws IOException {
    final DataInputStream dis = getDataInputStream();

    assertThatThrownBy(() -> redisData.fromDelta(dis)).isInstanceOf(
        IllegalStateException.class)
        .hasMessageContaining("unexpected " + ADD_BYTE_ARRAY_PAIRS);
  }

  private DataInputStream getDataInputStream() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    AddByteArrayPairs source = new AddByteArrayPairs(original, payload);

    source.serializeTo(dos);

    return new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
  }

  @SuppressWarnings("unused")
  private Object[] getDataTypeInstances() {
    return new Object[] {
        new Object[] {makeRedisList()},
        new Object[] {makeRedisSet()},
        new Object[] {makeRedisSortedSet()},
        new Object[] {new RedisString()}
    };
  }
}
