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

import static org.apache.geode.redis.internal.data.delta.DeltaType.REMOVE_BYTE_ARRAYS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.redis.internal.data.RedisData;
import org.apache.geode.redis.internal.data.RedisHash;
import org.apache.geode.redis.internal.data.RedisSet;
import org.apache.geode.redis.internal.data.RedisSortedSet;
import org.apache.geode.redis.internal.data.RedisString;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@RunWith(GeodeParamsRunner.class)
public class RemoveByteArraysDeltaUnitTest extends AbstractRedisDeltaUnitTest {
  @Test
  public void testRemoveByteArraysDeltaForRedisSet() throws Exception {
    RemoveByteArrays source =
        new RemoveByteArrays(Arrays.asList("zero".getBytes(), "two".getBytes()));
    DataInputStream dis = getDataInputStream(source);
    RedisSet redisSet = makeRedisSet();

    redisSet.fromDelta(dis);

    assertThat(redisSet.scard()).isEqualTo(1);
    assertThat(redisSet.sismember("one".getBytes())).isTrue();
  }

  @Test
  public void testRemoveByteArraysDeltaForRedisSortedSet() throws Exception {
    RemoveByteArrays source =
        new RemoveByteArrays(Arrays.asList("alpha".getBytes(), "gamma".getBytes()));
    DataInputStream dis = getDataInputStream(source);
    RedisSortedSet redisSortedSet = makeRedisSortedSet();

    redisSortedSet.fromDelta(dis);

    assertThat(redisSortedSet.zcard()).isEqualTo(1);
    assertThat(redisSortedSet.zrank("beta".getBytes())).isEqualTo(0L);
  }

  @Test
  public void testRemoveByteArraysDeltaForRedisHash() throws Exception {
    RemoveByteArrays source =
        new RemoveByteArrays(Collections.singletonList("zero".getBytes()));
    DataInputStream dis = getDataInputStream(source);
    RedisHash redisHash = makeRedisHash();

    redisHash.fromDelta(dis);

    assertThat(redisHash.hlen()).isEqualTo(2);
    assertThat(redisHash.hget("zero".getBytes())).isNull();
  }

  @Test
  @Parameters(method = "getDataTypeInstances")
  @TestCaseName("{method}: redisDataType:{0}")
  public void unsupportedDataTypesThrowException(RedisData redisData)
      throws IOException {
    RemoveByteArrays source =
        new RemoveByteArrays(Collections.singletonList("zero".getBytes()));
    final DataInputStream dis = getDataInputStream(source);

    assertThatThrownBy(() -> redisData.fromDelta(dis)).isInstanceOf(
        IllegalStateException.class)
        .hasMessageContaining("unexpected " + REMOVE_BYTE_ARRAYS);
  }

  private DataInputStream getDataInputStream(RemoveByteArrays source) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);

    source.serializeTo(dos);

    return new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
  }

  @SuppressWarnings("unused")
  private Object[] getDataTypeInstances() {
    return new Object[] {
        new Object[] {makeRedisList()},
        new Object[] {new RedisString()}
    };
  }
}
