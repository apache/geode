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

import static org.apache.geode.redis.internal.data.delta.DeltaType.ADD_BYTE_ARRAYS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.redis.internal.data.RedisData;
import org.apache.geode.redis.internal.data.RedisList;
import org.apache.geode.redis.internal.data.RedisSet;
import org.apache.geode.redis.internal.data.RedisString;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@RunWith(GeodeParamsRunner.class)
public class AddByteArraysDeltaUnitTest extends AbstractRedisDeltaUnitTest {
  @Test
  public void testAddByteArraysDelta_forRedisList() throws Exception {
    RedisList redisList = makeRedisList();
    DataInputStream dis = getDataInputStream(redisList.getVersion() + 1);

    redisList.fromDelta(dis);

    assertThat(redisList.llen()).isEqualTo(5);
    assertThat(redisList.lindex(0)).isEqualTo("firstNew".getBytes());
    assertThat(redisList.lindex(1)).isEqualTo("secondNew".getBytes());
    assertThat(redisList.lindex(2)).isEqualTo("zero".getBytes());
  }

  @Test
  public void testAddByteArraysDelta_forRedisSet() throws Exception {
    RedisSet redisSet = makeRedisSet();
    DataInputStream dis = getDataInputStream(redisSet.getVersion() + 1);
    redisSet.fromDelta(dis);

    assertThat(redisSet.scard()).isEqualTo(5);
    assertThat(redisSet.sismember("firstNew".getBytes())).isTrue();
    assertThat(redisSet.sismember("secondNew".getBytes())).isTrue();
  }

  @Test
  @Parameters(method = "getDataTypeInstances")
  @TestCaseName("{method}: redisDataType:{0}")
  public void unsupportedDataTypesThrowException_forAddByteArraysDelta(RedisData redisData)
      throws IOException {
    final DataInputStream dis = getDataInputStream(1);

    assertThatThrownBy(() -> redisData.fromDelta(dis)).isInstanceOf(
        IllegalStateException.class)
        .hasMessageContaining("unexpected " + ADD_BYTE_ARRAYS);
  }

  private DataInputStream getDataInputStream(int version) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    List<byte[]> deltaList = new ArrayList<>();
    deltaList.add("secondNew".getBytes());
    deltaList.add("firstNew".getBytes());

    AddByteArrays source = new AddByteArrays(deltaList, (byte) (version));
    source.serializeTo(dos);
    return new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
  }

  @SuppressWarnings("unused")
  private Object[] getDataTypeInstances() {
    return new Object[] {
        new Object[] {makeRedisHash()},
        new Object[] {makeRedisSortedSet()},
        new Object[] {new RedisString()}
    };
  }
}
