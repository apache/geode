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

import static org.apache.geode.redis.internal.data.delta.DeltaType.ADD_BYTE_ARRAYS_TAIL;
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
import org.apache.geode.redis.internal.data.RedisString;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@RunWith(GeodeParamsRunner.class)
public class AddByteArraysTailDeltaUnitTest extends AbstractRedisDeltaUnitTest {
  @Test
  public void testAddByteArraysTailDelta() throws Exception {
    RedisList redisList = makeRedisList();
    DataInputStream dis = getDataInputStream(redisList.getVersion() + 1);
    redisList.fromDelta(dis);

    assertThat(redisList.llen()).isEqualTo(5);
    assertThat(redisList.lindex(2)).isEqualTo("two".getBytes());
    assertThat(redisList.lindex(3)).isEqualTo("firstNew".getBytes());
    assertThat(redisList.lindex(4)).isEqualTo("secondNew".getBytes());
  }

  @Test
  @Parameters(method = "getDataTypeInstances")
  @TestCaseName("{method}: redisDataType:{0}")
  public void unsupportedDataTypesThrowException(RedisData redisData)
      throws IOException {
    final DataInputStream dis = getDataInputStream(1);

    assertThatThrownBy(() -> redisData.fromDelta(dis)).isInstanceOf(
        IllegalStateException.class)
        .hasMessageContaining("unexpected " + ADD_BYTE_ARRAYS_TAIL);
  }

  private DataInputStream getDataInputStream(int version) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    List<byte[]> deltaList = new ArrayList<>();
    deltaList.add("firstNew".getBytes());
    deltaList.add("secondNew".getBytes());
    AddByteArraysTail source =
        new AddByteArraysTail((byte) (version), deltaList);

    source.serializeTo(dos);

    return new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
  }

  @SuppressWarnings("unused")
  private Object[] getDataTypeInstances() {
    return new Object[] {
        new Object[] {makeRedisHash()},
        new Object[] {makeRedisSet()},
        new Object[] {makeRedisSortedSet()},
        new Object[] {new RedisString()}
    };
  }
}
