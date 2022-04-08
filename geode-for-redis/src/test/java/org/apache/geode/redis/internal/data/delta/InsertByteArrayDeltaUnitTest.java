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

import static org.apache.geode.redis.internal.data.delta.DeltaType.INSERT_BYTE_ARRAY;
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
import org.apache.geode.redis.internal.data.RedisList;
import org.apache.geode.redis.internal.data.RedisString;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@RunWith(GeodeParamsRunner.class)
public class InsertByteArrayDeltaUnitTest extends AbstractRedisDeltaUnitTest {
  @Test
  @Parameters(method = "getInsertByteArrayIndexes")
  @TestCaseName("{method}: index:{0}")
  public void testInsertByteArrayDelta(int index) throws Exception {
    RedisList redisList = makeRedisList();
    DataInputStream dis = getDataInputStream(index, redisList.getVersion() + 1);

    redisList.fromDelta(dis);

    assertThat(redisList.llen()).isEqualTo(4);
    assertThat(redisList.lindex(index)).isEqualTo("newElement".getBytes());
  }

  @Test
  @Parameters(method = "getDataTypeInstances")
  @TestCaseName("{method}: redisDataType:{0}")
  public void unsupportedDataTypesThrowException(RedisData redisData)
      throws IOException {
    final DataInputStream dis = getDataInputStream(1, 1);

    assertThatThrownBy(() -> redisData.fromDelta(dis)).isInstanceOf(
        IllegalStateException.class)
        .hasMessageContaining("unexpected " + INSERT_BYTE_ARRAY);
  }

  private DataInputStream getDataInputStream(int index, int version) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    InsertByteArray source =
        new InsertByteArray((byte) (version), "newElement".getBytes(), index);

    source.serializeTo(dos);

    return new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
  }

  @SuppressWarnings("unused")
  private Object[] getInsertByteArrayIndexes() {
    return new Object[] {
        0,
        1,
        2,
        3
    };
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
