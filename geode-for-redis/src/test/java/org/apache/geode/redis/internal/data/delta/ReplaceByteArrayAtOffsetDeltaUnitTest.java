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

import static org.apache.geode.redis.internal.data.delta.DeltaType.REPLACE_BYTE_ARRAY_AT_OFFSET;
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
public class ReplaceByteArrayAtOffsetDeltaUnitTest extends AbstractRedisDeltaUnitTest {
  private final String payload = "something amazing I guess";

  @Test
  public void testReplaceByteArrayAtOffsetDeltaForRedisString() throws Exception {
    DataInputStream dis = getDataInputStream(3);
    String original = "0123456789";
    RedisString redisString = new RedisString(original.getBytes());

    redisString.fromDelta(dis);

    assertThat(new String(redisString.get())).isEqualTo(original.substring(0, 3) + payload);
  }

  @Test
  public void testReplaceByteArrayAtOffsetDeltaForRedisList() throws Exception {
    DataInputStream dis = getDataInputStream(1);
    RedisList redisList = makeRedisList();

    redisList.fromDelta(dis);

    assertThat(redisList.llen()).isEqualTo(3);
    assertThat(redisList.lindex(0)).isEqualTo("zero".getBytes());
    assertThat(redisList.lindex(1)).isEqualTo(payload.getBytes());
    assertThat(redisList.lindex(2)).isEqualTo("two".getBytes());
  }

  @Test
  @Parameters(method = "getDataTypeInstances")
  @TestCaseName("{method}: redisDataType:{0}")
  public void unsupportedDataTypesThrowException(RedisData redisData)
      throws IOException {
    final DataInputStream dis = getDataInputStream(1);

    assertThatThrownBy(() -> redisData.fromDelta(dis)).isInstanceOf(
        IllegalStateException.class)
        .hasMessageContaining("unexpected " + REPLACE_BYTE_ARRAY_AT_OFFSET);
  }

  private DataInputStream getDataInputStream(int offset) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    ReplaceByteArrayAtOffset source = new ReplaceByteArrayAtOffset(offset, payload.getBytes());

    source.serializeTo(dos);

    return new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
  }

  @SuppressWarnings("unused")
  private Object[] getDataTypeInstances() {
    return new Object[] {
        new Object[] {makeRedisHash()},
        new Object[] {makeRedisSet()},
        new Object[] {makeRedisSortedSet()}
    };
  }
}
