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

import static org.apache.geode.redis.internal.data.delta.DeltaType.REPLACE_BYTE_ARRAYS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Set;

import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.redis.internal.data.RedisData;
import org.apache.geode.redis.internal.data.RedisSet;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@RunWith(GeodeParamsRunner.class)
public class ReplaceByteArraysDeltaUnitTest extends AbstractRedisDeltaUnitTest {
  @Test
  public void testReplaceByteArraysDelta_forRedisSet() throws Exception {
    DataInputStream dis = getDataInputStream();
    RedisSet redisSet = makeRedisSet();
    redisSet.fromDelta(dis);

    assertThat(redisSet.scard()).isEqualTo(1);
    assertThat(redisSet.sismember("alpha".getBytes())).isTrue();
  }

  @Test
  @Parameters(method = "getUnsupportedDataTypeInstancesForDelta")
  @TestCaseName("{method}: redisDataType:{0}")
  public void unsupportedDataTypesThrowException(RedisData redisData)
      throws IOException {
    final DataInputStream dis = getDataInputStream();

    assertThatThrownBy(() -> redisData.fromDelta(dis)).isInstanceOf(
        IllegalStateException.class)
        .hasMessage("unexpected " + REPLACE_BYTE_ARRAYS);
  }

  private DataInputStream getDataInputStream() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    Set<byte[]> newSet = new RedisSet.MemberSet(1);
    newSet.add("alpha".getBytes());
    ReplaceByteArrays source = new ReplaceByteArrays(newSet);

    source.serializeTo(dos);

    return new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
  }

  @SuppressWarnings("unused")
  private Object[] getUnsupportedDataTypeInstancesForDelta() {
    return new Object[] {
        new Object[] {makeRedisHash()},
        new Object[] {makeRedisList()},
        new Object[] {makeRedisSortedSet()},
        new Object[] {makeRedisString()}
    };
  }
}
