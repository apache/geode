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

import static org.apache.geode.redis.internal.data.delta.DeltaType.REPLACE_BYTE_AT_OFFSET;
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
import org.apache.geode.redis.internal.data.RedisString;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@RunWith(GeodeParamsRunner.class)
public class ReplaceByteAtOffsetDeltaUnitTest extends AbstractRedisDeltaUnitTest {
  private final byte[] byteToPut = "a".getBytes();

  @Test
  @Parameters(method = "getReplaceByteAtOffsetValues")
  @TestCaseName("{method}: original:{0}, offset:{1}, expected:{2}")
  public void testReplaceByteAtOffsetDelta(String original, int offset, String expected)
      throws Exception {
    DataInputStream dis = getDataInputStream(offset);
    RedisString redisString = new RedisString(original.getBytes());
    redisString.fromDelta(dis);

    assertThat(redisString.get()).isEqualTo(expected.getBytes());
  }

  @Test
  @Parameters(method = "getDataTypeInstances")
  @TestCaseName("{method}: redisDataType:{0}")
  public void unsupportedDataTypesThrowException(RedisData redisData)
      throws IOException {
    final DataInputStream dis = getDataInputStream(1);

    assertThatThrownBy(() -> redisData.fromDelta(dis)).isInstanceOf(
        IllegalStateException.class)
        .hasMessageContaining("unexpected " + REPLACE_BYTE_AT_OFFSET);
  }

  private DataInputStream getDataInputStream(int offset) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);

    ReplaceByteAtOffset source = new ReplaceByteAtOffset(offset, byteToPut[0]);

    source.serializeTo(dos);

    return new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
  }

  @SuppressWarnings("unused")
  private Object[] getReplaceByteAtOffsetValues() {
    // Values are original, offset, expected result
    return new Object[] {
        new Object[] {"01234567890", 0, "a1234567890"},
        new Object[] {"01234567890", 3, "012a4567890"},
        new Object[] {"01234567890", 10, "0123456789a"}
    };
  }

  @SuppressWarnings("unused")
  private Object[] getDataTypeInstances() {
    return new Object[] {
        new Object[] {makeRedisHash()},
        new Object[] {makeRedisList()},
        new Object[] {makeRedisSet()},
        new Object[] {makeRedisSortedSet()}
    };
  }
}
