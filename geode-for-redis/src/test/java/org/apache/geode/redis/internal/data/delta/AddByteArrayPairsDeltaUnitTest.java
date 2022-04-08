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
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.redis.internal.data.RedisHash;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@RunWith(GeodeParamsRunner.class)
public class AddByteArrayPairsDeltaUnitTest {

  @Test
  public void testAddByteArrayPairsDelta() throws Exception {
    byte[] original = "0123456789".getBytes();
    byte[] payload = "something amazing I guess".getBytes();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    AddByteArrayPairs source = new AddByteArrayPairs(original, payload);

    source.serializeTo(dos);

    RedisHash redisHash = makeRedisHash();

    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    redisHash.fromDelta(dis);

    assertThat(redisHash.hlen()).isEqualTo(4);
    assertThat(redisHash.hget(original)).isEqualTo(payload);
  }

  private RedisHash makeRedisHash() {
    List<byte[]> pairList = new ArrayList<>();
    pairList.add("zero".getBytes());
    pairList.add("firstVal".getBytes());
    pairList.add("one".getBytes());
    pairList.add("secondVal".getBytes());
    pairList.add("two".getBytes());
    pairList.add("thirdVal".getBytes());
    return new RedisHash(pairList);
  }
}
