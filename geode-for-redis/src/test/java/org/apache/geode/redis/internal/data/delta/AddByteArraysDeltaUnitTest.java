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

import org.apache.geode.redis.internal.data.RedisList;
import org.apache.geode.redis.internal.data.RedisSet;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@RunWith(GeodeParamsRunner.class)
public class AddByteArraysDeltaUnitTest {
  @Test
  public void testAddByteArraysDelta_forRedisList() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    List<byte[]> deltaList = new ArrayList<>();
    deltaList.add("secondNew".getBytes());
    deltaList.add("firstNew".getBytes());
    RedisList redisList = makeRedisList();
    AddByteArrays source = new AddByteArrays(deltaList, (byte) (redisList.getVersion() + 1));

    source.serializeTo(dos);

    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    redisList.fromDelta(dis);

    assertThat(redisList.llen()).isEqualTo(5);
    assertThat(redisList.lindex(0)).isEqualTo("firstNew".getBytes());
    assertThat(redisList.lindex(1)).isEqualTo("secondNew".getBytes());
    assertThat(redisList.lindex(2)).isEqualTo("zero".getBytes());
  }

  @Test
  public void testAddByteArraysDelta_forRedisSet() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    List<byte[]> deltaList = new ArrayList<>();
    deltaList.add("secondNew".getBytes());
    deltaList.add("firstNew".getBytes());
    RedisSet redisSet = makeRedisSet();
    AddByteArrays source = new AddByteArrays(deltaList, (byte) (redisSet.getVersion() + 1));

    source.serializeTo(dos);

    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    redisSet.fromDelta(dis);

    assertThat(redisSet.scard()).isEqualTo(5);
    assertThat(redisSet.sismember("firstNew".getBytes())).isTrue();
    assertThat(redisSet.sismember("secondNew".getBytes())).isTrue();
  }

  private RedisList makeRedisList() {
    RedisList redisList = new RedisList();
    redisList.applyAddByteArrayTailDelta("zero".getBytes());
    redisList.applyAddByteArrayTailDelta("one".getBytes());
    redisList.applyAddByteArrayTailDelta("two".getBytes());
    return redisList;
  }

  private RedisSet makeRedisSet() {
    RedisSet redisSet = new RedisSet(5);
    redisSet.membersAdd("zero".getBytes());
    redisSet.membersAdd("one".getBytes());
    redisSet.membersAdd("two".getBytes());
    return redisSet;
  }
}
