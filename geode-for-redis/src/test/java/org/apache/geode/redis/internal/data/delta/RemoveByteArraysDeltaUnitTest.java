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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.redis.internal.data.RedisHash;
import org.apache.geode.redis.internal.data.RedisSet;
import org.apache.geode.redis.internal.data.RedisSortedSet;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@RunWith(GeodeParamsRunner.class)
public class RemoveByteArraysDeltaUnitTest {
  @Test
  public void testRemoveByteArraysDeltaForRedisSet() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    RemoveByteArrays source =
        new RemoveByteArrays(Arrays.asList("zero".getBytes(), "two".getBytes()));

    source.serializeTo(dos);

    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    RedisSet redisSet = makeRedisSet();
    redisSet.fromDelta(dis);

    assertThat(redisSet.scard()).isEqualTo(1);
    assertThat(redisSet.sismember("one".getBytes())).isTrue();
  }

  @Test
  public void testRemoveByteArraysDeltaForRedisSortedSet() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    RemoveByteArrays source =
        new RemoveByteArrays(Arrays.asList("alpha".getBytes(), "gamma".getBytes()));

    source.serializeTo(dos);

    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    RedisSortedSet redisSortedSet = makeRedisSortedSet();
    redisSortedSet.fromDelta(dis);

    assertThat(redisSortedSet.zcard()).isEqualTo(1);
    assertThat(redisSortedSet.zrank("beta".getBytes())).isEqualTo(0L);
  }

  @Test
  public void testRemoveByteArraysDeltaForRedisHash() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    RemoveByteArrays source =
        new RemoveByteArrays(Collections.singletonList("zero".getBytes()));

    source.serializeTo(dos);

    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    RedisHash redisHash = makeRedisHash();
    redisHash.fromDelta(dis);

    assertThat(redisHash.hlen()).isEqualTo(2);
    assertThat(redisHash.hget("zero".getBytes())).isNull();
  }

  private RedisSet makeRedisSet() {
    RedisSet redisSet = new RedisSet(5);
    redisSet.membersAdd("zero".getBytes());
    redisSet.membersAdd("one".getBytes());
    redisSet.membersAdd("two".getBytes());
    return redisSet;
  }

  private RedisSortedSet makeRedisSortedSet() {
    RedisSortedSet redisSortedSet = new RedisSortedSet(3);
    redisSortedSet.memberAdd("alpha".getBytes(), 1.0d);
    redisSortedSet.memberAdd("beta".getBytes(), 2.0d);
    redisSortedSet.memberAdd("gamma".getBytes(), 4.0d);
    return redisSortedSet;
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
