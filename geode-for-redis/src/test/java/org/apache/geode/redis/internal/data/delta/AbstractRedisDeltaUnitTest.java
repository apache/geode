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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.geode.redis.internal.data.RedisHash;
import org.apache.geode.redis.internal.data.RedisList;
import org.apache.geode.redis.internal.data.RedisSet;
import org.apache.geode.redis.internal.data.RedisSortedSet;
import org.apache.geode.redis.internal.data.RedisString;

public abstract class AbstractRedisDeltaUnitTest {
  protected RedisHash makeRedisHash() {
    List<byte[]> pairList = new ArrayList<>();
    pairList.add("zero".getBytes());
    pairList.add("firstVal".getBytes());
    pairList.add("one".getBytes());
    pairList.add("secondVal".getBytes());
    pairList.add("two".getBytes());
    pairList.add("thirdVal".getBytes());
    return new RedisHash(pairList);
  }

  protected RedisList makeRedisList() {
    RedisList redisList = new RedisList();
    redisList.applyAddByteArrayTailDelta("zero".getBytes());
    redisList.applyAddByteArrayTailDelta("one".getBytes());
    redisList.applyAddByteArrayTailDelta("two".getBytes());
    return redisList;
  }

  protected RedisSet makeRedisSet() {
    Set<byte[]> bytes = new HashSet<>();
    bytes.add("zero".getBytes());
    bytes.add("one".getBytes());
    bytes.add("two".getBytes());
    return new RedisSet(bytes);
  }

  protected RedisSortedSet makeRedisSortedSet() {
    List<byte[]> members = Arrays.asList("alpha".getBytes(), "beta".getBytes(), "gamma".getBytes());
    double[] scores = {1.0d, 2.0d, 4.0d};
    return new RedisSortedSet(members, scores);
  }

  protected RedisString makeRedisString() {
    return new RedisString("radish".getBytes());
  }
}
