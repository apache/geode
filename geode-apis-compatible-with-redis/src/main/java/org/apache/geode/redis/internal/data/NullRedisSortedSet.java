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
 *
 */

package org.apache.geode.redis.internal.data;


import static java.util.Collections.singletonList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.executor.sortedset.SortedSetLexRangeOptions;
import org.apache.geode.redis.internal.executor.sortedset.SortedSetRankRangeOptions;
import org.apache.geode.redis.internal.executor.sortedset.SortedSetScoreRangeOptions;
import org.apache.geode.redis.internal.executor.sortedset.ZAddOptions;
import org.apache.geode.redis.internal.netty.Coder;

class NullRedisSortedSet extends RedisSortedSet {

  NullRedisSortedSet() {
    super(new ArrayList<>(), new double[0]);
  }

  @Override
  public boolean isNull() {
    return true;
  }

  @Override
  Object zadd(Region<RedisKey, RedisData> region, RedisKey key, List<byte[]> members,
      double[] scores, ZAddOptions options) {
    if (options.isXX()) {
      if (options.isINCR()) {
        return null;
      }
      return 0;
    }

    if (options.isINCR()) {
      return zaddIncr(region, key, members.get(0), scores[0]);
    }

    RedisSortedSet sortedSet = new RedisSortedSet(members, scores);
    region.create(key, sortedSet);

    return sortedSet.getSortedSetSize();
  }

  @Override
  long zcount(SortedSetScoreRangeOptions rangeOptions) {
    return 0;
  }

  @Override
  byte[] zincrby(Region<RedisKey, RedisData> region, RedisKey key, double increment,
      byte[] member) {
    RedisSortedSet sortedSet = new RedisSortedSet(singletonList(member), new double[] {increment});

    region.create(key, sortedSet);

    return Coder.doubleToBytes(increment);
  }

  @Override
  long zlexcount(SortedSetLexRangeOptions rangeOptions) {
    return 0;
  }

  @Override
  List<byte[]> zpopmax(Region<RedisKey, RedisData> region, RedisKey key, int count) {
    return Collections.emptyList();
  }

  @Override
  List<byte[]> zpopmin(Region<RedisKey, RedisData> region, RedisKey key, int count) {
    return Collections.emptyList();
  }

  @Override
  List<byte[]> zrange(SortedSetRankRangeOptions rangeOptions) {
    return Collections.emptyList();
  }

  @Override
  List<byte[]> zrangebylex(SortedSetLexRangeOptions rangeOptions) {
    return Collections.emptyList();
  }

  @Override
  List<byte[]> zrangebyscore(SortedSetScoreRangeOptions rangeOptions) {
    return Collections.emptyList();
  }

  @Override
  long zremrangebylex(Region<RedisKey, RedisData> region, RedisKey key,
      SortedSetLexRangeOptions rangeOptions) {
    return 0L;
  }

  long zremrangebyrank(Region<RedisKey, RedisData> region, RedisKey key,
      SortedSetRankRangeOptions rangeOptions) {
    return 0;
  }

  @Override
  long zremrangebyscore(Region<RedisKey, RedisData> region, RedisKey key,
      SortedSetScoreRangeOptions rangeOptions) {
    return 0;
  }

  @Override
  List<byte[]> zrevrange(SortedSetRankRangeOptions rangeOptions) {
    return Collections.emptyList();
  }

  @Override
  List<byte[]> zrevrangebylex(SortedSetLexRangeOptions rangeOptions) {
    return Collections.emptyList();
  }

  @Override
  List<byte[]> zrevrangebyscore(SortedSetScoreRangeOptions rangeOptions) {
    return Collections.emptyList();
  }

  @Override
  byte[] zscore(byte[] member) {
    return null;
  }

  private byte[] zaddIncr(Region<RedisKey, RedisData> region, RedisKey key, byte[] member,
      double increment) {
    // for zadd incr option, only one incrementing element pair is allowed to get here.
    return zincrby(region, key, increment, member);
  }
}
