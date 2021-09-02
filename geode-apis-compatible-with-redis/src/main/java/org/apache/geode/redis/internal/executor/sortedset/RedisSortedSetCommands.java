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

package org.apache.geode.redis.internal.executor.sortedset;

import java.util.List;

import org.apache.geode.redis.internal.data.RedisKey;

public interface RedisSortedSetCommands {

  Object zadd(RedisKey key, List<byte[]> scoresAndMembersToAdd, ZAddOptions options);

  long zcard(RedisKey key);

  long zcount(RedisKey key, SortedSetScoreRangeOptions rangeOptions);

  byte[] zincrby(RedisKey key, byte[] increment, byte[] member);

  long zlexcount(RedisKey key, SortedSetLexRangeOptions rangeOptions);

  List<byte[]> zpopmax(RedisKey key, int count);

  List<byte[]> zpopmin(RedisKey key, int count);

  List<byte[]> zrange(RedisKey key, SortedSetRankRangeOptions rangeOptions);

  List<byte[]> zrangebylex(RedisKey key, SortedSetLexRangeOptions rangeOptions);

  List<byte[]> zrangebyscore(RedisKey key, SortedSetScoreRangeOptions rangeOptions);

  long zrank(RedisKey key, byte[] member);

  long zrem(RedisKey key, List<byte[]> membersToRemove);

  long zremrangebylex(RedisKey key, SortedSetLexRangeOptions rangeOptions);

  long zremrangebyscore(RedisKey key, SortedSetScoreRangeOptions rangeOptions);

  List<byte[]> zrevrange(RedisKey key, SortedSetRankRangeOptions rangeOptions);

  List<byte[]> zrevrangebylex(RedisKey key, SortedSetLexRangeOptions rangeOptions);

  List<byte[]> zrevrangebyscore(RedisKey key, SortedSetScoreRangeOptions rangeOptions);

  long zrevrank(RedisKey key, byte[] member);

  byte[] zscore(RedisKey key, byte[] member);

  long zunionstore(RedisKey destinationKey, List<ZKeyWeight> keyWeights, ZAggregator aggregator);

}
