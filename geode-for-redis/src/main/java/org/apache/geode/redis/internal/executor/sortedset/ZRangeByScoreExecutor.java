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
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class ZRangeByScoreExecutor
    extends AbstractSortedSetRangeExecutor<SortedSetScoreRangeOptions> {

  @Override
  public boolean isRev() {
    return false;
  }

  @Override
  public SortedSetScoreRangeOptions createRangeOptions(List<byte[]> commandElements) {
    return new SortedSetScoreRangeOptions(commandElements, isRev());
  }

  @Override
  public RedisResponse getEmptyResponse() {
    return RedisResponse.emptyArray();
  }

  @Override
  public RedisResponse executeRangeCommand(ExecutionHandlerContext context, RedisKey key,
      SortedSetScoreRangeOptions options) {
    List<byte[]> result = context.zsetLockedExecute(key, true,
        zset -> zset.zrangebyscore(options));
    return RedisResponse.array(result, true);
  }
}
