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
package org.apache.geode.redis.internal.executor.key;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.data.RedisData;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.executor.CommandExecutor;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class TTLExecutor implements CommandExecutor {

  @Override
  public RedisResponse executeCommand(Command command,
      ExecutionHandlerContext context) {
    RedisKey key = command.getKey();

    long result = pttl(context, key);
    if (result > 0 && !timeUnitMillis()) {
      // Round up because redis does
      result = MILLISECONDS.toSeconds(result + 500);
    }

    return RedisResponse.integer(result);
  }

  protected boolean timeUnitMillis() {
    return false;
  }

  private static long pttl(ExecutionHandlerContext context, RedisKey key) {
    Region<RedisKey, RedisData> region = context.getRegion();
    long result = context.dataLockedExecute(key,
        data -> data.pttl(region, key));

    if (result == -2) {
      context.getRegionProvider().getRedisStats().incKeyspaceMisses();
    } else {
      context.getRegionProvider().getRedisStats().incKeyspaceHits();
    }

    return result;
  }
}
