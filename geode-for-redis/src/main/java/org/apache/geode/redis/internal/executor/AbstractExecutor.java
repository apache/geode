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
package org.apache.geode.redis.internal.executor;

import java.util.Collection;
import java.util.Set;

import org.apache.geode.cache.LowMemoryException;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.redis.internal.GeodeRedisServer;
import org.apache.geode.redis.internal.RedisCommandType;
import org.apache.geode.redis.internal.data.RedisData;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

/**
 * The AbstractExecutor is the base of all {@link Executor} types for the {@link GeodeRedisServer}.
 */
public abstract class AbstractExecutor implements Executor {

  protected static final int HASH_FIELD_INDEX = 2;

  protected RedisResponse respondBulkStrings(Object message) {
    if (message instanceof Collection) {
      return RedisResponse.array((Collection<?>) message, true);
    } else {
      return RedisResponse.bulkString(message);
    }
  }

  protected Region<RedisKey, RedisData> getDataRegion(ExecutionHandlerContext context) {
    return context.getRegionProvider().getLocalDataRegion();
  }

  protected void checkForLowMemory(RedisCommandType commandType, ExecutionHandlerContext context) {
    Set<DistributedMember> criticalMembers = context.getRegionProvider().getCriticalMembers();
    if (!criticalMembers.isEmpty()) {
      throw new LowMemoryException(
          String.format(
              "%s cannot be executed because the members %s are running low on memory",
              commandType.toString(), criticalMembers),
          criticalMembers);
    }
  }
}
