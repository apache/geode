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

import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_INTEGER;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_SYNTAX;
import static org.apache.geode.redis.internal.netty.Coder.bytesToLong;
import static org.apache.geode.redis.internal.netty.Coder.equalsIgnoreCaseBytes;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bWITHSCORES;

import java.util.List;

import org.apache.geode.redis.internal.executor.AbstractExecutor;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public abstract class AbstractZRangeExecutor extends AbstractExecutor {

  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {
    int min;
    int max;
    List<byte[]> commandElements = command.getProcessedCommand();

    try {
      // We need to be able to accept long arguments to match Redis behaviour, even though
      // internally int is sufficient, as sorted sets have a maximum size of Integer.MAX_VALUE
      long longMin = bytesToLong(commandElements.get(2));
      min = narrowLongToInt(longMin);
      long longMax = bytesToLong(commandElements.get(3));
      max = narrowLongToInt(longMax);
    } catch (NumberFormatException nfe) {
      return RedisResponse.error(ERROR_NOT_INTEGER);
    }

    boolean withScores = false;

    if (commandElements.size() == 5) {
      if (equalsIgnoreCaseBytes(commandElements.get(4), bWITHSCORES)) {
        withScores = true;
      } else {
        return RedisResponse.error(ERROR_SYNTAX);
      }
    }

    RedisSortedSetCommands redisSortedSetCommands = context.getSortedSetCommands();

    List<byte[]> retVal;
    if (isRev()) {
      retVal = redisSortedSetCommands.zrevrange(command.getKey(), min, max, withScores);
    } else {
      retVal = redisSortedSetCommands.zrange(command.getKey(), min, max, withScores);
    }

    return RedisResponse.array(retVal);
  }

  private int narrowLongToInt(long toBeNarrowed) {
    if (toBeNarrowed > Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    } else if (toBeNarrowed < Integer.MIN_VALUE) {
      return Integer.MIN_VALUE;
    } else {
      return (int) toBeNarrowed;
    }
  }

  public abstract boolean isRev();
}
