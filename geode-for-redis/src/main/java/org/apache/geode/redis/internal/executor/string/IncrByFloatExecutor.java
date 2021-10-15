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
package org.apache.geode.redis.internal.executor.string;

import static org.apache.geode.redis.internal.netty.Coder.isInfinity;

import java.math.BigDecimal;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.redis.internal.data.RedisData;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.executor.CommandExecutor;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Coder;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class IncrByFloatExecutor implements CommandExecutor {

  private static final int INCREMENT_INDEX = 2;

  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();
    Region<RedisKey, RedisData> region = context.getRegion();
    RedisKey key = command.getKey();

    Pair<BigDecimal, RedisResponse> validated =
        validateIncrByFloatArgument(commandElems.get(INCREMENT_INDEX));
    if (validated.getRight() != null) {
      return validated.getRight();
    }

    BigDecimal result = context.stringLockedExecute(key, false,
        string -> string.incrbyfloat(region, key, validated.getLeft()));

    return RedisResponse.bigDecimal(result);
  }

  public static Pair<BigDecimal, RedisResponse> validateIncrByFloatArgument(byte[] incrArray) {
    if (isInfinity(incrArray)) {
      return Pair.of(null, RedisResponse.error(RedisConstants.ERROR_NAN_OR_INFINITY));
    }

    BigDecimal increment;
    try {
      increment = Coder.bytesToBigDecimal(incrArray);
    } catch (NumberFormatException e) {
      return Pair.of(null, RedisResponse.error(RedisConstants.ERROR_NOT_A_VALID_FLOAT));
    }

    return Pair.of(increment, null);
  }
}
