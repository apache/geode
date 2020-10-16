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


import java.util.List;

import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.redis.internal.data.ByteArrayWrapper;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Coder;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class IncrByFloatExecutor extends StringExecutor {

  private static final int INCREMENT_INDEX = 2;

  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {

    List<byte[]> commandElems = command.getProcessedCommand();
    ByteArrayWrapper key = command.getKey();
    RedisStringCommands stringCommands = getRedisStringCommands(context);

    byte[] incrArray = commandElems.get(INCREMENT_INDEX);
    String doub = Coder.bytesToString(incrArray).toLowerCase();
    if (doub.contains("inf") || doub.contains("nan")) {
      return RedisResponse.error("increment would produce NaN or Infinity");
    }

    double increment;
    try {
      increment = Coder.bytesToDouble(incrArray);
    } catch (NumberFormatException e) {
      return RedisResponse.error(RedisConstants.ERROR_NOT_A_VALID_FLOAT);
    }

    double result = stringCommands.incrbyfloat(key, increment);

    return RedisResponse.doubleValue(result);
  }
}
