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

import org.apache.geode.redis.internal.RedisConstants.ArityDef;
import org.apache.geode.redis.internal.data.ByteArrayWrapper;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class StrlenExecutor extends StringExecutor {

  private final int KEY_DOES_NOT_EXIST = 0;

  @Override
  public RedisResponse executeCommandWithResponse(Command command,
      ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() != 2) {
      return RedisResponse.error(ArityDef.STRLEN);
    }

    RedisStringCommands stringCommands = getRedisStringCommands(context);

    ByteArrayWrapper key = command.getKey();
    ByteArrayWrapper value = stringCommands.get(key);

    if (value == null) {
      return RedisResponse.integer(KEY_DOES_NOT_EXIST);
    }

    return RedisResponse.integer(value.length());
  }
}
