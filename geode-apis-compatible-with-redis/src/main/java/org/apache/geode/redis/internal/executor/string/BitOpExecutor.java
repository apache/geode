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

import static org.apache.geode.redis.internal.RedisConstants.ERROR_SYNTAX;

import java.util.ArrayList;
import java.util.List;

import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.executor.AbstractExecutor;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class BitOpExecutor extends AbstractExecutor {

  protected static final String ERROR_BITOP_NOT =
      "BITOP NOT must be called with a single source key";

  @Override
  public RedisResponse executeCommand(Command command,
      ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    String operation = command.getStringKey().toUpperCase();
    if (!operation.equals("AND")
        && !operation.equals("OR")
        && !operation.equals("XOR")
        && !operation.equals("NOT")) {
      return RedisResponse.error(ERROR_SYNTAX);
    }

    RedisKey destKey = new RedisKey(commandElems.get(2));

    List<RedisKey> values = new ArrayList<>();
    for (int i = 3; i < commandElems.size(); i++) {
      RedisKey key = new RedisKey(commandElems.get(i));
      values.add(key);
    }
    if (operation.equals("NOT") && values.size() != 1) {
      return RedisResponse.error(ERROR_BITOP_NOT);
    }

    int result = context.getStringCommands().bitop(operation, destKey, values);

    return RedisResponse.integer(result);
  }
}
