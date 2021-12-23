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
package org.apache.geode.redis.internal.commands.executor.set;

import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_INTEGER;
import static org.apache.geode.redis.internal.data.RedisSet.srandmember;
import static org.apache.geode.redis.internal.netty.Coder.bytesToLong;
import static org.apache.geode.redis.internal.netty.Coder.narrowLongToInt;

import java.util.List;

import org.apache.geode.redis.internal.commands.Command;
import org.apache.geode.redis.internal.commands.executor.CommandExecutor;
import org.apache.geode.redis.internal.commands.executor.RedisResponse;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class SRandMemberExecutor implements CommandExecutor {

  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();
    RedisKey key = command.getKey();
    int argsCount = commandElems.size();
    int count;

    if (argsCount == 3) {
      try {
        count = narrowLongToInt(bytesToLong(commandElems.get(2)));
      } catch (NumberFormatException e) {
        return RedisResponse.error(ERROR_NOT_INTEGER);
      }
    } else {
      count = 1;
    }

    List<byte[]> results =
        context.lockedExecute(key, () -> srandmember(count, context.getRegionProvider(), key));
    if (argsCount == 2) {
      byte[] byteResult = null;
      if (!results.isEmpty()) {
        byteResult = results.get(0);
      }
      return RedisResponse.bulkString(byteResult);
    }
    return RedisResponse.array(results, true);
  }
}
