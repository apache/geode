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
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.data.RedisString;
import org.apache.geode.redis.internal.executor.CommandExecutor;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Coder;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class BitCountExecutor implements CommandExecutor {

  private static final String ERROR_NOT_INT = "The indexes provided must be numeric values";

  @Override
  public RedisResponse executeCommand(Command command,
      ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    RedisKey key = command.getKey();
    long result;

    if (commandElems.size() == 4) {
      int start;
      int end;
      try {
        start = Math.toIntExact(Coder.bytesToLong(commandElems.get(2)));
        end = Math.toIntExact(Coder.bytesToLong(commandElems.get(3)));
      } catch (NumberFormatException e) {
        return RedisResponse.error(ERROR_NOT_INT);
      } catch (ArithmeticException ex) {
        return RedisResponse.error(RedisConstants.ERROR_OUT_OF_RANGE);
      }
      result = context.stringLockedExecute(key, true, string -> string.bitcount(start, end));
    } else {
      result = context.stringLockedExecute(key, true, RedisString::bitcount);
    }
    return RedisResponse.integer(result);
  }
}
