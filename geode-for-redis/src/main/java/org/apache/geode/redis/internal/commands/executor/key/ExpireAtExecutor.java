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
package org.apache.geode.redis.internal.commands.executor.key;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_INTEGER;

import java.util.List;

import org.apache.geode.redis.internal.commands.Command;
import org.apache.geode.redis.internal.commands.executor.CommandExecutor;
import org.apache.geode.redis.internal.commands.executor.RedisResponse;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.netty.Coder;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class ExpireAtExecutor implements CommandExecutor {

  @Override
  public RedisResponse executeCommand(Command command,
      ExecutionHandlerContext context) {
    try {
      List<byte[]> commandElems = command.getProcessedCommand();
      RedisKey wKey = command.getKey();
      long timestamp = getTimestamp(commandElems.get(2));

      int result =
          context.dataLockedExecute(wKey, false,
              data -> data.pexpireat(context.getRegionProvider(), wKey, timestamp));

      return RedisResponse.integer(result);
    } catch (NumberFormatException e) {
      return RedisResponse.error(ERROR_NOT_INTEGER);
    }
  }

  private long getTimestamp(byte[] timestampByteArray) throws NumberFormatException {
    long result = Coder.bytesToLong(timestampByteArray);
    if (!timeUnitMillis()) {
      result = SECONDS.toMillis(result);
    }
    return result;
  }

  protected boolean timeUnitMillis() {
    return false;
  }
}
