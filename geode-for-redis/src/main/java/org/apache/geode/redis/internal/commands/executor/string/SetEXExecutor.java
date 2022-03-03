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
package org.apache.geode.redis.internal.commands.executor.string;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_INVALID_EXPIRE_TIME;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_INTEGER;
import static org.apache.geode.redis.internal.commands.executor.BaseSetOptions.Exists.NONE;
import static org.apache.geode.redis.internal.commands.executor.string.SetExecutor.set;

import java.util.List;

import org.apache.geode.redis.internal.commands.Command;
import org.apache.geode.redis.internal.commands.executor.CommandExecutor;
import org.apache.geode.redis.internal.commands.executor.RedisResponse;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.netty.Coder;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class SetEXExecutor implements CommandExecutor {

  private static final int VALUE_INDEX = 3;

  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {

    List<byte[]> commandElems = command.getProcessedCommand();

    RedisKey key = command.getKey();
    byte[] value = commandElems.get(VALUE_INDEX);

    byte[] expirationArray = commandElems.get(2);
    long expiration;
    try {
      expiration = Coder.bytesToLong(expirationArray);
    } catch (NumberFormatException e) {
      return RedisResponse.error(ERROR_NOT_INTEGER);
    }

    if (expiration <= 0) {
      return RedisResponse.error(String.format(ERROR_INVALID_EXPIRE_TIME,
          command.getCommandType().toString().toLowerCase()));
    }

    if (!timeUnitMillis()) {
      expiration = SECONDS.toMillis(expiration);
    }
    SetOptions setOptions = new SetOptions(NONE, expiration, false);

    context.lockedExecute(key, () -> set(context.getRegionProvider(), key, value, setOptions));

    return RedisResponse.ok();
  }

  protected boolean timeUnitMillis() {
    return false;
  }
}
