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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.geode.redis.internal.executor.string.SetOptions.Exists.NONE;

import java.util.List;

import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Coder;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class SetEXExecutor extends StringExecutor {

  private static final String ERROR_SECONDS_NOT_A_NUMBER =
      "The expiration argument provided was not a number";

  private static final String ERROR_SECONDS_NOT_LEGAL =
      "invalid expire time in setex";

  private static final String SUCCESS = "OK";

  private static final int VALUE_INDEX = 3;

  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {

    List<byte[]> commandElems = command.getProcessedCommand();
    RedisStringCommands stringCommands = getRedisStringCommands(context);

    RedisKey key = command.getKey();
    byte[] value = commandElems.get(VALUE_INDEX);

    byte[] expirationArray = commandElems.get(2);
    long expiration;
    try {
      expiration = Coder.bytesToLong(expirationArray);
    } catch (NumberFormatException e) {
      return RedisResponse.error(ERROR_SECONDS_NOT_A_NUMBER);
    }

    if (expiration <= 0) {
      return RedisResponse.error(ERROR_SECONDS_NOT_LEGAL);
    }

    if (!timeUnitMillis()) {
      expiration = SECONDS.toMillis(expiration);
    }
    SetOptions setOptions = new SetOptions(NONE, expiration, false);

    stringCommands.set(key, value, setOptions);

    return RedisResponse.string(SUCCESS);
  }

  protected boolean timeUnitMillis() {
    return false;
  }
}
