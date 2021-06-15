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

package org.apache.geode.redis.internal.executor.key;

import static org.apache.geode.redis.internal.RedisConstants.ERROR_INVALID_TTL;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_INTEGER;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_RESTORE_KEY_EXISTS;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_SYNTAX;

import java.util.List;

import org.apache.geode.redis.internal.RedisException;
import org.apache.geode.redis.internal.RedisRestoreKeyExistsException;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.executor.AbstractExecutor;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Coder;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class RestoreExecutor extends AbstractExecutor {

  private static final int TTL_INDEX = 2;

  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();
    RedisKey key = command.getKey();

    byte[] ttlByteArray = commandElems.get(TTL_INDEX);
    long ttl;
    try {
      ttl = Coder.bytesToLong(ttlByteArray);
    } catch (NumberFormatException e) {
      return RedisResponse.error(ERROR_NOT_INTEGER);
    }

    if (ttl < 0) {
      return RedisResponse.error(ERROR_INVALID_TTL);
    }

    RestoreOptions options;
    if (commandElems.size() > 4) {
      options = parseOptions(commandElems.subList(4, commandElems.size()));
    } else {
      options = new RestoreOptions();
    }

    try {
      context.getKeyCommands().restore(key, ttl, commandElems.get(3), options);
    } catch (RedisRestoreKeyExistsException redisRestoreKeyExistsException) {
      return RedisResponse.busykey(ERROR_RESTORE_KEY_EXISTS);
    }

    return RedisResponse.ok();
  }

  private RestoreOptions parseOptions(List<byte[]> options) {
    RestoreOptions restoreOptions = new RestoreOptions();
    for (byte[] optionBytes : options) {
      String option = Coder.bytesToString(optionBytes).toUpperCase();
      switch (option) {
        case "REPLACE":
          restoreOptions.setReplace(true);
          break;
        case "ABSTTL":
          restoreOptions.setAbsttl(true);
          break;
        default:
          throw new RedisException(ERROR_SYNTAX);
      }
    }

    return restoreOptions;
  }
}
