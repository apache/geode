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
import static org.apache.geode.redis.internal.RedisConstants.ERROR_KEY_EXISTS;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_INTEGER;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_SYNTAX;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bABSTTL;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bREPLACE;

import java.util.List;

import org.apache.geode.redis.internal.RedisException;
import org.apache.geode.redis.internal.data.AbstractRedisData;
import org.apache.geode.redis.internal.data.RedisData;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.data.RedisKeyExistsException;
import org.apache.geode.redis.internal.executor.CommandExecutor;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Coder;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class RestoreExecutor implements CommandExecutor {

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
      restore(context, key, ttl, commandElems.get(3), options);
    } catch (RedisKeyExistsException redisKeyExistsException) {
      return RedisResponse.busykey(ERROR_KEY_EXISTS);
    }

    return RedisResponse.ok();
  }

  private RestoreOptions parseOptions(List<byte[]> options) {
    RestoreOptions restoreOptions = new RestoreOptions();
    for (byte[] optionBytes : options) {
      if (Coder.equalsIgnoreCaseBytes(optionBytes, bREPLACE)) {
        restoreOptions.setReplace(true);
      } else if (Coder.equalsIgnoreCaseBytes(optionBytes, bABSTTL)) {
        restoreOptions.setAbsttl(true);
      } else {
        throw new RedisException(ERROR_SYNTAX);
      }
    }

    return restoreOptions;
  }

  private static void restore(ExecutionHandlerContext context, RedisKey key, long ttl,
      byte[] dataBytes, RestoreOptions options) {
    long expireAt;
    if (ttl == 0) {
      expireAt = AbstractRedisData.NO_EXPIRATION;
    } else {
      if (options.isAbsttl()) {
        expireAt = ttl;
      } else {
        expireAt = System.currentTimeMillis() + ttl;
      }
    }

    context.dataLockedExecute(key, data -> {
      RedisData value = data.restore(dataBytes, options.isReplace());
      ((AbstractRedisData) value).setExpirationTimestampNoDelta(expireAt);
      context.getRegion().put(key, value);
      return null;
    });
  }

}
