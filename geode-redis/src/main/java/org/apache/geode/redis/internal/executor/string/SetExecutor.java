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
import static org.apache.geode.redis.internal.RedisConstants.ERROR_INVALID_EXPIRE_TIME;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_INTEGER;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_SYNTAX;

import java.util.List;

import org.apache.geode.redis.internal.data.ByteArrayWrapper;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Coder;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class SetExecutor extends StringExecutor {

  private final String SUCCESS = "OK";

  @Override
  public RedisResponse executeCommand(Command command,
      ExecutionHandlerContext context) {

    List<byte[]> commandElems = command.getProcessedCommand();
    ByteArrayWrapper keyToSet = command.getKey();
    ByteArrayWrapper valueToSet = getValueToSet(commandElems);

    RedisStringCommands redisStringCommands = getRedisStringCommands(context);
    SetOptions setOptions;
    try {
      setOptions = parseCommandElems(commandElems);
    } catch (IllegalArgumentException ex) {
      return RedisResponse.error(ex.getMessage());
    }

    return doSet(command, context, keyToSet, valueToSet, redisStringCommands, setOptions);
  }

  private RedisResponse doSet(Command command, ExecutionHandlerContext context,
      ByteArrayWrapper key,
      ByteArrayWrapper value, RedisStringCommands redisStringCommands, SetOptions setOptions) {

    boolean result = redisStringCommands.set(key, value, setOptions);

    if (result) {
      return RedisResponse.string(SUCCESS);
    }

    return RedisResponse.nil();
  }

  private ByteArrayWrapper getValueToSet(List<byte[]> commandElems) {
    byte[] value = commandElems.get(2);
    return new ByteArrayWrapper(value);
  }


  private SetOptions parseCommandElems(List<byte[]> commandElems) throws IllegalArgumentException {
    boolean keepTTL = false;
    SetOptions.Exists existsOption = SetOptions.Exists.NONE;
    long expiration = 0L;

    for (int i = 3; i < commandElems.size(); i++) {
      String current_arg = Coder.bytesToString(commandElems.get(i)).toUpperCase();
      switch (current_arg) {
        case "KEEPTTL":
          keepTTL = true;
          break;
        case "EX":
          if (expiration != 0) {
            throw new IllegalArgumentException(ERROR_SYNTAX);
          }
          i++;
          expiration = parseExpirationTime(i, commandElems);
          expiration = SECONDS.toMillis(expiration);
          break;
        case "PX":
          if (expiration != 0) {
            throw new IllegalArgumentException(ERROR_SYNTAX);
          }
          i++;
          expiration = parseExpirationTime(i, commandElems);
          break;
        case "NX":
          if (existsOption != SetOptions.Exists.NONE) {
            throw new IllegalArgumentException(ERROR_SYNTAX);
          }
          existsOption = SetOptions.Exists.NX;
          break;
        case "XX":
          if (existsOption != SetOptions.Exists.NONE) {
            throw new IllegalArgumentException(ERROR_SYNTAX);
          }
          existsOption = SetOptions.Exists.XX;
          break;
        default:
          throw new IllegalArgumentException(ERROR_SYNTAX);
      }
    }

    return new SetOptions(existsOption, expiration, keepTTL);
  }

  private long parseExpirationTime(int index, List<byte[]> commandElems)
      throws IllegalArgumentException {
    String expirationString;

    try {
      expirationString = Coder.bytesToString(commandElems.get(index));
    } catch (IndexOutOfBoundsException e) {
      throw new IllegalArgumentException(ERROR_SYNTAX);
    }

    try {
      long expiration = Long.parseLong(expirationString);
      if (expiration <= 0) {
        throw new IllegalArgumentException(ERROR_INVALID_EXPIRE_TIME);
      }
      return expiration;
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(ERROR_NOT_INTEGER);
    }

  }
}
