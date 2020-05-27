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

import static org.apache.geode.redis.internal.RedisConstants.ERROR_INVALID_EXPIRE_TIME;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_INTEGER;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_SYNTAX;

import java.util.List;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisData;
import org.apache.geode.redis.internal.executor.AbstractExecutor;

public class SetExecutor extends StringExecutor {

  private final String SUCCESS = "OK";

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {

    List<byte[]> commandElems = command.getProcessedCommand();
    ByteArrayWrapper keyToSet = command.getKey();
    ByteArrayWrapper valueToSet = getValueToSet(commandElems);

    Region<ByteArrayWrapper, RedisData> region =
        context.getRegionProvider().getDataRegion();

    RedisStringCommands redisStringCommands = new RedisStringCommandsFunctionExecutor(region);
    SetOptions setOptions;
    try {
      setOptions = parseCommandElems(commandElems);
    } catch (IllegalArgumentException ex) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ex.getMessage()));
      return;
    }

    doSet(command, context, keyToSet, valueToSet, redisStringCommands, setOptions);
  }

  private void doSet(Command command, ExecutionHandlerContext context, ByteArrayWrapper key,
      ByteArrayWrapper value, RedisStringCommands redisStringCommands, SetOptions setOptions) {

    Boolean result = redisStringCommands.set(key, value, setOptions);

    if (result) {
      command.setResponse(Coder.getSimpleStringResponse(context.getByteBufAllocator(), SUCCESS));
      handleExpiration(context, key, setOptions);
    } else {
      command.setResponse(Coder.getNilResponse(context.getByteBufAllocator()));
    }
  }

  private ByteArrayWrapper getValueToSet(List<byte[]> commandElems) {
    byte[] value = commandElems.get(2);
    return new ByteArrayWrapper(value);
  }


  private SetOptions parseCommandElems(List<byte[]> commandElems) throws IllegalArgumentException {
    boolean keepTTL = false;
    SetOptions.Exists existsOption = SetOptions.Exists.NONE;
    SetOptions.ExpireUnit expireUnitOption = SetOptions.ExpireUnit.NONE;
    Long expiration = null;

    for (int i = 3; i < commandElems.size(); i++) {
      String current_arg = Coder.bytesToString(commandElems.get(i)).toUpperCase();
      switch (current_arg) {
        case "KEEPTTL":
          keepTTL = true;
          break;
        case "EX":
          if (expireUnitOption != SetOptions.ExpireUnit.NONE) {
            throw new IllegalArgumentException(ERROR_SYNTAX);
          }
          expireUnitOption = SetOptions.ExpireUnit.EX;
          i++;
          expiration = parseExpirationTime(current_arg, i, commandElems);
          break;
        case "PX":
          if (expireUnitOption != SetOptions.ExpireUnit.NONE) {
            throw new IllegalArgumentException(ERROR_SYNTAX);
          }
          expireUnitOption = SetOptions.ExpireUnit.PX;
          i++;
          expiration = parseExpirationTime(current_arg, i, commandElems);
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

    return new SetOptions(existsOption, expiration, expireUnitOption, keepTTL);
  }

  private long parseExpirationTime(String arg, int index, List<byte[]> commandElems)
      throws IllegalArgumentException {
    String expirationString;

    try {
      expirationString = Coder.bytesToString(commandElems.get(index));
    } catch (IndexOutOfBoundsException e) {
      throw new IllegalArgumentException(ERROR_SYNTAX);
    }

    long expiration = 0L;
    try {
      expiration = Long.parseLong(expirationString);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(ERROR_NOT_INTEGER);
    }

    if (expiration <= 0) {
      throw new IllegalArgumentException(ERROR_INVALID_EXPIRE_TIME);
    }

    if (arg.equalsIgnoreCase("EX")) {
      return expiration * AbstractExecutor.millisInSecond;
    } else if (arg.equalsIgnoreCase("PX")) {
      return expiration;
    } else {
      throw new IllegalArgumentException(ERROR_NOT_INTEGER);
    }
  }

  private void handleExpiration(ExecutionHandlerContext context, ByteArrayWrapper key,
      SetOptions setOptions) {
    if (setOptions.getExpiration() != null) {
      context.getRegionProvider().setExpiration(key, setOptions.getExpiration());
    } else {
      if (!setOptions.isKeepTTL()) {
        context.getRegionProvider().cancelKeyExpiration(key);
      }
    }
  }
}
