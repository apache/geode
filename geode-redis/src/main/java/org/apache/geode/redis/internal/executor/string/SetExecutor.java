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
import org.apache.geode.redis.internal.RedisConstants.ArityDef;
import org.apache.geode.redis.internal.RedisDataType;
import org.apache.geode.redis.internal.RedisDataTypeMismatchException;
import org.apache.geode.redis.internal.executor.AbstractExecutor;

public class SetExecutor extends StringExecutor {

  private final String SUCCESS = "OK";

  private final int VALUE_INDEX = 2;

  private boolean NX = false; // Set only if not exists, incompatible with XX
  private boolean XX = false; // Set only if exists, incompatible with NX
  private boolean KEEPTTL = false; // Keep existing TTL on key
  private long expiration = 0L;

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 3) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.SET));
      return;
    }

    ByteArrayWrapper key = command.getKey();
    ByteArrayWrapper value = getValue(commandElems);

    if (context.getKeyRegistrar().isProtected(key)) {
      throw new RedisDataTypeMismatchException("The key name \"" + key + "\" is protected");
    }

    String parseError = parseCommandElems(commandElems);
    if (parseError != null) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), parseError));
      return;
    }

    Region<ByteArrayWrapper, ByteArrayWrapper> region =
        context.getRegionProvider().getStringsRegion();

    if (NX) {
      setNX(region, command, key, value, context);
      return;
    }

    if (XX) {
      setXX(region, command, key, value, context);
      return;
    }

    set(command, context, region, key, value);
  }

  private ByteArrayWrapper getValue(List<byte[]> commandElems) {
    byte[] value = commandElems.get(VALUE_INDEX);
    return new ByteArrayWrapper(value);
  }

  private Region getRegion(ExecutionHandlerContext context, ByteArrayWrapper key) {
    RedisDataType redisDataType = context.getKeyRegistrar().getType(key);
    return context.getRegionProvider().getRegionForType(redisDataType);
  }

  private String parseCommandElems(List<byte[]> commandElems) {
    // Set only if exists, incompatible with EX
    boolean PX = false;
    // Set only if not exists, incompatible with PX
    boolean EX = false;

    NX = XX = EX = PX = KEEPTTL = false;

    expiration = 0L;
    for (int i = 3; i < commandElems.size(); i++) {
      String current_arg = Coder.bytesToString(commandElems.get(i)).toUpperCase();
      switch (current_arg) {
        case "KEEPTTL":
          KEEPTTL = true;
          break;
        case "EX":
          EX = true;
          i++;
          expiration = parseExpirationTime(current_arg, i, commandElems);
          break;
        case "PX":
          PX = true;
          i++;
          expiration = parseExpirationTime(current_arg, i, commandElems);
          break;
        case "NX":
          NX = true;
          break;
        case "XX":
          XX = true;
          break;
        default:
          return ERROR_SYNTAX;
      }
    }

    if (EX && PX) {
      return ERROR_SYNTAX;
    }

    if (NX && XX) {
      return ERROR_SYNTAX;
    }

    if (EX || PX) {
      if (expiration == -2L) {
        return ERROR_SYNTAX;
      }
      if (expiration == -1L) {
        return ERROR_NOT_INTEGER;
      }
      if (expiration == 0L) {
        return ERROR_INVALID_EXPIRE_TIME;
      }
    }

    return null;
  }

  private long parseExpirationTime(String arg, int index, List<byte[]> commandElems) {
    String expirationString;

    try {
      expirationString = Coder.bytesToString(commandElems.get(index));
    } catch (IndexOutOfBoundsException e) {
      return -2L;
    }

    long expiration = 0L;
    try {
      expiration = Long.parseLong(expirationString);
    } catch (NumberFormatException e) {
      return -1L;
    }

    if (expiration <= 0) {
      return 0L;
    }

    if (arg.equalsIgnoreCase("EX")) {
      return expiration * AbstractExecutor.millisInSecond;
    } else if (arg.equalsIgnoreCase("PX")) {
      return expiration;
    } else {
      return -1L;
    }
  }

  private void setNX(Region<ByteArrayWrapper, ByteArrayWrapper> region, Command command,
      ByteArrayWrapper key, ByteArrayWrapper valueWrapper,
      ExecutionHandlerContext context) {
    if (keyAlreadyExistsForDifferentDataType(context, key)) {
      command.setResponse(Coder.getNilResponse(context.getByteBufAllocator()));
      return;
    }

    checkAndSetDataType(key, context);
    Object oldValue = region.putIfAbsent(key, valueWrapper);

    if (oldValue != null) {
      command.setResponse(Coder.getNilResponse(context.getByteBufAllocator()));
      return;
    }

    command.setResponse(Coder.getSimpleStringResponse(context.getByteBufAllocator(), SUCCESS));
    handleExpiration(context, key);
  }

  private void setXX(Region<ByteArrayWrapper, ByteArrayWrapper> region, Command command,
      ByteArrayWrapper key, ByteArrayWrapper valueWrapper,
      ExecutionHandlerContext context) {
    if (region.containsKey(key) || keyAlreadyExistsForDifferentDataType(context, key)) {
      set(command, context, region, key, valueWrapper);
    } else {
      command.setResponse(Coder.getNilResponse(context.getByteBufAllocator()));
    }
  }

  private void set(Command command, ExecutionHandlerContext context,
      Region<ByteArrayWrapper, ByteArrayWrapper> stringsRegion, ByteArrayWrapper key,
      ByteArrayWrapper valueWrapper) {
    if (keyAlreadyExistsForDifferentDataType(context, key)) {
      removeOldValueAndDataTypeAssociation(context, key);
    }
    checkAndSetDataType(key, context);
    stringsRegion.put(key, valueWrapper);
    command.setResponse(Coder.getSimpleStringResponse(context.getByteBufAllocator(), SUCCESS));
    handleExpiration(context, key);
  }

  private boolean keyAlreadyExistsForDifferentDataType(ExecutionHandlerContext context,
      ByteArrayWrapper key) {
    try {
      checkDataType(key, RedisDataType.REDIS_STRING, context);
    } catch (RedisDataTypeMismatchException e) {
      return true;
    }

    return false;
  }

  private void removeOldValueAndDataTypeAssociation(ExecutionHandlerContext context,
      ByteArrayWrapper key) {
    Region oldRegion = getRegion(context, key);
    oldRegion.remove(key);
    context.getKeyRegistrar().unregister(key);
  }

  private void handleExpiration(ExecutionHandlerContext context, ByteArrayWrapper key) {
    if (expiration > 0L) {
      context.getRegionProvider().setExpiration(key, expiration);
    } else {
      if (!KEEPTTL) {
        context.getRegionProvider().cancelKeyExpiration(key);
      }
    }
  }
}
