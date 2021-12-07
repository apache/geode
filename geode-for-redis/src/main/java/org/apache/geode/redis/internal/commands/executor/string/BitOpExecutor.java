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

import static org.apache.geode.redis.internal.RedisConstants.ERROR_SYNTAX;
import static org.apache.geode.redis.internal.commands.executor.BaseSetOptions.Exists.NONE;

import java.util.ArrayList;
import java.util.List;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.redis.internal.commands.Command;
import org.apache.geode.redis.internal.commands.executor.CommandExecutor;
import org.apache.geode.redis.internal.commands.executor.RedisResponse;
import org.apache.geode.redis.internal.data.RedisDataType;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.data.RedisString;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;
import org.apache.geode.redis.internal.services.RegionProvider;

public class BitOpExecutor implements CommandExecutor {

  protected static final String ERROR_BITOP_NOT =
      "BITOP NOT must be called with a single source key";

  @Override
  public RedisResponse executeCommand(Command command,
      ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    // TODO: When BitOp becomes a supported command, replace this String conversion with byte[]
    // comparison and a class similar to ZAddOptions which should be passed into the bitop() methods
    String operation = command.getStringKey().toUpperCase();
    if (!operation.equals("AND")
        && !operation.equals("OR")
        && !operation.equals("XOR")
        && !operation.equals("NOT")) {
      return RedisResponse.error(ERROR_SYNTAX);
    }

    RedisKey destKey = new RedisKey(commandElems.get(2));

    List<RedisKey> values = new ArrayList<>();
    for (int i = 3; i < commandElems.size(); i++) {
      RedisKey key = new RedisKey(commandElems.get(i));
      values.add(key);
    }
    if (operation.equals("NOT") && values.size() != 1) {
      return RedisResponse.error(ERROR_BITOP_NOT);
    }

    int result = bitop(context, operation, destKey, values);

    return RedisResponse.integer(result);
  }


  private int bitop(ExecutionHandlerContext context, String operation, RedisKey key,
      List<RedisKey> sources) {
    List<byte[]> sourceValues = new ArrayList<>();
    int selfIndex = -1;
    // Read all the source values, except for self, before locking the stripe.
    for (RedisKey sourceKey : sources) {
      if (sourceKey.equals(key)) {
        // get self later after the stripe is locked
        selfIndex = sourceValues.size();
        sourceValues.add(null);
      } else {
        sourceValues.add(context.stringLockedExecute(sourceKey, true, RedisString::get));
      }
    }
    int indexOfSelf = selfIndex;
    RegionProvider regionProvider = context.getRegionProvider();
    return context.lockedExecute(key,
        () -> doBitOp(regionProvider, operation, key, indexOfSelf, sourceValues));
  }

  private enum BitOp {
    AND, OR, XOR
  }

  @Immutable
  private static final SetOptions bitOpSetOptions = new SetOptions(NONE, 0, true);

  private static int doBitOp(RegionProvider regionProvider, String operation, RedisKey key,
      int selfIndex,
      List<byte[]> sourceValues) {
    if (selfIndex != -1) {
      RedisString redisString =
          regionProvider.getTypedRedisData(RedisDataType.REDIS_STRING, key, true);
      if (!redisString.isNull()) {
        sourceValues.set(selfIndex, redisString.getValue());
      }
    }
    int maxLength = 0;
    for (byte[] sourceValue : sourceValues) {
      if (sourceValue != null && maxLength < sourceValue.length) {
        maxLength = sourceValue.length;
      }
    }
    byte[] newValue;
    switch (operation) {
      case "AND":
        newValue = doBitOp(BitOp.AND, sourceValues, maxLength);
        break;
      case "OR":
        newValue = doBitOp(BitOp.OR, sourceValues, maxLength);
        break;
      case "XOR":
        newValue = doBitOp(BitOp.XOR, sourceValues, maxLength);
        break;
      default: // NOT
        newValue = not(sourceValues.get(0), maxLength);
        break;
    }
    if (newValue.length == 0) {
      regionProvider.getDataRegion().remove(key);
    } else {
      SetExecutor.setRedisString(regionProvider, key, newValue, bitOpSetOptions);
    }
    return newValue.length;
  }

  private static byte[] doBitOp(BitOp bitOp, List<byte[]> sourceValues, int max) {
    byte[] dest = new byte[max];
    for (int i = 0; i < max; i++) {
      byte b = 0;
      boolean firstByte = true;
      for (byte[] sourceValue : sourceValues) {
        byte sourceByte = 0;
        if (sourceValue != null && i < sourceValue.length) {
          sourceByte = sourceValue[i];
        }
        if (firstByte) {
          b = sourceByte;
          firstByte = false;
        } else {
          switch (bitOp) {
            case AND:
              b &= sourceByte;
              break;
            case OR:
              b |= sourceByte;
              break;
            case XOR:
              b ^= sourceByte;
              break;
          }
        }
      }
      dest[i] = b;
    }
    return dest;
  }

  private static byte[] not(byte[] sourceValue, int max) {
    byte[] dest = new byte[max];
    if (sourceValue == null) {
      for (int i = 0; i < max; i++) {
        dest[i] = ~0;
      }
    } else {
      for (int i = 0; i < max; i++) {
        dest[i] = (byte) (~sourceValue[i] & 0xFF);
      }
    }
    return dest;
  }

}
