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

import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisConstants.ArityDef;
import org.apache.geode.redis.internal.RedisResponse;

public class BitPosExecutor extends StringExecutor {

  private final String ERROR_NOT_INT = "The numbers provided must be numeric values";

  private final String ERROR_BIT = "The bit must either be a 0 or 1";

  @Override
  public RedisResponse executeCommandWithResponse(Command command,
      ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();
    if (commandElems.size() < 3) {
      return RedisResponse.error(ArityDef.BITPOS);
    }

    ByteArrayWrapper key = command.getKey();
    ByteArrayWrapper string = getRedisStringCommands(context).get(key);

    int bit;
    int bitPosition = -1;
    boolean endSet = false;

    try {
      byte[] bitAr = commandElems.get(2);
      bit = Coder.bytesToInt(bitAr);
    } catch (NumberFormatException e) {
      return RedisResponse.error(ERROR_NOT_INT);
    }

    if (bit != 0 && bit != 1) {
      return RedisResponse.error(ERROR_BIT);
    }

    if (string == null || string.length() == 0) {
      // Redis returns 0 when key does not exists for this command
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), -bit));
      return RedisResponse.integer(-bit);
    }
    byte[] bytes = string.toBytes();
    int start = 0;
    int end = bytes.length - 1;
    if (commandElems.size() > 3) {
      try {
        byte[] startAr = commandElems.get(3);
        start = Coder.bytesToInt(startAr);
      } catch (NumberFormatException e) {
        return RedisResponse.error(ERROR_NOT_INT);
      }
    }

    if (commandElems.size() > 4) {
      try {
        byte[] endAr = commandElems.get(4);
        end = Coder.bytesToInt(endAr);
        endSet = true;
      } catch (NumberFormatException e) {
        return RedisResponse.error(ERROR_NOT_INT);
      }
    }

    if (start < 0) {
      start += bytes.length;
    }
    if (end < 0) {
      end += bytes.length;
    }

    if (start < 0) {
      start = 0;
    }
    if (end < 0) {
      end = 0;
    }

    if (start > bytes.length) {
      start = bytes.length - 1;
    }
    if (end > bytes.length) {
      end = bytes.length - 1;
    }

    if (end < start) {
      return RedisResponse.integer(-1);
    }

    outerLoop: for (int i = start; i <= end; i++) {
      int cBit;
      byte cByte = bytes[i];
      for (int j = 0; j < 8; j++) {
        cBit = (cByte & (0x80 >> j)) >> (7 - j);
        if (cBit == bit) {
          bitPosition = 8 * i + j;
          break outerLoop;
        }
      }
    }

    if (bit == 0 && bitPosition == -1 && !endSet) {
      bitPosition = bytes.length * 8;
    }

    return RedisResponse.integer(bitPosition);
  }

}
