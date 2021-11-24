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


import static org.apache.geode.redis.internal.netty.Coder.bytesToLong;
import static org.apache.geode.redis.internal.netty.Coder.narrowLongToInt;

import java.util.List;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.commands.Command;
import org.apache.geode.redis.internal.commands.executor.CommandExecutor;
import org.apache.geode.redis.internal.commands.executor.RedisResponse;
import org.apache.geode.redis.internal.data.RedisData;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class SetBitExecutor implements CommandExecutor {

  private static final String ERROR_NOT_INT = "The number provided must be numeric";

  private static final String ERROR_VALUE = "The value is out of range, must be 0 or 1";

  private static final String ERROR_ILLEGAL_OFFSET =
      "The offset is out of range, must be greater than or equal to 0  and at most 4294967295 (512MB)";

  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();
    Region<RedisKey, RedisData> region = context.getRegion();
    RedisKey key = command.getKey();

    long offset;
    int value;
    try {
      byte[] offAr = commandElems.get(2);
      byte[] valAr = commandElems.get(3);
      offset = bytesToLong(offAr);
      value = narrowLongToInt(bytesToLong(valAr));
    } catch (NumberFormatException e) {
      return RedisResponse.error(ERROR_NOT_INT);
    }

    if (value != 0 && value != 1) {
      return RedisResponse.error(ERROR_VALUE);
    }

    if (offset < 0 || offset > 4294967295L) {
      return RedisResponse.error(ERROR_ILLEGAL_OFFSET);
    }

    int byteIndex = (int) (offset / 8);
    byte bitIndex = (byte) (offset % 8);
    int returnBit = context.stringLockedExecute(key, false,
        string -> string.setbit(region, key, value, byteIndex, bitIndex));

    return RedisResponse.integer(returnBit);
  }
}
