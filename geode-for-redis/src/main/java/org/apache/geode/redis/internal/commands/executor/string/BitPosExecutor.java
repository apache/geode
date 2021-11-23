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

import org.apache.geode.redis.internal.commands.Command;
import org.apache.geode.redis.internal.commands.executor.CommandExecutor;
import org.apache.geode.redis.internal.commands.executor.RedisResponse;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class BitPosExecutor implements CommandExecutor {

  private static final String ERROR_NOT_INT = "The numbers provided must be numeric values";

  private static final String ERROR_BIT = "The bit must either be a 0 or 1";

  @Override
  public RedisResponse executeCommand(Command command,
      ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();
    RedisKey key = command.getKey();

    try {
      int bit = getBit(commandElems);
      if (bit != 0 && bit != 1) {
        return RedisResponse.error(ERROR_BIT);
      }
      int start = getStart(commandElems);
      Integer end = getEnd(commandElems);

      int bitPosition = context.stringLockedExecute(key, true,
          string -> string.bitpos(bit, start, end));

      return RedisResponse.integer(bitPosition);
    } catch (NumberFormatException e) {
      return RedisResponse.error(ERROR_NOT_INT);
    }
  }

  private int getBit(List<byte[]> commandElems) throws NumberFormatException {
    byte[] bitAr = commandElems.get(2);
    return narrowLongToInt(bytesToLong(bitAr));
  }

  private int getStart(List<byte[]> commandElems) throws NumberFormatException {
    int result = 0;
    if (commandElems.size() > 3) {
      byte[] startAr = commandElems.get(3);
      result = narrowLongToInt(bytesToLong(startAr));
    }
    return result;
  }

  private Integer getEnd(List<byte[]> commandElems) throws NumberFormatException {
    Integer result = null;
    if (commandElems.size() > 4) {
      byte[] endAr = commandElems.get(4);
      result = narrowLongToInt(bytesToLong(endAr));
    }
    return result;
  }

}
