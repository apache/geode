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

import static org.apache.geode.redis.internal.netty.Coder.bytesToLong;
import static org.apache.geode.redis.internal.netty.Coder.narrowLongToInt;

import java.util.List;

import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.executor.AbstractExecutor;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class BitPosExecutor extends AbstractExecutor {

  private static final String ERROR_NOT_INT = "The numbers provided must be numeric values";

  private static final String ERROR_BIT = "The bit must either be a 0 or 1";

  @Override
  public RedisResponse executeCommand(Command command,
      ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();
    RedisKey key = command.getKey();

    int bit;
    int start = 0;
    Integer end = null;

    try {
      byte[] bitAr = commandElems.get(2);
      bit = narrowLongToInt(bytesToLong(bitAr));
    } catch (NumberFormatException e) {
      return RedisResponse.error(ERROR_NOT_INT);
    }

    if (bit != 0 && bit != 1) {
      return RedisResponse.error(ERROR_BIT);
    }

    if (commandElems.size() > 3) {
      try {
        byte[] startAr = commandElems.get(3);
        start = narrowLongToInt(bytesToLong(startAr));
      } catch (NumberFormatException e) {
        return RedisResponse.error(ERROR_NOT_INT);
      }
    }

    if (commandElems.size() > 4) {
      try {
        byte[] endAr = commandElems.get(4);
        end = narrowLongToInt(bytesToLong(endAr));
      } catch (NumberFormatException e) {
        return RedisResponse.error(ERROR_NOT_INT);
      }
    }

    int bitPosition = context.getStringCommands().bitpos(key, bit, start, end);
    return RedisResponse.integer(bitPosition);
  }

}
