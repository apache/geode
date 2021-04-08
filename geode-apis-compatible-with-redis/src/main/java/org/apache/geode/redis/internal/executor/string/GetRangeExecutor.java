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

import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_INTEGER;

import java.util.List;

import org.apache.geode.redis.internal.data.ByteArrayWrapper;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Coder;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class GetRangeExecutor extends StringExecutor {

  private static final int startIndex = 2;
  private static final int stopIndex = 3;

  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    long start;
    long end;

    try {
      byte[] startI = commandElems.get(startIndex);
      byte[] stopI = commandElems.get(stopIndex);
      start = Coder.bytesToLong(startI);
      end = Coder.bytesToLong(stopI);
    } catch (NumberFormatException e) {
      return RedisResponse.error(ERROR_NOT_INTEGER);
    }

    RedisStringCommands stringCommands = getRedisStringCommands(context);
    RedisKey key = command.getKey();

    ByteArrayWrapper returnRange = stringCommands.getrange(key, start, end);

    if (returnRange == null) {
      return RedisResponse.nil();
    } else if (returnRange.length() == 0) {
      return RedisResponse.emptyString();
    } else {
      return respondBulkStrings(returnRange);
    }
  }
}
