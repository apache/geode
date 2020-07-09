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

import org.apache.geode.redis.internal.RedisConstants.ArityDef;
import org.apache.geode.redis.internal.data.ByteArrayWrapper;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Coder;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class SetRangeExecutor extends StringExecutor {

  private static final String ERROR_NOT_INT = "The number provided must be numeric";

  private static final String ERROR_ILLEGAL_OFFSET =
      "The offset is out of range, must be greater than or equal to 0 and the offset added to the length of the value must be less than 536870911 (512MB), the maximum allowed size";

  @Override
  public RedisResponse executeCommand(Command command,
      ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 4) {
      return RedisResponse.error(ArityDef.SETRANGE);
    }

    ByteArrayWrapper key = command.getKey();

    int offset;
    try {
      byte[] offAr = commandElems.get(2);
      offset = Coder.bytesToInt(offAr);
    } catch (NumberFormatException e) {
      return RedisResponse.error(ERROR_NOT_INT);
    }

    byte[] value = commandElems.get(3);

    if (offset < 0 || (offset + value.length) > 536870911) {
      return RedisResponse.error(ERROR_ILLEGAL_OFFSET);
    }


    RedisStringCommands stringCommands = getRedisStringCommands(context);
    int result = stringCommands.setrange(key, offset, value);

    return RedisResponse.integer(result);
  }

}
