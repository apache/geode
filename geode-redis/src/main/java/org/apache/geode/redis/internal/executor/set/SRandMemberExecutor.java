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
package org.apache.geode.redis.internal.executor.set;

import java.util.Collection;
import java.util.List;

import org.apache.geode.redis.internal.data.ByteArrayWrapper;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Coder;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class SRandMemberExecutor extends SetExecutor {

  private static final String ERROR_NOT_NUMERIC = "The count provided must be numeric";

  @Override
  public RedisResponse executeCommand(Command command,
      ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    ByteArrayWrapper key = command.getKey();

    boolean countSpecified = false;
    int count = 1;

    if (commandElems.size() > 2) {
      try {
        count = Coder.bytesToInt(commandElems.get(2));
        countSpecified = true;
      } catch (NumberFormatException e) {
        return RedisResponse.error(ERROR_NOT_NUMERIC);
      }
    }

    if (count == 0) {
      return RedisResponse.emptyArray();
    }

    RedisSetCommands redisSetCommands = createRedisSetCommands(context);
    Collection<ByteArrayWrapper> results = redisSetCommands.srandmember(key, count);

    if (countSpecified) {
      return RedisResponse.array(results);
    } else if (results.isEmpty()) {
      return RedisResponse.nil();
    } else {
      return RedisResponse.bulkString(results.iterator().next());
    }
  }
}
