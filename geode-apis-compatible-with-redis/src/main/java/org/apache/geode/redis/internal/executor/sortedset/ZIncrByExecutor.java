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
package org.apache.geode.redis.internal.executor.sortedset;


import static org.apache.geode.redis.internal.RedisConstants.ERROR_NAN_OR_INFINITY;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_A_VALID_FLOAT;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.geode.redis.internal.executor.AbstractExecutor;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class ZIncrByExecutor extends AbstractExecutor {
  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {
    RedisSortedSetCommands redisSortedSetCommands = context.getRedisSortedSetCommands();
    List<byte[]> commandElements = command.getProcessedCommand();
    Iterator<byte[]> commandIterator = commandElements.iterator();

    skipCommandAndKey(commandIterator);
    byte[] increment = commandIterator.next();
    byte[] member = commandIterator.next();

    byte[] retVal = redisSortedSetCommands.zincrby(command.getKey(), increment, member);
    if (Arrays.equals(retVal, ERROR_NOT_A_VALID_FLOAT.getBytes())) {
      return RedisResponse.error(ERROR_NOT_A_VALID_FLOAT);
    } else if (Arrays.equals(retVal, ERROR_NAN_OR_INFINITY.getBytes())) {
      return RedisResponse.error(ERROR_NAN_OR_INFINITY);
    }
    return RedisResponse.string(retVal);
  }

  private void skipCommandAndKey(Iterator<byte[]> commandIterator) {
    commandIterator.next();
    commandIterator.next();
  }
}
