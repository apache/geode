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
package org.apache.geode.redis.internal.executor.hash;

import java.util.List;

import org.apache.geode.redis.internal.data.ByteArrayWrapper;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Coder;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

/**
 * <pre>
 * Implementation of HINCRBYFLOAT Redis command.
 * The purpose is to increment the specified field of a hash for a given key.
 *  The value is floating number (represented as a double), by the specified increment.
 *
 * Examples:
 *
 * redis> HSET mykey field 10.50
 * (integer) 1
 * redis> HINCRBYFLOAT mykey field 0.1
 * "10.6"
 * redis> HINCRBYFLOAT mykey field -5
 * "5.6"
 * redis> HSET mykey field 5.0e3
 * (integer) 0
 * redis> HINCRBYFLOAT mykey field 2.0e2
 * "5200"
 *
 *
 * </pre>
 */
public class HIncrByFloatExecutor extends HashExecutor {

  private static final String ERROR_INCREMENT_NOT_USABLE =
      "The increment on this key must be floating point numeric";

  private static final int INCREMENT_INDEX = FIELD_INDEX + 1;

  @Override
  public RedisResponse executeCommand(Command command,
      ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();
    byte[] byteField = commandElems.get(FIELD_INDEX);
    ByteArrayWrapper field = new ByteArrayWrapper(byteField);

    byte[] incrArray = commandElems.get(INCREMENT_INDEX);
    double increment;
    try {
      increment = Coder.bytesToDouble(incrArray);
    } catch (NumberFormatException e) {
      return RedisResponse.error(ERROR_INCREMENT_NOT_USABLE);
    }

    ByteArrayWrapper key = command.getKey();
    RedisHashCommands redisHashCommands = createRedisHashCommands(context);

    double value = redisHashCommands.hincrbyfloat(key, field, increment);
    return RedisResponse.bulkString(value);
  }

}
