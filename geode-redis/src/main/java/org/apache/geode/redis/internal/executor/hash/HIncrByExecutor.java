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

import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;

/**
 * <pre>
 *
 * Implementation of the HINCRBY command to increment the number stored at field
 * in the hash stored at key by increment value.
 *
 * Examples:
 *
 * redis> HSET myhash field 5
 * (integer) 1
 * redis> HINCRBY myhash field 1
 * (integer) 6
 * redis> HINCRBY myhash field -1
 * (integer) 5
 * redis> HINCRBY myhash field -10
 * (integer) -5
 *
 *
 * </pre>
 */
public class HIncrByExecutor extends HashExecutor {

  private static final String ERROR_FIELD_NOT_USABLE = "The value at this field is not an integer";

  private static final String ERROR_INCREMENT_NOT_USABLE =
      "The increment on this key must be numeric";

  private static final String ERROR_OVERFLOW =
      "This incrementation cannot be performed due to overflow";

  private static final int INCREMENT_INDEX = FIELD_INDEX + 1;

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();
    ByteArrayWrapper key = command.getKey();
    byte[] byteField = commandElems.get(FIELD_INDEX);
    ByteArrayWrapper field = new ByteArrayWrapper(byteField);

    byte[] incrArray = commandElems.get(INCREMENT_INDEX);
    long increment;
    try {
      increment = Coder.bytesToLong(incrArray);
    } catch (NumberFormatException e) {
      command.setResponse(
          Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_INCREMENT_NOT_USABLE));
      return;
    }

    RedisHashCommands redisHashCommands =
        new RedisHashCommandsFunctionExecutor(context.getRegionProvider().getDataRegion());

    try {
      long value = redisHashCommands.hincrby(key, field, increment);
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), value));
    } catch (NumberFormatException ex) {
      command.setResponse(
          Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_FIELD_NOT_USABLE));
    } catch (ArithmeticException ex) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_OVERFLOW));
    }
  }

}
