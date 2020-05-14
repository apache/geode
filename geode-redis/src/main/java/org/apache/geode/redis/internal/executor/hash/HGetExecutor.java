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
import java.util.Map;

import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.CoderException;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisConstants;

/**
 * <pre>
 *
 * Implements the Redis HGET command to returns the value associated with field in the hash stored
 * at key.
 *
 * Examples:
 *
 * redis> HSET myhash field1 "foo" (integer) 1 redis> HGET myhash field1 "foo" redis> HGET myhash
 * field2
 *
 * <pre>
 */
public class HGetExecutor extends HashExecutor {

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    byte[] byteField = commandElems.get(FIELD_INDEX);
    ByteArrayWrapper field = new ByteArrayWrapper(byteField);

    ByteArrayWrapper key = command.getKey();

    Map<ByteArrayWrapper, ByteArrayWrapper> entry = getMap(context, key);

    if (entry == null) {
      command.setResponse(Coder.getNilResponse(context.getByteBufAllocator()));
      return;
    }

    ByteArrayWrapper valueWrapper = entry.get(field);
    try {
      if (valueWrapper != null) {
        command.setResponse(
            Coder.getBulkStringResponse(context.getByteBufAllocator(), valueWrapper.toBytes()));
      } else {
        command.setResponse(Coder.getNilResponse(context.getByteBufAllocator()));
      }
    } catch (CoderException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(),
          RedisConstants.SERVER_ERROR_MESSAGE));
    }
  }
}
