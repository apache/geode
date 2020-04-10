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
import org.apache.geode.redis.internal.RedisConstants.ArityDef;

/**
 * <pre>
 *
 * Implements the HMSet command.
 *
 * This command will set the specified fields to their given values in the hash stored at key.
 * This command overwrites any specified fields already in the hash.
 * A new key holding a hash is created, if the key does not exist.
 *
 * Examples:
 *
 * redis> HMSET myhash field1 "Hello" field2 "World"
 * "OK"
 * redis> HGET myhash field1
 * "Hello"
 * redis> HGET myhash field2
 * "World"
 *
 * </pre>
 */
public class HMSetExecutor extends HashExecutor {

  private static final String SUCCESS = "OK";

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<ByteArrayWrapper> commandElems = command.getProcessedCommandWrappers();

    if (commandElems.size() < 4 || commandElems.size() % 2 == 1) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.HMSET));
      return;
    }

    ByteArrayWrapper key = command.getKey();
    RedisHash hash = new GeodeRedisHashSynchronized(key, context);
    hash.hset(commandElems.subList(2, commandElems.size()), false);
    command.setResponse(Coder.getSimpleStringResponse(context.getByteBufAllocator(), SUCCESS));
  }

}
