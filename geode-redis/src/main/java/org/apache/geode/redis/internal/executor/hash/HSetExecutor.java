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
import org.apache.geode.redis.internal.Extendable;
import org.apache.geode.redis.internal.RedisConstants.ArityDef;

/**
 * <pre>
 * Implements the HSET command to sets field in the hash stored at key to value.
 * A new entry in the hash is created if key does not exist.
 * Any existing in the hash with the given key is overwritten.
 *
 * Examples:
 *
 * redis> HSET myhash field1 "Hello"
 * (integer) 1
 * redis> HGET myhash field1
 *
 *
 * </pre>
 */
public class HSetExecutor extends HashExecutor implements Extendable {

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<ByteArrayWrapper> commandElems = command.getProcessedCommandWrappers();

    if (commandElems.size() < 4 || commandElems.size() % 2 == 1) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), getArgsError()));
      return;
    }

    ByteArrayWrapper key = command.getKey();

    RedisHash hash = new GeodeRedisHashSynchronized(key, context);

    int fieldsAdded = hash.hset(commandElems.subList(2, commandElems.size()), onlySetOnAbsent());

    command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), fieldsAdded));
  }

  protected boolean onlySetOnAbsent() {
    return false;
  }

  @Override
  public String getArgsError() {
    return ArityDef.HSET;
  }
}
