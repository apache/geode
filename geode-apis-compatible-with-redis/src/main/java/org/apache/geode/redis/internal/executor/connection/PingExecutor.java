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
 *
 */
package org.apache.geode.redis.internal.executor.connection;

import static org.apache.geode.redis.internal.netty.Coder.stringToBytes;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bPING_RESPONSE;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bPING_RESPONSE_LOWERCASE;

import java.util.Arrays;
import java.util.List;

import org.apache.geode.redis.internal.executor.AbstractExecutor;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class PingExecutor extends AbstractExecutor {

  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();
    RedisResponse redisResponse;

    if (!context.getClient().hasSubscriptions()) {
      byte[] result;
      if (commandElems.size() > 1) {
        result = commandElems.get(1);
      } else {
        result = bPING_RESPONSE;
      }
      redisResponse = RedisResponse.string(result);
    } else {
      byte[] result;

      if (commandElems.size() > 1) {
        result = commandElems.get(1);
      } else {
        result = stringToBytes("");
      }
      redisResponse = RedisResponse.array(Arrays.asList(bPING_RESPONSE_LOWERCASE, result));
    }

    return redisResponse;
  }
}
