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

package org.apache.geode.redis.internal.executor.server;

import java.util.Arrays;
import java.util.List;

import org.apache.geode.redis.internal.executor.AbstractExecutor;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class CommandExecutor extends AbstractExecutor {
  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {

    List<Object> response = Arrays.asList(
        Arrays.asList(
            "hmset",
            -4,
            Arrays.asList("write", "denyoom", "fast"),
            1, 1, 1),
        Arrays.asList(
            "del",
            -2,
            Arrays.asList("write"),
            1, -1, 1),
        Arrays.asList(
            "ping",
            -1,
            Arrays.asList("stale", "fast"),
            0, 0, 0),
        Arrays.asList(
            "flushall",
            -1,
            Arrays.asList("write"),
            0, 0, 0),
        Arrays.asList(
            "hgetall",
            2,
            Arrays.asList("random", "readonly"),
            1, 1, 1),
        Arrays.asList(
            "hset",
            -4,
            Arrays.asList("write", "denyoom", "fast"),
            1, 1, 1),
        Arrays.asList(
            "get",
            2,
            Arrays.asList("readonly"),
            1, 1, 1),
        Arrays.asList(
            "set",
            -3,
            Arrays.asList("write"),
            1, 1, 1));

    return RedisResponse.array(response);
  }
}
