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

import static org.apache.geode.redis.internal.netty.Coder.toUpperCaseBytes;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.GET;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.LEN;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.RESET;

import java.util.Arrays;

import org.apache.geode.redis.internal.executor.CommandExecutor;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

// TODO: only exists for Redis monitoring software, maybe make functional someday?
public class SlowlogExecutor implements CommandExecutor {

  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {
    byte[] subCommand = toUpperCaseBytes(command.getBytesKey());
    if (Arrays.equals(subCommand, GET)) {
      return RedisResponse.emptyArray();
    } else if (Arrays.equals(subCommand, LEN)) {
      return RedisResponse.integer(0);
    } else if (Arrays.equals(subCommand, RESET)) {
      return RedisResponse.ok();
    } else {
      return null;
    }
  }
}
