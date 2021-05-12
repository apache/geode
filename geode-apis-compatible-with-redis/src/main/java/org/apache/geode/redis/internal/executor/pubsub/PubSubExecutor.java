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


package org.apache.geode.redis.internal.executor.pubsub;

import static org.apache.geode.redis.internal.RedisConstants.ERROR_UNKNOWN_PUBSUB_SUBCOMMAND;

import java.util.Arrays;
import java.util.List;

import org.apache.geode.redis.internal.executor.Executor;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Coder;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class PubSubExecutor implements Executor {

  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {

    byte[] subCommand = command.getProcessedCommand().get(1);

    if (!Arrays.equals(subCommand, Coder.stringToBytes("channels"))) {
      return RedisResponse
          .error(String.format(ERROR_UNKNOWN_PUBSUB_SUBCOMMAND, new String(subCommand)));
    }

    // in a subsequent story, a new parameter requirement class
    // specific to subCommands might be a better way to do this
    if (command.getProcessedCommand().size() > 3) {
      return RedisResponse
          .error(String.format(ERROR_UNKNOWN_PUBSUB_SUBCOMMAND, new String(subCommand)));
    }

    List<byte[]> response;
    if (command.getProcessedCommand().size() > 2) {
      byte[] pattern = command.getProcessedCommand().get(2);
      response = context.getPubSub().findChannelNames(pattern);
    } else {
      response = context.getPubSub().findChannelNames();
    }

    return RedisResponse.array(response);
  }
}
