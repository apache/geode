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

import java.util.ArrayList;
import java.util.List;

import org.apache.geode.redis.internal.executor.Executor;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Coder;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class PubSubExecutor implements Executor {

  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {

    List<byte[]> processedCommand = command.getProcessedCommand();
    String subCommand = Coder.bytesToString(processedCommand.get(1)).toLowerCase();

    List<Object> response;

    switch (subCommand) {
      case "channels":
        // in a subsequent story, a new parameter requirement class
        // specific to subCommands might be a better way to do this
        if (processedCommand.size() > 3) {
          return RedisResponse
              .error(String.format(ERROR_UNKNOWN_PUBSUB_SUBCOMMAND, subCommand));
        }

        response = doChannels(processedCommand, context);
        break;

      case "numsub":
        response = doNumsub(processedCommand, context);
        break;

      default:
        return RedisResponse
            .error(String.format(ERROR_UNKNOWN_PUBSUB_SUBCOMMAND, subCommand));

    }

    return RedisResponse.array(response);
  }

  private List<Object> doChannels(List<byte[]> processedCommand, ExecutionHandlerContext context) {
    List<Object> response;

    if (processedCommand.size() > 2) {
      byte[] pattern = processedCommand.get(2);
      response = context.getPubSub().findChannelNames(pattern);
    } else {
      response = context.getPubSub().findChannelNames();
    }

    return response;
  }

  private List<Object> doNumsub(List<byte[]> processedCommand, ExecutionHandlerContext context) {

    ArrayList<byte[]> channelNames = new ArrayList<>();

    for (int i = 2; i < processedCommand.size(); i++) {
      channelNames.add(processedCommand.get(i));
    }

    return context.getPubSub().findNumberOfSubscribersForChannel(channelNames);
  }
}
