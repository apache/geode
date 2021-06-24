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
import static org.apache.geode.redis.internal.netty.Coder.bytesToString;
import static org.apache.geode.redis.internal.netty.Coder.equalsIgnoreCaseBytes;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bCHANNELS;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bNUMPAT;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bNUMSUB;

import java.util.List;

import org.apache.geode.redis.internal.executor.Executor;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class PubSubExecutor implements Executor {

  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commands = command.getProcessedCommand();
    byte[] subCommand = commands.get(1);

    if (equalsIgnoreCaseBytes(subCommand, bCHANNELS)) {
      if (commands.size() > 3) {
        return RedisResponse
            .error(String.format(ERROR_UNKNOWN_PUBSUB_SUBCOMMAND, new String(subCommand)));
      }
      List<byte[]> channelsResponse = doChannels(commands, context);
      return RedisResponse.array(channelsResponse);
    } else if (equalsIgnoreCaseBytes(subCommand, bNUMSUB)) {
      List<Object> numSubresponse = context.getPubSub()
          .findNumberOfSubscribersPerChannel(commands.subList(2, commands.size()));
      return RedisResponse.array(numSubresponse);
    } else if (equalsIgnoreCaseBytes(subCommand, bNUMPAT)) {
      Long numPatResponse = context.getPubSub().findNumberOfSubscribedPatterns();
      return RedisResponse.integer(numPatResponse);
    }

    return RedisResponse
        .error(String.format(ERROR_UNKNOWN_PUBSUB_SUBCOMMAND, bytesToString(subCommand)));
  }

  private List<byte[]> doChannels(List<byte[]> processedCommand, ExecutionHandlerContext context) {
    List<byte[]> response;

    if (processedCommand.size() > 2) {
      response = context.getPubSub().findChannelNames(processedCommand.get(2));
    } else {
      response = context.getPubSub().findChannelNames();
    }

    return response;
  }

}
