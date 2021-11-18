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
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.CHANNELS;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.NUMPAT;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.NUMSUB;

import java.util.List;

import org.apache.geode.redis.internal.executor.CommandExecutor;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class PubSubExecutor implements CommandExecutor {

  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> args = command.getCommandArguments();
    byte[] subCommand = args.get(0);

    if (equalsIgnoreCaseBytes(subCommand, CHANNELS)) {
      if (args.size() > 2) {
        return RedisResponse
            .error(String.format(ERROR_UNKNOWN_PUBSUB_SUBCOMMAND, new String(subCommand)));
      }
      List<byte[]> channelsResponse = doChannels(args, context);
      return RedisResponse.array(channelsResponse, true);
    } else if (equalsIgnoreCaseBytes(subCommand, NUMSUB)) {
      List<Object> numSubresponse = context.getPubSub()
          .findNumberOfSubscribersPerChannel(args.subList(1, args.size()));
      return RedisResponse.array(numSubresponse, true);
    } else if (equalsIgnoreCaseBytes(subCommand, NUMPAT)) {
      if (args.size() > 1) {
        return RedisResponse
            .error(String.format(ERROR_UNKNOWN_PUBSUB_SUBCOMMAND, new String(subCommand)));
      }
      long numPatResponse = context.getPubSub().findNumberOfUniqueSubscribedPatterns();
      return RedisResponse.integer(numPatResponse);
    }

    return RedisResponse
        .error(String.format(ERROR_UNKNOWN_PUBSUB_SUBCOMMAND, bytesToString(subCommand)));
  }

  private List<byte[]> doChannels(List<byte[]> args, ExecutionHandlerContext context) {
    if (args.size() > 1) {
      return context.getPubSub().findChannelNames(args.get(1));
    } else {
      return context.getPubSub().findChannelNames();
    }
  }

}
