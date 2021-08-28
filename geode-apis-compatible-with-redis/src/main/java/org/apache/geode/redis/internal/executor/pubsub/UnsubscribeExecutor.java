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

package org.apache.geode.redis.internal.executor.pubsub;

import static java.util.Collections.singletonList;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bUNSUBSCRIBE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.geode.redis.internal.executor.AbstractExecutor;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Client;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class UnsubscribeExecutor extends AbstractExecutor {

  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> channelNames = extractChannelNames(command);
    Collection<Collection<?>> response = unsubscribe(context, channelNames);
    return RedisResponse.flattenedArray(response);
  }

  private List<byte[]> extractChannelNames(Command command) {
    return command.getProcessedCommand().stream().skip(1).collect(Collectors.toList());
  }

  private static final Collection<Collection<?>> EMPTY_RESULT = singletonList(createItem(null, 0));

  private Collection<Collection<?>> unsubscribe(ExecutionHandlerContext context,
      List<byte[]> channelNames) {
    Client client = context.getClient();
    if (channelNames.isEmpty()) {
      channelNames = client.getChannelSubscriptions();
      if (channelNames.isEmpty()) {
        return EMPTY_RESULT;
      }
    }
    Collection<Collection<?>> response = new ArrayList<>(channelNames.size());
    for (byte[] channel : channelNames) {
      long subscriptionCount = context.getPubSub().unsubscribe(channel, client);
      response.add(createItem(channel, subscriptionCount));
    }

    return response;
  }

  private static List<Object> createItem(byte[] channelName, long subscriptionCount) {
    return Arrays.asList(bUNSUBSCRIBE, channelName, subscriptionCount);
  }

}
