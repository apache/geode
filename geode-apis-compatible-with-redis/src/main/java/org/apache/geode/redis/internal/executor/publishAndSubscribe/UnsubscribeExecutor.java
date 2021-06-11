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

package org.apache.geode.redis.internal.executor.publishAndSubscribe;

import static org.apache.geode.redis.internal.publishAndSubscribe.Subscription.Type.CHANNEL;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.geode.redis.internal.executor.AbstractExecutor;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class UnsubscribeExecutor extends AbstractExecutor {

  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {

    context.eventLoopReady();

    List<byte[]> channelNames = extractChannelNames(command);
    if (channelNames.isEmpty()) {
      channelNames = context.getPubSub().findSubscriptionNames(context.getClient(), CHANNEL);
    }

    Collection<Collection<?>> response = unsubscribe(context, channelNames);

    return RedisResponse.flattenedArray(response);
  }

  private List<byte[]> extractChannelNames(Command command) {
    return command.getProcessedCommand().stream().skip(1).collect(Collectors.toList());
  }

  private Collection<Collection<?>> unsubscribe(ExecutionHandlerContext context,
      List<byte[]> channelNames) {
    Collection<Collection<?>> response = new ArrayList<>();

    if (channelNames.isEmpty()) {
      response.add(createItem(null, 0));
    } else {
      for (byte[] channel : channelNames) {
        long subscriptionCount = context.getPubSub().unsubscribe(channel, context.getClient());

        response.add(createItem(channel, subscriptionCount));
      }
    }

    return response;
  }

  private ArrayList<Object> createItem(byte[] channelName, long subscriptionCount) {
    ArrayList<Object> oneItem = new ArrayList<>();
    oneItem.add("unsubscribe");
    oneItem.add(channelName);
    oneItem.add(subscriptionCount);
    return oneItem;
  }

}
