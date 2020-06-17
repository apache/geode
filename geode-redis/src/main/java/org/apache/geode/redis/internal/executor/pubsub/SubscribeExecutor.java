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

import java.util.ArrayList;
import java.util.Collection;

import org.apache.geode.redis.internal.executor.AbstractExecutor;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class SubscribeExecutor extends AbstractExecutor {

  @Override
  public RedisResponse executeCommand(Command command,
      ExecutionHandlerContext context) {
    Collection<Collection<?>> items = new ArrayList<>();
    for (int i = 1; i < command.getProcessedCommand().size(); i++) {
      Collection<Object> item = new ArrayList<>();
      byte[] channelName = command.getProcessedCommand().get(i);
      long subscribedChannels =
          context.getPubSub().subscribe(new String(channelName), context, context.getClient());

      item.add("subscribe");
      item.add(channelName);
      item.add(subscribedChannels);

      items.add(item);
    }

    context.changeChannelEventLoopGroup(context.getSubscriberGroup());

    return RedisResponse.flattenedArray(items);
  }

}
