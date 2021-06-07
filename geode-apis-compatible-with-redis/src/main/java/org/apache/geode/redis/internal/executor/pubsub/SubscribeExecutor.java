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
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

import org.apache.geode.redis.internal.executor.AbstractExecutor;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;
import org.apache.geode.redis.internal.publishAndSubscribe.SubscribeResult;

public class SubscribeExecutor extends AbstractExecutor {

  @Override
  public RedisResponse executeCommand(Command command,
      ExecutionHandlerContext context) {

    context.eventLoopReady();

    Collection<SubscribeResult> results = new ArrayList<>();
    for (int i = 1; i < command.getProcessedCommand().size(); i++) {
      byte[] channelName = command.getProcessedCommand().get(i);
      SubscribeResult result =
          context.getPubSub().subscribe(channelName, context, context.getClient());
      results.add(result);
    }

    Collection<Collection<?>> items = new ArrayList<>();
    for (SubscribeResult result : results) {
      Collection<Object> item = new ArrayList<>();
      item.add("subscribe");
      item.add(result.getChannel());
      item.add(result.getChannelCount());
      items.add(item);
    }

    CountDownLatch subscriberLatch = context.getOrCreateEventLoopLatch();

    Runnable callback = () -> {
      Consumer<Boolean> innerCallback = success -> {
        for (SubscribeResult result : results) {
          if (result.getSubscription() != null) {
            if (success) {
              result.getSubscription().readyToPublish();
            } else {
              result.getSubscription().shutdown();
            }
          }
        }
        subscriberLatch.countDown();
      };
      context.changeChannelEventLoopGroup(context.getSubscriberGroup(), innerCallback);
    };

    RedisResponse response = RedisResponse.flattenedArray(items);
    response.setAfterWriteCallback(callback);

    return response;
  }

}
