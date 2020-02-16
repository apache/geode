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

package org.apache.geode.redis.internal;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.redis.internal.org.apache.hadoop.fs.GlobPattern;

/**
 * Concrete class that manages publish and subscribe functionality. Since Redis subscriptions
 * require a persistent connection we need to have a way to track the existing clients that are
 * expecting to receive published messages.
 */
public class PubSubImpl implements PubSub {
  public static final String REDIS_PUB_SUB_FUNCTION_ID = "redisPubSubFunctionID";

  private final Subscriptions subscriptions;

  public PubSubImpl(Subscriptions subscriptions) {
    this.subscriptions = subscriptions;

    registerPublishFunction();
  }

  @Override
  public long publish(String channel, String message) {
    ResultCollector<String[], List<Long>> subscriberCountCollector = FunctionService
        .onMembers()
        .setArguments(new String[] {channel, message})
        .execute(REDIS_PUB_SUB_FUNCTION_ID);

    List<Long> subscriberCounts = subscriberCountCollector.getResult();

    return subscriberCounts.stream().mapToLong(x -> x).sum();
  }

  @Override
  public long subscribe(String channel, ExecutionHandlerContext context, Client client) {
    if (subscriptions.exists(channel, client)) {
      return subscriptions.findSubscriptions(client).size();
    }
    Subscription subscription = new ChannelSubscription(client, channel, context);
    subscriptions.add(subscription);
    return subscriptions.findSubscriptions(client).size();
  }

  @Override
  public long psubscribe(GlobPattern pattern, ExecutionHandlerContext context, Client client) {
    if (subscriptions.exists(pattern, client)) {
      return subscriptions.findSubscriptions(client).size();
    }
    Subscription subscription = new PatternSubscription(client, pattern, context);
    subscriptions.add(subscription);

    return subscriptions.findSubscriptions(client).size();
  }

  private void registerPublishFunction() {
    FunctionService.registerFunction(new Function<String[]>() {
      @Override
      public String getId() {
        return REDIS_PUB_SUB_FUNCTION_ID;
      }

      @Override
      public void execute(FunctionContext<String[]> context) {
        String[] publishMessage = context.getArguments();
        long subscriberCount = publishMessageToSubscribers(publishMessage[0], publishMessage[1]);
        context.getResultSender().lastResult(subscriberCount);
      }
    });
  }

  @Override
  public long unsubscribe(String channel, Client client) {
    this.subscriptions.remove(channel, client);
    return this.subscriptions.findSubscriptions(client).size();
  }

  @Override
  public long punsubscribe(GlobPattern pattern, Client client) {
    this.subscriptions.remove(pattern, client);
    return this.subscriptions.findSubscriptions(client).size();
  }

  @VisibleForTesting
  long publishMessageToSubscribers(String channel, String message) {

    Map<Boolean, List<PublishResult>> results = this.subscriptions
        .findSubscriptions(channel)
        .stream()
        .map(subscription -> subscription.publishMessage(channel, message))
        .collect(Collectors.partitioningBy(PublishResult::isSuccessful));

    prune(results.get(false));

    return results.get(true).size();
  }

  private void prune(List<PublishResult> failedSubscriptions) {
    failedSubscriptions.forEach(publishResult -> {
      Client client = publishResult.getClient();
      if (client.isDead()) {
        subscriptions.remove(client);
      }
    });
  }
}
