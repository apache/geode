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

package org.apache.geode.redis.internal.pubsub;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.partition.PartitionMemberInfo;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.cache.partition.PartitionRegionInfo;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.redis.internal.data.ByteArrayWrapper;
import org.apache.geode.redis.internal.data.RedisData;
import org.apache.geode.redis.internal.executor.GlobPattern;
import org.apache.geode.redis.internal.netty.Client;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

/**
 * Concrete class that manages publish and subscribe functionality. Since Redis subscriptions
 * require a persistent connection we need to have a way to track the existing clients that are
 * expecting to receive published messages.
 */
public class PubSubImpl implements PubSub {
  public static final String REDIS_PUB_SUB_FUNCTION_ID = "redisPubSubFunctionID";

  private static final Logger logger = LogService.getLogger();

  private final Subscriptions subscriptions;

  public PubSubImpl(Subscriptions subscriptions) {
    this.subscriptions = subscriptions;

    registerPublishFunction();
  }

  @Override
  public long publish(
      Region<ByteArrayWrapper, RedisData> dataRegion,
      String channel, byte[] message) {
    PartitionRegionInfo info = PartitionRegionHelper.getPartitionRegionInfo(dataRegion);
    Set<DistributedMember> membersWithDataRegion = new HashSet<>();
    for (PartitionMemberInfo memberInfo : info.getPartitionMemberInfo()) {
      membersWithDataRegion.add(memberInfo.getDistributedMember());
    }
    @SuppressWarnings("unchecked")
    ResultCollector<String[], List<Long>> subscriberCountCollector = FunctionService
        .onMembers(membersWithDataRegion)
        .setArguments(new Object[] {channel, message})
        .execute(REDIS_PUB_SUB_FUNCTION_ID);

    List<Long> subscriberCounts = null;

    try {
      subscriberCounts = subscriberCountCollector.getResult();
    } catch (Exception e) {
      logger.warn("Failed to execute publish function {}", e.getMessage());
      return 0;
    }

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
    FunctionService.registerFunction(new InternalFunction<Object[]>() {
      @Override
      public String getId() {
        return REDIS_PUB_SUB_FUNCTION_ID;
      }

      @Override
      public void execute(FunctionContext<Object[]> context) {
        Object[] publishMessage = context.getArguments();
        long subscriberCount =
            publishMessageToSubscribers((String) publishMessage[0], (byte[]) publishMessage[1]);
        context.getResultSender().lastResult(subscriberCount);
      }

      /**
       * Since the publish process uses an onMembers function call, we don't want to re-publish
       * to members if one fails.
       * TODO: Revisit this in the event that we instead use an onMember call against individual
       * members.
       */
      @Override
      public boolean isHA() {
        return false;
      }
    });
  }

  @Override
  public long unsubscribe(String channel, Client client) {
    subscriptions.remove(channel, client);
    return subscriptions.findSubscriptions(client).size();
  }

  @Override
  public long punsubscribe(GlobPattern pattern, Client client) {
    subscriptions.remove(pattern, client);
    return subscriptions.findSubscriptions(client).size();
  }

  @VisibleForTesting
  long publishMessageToSubscribers(String channel, byte[] message) {

    List<Subscription> foundSubscriptions = subscriptions
        .findSubscriptions(channel);
    if (foundSubscriptions.isEmpty()) {
      return 0;
    }

    PublishResultCollector publishResultCollector =
        new PublishResultCollector(foundSubscriptions.size(), subscriptions);

    foundSubscriptions.forEach(
        subscription -> subscription.publishMessage(channel, message, publishResultCollector));

    return publishResultCollector.getSuccessCount();
  }

}
