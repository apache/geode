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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;

/**
 * Concrete class that manages publish and subscribe functionality. Since Redis subscriptions
 * require a persistent connection we need to have a way to track the existing clients that are
 * expecting to receive published messages.
 */
public class PubSubImpl implements PubSub {
  public static final String REDIS_PUB_SUB_FUNCTION_ID = "redisPubSubFunctionID";

  Subscribers subscribers = new Subscribers();

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
    if (subscribers.exists(channel, client)) {
      return subscribers.findSubscribers(client).size();
    }
    Subscriber subscriber = new Subscriber(client, channel, context);
    subscribers.add(subscriber);
    return subscribers.findSubscribers(client).size();
  }

  public void registerPublishFunction() {
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
    this.subscribers.remove(channel, client);
    return this.subscribers.findSubscribers(client).size();
  }

  private long publishMessageToSubscribers(String channel, String message) {
    Map<Boolean, List<Subscriber>> results = this.subscribers
        .findSubscribers(channel)
        .stream()
        .collect(
            Collectors.partitioningBy(subscriber -> subscriber.publishMessage(channel, message)));

    prune(results.get(false));

    return results.get(true).size();
  }

  private void prune(List<Subscriber> failedSubscribers) {
    failedSubscribers.forEach(subscriber -> {
      if (subscriber.client.isDead()) {
        subscribers.remove(subscriber.client);
      }
    });
  }

  private class Subscribers {
    List<Subscriber> subscribers = new ArrayList<>();

    private boolean exists(String channel, Client client) {
      return subscribers.stream().anyMatch(subscriber -> subscriber.isEqualTo(channel, client));
    }

    private List<Subscriber> findSubscribers(Client client) {
      return subscribers.stream().filter(subscriber -> subscriber.client.equals(client))
          .collect(Collectors.toList());
    }

    private List<Subscriber> findSubscribers(String channel) {
      return subscribers.stream().filter(subscriber -> subscriber.channel.equals(channel))
          .collect(Collectors.toList());
    }

    public void add(Subscriber subscriber) {
      this.subscribers.add(subscriber);
    }

    public void remove(String channel, Client client) {
      this.subscribers.removeIf(subscriber -> subscriber.isEqualTo(channel, client));
    }

    public void remove(Client client) {
      this.subscribers.removeIf(subscriber -> subscriber.client.equals(client));
    }
  }
}
