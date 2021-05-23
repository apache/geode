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

import java.util.concurrent.CountDownLatch;

import io.netty.channel.ChannelFuture;

import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Client;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public abstract class AbstractSubscription implements Subscription {
  private final Client client;
  private final ExecutionHandlerContext context;

  // Two things have to happen before we are ready to publish:
  // 1 - we need to make sure the subscriber has switched EventLoopGroups
  // 2 - the response to the SUBSCRIBE command has been submitted to the client
  private final CountDownLatch readyForPublish = new CountDownLatch(1);
  private final Subscriptions subscriptions;
  private boolean running = true;

  AbstractSubscription(Client client, ExecutionHandlerContext context,
      Subscriptions subscriptions) {
    if (client == null) {
      throw new IllegalArgumentException("client cannot be null");
    }
    if (context == null) {
      throw new IllegalArgumentException("context cannot be null");
    }
    if (subscriptions == null) {
      throw new IllegalArgumentException("subscriptions cannot be null");
    }
    this.client = client;
    this.context = context;
    this.subscriptions = subscriptions;

    client.addShutdownListener(future -> shutdown());
  }

  @Override
  public void readyToPublish() {
    readyForPublish.countDown();
  }

  @Override
  public void publishMessage(byte[] channel, byte[] message,
      PublishResultCollector publishResultCollector) {
    try {
      readyForPublish.await();
    } catch (InterruptedException e) {
      // we must be shutting down or registration failed
      // noinspection ResultOfMethodCallIgnored
      Thread.interrupted();
      running = false;
    }

    if (running) {
      writeToChannel(constructResponse(channel, message), publishResultCollector);
    } else {
      publishResultCollector.failure(client);
    }
  }

  @Override
  public synchronized void shutdown() {
    running = false;
    subscriptions.remove(client);
    // release any threads currently waiting to publish
    readyToPublish();
  }

  public Client getClient() {
    return client;
  }

  @Override
  public boolean matchesClient(Client client) {
    return this.client.equals(client);
  }

  private RedisResponse constructResponse(byte[] channel, byte[] message) {
    return RedisResponse.array(createResponse(channel, message));
  }

  /**
   * We want to determine if the response, to the client, resulted in an error - for example if the
   * client has disconnected and the write fails. In such cases we need to be able to notify the
   * caller.
   */
  private void writeToChannel(RedisResponse response, PublishResultCollector resultCollector) {

    ChannelFuture result = context.writeToChannel(response)
        .syncUninterruptibly();

    if (result.cause() == null) {
      resultCollector.success();
    } else {
      resultCollector.failure(client);
    }

  }
}
