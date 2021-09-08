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

import java.util.List;
import java.util.concurrent.CountDownLatch;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Client;

public abstract class AbstractSubscription implements Subscription {
  private final Client client;
  private final byte[] subscriptionName;
  // Before we are ready to publish we need to make sure that the response to the
  // SUBSCRIBE command has been sent back to the client.
  private final CountDownLatch readyForPublish = new CountDownLatch(1);
  private volatile boolean running = true;

  AbstractSubscription(Client client, byte[] subscriptionName) {
    this.client = client;
    this.subscriptionName = subscriptionName;
  }

  @Override
  public void readyToPublish() {
    readyForPublish.countDown();
  }

  @Override
  public void publishMessage(byte[] channel, byte[] message) {
    try {
      readyForPublish.await();
    } catch (InterruptedException e) {
      // we must be shutting down or registration failed
      Thread.currentThread().interrupt();
      shutdown();
    }

    if (running) {
      ChannelFuture writeResult = getClient().writeToChannel(constructResponse(channel, message));
      writeResult.addListener((ChannelFutureListener) f -> {
        if (f.cause() != null) {
          shutdown();
        }
      });
    }
  }

  @Override
  public Client getClient() {
    return client;
  }

  @Override
  public byte[] getSubscriptionName() {
    return subscriptionName;
  }

  /**
   * Most of the cleanup is done by a channel shutdown listener that calls
   * {@link PubSub#clientDisconnect(Client)}.
   * So this method only marks this subscription as no longer running.
   */
  private void shutdown() {
    running = false;
  }

  @VisibleForTesting
  boolean isShutdown() {
    return !running;
  }

  private RedisResponse constructResponse(byte[] channel, byte[] message) {
    return RedisResponse.array(createResponse(channel, message));
  }

  protected abstract List<Object> createResponse(byte[] channel, byte[] message);

}
