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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Client;

public abstract class AbstractSubscription implements Subscription {
  private final Client client;
  private final byte[] subscriptionName;
  // Before we are ready to publish we need to make sure that the response to the
  // SUBSCRIBE command has been sent back to the client.
  private final CountDownLatch readyForPublish = new CountDownLatch(1);
  private boolean running = true;

  AbstractSubscription(Client client,
      Subscriptions subscriptions, byte[] subscriptionName) {
    this.client = client;
    this.subscriptionName = subscriptionName;

    getClient().addShutdownListener(future -> shutdown(subscriptions));
  }

  @Override
  public void readyToPublish() {
    readyForPublish.countDown();
  }

  @Override
  public void publishMessage(byte[] channel, byte[] message, Subscriptions subscriptions) {
    try {
      readyForPublish.await();
    } catch (InterruptedException e) {
      // we must be shutting down or registration failed
      Thread.currentThread().interrupt();
      running = false;
    }

    if (running) {
      ChannelFuture writeResult = getClient().writeToChannel(constructResponse(channel, message));
      System.out.println("writeREsult=" + writeResult);
      writeResult.addListener((ChannelFutureListener) f -> {
        if (f.cause() != null) {
          shutdown(subscriptions);
        }
      });
    }
  }

  @Override
  public Client getClient() {
    return client;
  }

  @Override
  public boolean isEqualTo(byte[] subscriptionName, Client client) {
    return getClient().equals(client) && Arrays.equals(getSubscriptionName(), subscriptionName);
  }

  @Override
  public byte[] getSubscriptionName() {
    return subscriptionName;
  }

  private RedisResponse constructResponse(byte[] channel, byte[] message) {
    return RedisResponse.array(createResponse(channel, message));
  }

  private synchronized void shutdown(Subscriptions subscriptions) {
    running = false;
    subscriptions.remove(getClient());
  }

  protected abstract List<Object> createResponse(byte[] channel, byte[] message);

}
