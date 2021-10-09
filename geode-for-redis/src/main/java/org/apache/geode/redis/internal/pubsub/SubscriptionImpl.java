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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.redis.internal.netty.Client;

/**
 * To save memory this class no longer stores its client and name
 * since those are kept in the data structures that store Subscriptions.
 */
public class SubscriptionImpl implements Subscription {
  private final Client client;
  // Before we are ready to publish we need to make sure that the response to the
  // SUBSCRIBE command has been sent back to the client.
  private CountDownLatch readyForPublish = new CountDownLatch(1);
  private volatile boolean running = true;

  public SubscriptionImpl(Client client) {
    this.client = client;
  }

  @Override
  public void readyToPublish() {
    readyForPublish.countDown();
    readyForPublish = null;
  }

  private void waitUntilReadyToPublish() {
    CountDownLatch latch = readyForPublish;
    if (latch != null) {
      try {
        latch.await();
      } catch (InterruptedException e) {
        // we must be shutting down or registration failed
        Thread.currentThread().interrupt();
        shutdown();
      }
    }
  }

  @Override
  public Client getClient() {
    return client;
  }

  @Override
  public void writeBufferToChannel(ByteBuf writeBuf) {
    waitUntilReadyToPublish();
    if (running) {
      client.writeBufferToChannel(writeBuf).addListener(this);
    }
  }

  @Override
  public ByteBuf getChannelWriteBuffer() {
    return client.getChannelWriteBuffer();
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

  //////////////////////// ChannelFutureListener methods ////////////////////////////////

  @Override
  public void operationComplete(ChannelFuture future) {
    if (future.cause() != null) {
      shutdown();
    }
  }
}
