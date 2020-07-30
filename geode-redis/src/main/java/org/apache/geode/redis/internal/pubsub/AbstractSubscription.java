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

import io.netty.channel.ChannelFutureListener;
import org.apache.logging.log4j.Logger;

import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Client;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public abstract class AbstractSubscription implements Subscription {
  private static final Logger logger = LogService.getLogger();
  private final Client client;
  private final ExecutionHandlerContext context;
  private final CountDownLatch readyForPublish = new CountDownLatch(1);

  AbstractSubscription(Client client, ExecutionHandlerContext context) {
    if (client == null) {
      throw new IllegalArgumentException("client cannot be null");
    }
    if (context == null) {
      throw new IllegalArgumentException("context cannot be null");
    }
    this.client = client;
    this.context = context;
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
      Thread.interrupted();
      // we must be shutting down; so just fall through and do the publish
    }
    writeToChannel(constructResponse(channel, message), publishResultCollector);
  }

  Client getClient() {
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
   * We want to determine if the response, to the client, resulted in an error - for example if
   * the client has disconnected and the write fails. In such cases we need to be able to notify
   * the caller.
   */
  private void writeToChannel(RedisResponse response, PublishResultCollector resultCollector) {
    context.writeToChannel(response)
        .addListener((ChannelFutureListener) future -> {
          if (future.cause() == null) {
            resultCollector.success();
          } else {
            resultCollector.failure(client);
          }
        });
  }
}
