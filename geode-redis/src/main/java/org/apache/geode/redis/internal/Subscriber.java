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

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

class Subscriber {
  public final Client client;
  public final String channel;
  private ExecutionHandlerContext context;

  public Subscriber(Client client, String channel,
      ExecutionHandlerContext context) {

    this.client = client;
    this.channel = channel;
    this.context = context;
  }

  public boolean isEqualTo(String channel, Client client) {
    return channel.equals(this.channel) && client.equals(this.client);
  }

  public boolean publishMessage(String channel, String message) {
    ByteBuf messageByteBuffer;
    try {
      messageByteBuffer = Coder.getArrayResponse(context.getByteBufAllocator(),
          Arrays.asList("message", channel, message));
    } catch (CoderException e) {
      throw new RuntimeException(e);
    }

    CountDownLatch latch = new CountDownLatch(1);

    ChannelFutureListener channelFutureListener = future -> latch.countDown();

    ChannelFuture channelFuture =
        context.writeToChannelWithListener(messageByteBuffer, channelFutureListener);

    try {
      latch.await(1, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    return channelFuture.cause() == null;
  }
}
