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
import java.util.concurrent.ExecutionException;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import org.apache.logging.log4j.Logger;

import org.apache.geode.logging.internal.log4j.api.LogService;

class Subscriber {
  private static final Logger logger = LogService.getLogger();
  public final Client client;
  public final String channel;
  private ExecutionHandlerContext context;

  public Subscriber(Client client, String channel, ExecutionHandlerContext context) {
    this.client = client;
    this.channel = channel;
    this.context = context;
  }

  public boolean isEqualTo(String channel, Client client) {
    if (channel == null || client == null) {
      return false;
    }
    return channel.equals(this.channel) && client.equals(this.client);
  }

  public boolean publishMessage(String channel, String message) {
    ByteBuf messageByteBuffer = constructResponse(channel, message);
    if (messageByteBuffer == null) {
      return false;
    }

    return writeToChannelSynchronously(messageByteBuffer);
  }

  private ByteBuf constructResponse(String channel, String message) {
    ByteBuf messageByteBuffer;
    try {
      messageByteBuffer = Coder.getArrayResponse(context.getByteBufAllocator(),
          Arrays.asList("message", channel, message));
    } catch (CoderException e) {
      logger.warn("Unable to encode publish message", e);
      return null;
    }
    return messageByteBuffer;
  }

  /**
   * This method turns the response into a synchronous call. We want to
   * determine if the response, to the client, resulted in an error - for example if the client has
   * disconnected and the write fails. In such cases we need to be able to notify the caller.
   */
  private boolean writeToChannelSynchronously(ByteBuf messageByteBuffer) {
    ChannelFuture channelFuture = context.writeToChannel(messageByteBuffer);

    try {
      channelFuture.get();
    } catch (InterruptedException | ExecutionException e) {
      return false;
    }
    return channelFuture.cause() == null;
  }
}
