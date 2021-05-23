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
 */

package org.apache.geode.redis.internal.proxy;

import java.util.Map;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.redis.ErrorRedisMessage;

public class MovedResponseHandler extends ChannelInboundHandlerAdapter {

  private final Map<HostPort, HostPort> mappings;
  private final Channel inboundChannel;

  public MovedResponseHandler(Channel inboundChannel, Map<HostPort, HostPort> mappings) {
    this.inboundChannel = inboundChannel;
    this.mappings = mappings;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
    if (msg instanceof ErrorRedisMessage) {
      String content = ((ErrorRedisMessage) msg).content();
      if (content.startsWith("MOVED")) {
        for (Map.Entry<HostPort, HostPort> entry : mappings.entrySet()) {
          String hostPort = entry.getKey().getHost() + ":" + entry.getKey().getPort();
          int index = content.indexOf(hostPort);
          if (index >= 0) {
            String newHostPort = entry.getValue().getHost() + ":" + entry.getValue().getPort();
            String response = content.substring(0, index) + newHostPort;
            inboundChannel.writeAndFlush(new ErrorRedisMessage(response));
            return;
          }
        }

        throw new IllegalStateException("Unmapped MOVED received: " + content);
      }
    }

    // Hand off to next handler
    ctx.fireChannelRead(msg);
  }
}
