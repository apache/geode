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

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.logging.log4j.Logger;

import org.apache.geode.logging.internal.log4j.api.LogService;

public class RedisProxyOutboundHandler extends ChannelInboundHandlerAdapter {

  private static final Logger logger = LogService.getLogger();
  private final Queue<RedisResponseProcessor> processors = new LinkedBlockingQueue<>();
  private final Channel inboundChannel;

  public RedisProxyOutboundHandler(Channel inboundChannel) {
    this.inboundChannel = inboundChannel;
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) {
    ctx.read();
  }

  @Override
  public void channelRead(final ChannelHandlerContext ctx, Object msg) {
    RedisResponseProcessor processor = processors.poll();
    if (processor == null) {
      logger.warn("No processor queued - will use default noop processor");
      processor = NoopRedisResponseProcessor.INSTANCE;
    }

    inboundChannel.writeAndFlush(processor.process(msg, ctx.channel()))
        .addListener((ChannelFutureListener) future -> {
          if (future.isSuccess()) {
            inboundChannel.read();
            // ctx.channel().read();
          } else {
            logger.error("Failed to return response on inboundChannel", future.cause());
            future.channel().close();
          }
        });
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) {
    RedisProxyInboundHandler.closeOnFlush(inboundChannel);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    if (!cause.getMessage().contains("Connection reset by peer")) {
      logger.info(cause);
    }
    RedisProxyInboundHandler.closeOnFlush(ctx.channel());
  }

  public void addResponseProcessor(RedisResponseProcessor processor) {
    processors.add(processor);
  }
}
