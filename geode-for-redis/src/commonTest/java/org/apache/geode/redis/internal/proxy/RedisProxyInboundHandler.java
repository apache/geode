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

import java.net.InetSocketAddress;
import java.util.Map;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.RedisArrayAggregator;
import io.netty.handler.codec.redis.RedisBulkStringAggregator;
import io.netty.handler.codec.redis.RedisDecoder;
import io.netty.handler.codec.redis.RedisEncoder;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.util.CharsetUtil;
import org.apache.logging.log4j.Logger;

import org.apache.geode.logging.internal.log4j.api.LogService;

public class RedisProxyInboundHandler extends ChannelInboundHandlerAdapter {

  private static final Logger logger = LogService.getLogger();
  private final String remoteHost;
  private final int remotePort;
  private final Map<HostPort, HostPort> mappings;
  private Channel outboundChannel;
  private Channel inboundChannel;
  private RedisProxyOutboundHandler outboundHandler;
  private final ClusterSlotsResponseProcessor slotsResponseProcessor;
  private final ClusterNodesResponseProcessor nodesResponseProcessor;
  private MovedResponseHandler movedResponseHandler;

  public RedisProxyInboundHandler(Channel inboundChannel, String remoteHost, int remotePort,
      Map<HostPort, HostPort> mappings) {
    this.inboundChannel = inboundChannel;
    this.remoteHost = remoteHost;
    this.remotePort = remotePort;
    this.mappings = mappings;
    this.slotsResponseProcessor = new ClusterSlotsResponseProcessor(mappings);
    this.nodesResponseProcessor = new ClusterNodesResponseProcessor(mappings);
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) {
    Channel inboundChannel = ctx.channel();
    outboundHandler = new RedisProxyOutboundHandler(inboundChannel);
    movedResponseHandler = new MovedResponseHandler(inboundChannel, mappings);

    // Start the connection attempt.
    Bootstrap b = new Bootstrap();
    b.group(inboundChannel.eventLoop())
        .channel(ctx.channel().getClass())
        .handler(new ChannelInitializer<SocketChannel>() {
          @Override
          protected void initChannel(SocketChannel ch) {
            ChannelPipeline p = ch.pipeline();
            p.addLast(new RedisEncoder());
            p.addLast(new RedisDecoder());
            p.addLast(new RedisBulkStringAggregator());
            p.addLast(new RedisArrayAggregator());
            p.addLast(movedResponseHandler);
            p.addLast(outboundHandler);
          }
        });
    ChannelFuture f = b.connect(remoteHost, remotePort);
    outboundChannel = f.channel();
    f.addListener((ChannelFutureListener) future -> {
      if (future.isSuccess()) {
        InetSocketAddress target = (InetSocketAddress) inboundChannel.localAddress();
        for (Map.Entry<HostPort, HostPort> entry : mappings.entrySet()) {
          HostPort exposed = entry.getValue();
          if (target.getPort() == exposed.getPort()) {
            logger.info("Established proxy connection {} -> {} -> {}",
                inboundChannel.remoteAddress(),
                inboundChannel.localAddress(),
                entry.getKey());
            break;
          }
        }
        inboundChannel.config().setAutoRead(true);
      } else {
        logger.error("Failed to connect", future.cause());
        inboundChannel.close();
      }
    });
  }

  /**
   * Any redis commands which return an IP or port which needs to be translated into an external
   * IP/port need to be added to this method and an appropriate {@link RedisResponseProcessor} needs
   * to be implemented.
   * <p/>
   * Note that each inbound command has an explicit outbound processor associated. Commands that do
   * not need any processing are simply handled by a {@link NoopRedisResponseProcessor}.
   */
  @Override
  public void channelRead(final ChannelHandlerContext ctx, Object msg) {
    if (outboundChannel.isActive()) {
      // Commands always consist of an array of bulk strings
      ArrayRedisMessage rMessage = (ArrayRedisMessage) msg;
      String command = getArg(rMessage, 0);

      switch (command.toLowerCase()) {
        case "cluster":
          String sub = getArg(rMessage, 1);
          if ("slots".equals(sub)) {
            outboundHandler.addResponseProcessor(slotsResponseProcessor);
          } else if ("nodes".equals(sub)) {
            outboundHandler.addResponseProcessor(nodesResponseProcessor);
          }
          break;
        case "hello":
          // Lettuce tries to use RESP 3 if possible but Netty's Redis codecs can't handle that yet.
          // We implicitly fake the client into thinking this is an older Redis so that it uses
          // RESP 2.
          RedisMessage error = new ErrorRedisMessage("ERR unknown command");
          inboundChannel.writeAndFlush(error);
          return;
        default:
          outboundHandler.addResponseProcessor(NoopRedisResponseProcessor.INSTANCE);
      }

      outboundChannel.writeAndFlush(msg)
          .addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
              // was able to flush out data, start to read the next chunk
              ctx.channel().read();
            } else {
              logger.error("Failed to write to outboundChannel", future.cause());
              future.channel().close();
            }
          });
    }
  }

  private String getArg(ArrayRedisMessage redisArray, int index) {
    if (index >= redisArray.children().size()) {
      return null;
    }
    return ((FullBulkStringRedisMessage) redisArray.children().get(index))
        .content().toString(CharsetUtil.UTF_8).toLowerCase();
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) {
    if (outboundChannel != null) {
      closeOnFlush(outboundChannel);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    if (!cause.getMessage().contains("Connection reset by peer")) {
      logger.info(cause);
    }
    closeOnFlush(ctx.channel());
  }

  /**
   * Closes the specified channel after all queued write requests are flushed.
   */
  static void closeOnFlush(Channel ch) {
    if (ch.isActive()) {
      ch.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
    }
  }
}
