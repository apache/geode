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
import java.util.HashMap;
import java.util.Map;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.redis.RedisArrayAggregator;
import io.netty.handler.codec.redis.RedisBulkStringAggregator;
import io.netty.handler.codec.redis.RedisDecoder;
import io.netty.handler.codec.redis.RedisEncoder;
import io.netty.util.concurrent.DefaultThreadFactory;


/**
 * This proxy handles mangling Redis responses in whatever way is necessary. In the case of
 * creating a docker-based redis cluster, we need to translate internal addresses into external
 * addresses - see {@link RedisProxyInboundHandler#channelRead(ChannelHandlerContext, Object)}.
 */
public final class RedisProxy {

  private final EventLoopGroup bossGroup;
  private final EventLoopGroup workerGroup;
  private final int exposedPort;
  private final Map<HostPort, HostPort> mappings = new HashMap<>();

  public RedisProxy(int targetPort) {
    bossGroup = new NioEventLoopGroup(1);
    workerGroup = new NioEventLoopGroup(2, new DefaultThreadFactory("RedisProxy"));

    ChannelFuture future = new ServerBootstrap().group(bossGroup, workerGroup)
        .channel(NioServerSocketChannel.class)
        .childHandler(new ChannelInitializer<SocketChannel>() {
          @Override
          protected void initChannel(SocketChannel ch) {
            ChannelPipeline p = ch.pipeline();
            p.addLast(new RedisEncoder());
            p.addLast(new RedisDecoder());
            p.addLast(new RedisBulkStringAggregator());
            p.addLast(new RedisArrayAggregator());
            p.addLast(new RedisProxyInboundHandler(ch, "127.0.0.1", targetPort, mappings));
          }
        })
        .childOption(ChannelOption.AUTO_READ, false)
        .bind(0);
    future.awaitUninterruptibly();

    exposedPort = ((InetSocketAddress) future.channel().localAddress()).getPort();
  }

  public int getExposedPort() {
    return exposedPort;
  }

  public void configure(Map<HostPort, HostPort> mappings) {
    this.mappings.putAll(mappings);
  }

  public void stop() {
    bossGroup.shutdownGracefully();
    workerGroup.shutdownGracefully();
  }

}
