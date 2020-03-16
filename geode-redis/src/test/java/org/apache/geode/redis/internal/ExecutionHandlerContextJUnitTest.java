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

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.util.concurrent.EventExecutor;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.geode.cache.Cache;
import org.apache.geode.redis.GeodeRedisServer;

/**
 * Test cases for ExecutionHandlerContext
 */
public class ExecutionHandlerContextJUnitTest {
  /**
   *
   * @throws Exception the exception
   */
  @Test
  public void testChannelReadChannelHandlerContextObject() throws Exception {
    Cache cache = Mockito.mock(Cache.class);

    Channel ch = Mockito.mock(Channel.class);
    ChannelPipeline channelPipeline = Mockito.mock(ChannelPipeline.class);
    EventExecutor eventExecutor = Mockito.mock(EventExecutor.class);
    ChannelHandlerContext channelHandlerContext = Mockito.mock(ChannelHandlerContext.class);
    @SuppressWarnings("deprecation")
    org.apache.geode.LogWriter logWriter = Mockito.mock(org.apache.geode.LogWriter.class);
    Command msg = Mockito.mock(Command.class);
    RegionProvider regionProvider = Mockito.mock(RegionProvider.class);
    GeodeRedisServer server = Mockito.mock(GeodeRedisServer.class);
    RedisCommandType redisCommandType = Mockito.mock(RedisCommandType.class);
    KeyRegistrar keyRegistrar = Mockito.mock(KeyRegistrar.class);
    PubSub pubSub = Mockito.mock(PubSub.class);
    RedisLockService lockService = Mockito.mock(RedisLockService.class);

    Mockito.when(cache.getLogger()).thenReturn(logWriter);
    Mockito.when(ch.pipeline()).thenReturn(channelPipeline);
    Mockito.when(channelPipeline.lastContext()).thenReturn(channelHandlerContext);
    Mockito.when(channelHandlerContext.executor()).thenReturn(eventExecutor);

    byte[] pwd = null;
    ExecutionHandlerContext handler =
        new ExecutionHandlerContext(ch, cache, regionProvider, server, pwd, keyRegistrar, pubSub,
            lockService);

    Mockito.when(msg.getCommandType()).thenReturn(redisCommandType);
    Executor exec = Mockito.mock(Executor.class);
    Mockito.when(redisCommandType.getExecutor()).thenReturn(exec);

    ChannelHandlerContext ctx = null;
    handler.channelRead(ctx, msg);

  }

}
