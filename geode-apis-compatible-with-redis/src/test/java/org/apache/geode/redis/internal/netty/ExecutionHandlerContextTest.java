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
package org.apache.geode.redis.internal.netty;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.redis.internal.RegionProvider;
import org.apache.geode.redis.internal.pubsub.PubSub;
import org.apache.geode.redis.internal.statistics.RedisStats;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class ExecutionHandlerContextTest {

  @Rule
  public ExecutorServiceRule executorServiceRule = new ExecutorServiceRule();

  private ExecutionHandlerContext executionHandlerContext;

  @Before
  public void setUp() {
    Channel channel = mock(Channel.class);
    RegionProvider regionProvider = mock(RegionProvider.class);
    PubSub pubsub = mock(PubSub.class);
    Supplier<Boolean> allowUnsupportedSupplier = () -> true;
    Runnable shutdownInvoker = mock(Runnable.class);
    RedisStats redisStats = mock(RedisStats.class);
    ExecutorService backgroundExecutor = executorServiceRule.getExecutorService();
    EventLoopGroup subscriberGroup = mock(EventLoopGroup.class);
    byte[] password = null;
    int serverPort = 0;
    DistributedMember member = mock(DistributedMember.class);
    executionHandlerContext = new ExecutionHandlerContext(channel, regionProvider, pubsub,
        allowUnsupportedSupplier, shutdownInvoker, redisStats, backgroundExecutor, subscriberGroup,
        password, serverPort, member);
  }

  @Test
  public void channelIsNotClosedWhenIOExceptionIsThrownFromCommandExecution() throws Exception {
    ChannelHandlerContext context = mock(ChannelHandlerContext.class);
    Channel channel = mock(Channel.class);
    when(context.channel()).thenReturn(channel);

    Command throwsIOExceptionCommand = mock(Command.class);
    when(throwsIOExceptionCommand.getChannelHandlerContext()).thenReturn(context);
    when(throwsIOExceptionCommand.execute(any())).thenThrow(new IOException());

    executionHandlerContext.channelRead(context, throwsIOExceptionCommand);

    executorServiceRule.getExecutorService().shutdown();
    await().until(() -> executorServiceRule.getExecutorService().isTerminated());

    verify(channel, never()).close();
  }
}
