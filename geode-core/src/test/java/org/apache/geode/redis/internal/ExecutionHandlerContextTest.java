package org.apache.geode.redis.internal;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

import org.apache.geode.LogWriter;
import org.apache.geode.cache.Cache;
import org.apache.geode.redis.GeodeRedisServer;
import org.junit.Test;
import org.mockito.Mockito;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.util.concurrent.EventExecutor;

/**
 * Test cases for ExecutionHandlerContext
 * 
 * @author Gregory Green
 *
 */
public class ExecutionHandlerContextTest {
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
    LogWriter logWriter = Mockito.mock(LogWriter.class);
    Command msg = Mockito.mock(Command.class);
    RegionProvider regionProvider = Mockito.mock(RegionProvider.class);
    GeodeRedisServer server = Mockito.mock(GeodeRedisServer.class);
    RedisCommandType redisCommandType = Mockito.mock(RedisCommandType.class);

    Mockito.when(cache.getLogger()).thenReturn(logWriter);
    Mockito.when(ch.pipeline()).thenReturn(channelPipeline);
    Mockito.when(channelPipeline.lastContext()).thenReturn(channelHandlerContext);
    Mockito.when(channelHandlerContext.executor()).thenReturn(eventExecutor);


    byte[] pwd = null;
    ExecutionHandlerContext handler =
        new ExecutionHandlerContext(ch, cache, regionProvider, server, pwd);

    Mockito.when(msg.getCommandType()).thenReturn(redisCommandType);
    Executor exec = Mockito.mock(Executor.class);
    Mockito.when(redisCommandType.getExecutor()).thenReturn(exec);

    ChannelHandlerContext ctx = null;
    handler.channelRead(ctx, msg);

  }

}
