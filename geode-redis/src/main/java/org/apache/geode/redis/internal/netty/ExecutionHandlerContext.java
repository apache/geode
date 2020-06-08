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
package org.apache.geode.redis.internal.netty;

import static org.apache.geode.redis.internal.RedisCommandType.SUBSCRIBE;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.codec.DecoderException;
import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.redis.internal.GeodeRedisServer;
import org.apache.geode.redis.internal.ParameterRequirements.RedisParametersMismatchException;
import org.apache.geode.redis.internal.RedisCommandType;
import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.redis.internal.RegionProvider;
import org.apache.geode.redis.internal.data.RedisDataTypeMismatchException;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.pubsub.PubSub;

/**
 * This class extends {@link ChannelInboundHandlerAdapter} from Netty and it is the last part of the
 * channel pipeline. The {@link ByteToCommandDecoder} forwards a {@link Command} to this class which
 * executes it and sends the result back to the client. Additionally, all exception handling is done
 * by this class.
 * <p>
 * Besides being part of Netty's pipeline, this class also serves as a context to the execution of a
 * command. It provides access to the {@link RegionProvider} and anything else an executing {@link
 * Command} may need.
 */
public class ExecutionHandlerContext extends ChannelInboundHandlerAdapter {

  private static final Logger logger = LogService.getLogger();

  private final Cache cache;
  private final GeodeRedisServer server;
  private final Channel channel;
  private final EventLoopGroup subscriberEventLoopGroup;
  private final AtomicBoolean needChannelFlush;
  private final ByteBufAllocator byteBufAllocator;
  private final RegionProvider regionProvider;
  private final byte[] authPassword;

  private boolean isAuthenticated;

  private final PubSub pubSub;

  public PubSub getPubSub() {
    return pubSub;
  }

  /**
   * Default constructor for execution contexts.
   *
   * @param channel Channel used by this context, should be one to one
   * @param cache The Geode cache instance of this vm
   * @param regionProvider The region provider of this context
   * @param server Instance of the server it is attached to, only used so that any execution
   *        can initiate a shutdown
   * @param password Authentication password for each context, can be null
   */
  public ExecutionHandlerContext(Channel channel, Cache cache, RegionProvider regionProvider,
      GeodeRedisServer server, byte[] password,
      PubSub pubSub,
      EventLoopGroup subscriberEventLoopGroup) {
    this.pubSub = pubSub;
    if (channel == null || cache == null || regionProvider == null || server == null) {
      throw new IllegalArgumentException("Only the authentication password may be null");
    }
    this.cache = cache;
    this.server = server;
    this.channel = channel;
    this.subscriberEventLoopGroup = subscriberEventLoopGroup;
    this.needChannelFlush = new AtomicBoolean(false);
    this.byteBufAllocator = this.channel.alloc();
    this.regionProvider = regionProvider;
    this.authPassword = password;
    this.isAuthenticated = password != null ? false : true;

  }

  private void flushChannel() {
    while (needChannelFlush.getAndSet(false)) {
      channel.flush();
    }
  }

  public ChannelFuture writeToChannel(ByteBuf message) {
    return channel.writeAndFlush(message, channel.newPromise());
  }

  public ChannelFuture writeToChannel(RedisResponse response) {
    return channel.writeAndFlush(response.encode(byteBufAllocator), channel.newPromise());
  }

  /**
   * This will handle the execution of received commands
   */
  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    Command command = (Command) msg;
    try {
      if (logger.isDebugEnabled()) {
        logger.debug("Executing Redis command: {}", command);
      }

      executeCommand(ctx, command);
    } catch (Exception e) {
      logger.warn("Execution of Redis command {} failed: {}", command, e);
      throw e;
    }

  }

  /**
   * Exception handler for the entire pipeline
   */
  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    if (cause instanceof IOException) {
      channelInactive(ctx);
      return;
    }
    writeToChannel(getExceptionResponse(ctx, cause));
  }

  private RedisResponse getExceptionResponse(ChannelHandlerContext ctx, Throwable cause) {
    RedisResponse response;

    if (cause instanceof FunctionException) {
      Throwable th = cause.getCause();
      if (th == null) {
        FunctionException functionException = (FunctionException) cause;
        if (functionException.getExceptions() != null) {
          th = functionException.getExceptions().get(0);
        }
      }
      if (th != null) {
        cause = th;
      }
    }

    if (cause instanceof NumberFormatException) {
      response = RedisResponse.error(cause.getMessage());
    } else if (cause instanceof ArithmeticException) {
      response = RedisResponse.error(cause.getMessage());
    } else if (cause instanceof RedisDataTypeMismatchException) {
      response = RedisResponse.wrongType(cause.getMessage());
    } else if (cause instanceof DecoderException
        && cause.getCause() instanceof RedisCommandParserException) {
      response = RedisResponse.error(RedisConstants.PARSING_EXCEPTION_MESSAGE);

    } else if (cause instanceof InterruptedException || cause instanceof CacheClosedException) {
      response = RedisResponse.error(RedisConstants.SERVER_ERROR_SHUTDOWN);
    } else if (cause instanceof IllegalStateException
        || cause instanceof RedisParametersMismatchException) {
      response = RedisResponse.error(cause.getMessage());
    } else {
      if (logger.isErrorEnabled()) {
        logger.error("GeodeRedisServer-Unexpected error handler for " + ctx.channel(), cause);
      }
      response = RedisResponse.error(RedisConstants.SERVER_ERROR_MESSAGE);
    }

    return response;
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) {
    if (logger.isDebugEnabled()) {
      logger.debug("GeodeRedisServer-Connection closing with " + ctx.channel().remoteAddress());
    }
    ctx.channel().close();
    ctx.close();
  }

  private void executeCommand(ChannelHandlerContext ctx, Command command) throws Exception {
    RedisResponse response;

    if (!isAuthenticated) {
      response = handleUnAuthenticatedCommand(command);
      writeToChannel(response);
      return;
    }

    if (command.isUnsupported() && !allowUnsupportedCommands()) {
      writeToChannel(RedisResponse.error(RedisConstants.ERROR_UNSUPPORTED_COMMAND));
      return;
    }

    if (command.isUnimplemented()) {
      logger.info("Failed " + command.getCommandType() + " because it is not implemented.");
      writeToChannel(RedisResponse.error(command.getCommandType() + " is not implemented."));
      return;
    }

    if (command.isOfType(RedisCommandType.SHUTDOWN)) {
      this.server.shutdown();
      return;
    }

    response = command.execute(this);

    logResponse(response);
    moveSubscribeToNewEventLoopGroup(ctx, command);

    // TODO: Clean this up once all Executors are using RedisResponse
    if (response == null) {
      writeToChannel(command.getResponse());
    } else if (response != null) {
      writeToChannel(response);
    }

    if (command.isOfType(RedisCommandType.QUIT)) {
      channelInactive(ctx);
    }
  }

  private boolean allowUnsupportedCommands() {
    return this.server.allowUnsupportedCommands();
  }


  private RedisResponse handleUnAuthenticatedCommand(Command command) {
    RedisResponse response;
    if (command.isOfType(RedisCommandType.AUTH)) {
      response = command.execute(this);
    } else {
      response = RedisResponse.error(RedisConstants.ERROR_NOT_AUTH);
    }
    return response;
  }


  /**
   * SUBSCRIBE commands run in their own {@link EventLoopGroup}
   */
  private void moveSubscribeToNewEventLoopGroup(ChannelHandlerContext ctx, Command command)
      throws InterruptedException {
    if (command.isOfType(SUBSCRIBE)) {
      CountDownLatch latch = new CountDownLatch(0);
      ctx.channel().deregister().addListener((ChannelFutureListener) future -> {
        subscriberEventLoopGroup.register(ctx.channel()).sync();
        latch.countDown();
      });
      latch.await();
    }
  }

  private void logResponse(RedisResponse response) {
    if (logger.isDebugEnabled() && response != null) {
      ByteBuf buf = response.encode(new UnpooledByteBufAllocator(false));
      logger.debug("Redis command returned: {}",
          Command.getHexEncodedString(buf.array(), buf.readableBytes()));
    }
  }

  /**
   * {@link ByteBuf} allocator for this context. All executors must use this pooled allocator as
   * opposed to having unpooled buffers for maximum performance
   *
   * @return allocator instance
   */
  public ByteBufAllocator getByteBufAllocator() {
    return this.byteBufAllocator;
  }

  /**
   * Gets the provider of Regions
   */
  public RegionProvider getRegionProvider() {
    return this.regionProvider;
  }

  /**
   * Get the channel for this context
   *
   *
   * public Channel getChannel() { return this.channel; }
   */

  /**
   * Get the authentication password, this will be same server wide. It is exposed here as opposed
   * to {@link GeodeRedisServer}.
   */
  public byte[] getAuthPassword() {
    return this.authPassword;
  }

  /**
   * Checker if user has authenticated themselves
   *
   * @return True if no authentication required or authentication complete, false otherwise
   */
  public boolean isAuthenticated() {
    return this.isAuthenticated;
  }

  /**
   * Lets this context know the authentication is complete
   */
  public void setAuthenticationVerified() {
    this.isAuthenticated = true;
  }

  public Client getClient() {
    return new Client(channel);
  }

}
