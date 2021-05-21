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


import java.io.IOException;
import java.math.BigInteger;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

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

import org.apache.geode.ForcedDisconnectException;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.LowMemoryException;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionInvocationTargetException;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.redis.internal.GeodeRedisServer;
import org.apache.geode.redis.internal.ParameterRequirements.RedisParametersMismatchException;
import org.apache.geode.redis.internal.RedisCommandType;
import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.redis.internal.RegionProvider;
import org.apache.geode.redis.internal.data.CommandHelper;
import org.apache.geode.redis.internal.data.RedisDataTypeMismatchException;
import org.apache.geode.redis.internal.executor.CommandFunction;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.executor.UnknownExecutor;
import org.apache.geode.redis.internal.executor.hash.RedisHashCommands;
import org.apache.geode.redis.internal.executor.sortedset.RedisSortedSetCommands;
import org.apache.geode.redis.internal.pubsub.PubSub;
import org.apache.geode.redis.internal.statistics.RedisStats;

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
  private static final Command TERMINATE_COMMAND = new Command();

  private final Client client;
  private final Channel channel;
  private final RegionProvider regionProvider;
  private final PubSub pubsub;
  private final ByteBufAllocator byteBufAllocator;
  private final byte[] authPassword;
  private final Supplier<Boolean> allowUnsupportedSupplier;
  private final Runnable shutdownInvoker;
  private final RedisStats redisStats;
  private final EventLoopGroup subscriberGroup;
  private final DistributedMember member;
  private final CommandHelper commandHelper;
  private BigInteger scanCursor;
  private BigInteger sscanCursor;
  private final AtomicBoolean channelInactive = new AtomicBoolean();
  private final int MAX_QUEUED_COMMANDS =
      Integer.getInteger("geode.redis.commandQueueSize", 1000);
  private final LinkedBlockingQueue<Command> commandQueue =
      new LinkedBlockingQueue<>(MAX_QUEUED_COMMANDS);

  private final int serverPort;
  private CountDownLatch eventLoopSwitched;

  private boolean isAuthenticated;

  /**
   * Default constructor for execution contexts.
   *
   * @param channel Channel used by this context, should be one to one
   * @param password Authentication password for each context, can be null
   */
  public ExecutionHandlerContext(Channel channel,
      RegionProvider regionProvider,
      PubSub pubsub,
      Supplier<Boolean> allowUnsupportedSupplier,
      Runnable shutdownInvoker,
      RedisStats redisStats,
      ExecutorService backgroundExecutor,
      EventLoopGroup subscriberGroup,
      byte[] password,
      int serverPort,
      DistributedMember member,
      CommandHelper commandHelper) {
    this.channel = channel;
    this.regionProvider = regionProvider;
    this.pubsub = pubsub;
    this.allowUnsupportedSupplier = allowUnsupportedSupplier;
    this.shutdownInvoker = shutdownInvoker;
    this.redisStats = redisStats;
    this.subscriberGroup = subscriberGroup;
    this.client = new Client(channel);
    this.byteBufAllocator = this.channel.alloc();
    this.authPassword = password;
    this.isAuthenticated = password == null;
    this.serverPort = serverPort;
    this.member = member;
    this.commandHelper = commandHelper;
    this.scanCursor = new BigInteger("0");
    this.sscanCursor = new BigInteger("0");
    redisStats.addClient();

    backgroundExecutor.submit(this::processCommandQueue);
  }

  public ChannelFuture writeToChannel(RedisResponse response) {
    return channel.writeAndFlush(response.encode(byteBufAllocator), channel.newPromise())
        .addListener((ChannelFutureListener) f -> {
          response.afterWrite();
          logResponse(response, channel.remoteAddress(), f.cause());
        });
  }

  private void processCommandQueue() {
    while (true) {
      Command command = takeCommandFromQueue();
      if (command == TERMINATE_COMMAND) {
        return;
      }
      try {
        executeCommand(command);
        redisStats.incCommandsProcessed();
      } catch (Throwable ex) {
        exceptionCaught(command.getChannelHandlerContext(), ex);
      }
    }
  }

  private Command takeCommandFromQueue() {
    try {
      return commandQueue.take();
    } catch (InterruptedException e) {
      logger.info("Command queue thread interrupted");
      return TERMINATE_COMMAND;
    }
  }

  /**
   * This will handle the execution of received commands
   */
  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    Command command = (Command) msg;
    command.setChannelHandlerContext(ctx);
    if (!channelInactive.get()) {
      commandQueue.put(command);
    }
  }

  /**
   * Exception handler for the entire pipeline
   */
  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    RedisResponse exceptionResponse = getExceptionResponse(ctx, cause);
    if (exceptionResponse != null) {
      writeToChannel(exceptionResponse);
    }
  }

  public EventLoopGroup getSubscriberGroup() {
    return subscriberGroup;
  }

  public synchronized void changeChannelEventLoopGroup(EventLoopGroup newGroup,
      Consumer<Boolean> callback) {
    if (newGroup.equals(channel.eventLoop())) {
      // already registered with newGroup
      callback.accept(true);
      return;
    }
    channel.deregister().addListener((ChannelFutureListener) future -> {
      boolean registerSuccess = true;
      synchronized (channel) {
        if (!channel.isRegistered()) {
          try {
            newGroup.register(channel).sync();
          } catch (Exception e) {
            logger.warn("Unable to register new EventLoopGroup: {}", e.getMessage());
            registerSuccess = false;
          }
        }
      }
      callback.accept(registerSuccess);
    });
  }

  private RedisResponse getExceptionResponse(ChannelHandlerContext ctx, Throwable cause) {
    RedisResponse response;
    if (cause instanceof IOException) {
      channelInactive(ctx);
      return null;
    }

    if (cause instanceof FunctionException
        && !(cause instanceof FunctionInvocationTargetException)) {
      Throwable th = CommandFunction.getInitialCause((FunctionException) cause);
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
    } else if (cause instanceof LowMemoryException) {
      response = RedisResponse.oom(RedisConstants.ERROR_OOM_COMMAND_NOT_ALLOWED);
    } else if (cause instanceof DecoderException
        && cause.getCause() instanceof RedisCommandParserException) {
      response = RedisResponse.error(RedisConstants.PARSING_EXCEPTION_MESSAGE);

    } else if (cause instanceof InterruptedException || cause instanceof CacheClosedException) {
      response = RedisResponse.error(RedisConstants.SERVER_ERROR_SHUTDOWN);
    } else if (cause instanceof IllegalStateException
        || cause instanceof RedisParametersMismatchException) {
      response = RedisResponse.error(cause.getMessage());
    } else if (cause instanceof FunctionInvocationTargetException
        || cause instanceof DistributedSystemDisconnectedException
        || cause instanceof ForcedDisconnectException) {
      // This indicates a member departed or got disconnected
      logger.warn(
          "Closing client connection because one of the servers doing this operation departed.");
      channelInactive(ctx);
      response = null;
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
    if (channelInactive.compareAndSet(false, true)) {
      if (logger.isDebugEnabled()) {
        logger.debug("GeodeRedisServer-Connection closing with " + ctx.channel().remoteAddress());
      }

      commandQueue.clear();
      commandQueue.offer(TERMINATE_COMMAND);
      redisStats.removeClient();
      ctx.channel().close();
      ctx.close();
    }
  }

  private void executeCommand(Command command) throws Exception {
    try {
      if (logger.isDebugEnabled()) {
        logger.debug("Executing Redis command: {} - {}", command,
            channel.remoteAddress().toString());
      }

      // Note: Some Redis 6 clients look for an 'unknown command' error when
      // connecting to Redis <= 5 servers. So we need to check for unknown BEFORE auth.
      if (command.isUnknown()) {
        writeToChannel(command.execute(this));
        return;
      }

      if (!isAuthenticated()) {
        writeToChannel(handleUnAuthenticatedCommand(command));
        return;
      }

      if (command.isUnsupported() && !allowUnsupportedCommands()) {
        writeToChannel(new UnknownExecutor().executeCommand(command, this));
        return;
      }

      if (!getPubSub().findSubscriptionNames(getClient()).isEmpty()) {
        if (!command.getCommandType().isAllowedWhileSubscribed()) {
          writeToChannel(RedisResponse
              .error("only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT allowed in this context"));
        }
      }

      final long start = redisStats.startCommand();
      try {
        writeToChannel(command.execute(this));
      } finally {
        redisStats.endCommand(command.getCommandType(), start);
      }

      if (command.isOfType(RedisCommandType.QUIT)) {
        channelInactive(command.getChannelHandlerContext());
      }
    } catch (Exception e) {
      logger.warn("Execution of Redis command {} failed: {}", command, e);
      throw e;
    }
  }

  public boolean allowUnsupportedCommands() {
    return allowUnsupportedSupplier.get();
  }

  private RedisResponse handleUnAuthenticatedCommand(Command command) throws Exception {
    RedisResponse response;
    if (command.isOfType(RedisCommandType.AUTH)) {
      response = command.execute(this);
    } else {
      response = RedisResponse.customError(RedisConstants.ERROR_NOT_AUTH);
    }
    return response;
  }

  private void logResponse(RedisResponse response, Object extraMessage, Throwable cause) {
    if (logger.isDebugEnabled() && response != null) {
      ByteBuf buf = response.encode(new UnpooledByteBufAllocator(false));
      if (cause == null) {
        logger.debug("Redis command returned: {} - {}",
            Command.getHexEncodedString(buf.array(), buf.readableBytes()), extraMessage);
      } else {
        logger.debug("Redis command FAILED to return: {} - {}",
            Command.getHexEncodedString(buf.array(), buf.readableBytes()), extraMessage, cause);
      }
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
    return regionProvider;
  }

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

  public int getServerPort() {
    return serverPort;
  }

  public Client getClient() {
    return client;
  }

  public void shutdown() {
    shutdownInvoker.run();
  }

  public PubSub getPubSub() {
    return pubsub;
  }

  public RedisStats getRedisStats() {
    return redisStats;
  }

  public BigInteger getScanCursor() {
    return scanCursor;
  }

  public void setScanCursor(BigInteger scanCursor) {
    this.scanCursor = scanCursor;
  }

  public BigInteger getSscanCursor() {
    return sscanCursor;
  }

  public void setSscanCursor(BigInteger sscanCursor) {
    this.sscanCursor = sscanCursor;
  }

  public String getMemberName() {
    return member.getUniqueId();
  }


  public CommandHelper getCommandHelper() {
    return commandHelper;
  }

  /**
   * This method and {@link #eventLoopReady()} are relevant for pubsub related commands which need
   * to return responses on a different EventLoopGroup. We need to ensure that the EventLoopGroup
   * switch has occurred before subsequent commands are executed.
   */
  public CountDownLatch getOrCreateEventLoopLatch() {
    if (eventLoopSwitched != null) {
      return eventLoopSwitched;
    }

    eventLoopSwitched = new CountDownLatch(1);
    return eventLoopSwitched;
  }

  /**
   * Signals that we have successfully switched over to a new EventLoopGroup.
   */
  public void eventLoopReady() {
    if (eventLoopSwitched == null) {
      return;
    }

    try {
      eventLoopSwitched.await();
    } catch (InterruptedException e) {
      logger.info("Event loop interrupted", e);
    }
  }

  public RedisHashCommands getRedisHashCommands() {
    return regionProvider.getHashCommands();
  }

  public RedisSortedSetCommands getRedisSortedSetCommands() {
    return regionProvider.getSortedSetCommands();
  }

}
