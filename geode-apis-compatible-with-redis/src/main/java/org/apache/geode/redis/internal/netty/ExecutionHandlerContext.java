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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.DecoderException;
import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.LowMemoryException;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.redis.internal.GeodeRedisServer;
import org.apache.geode.redis.internal.RedisCommandType;
import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.redis.internal.RedisException;
import org.apache.geode.redis.internal.RegionProvider;
import org.apache.geode.redis.internal.data.RedisDataMovedException;
import org.apache.geode.redis.internal.data.RedisDataTypeMismatchException;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.executor.UnknownExecutor;
import org.apache.geode.redis.internal.executor.hash.RedisHashCommands;
import org.apache.geode.redis.internal.executor.key.RedisKeyCommands;
import org.apache.geode.redis.internal.executor.set.RedisSetCommands;
import org.apache.geode.redis.internal.executor.sortedset.RedisSortedSetCommands;
import org.apache.geode.redis.internal.executor.string.RedisStringCommands;
import org.apache.geode.redis.internal.parameters.RedisParametersMismatchException;
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

  private final Client client;
  private final RegionProvider regionProvider;
  private final PubSub pubsub;
  private final byte[] authPassword;
  private final Supplier<Boolean> allowUnsupportedSupplier;
  private final Runnable shutdownInvoker;
  private final RedisStats redisStats;
  private final DistributedMember member;
  private BigInteger scanCursor;
  private BigInteger sscanCursor;
  private final AtomicBoolean channelInactive = new AtomicBoolean();

  private final int serverPort;

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
      byte[] password,
      int serverPort,
      DistributedMember member) {
    this.regionProvider = regionProvider;
    this.pubsub = pubsub;
    this.allowUnsupportedSupplier = allowUnsupportedSupplier;
    this.shutdownInvoker = shutdownInvoker;
    this.redisStats = redisStats;
    this.client = new Client(channel, pubsub);
    this.authPassword = password;
    this.isAuthenticated = password == null;
    this.serverPort = serverPort;
    this.member = member;
    this.scanCursor = new BigInteger("0");
    this.sscanCursor = new BigInteger("0");
    redisStats.addClient();
  }

  public ChannelFuture writeToChannel(RedisResponse response) {
    return client.writeToChannel(response);
  }

  /**
   * This will handle the execution of received commands
   */
  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
    Command command = (Command) msg;
    command.setChannelHandlerContext(ctx);
    if (!channelInactive.get()) {
      try {
        executeCommand(command);
        redisStats.incCommandsProcessed();
      } catch (Throwable ex) {
        exceptionCaught(command.getChannelHandlerContext(), ex);
      }
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

  private RedisResponse getExceptionResponse(ChannelHandlerContext ctx, Throwable cause) {
    if (cause instanceof IOException) {
      channelInactive(ctx);
      return null;
    }

    Throwable rootCause = getRootCause(cause);
    if (rootCause instanceof RedisDataMovedException) {
      return RedisResponse.moved(rootCause.getMessage());
    } else if (rootCause instanceof RedisDataTypeMismatchException) {
      return RedisResponse.wrongType(rootCause.getMessage());
    } else if (rootCause instanceof IllegalStateException
        || rootCause instanceof RedisParametersMismatchException
        || rootCause instanceof RedisException
        || rootCause instanceof NumberFormatException
        || rootCause instanceof ArithmeticException) {
      return RedisResponse.error(rootCause.getMessage());
    } else if (rootCause instanceof LowMemoryException) {
      return RedisResponse.oom(RedisConstants.ERROR_OOM_COMMAND_NOT_ALLOWED);
    } else if (rootCause instanceof DecoderException
        && rootCause.getCause() instanceof RedisCommandParserException) {
      return RedisResponse
          .error(RedisConstants.PARSING_EXCEPTION_MESSAGE + ": " + rootCause.getMessage());
    } else if (rootCause instanceof InterruptedException
        || rootCause instanceof CacheClosedException) {
      return RedisResponse.error(RedisConstants.SERVER_ERROR_SHUTDOWN);
    } else {
      if (logger.isErrorEnabled()) {
        logger.error("GeodeRedisServer-Unexpected error handler for {}", ctx.channel(), rootCause);
      }
      return RedisResponse.error(RedisConstants.SERVER_ERROR_MESSAGE);
    }
  }

  private Throwable getRootCause(Throwable cause) {
    Throwable root = cause;
    while (root.getCause() != null) {
      root = root.getCause();
    }
    return root;
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) {
    if (channelInactive.compareAndSet(false, true)) {
      if (logger.isDebugEnabled()) {
        logger.debug("GeodeRedisServer-Connection closing with " + ctx.channel().remoteAddress());
      }

      redisStats.removeClient();
      ctx.channel().close();
      ctx.close();
    }
  }

  private void executeCommand(Command command) throws Exception {
    try {
      if (logger.isDebugEnabled()) {
        logger.debug("Executing Redis command: {} - {}", command,
            getClient().getRemoteAddress());
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

      if (getClient().hasSubscriptions()) {
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
      if (!(e instanceof RedisDataMovedException)) {
        logger.warn("Execution of Redis command {} failed: {}", command, e);
      }
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

  public RedisHashCommands getHashCommands() {
    return regionProvider.getHashCommands();
  }

  public RedisSortedSetCommands getSortedSetCommands() {
    return regionProvider.getSortedSetCommands();
  }

  public RedisKeyCommands getKeyCommands() {
    return regionProvider.getKeyCommands();
  }

  public RedisStringCommands getStringCommands() {
    return regionProvider.getStringCommands();
  }

  public RedisSetCommands getSetCommands() {
    return regionProvider.getSetCommands();
  }

}
