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

import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_AUTHORIZED;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.logging.log4j.Logger;
import org.apache.shiro.subject.Subject;

import org.apache.geode.CancelException;
import org.apache.geode.cache.LowMemoryException;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.redis.internal.RedisCommandType;
import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.redis.internal.RedisException;
import org.apache.geode.redis.internal.RegionProvider;
import org.apache.geode.redis.internal.data.RedisData;
import org.apache.geode.redis.internal.data.RedisDataMovedException;
import org.apache.geode.redis.internal.data.RedisDataType;
import org.apache.geode.redis.internal.data.RedisDataTypeMismatchException;
import org.apache.geode.redis.internal.data.RedisHash;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.data.RedisSet;
import org.apache.geode.redis.internal.data.RedisSortedSet;
import org.apache.geode.redis.internal.data.RedisString;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.executor.UnknownExecutor;
import org.apache.geode.redis.internal.parameters.RedisParametersMismatchException;
import org.apache.geode.redis.internal.pubsub.PubSub;
import org.apache.geode.redis.internal.statistics.RedisStats;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.security.ResourcePermission;
import org.apache.geode.security.SecurityManager;

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

  private static final ResourcePermission RESOURCE_PERMISSION;

  private final Client client;
  private final RegionProvider regionProvider;
  private final PubSub pubsub;
  private final String redisUsername;
  private final Supplier<Boolean> allowUnsupportedSupplier;
  private final Runnable shutdownInvoker;
  private final RedisStats redisStats;
  private final DistributedMember member;
  private final SecurityService securityService;
  private BigInteger scanCursor;
  private BigInteger sscanCursor;
  private final AtomicBoolean channelInactive = new AtomicBoolean();

  private final int serverPort;

  private Subject subject;

  static {
    String resourcePermission = System.getProperty("redis.resource-permission",
        "DATA:WRITE:" + RegionProvider.REDIS_DATA_REGION);
    String[] parts = resourcePermission.split(":");
    if (parts.length != 3) {
      parts = new String[] {"DATA", "WRITE", RegionProvider.REDIS_DATA_REGION};
    }

    RESOURCE_PERMISSION = new ResourcePermission(parts[0], parts[1], parts[2]);
  }

  /**
   * Default constructor for execution contexts.
   */
  public ExecutionHandlerContext(Channel channel,
      RegionProvider regionProvider,
      PubSub pubsub,
      Supplier<Boolean> allowUnsupportedSupplier,
      Runnable shutdownInvoker,
      RedisStats redisStats,
      String username,
      int serverPort,
      DistributedMember member,
      SecurityService securityService) {
    this.regionProvider = regionProvider;
    this.pubsub = pubsub;
    this.allowUnsupportedSupplier = allowUnsupportedSupplier;
    this.shutdownInvoker = shutdownInvoker;
    this.redisStats = redisStats;
    this.redisUsername = username;
    this.client = new Client(channel, pubsub);
    this.serverPort = serverPort;
    this.member = member;
    this.securityService = securityService;
    this.scanCursor = new BigInteger("0");
    this.sscanCursor = new BigInteger("0");
    redisStats.addClient();

    channel.closeFuture().addListener(future -> logout());
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
    } else if (rootCause instanceof RedisCommandParserException) {
      return RedisResponse
          .error(RedisConstants.PARSING_EXCEPTION_MESSAGE + ": " + rootCause.getMessage());
    } else if (rootCause instanceof InterruptedException || rootCause instanceof CancelException) {
      logger
          .warn("Closing Redis client connection because the server doing this operation departed: "
              + rootCause.getMessage());
      channelInactive(ctx);
      return null;
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

      if (!isAuthorized()) {
        writeToChannel(RedisResponse.error(ERROR_NOT_AUTHORIZED));
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
      response = RedisResponse.noAuth(RedisConstants.ERROR_NOT_AUTHENTICATED);
    }
    return response;
  }

  /**
   * If previously authenticated, logs out the current user
   */
  private void logout() {
    if (subject != null) {
      subject.logout();
      subject = null;
    }
  }

  /**
   * Gets the provider of Regions
   */
  public RegionProvider getRegionProvider() {
    return regionProvider;
  }

  /**
   * Get the default username. This is the username that will be passed to the
   * {@link SecurityManager} in response to
   * an {@code AUTH password} command.
   */
  public String getRedisUsername() {
    return redisUsername;
  }

  /**
   * Check if the client has authenticated.
   *
   * @return True if no authentication required or authentication is complete, false otherwise
   */
  public boolean isAuthenticated() {
    return (!securityService.isIntegratedSecurity()) || subject != null;
  }

  public boolean isAuthorized() {
    if (subject == null) {
      return true;
    }

    try {
      securityService.authorize(RESOURCE_PERMISSION, subject);
    } catch (NotAuthorizedException nex) {
      return false;
    }
    return true;
  }

  /**
   * Sets an authenticated principal in the context. This implies that the connection has been
   * successfully authenticated.
   */
  public void setSubject(Subject subject) {
    this.subject = subject;
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

  public SecurityService getSecurityService() {
    return securityService;
  }

  public void checkForLowMemory(RedisCommandType commandType) {
    Set<DistributedMember> criticalMembers = getRegionProvider().getCriticalMembers();
    if (!criticalMembers.isEmpty()) {
      throw new LowMemoryException(
          String.format(
              "%s cannot be executed because the members %s are running low on memory",
              commandType.toString(), criticalMembers),
          criticalMembers);
    }
  }


  public Region<RedisKey, RedisData> getRegion() {
    return getRegionProvider().getLocalDataRegion();
  }

  public <T> T lockedExecute(RedisKey key, Callable<T> callable) {
    return getRegionProvider().lockedExecute(key, callable);
  }

  public <T> T lockedExecute(RedisKey key, List<RedisKey> keysToLock, Callable<T> callable) {
    return getRegionProvider().lockedExecute(key, keysToLock, callable);
  }

  public <T> T lockedExecuteInTransaction(RedisKey key, List<RedisKey> keysToLock,
      Callable<T> callable) {
    return getRegionProvider().lockedExecuteInTransaction(key, keysToLock, callable);
  }

  public interface FailableFunction<T, R> {
    R apply(T t) throws Exception;
  }

  public <R> R stringLockedExecute(RedisKey key, boolean updateStats,
      FailableFunction<RedisString, R> function) {
    return getRegionProvider().lockedExecute(key,
        () -> function.apply(getRedisString(key, updateStats)));
  }

  public <R> R stringLockedExecute(RedisKey key, boolean updateStats, boolean ignoreType,
      FailableFunction<RedisString, R> function) {
    if (ignoreType) {
      return getRegionProvider().lockedExecute(key,
          () -> function.apply(getRedisStringIgnoringType(key, updateStats)));
    } else {
      return stringLockedExecute(key, updateStats, function);
    }
  }

  private RedisString getRedisString(RedisKey key, boolean updateStats) {
    return getRegionProvider()
        .getTypedRedisData(RedisDataType.REDIS_STRING, key, updateStats);
  }

  private RedisString getRedisStringIgnoringType(RedisKey key, boolean updateStats) {
    return getRegionProvider().getRedisStringIgnoringType(key, updateStats);
  }


  public <R> R hashLockedExecute(RedisKey key, boolean updateStats,
      FailableFunction<RedisHash, R> function) {
    return getRegionProvider().lockedExecute(key,
        () -> function.apply(getRedisHash(key, updateStats)));
  }

  private RedisHash getRedisHash(RedisKey key, boolean updateStats) {
    return getRegionProvider().getTypedRedisData(RedisDataType.REDIS_HASH, key, updateStats);
  }


  public <R> R zsetLockedExecute(RedisKey key, boolean updateStats,
      FailableFunction<RedisSortedSet, R> function) {
    return getRegionProvider().lockedExecute(key,
        () -> function.apply(getRedisSortedSet(key, updateStats)));
  }

  private RedisSortedSet getRedisSortedSet(RedisKey key, boolean updateStats) {
    return getRegionProvider().getTypedRedisData(RedisDataType.REDIS_SORTED_SET, key, updateStats);
  }


  public <R> R setLockedExecute(RedisKey key, boolean updateStats,
      FailableFunction<RedisSet, R> function) {
    return getRegionProvider().lockedExecute(key,
        () -> function.apply(getRedisSet(key, updateStats)));
  }

  public RedisSet getRedisSet(RedisKey key, boolean updateStats) {
    return getRegionProvider().getTypedRedisData(RedisDataType.REDIS_SET, key, updateStats);
  }


  public <R> R dataLockedExecute(RedisKey key,
      FailableFunction<RedisData, R> function) {
    return getRegionProvider().lockedExecute(key,
        () -> function.apply(getRedisData(key)));
  }

  public RedisData getRedisData(RedisKey key) {
    return getRegionProvider().getRedisData(key);
  }

}
