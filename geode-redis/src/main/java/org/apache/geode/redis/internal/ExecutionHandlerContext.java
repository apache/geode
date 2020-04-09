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
package org.apache.geode.redis.internal;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.DecoderException;
import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.TransactionException;
import org.apache.geode.cache.TransactionId;
import org.apache.geode.cache.UnsupportedOperationInTransactionException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.redis.GeodeRedisServer;
import org.apache.geode.redis.internal.executor.transactions.TransactionExecutor;

/**
 * This class extends {@link ChannelInboundHandlerAdapter} from Netty and it is the last part of the
 * channel pipeline. The {@link ByteToCommandDecoder} forwards a {@link Command} to this class which
 * executes it and sends the result back to the client. Additionally, all exception handling is done
 * by this class.
 * <p>
 * Besides being part of Netty's pipeline, this class also serves as a context to the execution of a
 * command. It abstracts transactions, provides access to the {@link RegionProvider} and anything
 * else an executing {@link Command} may need.
 */
public class ExecutionHandlerContext extends ChannelInboundHandlerAdapter {

  private static final Logger logger = LogService.getLogger();
  private static final int WAIT_REGION_DSTRYD_MILLIS = 100;
  private static final int MAXIMUM_NUM_RETRIES = (1000 * 60) / WAIT_REGION_DSTRYD_MILLIS; // 60
                                                                                          // seconds
  private final RedisLockService lockService;

  private final Cache cache;
  private final GeodeRedisServer server;
  private final Channel channel;
  private final AtomicBoolean needChannelFlush;
  private final ByteBufAllocator byteBufAllocator;
  /**
   * TransactionId for any transactions started by this client
   */
  private TransactionId transactionID;

  /**
   * Queue of commands for a given transaction
   */
  private Queue<Command> transactionQueue;
  private final RegionProvider regionProvider;
  private final byte[] authPassword;

  private boolean isAuthenticated;

  public KeyRegistrar getKeyRegistrar() {
    return keyRegistrar;
  }

  private final KeyRegistrar keyRegistrar;
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
   *        can initiate a shutdwon
   * @param password Authentication password for each context, can be null
   */
  public ExecutionHandlerContext(Channel channel, Cache cache, RegionProvider regionProvider,
      GeodeRedisServer server, byte[] password, KeyRegistrar keyRegistrar, PubSub pubSub,
      RedisLockService lockService) {
    this.keyRegistrar = keyRegistrar;
    this.lockService = lockService;
    this.pubSub = pubSub;
    if (channel == null || cache == null || regionProvider == null || server == null) {
      throw new IllegalArgumentException("Only the authentication password may be null");
    }
    this.cache = cache;
    this.server = server;
    this.channel = channel;
    this.needChannelFlush = new AtomicBoolean(false);
    this.byteBufAllocator = this.channel.alloc();
    this.transactionID = null;
    this.transactionQueue = null; // Lazy
    this.regionProvider = regionProvider;
    this.authPassword = password;
    this.isAuthenticated = password != null ? false : true;

  }

  public RedisLockService getLockService() {
    return this.lockService;
  }

  private void flushChannel() {
    while (needChannelFlush.getAndSet(false)) {
      channel.flush();
    }
  }

  public ChannelFuture writeToChannel(ByteBuf message) {
    return channel.writeAndFlush(message, channel.newPromise());
  }

  /**
   * This will handle the execution of received commands
   */
  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    Command command = (Command) msg;
    try {
      logger.info("Executing Redis command: {}", command);
      executeCommand(ctx, command);
    } catch (Exception e) {
      logger.error(
          "Execution of  Redis command " + command + " failed",
          e);
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
    ByteBuf response = getExceptionResponse(ctx, cause);
    writeToChannel(response);
  }

  private ByteBuf getExceptionResponse(ChannelHandlerContext ctx, Throwable cause) {
    ByteBuf response;
    if (cause instanceof RedisDataTypeMismatchException) {
      response = Coder.getWrongTypeResponse(this.byteBufAllocator, cause.getMessage());
    } else if (cause instanceof DecoderException
        && cause.getCause() instanceof RedisCommandParserException) {
      response =
          Coder.getErrorResponse(this.byteBufAllocator, RedisConstants.PARSING_EXCEPTION_MESSAGE);
    } else if (cause instanceof RegionCreationException) {
      this.logger.error(cause);
      response =
          Coder.getErrorResponse(this.byteBufAllocator, RedisConstants.ERROR_REGION_CREATION);
    } else if (cause instanceof InterruptedException || cause instanceof CacheClosedException) {
      response =
          Coder.getErrorResponse(this.byteBufAllocator, RedisConstants.SERVER_ERROR_SHUTDOWN);
    } else if (cause instanceof IllegalStateException) {
      response = Coder.getErrorResponse(this.byteBufAllocator, cause.getMessage());
    } else {
      if (logger.isErrorEnabled()) {
        logger.error("GeodeRedisServer-Unexpected error handler for " + ctx.channel(), cause);
      }
      response = Coder.getErrorResponse(this.byteBufAllocator, RedisConstants.SERVER_ERROR_MESSAGE);
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
    RedisCommandType type = command.getCommandType();
    Executor exec = type.getExecutor();
    if (isAuthenticated) {
      if (type == RedisCommandType.SHUTDOWN) {
        this.server.shutdown();
        return;
      }
      if (hasTransaction() && !(exec instanceof TransactionExecutor)) {
        executeWithTransaction(ctx, exec, command);
      } else {
        executeWithoutTransaction(exec, command);
      }

      if (hasTransaction() && command.getCommandType() != RedisCommandType.MULTI) {
        writeToChannel(
            Coder.getSimpleStringResponse(this.byteBufAllocator, RedisConstants.COMMAND_QUEUED));
      } else {
        ByteBuf response = command.getResponse();
        writeToChannel(response);
      }
    } else if (type == RedisCommandType.QUIT) {
      exec.executeCommand(command, this);
      ByteBuf response = command.getResponse();
      writeToChannel(response);
      channelInactive(ctx);
    } else if (type == RedisCommandType.AUTH) {
      exec.executeCommand(command, this);
      ByteBuf response = command.getResponse();
      writeToChannel(response);
    } else {
      ByteBuf r = Coder.getNoAuthResponse(this.byteBufAllocator, RedisConstants.ERROR_NOT_AUTH);
      writeToChannel(r);
    }
  }

  /**
   * Private helper method to execute a command without a transaction, done for special exception
   * handling neatness
   *
   * @param exec Executor to use
   * @param command Command to execute
   * @throws Exception Throws exception if exception is from within execution and not to be handled
   */
  private void executeWithoutTransaction(final Executor exec, Command command) throws Exception {
    Exception cause = null;
    for (int i = 0; i < MAXIMUM_NUM_RETRIES; i++) {
      try {
        exec.executeCommand(command, this);
        return;
      } catch (Exception e) {
        logger.error(e);

        cause = e;
        if (e instanceof RegionDestroyedException || e instanceof RegionNotFoundException
            || e.getCause() instanceof QueryInvocationTargetException) {
          Thread.sleep(WAIT_REGION_DSTRYD_MILLIS);
        }
      }
    }
    throw cause;
  }

  private void executeWithTransaction(ChannelHandlerContext ctx, final Executor exec,
      Command command) throws Exception {
    CacheTransactionManager txm = cache.getCacheTransactionManager();
    TransactionId transactionId = getTransactionID();
    txm.resume(transactionId);
    try {
      exec.executeCommand(command, this);
    } catch (UnsupportedOperationInTransactionException e) {
      command.setResponse(Coder.getErrorResponse(this.byteBufAllocator,
          RedisConstants.ERROR_UNSUPPORTED_OPERATION_IN_TRANSACTION));
    } catch (TransactionException e) {
      command.setResponse(Coder.getErrorResponse(this.byteBufAllocator,
          RedisConstants.ERROR_TRANSACTION_EXCEPTION));
    } catch (Exception e) {
      ByteBuf response = getExceptionResponse(ctx, e);
      command.setResponse(response);
    }
    getTransactionQueue().add(command);
    transactionId = txm.suspend();
    setTransactionID(transactionId);
  }

  /**
   * Get the current transacationId
   *
   * @return The current transactionId, null if one doesn't exist
   */
  public TransactionId getTransactionID() {
    return this.transactionID;
  }

  /**
   * Check if client has transaction
   *
   * @return True if client has transaction, false otherwise
   */
  public boolean hasTransaction() {
    return transactionID != null;
  }

  /**
   * Setter method for transaction
   *
   * @param id TransactionId of current transaction for client
   */
  public void setTransactionID(TransactionId id) {
    this.transactionID = id;
  }

  /**
   * Reset the transaction of client
   */
  public void clearTransaction() {
    this.transactionID = null;
    if (this.transactionQueue != null) {
      for (Command c : this.transactionQueue) {
        ByteBuf r = c.getResponse();
        if (r != null) {
          r.release();
        }
      }
      this.transactionQueue.clear();
    }
  }

  /**
   * Getter for transaction command queue
   *
   * @return Command queue
   */
  public Queue<Command> getTransactionQueue() {
    if (this.transactionQueue == null) {
      this.transactionQueue = new ConcurrentLinkedQueue<Command>();
    }
    return this.transactionQueue;
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
   * Getter for manager to allow pausing and resuming transactions
   */
  public CacheTransactionManager getCacheTransactionManager() {
    return this.cache.getCacheTransactionManager();
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
