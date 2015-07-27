package com.gemstone.gemfire.internal.redis;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.DecoderException;
import io.netty.util.concurrent.EventExecutor;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheTransactionManager;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.TransactionException;
import com.gemstone.gemfire.cache.TransactionId;
import com.gemstone.gemfire.cache.UnsupportedOperationInTransactionException;
import com.gemstone.gemfire.internal.redis.executor.transactions.TransactionExecutor;
import com.gemstone.gemfire.redis.GemFireRedisServer;

public class ExecutionHandlerContext extends ChannelInboundHandlerAdapter {

  private static final int MAXIMUM_NUM_RETRIES = 5;

  private final Cache cache;
  private final GemFireRedisServer server;
  private final LogWriter logger;
  private final Channel channel;
  private final AtomicBoolean needChannelFlush;
  private final Runnable flusher;
  private final EventExecutor lastExecutor;
  private final ByteBufAllocator byteBufAllocator;
  /**
   * TransactionId for any transactions started by this client
   */
  private TransactionId transactionID;

  /**
   * Queue of commands for a given transaction
   */
  private Queue<Command> transactionQueue;
  private final RegionCache regionCache;
  private final byte[] authPwd;

  private boolean isAuthenticated;

  public ExecutionHandlerContext(Channel ch, Cache cache, RegionCache regions, GemFireRedisServer server, byte[] pwd) {
    this.cache = cache;
    this.server = server;
    this.logger = cache.getLogger();
    this.channel = ch;
    this.needChannelFlush  = new AtomicBoolean(false);
    this.flusher = new Runnable() {

      @Override
      public void run() {
        flushChannel();
      }

    };
    this.lastExecutor = channel.pipeline().lastContext().executor();
    this.byteBufAllocator = channel.alloc();
    this.transactionID = null;
    this.transactionQueue = null; // Lazy
    this.regionCache = regions;
    this.authPwd = pwd;
    this.isAuthenticated = pwd != null ? false : true;
  }

  private void flushChannel() {
    while (needChannelFlush.getAndSet(false)) {
      channel.flush();
    }
  }

  private void writeToChannel(Object message) {
    channel.write(message, channel.voidPromise());
    if (!needChannelFlush.getAndSet(true)) {
      this.lastExecutor.execute(flusher);
    }
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    Command command = (Command) msg;
    executeCommand(ctx, command);
  }

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
    if (cause instanceof RedisDataTypeMismatchException)
      response = Coder.getWrongTypeResponse(this.byteBufAllocator, cause.getMessage());
    else if (cause instanceof DecoderException && cause.getCause() instanceof RedisCommandParserException)
      response = Coder.getErrorResponse(this.byteBufAllocator, RedisConstants.PARSING_EXCEPTION_MESSAGE);
    else if (cause instanceof RegionCreationException)
      response = Coder.getErrorResponse(this.byteBufAllocator, RedisConstants.ERROR_REGION_CREATION);
    else if (cause instanceof InterruptedException)
      response = Coder.getErrorResponse(this.byteBufAllocator, RedisConstants.SERVER_ERROR_SHUTDOWN);
    else if (cause instanceof IllegalStateException) {
      response = Coder.getErrorResponse(this.byteBufAllocator,  cause.getMessage());
    } else {
      if (this.logger.errorEnabled())
        this.logger.error("GemFireRedisServer-Unexpected error handler", cause);
      response = Coder.getErrorResponse(this.byteBufAllocator, RedisConstants.SERVER_ERROR_MESSAGE);
    }
    return response;
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) {
    if (logger.fineEnabled())
      logger.fine("GemFireRedisServer-Connection closing with " + ctx.channel().remoteAddress());
    ctx.channel().close();
    ctx.close();
  }

  /**
   * This method is used to execute the command. The executor is 
   * determined by the {@link RedisCommandType} and then the execution
   * is started.
   * 
   * @param command Command to be executed
   * @param cache The Cache instance of this server
   * @param client The client data associated with the client
   * @throws Exception 
   */
  public void executeCommand(ChannelHandlerContext ctx, Command command) throws Exception {
    RedisCommandType type = command.getCommandType();
    Executor exec = type.getExecutor();
    if (isAuthenticated) {
      if (type == RedisCommandType.SHUTDOWN) {
        this.server.shutdown();
        return;
      }
      if (hasTransaction() && !(exec instanceof TransactionExecutor))
        executeWithTransaction(ctx, exec, command);
      else
        executeWithoutTransaction(exec, command, MAXIMUM_NUM_RETRIES); 

      if (hasTransaction() && command.getCommandType() != RedisCommandType.MULTI) {
        writeToChannel(Coder.getSimpleStringResponse(this.byteBufAllocator, RedisConstants.COMMAND_QUEUED));
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
   * Private helper method to execute a command without a transaction, done for
   * special exception handling neatness
   * 
   * @param exec Executor to use
   * @param command Command to execute
   * @param cache Cache instance
   * @param client Client data associated with client
   * @param n Recursive max depth of retries
   * @throws Exception Throws exception if exception is from within execution and not to be handled
   */
  private void executeWithoutTransaction(final Executor exec, Command command, int n) throws Exception {
    try {
      exec.executeCommand(command, this);
    } catch (RegionDestroyedException e) {
      if (n > 0)
        executeWithoutTransaction(exec, command, n - 1);
      else
        throw e;
    }
  }

  /**
   * Private method to execute a command when a transaction has been started
   * 
   * @param exec Executor to use
   * @param command Command to execute
   * @param cache Cache instance
   * @param client Client data associated with client
   * @throws Exception Throws exception if exception is from within execution and unrelated to transactions
   */
  private void executeWithTransaction(ChannelHandlerContext ctx, final Executor exec, Command command) throws Exception {
    CacheTransactionManager txm = cache.getCacheTransactionManager();
    TransactionId transactionId = getTransactionID();
    txm.resume(transactionId);
    try {
      exec.executeCommand(command, this);
    } catch(UnsupportedOperationInTransactionException e) {
      command.setResponse(Coder.getErrorResponse(this.byteBufAllocator, RedisConstants.ERROR_UNSUPPORTED_OPERATION_IN_TRANSACTION));
    } catch (TransactionException e) {
      command.setResponse(Coder.getErrorResponse(this.byteBufAllocator, RedisConstants.ERROR_TRANSACTION_EXCEPTION));
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
        if (r != null)
          r.release();
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
    if (this.transactionQueue == null)
      this.transactionQueue = new ConcurrentLinkedQueue<Command>();
    return this.transactionQueue;
  }

  public ByteBufAllocator getByteBufAllocator() {
    return this.byteBufAllocator;
  }

  public RegionCache getRegionCache() {
    return this.regionCache;
  }

  public CacheTransactionManager getCacheTransactionManager() {
    return this.cache.getCacheTransactionManager();
  }

  public LogWriter getLogger() {
    return this.cache.getLogger();
  }

  public Channel getChannel() {
    return this.channel;
  }

  public byte[] getAuthPwd() {
    return this.authPwd;
  }

  public boolean isAuthenticated() {
    return this.isAuthenticated;
  }

  public void setAuthenticationVerified() {
    this.isAuthenticated = true;
  }
}
