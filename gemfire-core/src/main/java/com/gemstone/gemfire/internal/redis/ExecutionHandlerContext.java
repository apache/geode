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
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheTransactionManager;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.TransactionException;
import com.gemstone.gemfire.cache.TransactionId;
import com.gemstone.gemfire.cache.UnsupportedOperationInTransactionException;
import com.gemstone.gemfire.cache.query.QueryInvocationTargetException;
import com.gemstone.gemfire.internal.redis.executor.transactions.TransactionExecutor;
import com.gemstone.gemfire.redis.GemFireRedisServer;

/**
 * This class extends {@link ChannelInboundHandlerAdapter} from Netty and it is
 * the last part of the channel pipeline. The {@link ByteToCommandDecoder} forwards a
 * {@link Command} to this class which executes it and sends the result back to the
 * client. Additionally, all exception handling is done by this class. 
 * <p>
 * Besides being part of Netty's pipeline, this class also serves as a context to the
 * execution of a command. It abstracts transactions, provides access to the {@link RegionProvider}
 * and anything else an executing {@link Command} may need.
 * 
 * @author Vitaliy Gavrilov
 *
 */
public class ExecutionHandlerContext extends ChannelInboundHandlerAdapter {

  private static final int WAIT_REGION_DSTRYD_MILLIS = 100;
  private static final int MAXIMUM_NUM_RETRIES = (1000*60)/WAIT_REGION_DSTRYD_MILLIS; // 60 seconds total

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
  private final RegionProvider regionProvider;
  private final byte[] authPwd;

  private boolean isAuthenticated;

  /**
   * Default constructor for execution contexts. 
   * 
   * @param ch Channel used by this context, should be one to one
   * @param cache The Geode cache instance of this vm
   * @param regionProvider The region provider of this context
   * @param server Instance of the server it is attached to, only used so that any execution can initiate a shutdwon
   * @param pwd Authentication password for each context, can be null
   */
  public ExecutionHandlerContext(Channel ch, Cache cache, RegionProvider regionProvider, GemFireRedisServer server, byte[] pwd) {
    if (ch == null || cache == null || regionProvider == null || server == null)
      throw new IllegalArgumentException("Only the authentication password may be null");
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
    this.regionProvider = regionProvider;
    this.authPwd = pwd;
    this.isAuthenticated = pwd != null ? false : true;
  }

  private void flushChannel() {
    while (needChannelFlush.getAndSet(false)) {
      channel.flush();
    }
  }

  private void writeToChannel(ByteBuf message) {
    channel.write(message, channel.voidPromise());
    if (!needChannelFlush.getAndSet(true)) {
      this.lastExecutor.execute(flusher);
    }
  }

  /**
   * This will handle the execution of received commands
   */
  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    Command command = (Command) msg;
    executeCommand(ctx, command);
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
    if (cause instanceof RedisDataTypeMismatchException)
      response = Coder.getWrongTypeResponse(this.byteBufAllocator, cause.getMessage());
    else if (cause instanceof DecoderException && cause.getCause() instanceof RedisCommandParserException)
      response = Coder.getErrorResponse(this.byteBufAllocator, RedisConstants.PARSING_EXCEPTION_MESSAGE);
    else if (cause instanceof RegionCreationException) {
      this.logger.error(cause);
      response = Coder.getErrorResponse(this.byteBufAllocator, RedisConstants.ERROR_REGION_CREATION);
    } else if (cause instanceof InterruptedException || cause instanceof CacheClosedException)
      response = Coder.getErrorResponse(this.byteBufAllocator, RedisConstants.SERVER_ERROR_SHUTDOWN);
    else if (cause instanceof IllegalStateException) {
      response = Coder.getErrorResponse(this.byteBufAllocator, cause.getMessage());
    } else {
      if (this.logger.errorEnabled())
        this.logger.error("GemFireRedisServer-Unexpected error handler for " + ctx.channel(), cause);
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

  private void executeCommand(ChannelHandlerContext ctx, Command command) throws Exception {
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
        executeWithoutTransaction(exec, command); 

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
   * @throws Exception Throws exception if exception is from within execution and not to be handled
   */
  private void executeWithoutTransaction(final Executor exec, Command command) throws Exception {
    Exception cause = null;
    for (int i = 0; i < MAXIMUM_NUM_RETRIES; i++) {
      try {
        exec.executeCommand(command, this);
        return;
      } catch (Exception e) {
        cause = e;
        if (e instanceof RegionDestroyedException || e.getCause() instanceof QueryInvocationTargetException)
          Thread.sleep(WAIT_REGION_DSTRYD_MILLIS);
      }
    }
    throw cause;
  }

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

  /**
   * {@link ByteBuf} allocator for this context. All executors
   * must use this pooled allocator as opposed to having unpooled buffers
   * for maximum performance
   * 
   * @return allocator instance
   */
  public ByteBufAllocator getByteBufAllocator() {
    return this.byteBufAllocator;
  }

  /**
   * Gets the provider of Regions
   * 
   * @return Provider
   */
  public RegionProvider getRegionProvider() {
    return this.regionProvider;
  }

  /**
   * Getter for manager to allow pausing and resuming transactions
   * @return Instance
   */
  public CacheTransactionManager getCacheTransactionManager() {
    return this.cache.getCacheTransactionManager();
  }

  /**
   * Getter for logger
   * @return instance
   */
  public LogWriter getLogger() {
    return this.cache.getLogger();
  }

  /**
   * Get the channel for this context
   * @return instance
   *
  public Channel getChannel() {
    return this.channel;
  }
   */

  /**
   * Get the authentication password, this will be same server wide.
   * It is exposed here as opposed to {@link GemFireRedisServer}.
   * @return password
   */
  public byte[] getAuthPwd() {
    return this.authPwd;
  }

  /**
   * Checker if user has authenticated themselves
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
}
