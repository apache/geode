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
package org.apache.geode.internal.cache.wan;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.GemFireIOException;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.client.ServerConnectivityException;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.cache.client.internal.Connection;
import org.apache.geode.cache.client.internal.SenderProxy;
import org.apache.geode.cache.client.internal.pooling.ConnectionDestroyedException;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.UpdateAttributesProcessor;
import org.apache.geode.internal.cache.tier.sockets.MessageTooLargeException;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;
import org.apache.geode.pdx.PdxRegistryMismatchException;
import org.apache.geode.security.GemFireSecurityException;

/**
 * @since GemFire 7.0
 */
public class GatewaySenderEventRemoteDispatcher implements GatewaySenderEventDispatcher {

  private static final Logger logger = LogService.getLogger();

  protected final AbstractGatewaySenderEventProcessor processor;

  private volatile Connection connection;

  private final Set<String> notFoundRegions = new HashSet<String>();

  private final Object notFoundRegionsSync = new Object();

  private final AbstractGatewaySender sender;

  private AckReaderThread ackReaderThread;

  private ReentrantReadWriteLock connectionLifeCycleLock = new ReentrantReadWriteLock();

  /**
   * This count is reset to 0 each time a successful connection is made.
   */
  private int failedConnectCount = 0;

  void setAckReaderThread(AckReaderThread ackReaderThread) {
    this.ackReaderThread = ackReaderThread;
  }

  public GatewaySenderEventRemoteDispatcher(AbstractGatewaySenderEventProcessor eventProcessor) {
    this.processor = eventProcessor;
    this.sender = eventProcessor.getSender();
    // this.ackReaderThread = new AckReaderThread(sender);
    try {
      initializeConnection();
    } catch (GatewaySenderException e) {
      // It is ok to ignore this exception. It is logged in the initializeConnection call.
    }
  }

  GatewaySenderEventRemoteDispatcher(AbstractGatewaySenderEventProcessor processor,
      Connection connection) {
    this.processor = processor;
    this.sender = processor.getSender();
    this.connection = connection;
  }

  protected GatewayAck readAcknowledgement() {
    SenderProxy sp = new SenderProxy(this.processor.getSender().getProxy());
    GatewayAck ack = null;
    Exception ex;
    try {
      connection = getConnection(false);
      if (logger.isDebugEnabled()) {
        logger.debug(" Receiving ack on the thread {}", connection);
      }
      this.connectionLifeCycleLock.readLock().lock();
      try {
        if (connection != null && !processor.isStopped()) {
          ack = (GatewayAck) sp.receiveAckFromReceiver(connection);
        }
      } finally {
        this.connectionLifeCycleLock.readLock().unlock();
      }

    } catch (Exception e) {
      Throwable t = e.getCause();
      if (t instanceof BatchException70) {
        // A BatchException has occurred.
        // Do not process the connection as dead since it is not dead.
        ex = (BatchException70) t;
      } else if (e instanceof GatewaySenderException) { // This Exception is thrown from
                                                        // getConnection
        ex = (Exception) e.getCause();
      } else {
        ex = e;
        // keep using the connection if we had a batch exception. Else, destroy
        // it
        destroyConnection();
      }
      if (this.sender.getProxy() == null || this.sender.getProxy().isDestroyed()) {
        // if our pool is shutdown then just be silent
      } else if (ex instanceof IOException
          || (ex instanceof ServerConnectivityException
              && !(ex.getCause() instanceof PdxRegistryMismatchException))
          || ex instanceof ConnectionDestroyedException) {
        // If the cause is an IOException or a ServerException, sleep and retry.
        // Sleep for a bit and recheck.
        try {
          Thread.sleep(100);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
        }
      } else {
        if (!(ex instanceof CancelException)) {
          logger.fatal(
              LocalizedMessage.create(
                  LocalizedStrings.GatewayEventRemoteDispatcher_STOPPING_THE_PROCESSOR_BECAUSE_THE_FOLLOWING_EXCEPTION_OCCURRED_WHILE_PROCESSING_A_BATCH),
              ex);
        }
        this.processor.setIsStopped(true);
      }
    }
    return ack;
  }

  @Override
  public boolean dispatchBatch(List events, boolean removeFromQueueOnException, boolean isRetry) {
    GatewaySenderStats statistics = this.sender.getStatistics();
    boolean success = false;
    try {
      long start = statistics.startTime();
      success = _dispatchBatch(events, isRetry);
      if (success) {
        statistics.endBatch(start, events.size());
      }
    } catch (GatewaySenderException ge) {

      Throwable t = ge.getCause();
      if (this.sender.getProxy() == null || this.sender.getProxy().isDestroyed()) {
        // if our pool is shutdown then just be silent
      } else if (t instanceof IOException || t instanceof ServerConnectivityException
          || t instanceof ConnectionDestroyedException || t instanceof MessageTooLargeException
          || t instanceof IllegalStateException || t instanceof GemFireSecurityException) {
        this.processor.handleException();
        // If the cause is an IOException or a ServerException, sleep and retry.
        // Sleep for a bit and recheck.
        try {
          Thread.sleep(100);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
        }
        if (logger.isDebugEnabled()) {
          logger.debug("Because of IOException, failed to dispatch a batch with id : {}",
              this.processor.getBatchId());
        }
      } else {
        logger.fatal(
            LocalizedMessage.create(
                LocalizedStrings.GatewayEventRemoteDispatcher_STOPPING_THE_PROCESSOR_BECAUSE_THE_FOLLOWING_EXCEPTION_OCCURRED_WHILE_PROCESSING_A_BATCH),
            ge);
        this.processor.setIsStopped(true);
      }
    } catch (CancelException e) {
      if (logger.isDebugEnabled()) {
        logger
            .debug("Stopping the processor because cancellation occurred while processing a batch");
      }
      this.processor.setIsStopped(true);
      throw e;
    } catch (Exception e) {
      this.processor.setIsStopped(true);
      logger.fatal(
          LocalizedMessage.create(
              LocalizedStrings.GatewayEventRemoteDispatcher_STOPPING_THE_PROCESSOR_BECAUSE_THE_FOLLOWING_EXCEPTION_OCCURRED_WHILE_PROCESSING_A_BATCH),
          e);
    }
    return success;
  }

  private boolean _dispatchBatch(List events, boolean isRetry) {
    Exception ex = null;
    int currentBatchId = this.processor.getBatchId();
    connection = getConnection(true);
    int batchIdForThisConnection = this.processor.getBatchId();
    GatewaySenderStats statistics = this.sender.getStatistics();
    // This means we are writing to a new connection than the previous batch.
    // i.e The connection has been reset. It also resets the batchId.
    if (currentBatchId != batchIdForThisConnection || this.processor.isConnectionReset()) {
      return false;
    }
    try {
      if (this.processor.isConnectionReset()) {
        isRetry = true;
      }
      SenderProxy sp = new SenderProxy(this.sender.getProxy());
      this.connectionLifeCycleLock.readLock().lock();
      try {
        if (connection != null) {
          sp.dispatchBatch_NewWAN(connection, events, currentBatchId,
              sender.isRemoveFromQueueOnException(), isRetry);
          if (logger.isDebugEnabled()) {
            logger.debug(
                "{} : Dispatched batch (id={}) of {} events, queue size: {} on connection {}",
                this.processor.getSender(), currentBatchId, events.size(),
                this.processor.getQueue().size(), connection);
          }
        } else {
          throw new ConnectionDestroyedException();
        }
      } finally {
        this.connectionLifeCycleLock.readLock().unlock();
      }
      return true;
    } catch (ServerOperationException e) {
      Throwable t = e.getCause();
      if (t instanceof BatchException70) {
        // A BatchException has occurred.
        // Do not process the connection as dead since it is not dead.
        ex = (BatchException70) t;
      } else {
        ex = e;
        // keep using the connection if we had a batch exception. Else, destroy it
        destroyConnection();
      }
      throw new GatewaySenderException(
          LocalizedStrings.GatewayEventRemoteDispatcher_0_EXCEPTION_DURING_PROCESSING_BATCH_1_ON_CONNECTION_2
              .toLocalizedString(new Object[] {this, Integer.valueOf(currentBatchId), connection}),
          ex);
    } catch (GemFireIOException e) {
      Throwable t = e.getCause();
      if (t instanceof MessageTooLargeException) {
        // A MessageTooLargeException has occurred.
        // Do not process the connection as dead since it is not dead.
        ex = (MessageTooLargeException) t;
        // Reduce the batch size by half of the configured batch size or number of events in the
        // current batch (whichever is less)
        int newBatchSize = Math.min(events.size(), this.processor.getBatchSize()) / 2;
        logger.warn(LocalizedMessage.create(
            LocalizedStrings.GatewaySenderEventRemoteDispatcher_MESSAGE_TOO_LARGE_EXCEPTION,
            new Object[] {events.size(), newBatchSize}), e);
        this.processor.setBatchSize(newBatchSize);
        statistics.incBatchesResized();
      } else {
        ex = e;
        // keep using the connection if we had a MessageTooLargeException. Else, destroy it
        destroyConnection();
      }
      throw new GatewaySenderException(
          LocalizedStrings.GatewayEventRemoteDispatcher_0_EXCEPTION_DURING_PROCESSING_BATCH_1_ON_CONNECTION_2
              .toLocalizedString(new Object[] {this, Integer.valueOf(currentBatchId), connection}),
          ex);
    } catch (IllegalStateException e) {
      this.processor.setException(new GatewaySenderException(e));
      throw new GatewaySenderException(
          LocalizedStrings.GatewayEventRemoteDispatcher_0_EXCEPTION_DURING_PROCESSING_BATCH_1_ON_CONNECTION_2
              .toLocalizedString(new Object[] {this, Integer.valueOf(currentBatchId), connection}),
          e);
    } catch (Exception e) {
      // An Exception has occurred. Get its cause.
      Throwable t = e.getCause();
      if (t instanceof IOException) {
        // An IOException has occurred.
        ex = (IOException) t;
      } else {
        ex = e;
      }
      // the cause is not going to be BatchException70. So, destroy the connection
      destroyConnection();

      throw new GatewaySenderException(
          LocalizedStrings.GatewayEventRemoteDispatcher_0_EXCEPTION_DURING_PROCESSING_BATCH_1_ON_CONNECTION_2
              .toLocalizedString(new Object[] {this, Integer.valueOf(currentBatchId), connection}),
          ex);
    }
  }

  /**
   * Acquires or adds a new <code>Connection</code> to the corresponding <code>Gateway</code>
   *
   * @return the <code>Connection</code>
   *
   */
  public Connection getConnection(boolean startAckReaderThread) throws GatewaySenderException {
    if (this.processor.isStopped()) {
      stop();
      return null;
    }
    // IF the connection is null
    // OR the connection's ServerLocation doesn't match with the one stored in sender
    // THEN initialize the connection
    if (!this.sender.isParallel()) {
      if (this.connection == null || this.connection.isDestroyed()
          || !this.connection.getServer().equals(this.sender.getServerLocation())) {
        if (logger.isDebugEnabled()) {
          logger.debug(
              "Initializing new connection as serverLocation of old connection is : {} and the serverLocation to connect is {}",
              ((this.connection == null) ? "null" : this.connection.getServer()),
              this.sender.getServerLocation());
        }
        // Initialize the connection
        initializeConnection();
      }
    } else {
      if (this.connection == null || this.connection.isDestroyed()) {
        initializeConnection();
      }
    }

    // Here we might wait on a connection to another server if I was secondary
    // so don't start waiting until I am primary
    InternalCache cache = this.sender.getCache();
    if (cache != null && !cache.isClosed()) {
      if (this.sender.isPrimary() && (this.connection != null)) {
        if (this.ackReaderThread == null || !this.ackReaderThread.isRunning()) {
          this.ackReaderThread = new AckReaderThread(this.sender, this.processor);
          this.ackReaderThread.start();
          this.ackReaderThread.waitForRunningAckReaderThreadRunningState();
        }
      }
    }
    return this.connection;
  }

  public void destroyConnection() {
    this.connectionLifeCycleLock.writeLock().lock();
    try {
      Connection con = this.connection;
      if (con != null) {
        if (!con.isDestroyed()) {
          con.destroy();
          this.sender.getProxy().returnConnection(con);
        }

        // Reset the connection so the next time through a new one will be
        // obtained
        this.connection = null;
        this.sender.setServerLocation(null);
      }
    } finally {
      this.connectionLifeCycleLock.writeLock().unlock();
    }
  }

  /**
   * Initializes the <code>Connection</code>.
   *
   */
  private void initializeConnection() throws GatewaySenderException, GemFireSecurityException {
    if (ackReaderThread != null) {
      ackReaderThread.shutDownAckReaderConnection(connection);
    }
    this.connectionLifeCycleLock.writeLock().lock();
    try {
      // Attempt to acquire a connection
      if (this.sender.getProxy() == null || this.sender.getProxy().isDestroyed()) {
        this.sender.initProxy();
      } else {
        this.processor.resetBatchId();
      }
      Connection con;
      try {
        if (this.sender.isParallel()) {
          /*
           * TODO - The use of acquireConnection should be removed from the gateway code. This
           * method is fine for tests, but these connections should really be managed inside the
           * pool code. If the gateway needs to persistent connection to a single server, which
           * should create have the OpExecutor that holds a reference to the connection (similar to
           * the way we do with thread local connections). Use {@link
           * ExecutablePool#setupServerAffinity(boolean)} for gateway code
           */
          con = this.sender.getProxy().acquireConnection();
          // For parallel sender, setting server location will not matter.
          // everytime it will ask for acquire connection whenever it needs it. I
          // am saving this server location for command purpose
          sender.setServerLocation(con.getServer());
        } else {
          synchronized (this.sender.getLockForConcurrentDispatcher()) {
            ServerLocation server = this.sender.getServerLocation();
            if (server != null) {
              if (logger.isDebugEnabled()) {
                logger.debug("ServerLocation is: {}. Connecting to this serverLocation...", server);
              }
              con = this.sender.getProxy().acquireConnection(server);
            } else {
              if (logger.isDebugEnabled()) {
                logger.debug("ServerLocation is null. Creating new connection. ");
              }
              con = this.sender.getProxy().acquireConnection();
              // Acquired connection from pool!! Update the server location
              // information in the sender and
              // distribute the information to other senders ONLY IF THIS SENDER
              // IS
              // PRIMARY
              if (this.sender.isPrimary()) {
                if (sender.getServerLocation() == null) {
                  sender.setServerLocation(con.getServer());
                }
                new UpdateAttributesProcessor(this.sender).distribute(false);
              }
            }
          }
        }
      } catch (ServerConnectivityException e) {
        // Get the exception to throw
        GatewaySenderException gse = getInitializeConnectionExceptionToThrow(e);

        // Set the serverLocation to null so that a new connection can be obtained in next attempt
        this.sender.setServerLocation(null);

        // Log the exception if necessary
        if (logConnectionFailure()) {
          // only log this message once; another msg is logged once we connect
          logger.warn(LocalizedMessage.create(
              LocalizedStrings.GatewayEventRemoteDispatcher_0_COULD_NOT_CONNECT_1,
              new Object[] {this.processor.getSender().getId(), gse.getCause().getMessage()}));
        }

        // Increment failed connection count
        this.failedConnectCount++;

        // Throw the exception
        throw gse;
      }
      if (this.failedConnectCount > 0) {
        Object[] logArgs =
            new Object[] {this.processor.getSender().getId(), con, this.failedConnectCount};
        logger.info(LocalizedMessage.create(
            LocalizedStrings.GatewayEventRemoteDispatcher_0_USING_1_AFTER_2_FAILED_CONNECT_ATTEMPTS,
            logArgs));
        this.failedConnectCount = 0;
      } else {
        Object[] logArgs = new Object[] {this.processor.getSender().getId(), con};
        logger.info(LocalizedMessage.create(LocalizedStrings.GatewayEventRemoteDispatcher_0_USING_1,
            logArgs));
      }
      this.connection = con;
      this.processor.checkIfPdxNeedsResend(this.connection.getQueueStatus().getPdxSize());
    } catch (ConnectionDestroyedException e) {
      throw new GatewaySenderException(
          LocalizedStrings.GatewayEventRemoteDispatcher_0_COULD_NOT_CONNECT_1.toLocalizedString(
              new Object[] {this.processor.getSender().getId(), e.getMessage()}),
          e);
    } finally {
      this.connectionLifeCycleLock.writeLock().unlock();
    }
  }

  private GatewaySenderException getInitializeConnectionExceptionToThrow(
      ServerConnectivityException e) {
    GatewaySenderException gse = null;
    if (e.getCause() instanceof GemFireSecurityException) {
      gse = new GatewaySenderException(e.getCause());
    } else {
      List<ServerLocation> servers = this.sender.getProxy().getCurrentServers();
      String ioMsg;
      if (servers.size() == 0) {
        ioMsg = LocalizedStrings.GatewayEventRemoteDispatcher_THERE_ARE_NO_ACTIVE_SERVERS
            .toLocalizedString();
      } else {
        final StringBuilder buffer = new StringBuilder();
        for (ServerLocation server : servers) {
          String endpointName = String.valueOf(server);
          if (buffer.length() > 0) {
            buffer.append(", ");
          }
          buffer.append(endpointName);
        }
        ioMsg =
            LocalizedStrings.GatewayEventRemoteDispatcher_NO_AVAILABLE_CONNECTION_WAS_FOUND_BUT_THE_FOLLOWING_ACTIVE_SERVERS_EXIST_0
                .toLocalizedString(buffer.toString());
      }
      IOException ex = new IOException(ioMsg);
      gse = new GatewaySenderException(
          LocalizedStrings.GatewayEventRemoteDispatcher_0_COULD_NOT_CONNECT_1.toLocalizedString(
              new Object[] {this.processor.getSender().getId(), ex.getMessage()}),
          ex);
    }
    return gse;
  }

  protected boolean logConnectionFailure() {
    // always log the first failure
    if (logger.isDebugEnabled() || this.failedConnectCount == 0) {
      return true;
    } else {
      // subsequent failures will be logged on 30th, 300th, 3000th try
      // each try is at 100millis from higher layer so this accounts for logging
      // after 3s, 30s and then every 5mins
      if (this.failedConnectCount >= 3000) {
        return (this.failedConnectCount % 3000) == 0;
      } else {
        return (this.failedConnectCount == 30 || this.failedConnectCount == 300);
      }
    }
  }

  public static class GatewayAck {
    private int batchId;

    private int numEvents;

    private BatchException70 be;

    public GatewayAck(BatchException70 be, int bId) {
      this.be = be;
      this.batchId = bId;
    }

    public GatewayAck(int batchId, int numEvents) {
      this.batchId = batchId;
      this.numEvents = numEvents;
    }

    /**
     * @return the numEvents
     */
    public int getNumEvents() {
      return numEvents;
    }

    /**
     * @return the batchId
     */
    public int getBatchId() {
      return batchId;
    }

    public BatchException70 getBatchException() {
      return this.be;
    }
  }

  class AckReaderThread extends Thread {

    private Object runningStateLock = new Object();

    /**
     * boolean to make a shutdown request
     */
    private volatile boolean shutdown = false;

    private final InternalCache cache;

    private volatile boolean ackReaderThreadRunning = false;

    public AckReaderThread(GatewaySender sender, AbstractGatewaySenderEventProcessor processor) {
      this(sender, processor.getName());
    }

    boolean isShutdown() {
      return shutdown;
    }

    public AckReaderThread(GatewaySender sender, String name) {
      super("AckReaderThread for : " + name);
      this.setDaemon(true);
      this.cache = ((AbstractGatewaySender) sender).getCache();
    }

    public void waitForRunningAckReaderThreadRunningState() {
      synchronized (runningStateLock) {
        while (!this.ackReaderThreadRunning) {
          try {
            this.runningStateLock.wait();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            break;
          }
        }
      }
    }

    private boolean checkCancelled() {
      if (shutdown) {
        return true;
      }

      if (cache.getCancelCriterion().isCancelInProgress()) {
        return true;
      }
      return false;
    }

    @Override
    public void run() {
      if (logger.isDebugEnabled()) {
        logger.debug("AckReaderThread started.. ");
      }

      synchronized (runningStateLock) {
        ackReaderThreadRunning = true;
        this.runningStateLock.notifyAll();
      }

      try {
        for (;;) {
          if (checkCancelled()) {
            break;
          }
          GatewayAck ack = readAcknowledgement();
          if (ack != null) {
            boolean gotBatchException = ack.getBatchException() != null;
            int batchId = ack.getBatchId();
            int numEvents = ack.getNumEvents();

            // If the batch is successfully processed, remove it from the
            // queue.
            if (gotBatchException) {
              logger.warn(LocalizedMessage.create(
                  LocalizedStrings.GatewaySenderEventRemoteDispatcher_GATEWAY_SENDER_0_RECEIVED_ACK_FOR_BATCH_ID_1_WITH_EXCEPTION,
                  new Object[] {processor.getSender(), ack.getBatchId()}));
              // If we get PDX related exception in the batch exception then try
              // to resend all the pdx events as well in the next batch.
              final GatewaySenderStats statistics = sender.getStatistics();
              statistics.incBatchesRedistributed();
              if (sender.isRemoveFromQueueOnException()) {
                // log the batchExceptions
                logBatchExceptions(ack.getBatchException());
                processor.handleSuccessBatchAck(batchId);
              } else {
                // log the batchExceptions. These are exceptions that were not retried on the remote
                // site (e.g. NotAuthorizedException)
                // @TODO Shoud anything else be done here to warn that events are lost even though
                // the boolean is false
                logBatchExceptions(ack.getBatchException());
                processor.handleSuccessBatchAck(batchId);
              }
            } // unsuccessful batch
            else { // The batch was successful.
              if (logger.isDebugEnabled()) {
                logger.debug("Gateway Sender {} : Received ack for batch id {} of {} events",
                    processor.getSender(), ack.getBatchId(), ack.getNumEvents());
              }
              processor.handleSuccessBatchAck(batchId);
            }
          } else {
            // If we have received IOException.
            if (logger.isDebugEnabled()) {
              logger.debug("{}: Received null ack from remote site.", processor.getSender());
            }
            processor.handleException();
            try { // This wait is before trying to getting new connection to
                  // receive ack. Without this there will be continuous call to
                  // getConnection
              Thread.sleep(GatewaySender.CONNECTION_RETRY_INTERVAL);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          }
        }
      } catch (Exception e) {
        if (!checkCancelled()) {
          logger.fatal(
              LocalizedMessage.create(
                  LocalizedStrings.GatewayEventRemoteDispatcher_STOPPING_THE_PROCESSOR_BECAUSE_THE_FOLLOWING_EXCEPTION_OCCURRED_WHILE_PROCESSING_A_BATCH),
              e);
        }
        sender.getLifeCycleLock().writeLock().lock();
        try {
          processor.stopProcessing();
          sender.clearTempEventsAfterSenderStopped();
        } finally {
          sender.getLifeCycleLock().writeLock().unlock();
        }
        // destroyConnection();
      } finally {
        if (logger.isDebugEnabled()) {
          logger.debug("AckReaderThread exiting. ");
        }
        ackReaderThreadRunning = false;
      }
    }

    protected void logBatchExceptions(BatchException70 exception) {
      try {
        for (BatchException70 be : exception.getExceptions()) {
          boolean logWarning = true;
          if (be.getCause() instanceof RegionDestroyedException) {
            RegionDestroyedException rde = (RegionDestroyedException) be.getCause();
            synchronized (notFoundRegionsSync) {
              if (notFoundRegions.contains(rde.getRegionFullPath())) {
                logWarning = false;
              } else {
                notFoundRegions.add(rde.getRegionFullPath());
              }
            }
          } else if (be.getCause() instanceof IllegalStateException
              && be.getCause().getMessage().contains("Unknown pdx type")) {
            List<GatewaySenderEventImpl> pdxEvents =
                processor.getBatchIdToPDXEventsMap().get(be.getBatchId());
            if (logWarning) {
              logger.warn(LocalizedMessage.create(
                  LocalizedStrings.GatewayEventRemoteDispatcher_A_BATCHEXCEPTION_OCCURRED_PROCESSING_PDX_EVENT__0,
                  be.getIndex()), be);
            }
            if (pdxEvents != null) {
              for (GatewaySenderEventImpl senderEvent : pdxEvents) {
                senderEvent.isAcked = false;
              }
              GatewaySenderEventImpl gsEvent = pdxEvents.get(be.getIndex());
              if (logWarning) {
                logger.warn(LocalizedMessage.create(
                    LocalizedStrings.GatewayEventRemoteDispatcher_THE_EVENT_BEING_PROCESSED_WHEN_THE_BATCHEXCEPTION_OCCURRED_WAS__0,
                    gsEvent));
              }
            }
            continue;
          }
          if (logWarning) {
            logger.warn(LocalizedMessage.create(
                LocalizedStrings.GatewayEventRemoteDispatcher_A_BATCHEXCEPTION_OCCURRED_PROCESSING_EVENT__0,
                be.getIndex()), be);
          }
          List<GatewaySenderEventImpl>[] eventsArr =
              processor.getBatchIdToEventsMap().get(be.getBatchId());
          if (eventsArr != null) {
            List<GatewaySenderEventImpl> filteredEvents = eventsArr[1];
            GatewaySenderEventImpl gsEvent =
                (GatewaySenderEventImpl) filteredEvents.get(be.getIndex());
            if (logWarning) {
              logger.warn(LocalizedMessage.create(
                  LocalizedStrings.GatewayEventRemoteDispatcher_THE_EVENT_BEING_PROCESSED_WHEN_THE_BATCHEXCEPTION_OCCURRED_WAS__0,
                  gsEvent));
            }
          }
        }
      } catch (Exception e) {
        logger.warn(
            LocalizedMessage.create(
                LocalizedStrings.GatewayEventRemoteDispatcher_AN_EXCEPTION_OCCURRED_PROCESSING_A_BATCHEXCEPTION__0),
            e);
      }
    }

    boolean isRunning() {
      return this.ackReaderThreadRunning;
    }

    public void shutdown() {
      // we need to destroy connection irrespective of we are listening on it or
      // not. No need to take lock as the reader thread may be blocked and we might not
      // get chance to destroy unless that returns.
      Connection conn = connection;
      if (conn != null) {
        shutDownAckReaderConnection(conn);
        if (!conn.isDestroyed()) {
          conn.destroy();
          sender.getProxy().returnConnection(conn);
        }
      }
      this.shutdown = true;
      boolean interrupted = Thread.interrupted();
      try {
        this.join(15 * 1000);
      } catch (InterruptedException e) {
        interrupted = true;
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
      if (this.isAlive()) {
        logger.warn(LocalizedMessage
            .create(LocalizedStrings.GatewaySender_ACKREADERTHREAD_IGNORED_CANCELLATION));
      }
    }

    protected void shutDownAckReaderConnection(Connection connection) {
      Connection conn = connection;
      // attempt to unblock the ackReader thread by shutting down the inputStream, if it was stuck
      // on a read
      try {
        if (conn != null && conn.getInputStream() != null) {
          conn.getInputStream().close();
        }
      } catch (IOException e) {
        logger.warn("Unable to shutdown AckReaderThread Connection");
      } catch (ConnectionDestroyedException e) {
        logger.info("AckReader shutting down and connection already destroyed");
      }
    }
  }

  public void stopAckReaderThread() {
    if (this.ackReaderThread != null) {
      this.ackReaderThread.shutdown();
    }
  }

  @Override
  public boolean isRemoteDispatcher() {
    return true;
  }

  @Override
  public boolean isConnectedToRemote() {
    return connection != null && !connection.isDestroyed();
  }

  public void shutDownAckReaderConnection() {
    if (ackReaderThread != null) {
      ackReaderThread.shutDownAckReaderConnection(connection);
      ackReaderThread.shutdown();
    }
  }

  public void stop() {
    stopAckReaderThread();
    if (this.processor.isStopped()) {
      destroyConnection();
    }
  }
}
