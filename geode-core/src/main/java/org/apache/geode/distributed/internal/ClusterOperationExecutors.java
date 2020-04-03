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
package org.apache.geode.distributed.internal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.InternalGemFireError;
import org.apache.geode.SystemFailure;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.gms.messages.ViewAckMessage;
import org.apache.geode.internal.logging.CoreLoggingExecutors;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.monitoring.ThreadsMonitoring;
import org.apache.geode.internal.monitoring.ThreadsMonitoringImpl;
import org.apache.geode.internal.monitoring.ThreadsMonitoringImplDummy;
import org.apache.geode.internal.tcp.ConnectionTable;
import org.apache.geode.logging.internal.log4j.api.LogService;

public class ClusterOperationExecutors implements OperationExecutors {
  private static final Logger logger = LogService.getLogger();

  /**
   * maximum time, in milliseconds, to wait for all threads to exit
   */
  static final int MAX_STOP_TIME = 20000;

  /**
   * Time to sleep, in milliseconds, while polling to see if threads have finished
   */
  static final int STOP_PAUSE_TIME = 1000;

  /**
   * Flag indicating whether to use single Serial-Executor thread or Multiple Serial-executor
   * thread,
   */
  private static final boolean MULTI_SERIAL_EXECUTORS =
      !Boolean.getBoolean("DistributionManager.singleSerialExecutor");

  private static final int MAX_WAITING_THREADS =
      Integer.getInteger("DistributionManager.MAX_WAITING_THREADS", Integer.MAX_VALUE);

  private static final int MAX_PR_META_DATA_CLEANUP_THREADS =
      Integer.getInteger("DistributionManager.MAX_PR_META_DATA_CLEANUP_THREADS", 1);

  private static final int MAX_PR_THREADS = Integer.getInteger("DistributionManager.MAX_PR_THREADS",
      Math.max(Runtime.getRuntime().availableProcessors() * 4, 16));


  private static final int INCOMING_QUEUE_LIMIT =
      Integer.getInteger("DistributionManager.INCOMING_QUEUE_LIMIT", 80000);

  /** Throttling based on the Queue byte size */
  private static final double THROTTLE_PERCENT = (double) (Integer
      .getInteger("DistributionManager.SERIAL_QUEUE_THROTTLE_PERCENT", 75)) / 100;

  private static final int SERIAL_QUEUE_BYTE_LIMIT = Integer
      .getInteger("DistributionManager.SERIAL_QUEUE_BYTE_LIMIT", (40 * (1024 * 1024)));

  private static final int SERIAL_QUEUE_THROTTLE =
      Integer.getInteger("DistributionManager.SERIAL_QUEUE_THROTTLE",
          (int) (SERIAL_QUEUE_BYTE_LIMIT * THROTTLE_PERCENT));

  private static final int TOTAL_SERIAL_QUEUE_BYTE_LIMIT =
      Integer.getInteger("DistributionManager.TOTAL_SERIAL_QUEUE_BYTE_LIMIT", (80 * (1024 * 1024)));

  private static final int TOTAL_SERIAL_QUEUE_THROTTLE =
      Integer.getInteger("DistributionManager.TOTAL_SERIAL_QUEUE_THROTTLE",
          (int) (SERIAL_QUEUE_BYTE_LIMIT * THROTTLE_PERCENT));

  /** Throttling based on the Queue item size */
  private static final int SERIAL_QUEUE_SIZE_LIMIT =
      Integer.getInteger("DistributionManager.SERIAL_QUEUE_SIZE_LIMIT", 20000);

  private static final int SERIAL_QUEUE_SIZE_THROTTLE =
      Integer.getInteger("DistributionManager.SERIAL_QUEUE_SIZE_THROTTLE",
          (int) (SERIAL_QUEUE_SIZE_LIMIT * THROTTLE_PERCENT));

  /** Max number of serial Queue executors, in case of multi-serial-queue executor */
  private static final int MAX_SERIAL_QUEUE_THREAD =
      Integer.getInteger("DistributionManager.MAX_SERIAL_QUEUE_THREAD", 20);

  // 76 not in use


  /**
   * Executor for view related messages
   *
   * @see ViewAckMessage
   */
  public static final int VIEW_EXECUTOR = 79;


  private InternalDistributedSystem system;

  private DistributionStats stats;


  /** Message processing thread pool */
  private ExecutorService threadPool;

  /**
   * High Priority processing thread pool, used for initializing messages such as UpdateAttributes
   * and CreateRegion messages
   */
  private ExecutorService highPriorityPool;

  /**
   * Waiting Pool, used for messages that may have to wait on something. Use this separate pool with
   * an unbounded queue so that waiting runnables don't get in the way of other processing threads.
   * Used for threads that will most likely have to wait for a region to be finished initializing
   * before it can proceed
   */
  private ExecutorService waitingPool;

  private ExecutorService prMetaDataCleanupThreadPool;

  /**
   * Thread used to decouple {@link org.apache.geode.internal.cache.partitioned.PartitionMessage}s
   * from {@link org.apache.geode.internal.cache.DistributedCacheOperation}s </b>
   *
   * @see #SERIAL_EXECUTOR
   */
  private ExecutorService partitionedRegionThread;
  private ExecutorService partitionedRegionPool;

  /** Function Execution executors */
  private ExecutorService functionExecutionThread;
  private ExecutorService functionExecutionPool;

  /** Message processing executor for serial, ordered, messages. */
  private ExecutorService serialThread;

  /**
   * If using a throttling queue for the serialThread, we cache the queue here so we can see if
   * delivery would block
   */
  private ThrottlingMemLinkedQueueWithDMStats<Runnable> serialQueue;

  /**
   * Thread Monitor mechanism to monitor system threads
   *
   * @see org.apache.geode.internal.monitoring.ThreadsMonitoring
   */
  private ThreadsMonitoring threadMonitor;

  private SerialQueuedExecutorPool serialQueuedExecutorPool;


  ClusterOperationExecutors(DistributionStats stats,
      InternalDistributedSystem system) {

    this.stats = stats;
    this.system = system;

    DistributionConfig config = system.getConfig();

    threadMonitor = config.getThreadMonitorEnabled() ? new ThreadsMonitoringImpl(system)
        : new ThreadsMonitoringImplDummy();


    if (MULTI_SERIAL_EXECUTORS) {
      if (logger.isInfoEnabled(LogMarker.DM_MARKER)) {
        logger.info(LogMarker.DM_MARKER,
            "Serial Queue info :" + " THROTTLE_PERCENT: " + THROTTLE_PERCENT
                + " SERIAL_QUEUE_BYTE_LIMIT :" + SERIAL_QUEUE_BYTE_LIMIT
                + " SERIAL_QUEUE_THROTTLE :" + SERIAL_QUEUE_THROTTLE
                + " TOTAL_SERIAL_QUEUE_BYTE_LIMIT :" + TOTAL_SERIAL_QUEUE_BYTE_LIMIT
                + " TOTAL_SERIAL_QUEUE_THROTTLE :" + TOTAL_SERIAL_QUEUE_THROTTLE
                + " SERIAL_QUEUE_SIZE_LIMIT :" + SERIAL_QUEUE_SIZE_LIMIT
                + " SERIAL_QUEUE_SIZE_THROTTLE :" + SERIAL_QUEUE_SIZE_THROTTLE);
      }
      // when TCP/IP is disabled we can't throttle the serial queue or we run the risk of
      // distributed deadlock when we block the UDP reader thread
      boolean throttlingDisabled = system.getConfig().getDisableTcp();
      serialQueuedExecutorPool =
          new SerialQueuedExecutorPool(stats, throttlingDisabled, threadMonitor);
    }

    {
      BlockingQueue<Runnable> poolQueue;
      if (SERIAL_QUEUE_BYTE_LIMIT == 0) {
        poolQueue = new OverflowQueueWithDMStats<>(stats.getSerialQueueHelper());
      } else {
        serialQueue =
            new ThrottlingMemLinkedQueueWithDMStats<>(TOTAL_SERIAL_QUEUE_BYTE_LIMIT,
                TOTAL_SERIAL_QUEUE_THROTTLE, SERIAL_QUEUE_SIZE_LIMIT, SERIAL_QUEUE_SIZE_THROTTLE,
                stats.getSerialQueueHelper());
        poolQueue = serialQueue;
      }
      serialThread = CoreLoggingExecutors.newSerialThreadPool("Serial Message Processor",
          thread -> stats.incSerialThreadStarts(),
          this::doSerialThread, stats.getSerialProcessorHelper(),
          threadMonitor, poolQueue);

    }

    threadPool =
        CoreLoggingExecutors.newThreadPoolWithFeedStatistics("Pooled Message Processor ",
            thread -> stats.incProcessingThreadStarts(), this::doProcessingThread,
            MAX_THREADS, stats.getNormalPoolHelper(), threadMonitor,
            INCOMING_QUEUE_LIMIT, stats.getOverflowQueueHelper());

    highPriorityPool = CoreLoggingExecutors.newThreadPoolWithFeedStatistics(
        "Pooled High Priority Message Processor ",
        thread -> stats.incHighPriorityThreadStarts(), this::doHighPriorityThread,
        MAX_THREADS, stats.getHighPriorityPoolHelper(), threadMonitor,
        INCOMING_QUEUE_LIMIT, stats.getHighPriorityQueueHelper());

    {
      BlockingQueue<Runnable> poolQueue;
      if (MAX_WAITING_THREADS == Integer.MAX_VALUE) {
        // no need for a queue since we have infinite threads
        poolQueue = new SynchronousQueue<>();
      } else {
        poolQueue = new OverflowQueueWithDMStats<>(stats.getWaitingQueueHelper());
      }
      waitingPool = CoreLoggingExecutors.newThreadPool("Pooled Waiting Message Processor ",
          thread -> stats.incWaitingThreadStarts(), this::doWaitingThread,
          MAX_WAITING_THREADS, stats.getWaitingPoolHelper(), threadMonitor, poolQueue);
    }

    // should this pool using the waiting pool stats?
    prMetaDataCleanupThreadPool =
        CoreLoggingExecutors.newThreadPoolWithFeedStatistics(
            "PrMetaData cleanup Message Processor ",
            thread -> stats.incWaitingThreadStarts(), this::doWaitingThread,
            MAX_PR_META_DATA_CLEANUP_THREADS, stats.getWaitingPoolHelper(), threadMonitor,
            0, stats.getWaitingQueueHelper());

    if (MAX_PR_THREADS > 1) {
      partitionedRegionPool =
          CoreLoggingExecutors.newThreadPoolWithFeedStatistics(
              "PartitionedRegion Message Processor",
              thread -> stats.incPartitionedRegionThreadStarts(), this::doPartitionRegionThread,
              MAX_PR_THREADS, stats.getPartitionedRegionPoolHelper(), threadMonitor,
              INCOMING_QUEUE_LIMIT, stats.getPartitionedRegionQueueHelper());
    } else {
      partitionedRegionThread = CoreLoggingExecutors.newSerialThreadPoolWithFeedStatistics(
          "PartitionedRegion Message Processor",
          thread -> stats.incPartitionedRegionThreadStarts(), this::doPartitionRegionThread,
          stats.getPartitionedRegionPoolHelper(), threadMonitor,
          INCOMING_QUEUE_LIMIT, stats.getPartitionedRegionQueueHelper());
    }
    if (MAX_FE_THREADS > 1) {
      functionExecutionPool =
          CoreLoggingExecutors.newFunctionThreadPoolWithFeedStatistics(
              FUNCTION_EXECUTION_PROCESSOR_THREAD_PREFIX,
              thread -> stats.incFunctionExecutionThreadStarts(), this::doFunctionExecutionThread,
              MAX_FE_THREADS, stats.getFunctionExecutionPoolHelper(), threadMonitor,
              INCOMING_QUEUE_LIMIT, stats.getFunctionExecutionQueueHelper());
    } else {
      functionExecutionThread =
          CoreLoggingExecutors.newSerialThreadPoolWithFeedStatistics(
              FUNCTION_EXECUTION_PROCESSOR_THREAD_PREFIX,
              thread -> stats.incFunctionExecutionThreadStarts(), this::doFunctionExecutionThread,
              stats.getFunctionExecutionPoolHelper(), threadMonitor,
              INCOMING_QUEUE_LIMIT, stats.getFunctionExecutionQueueHelper());
    }
  }

  /**
   * Returns the executor for the given type of processor.
   */
  @Override
  public Executor getExecutor(int processorType, InternalDistributedMember sender) {
    switch (processorType) {
      case STANDARD_EXECUTOR:
        return getThreadPool();
      case SERIAL_EXECUTOR:
        return getSerialExecutor(sender);
      case HIGH_PRIORITY_EXECUTOR:
        return getHighPriorityThreadPool();
      case WAITING_POOL_EXECUTOR:
        return getWaitingThreadPool();
      case PARTITIONED_REGION_EXECUTOR:
        return getPartitionedRegionExcecutor();
      case REGION_FUNCTION_EXECUTION_EXECUTOR:
        return getFunctionExecutor();
      default:
        throw new InternalGemFireError(String.format("unknown processor type %s",
            processorType));
    }
  }

  @Override
  public ExecutorService getThreadPool() {
    return threadPool;
  }

  @Override
  public ExecutorService getHighPriorityThreadPool() {
    return highPriorityPool;
  }

  @Override
  public ExecutorService getWaitingThreadPool() {
    return waitingPool;
  }

  @Override
  public ExecutorService getPrMetaDataCleanupThreadPool() {
    return prMetaDataCleanupThreadPool;
  }

  private Executor getPartitionedRegionExcecutor() {
    if (partitionedRegionThread != null) {
      return partitionedRegionThread;
    } else {
      return partitionedRegionPool;
    }
  }


  @Override
  public Executor getFunctionExecutor() {
    if (functionExecutionThread != null) {
      return functionExecutionThread;
    } else {
      return functionExecutionPool;
    }
  }

  private Executor getSerialExecutor(InternalDistributedMember sender) {
    if (MULTI_SERIAL_EXECUTORS) {
      return serialQueuedExecutorPool.getThrottledSerialExecutor(sender);
    } else {
      return serialThread;
    }
  }

  /** returns the serialThread's queue if throttling is being used, null if not */
  @Override
  public OverflowQueueWithDMStats<Runnable> getSerialQueue(InternalDistributedMember sender) {
    if (MULTI_SERIAL_EXECUTORS) {
      return serialQueuedExecutorPool.getSerialQueue(sender);
    } else {
      return serialQueue;
    }
  }

  public ThreadsMonitoring getThreadMonitoring() {
    return threadMonitor;
  }



  private void doFunctionExecutionThread(Runnable command) {
    stats.incFunctionExecutionThreads(1);
    FunctionExecutionPooledExecutor.setIsFunctionExecutionThread(Boolean.TRUE);
    try {
      ConnectionTable.threadWantsSharedResources();
      runUntilShutdown(command);
    } finally {
      ConnectionTable.releaseThreadsSockets();
      stats.incFunctionExecutionThreads(-1);
      FunctionExecutionPooledExecutor.setIsFunctionExecutionThread(Boolean.FALSE);
    }
  }

  private void doProcessingThread(Runnable command) {
    stats.incNumProcessingThreads(1);
    try {
      ConnectionTable.threadWantsSharedResources();
      runUntilShutdown(command);
    } finally {
      ConnectionTable.releaseThreadsSockets();
      stats.incNumProcessingThreads(-1);
    }
  }

  private void doHighPriorityThread(Runnable command) {
    stats.incHighPriorityThreads(1);
    try {
      ConnectionTable.threadWantsSharedResources();
      runUntilShutdown(command);
    } finally {
      ConnectionTable.releaseThreadsSockets();
      stats.incHighPriorityThreads(-1);
    }
  }

  private void doWaitingThread(Runnable command) {
    stats.incWaitingThreads(1);
    try {
      ConnectionTable.threadWantsSharedResources();
      runUntilShutdown(command);
    } finally {
      ConnectionTable.releaseThreadsSockets();
      stats.incWaitingThreads(-1);
    }
  }

  private void doPartitionRegionThread(Runnable command) {
    stats.incPartitionedRegionThreads(1);
    try {
      ConnectionTable.threadWantsSharedResources();
      runUntilShutdown(command);
    } finally {
      ConnectionTable.releaseThreadsSockets();
      stats.incPartitionedRegionThreads(-1);
    }
  }

  private void doSerialThread(Runnable command) {
    stats.incNumSerialThreads(1);
    try {
      ConnectionTable.threadWantsSharedResources();
      runUntilShutdown(command);
    } finally {
      ConnectionTable.releaseThreadsSockets();
      stats.incNumSerialThreads(-1);
    }
  }

  private void runUntilShutdown(Runnable r) {
    try {
      r.run();
    } catch (CancelException e) {
      if (logger.isTraceEnabled()) {
        logger.trace("Caught shutdown exception", e);
      }
    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error. We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (Throwable t) {
      SystemFailure.checkFailure();
      if (!system.isDisconnecting()) {
        logger.debug("Caught unusual exception during shutdown: {}", t.getMessage(), t);
      } else {
        logger.warn("Task failed with exception", t);
      }
    }
  }

  void askThreadsToStop() {
    // Stop executors after they have finished
    ExecutorService es;
    threadMonitor.close();
    es = serialThread;
    if (es != null) {
      es.shutdown();
    }
    if (serialQueuedExecutorPool != null) {
      serialQueuedExecutorPool.shutdown();
    }
    es = functionExecutionThread;
    if (es != null) {
      es.shutdown();
    }
    es = functionExecutionPool;
    if (es != null) {
      es.shutdown();
    }
    es = partitionedRegionThread;
    if (es != null) {
      es.shutdown();
    }
    es = partitionedRegionPool;
    if (es != null) {
      es.shutdown();
    }
    es = highPriorityPool;
    if (es != null) {
      es.shutdown();
    }
    es = waitingPool;
    if (es != null) {
      es.shutdown();
    }
    es = prMetaDataCleanupThreadPool;
    if (es != null) {
      es.shutdown();
    }
    es = threadPool;
    if (es != null) {
      es.shutdown();
    }
  }

  void waitForThreadsToStop(long timeInMillis) throws InterruptedException {
    long start = System.currentTimeMillis();
    long remaining = timeInMillis;

    ExecutorService[] allExecutors = new ExecutorService[] {serialThread,
        functionExecutionThread, functionExecutionPool, partitionedRegionThread,
        partitionedRegionPool, highPriorityPool, waitingPool,
        prMetaDataCleanupThreadPool, threadPool};
    for (ExecutorService es : allExecutors) {
      if (es != null) {
        es.awaitTermination(remaining, TimeUnit.MILLISECONDS);
      }
      remaining = timeInMillis - (System.currentTimeMillis() - start);
      if (remaining <= 0) {
        return;
      }
    }

    serialQueuedExecutorPool.awaitTermination(remaining, TimeUnit.MILLISECONDS);
  }

  private boolean executorAlive(ExecutorService tpe, String name) {
    if (tpe == null) {
      return false;
    } else {
      int ac = ((ThreadPoolExecutor) tpe).getActiveCount();
      // boolean result = tpe.getActiveCount() > 0;
      if (ac > 0) {
        if (logger.isDebugEnabled()) {
          logger.debug("Still waiting for {} threads in '{}' pool to exit", ac, name);
        }
        return true;
      } else {
        return false;
      }
    }
  }


  /**
   * Wait for the ancillary queues to exit. Kills them if they are still around.
   *
   */
  void forceThreadsToStop() {
    long endTime = System.currentTimeMillis() + MAX_STOP_TIME;
    StringBuilder culprits;
    for (;;) {
      boolean stillAlive = false;
      culprits = new StringBuilder();
      if (executorAlive(serialThread, "serial thread")) {
        stillAlive = true;
        culprits.append(" serial thread;");
      }
      if (executorAlive(partitionedRegionThread, "partitioned region thread")) {
        stillAlive = true;
        culprits.append(" partitioned region thread;");
      }
      if (executorAlive(partitionedRegionPool, "partitioned region pool")) {
        stillAlive = true;
        culprits.append(" partitioned region pool;");
      }
      if (executorAlive(highPriorityPool, "high priority pool")) {
        stillAlive = true;
        culprits.append(" high priority pool;");
      }
      if (executorAlive(waitingPool, "waiting pool")) {
        stillAlive = true;
        culprits.append(" waiting pool;");
      }
      if (executorAlive(prMetaDataCleanupThreadPool, "prMetaDataCleanupThreadPool")) {
        stillAlive = true;
        culprits.append(" special waiting pool;");
      }
      if (executorAlive(threadPool, "thread pool")) {
        stillAlive = true;
        culprits.append(" thread pool;");
      }

      if (!stillAlive)
        return;

      long now = System.currentTimeMillis();
      if (now >= endTime)
        break;

      try {
        Thread.sleep(STOP_PAUSE_TIME);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        // Desperation, the shutdown thread is being killed. Don't
        // consult a CancelCriterion.
        logger.warn("Interrupted during shutdown", e);
        break;
      }
    } // for

    logger.warn("Daemon threads are slow to stop; culprits include: {}",
        culprits);

    // Kill with no mercy
    if (serialThread != null) {
      serialThread.shutdownNow();
    }
    if (functionExecutionThread != null) {
      functionExecutionThread.shutdownNow();
    }
    if (functionExecutionPool != null) {
      functionExecutionPool.shutdownNow();
    }
    if (partitionedRegionThread != null) {
      partitionedRegionThread.shutdownNow();
    }
    if (partitionedRegionPool != null) {
      partitionedRegionPool.shutdownNow();
    }
    if (highPriorityPool != null) {
      highPriorityPool.shutdownNow();
    }
    if (waitingPool != null) {
      waitingPool.shutdownNow();
    }
    if (prMetaDataCleanupThreadPool != null) {
      prMetaDataCleanupThreadPool.shutdownNow();
    }
    if (threadPool != null) {
      threadPool.shutdownNow();
    }
  }

  public void handleManagerDeparture(InternalDistributedMember theId) {
    if (serialQueuedExecutorPool != null) {
      serialQueuedExecutorPool.handleMemberDeparture(theId);
    }
  }

  /**
   * This class is used for DM's multi serial executor. The serial messages are managed/executed by
   * multiple serial thread. This class takes care of executing messages related to a sender using
   * the same thread.
   */
  private static class SerialQueuedExecutorPool {
    /** To store the serial threads */
    final ConcurrentMap<Integer, ExecutorService> serialQueuedExecutorMap =
        new ConcurrentHashMap<>(MAX_SERIAL_QUEUE_THREAD);

    /** To store the queue associated with thread */
    final Map<Integer, OverflowQueueWithDMStats<Runnable>> serialQueuedMap =
        new HashMap<>(MAX_SERIAL_QUEUE_THREAD);

    /** Holds mapping between sender to the serial thread-id */
    final Map<InternalDistributedMember, Integer> senderToSerialQueueIdMap = new HashMap<>();

    /**
     * Holds info about unused thread, a thread is marked unused when the member associated with it
     * has left distribution system.
     */
    final ArrayList<Integer> threadMarkedForUse = new ArrayList<>();

    final DistributionStats stats;

    final boolean throttlingDisabled;

    final ThreadsMonitoring threadMonitoring;

    SerialQueuedExecutorPool(DistributionStats stats,
        boolean throttlingDisabled, ThreadsMonitoring tMonitoring) {
      this.stats = stats;
      this.throttlingDisabled = throttlingDisabled;
      threadMonitoring = tMonitoring;
    }

    /*
     * Returns an id of the thread in serialQueuedExecutorMap, thats mapped to the given seder.
     *
     *
     * @param createNew boolean flag to indicate whether to create a new id, if id doesnot exists.
     */
    private Integer getQueueId(InternalDistributedMember sender, boolean createNew) {
      // Create a new Id.
      Integer queueId;

      synchronized (senderToSerialQueueIdMap) {
        // Check if there is a executor associated with this sender.
        queueId = senderToSerialQueueIdMap.get(sender);

        if (!createNew || queueId != null) {
          return queueId;
        }

        // Create new.
        // Check if any threads are availabe that is marked for Use.
        if (!threadMarkedForUse.isEmpty()) {
          queueId = threadMarkedForUse.remove(0);
        }
        // If Map is full, use the threads in round-robin fashion.
        if (queueId == null) {
          queueId = (serialQueuedExecutorMap.size() + 1) % MAX_SERIAL_QUEUE_THREAD;
        }
        senderToSerialQueueIdMap.put(sender, queueId);
      }
      return queueId;
    }

    /*
     * Returns the queue associated with this sender. Used in FlowControl for throttling (based on
     * queue size).
     */
    OverflowQueueWithDMStats<Runnable> getSerialQueue(InternalDistributedMember sender) {
      Integer queueId = getQueueId(sender, false);
      if (queueId == null) {
        return null;
      }
      return serialQueuedMap.get(queueId);
    }

    /*
     * Returns the serial queue executor, before returning the thread this applies throttling, based
     * on the total serial queue size (total - sum of all the serial queue size). The throttling is
     * applied during put event, this doesnt block the extract operation on the queue.
     *
     */
    ExecutorService getThrottledSerialExecutor(
        InternalDistributedMember sender) {
      ExecutorService executor = getSerialExecutor(sender);

      // Get the total serial queue size.
      long totalSerialQueueMemSize = stats.getInternalSerialQueueBytes();

      // for tcp socket reader threads, this code throttles the thread
      // to keep the sender-side from overwhelming the receiver.
      // UDP readers are throttled in the FC protocol, which queries
      // the queue to see if it should throttle
      if (stats.getInternalSerialQueueBytes() > TOTAL_SERIAL_QUEUE_THROTTLE
          && !DistributionMessage.isMembershipMessengerThread()) {
        do {
          boolean interrupted = Thread.interrupted();
          try {
            float throttlePercent = (float) (totalSerialQueueMemSize - TOTAL_SERIAL_QUEUE_THROTTLE)
                / (float) (TOTAL_SERIAL_QUEUE_BYTE_LIMIT - TOTAL_SERIAL_QUEUE_THROTTLE);
            int sleep = (int) (100.0 * throttlePercent);
            sleep = Math.max(sleep, 1);
            Thread.sleep(sleep);
          } catch (InterruptedException ex) {
            interrupted = true;
            // FIXME-InterruptedException
            // Perhaps we should return null here?
          } finally {
            if (interrupted) {
              Thread.currentThread().interrupt();
            }
          }
          stats.getSerialQueueHelper().incThrottleCount();
        } while (stats.getInternalSerialQueueBytes() >= TOTAL_SERIAL_QUEUE_BYTE_LIMIT);
      }
      return executor;
    }

    /*
     * Returns the serial queue executor for the given sender.
     */
    ExecutorService getSerialExecutor(InternalDistributedMember sender) {
      ExecutorService executor;
      Integer queueId = getQueueId(sender, true);
      if ((executor =
          serialQueuedExecutorMap.get(queueId)) != null) {
        return executor;
      }
      // If executor doesn't exists for this sender, create one.
      executor = createSerialExecutor(queueId);

      serialQueuedExecutorMap.put(queueId, executor);

      if (logger.isDebugEnabled()) {
        logger.debug(
            "Created Serial Queued Executor With queueId {}. Total number of live Serial Threads :{}",
            queueId, serialQueuedExecutorMap.size());
      }
      stats.incSerialPooledThread();
      return executor;
    }

    /*
     * Creates a serial queue executor.
     */
    private ExecutorService createSerialExecutor(final Integer id) {

      OverflowQueueWithDMStats<Runnable> poolQueue;

      if (SERIAL_QUEUE_BYTE_LIMIT == 0 || throttlingDisabled) {
        poolQueue = new OverflowQueueWithDMStats<>(stats.getSerialQueueHelper());
      } else {
        poolQueue = new ThrottlingMemLinkedQueueWithDMStats<>(SERIAL_QUEUE_BYTE_LIMIT,
            SERIAL_QUEUE_THROTTLE, SERIAL_QUEUE_SIZE_LIMIT, SERIAL_QUEUE_SIZE_THROTTLE,
            stats.getSerialQueueHelper());
      }

      serialQueuedMap.put(id, poolQueue);

      return CoreLoggingExecutors.newSerialThreadPool("Pooled Serial Message Processor" + id + "-",
          thread -> stats.incSerialPooledThreadStarts(), this::doSerialPooledThread,
          stats.getSerialPooledProcessorHelper(), threadMonitoring, poolQueue);
    }

    private void doSerialPooledThread(Runnable command) {
      ConnectionTable.threadWantsSharedResources();
      try {
        command.run();
      } finally {
        ConnectionTable.releaseThreadsSockets();
      }
    }

    /*
     * Does cleanup relating to this member. And marks the serial executor associated with this
     * member for re-use.
     */
    private void handleMemberDeparture(InternalDistributedMember member) {
      Integer queueId = getQueueId(member, false);
      if (queueId == null) {
        return;
      }

      boolean isUsed = false;

      synchronized (senderToSerialQueueIdMap) {
        senderToSerialQueueIdMap.remove(member);

        // Check if any other members are using the same executor.
        for (Integer value : senderToSerialQueueIdMap.values()) {
          if (value.equals(queueId)) {
            isUsed = true;
            break;
          }
        }

        // If not used mark this as unused.
        if (!isUsed) {
          if (logger.isInfoEnabled(LogMarker.DM_MARKER))
            logger.info(LogMarker.DM_MARKER,
                "Marking the SerialQueuedExecutor with id : {} used by the member {} to be unused.",
                new Object[] {queueId, member});

          threadMarkedForUse.add(queueId);
        }
      }
    }

    private void awaitTermination(long time, TimeUnit unit) throws InterruptedException {
      long remainingNanos = unit.toNanos(time);
      long start = System.nanoTime();
      for (ExecutorService executor : serialQueuedExecutorMap.values()) {
        executor.awaitTermination(remainingNanos, TimeUnit.NANOSECONDS);
        remainingNanos = (System.nanoTime() - start);
        if (remainingNanos <= 0) {
          return;
        }
      }
    }

    private void shutdown() {
      for (ExecutorService executor : serialQueuedExecutorMap
          .values()) {
        executor.shutdown();
      }
    }
  }

}
