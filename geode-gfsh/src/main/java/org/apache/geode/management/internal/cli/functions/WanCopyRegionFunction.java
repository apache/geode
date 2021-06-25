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
package org.apache.geode.management.internal.cli.functions;

import java.io.IOException;
import java.io.Serializable;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.EntryDestroyedException;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.AllConnectionsInUseException;
import org.apache.geode.cache.client.NoAvailableServersException;
import org.apache.geode.cache.client.ServerConnectivityException;
import org.apache.geode.cache.client.internal.Connection;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.client.internal.pooling.ConnectionDestroyedException;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.wan.GatewayQueueEvent;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.DefaultEntryEventFactory;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EntrySnapshot;
import org.apache.geode.internal.cache.EnumListenerEvent;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.NonTXEntry;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.BatchException70;
import org.apache.geode.internal.cache.wan.GatewaySenderEventDispatcher;
import org.apache.geode.internal.cache.wan.GatewaySenderEventImpl;
import org.apache.geode.internal.cache.wan.InternalGatewaySender;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.logging.internal.executors.LoggingExecutors;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.cli.CliFunction;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.i18n.CliStrings;

/**
 * Class for copying via WAN the contents of a region
 * It must be executed in all members of the Geode cluster that host the region
 * to be copied. (called with onServers() or withMembers() passing the list
 * of all members hosting the region).
 * It also offers the possibility to cancel an ongoing execution of this function.
 * The copying itself is executed in a new thread with a known name
 * (parameterized with the regionName and senderId) in order to allow
 * to cancel ongoing invocations by interrupting that thread.
 *
 * It accepts the following arguments in an array of objects
 * 0: regionName (String)
 * 1: senderId (String)
 * 2: isCancel (Boolean): If true, it indicates that an ongoing execution of this
 * function for the given region and senderId must be stopped. Otherwise,
 * it indicates that the region must be copied.
 * 3: maxRate (Long) maximum copy rate in entries per second. In the case of
 * parallel gateway senders, the maxRate is per server hosting the region.
 * 4: batchSize (Integer): the size of the batches. Region entries are copied in batches of the
 * passed size. After each batch is sent, the function checks if the command
 * must be canceled and also sleeps for some time if necessary to adjust the
 * copy rate to the one passed as argument.
 */
public class WanCopyRegionFunction extends CliFunction<Object[]> implements Declarable {
  private static final Logger logger = LogService.getLogger();
  private static final long serialVersionUID = 1L;

  public static final String ID = WanCopyRegionFunction.class.getName();

  private static final int MAX_BATCH_SEND_RETRIES = 1;

  private static final int THREAD_POOL_SIZE = 10;

  private final Clock clock;
  private final ThreadSleeper threadSleeper;

  private static final ExecutorService executor = LoggingExecutors
      .newFixedThreadPool(THREAD_POOL_SIZE, "wanCopyRegionFunctionThread_", true);

  /**
   * Contains the ongoing executions of this function
   */
  private static final Map<String, Future> executions = new ConcurrentHashMap<>();

  private volatile int batchId = 0;

  public WanCopyRegionFunction() {
    this(Clock.systemDefaultZone(), new ThreadSleeper());
  }

  @VisibleForTesting
  WanCopyRegionFunction(Clock clock, ThreadSleeper threadSleeper) {
    this.clock = clock;
    this.threadSleeper = threadSleeper;
  }

  @Override
  public String getId() {
    return ID;
  }

  @Override
  public boolean hasResult() {
    return true;
  }

  @Override
  public boolean isHA() {
    return false;
  }

  @Override
  public CliFunctionResult executeFunction(FunctionContext<Object[]> context) {
    final Object[] args = context.getArguments();
    if (args.length < 5) {
      throw new IllegalStateException(
          "Arguments length does not match required length.");
    }
    final String regionName = (String) args[0];
    final String senderId = (String) args[1];
    final boolean isCancel = (Boolean) args[2];
    long maxRate = (Long) args[3];
    int batchSize = (Integer) args[4];

    if (regionName.endsWith("*") && senderId.equals("*") && isCancel) {
      return cancelAllWanCopyRegion(context);
    }

    if (isCancel) {
      return cancelWanCopyRegion(context, regionName, senderId);
    }

    final InternalCache cache = (InternalCache) context.getCache();

    final Region region = cache.getRegion(regionName);
    if (region == null) {
      return new CliFunctionResult(context.getMemberName(), CliFunctionResult.StatusState.ERROR,
          CliStrings.format(CliStrings.WAN_COPY_REGION__MSG__REGION__NOT__FOUND, regionName));
    }

    GatewaySender sender = cache.getGatewaySender(senderId);
    if (sender == null) {
      return new CliFunctionResult(context.getMemberName(), CliFunctionResult.StatusState.ERROR,
          CliStrings.format(CliStrings.WAN_COPY_REGION__MSG__SENDER__NOT__FOUND, senderId));
    }

    if (!region.getAttributes().getGatewaySenderIds().contains(sender.getId())) {
      return new CliFunctionResult(context.getMemberName(), CliFunctionResult.StatusState.ERROR,
          CliStrings.format(CliStrings.WAN_COPY_REGION__MSG__REGION__NOT__USING_SENDER, regionName,
              senderId));
    }

    if (!sender.isRunning()) {
      return new CliFunctionResult(context.getMemberName(), CliFunctionResult.StatusState.ERROR,
          CliStrings.format(CliStrings.WAN_COPY_REGION__MSG__SENDER__NOT__RUNNING, senderId));
    }

    if (!sender.isParallel() && !((InternalGatewaySender) sender).isPrimary()) {
      return new CliFunctionResult(context.getMemberName(), CliFunctionResult.StatusState.OK,
          CliStrings.format(CliStrings.WAN_COPY_REGION__MSG__SENDER__SERIAL__AND__NOT__PRIMARY,
              senderId));
    }

    try {
      return executeWanCopyRegionFunctionInNewThread(context, region, regionName, sender, maxRate,
          batchSize);
    } catch (InterruptedException | CancellationException e) {
      return new CliFunctionResult(context.getMemberName(), CliFunctionResult.StatusState.ERROR,
          CliStrings.WAN_COPY_REGION__MSG__CANCELED__BEFORE__HAVING__COPIED);
    } catch (ExecutionException e) {
      return new CliFunctionResult(context.getMemberName(), CliFunctionResult.StatusState.ERROR,
          CliStrings.format(CliStrings.WAN_COPY_REGION__MSG__EXECUTION__FAILED, e.getMessage()));
    }
  }

  private CliFunctionResult executeWanCopyRegionFunctionInNewThread(
      FunctionContext<Object[]> context,
      Region region, String regionName, GatewaySender sender, long maxRate, int batchSize)
      throws InterruptedException, ExecutionException, CancellationException {
    String executionName = getExecutionName(regionName, sender.getId());
    Callable<CliFunctionResult> callable =
        new wanCopyRegionCallable(context, region, sender, maxRate, batchSize);
    FutureTask<CliFunctionResult> future = new FutureTask<>(callable);

    if (executions.putIfAbsent(executionName, future) != null) {
      return new CliFunctionResult(context.getMemberName(), CliFunctionResult.StatusState.ERROR,
          CliStrings.format(CliStrings.WAN_COPY_REGION__MSG__ALREADY__RUNNING__COMMAND,
              regionName, sender.getId()));
    }

    try {
      executor.execute(future);
      return future.get();
    } finally {
      executions.remove(executionName);
    }
  }

  class wanCopyRegionCallable implements Callable<CliFunctionResult> {
    private final FunctionContext<Object[]> context;
    private final Region region;
    private final GatewaySender sender;
    private final long maxRate;
    private final int batchSize;

    public wanCopyRegionCallable(final FunctionContext<Object[]> context, final Region region,
        final GatewaySender sender, final long maxRate,
        final int batchSize) {
      this.context = context;
      this.region = region;
      this.sender = sender;
      this.maxRate = maxRate;
      this.batchSize = batchSize;
    }

    @Override
    public CliFunctionResult call() throws Exception {
      return wanCopyRegion(context, region, sender, maxRate, batchSize);
    }
  }

  @VisibleForTesting
  CliFunctionResult wanCopyRegion(FunctionContext<Object[]> context, Region region,
      GatewaySender sender, long maxRate, int batchSize) throws InterruptedException {
    ConnectionState connectionState = new ConnectionState();
    int copiedEntries = 0;
    final InternalCache cache = (InternalCache) context.getCache();
    Iterator<?> entriesIter = getEntries(region, sender).iterator();
    final long startTime = clock.millis();

    try {
      while (entriesIter.hasNext()) {
        List<GatewayQueueEvent> batch =
            createBatch((InternalRegion) region, sender, batchSize, cache, entriesIter);
        if (batch.size() == 0) {
          continue;
        }
        Optional<CliFunctionResult> connectionError =
            connectionState.connectIfNeeded(context, sender);
        if (connectionError.isPresent()) {
          return connectionError.get();
        }
        Optional<CliFunctionResult> error =
            sendBatch(context, sender, batch, connectionState, copiedEntries);
        if (error.isPresent()) {
          return error.get();
        }
        copiedEntries += batch.size();
        doPostSendBatchActions(startTime, copiedEntries, maxRate);
      }
    } finally {
      connectionState.close();
    }

    if (region.isDestroyed()) {
      return new CliFunctionResult(context.getMemberName(), CliFunctionResult.StatusState.ERROR,
          CliStrings.format(
              CliStrings.WAN_COPY_REGION__MSG__ERROR__AFTER__HAVING__COPIED,
              "Region destroyed",
              copiedEntries));
    }

    return new CliFunctionResult(context.getMemberName(), CliFunctionResult.StatusState.OK,
        CliStrings.format(CliStrings.WAN_COPY_REGION__MSG__COPIED__ENTRIES,
            copiedEntries));
  }

  private Optional<CliFunctionResult> sendBatch(FunctionContext<Object[]> context,
      GatewaySender sender, List<GatewayQueueEvent> batch,
      ConnectionState connectionState, int copiedEntries) {
    GatewaySenderEventDispatcher dispatcher =
        ((AbstractGatewaySender) sender).getEventProcessor().getDispatcher();
    int retries = 0;

    while (true) {
      try {
        dispatcher.sendBatch(batch, connectionState.getConnection(),
            connectionState.getSenderPool(), getAndIncrementBatchId(), true);
        return Optional.empty();
      } catch (BatchException70 e) {
        return Optional.of(new CliFunctionResult(context.getMemberName(),
            CliFunctionResult.StatusState.ERROR,
            CliStrings.format(
                CliStrings.WAN_COPY_REGION__MSG__ERROR__AFTER__HAVING__COPIED,
                e.getExceptions().get(0).getCause(), copiedEntries)));
      } catch (ConnectionDestroyedException | ServerConnectivityException e) {
        Optional<CliFunctionResult> error =
            connectionState.reconnect(context, retries++, copiedEntries, e);
        if (error.isPresent()) {
          return error;
        }
      }
    }
  }

  List<GatewayQueueEvent> createBatch(InternalRegion region, GatewaySender sender,
      int batchSize, InternalCache cache, Iterator<?> iter) {
    int batchIndex = 0;
    List<GatewayQueueEvent> batch = new ArrayList<>();

    while (iter.hasNext() && batchIndex < batchSize) {
      GatewayQueueEvent event =
          createGatewaySenderEvent(cache, region, sender, (Region.Entry) iter.next());
      if (event != null) {
        batch.add(event);
        batchIndex++;
      }
    }
    return batch;
  }

  Set<?> getEntries(Region region, GatewaySender sender) {
    if (region instanceof PartitionedRegion && sender.isParallel()) {
      return ((PartitionedRegion) region).getDataStore().getAllLocalBucketRegions()
          .stream()
          .flatMap(br -> ((Set<?>) br.entrySet()).stream()).collect(Collectors.toSet());
    }
    return region.entrySet();
  }

  @VisibleForTesting
  GatewayQueueEvent createGatewaySenderEvent(InternalCache cache,
      InternalRegion region, GatewaySender sender, Region.Entry entry) {
    final EntryEventImpl event;
    if (region instanceof PartitionedRegion) {
      event = createEventForPartitionedRegion(sender, cache, region, entry);
    } else {
      event = createEventForReplicatedRegion(cache, region, entry);
    }
    if (event == null) {
      return null;
    }
    try {
      return new GatewaySenderEventImpl(EnumListenerEvent.AFTER_UPDATE_WITH_GENERATE_CALLBACKS,
          event, null, true);
    } catch (IOException e) {
      logger.error("Error when creating event in wan-copy: {}", e);
      return null;
    }
  }

  final CliFunctionResult cancelWanCopyRegion(FunctionContext<Object[]> context,
      String regionName, String senderId) {
    Future execution = executions.remove(getExecutionName(regionName, senderId));
    if (execution == null) {
      return new CliFunctionResult(context.getMemberName(), CliFunctionResult.StatusState.ERROR,
          CliStrings.format(CliStrings.WAN_COPY_REGION__MSG__NO__RUNNING__COMMAND,
              regionName, senderId));
    }
    execution.cancel(true);
    return new CliFunctionResult(context.getMemberName(), CliFunctionResult.StatusState.OK,
        CliStrings.WAN_COPY_REGION__MSG__EXECUTION__CANCELED);
  }

  final CliFunctionResult cancelAllWanCopyRegion(FunctionContext<Object[]> context) {
    String executionsString = executions.keySet().toString();
    for (Future execution : executions.values()) {
      execution.cancel(true);
    }
    executions.clear();
    return new CliFunctionResult(context.getMemberName(), CliFunctionResult.StatusState.OK,
        CliStrings.format(CliStrings.WAN_COPY_REGION__MSG__EXECUTIONS__CANCELED, executionsString));
  }

  public static String getExecutionName(String regionName, String senderId) {
    return "(" + regionName + "," + senderId + ")";
  }

  /**
   * It runs the actions to be done after a batch has been
   * sent: throw an interrupted exception if the operation was canceled and
   * adjust the rate of copying by sleeping if necessary.
   *
   * @param startTime time at which the entries started to be copied
   * @param copiedEntries number of entries copied so far
   * @param maxRate maximum copying rate
   */
  void doPostSendBatchActions(long startTime, int copiedEntries, long maxRate)
      throws InterruptedException {
    long sleepMs = getTimeToSleep(startTime, copiedEntries, maxRate);
    if (sleepMs > 0) {
      logger.info("{}: Sleeping for {} ms to accommodate to requested maxRate",
          this.getClass().getSimpleName(), sleepMs);
      threadSleeper.millis(sleepMs);
    } else {
      if (Thread.currentThread().isInterrupted()) {
        throw new InterruptedException();
      }
    }
  }

  private int getAndIncrementBatchId() {
    if (batchId + 1 == Integer.MAX_VALUE) {
      batchId = 0;
    }
    return batchId++;
  }

  @VisibleForTesting
  long getTimeToSleep(long startTime, int copiedEntries, long maxRate) {
    if (maxRate == 0) {
      return 0;
    }
    final long elapsedMs = clock.millis() - startTime;
    if (elapsedMs != 0 && (copiedEntries * 1000.0) / (double) elapsedMs <= maxRate) {
      return 0;
    }
    final long targetElapsedMs = (copiedEntries * 1000L) / maxRate;
    return targetElapsedMs - elapsedMs;
  }

  private EntryEventImpl createEventForReplicatedRegion(InternalCache cache, InternalRegion region,
      Region.Entry entry) {
    return createEvent(cache, region, entry);
  }

  private EntryEventImpl createEventForPartitionedRegion(GatewaySender sender, InternalCache cache,
      InternalRegion region,
      Region.Entry entry) {
    EntryEventImpl event = createEvent(cache, region, entry);
    if (event == null) {
      return null;
    }
    BucketRegion bucketRegion = ((PartitionedRegion) event.getRegion()).getDataStore()
        .getLocalBucketById(event.getKeyInfo().getBucketId());
    if (bucketRegion != null && !bucketRegion.getBucketAdvisor().isPrimary()
        && sender.isParallel()) {
      return null;
    }
    if (bucketRegion != null) {
      bucketRegion.handleWANEvent(event);
    }
    return event;
  }

  private EntryEventImpl createEvent(InternalCache cache, InternalRegion region,
      Region.Entry entry) {
    EntryEventImpl event;
    try {
      event = new DefaultEntryEventFactory().create(region, Operation.UPDATE,
          entry.getKey(),
          entry.getValue(), null, false,
          (cache).getInternalDistributedSystem().getDistributedMember(), false);
    } catch (EntryDestroyedException e) {
      return null;
    }
    if (entry instanceof NonTXEntry) {
      event.setVersionTag(((NonTXEntry) entry).getRegionEntry().getVersionStamp().asVersionTag());
    } else {
      event.setVersionTag(((EntrySnapshot) entry).getVersionTag());
    }
    event.setNewEventId(cache.getInternalDistributedSystem());
    return event;
  }

  static class ConnectionState {
    private volatile Connection connection = null;
    private volatile PoolImpl senderPool = null;

    public Connection getConnection() {
      return connection;
    }

    public PoolImpl getSenderPool() {
      return senderPool;
    }

    public Optional<CliFunctionResult> connectIfNeeded(FunctionContext<Object[]> context,
        GatewaySender sender) {
      if (senderPool == null) {
        senderPool = ((AbstractGatewaySender) sender).getProxy();
        if (senderPool == null) {
          return Optional.of(new CliFunctionResult(context.getMemberName(),
              CliFunctionResult.StatusState.ERROR,
              CliStrings.WAN_COPY_REGION__MSG__NO__CONNECTION__POOL));
        }
        connection = senderPool.acquireConnection();
        if (connection.getWanSiteVersion() < KnownVersion.GEODE_1_15_0.ordinal()) {
          return Optional.of(new CliFunctionResult(context.getMemberName(),
              CliFunctionResult.StatusState.ERROR,
              CliStrings.WAN_COPY_REGION__MSG__COMMAND__NOT__SUPPORTED__AT__REMOTE__SITE));
        }
      }
      return Optional.empty();
    }

    public Optional<CliFunctionResult> reconnect(FunctionContext<Object[]> context, int retries,
        int copiedEntries, Exception e) {
      close();
      if (retries++ >= MAX_BATCH_SEND_RETRIES) {
        return Optional.of(new CliFunctionResult(context.getMemberName(),
            CliFunctionResult.StatusState.ERROR,
            CliStrings.format(
                CliStrings.WAN_COPY_REGION__MSG__ERROR__AFTER__HAVING__COPIED,
                "Connection error", copiedEntries)));
      }
      logger.error("Exception {} in sendBatch. Retrying", e.getClass().getName());
      try {
        connection = senderPool.acquireConnection();
      } catch (NoAvailableServersException | AllConnectionsInUseException e1) {
        return Optional.of(new CliFunctionResult(context.getMemberName(),
            CliFunctionResult.StatusState.ERROR,
            CliStrings.format(
                CliStrings.WAN_COPY_REGION__MSG__NO__CONNECTION,
                copiedEntries)));
      }
      return Optional.empty();
    }

    void close() {
      if (senderPool != null && connection != null) {
        try {
          connection.close(false);
        } catch (Exception e) {
          logger.error("Error closing the connection used to wan-copy region entries");
        }
        senderPool.returnConnection(connection);
      }
      connection = null;
    }
  }

  static class ThreadSleeper implements Serializable {
    void millis(long millis) throws InterruptedException {
      Thread.sleep(millis);
    }
  }
}
