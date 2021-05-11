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
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.client.internal.Connection;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.cache.wan.internal.GatewaySenderEventRemoteDispatcher;
import org.apache.geode.cache.wan.internal.client.locator.GatewaySenderBatchOp;
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
import org.apache.geode.internal.cache.wan.GatewaySenderEventCallbackArgument;
import org.apache.geode.internal.cache.wan.GatewaySenderEventImpl;
import org.apache.geode.internal.cache.wan.InternalGatewaySender;
import org.apache.geode.logging.internal.executors.LoggingExecutors;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.cli.CliFunction;
import org.apache.geode.management.internal.functions.CliFunctionResult;

/**
 * Class for WAN replicating the contents of a region
 * It must be executed in all members of the Geode cluster that host the region
 * to be replicated. (called with onServers() or withMembers passing the list
 * of all members hosting the region).
 * It also offers the possibility to cancel an ongoing execution of this function.
 * The replication itself is executed in a new thread with a known name
 * (parameterized with the regionName and senderId) in order to allow
 * to cancel ongoing invocations by interrupting that thread.
 *
 * It accepts the following arguments in an array of objects
 * 0: regionName (String)
 * 1: senderId (String)
 * 2: isCancel (Boolean): If true, it indicates that an ongoing execution of this
 * function for the given region and senderId must be stopped. Otherwise,
 * it indicates that the region must be replicated.
 * 3: maxRate (Long) maximum replication rate in entries per second.
 * 4: batchSize (Integer): the size of the batches. Region entries are replicated in batches of the
 * passed size. After each batch is sent, the function checks if the command
 * must be canceled and also sleeps for some time if necessary to adjust the
 * replication rate to the one passed as argument.
 */
public class ReplicateRegionFunction extends CliFunction<String[]> implements Declarable {
  private static final Logger logger = LogService.getLogger();
  private static final long serialVersionUID = 1L;

  public static final String ID = ReplicateRegionFunction.class.getName();

  private Clock clock = Clock.systemDefaultZone();
  private ThreadSleeper threadSleeper = new ThreadSleeper();

  static class ThreadSleeper implements Serializable {
    void millis(long millis) throws InterruptedException {
      Thread.sleep(millis);
    }
  }

  @VisibleForTesting
  public void setClock(Clock clock) {
    this.clock = clock;
  }

  @VisibleForTesting
  public void setThreadSleeper(ThreadSleeper ts) {
    this.threadSleeper = ts;
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
  public CliFunctionResult executeFunction(FunctionContext<String[]> context) {
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

    final InternalCache cache = (InternalCache) context.getCache();
    final Region region = cache.getRegion(regionName);
    if (isCancel) {
      return cancelReplicateRegion(context, region, senderId);
    }

    GatewaySender sender = cache.getGatewaySender(senderId);
    if (!sender.isRunning()) {
      return new CliFunctionResult(context.getMemberName(), CliFunctionResult.StatusState.ERROR,
          "Sender not running");
    }

    if (!sender.isParallel() && !((InternalGatewaySender) sender).isPrimary()) {
      return new CliFunctionResult(context.getMemberName(), CliFunctionResult.StatusState.OK,
          "Sender is serial and not primary. 0 entries replicated.");
    }

    try {
      return executeReplicateFunctionInNewThread(context, region, sender, maxRate, batchSize);
    } catch (InterruptedException e) {
      return new CliFunctionResult(context.getMemberName(), CliFunctionResult.StatusState.ERROR,
          "Execution canceled.");
    } catch (ExecutionException e) {
      return new CliFunctionResult(context.getMemberName(), CliFunctionResult.StatusState.ERROR,
          "Execution failed: " + e.getMessage());
    }
  }

  private CliFunctionResult executeReplicateFunctionInNewThread(FunctionContext<String[]> context,
      Region region, GatewaySender sender, long maxRate, int batchSize)
      throws InterruptedException, ExecutionException {
    Callable<CliFunctionResult> callable =
        new ReplicateRegionCallable(context, region, sender, maxRate, batchSize);
    FutureTask<CliFunctionResult> futureTask = new FutureTask<>(callable);
    ExecutorService executor = LoggingExecutors
        .newSingleThreadExecutor(getReplicateRegionFunctionThreadName(region.getName(),
            sender.getId()), true);
    executor.execute(futureTask);
    return futureTask.get();
  }

  class ReplicateRegionCallable implements Callable<CliFunctionResult> {
    private final FunctionContext<String[]> context;
    private final Region region;
    private final GatewaySender sender;
    private final long maxRate;
    private final int batchSize;

    public ReplicateRegionCallable(final FunctionContext<String[]> context, final Region region,
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
      return replicateRegion(context, region, sender, maxRate, batchSize);
    }
  }

  private CliFunctionResult replicateRegion(FunctionContext<String[]> context, Region region,
      GatewaySender sender, long maxRate, int batchSize) {
    final Set<?> entries;
    if (region instanceof PartitionedRegion && sender.isParallel()) {
      entries =
          ((PartitionedRegion) region).getDataStore().getAllLocalBucketRegions()
              .stream()
              .flatMap(br -> ((Set<?>) br.entrySet()).stream()).collect(Collectors.toSet());
    } else {
      entries = region.entrySet();
    }

    final InternalCache cache = (InternalCache) context.getCache();
    int replicatedEntries = 0;
    final long startTime = clock.millis();

    Iterator<?> iter = entries.iterator();

    while (iter.hasNext()) {
      int batchIndex = 0;
      List<GatewaySenderEventImpl> batch = new ArrayList<>();
      while (iter.hasNext() && batchIndex < batchSize) {
        GatewaySenderEventImpl event = createGatewaySenderEvent(cache, (InternalRegion) region,
            sender, (Region.Entry) iter.next());
        if (event != null) {
          batch.add(event);
          batchIndex++;
        }
      }
      try {
        sendBatch(sender,
            replicatedEntries, maxRate, startTime, batch);
        replicatedEntries += batch.size();
      } catch (InterruptedException e) {
        return new CliFunctionResult(context.getMemberName(), CliFunctionResult.StatusState.ERROR,
            "Operation canceled after having replicated " + replicatedEntries + " entries");
      } catch (Exception e) {
        return new CliFunctionResult(context.getMemberName(), CliFunctionResult.StatusState.ERROR,
            "Error in operation after having replicated " + replicatedEntries + " entries");
      }
    }

    return new CliFunctionResult(context.getMemberName(), CliFunctionResult.StatusState.OK,
        "Entries replicated: " + replicatedEntries);
  }

  private GatewaySenderEventImpl createGatewaySenderEvent(InternalCache cache,
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
      return new GatewaySenderEventImpl(EnumListenerEvent.AFTER_CREATE, event, null, true);
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }

  private void sendBatch(GatewaySender sender,
      int replicatedEntries, long maxRate, long startTime,
      List batch) throws InterruptedException, BatchException70 {
    PoolImpl senderPool = ((AbstractGatewaySender) sender).getProxy();
    Connection connection = senderPool.acquireConnection();
    int batchId = 1971;
    GatewaySenderBatchOp.executeOn(connection, senderPool, batch, batchId, false, false);
    GatewaySenderEventRemoteDispatcher.GatewayAck ack =
        (GatewaySenderEventRemoteDispatcher.GatewayAck) GatewaySenderBatchOp.executeOn(connection,
            senderPool);
    if (ack == null) {
      throw new BatchException70("Unknown error sending batch", null, 0, batchId);
    }
    if (ack.getBatchException() != null) {
      throw ack.getBatchException();
    }
    doActionsIfBatchReplicated(startTime, replicatedEntries + batch.size(), maxRate);
  }

  final CliFunctionResult cancelReplicateRegion(FunctionContext<String[]> context, Region region,
      String senderId) {
    String threadBaseName = getReplicateRegionFunctionThreadName(region.getName(), senderId);
    long threadId = 0;
    for (Thread t : Thread.getAllStackTraces().keySet()) {
      if (t.getName().startsWith(threadBaseName)) {
        t.interrupt();
        threadId = t.getId();
        break;
      }
    }
    if (threadId == 0) {
      return new CliFunctionResult(context.getMemberName(), CliFunctionResult.StatusState.ERROR,
          "No running command to be canceled for region " + region + " and sender: " + senderId);
    }
    return new CliFunctionResult(context.getMemberName(), CliFunctionResult.StatusState.OK,
        "Canceled command");
  }

  public static String getReplicateRegionFunctionThreadName(String regionName, String senderId) {
    return "replicateRegionFunctionThread_" + regionName + "_" + senderId;
  }

  /**
   * If a complete batch in the last cycle has not been replicated yet it returns false.
   * Otherwise, it returns true and runs the actions to be done when a batch has been
   * replicated: throw an interrupted exception if the operation was canceled and
   * adjust the rate of replication by sleeping if necessary.
   *
   * @param startTime time at which the entries started to be replicated
   * @param replicatedEntries number of entries replicated so far
   * @param maxRate maximum rate of replication
   */
  void doActionsIfBatchReplicated(long startTime, int replicatedEntries, long maxRate)
      throws InterruptedException {
    if (Thread.currentThread().isInterrupted()) {
      throw new InterruptedException();
    }
    if (maxRate == 0) {
      return;
    }
    final long currTime = clock.millis();
    final long elapsedMs = currTime - startTime;
    if (elapsedMs == 0 || replicatedEntries * 1000L / elapsedMs > maxRate) {
      final long targetElapsedMs = (replicatedEntries * 1000L) / maxRate;
      final long sleepMs = targetElapsedMs - elapsedMs;
      logger.info("Sleeping for {} ms", sleepMs);
      threadSleeper.millis(sleepMs);
    }
  }

  private EntryEventImpl createEventForReplicatedRegion(InternalCache cache, InternalRegion region,
      Region.Entry entry) {
    return createEvent(cache, region, entry);
  }

  private EntryEventImpl createEventForPartitionedRegion(GatewaySender sender, InternalCache cache,
      InternalRegion region,
      Region.Entry entry) {
    EntryEventImpl event = createEvent(cache, region, entry);
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
    EntryEventImpl event = new DefaultEntryEventFactory().create(region, Operation.UPDATE,
        entry.getKey(),
        entry.getValue(), null, false,
        (cache).getInternalDistributedSystem().getDistributedMember(), false);
    if (entry instanceof NonTXEntry) {
      event.setVersionTag(((NonTXEntry) entry).getRegionEntry().getVersionStamp().asVersionTag());
    } else {
      event.setVersionTag(((EntrySnapshot) entry).getVersionTag());
    }
    event.setNewEventId(cache.getInternalDistributedSystem());
    GatewaySenderEventCallbackArgument geCallbackArg = new GatewaySenderEventCallbackArgument(null,
        cache.getInternalDistributedSystem().getDistributionManager().getDistributedSystemId(),
        getRemoteDsIds(cache, region));
    event.setCallbackArgument(geCallbackArg);
    return event;
  }

  private List<Integer> getRemoteDsIds(Cache cache, Region region) {
    Set<String> senderIds = ((RegionAttributes<?, ?>) region.getAttributes()).getGatewaySenderIds();
    return senderIds.stream()
        .map(cache::getGatewaySender)
        .map(sender -> sender.getRemoteDSId())
        .collect(Collectors.toList());
  }
}
