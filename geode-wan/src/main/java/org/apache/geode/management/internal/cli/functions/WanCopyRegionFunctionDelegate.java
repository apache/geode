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

import static org.apache.geode.cache.wan.internal.cli.commands.WanCopyRegionCommand.WAN_COPY_REGION__MSG__COMMAND__NOT__SUPPORTED__AT__REMOTE__SITE;
import static org.apache.geode.cache.wan.internal.cli.commands.WanCopyRegionCommand.WAN_COPY_REGION__MSG__ERROR__AFTER__HAVING__COPIED;
import static org.apache.geode.cache.wan.internal.cli.commands.WanCopyRegionCommand.WAN_COPY_REGION__MSG__NO__CONNECTION;
import static org.apache.geode.cache.wan.internal.cli.commands.WanCopyRegionCommand.WAN_COPY_REGION__MSG__NO__CONNECTION__POOL;

import java.io.IOException;
import java.io.Serializable;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.EntryDestroyedException;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.AllConnectionsInUseException;
import org.apache.geode.cache.client.NoAvailableServersException;
import org.apache.geode.cache.client.ServerConnectivityException;
import org.apache.geode.cache.client.internal.Connection;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.client.internal.pooling.ConnectionDestroyedException;
import org.apache.geode.cache.wan.GatewayQueueEvent;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.cache.wan.internal.cli.commands.WanCopyRegionCommand;
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
import org.apache.geode.internal.cache.wan.GatewaySenderEventImpl.TransactionMetadataDisposition;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.i18n.CliStrings;

public class WanCopyRegionFunctionDelegate implements Serializable {
  private static final int MAX_BATCH_SEND_RETRIES = 1;

  private int batchId = 0;
  private final Clock clock;
  private final ThreadSleeper threadSleeper;
  private final EventCreator eventCreator;

  private static final Logger logger = LogService.getLogger();

  public WanCopyRegionFunctionDelegate() {
    this(Clock.systemDefaultZone(), new ThreadSleeperImpl(), new EventCreatorImpl());
  }

  public WanCopyRegionFunctionDelegate(Clock clock, ThreadSleeper threadSleeper,
      EventCreator eventCreator) {
    this.clock = clock;
    this.threadSleeper = threadSleeper;
    this.eventCreator = eventCreator;
  }

  public CliFunctionResult wanCopyRegion(InternalCache cache, String memberName,
      Region<?, ?> region,
      GatewaySender sender, long maxRate, int batchSize) throws InterruptedException {
    ConnectionState connectionState = new ConnectionState();
    int copiedEntries = 0;
    Iterator<?> entriesIter = getEntries(region, sender).iterator();
    final long startTime = clock.millis();

    try {
      while (entriesIter.hasNext()) {
        List<GatewayQueueEvent<?, ?>> batch =
            createBatch((InternalRegion) region, sender, batchSize, cache, entriesIter);
        if (batch.size() == 0) {
          continue;
        }
        Optional<CliFunctionResult> connectionError =
            connectionState.connectIfNeeded(memberName, sender);
        if (connectionError.isPresent()) {
          return connectionError.get();
        }
        Optional<CliFunctionResult> error =
            sendBatch(memberName, sender, batch, connectionState, copiedEntries);
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
      return new CliFunctionResult(memberName, CliFunctionResult.StatusState.ERROR,
          CliStrings.format(
              WanCopyRegionCommand.WAN_COPY_REGION__MSG__ERROR__AFTER__HAVING__COPIED,
              "Region destroyed",
              copiedEntries));
    }

    return new CliFunctionResult(memberName, CliFunctionResult.StatusState.OK,
        CliStrings.format(WanCopyRegionCommand.WAN_COPY_REGION__MSG__COPIED__ENTRIES,
            copiedEntries));
  }

  private Optional<CliFunctionResult> sendBatch(String memberName,
      GatewaySender sender, List<GatewayQueueEvent<?, ?>> batch,
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
        return Optional.of(new CliFunctionResult(memberName,
            CliFunctionResult.StatusState.ERROR,
            CliStrings.format(
                WAN_COPY_REGION__MSG__ERROR__AFTER__HAVING__COPIED,
                e.getExceptions().get(0).getCause(), copiedEntries)));
      } catch (ConnectionDestroyedException | ServerConnectivityException e) {
        Optional<CliFunctionResult> error =
            connectionState.reconnect(memberName, retries++, copiedEntries, e);
        if (error.isPresent()) {
          return error;
        }
      }
    }
  }

  private List<GatewayQueueEvent<?, ?>> createBatch(InternalRegion region, GatewaySender sender,
      int batchSize, InternalCache cache, Iterator<?> iter) {
    int batchIndex = 0;
    List<GatewayQueueEvent<?, ?>> batch = new ArrayList<>();

    while (iter.hasNext() && batchIndex < batchSize) {
      GatewayQueueEvent<?, ?> event =
          eventCreator.createGatewaySenderEvent(cache, region, sender,
              (Region.Entry<?, ?>) iter.next());
      if (event != null) {
        batch.add(event);
        batchIndex++;
      }
    }
    return batch;
  }

  private Set<?> getEntries(Region<?, ?> region, GatewaySender sender) {
    if (region instanceof PartitionedRegion && sender.isParallel()) {
      return ((PartitionedRegion) region).getDataStore().getAllLocalBucketRegions()
          .stream()
          .flatMap(br -> ((Set<?>) br.entrySet()).stream()).collect(Collectors.toSet());
    }
    return region.entrySet();
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
  @VisibleForTesting
  void doPostSendBatchActions(long startTime, int copiedEntries, long maxRate)
      throws InterruptedException {
    long sleepMs = getTimeToSleep(startTime, copiedEntries, maxRate);
    if (sleepMs > 0) {
      logger.info("{}: Sleeping for {} ms to accommodate to requested maxRate",
          this.getClass().getSimpleName(), sleepMs);
      threadSleeper.sleep(sleepMs);
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


  static class ConnectionState {
    private volatile Connection connection = null;
    private volatile PoolImpl senderPool = null;

    public Connection getConnection() {
      return connection;
    }

    public PoolImpl getSenderPool() {
      return senderPool;
    }

    public Optional<CliFunctionResult> connectIfNeeded(String memberName,
        GatewaySender sender) {
      if (senderPool == null) {
        senderPool = ((AbstractGatewaySender) sender).getProxy();
        if (senderPool == null) {
          return Optional.of(new CliFunctionResult(memberName,
              CliFunctionResult.StatusState.ERROR,
              WAN_COPY_REGION__MSG__NO__CONNECTION__POOL));
        }
        connection = senderPool.acquireConnection();
        if (connection.getWanSiteVersion() < KnownVersion.GEODE_1_15_0.ordinal()) {
          return Optional.of(new CliFunctionResult(memberName,
              CliFunctionResult.StatusState.ERROR,
              WAN_COPY_REGION__MSG__COMMAND__NOT__SUPPORTED__AT__REMOTE__SITE));
        }
      }
      return Optional.empty();
    }

    public Optional<CliFunctionResult> reconnect(String memberName, int retries,
        int copiedEntries, Exception e) {
      close();
      if (retries >= MAX_BATCH_SEND_RETRIES) {
        return Optional.of(new CliFunctionResult(memberName,
            CliFunctionResult.StatusState.ERROR,
            CliStrings.format(
                WAN_COPY_REGION__MSG__ERROR__AFTER__HAVING__COPIED,
                "Connection error", copiedEntries)));
      }
      logger.error("Exception {} in sendBatch. Retrying", e.getClass().getName());
      try {
        connection = senderPool.acquireConnection();
      } catch (NoAvailableServersException | AllConnectionsInUseException e1) {
        return Optional.of(new CliFunctionResult(memberName,
            CliFunctionResult.StatusState.ERROR,
            CliStrings.format(
                WAN_COPY_REGION__MSG__NO__CONNECTION,
                copiedEntries)));
      }
      return Optional.empty();
    }

    public void close() {
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


  @FunctionalInterface
  interface ThreadSleeper extends Serializable {
    void sleep(long millis) throws InterruptedException;
  }

  static class ThreadSleeperImpl implements ThreadSleeper {
    @Override
    public void sleep(long millis) throws InterruptedException {
      Thread.sleep(millis);
    }
  }


  @FunctionalInterface
  interface EventCreator extends Serializable {
    GatewayQueueEvent<?, ?> createGatewaySenderEvent(InternalCache cache,
        InternalRegion region, GatewaySender sender, Region.Entry<?, ?> entry);
  }

  static class EventCreatorImpl implements EventCreator {
    @VisibleForTesting
    public GatewayQueueEvent<?, ?> createGatewaySenderEvent(InternalCache cache,
        InternalRegion region, GatewaySender sender, Region.Entry<?, ?> entry) {
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
            event, null, TransactionMetadataDisposition.EXCLUDE);
      } catch (IOException e) {
        logger.error("Error when creating event in wan-copy: {}", e.getMessage());
        return null;
      }
    }

    private EntryEventImpl createEventForReplicatedRegion(InternalCache cache,
        InternalRegion region,
        Region.Entry<?, ?> entry) {
      return createEvent(cache, region, entry);
    }

    private EntryEventImpl createEventForPartitionedRegion(GatewaySender sender,
        InternalCache cache,
        InternalRegion region,
        Region.Entry<?, ?> entry) {
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
        Region.Entry<?, ?> entry) {
      EntryEventImpl event;
      try {
        event = new DefaultEntryEventFactory().create(region, Operation.UPDATE,
            entry.getKey(),
            entry.getValue(), null, false,
            cache.getInternalDistributedSystem().getDistributedMember(), false);
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
  }
}
