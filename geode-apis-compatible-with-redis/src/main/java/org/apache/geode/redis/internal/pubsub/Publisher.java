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
package org.apache.geode.redis.internal.pubsub;

import static org.apache.geode.logging.internal.executors.LoggingExecutors.newCachedThreadPool;
import static org.apache.geode.redis.internal.netty.Coder.bytesToString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.redis.internal.RegionProvider;
import org.apache.geode.redis.internal.netty.Client;

/**
 * This class deals with doing a redis pubsub publish operation.
 * Since publish requests from the same client must have order
 * preserved, and since publish requests myst be done in a thread
 * other than the one the request arrived from, this class has an
 * instance of ClientPublisher for any Client that does a publish.
 * The ClientPublisher maintains an ordered queue of requests
 * and is careful to process them in the order they arrived.
 * It supports batching them which can be a significant win as
 * soon as a second geode server is added to the cluster.
 * WARNING: The queue on the ClientPublisher is unbounded
 * which can cause the server to run out of memory if the
 * client keeps sending publish requests faster than the server
 * can process them. It would be nice for this queue to have
 * a bound but that could cause a hang because of the way
 * we use Netty.
 */
public class Publisher {
  private static final Logger logger = LogService.getLogger();

  private final ExecutorService executor;
  private final RegionProvider regionProvider;
  private final Subscriptions subscriptions;
  private final Map<Client, ClientPublisher> clientPublishers = new ConcurrentHashMap<>();

  public Publisher(RegionProvider regionProvider, Subscriptions subscriptions) {
    this.executor = createExecutorService();
    this.regionProvider = regionProvider;
    this.subscriptions = subscriptions;
    registerPublishFunction();
  }

  @VisibleForTesting
  Publisher(RegionProvider regionProvider, Subscriptions subscriptions, ExecutorService executor) {
    this.executor = executor;
    this.regionProvider = regionProvider;
    this.subscriptions = subscriptions;
    // no need to register function in unit tests
  }

  public void publish(Client client, byte[] channel, byte[] message) {
    ClientPublisher clientPublisher =
        clientPublishers.computeIfAbsent(client, key -> new ClientPublisher());
    clientPublisher.publish(channel, message);
  }

  public void disconnect(Client client) {
    clientPublishers.remove(client);
  }

  public void close() {
    executor.shutdown();
    try {
      if (!executor.awaitTermination(1, TimeUnit.MINUTES)) {
        logger.warn("Timed out waiting for queued publish requests to be sent.");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @VisibleForTesting
  int getClientCount() {
    return clientPublishers.size();
  }


  private static ExecutorService createExecutorService() {
    return newCachedThreadPool("GeodeRedisServer-Publish-", true);
  }

  private void registerPublishFunction() {
    FunctionService.registerFunction(new PublishFunction(this));
  }

  @SuppressWarnings("unchecked")
  private void internalPublish(List<PublishRequest> batch) {
    Set<DistributedMember> remoteMembers = regionProvider.getRemoteRegionMembers();
    try {
      ResultCollector<?, ?> resultCollector = null;
      try {
        if (!remoteMembers.isEmpty()) {
          // send function to remotes
          resultCollector = FunctionService
              .onMembers(remoteMembers)
              .setArguments(batch)
              .execute(PublishFunction.ID);
        }
      } finally {
        // execute it locally
        publishBatchToLocalSubscribers(batch);
        if (resultCollector != null) {
          // block until remote execute completes
          resultCollector.getResult();
        }
      }
    } catch (Exception e) {
      // the onMembers contract is for execute to throw an exception
      // if one of the members goes down.
      if (logger.isDebugEnabled()) {
        logger.debug(
            "Exception executing publish function on batch {}. If a server departed during the publish then an exception is expected.",
            batch, e);
      }
    }
  }

  private void publishBatchToLocalSubscribers(List<PublishRequest> batch) {
    batch.forEach(request -> subscriptions.forEachSubscription(request.getChannel(),
        (subscriptionName, channelToMatch, client, subscription) -> subscription.publishMessage(
            channelToMatch != null, subscriptionName,
            client, request.getChannel(), request.getMessage())));
  }

  public static class PublishRequest implements DataSerializableFixedID {
    // note final can not be used because of DataSerializableFixedID
    private byte[] channel;
    private byte[] message;

    public PublishRequest(byte[] channel, byte[] message) {
      this.channel = channel;
      this.message = message;
    }

    @SuppressWarnings("unused")
    public PublishRequest() {
      // needed for DataSerializableFixedID
    }

    public byte[] getChannel() {
      return channel;
    }

    public byte[] getMessage() {
      return message;
    }

    @Override
    public int getDSFID() {
      return PUBLISH_REQUEST;
    }

    @Override
    public void toData(DataOutput out, SerializationContext context) throws IOException {
      DataSerializer.writeByteArray(getChannel(), out);
      DataSerializer.writeByteArray(getMessage(), out);
    }

    @Override
    public void fromData(DataInput in, DeserializationContext context)
        throws IOException {
      channel = DataSerializer.readByteArray(in);
      message = DataSerializer.readByteArray(in);
    }

    @Override
    public KnownVersion[] getSerializationVersions() {
      return null;
    }

    @Override
    public String toString() {
      return "PublishRequest{" +
          "channel=" + bytesToString(channel) +
          ", message=" + bytesToString(message) +
          '}';
    }
  }

  /**
   * Manages all the publish requests from a particular client.
   * The order of these requests is maintained, and they will be
   * delivered to subscribers in that order.
   * Since the delivery to subscribers can be slower than the rate
   * at which publish requests arrive, this publisher supports
   * batching to allow multiple requests to be sent with a single
   * function call.
   */
  private class ClientPublisher {
    /**
     * The queue of incoming publish requests from a particular client.
     */
    private final BlockingQueue<PublishRequest> requests = new LinkedBlockingQueue<>();
    private static final int MAX_BATCH_SIZE = 256;
    /**
     * Requests are removed from the "requests" queue and put into this batch.
     * The batch will contain up to MAX_BATCH_SIZE requests but could contain
     * any number less than that. All the requests added to this batch will be
     * sent to subscribers before an attempt is made to fill it again.
     */
    private final List<PublishRequest> batch = new ArrayList<>(MAX_BATCH_SIZE);
    /**
     * True if an executor thread is currently working on our queue.
     * Any use of this field must be protected by synchronization.
     * Note that currently only a single executor thread will be working on
     * a given ClientPublisher.
     */
    private boolean active;

    public void publish(byte[] channel, byte[] message) {
      // Only one thread for a given Client will call this method
      // and this is the only place that adds to the queue.
      // But one of the executor threads can be concurrently
      // removing items from the queue.
      requests.add(new PublishRequest(channel, message));
      fillBatchIfNeeded();
    }

    private synchronized void fillBatchIfNeeded() {
      // if active then the executor is already working on our batch
      if (!active) {
        // This should only happen when our queue is empty,
        // and we add a request to it.
        fillBatch();
      }
    }

    private synchronized void fillBatch() {
      batch.clear();
      if (requests.drainTo(batch, MAX_BATCH_SIZE) > 0) {
        active = true;
        publishBatch();
      } else {
        active = false;
      }
    }

    private void publishBatch() {
      executor.execute(() -> {
        try {
          internalPublish(batch);
        } finally {
          fillBatch();
        }
      });
    }
  }

  private static class PublishFunction implements InternalFunction<List<PublishRequest>> {
    public static final String ID = "redisPubSubFunctionID";
    /**
     * this class is never serialized (since it implemented getId)
     * but make tools happy by setting its serialVersionUID.
     */
    private static final long serialVersionUID = -1L;

    private final Publisher publisher;

    public PublishFunction(Publisher publisher) {
      this.publisher = publisher;
    }

    @Override
    public String getId() {
      return ID;
    }

    @Override
    public void execute(FunctionContext<List<PublishRequest>> context) {
      publisher.publishBatchToLocalSubscribers(context.getArguments());
      context.getResultSender().lastResult(true);
    }

    /**
     * Since the publish process uses an onMembers function call, we don't want to re-publish
     * to members if one fails.
     * TODO: Revisit this in the event that we instead use an onMember call against individual
     * members.
     */
    @Override
    public boolean isHA() {
      return false;
    }

    @Override
    public boolean hasResult() {
      return true; // this is needed to preserve ordering
    }
  }
}
