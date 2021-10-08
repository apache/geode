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

import static java.util.Arrays.asList;
import static org.apache.geode.internal.lang.utils.JavaWorkarounds.computeIfAbsent;
import static org.apache.geode.logging.internal.executors.LoggingExecutors.newCachedThreadPool;
import static org.apache.geode.redis.internal.netty.Coder.bytesToString;
import static org.apache.geode.redis.internal.netty.Coder.getInternalErrorResponse;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bMESSAGE;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bPMESSAGE;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;
import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.redis.internal.RegionProvider;
import org.apache.geode.redis.internal.netty.Client;
import org.apache.geode.redis.internal.netty.Coder;
import org.apache.geode.redis.internal.netty.CoderException;
import org.apache.geode.redis.internal.pubsub.Subscriptions.PatternSubscriptions;

/**
 * This class deals with doing a redis pubsub publish operation.
 * Since publish requests from the same client must have order
 * preserved, and since publish requests must be done in a thread
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
        computeIfAbsent(clientPublishers, client, key -> new ClientPublisher());
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
  private void internalPublish(PublishRequestBatch batch) {
    Set<DistributedMember> remoteMembers = regionProvider.getRemoteRegionMembers();
    List<PublishRequest> optimizedBatch = batch.optimize();
    try {
      ResultCollector<?, ?> resultCollector = null;
      try {
        if (!remoteMembers.isEmpty()) {
          // send function to remotes
          resultCollector = FunctionService
              .onMembers(remoteMembers)
              .setArguments(optimizedBatch)
              .execute(PublishFunction.ID);
        }
      } finally {
        // execute it locally
        publishBatchToLocalSubscribers(optimizedBatch);
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
    batch.forEach(request -> {
      publishRequestToLocalChannelSubscribers(request);
      publishRequestToLocalPatternSubscribers(request);
    });
  }

  private static void writeArrayResponse(ByteBuf buffer, Object... items) {
    buffer.markWriterIndex();
    try {
      Coder.getArrayResponse(buffer, asList(items), true);
    } catch (CoderException e) {
      buffer.resetWriterIndex();
      getInternalErrorResponse(buffer, e.getMessage());
    }
  }


  private void publishRequestToLocalChannelSubscribers(PublishRequest request) {
    Collection<Subscription> channelSubscriptions =
        subscriptions.getChannelSubscriptions(request.getChannel());
    if (channelSubscriptions.isEmpty()) {
      return;
    }
    ByteBuf writeBuf = channelSubscriptions.iterator().next().getChannelWriteBuffer();
    for (byte[] message : request.getMessages()) {
      writeArrayResponse(writeBuf, bMESSAGE, request.getChannel(), message);
    }
    if (channelSubscriptions.size() == 1) {
      Subscription singleSubscription = channelSubscriptions.iterator().next();
      singleSubscription.writeBufferToChannel(writeBuf);
    } else {
      // This pattern of using retainedDuplicate and ReferenceCountUtil.release
      // came from io.netty.channel.group.DefaultChannelGroup
      for (Subscription subscription : channelSubscriptions) {
        subscription.writeBufferToChannel(writeBuf.retainedDuplicate());
      }
      ReferenceCountUtil.release(writeBuf);
    }
  }

  private void publishRequestToLocalPatternSubscribers(PublishRequest request) {
    List<PatternSubscriptions> patternSubscriptionList =
        subscriptions.getPatternSubscriptions(request.getChannel());
    for (PatternSubscriptions patternSubscriptions : patternSubscriptionList) {
      publishRequestToPatternSubscriptions(request, patternSubscriptions);
    }
  }

  private static void publishRequestToPatternSubscriptions(PublishRequest request,
      PatternSubscriptions patternSubscriptions) {
    ByteBuf writeBuf = patternSubscriptions.getFirst().getChannelWriteBuffer();
    for (byte[] message : request.getMessages()) {
      writeArrayResponse(writeBuf, bPMESSAGE, patternSubscriptions.getPattern(),
          request.getChannel(), message);
    }
    if (patternSubscriptions.size() == 1) {
      Subscription singleSubscription = patternSubscriptions.getFirst();
      singleSubscription.writeBufferToChannel(writeBuf);
    } else {
      // This pattern of using retainedDuplicate and ReferenceCountUtil.release
      // came from io.netty.channel.group.DefaultChannelGroup
      for (Subscription subscription : patternSubscriptions.getSubscriptions()) {
        subscription.writeBufferToChannel(writeBuf.retainedDuplicate());
      }
      ReferenceCountUtil.release(writeBuf);
    }
  }

  public static class PublishRequest implements DataSerializableFixedID {
    // note final can not be used because of DataSerializableFixedID
    private byte[] channel;
    private List<byte[]> messages;

    public PublishRequest(byte[] channel) {
      this.channel = channel;
      this.messages = new ArrayList<>();
    }

    @SuppressWarnings("unused")
    public PublishRequest() {
      // needed for DataSerializableFixedID
    }

    public void addMessage(byte[] message) {
      this.messages.add(message);
    }

    public byte[] getChannel() {
      return channel;
    }

    public void setChannel(byte[] channel) {
      this.channel = channel;
    }

    public List<byte[]> getMessages() {
      return messages;
    }

    public void clear() {
      channel = null;
      messages.clear();
    }

    @Override
    public int getDSFID() {
      return PUBLISH_REQUEST;
    }

    @Override
    public void toData(DataOutput out, SerializationContext context) throws IOException {
      DataSerializer.writeByteArray(getChannel(), out);
      InternalDataSerializer.writeArrayLength(messages.size(), out);
      for (byte[] message : messages) {
        DataSerializer.writeByteArray(message, out);
      }
    }

    @Override
    public void fromData(DataInput in, DeserializationContext context)
        throws IOException {
      channel = DataSerializer.readByteArray(in);
      int messageCount = InternalDataSerializer.readArrayLength(in);
      messages = new ArrayList<>(messageCount);
      for (int i = 0; i < messageCount; i++) {
        messages.add(DataSerializer.readByteArray(in));
      }
    }

    @Override
    public KnownVersion[] getSerializationVersions() {
      return null;
    }

    @Override
    public String toString() {
      return "PublishRequest{" +
          "channel=" + bytesToString(channel) +
          ", messages=" + messages +
          '}';
    }
  }

  public static class LocalPublishRequest {
    private final byte[] channel;
    private final byte[] message;

    public LocalPublishRequest(byte[] channel, byte[] message) {
      this.channel = channel;
      this.message = message;
    }

    public byte[] getChannel() {
      return channel;
    }

    public byte[] getMessage() {
      return message;
    }

    @Override
    public String toString() {
      return "LocalPublishRequest{" +
          "channel=" + bytesToString(channel) +
          ", message=" + bytesToString(message) +
          '}';
    }
  }

  /**
   * Requests are removed from the "requests" queue and put into this batch.
   * The batch will contain up to MAX_BATCH_SIZE requests but could contain
   * any number less than that. All the requests added to this batch will be
   * sent to subscribers before an attempt is made to fill it again.
   * Each ClientPublisher has its own instance of this class.
   */
  private static class PublishRequestBatch {
    private static final int MAX_BATCH_SIZE = 1024;
    private final List<LocalPublishRequest> batch = new ArrayList<>(MAX_BATCH_SIZE);
    private final List<PublishRequest> optimizedBatch = new ArrayList<>();
    private final PublishRequest firstPublishRequest = new PublishRequest(null);

    public boolean fill(BlockingQueue<LocalPublishRequest> requests) {
      batch.clear();
      return requests.drainTo(batch, MAX_BATCH_SIZE) > 0;
    }

    /**
     * consecutive LocalPublishRequests on same channel are conflated into a single PublishRequest
     */
    public List<PublishRequest> optimize() {
      optimizedBatch.clear();
      PublishRequest currentPublishRequest = null;
      for (LocalPublishRequest localPublishRequest : batch) {
        if (currentPublishRequest == null ||
            !Arrays.equals(currentPublishRequest.getChannel(), localPublishRequest.getChannel())) {
          if (optimizedBatch.isEmpty()) {
            currentPublishRequest = firstPublishRequest;
            firstPublishRequest.clear();
            currentPublishRequest.setChannel(localPublishRequest.getChannel());
          } else {
            currentPublishRequest = new PublishRequest(localPublishRequest.getChannel());
          }
          optimizedBatch.add(currentPublishRequest);
        }
        currentPublishRequest.addMessage(localPublishRequest.getMessage());
      }
      return optimizedBatch;
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
    private final BlockingQueue<LocalPublishRequest> requests = new LinkedBlockingQueue<>();

    private final PublishRequestBatch batch = new PublishRequestBatch();

    /**
     * True if an executor thread is currently working on our queue.
     * Any use of this field must be protected by synchronization.
     * Note that currently only a single executor thread will be working on
     * a given ClientPublisher.
     */
    private volatile boolean active;

    public void publish(byte[] channel, byte[] message) {
      // Only one thread for a given Client will call this method
      // and this is the only place that adds to the queue.
      // But one of the executor threads can be concurrently
      // removing items from the queue.
      requests.add(new LocalPublishRequest(channel, message));
      if (!active) {
        fillBatchIfNeeded();
      }
    }

    private synchronized void fillBatchIfNeeded() {
      // if active then the executor is already working on our batch
      if (!active) {
        // This should only happen when our queue is empty,
        // and we add a request to it.
        if (refillBatch()) {
          publishBatch();
        }
      }
    }

    /**
     * returns true if batch refilled; false if batch is empty
     */
    private synchronized boolean refillBatch() {
      active = batch.fill(requests);
      return active;
    }

    private void publishBatch() {
      executor.execute(() -> {
        do {
          internalPublish(batch);
        } while (refillBatch());
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
