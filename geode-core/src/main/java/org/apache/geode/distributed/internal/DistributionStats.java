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

import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;

import org.apache.logging.log4j.Logger;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.StatisticsType;
import org.apache.geode.StatisticsTypeFactory;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.internal.NanoTimer;
import org.apache.geode.internal.statistics.StatisticsTypeFactoryImpl;
import org.apache.geode.internal.util.Breadcrumbs;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * This class maintains statistics in GemFire about the distribution manager and distribution in
 * general.
 *
 *
 */
public class DistributionStats implements DMStats {
  private static final Logger logger = LogService.getLogger();

  @MakeNotStatic
  public static boolean enableClockStats = false;


  ////////////////// Statistic "Id" Fields //////////////////

  @Immutable
  private static final StatisticsType type;
  private static final int sentMessagesId;
  private static final int sentCommitMessagesId;
  private static final int commitWaitsId;
  @VisibleForTesting
  static final int sentMessagesTimeId;
  @VisibleForTesting
  static final int sentMessagesMaxTimeId;
  private static final int broadcastMessagesId;
  private static final int broadcastMessagesTimeId;
  private static final int receivedMessagesId;
  private static final int receivedBytesId;
  private static final int sentBytesId;
  private static final int processedMessagesId;
  private static final int processedMessagesTimeId;
  private static final int messageProcessingScheduleTimeId;
  private static final int messageChannelTimeId;
  private static final int udpDispatchRequestTimeId;
  private static final int replyMessageTimeId;
  private static final int distributeMessageTimeId;
  private static final int nodesId;
  private static final int overflowQueueSizeId;
  private static final int processingThreadsId;
  private static final int serialThreadsId;
  private static final int waitingThreadsId;
  private static final int highPriorityThreadsId;
  private static final int partitionedRegionThreadsId;
  private static final int functionExecutionThreadsId;
  private static final int partitionedRegionThreadJobsId;
  private static final int functionExecutionThreadJobsId;
  private static final int waitingQueueSizeId;
  private static final int overflowQueueThrottleTimeId;
  private static final int overflowQueueThrottleCountId;
  private static final int highPriorityQueueSizeId;
  private static final int highPriorityQueueThrottleTimeId;
  private static final int highPriorityQueueThrottleCountId;
  private static final int partitionedRegionQueueSizeId;
  private static final int partitionedRegionQueueThrottleTimeId;
  private static final int partitionedRegionQueueThrottleCountId;
  private static final int functionExecutionQueueSizeId;
  private static final int functionExecutionQueueThrottleCountId;
  private static final int functionExecutionQueueThrottleTimeId;
  private static final int serialQueueSizeId;
  @VisibleForTesting
  static final int serialQueueBytesId;
  private static final int serialPooledThreadId;
  private static final int serialQueueThrottleTimeId;
  private static final int serialQueueThrottleCountId;
  private static final int replyWaitsInProgressId;
  private static final int replyWaitsCompletedId;
  @VisibleForTesting
  static final int replyWaitTimeId;
  private static final int replyTimeoutsId;
  @VisibleForTesting
  static final int replyWaitMaxTimeId;
  private static final int receiverConnectionsId;
  private static final int failedAcceptsId;
  private static final int failedConnectsId;
  private static final int reconnectAttemptsId;
  private static final int lostConnectionLeaseId;
  private static final int sharedOrderedSenderConnectionsId;
  private static final int sharedUnorderedSenderConnectionsId;
  private static final int threadOrderedSenderConnectionsId;
  private static final int threadUnorderedSenderConnectionsId;
  static final int senderCreateTimeId;
  static final int senderCreatesInProgressId;

  private static final int syncSocketWritesInProgressId;
  private static final int syncSocketWriteTimeId;
  private static final int syncSocketWritesId;
  private static final int syncSocketWriteBytesId;

  private static final int ucastReadsId;
  private static final int ucastReadBytesId;
  private static final int ucastWritesId;
  private static final int ucastWriteBytesId;
  private static final int ucastRetransmitsId;

  private static final int mcastReadsId;
  private static final int mcastReadBytesId;
  private static final int mcastWritesId;
  private static final int mcastWriteBytesId;
  private static final int mcastRetransmitsId;
  private static final int mcastRetransmitRequestsId;

  private static final int serializationTimeId;
  private static final int serializationsId;
  private static final int serializedBytesId;

  private static final int pdxSerializationsId;
  private static final int pdxSerializedBytesId;

  private static final int deserializationTimeId;
  private static final int deserializationsId;
  private static final int deserializedBytesId;
  private static final int pdxDeserializationsId;
  private static final int pdxDeserializedBytesId;
  private static final int pdxInstanceDeserializationsId;
  private static final int pdxInstanceDeserializationTimeId;
  private static final int pdxInstanceCreationsId;

  private static final int msgSerializationTimeId;
  private static final int msgDeserializationTimeId;

  private static final int udpMsgEncryptionTimeId;
  private static final int udpMsgDecryptionTimeId;

  private static final int batchSendTimeId;
  private static final int batchCopyTimeId;
  private static final int batchWaitTimeId;
  private static final int batchFlushTimeId;

  private static final int threadOwnedReceiversId;
  private static final int threadOwnedReceiversId2;

  private static final int asyncSocketWritesInProgressId;
  private static final int asyncSocketWritesId;
  private static final int asyncSocketWriteRetriesId;
  private static final int asyncSocketWriteTimeId;
  private static final int asyncSocketWriteBytesId;

  private static final int socketLocksInProgressId;
  private static final int socketLocksId;
  private static final int socketLockTimeId;

  private static final int bufferAcquiresInProgressId;
  private static final int bufferAcquiresId;
  private static final int bufferAcquireTimeId;

  private static final int asyncQueueAddTimeId;
  private static final int asyncQueueRemoveTimeId;

  private static final int asyncQueuesId;
  private static final int asyncQueueFlushesInProgressId;
  private static final int asyncQueueFlushesCompletedId;
  private static final int asyncQueueFlushTimeId;
  private static final int asyncQueueTimeoutExceededId;
  private static final int asyncQueueSizeExceededId;
  private static final int asyncDistributionTimeoutExceededId;
  private static final int asyncQueueSizeId;
  private static final int asyncQueuedMsgsId;
  private static final int asyncDequeuedMsgsId;
  private static final int asyncConflatedMsgsId;

  private static final int asyncThreadsId;
  private static final int asyncThreadInProgressId;
  private static final int asyncThreadCompletedId;
  private static final int asyncThreadTimeId;

  private static final int receiverDirectBufferSizeId;
  private static final int receiverHeapBufferSizeId;
  private static final int senderDirectBufferSizeId;
  private static final int senderHeapBufferSizeId;

  private static final int messagesBeingReceivedId;
  private static final int messageBytesBeingReceivedId;

  private static final int serialThreadStartsId;
  private static final int processingThreadStartsId;
  private static final int highPriorityThreadStartsId;
  private static final int waitingThreadStartsId;
  private static final int partitionedRegionThreadStartsId;
  private static final int functionExecutionThreadStartsId;
  private static final int serialPooledThreadStartsId;
  private static final int TOSentMsgId;

  private static final int replyHandoffTimeId;

  private static final int serialThreadJobsId;
  private static final int serialPooledThreadJobsId;
  private static final int pooledMessageThreadJobsId;
  private static final int highPriorityThreadJobsId;
  private static final int waitingPoolThreadJobsId;

  private static final int eldersId;
  private static final int initialImageMessagesInFlightId;
  private static final int initialImageRequestsInProgressId;

  // For GMSHealthMonitor
  private static final int heartbeatRequestsSentId;
  private static final int heartbeatRequestsReceivedId;
  private static final int heartbeatsSentId;
  private static final int heartbeatsReceivedId;
  private static final int suspectsSentId;
  private static final int suspectsReceivedId;
  private static final int finalCheckRequestsSentId;
  private static final int finalCheckRequestsReceivedId;
  private static final int finalCheckResponsesSentId;
  private static final int finalCheckResponsesReceivedId;
  private static final int tcpFinalCheckRequestsSentId;
  private static final int tcpFinalCheckRequestsReceivedId;
  private static final int tcpFinalCheckResponsesSentId;
  private static final int tcpFinalCheckResponsesReceivedId;
  private static final int udpFinalCheckRequestsSentId;
  private static final int udpFinalCheckResponsesReceivedId;

  static {
    String statName = "DistributionStats";
    String statDescription = "Statistics on the gemfire distribution layer.";

    final String sentMessagesDesc =
        "The number of distribution messages that this GemFire system has sent. This includes broadcastMessages.";
    final String sentCommitMessagesDesc =
        "The number of transaction commit messages that this GemFire system has created to be sent. Note that it is possible for a commit to only create one message even though it will end up being sent to multiple recipients.";
    final String commitWaitsDesc =
        "The number of transaction commits that had to wait for a response before they could complete.";
    final String sentMessagesTimeDesc =
        "The total amount of time this distribution manager has spent sending messages. This includes broadcastMessagesTime.";
    final String sentMessagesMaxTimeDesc =
        "The highest amount of time this distribution manager has spent distributing a single message to the network.";
    final String broadcastMessagesDesc =
        "The number of distribution messages that this GemFire system has broadcast. A broadcast message is one sent to every other manager in the group.";
    final String broadcastMessagesTimeDesc =
        "The total amount of time this distribution manager has spent broadcasting messages. A broadcast message is one sent to every other manager in the group.";
    final String receivedMessagesDesc =
        "The number of distribution messages that this GemFire system has received.";
    final String receivedBytesDesc =
        "The number of distribution message bytes that this GemFire system has received.";
    final String sentBytesDesc =
        "The number of distribution message bytes that this GemFire system has sent.";
    final String processedMessagesDesc =
        "The number of distribution messages that this GemFire system has processed.";
    final String processedMessagesTimeDesc =
        "The amount of time this distribution manager has spent in message.process().";
    final String messageProcessingScheduleTimeDesc =
        "The amount of time this distribution manager has spent dispatching message to processor threads.";
    final String overflowQueueSizeDesc =
        "The number of normal distribution messages currently waiting to be processed.";
    final String waitingQueueSizeDesc =
        "The number of distribution messages currently waiting for some other resource before they can be processed.";
    final String overflowQueueThrottleTimeDesc =
        "The total amount of time, in nanoseconds, spent delayed by the overflow queue throttle.";
    final String overflowQueueThrottleCountDesc =
        "The total number of times a thread was delayed in adding a normal message to the overflow queue.";
    final String highPriorityQueueSizeDesc =
        "The number of high priority distribution messages currently waiting to be processed.";
    final String highPriorityQueueThrottleTimeDesc =
        "The total amount of time, in nanoseconds, spent delayed by the high priority queue throttle.";
    final String highPriorityQueueThrottleCountDesc =
        "The total number of times a thread was delayed in adding a normal message to the high priority queue.";
    final String serialQueueSizeDesc =
        "The number of serial distribution messages currently waiting to be processed.";
    final String serialQueueBytesDesc =
        "The approximate number of bytes consumed by serial distribution messages currently waiting to be processed.";
    final String serialPooledThreadDesc =
        "The number of threads created in the SerialQueuedExecutorPool.";
    final String serialQueueThrottleTimeDesc =
        "The total amount of time, in nanoseconds, spent delayed by the serial queue throttle.";
    final String serialQueueThrottleCountDesc =
        "The total number of times a thread was delayed in adding a ordered message to the serial queue.";
    final String serialThreadsDesc =
        "The number of threads currently processing serial/ordered messages.";
    final String processingThreadsDesc =
        "The number of threads currently processing normal messages.";
    final String highPriorityThreadsDesc =
        "The number of threads currently processing high priority messages.";
    final String partitionedRegionThreadsDesc =
        "The number of threads currently processing partitioned region messages.";
    final String functionExecutionThreadsDesc =
        "The number of threads currently processing function execution messages.";
    final String waitingThreadsDesc =
        "The number of threads currently processing messages that had to wait for a resource.";
    final String messageChannelTimeDesc =
        "The total amount of time received messages spent in the distribution channel";
    final String udpDispatchRequestTimeDesc =
        "The total amount of time spent deserializing and dispatching UDP messages in the message-reader thread.";
    final String replyMessageTimeDesc =
        "The amount of time spent processing reply messages. This includes both processedMessagesTime and messageProcessingScheduleTime.";
    final String distributeMessageTimeDesc =
        "The amount of time it takes to prepare a message and send it on the network.  This includes sentMessagesTime.";
    final String nodesDesc = "The current number of nodes in this distributed system.";
    final String replyWaitsInProgressDesc = "Current number of threads waiting for a reply.";
    final String replyWaitsCompletedDesc =
        "Total number of times waits for a reply have completed.";
    final String replyWaitTimeDesc = "Total time spent waiting for a reply to a message.";
    final String replyWaitMaxTimeDesc =
        "Maximum time spent transmitting and then waiting for a reply to a message. See sentMessagesMaxTime for related information";
    final String replyTimeoutsDesc = "Total number of message replies that have timed out.";
    final String receiverConnectionsDesc =
        "Current number of sockets dedicated to receiving messages.";
    final String failedAcceptsDesc =
        "Total number of times an accept (receiver creation) of a connect from some other member has failed";
    final String failedConnectsDesc =
        "Total number of times a connect (sender creation) to some other member has failed.";
    final String reconnectAttemptsDesc =
        "Total number of times an established connection was lost and a reconnect was attempted.";
    final String lostConnectionLeaseDesc =
        "Total number of times an unshared sender socket has remained idle long enough that its lease expired.";
    final String sharedOrderedSenderConnectionsDesc =
        "Current number of shared sockets dedicated to sending ordered messages.";
    final String sharedUnorderedSenderConnectionsDesc =
        "Current number of shared sockets dedicated to sending unordered messages.";
    final String threadOrderedSenderConnectionsDesc =
        "Current number of thread sockets dedicated to sending ordered messages.";
    final String threadUnorderedSenderConnectionsDesc =
        "Current number of thread sockets dedicated to sending unordered messages.";

    final String asyncQueuesDesc = "The current number of queues for asynchronous messaging.";
    final String asyncQueueFlushesInProgressDesc =
        "Current number of asynchronous queues being flushed.";
    final String asyncQueueFlushesCompletedDesc =
        "Total number of asynchronous queue flushes completed.";
    final String asyncQueueFlushTimeDesc = "Total time spent flushing asynchronous queues.";
    final String asyncQueueTimeoutExceededDesc =
        "Total number of asynchronous queues that have timed out by being blocked for more than async-queue-timeout milliseconds.";
    final String asyncQueueSizeExceededDesc =
        "Total number of asynchronous queues that have exceeded max size.";
    final String asyncDistributionTimeoutExceededDesc =
        "Total number of times the async-distribution-timeout has been exceeded during a socket write.";
    final String asyncQueueSizeDesc = "The current size in bytes used for asynchronous queues.";
    final String asyncQueuedMsgsDesc =
        "The total number of queued messages used for asynchronous queues.";
    final String asyncDequeuedMsgsDesc =
        "The total number of queued messages that have been removed from the queue and successfully sent.";
    final String asyncConflatedMsgsDesc =
        "The total number of queued conflated messages used for asynchronous queues.";

    final String asyncThreadsDesc = "Total number of asynchronous message queue threads.";
    final String asyncThreadInProgressDesc =
        "Current iterations of work performed by asynchronous message queue threads.";
    final String asyncThreadCompletedDesc =
        "Total number of iterations of work performed by asynchronous message queue threads.";
    final String asyncThreadTimeDesc =
        "Total time spent by asynchronous message queue threads performing iterations.";
    final String receiverDirectBufferSizeDesc =
        "Current number of bytes allocated from direct memory as buffers for incoming messages.";
    final String receiverHeapBufferSizeDesc =
        "Current number of bytes allocated from Java heap memory as buffers for incoming messages.";
    final String senderDirectBufferSizeDesc =
        "Current number of bytes allocated from direct memory as buffers for outgoing messages.";
    final String senderHeapBufferSizeDesc =
        "Current number of bytes allocated from Java heap memory as buffers for outoing messages.";

    final String replyHandoffTimeDesc =
        "Total number of seconds to switch thread contexts from processing thread to application thread.";

    final String partitionedRegionThreadJobsDesc =
        "The number of messages currently being processed by partitioned region threads";
    final String functionExecutionThreadJobsDesc =
        "The number of messages currently being processed by function execution threads";
    final String serialThreadJobsDesc =
        "The number of messages currently being processed by serial threads.";
    final String serialPooledThreadJobsDesc =
        "The number of messages currently being processed by pooled serial processor threads.";
    final String processingThreadJobsDesc =
        "The number of messages currently being processed by pooled message processor threads.";
    final String highPriorityThreadJobsDesc =
        "The number of messages currently being processed by high priority processor threads.";
    final String waitingThreadJobsDesc =
        "The number of messages currently being processed by waiting pooly processor threads.";

    final String eldersDesc = "Current number of system elders hosted in this member.";
    final String initialImageMessagesInFlightDesc =
        "The number of messages with initial image data sent from this member that have not yet been acknowledged.";
    final String initialImageRequestsInProgressDesc =
        "The number of initial images this member is currently receiving.";

    // For GMSHealthMonitor
    final String heartbeatRequestsSentDesc =
        "Heartbeat request messages that this member has sent.";
    final String heartbeatRequestsReceivedDesc =
        "Heartbeat request messages that this member has received.";

    final String heartbeatsSentDesc = "Heartbeat messages that this member has sent.";
    final String heartbeatsReceivedDesc = "Heartbeat messages that this member has received.";

    final String suspectsSentDesc = "Suspect member messages that this member has sent.";
    final String suspectsReceivedDesc = "Suspect member messages that this member has received.";

    final String finalCheckRequestsSentDesc = "Final check requests that this member has sent.";
    final String finalCheckRequestsReceivedDesc =
        "Final check requests that this member has received.";

    final String finalCheckResponsesSentDesc = "Final check responses that this member has sent.";
    final String finalCheckResponsesReceivedDesc =
        "Final check responses that this member has received.";

    final String tcpFinalCheckRequestsSentDesc =
        "TCP final check requests that this member has sent.";
    final String tcpFinalCheckRequestsReceivedDesc =
        "TCP final check requests that this member has received.";

    final String tcpFinalCheckResponsesSentDesc =
        "TCP final check responses that this member has sent.";
    final String tcpFinalCheckResponsesReceivedDesc =
        "TCP final check responses that this member has received.";

    final String udpFinalCheckRequestsSentDesc =
        "UDP final check requests that this member has sent.";
    final String udpFinalCheckRequestsReceivedDesc =
        "UDP final check requests that this member has received.";

    final String udpFinalCheckResponsesSentDesc =
        "UDP final check responses that this member has sent.";
    final String udpFinalCheckResponsesReceivedDesc =
        "UDP final check responses that this member has received.";

    final String senderCreatesDesc =
        "Total amount of time, in nanoseconds, spent creating a sender.";
    final String senderCreatesInProgressDesc =
        "Current number of sender creations in progress.";

    StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();

    type = f.createType(statName, statDescription, new StatisticDescriptor[] {
        f.createLongCounter("sentMessages", sentMessagesDesc, "messages"),
        f.createLongCounter("commitMessages", sentCommitMessagesDesc, "messages"),
        f.createLongCounter("commitWaits", commitWaitsDesc, "messages"),
        f.createLongCounter("sentMessagesTime", sentMessagesTimeDesc, "nanoseconds", false),
        f.createLongGauge("sentMessagesMaxTime", sentMessagesMaxTimeDesc, "milliseconds", false),
        f.createLongCounter("broadcastMessages", broadcastMessagesDesc, "messages"),
        f.createLongCounter("broadcastMessagesTime", broadcastMessagesTimeDesc, "nanoseconds",
            false),
        f.createLongCounter("receivedMessages", receivedMessagesDesc, "messages"),
        f.createLongCounter("receivedBytes", receivedBytesDesc, "bytes"),
        f.createLongCounter("sentBytes", sentBytesDesc, "bytes"),
        f.createLongCounter("processedMessages", processedMessagesDesc, "messages"),
        f.createLongCounter("processedMessagesTime", processedMessagesTimeDesc, "nanoseconds",
            false),
        f.createLongCounter("messageProcessingScheduleTime", messageProcessingScheduleTimeDesc,
            "nanoseconds", false),
        f.createIntGauge("overflowQueueSize", overflowQueueSizeDesc, "messages"),
        f.createIntGauge("waitingQueueSize", waitingQueueSizeDesc, "messages"),
        f.createIntGauge("overflowQueueThrottleCount", overflowQueueThrottleCountDesc, "delays"),
        f.createLongCounter("overflowQueueThrottleTime", overflowQueueThrottleTimeDesc,
            "nanoseconds", false),
        f.createIntGauge("highPriorityQueueSize", highPriorityQueueSizeDesc, "messages"),
        f.createIntGauge("highPriorityQueueThrottleCount", highPriorityQueueThrottleCountDesc,
            "delays"),
        f.createLongCounter("highPriorityQueueThrottleTime", highPriorityQueueThrottleTimeDesc,
            "nanoseconds", false),
        f.createIntGauge("partitionedRegionQueueSize", highPriorityQueueSizeDesc, "messages"),
        f.createIntGauge("partitionedRegionQueueThrottleCount", highPriorityQueueThrottleCountDesc,
            "delays"),
        f.createLongCounter("partitionedRegionQueueThrottleTime", highPriorityQueueThrottleTimeDesc,
            "nanoseconds", false),
        f.createIntGauge("functionExecutionQueueSize", highPriorityQueueSizeDesc, "messages"),
        f.createIntGauge("functionExecutionQueueThrottleCount", highPriorityQueueThrottleCountDesc,
            "delays"),
        f.createLongCounter("functionExecutionQueueThrottleTime", highPriorityQueueThrottleTimeDesc,
            "nanoseconds", false),
        f.createIntGauge("serialQueueSize", serialQueueSizeDesc, "messages"),
        f.createIntGauge("serialQueueBytes", serialQueueBytesDesc, "bytes"),
        f.createIntCounter("serialPooledThread", serialPooledThreadDesc, "threads"),
        f.createIntGauge("serialQueueThrottleCount", serialQueueThrottleCountDesc, "delays"),
        f.createLongCounter("serialQueueThrottleTime", serialQueueThrottleTimeDesc, "nanoseconds",
            false),
        f.createIntGauge("serialThreads", serialThreadsDesc, "threads"),
        f.createIntGauge("processingThreads", processingThreadsDesc, "threads"),
        f.createIntGauge("highPriorityThreads", highPriorityThreadsDesc, "threads"),
        f.createIntGauge("partitionedRegionThreads", partitionedRegionThreadsDesc, "threads"),
        f.createIntGauge("functionExecutionThreads", functionExecutionThreadsDesc, "threads"),
        f.createIntGauge("waitingThreads", waitingThreadsDesc, "threads"),
        f.createLongCounter("messageChannelTime", messageChannelTimeDesc, "nanoseconds", false),
        f.createLongCounter("udpDispatchRequestTime", udpDispatchRequestTimeDesc, "nanoseconds",
            false),
        f.createLongCounter("replyMessageTime", replyMessageTimeDesc, "nanoseconds", false),
        f.createLongCounter("distributeMessageTime", distributeMessageTimeDesc, "nanoseconds",
            false),
        f.createIntGauge("nodes", nodesDesc, "nodes"),
        f.createIntGauge("replyWaitsInProgress", replyWaitsInProgressDesc, "operations"),
        f.createIntCounter("replyWaitsCompleted", replyWaitsCompletedDesc, "operations"),
        f.createLongCounter("replyWaitTime", replyWaitTimeDesc, "nanoseconds", false),
        f.createLongGauge("replyWaitMaxTime", replyWaitMaxTimeDesc, "milliseconds", false),
        f.createLongCounter("replyTimeouts", replyTimeoutsDesc, "timeouts", false),
        f.createIntGauge("receivers", receiverConnectionsDesc, "sockets"),
        f.createIntGauge("sendersSO", sharedOrderedSenderConnectionsDesc, "sockets"),
        f.createIntGauge("sendersSU", sharedUnorderedSenderConnectionsDesc, "sockets"),
        f.createIntGauge("sendersTO", threadOrderedSenderConnectionsDesc, "sockets"),
        f.createIntGauge("sendersTU", threadUnorderedSenderConnectionsDesc, "sockets"),
        f.createIntCounter("failedAccepts", failedAcceptsDesc, "accepts"),
        f.createIntCounter("failedConnects", failedConnectsDesc, "connects"),
        f.createIntCounter("reconnectAttempts", reconnectAttemptsDesc, "connects"),
        f.createIntCounter("senderTimeouts", lostConnectionLeaseDesc, "expirations"),
        f.createLongCounter("senderCreateTime", senderCreatesDesc, "nanoseconds"),
        f.createLongGauge("senderCreatesInProgress", senderCreatesInProgressDesc, "operations"),

        f.createIntGauge("syncSocketWritesInProgress",
            "Current number of synchronous/blocking socket write calls in progress.", "writes"),
        f.createLongCounter("syncSocketWriteTime",
            "Total amount of time, in nanoseconds, spent in synchronous/blocking socket write calls.",
            "nanoseconds"),
        f.createIntCounter("syncSocketWrites",
            "Total number of completed synchronous/blocking socket write calls.", "writes"),
        f.createLongCounter("syncSocketWriteBytes",
            "Total number of bytes sent out in synchronous/blocking mode on sockets.", "bytes"),

        f.createIntCounter("ucastReads", "Total number of unicast datagrams received", "datagrams"),
        f.createLongCounter("ucastReadBytes", "Total number of bytes received in unicast datagrams",
            "bytes"),
        f.createIntCounter("ucastWrites", "Total number of unicast datagram socket write calls.",
            "writes"),
        f.createLongCounter("ucastWriteBytes",
            "Total number of bytes sent out on unicast datagram sockets.", "bytes"),
        f.createIntCounter("ucastRetransmits",
            "Total number of unicast datagram socket retransmissions", "writes"),

        f.createIntCounter("mcastReads", "Total number of multicast datagrams received",
            "datagrams"),
        f.createLongCounter("mcastReadBytes",
            "Total number of bytes received in multicast datagrams", "bytes"),
        f.createIntCounter("mcastWrites", "Total number of multicast datagram socket write calls.",
            "writes"),
        f.createLongCounter("mcastWriteBytes",
            "Total number of bytes sent out on multicast datagram sockets.", "bytes"),
        f.createIntCounter("mcastRetransmits",
            "Total number of multicast datagram socket retransmissions", "writes"),
        f.createIntCounter("mcastRetransmitRequests",
            "Total number of multicast datagram socket retransmission requests sent to other processes",
            "requests"),

        f.createLongCounter("serializationTime",
            "Total amount of time, in nanoseconds, spent serializing objects. This includes pdx serializations.",
            "nanoseconds"),
        f.createIntCounter("serializations",
            "Total number of object serialization calls. This includes pdx serializations.", "ops"),
        f.createLongCounter("serializedBytes",
            "Total number of bytes produced by object serialization. This includes pdx serializations.",
            "bytes"),
        f.createIntCounter("pdxSerializations", "Total number of pdx serializations.", "ops"),
        f.createLongCounter("pdxSerializedBytes",
            "Total number of bytes produced by pdx serialization.", "bytes"),
        f.createLongCounter("deserializationTime",
            "Total amount of time, in nanoseconds, spent deserializing objects. This includes deserialization that results in a PdxInstance.",
            "nanoseconds"),
        f.createIntCounter("deserializations",
            "Total number of object deserialization calls. This includes deserialization that results in a PdxInstance.",
            "ops"),
        f.createLongCounter("deserializedBytes",
            "Total number of bytes read by object deserialization. This includes deserialization that results in a PdxInstance.",
            "bytes"),
        f.createIntCounter("pdxDeserializations", "Total number of pdx deserializations.", "ops"),
        f.createLongCounter("pdxDeserializedBytes",
            "Total number of bytes read by pdx deserialization.", "bytes"),
        f.createLongCounter("msgSerializationTime",
            "Total amount of time, in nanoseconds, spent serializing messages.", "nanoseconds"),
        f.createLongCounter("msgDeserializationTime",
            "Total amount of time, in nanoseconds, spent deserializing messages.", "nanoseconds"),
        f.createLongCounter("udpMsgEncryptionTime",
            "Total amount of time, in nanoseconds, spent encrypting udp messages.", "nanoseconds"),
        f.createLongCounter("udpMsgDecryptionTime",
            "Total amount of time, in nanoseconds, spent decrypting udp messages.", "nanoseconds"),
        f.createIntCounter("pdxInstanceDeserializations",
            "Total number of times getObject has been called on a PdxInstance.", "ops"),
        f.createLongCounter("pdxInstanceDeserializationTime",
            "Total amount of time, in nanoseconds, spent deserializing PdxInstances by calling getObject.",
            "nanoseconds"),
        f.createIntCounter("pdxInstanceCreations",
            "Total number of times a deserialization created a PdxInstance.", "ops"),

        f.createLongCounter("batchSendTime",
            "Total amount of time, in nanoseconds, spent queueing and flushing message batches",
            "nanoseconds"),
        f.createLongCounter("batchWaitTime", "Reserved for future use", "nanoseconds"),
        f.createLongCounter("batchCopyTime",
            "Total amount of time, in nanoseconds, spent copying messages for batched transmission",
            "nanoseconds"),
        f.createLongCounter("batchFlushTime",
            "Total amount of time, in nanoseconds, spent flushing batched messages to the network",
            "nanoseconds"),

        f.createIntGauge("asyncSocketWritesInProgress",
            "Current number of non-blocking socket write calls in progress.", "writes"),
        f.createIntCounter("asyncSocketWrites",
            "Total number of non-blocking socket write calls completed.", "writes"),
        f.createIntCounter("asyncSocketWriteRetries",
            "Total number of retries needed to write a single block of data using non-blocking socket write calls.",
            "writes"),
        f.createLongCounter("asyncSocketWriteTime",
            "Total amount of time, in nanoseconds, spent in non-blocking socket write calls.",
            "nanoseconds"),
        f.createLongCounter("asyncSocketWriteBytes",
            "Total number of bytes sent out on non-blocking sockets.", "bytes"),

        f.createLongCounter("asyncQueueAddTime",
            "Total amount of time, in nanoseconds, spent in adding messages to async queue.",
            "nanoseconds"),
        f.createLongCounter("asyncQueueRemoveTime",
            "Total amount of time, in nanoseconds, spent in removing messages from async queue.",
            "nanoseconds"),

        f.createIntGauge("asyncQueues", asyncQueuesDesc, "queues"),
        f.createIntGauge("asyncQueueFlushesInProgress", asyncQueueFlushesInProgressDesc,
            "operations"),
        f.createIntCounter("asyncQueueFlushesCompleted", asyncQueueFlushesCompletedDesc,
            "operations"),
        f.createLongCounter("asyncQueueFlushTime", asyncQueueFlushTimeDesc, "nanoseconds", false),
        f.createIntCounter("asyncQueueTimeoutExceeded", asyncQueueTimeoutExceededDesc, "timeouts"),
        f.createIntCounter("asyncQueueSizeExceeded", asyncQueueSizeExceededDesc, "operations"),
        f.createIntCounter("asyncDistributionTimeoutExceeded", asyncDistributionTimeoutExceededDesc,
            "operations"),
        f.createLongGauge("asyncQueueSize", asyncQueueSizeDesc, "bytes"),
        f.createLongCounter("asyncQueuedMsgs", asyncQueuedMsgsDesc, "msgs"),
        f.createLongCounter("asyncDequeuedMsgs", asyncDequeuedMsgsDesc, "msgs"),
        f.createLongCounter("asyncConflatedMsgs", asyncConflatedMsgsDesc, "msgs"),

        f.createIntGauge("asyncThreads", asyncThreadsDesc, "threads"),
        f.createIntGauge("asyncThreadInProgress", asyncThreadInProgressDesc, "operations"),
        f.createIntCounter("asyncThreadCompleted", asyncThreadCompletedDesc, "operations"),
        f.createLongCounter("asyncThreadTime", asyncThreadTimeDesc, "nanoseconds", false),

        f.createLongGauge("receiversTO",
            "Number of receiver threads owned by non-receiver threads in other members.",
            "threads"),
        f.createLongGauge("receiversTO2",
            "Number of receiver threads owned in turn by receiver threads in other members",
            "threads"),

        f.createLongGauge("receiverDirectBufferSize", receiverDirectBufferSizeDesc, "bytes"),
        f.createLongGauge("receiverHeapBufferSize", receiverHeapBufferSizeDesc, "bytes"),
        f.createLongGauge("senderDirectBufferSize", senderDirectBufferSizeDesc, "bytes"),
        f.createLongGauge("senderHeapBufferSize", senderHeapBufferSizeDesc, "bytes"),
        f.createIntGauge("socketLocksInProgress",
            "Current number of threads waiting to lock a socket", "threads", false),
        f.createIntCounter("socketLocks", "Total number of times a socket has been locked.",
            "locks"),
        f.createLongCounter("socketLockTime",
            "Total amount of time, in nanoseconds, spent locking a socket", "nanoseconds", false),
        f.createIntGauge("bufferAcquiresInProgress",
            "Current number of threads waiting to acquire a buffer", "threads", false),
        f.createIntCounter("bufferAcquires", "Total number of times a buffer has been acquired.",
            "operations"),
        f.createLongCounter("bufferAcquireTime",
            "Total amount of time, in nanoseconds, spent acquiring a socket", "nanoseconds", false),

        f.createIntGauge("messagesBeingReceived",
            "Current number of message being received off the network or being processed after reception.",
            "messages"),
        f.createLongGauge("messageBytesBeingReceived",
            "Current number of bytes consumed by messages being received or processed.", "bytes"),

        f.createLongCounter("serialThreadStarts",
            "Total number of times a thread has been created for the serial message executor.",
            "starts", false),
        f.createLongCounter("processingThreadStarts",
            "Total number of times a thread has been created for the pool processing normal messages.",
            "starts", false),
        f.createLongCounter("highPriorityThreadStarts",
            "Total number of times a thread has been created for the pool handling high priority messages.",
            "starts", false),
        f.createLongCounter("waitingThreadStarts",
            "Total number of times a thread has been created for the waiting pool.", "starts",
            false),
        f.createLongCounter("partitionedRegionThreadStarts",
            "Total number of times a thread has been created for the pool handling partitioned region messages.",
            "starts", false),
        f.createLongCounter("functionExecutionThreadStarts",
            "Total number of times a thread has been created for the pool handling function execution messages.",
            "starts", false),
        f.createLongCounter("serialPooledThreadStarts",
            "Total number of times a thread has been created for the serial pool(s).", "starts",
            false),
        f.createLongCounter("TOSentMsgs", "Total number of messages sent on thread owned senders",
            "messages", false),
        f.createLongCounter("replyHandoffTime", replyHandoffTimeDesc, "nanoseconds"),

        f.createIntGauge("partitionedRegionThreadJobs", partitionedRegionThreadJobsDesc,
            "messages"),
        f.createIntGauge("functionExecutionThreadJobs", functionExecutionThreadJobsDesc,
            "messages"),
        f.createIntGauge("serialThreadJobs", serialThreadJobsDesc, "messages"),
        f.createIntGauge("serialPooledThreadJobs", serialPooledThreadJobsDesc, "messages"),
        f.createIntGauge("processingThreadJobs", processingThreadJobsDesc, "messages"),
        f.createIntGauge("highPriorityThreadJobs", highPriorityThreadJobsDesc, "messages"),
        f.createIntGauge("waitingThreadJobs", waitingThreadJobsDesc, "messages"),

        f.createIntGauge("elders", eldersDesc, "elders"),
        f.createIntGauge("initialImageMessagesInFlight", initialImageMessagesInFlightDesc,
            "messages"),
        f.createIntGauge("initialImageRequestsInProgress", initialImageRequestsInProgressDesc,
            "requests"),

        // For GMSHealthMonitor
        f.createLongCounter("heartbeatRequestsSent", heartbeatRequestsSentDesc, "messages"),
        f.createLongCounter("heartbeatRequestsReceived", heartbeatRequestsReceivedDesc, "messages"),
        f.createLongCounter("heartbeatsSent", heartbeatsSentDesc, "messages"),
        f.createLongCounter("heartbeatsReceived", heartbeatsReceivedDesc, "messages"),
        f.createLongCounter("suspectsSent", suspectsSentDesc, "messages"),
        f.createLongCounter("suspectsReceived", suspectsReceivedDesc, "messages"),
        f.createLongCounter("finalCheckRequestsSent", finalCheckRequestsSentDesc, "messages"),
        f.createLongCounter("finalCheckRequestsReceived", finalCheckRequestsReceivedDesc,
            "messages"),
        f.createLongCounter("finalCheckResponsesSent", finalCheckResponsesSentDesc, "messages"),
        f.createLongCounter("finalCheckResponsesReceived", finalCheckResponsesReceivedDesc,
            "messages"),
        f.createLongCounter("tcpFinalCheckRequestsSent", tcpFinalCheckRequestsSentDesc, "messages"),
        f.createLongCounter("tcpFinalCheckRequestsReceived", tcpFinalCheckRequestsReceivedDesc,
            "messages"),
        f.createLongCounter("tcpFinalCheckResponsesSent", tcpFinalCheckResponsesSentDesc,
            "messages"),
        f.createLongCounter("tcpFinalCheckResponsesReceived", tcpFinalCheckResponsesReceivedDesc,
            "messages"),
        f.createLongCounter("udpFinalCheckRequestsSent", udpFinalCheckRequestsSentDesc, "messages"),
        f.createLongCounter("udpFinalCheckRequestsReceived", udpFinalCheckRequestsReceivedDesc,
            "messages"),
        f.createLongCounter("udpFinalCheckResponsesSent", udpFinalCheckResponsesSentDesc,
            "messages"),
        f.createLongCounter("udpFinalCheckResponsesReceived", udpFinalCheckResponsesReceivedDesc,
            "messages"),});

    // Initialize id fields
    sentMessagesId = type.nameToId("sentMessages");
    sentCommitMessagesId = type.nameToId("commitMessages");
    commitWaitsId = type.nameToId("commitWaits");
    sentMessagesTimeId = type.nameToId("sentMessagesTime");
    sentMessagesMaxTimeId = type.nameToId("sentMessagesMaxTime");
    broadcastMessagesId = type.nameToId("broadcastMessages");
    broadcastMessagesTimeId = type.nameToId("broadcastMessagesTime");
    receivedMessagesId = type.nameToId("receivedMessages");
    receivedBytesId = type.nameToId("receivedBytes");
    sentBytesId = type.nameToId("sentBytes");
    processedMessagesId = type.nameToId("processedMessages");
    processedMessagesTimeId = type.nameToId("processedMessagesTime");
    messageProcessingScheduleTimeId = type.nameToId("messageProcessingScheduleTime");
    messageChannelTimeId = type.nameToId("messageChannelTime");
    udpDispatchRequestTimeId = type.nameToId("udpDispatchRequestTime");
    replyMessageTimeId = type.nameToId("replyMessageTime");
    distributeMessageTimeId = type.nameToId("distributeMessageTime");
    nodesId = type.nameToId("nodes");
    overflowQueueSizeId = type.nameToId("overflowQueueSize");
    waitingQueueSizeId = type.nameToId("waitingQueueSize");
    overflowQueueThrottleTimeId = type.nameToId("overflowQueueThrottleTime");
    overflowQueueThrottleCountId = type.nameToId("overflowQueueThrottleCount");
    highPriorityQueueSizeId = type.nameToId("highPriorityQueueSize");
    highPriorityQueueThrottleTimeId = type.nameToId("highPriorityQueueThrottleTime");
    highPriorityQueueThrottleCountId = type.nameToId("highPriorityQueueThrottleCount");
    partitionedRegionQueueSizeId = type.nameToId("partitionedRegionQueueSize");
    partitionedRegionQueueThrottleTimeId = type.nameToId("partitionedRegionQueueThrottleTime");
    partitionedRegionQueueThrottleCountId = type.nameToId("partitionedRegionQueueThrottleCount");
    functionExecutionQueueSizeId = type.nameToId("functionExecutionQueueSize");
    functionExecutionQueueThrottleTimeId = type.nameToId("functionExecutionQueueThrottleTime");
    functionExecutionQueueThrottleCountId = type.nameToId("functionExecutionQueueThrottleCount");
    serialQueueSizeId = type.nameToId("serialQueueSize");
    serialQueueBytesId = type.nameToId("serialQueueBytes");
    serialPooledThreadId = type.nameToId("serialPooledThread");
    serialQueueThrottleTimeId = type.nameToId("serialQueueThrottleTime");
    serialQueueThrottleCountId = type.nameToId("serialQueueThrottleCount");
    serialThreadsId = type.nameToId("serialThreads");
    processingThreadsId = type.nameToId("processingThreads");
    highPriorityThreadsId = type.nameToId("highPriorityThreads");
    partitionedRegionThreadsId = type.nameToId("partitionedRegionThreads");
    functionExecutionThreadsId = type.nameToId("functionExecutionThreads");
    waitingThreadsId = type.nameToId("waitingThreads");
    replyWaitsInProgressId = type.nameToId("replyWaitsInProgress");
    replyWaitsCompletedId = type.nameToId("replyWaitsCompleted");
    replyWaitTimeId = type.nameToId("replyWaitTime");
    replyTimeoutsId = type.nameToId("replyTimeouts");
    replyWaitMaxTimeId = type.nameToId("replyWaitMaxTime");
    receiverConnectionsId = type.nameToId("receivers");
    failedAcceptsId = type.nameToId("failedAccepts");
    failedConnectsId = type.nameToId("failedConnects");
    reconnectAttemptsId = type.nameToId("reconnectAttempts");
    lostConnectionLeaseId = type.nameToId("senderTimeouts");
    sharedOrderedSenderConnectionsId = type.nameToId("sendersSO");
    sharedUnorderedSenderConnectionsId = type.nameToId("sendersSU");
    threadOrderedSenderConnectionsId = type.nameToId("sendersTO");
    threadUnorderedSenderConnectionsId = type.nameToId("sendersTU");
    senderCreateTimeId = type.nameToId("senderCreateTime");
    senderCreatesInProgressId = type.nameToId("senderCreatesInProgress");

    syncSocketWritesInProgressId = type.nameToId("syncSocketWritesInProgress");
    syncSocketWriteTimeId = type.nameToId("syncSocketWriteTime");
    syncSocketWritesId = type.nameToId("syncSocketWrites");
    syncSocketWriteBytesId = type.nameToId("syncSocketWriteBytes");

    ucastReadsId = type.nameToId("ucastReads");
    ucastReadBytesId = type.nameToId("ucastReadBytes");
    ucastWritesId = type.nameToId("ucastWrites");
    ucastWriteBytesId = type.nameToId("ucastWriteBytes");
    ucastRetransmitsId = type.nameToId("ucastRetransmits");

    mcastReadsId = type.nameToId("mcastReads");
    mcastReadBytesId = type.nameToId("mcastReadBytes");
    mcastWritesId = type.nameToId("mcastWrites");
    mcastWriteBytesId = type.nameToId("mcastWriteBytes");
    mcastRetransmitsId = type.nameToId("mcastRetransmits");
    mcastRetransmitRequestsId = type.nameToId("mcastRetransmitRequests");

    serializationTimeId = type.nameToId("serializationTime");
    serializationsId = type.nameToId("serializations");
    serializedBytesId = type.nameToId("serializedBytes");
    deserializationTimeId = type.nameToId("deserializationTime");
    deserializationsId = type.nameToId("deserializations");
    deserializedBytesId = type.nameToId("deserializedBytes");
    pdxSerializationsId = type.nameToId("pdxSerializations");
    pdxSerializedBytesId = type.nameToId("pdxSerializedBytes");
    pdxDeserializationsId = type.nameToId("pdxDeserializations");
    pdxDeserializedBytesId = type.nameToId("pdxDeserializedBytes");
    pdxInstanceDeserializationsId = type.nameToId("pdxInstanceDeserializations");
    pdxInstanceDeserializationTimeId = type.nameToId("pdxInstanceDeserializationTime");
    pdxInstanceCreationsId = type.nameToId("pdxInstanceCreations");

    msgSerializationTimeId = type.nameToId("msgSerializationTime");
    msgDeserializationTimeId = type.nameToId("msgDeserializationTime");

    udpMsgEncryptionTimeId = type.nameToId("udpMsgEncryptionTime");
    udpMsgDecryptionTimeId = type.nameToId("udpMsgDecryptionTime");

    batchSendTimeId = type.nameToId("batchSendTime");
    batchCopyTimeId = type.nameToId("batchCopyTime");
    batchWaitTimeId = type.nameToId("batchWaitTime");
    batchFlushTimeId = type.nameToId("batchFlushTime");

    asyncSocketWritesInProgressId = type.nameToId("asyncSocketWritesInProgress");
    asyncSocketWritesId = type.nameToId("asyncSocketWrites");
    asyncSocketWriteRetriesId = type.nameToId("asyncSocketWriteRetries");
    asyncSocketWriteTimeId = type.nameToId("asyncSocketWriteTime");
    asyncSocketWriteBytesId = type.nameToId("asyncSocketWriteBytes");

    asyncQueueAddTimeId = type.nameToId("asyncQueueAddTime");
    asyncQueueRemoveTimeId = type.nameToId("asyncQueueRemoveTime");

    asyncQueuesId = type.nameToId("asyncQueues");
    asyncQueueFlushesInProgressId = type.nameToId("asyncQueueFlushesInProgress");
    asyncQueueFlushesCompletedId = type.nameToId("asyncQueueFlushesCompleted");
    asyncQueueFlushTimeId = type.nameToId("asyncQueueFlushTime");
    asyncQueueTimeoutExceededId = type.nameToId("asyncQueueTimeoutExceeded");
    asyncQueueSizeExceededId = type.nameToId("asyncQueueSizeExceeded");
    asyncDistributionTimeoutExceededId = type.nameToId("asyncDistributionTimeoutExceeded");
    asyncQueueSizeId = type.nameToId("asyncQueueSize");
    asyncQueuedMsgsId = type.nameToId("asyncQueuedMsgs");
    asyncDequeuedMsgsId = type.nameToId("asyncDequeuedMsgs");
    asyncConflatedMsgsId = type.nameToId("asyncConflatedMsgs");

    asyncThreadsId = type.nameToId("asyncThreads");
    asyncThreadInProgressId = type.nameToId("asyncThreadInProgress");
    asyncThreadCompletedId = type.nameToId("asyncThreadCompleted");
    asyncThreadTimeId = type.nameToId("asyncThreadTime");

    threadOwnedReceiversId = type.nameToId("receiversTO");
    threadOwnedReceiversId2 = type.nameToId("receiversTO2");

    receiverDirectBufferSizeId = type.nameToId("receiverDirectBufferSize");
    receiverHeapBufferSizeId = type.nameToId("receiverHeapBufferSize");
    senderDirectBufferSizeId = type.nameToId("senderDirectBufferSize");
    senderHeapBufferSizeId = type.nameToId("senderHeapBufferSize");

    socketLocksInProgressId = type.nameToId("socketLocksInProgress");
    socketLocksId = type.nameToId("socketLocks");
    socketLockTimeId = type.nameToId("socketLockTime");

    bufferAcquiresInProgressId = type.nameToId("bufferAcquiresInProgress");
    bufferAcquiresId = type.nameToId("bufferAcquires");
    bufferAcquireTimeId = type.nameToId("bufferAcquireTime");
    messagesBeingReceivedId = type.nameToId("messagesBeingReceived");
    messageBytesBeingReceivedId = type.nameToId("messageBytesBeingReceived");

    serialThreadStartsId = type.nameToId("serialThreadStarts");
    processingThreadStartsId = type.nameToId("processingThreadStarts");
    highPriorityThreadStartsId = type.nameToId("highPriorityThreadStarts");
    waitingThreadStartsId = type.nameToId("waitingThreadStarts");
    partitionedRegionThreadStartsId = type.nameToId("partitionedRegionThreadStarts");
    functionExecutionThreadStartsId = type.nameToId("functionExecutionThreadStarts");
    serialPooledThreadStartsId = type.nameToId("serialPooledThreadStarts");
    TOSentMsgId = type.nameToId("TOSentMsgs");
    replyHandoffTimeId = type.nameToId("replyHandoffTime");
    partitionedRegionThreadJobsId = type.nameToId("partitionedRegionThreadJobs");
    functionExecutionThreadJobsId = type.nameToId("functionExecutionThreadJobs");
    serialThreadJobsId = type.nameToId("serialThreadJobs");
    serialPooledThreadJobsId = type.nameToId("serialPooledThreadJobs");
    pooledMessageThreadJobsId = type.nameToId("processingThreadJobs");
    highPriorityThreadJobsId = type.nameToId("highPriorityThreadJobs");
    waitingPoolThreadJobsId = type.nameToId("waitingThreadJobs");

    eldersId = type.nameToId("elders");
    initialImageMessagesInFlightId = type.nameToId("initialImageMessagesInFlight");
    initialImageRequestsInProgressId = type.nameToId("initialImageRequestsInProgress");

    // For GMSHealthMonitor
    heartbeatRequestsSentId = type.nameToId("heartbeatRequestsSent");
    heartbeatRequestsReceivedId = type.nameToId("heartbeatRequestsReceived");
    heartbeatsSentId = type.nameToId("heartbeatsSent");
    heartbeatsReceivedId = type.nameToId("heartbeatsReceived");
    suspectsSentId = type.nameToId("suspectsSent");
    suspectsReceivedId = type.nameToId("suspectsReceived");
    finalCheckRequestsSentId = type.nameToId("finalCheckRequestsSent");
    finalCheckRequestsReceivedId = type.nameToId("finalCheckRequestsReceived");
    finalCheckResponsesSentId = type.nameToId("finalCheckResponsesSent");
    finalCheckResponsesReceivedId = type.nameToId("finalCheckResponsesReceived");
    tcpFinalCheckRequestsSentId = type.nameToId("tcpFinalCheckRequestsSent");
    tcpFinalCheckRequestsReceivedId = type.nameToId("tcpFinalCheckRequestsReceived");
    tcpFinalCheckResponsesSentId = type.nameToId("tcpFinalCheckResponsesSent");
    tcpFinalCheckResponsesReceivedId = type.nameToId("tcpFinalCheckResponsesReceived");
    udpFinalCheckRequestsSentId = type.nameToId("udpFinalCheckRequestsSent");
    udpFinalCheckResponsesReceivedId = type.nameToId("udpFinalCheckResponsesReceived");
  }

  /** The Statistics object that we delegate most behavior to */
  private final Statistics stats;

  private final LongSupplier clock;

  private final MaxLongGauge maxReplyWaitTime;
  private final MaxLongGauge maxSentMessagesTime;
  private LongAdder serialQueueBytes = new LongAdder();

  //////////////////////// Constructors ////////////////////////

  /**
   * Creates a new <code>DistributionStats</code> and registers itself with the given statistics
   * factory.
   */
  public DistributionStats(StatisticsFactory f, long statId) {
    this(f, "distributionStats", statId, DistributionStats::getStatTime);
  }

  @VisibleForTesting
  public DistributionStats(StatisticsFactory factory, String textId, long statId,
      LongSupplier clock) {
    this(factory == null ? null : factory.createAtomicStatistics(type, textId, statId), clock);
  }

  @VisibleForTesting
  public DistributionStats(Statistics statistics) {
    this(statistics, DistributionStats::getStatTime);
  }

  @VisibleForTesting
  public DistributionStats(Statistics statistics, LongSupplier clock) {
    stats = statistics;
    this.clock = clock;
    maxReplyWaitTime = new MaxLongGauge(replyWaitMaxTimeId, stats);
    maxSentMessagesTime = new MaxLongGauge(sentMessagesMaxTimeId, stats);
  }

  /**
   * Returns the current NanoTime or, if clock stats are disabled, zero.
   *
   * @since GemFire 5.0
   */
  public static long getStatTime() {
    return enableClockStats ? NanoTimer.getTime() : 0;
  }

  ////////////////////// Instance Methods //////////////////////

  private long getTime() {
    return clock.getAsLong();
  }

  public void close() {
    this.stats.close();
  }

  /**
   * Returns the total number of messages sent by the distribution manager
   */
  @Override
  public long getSentMessages() {
    return this.stats.getLong(sentMessagesId);
  }

  @Override
  public void incTOSentMsg() {
    this.stats.incLong(TOSentMsgId, 1);
  }

  @Override
  public long getSentCommitMessages() {
    return this.stats.getLong(sentCommitMessagesId);
  }

  @Override
  public long getCommitWaits() {
    return this.stats.getLong(commitWaitsId);
  }

  /**
   * Increments the total number of messages sent by the distribution manager
   */
  @Override
  public void incSentMessages(long messages) {
    this.stats.incLong(sentMessagesId, messages);
  }

  /**
   * Increments the total number of transactino commit messages sent by the distribution manager
   */
  @Override
  public void incSentCommitMessages(long messages) {
    this.stats.incLong(sentCommitMessagesId, messages);
  }

  @Override
  public void incCommitWaits() {
    this.stats.incLong(commitWaitsId, 1);
  }

  /**
   * Returns the total number of nanoseconds spent sending messages.
   */
  @Override
  public long getSentMessagesTime() {
    return this.stats.getLong(sentMessagesTimeId);
  }

  /**
   * Increments the total number of nanoseconds spend sending messages.
   * <p>
   * This also sets the sentMessagesMaxTime, if appropriate
   */
  @Override
  public void incSentMessagesTime(long nanos) {
    if (enableClockStats) {
      this.stats.incLong(sentMessagesTimeId, nanos);
      long millis = NanoTimer.nanosToMillis(nanos);
      maxSentMessagesTime.recordMax(millis);
    }
  }

  /**
   * Returns the total number of messages broadcast by the distribution manager
   */
  @Override
  public long getBroadcastMessages() {
    return this.stats.getLong(broadcastMessagesId);
  }

  /**
   * Increments the total number of messages broadcast by the distribution manager
   */
  @Override
  public void incBroadcastMessages(long messages) {
    this.stats.incLong(broadcastMessagesId, messages);
  }

  /**
   * Returns the total number of nanoseconds spent sending messages.
   */
  @Override
  public long getBroadcastMessagesTime() {
    return this.stats.getLong(broadcastMessagesTimeId);
  }

  /**
   * Increments the total number of nanoseconds spend sending messages.
   */
  @Override
  public void incBroadcastMessagesTime(long nanos) {
    if (enableClockStats) {
      this.stats.incLong(broadcastMessagesTimeId, nanos);
    }
  }

  /**
   * Returns the total number of messages received by the distribution manager
   */
  @Override
  public long getReceivedMessages() {
    return this.stats.getLong(receivedMessagesId);
  }

  /**
   * Increments the total number of messages received by the distribution manager
   */
  @Override
  public void incReceivedMessages(long messages) {
    this.stats.incLong(receivedMessagesId, messages);
  }

  /**
   * Returns the total number of bytes received by the distribution manager
   */
  @Override
  public long getReceivedBytes() {
    return this.stats.getLong(receivedBytesId);
  }

  @Override
  public void incReceivedBytes(long bytes) {
    this.stats.incLong(receivedBytesId, bytes);
  }

  @Override
  public void incSentBytes(long bytes) {
    this.stats.incLong(sentBytesId, bytes);
  }

  /**
   * Returns the total number of messages processed by the distribution manager
   */
  @Override
  public long getProcessedMessages() {
    return this.stats.getLong(processedMessagesId);
  }

  /**
   * Increments the total number of messages processed by the distribution manager
   */
  @Override
  public void incProcessedMessages(long messages) {
    this.stats.incLong(processedMessagesId, messages);
  }

  /**
   * Returns the total number of nanoseconds spent processing messages.
   */
  @Override
  public long getProcessedMessagesTime() {
    return this.stats.getLong(processedMessagesTimeId);
  }

  /**
   * Increments the total number of nanoseconds spend processing messages.
   */
  @Override
  public void incProcessedMessagesTime(long start) {
    if (enableClockStats) {
      this.stats.incLong(processedMessagesTimeId, getTime() - start);
    }
  }

  /**
   * Returns the total number of nanoseconds spent scheduling messages to be processed.
   */
  @Override
  public long getMessageProcessingScheduleTime() {
    return this.stats.getLong(messageProcessingScheduleTimeId);
  }

  /**
   * Increments the total number of nanoseconds spent scheduling messages to be processed.
   */
  @Override
  public void incMessageProcessingScheduleTime(long elapsed) {
    if (enableClockStats) {
      this.stats.incLong(messageProcessingScheduleTimeId, elapsed);
    }
  }

  @Override
  public int getOverflowQueueSize() {
    return this.stats.getInt(overflowQueueSizeId);
  }

  @Override
  public void incOverflowQueueSize(int messages) {
    this.stats.incInt(overflowQueueSizeId, messages);
  }

  protected void incWaitingQueueSize(int messages) {
    this.stats.incInt(waitingQueueSizeId, messages);
  }

  protected void incOverflowQueueThrottleCount(int delays) {
    this.stats.incInt(overflowQueueThrottleCountId, delays);
  }

  protected void incOverflowQueueThrottleTime(long nanos) {
    if (enableClockStats) {
      this.stats.incLong(overflowQueueThrottleTimeId, nanos);
    }
  }

  protected void incHighPriorityQueueSize(int messages) {
    this.stats.incInt(highPriorityQueueSizeId, messages);
  }

  protected void incHighPriorityQueueThrottleCount(int delays) {
    this.stats.incInt(highPriorityQueueThrottleCountId, delays);
  }

  protected void incHighPriorityQueueThrottleTime(long nanos) {
    if (enableClockStats) {
      this.stats.incLong(highPriorityQueueThrottleTimeId, nanos);
    }
  }

  protected void incPartitionedRegionQueueSize(int messages) {
    this.stats.incInt(partitionedRegionQueueSizeId, messages);
  }

  protected void incPartitionedRegionQueueThrottleCount(int delays) {
    this.stats.incInt(partitionedRegionQueueThrottleCountId, delays);
  }

  protected void incPartitionedRegionQueueThrottleTime(long nanos) {
    if (enableClockStats) {
      this.stats.incLong(partitionedRegionQueueThrottleTimeId, nanos);
    }
  }

  protected void incFunctionExecutionQueueSize(int messages) {
    this.stats.incInt(functionExecutionQueueSizeId, messages);
  }

  protected void incFunctionExecutionQueueThrottleCount(int delays) {
    this.stats.incInt(functionExecutionQueueThrottleCountId, delays);
  }

  protected void incFunctionExecutionQueueThrottleTime(long nanos) {
    if (enableClockStats) {
      this.stats.incLong(functionExecutionQueueThrottleTimeId, nanos);
    }
  }

  protected void incSerialQueueSize(int messages) {
    this.stats.incInt(serialQueueSizeId, messages);
  }

  protected void incSerialQueueBytes(int amount) {
    serialQueueBytes.add(amount);
    this.stats.incInt(serialQueueBytesId, amount);
  }

  public long getInternalSerialQueueBytes() {
    return serialQueueBytes.longValue();
  }

  protected void incSerialPooledThread() {
    this.stats.incInt(serialPooledThreadId, 1);
  }

  protected void incSerialQueueThrottleCount(int delays) {
    this.stats.incInt(serialQueueThrottleCountId, delays);
  }

  protected void incSerialQueueThrottleTime(long nanos) {
    if (enableClockStats) {
      this.stats.incLong(serialQueueThrottleTimeId, nanos);
    }
  }

  @Override
  public int getNumProcessingThreads() {
    return this.stats.getInt(processingThreadsId);
  }

  @Override
  public void incNumProcessingThreads(int threads) {
    this.stats.incInt(processingThreadsId, threads);
  }

  @Override
  public int getNumSerialThreads() {
    return this.stats.getInt(serialThreadsId);
  }

  @Override
  public void incNumSerialThreads(int threads) {
    this.stats.incInt(serialThreadsId, threads);
  }

  protected void incWaitingThreads(int threads) {
    this.stats.incInt(waitingThreadsId, threads);
  }

  protected void incHighPriorityThreads(int threads) {
    this.stats.incInt(highPriorityThreadsId, threads);
  }

  protected void incPartitionedRegionThreads(int threads) {
    this.stats.incInt(partitionedRegionThreadsId, threads);
  }

  protected void incFunctionExecutionThreads(int threads) {
    this.stats.incInt(functionExecutionThreadsId, threads);
  }

  @Override
  public void incMessageChannelTime(long delta) {
    if (enableClockStats) {
      this.stats.incLong(messageChannelTimeId, delta);
    }
  }

  @Override
  public long startUDPDispatchRequest() {
    return getTime();
  }

  @Override
  public void endUDPDispatchRequest(long start) {
    if (enableClockStats) {
      this.stats.incLong(udpDispatchRequestTimeId, getTime() - start);
    }
  }

  @Override
  public long getUDPDispatchRequestTime() {
    return this.stats.getLong(udpDispatchRequestTimeId);
  }

  @Override
  public long getReplyMessageTime() {
    return this.stats.getLong(replyMessageTimeId);
  }

  @Override
  public void incReplyMessageTime(long val) {
    if (enableClockStats) {
      this.stats.incLong(replyMessageTimeId, val);
    }
  }

  @Override
  public long getDistributeMessageTime() {
    return this.stats.getLong(distributeMessageTimeId);
  }

  @Override
  public void incDistributeMessageTime(long val) {
    if (enableClockStats) {
      this.stats.incLong(distributeMessageTimeId, val);
    }
  }

  @Override
  public int getNodes() {
    return this.stats.getInt(nodesId);
  }

  @Override
  public void setNodes(int val) {
    this.stats.setInt(nodesId, val);
  }

  @Override
  public void incNodes(int val) {
    this.stats.incInt(nodesId, val);
  }

  @Override
  public int getReplyWaitsInProgress() {
    return stats.getInt(replyWaitsInProgressId);
  }

  @Override
  public int getReplyWaitsCompleted() {
    return stats.getInt(replyWaitsCompletedId);
  }

  @Override
  public long getReplyWaitTime() {
    return stats.getLong(replyWaitTimeId);
  }

  @Override
  public long startSocketWrite(boolean sync) {
    if (sync) {
      stats.incInt(syncSocketWritesInProgressId, 1);
    } else {
      stats.incInt(asyncSocketWritesInProgressId, 1);
    }
    return getTime();
  }

  @Override
  public void endSocketWrite(boolean sync, long start, int bytesWritten, int retries) {
    final long now = getTime();
    if (sync) {
      stats.incInt(syncSocketWritesInProgressId, -1);
      stats.incInt(syncSocketWritesId, 1);
      stats.incLong(syncSocketWriteBytesId, bytesWritten);
      if (enableClockStats) {
        stats.incLong(syncSocketWriteTimeId, now - start);
      }
    } else {
      stats.incInt(asyncSocketWritesInProgressId, -1);
      stats.incInt(asyncSocketWritesId, 1);
      if (retries != 0) {
        stats.incInt(asyncSocketWriteRetriesId, retries);
      }
      stats.incLong(asyncSocketWriteBytesId, bytesWritten);
      if (enableClockStats) {
        stats.incLong(asyncSocketWriteTimeId, now - start);
      }
    }
  }

  @Override
  public long startSocketLock() {
    stats.incInt(socketLocksInProgressId, 1);
    return getTime();
  }

  @Override
  public void endSocketLock(long start) {
    long ts = getTime();
    stats.incInt(socketLocksInProgressId, -1);
    stats.incInt(socketLocksId, 1);
    stats.incLong(socketLockTimeId, ts - start);
  }

  @Override
  public long startBufferAcquire() {
    stats.incInt(bufferAcquiresInProgressId, 1);
    return getTime();
  }

  @Override
  public void endBufferAcquire(long start) {
    long ts = getTime();
    stats.incInt(bufferAcquiresInProgressId, -1);
    stats.incInt(bufferAcquiresId, 1);
    stats.incLong(bufferAcquireTimeId, ts - start);
  }

  @Override
  public void incUcastWriteBytes(int bytesWritten) {
    stats.incInt(ucastWritesId, 1);
    stats.incLong(ucastWriteBytesId, bytesWritten);
  }

  @Override
  public void incMcastWriteBytes(int bytesWritten) {
    stats.incInt(mcastWritesId, 1);
    stats.incLong(mcastWriteBytesId, bytesWritten);
  }

  @Override
  public int getMcastWrites() {
    return stats.getInt(mcastWritesId);
  }

  @Override
  public int getMcastReads() {
    return stats.getInt(mcastReadsId);
  }

  @Override
  public long getUDPMsgDecryptionTime() {
    return stats.getLong(udpMsgDecryptionTimeId);
  }

  @Override
  public long getUDPMsgEncryptionTiime() {
    return stats.getLong(udpMsgEncryptionTimeId);
  }

  @Override
  public void incMcastReadBytes(int amount) {
    stats.incInt(mcastReadsId, 1);
    stats.incLong(mcastReadBytesId, amount);
  }

  @Override
  public void incUcastReadBytes(int amount) {
    stats.incInt(ucastReadsId, 1);
    stats.incLong(ucastReadBytesId, amount);
  }

  @Override
  public long startSerialization() {
    return getTime();
  }

  @Override
  public void endSerialization(long start, int bytes) {
    if (enableClockStats) {
      stats.incLong(serializationTimeId, getTime() - start);
    }
    stats.incInt(serializationsId, 1);
    stats.incLong(serializedBytesId, bytes);
  }

  @Override
  public long startPdxInstanceDeserialization() {
    return getTime();
  }

  @Override
  public void endPdxInstanceDeserialization(long start) {
    if (enableClockStats) {
      stats.incLong(pdxInstanceDeserializationTimeId, getTime() - start);
    }
    stats.incInt(pdxInstanceDeserializationsId, 1);
  }

  @Override
  public void incPdxSerialization(int bytes) {
    stats.incInt(pdxSerializationsId, 1);
    stats.incLong(pdxSerializedBytesId, bytes);
  }

  @Override
  public void incPdxDeserialization(int bytes) {
    stats.incInt(pdxDeserializationsId, 1);
    stats.incLong(pdxDeserializedBytesId, bytes);
  }

  @Override
  public void incPdxInstanceCreations() {
    stats.incInt(pdxInstanceCreationsId, 1);
  }

  @Override
  public long startDeserialization() {
    return getTime();
  }

  @Override
  public void endDeserialization(long start, int bytes) {
    if (enableClockStats) {
      stats.incLong(deserializationTimeId, getTime() - start);
    }
    stats.incInt(deserializationsId, 1);
    stats.incLong(deserializedBytesId, bytes);
  }

  @Override
  public long startMsgSerialization() {
    return getTime();
  }

  @Override
  public void endMsgSerialization(long start) {
    if (enableClockStats) {
      stats.incLong(msgSerializationTimeId, getTime() - start);
    }
  }

  @Override
  public long startUDPMsgEncryption() {
    return getTime();
  }

  @Override
  public void endUDPMsgEncryption(long start) {
    if (enableClockStats) {
      stats.incLong(udpMsgEncryptionTimeId, getTime() - start);
    }
  }

  @Override
  public long startMsgDeserialization() {
    return getTime();
  }

  @Override
  public void endMsgDeserialization(long start) {
    if (enableClockStats) {
      stats.incLong(msgDeserializationTimeId, getTime() - start);
    }
  }

  @Override
  public long startUDPMsgDecryption() {
    return getTime();
  }

  @Override
  public void endUDPMsgDecryption(long start) {
    if (enableClockStats) {
      stats.incLong(udpMsgDecryptionTimeId, getTime() - start);
    }
  }

  /**
   * @return the timestamp that marks the start of the operation
   */
  @Override
  public long startReplyWait() {
    stats.incInt(replyWaitsInProgressId, 1);
    return getTime();
  }


  @Override
  public void endReplyWait(long startNanos, long initTime) {
    if (enableClockStats) {
      stats.incLong(replyWaitTimeId, getTime() - startNanos);
      // this.replyWaitHistogram.endOp(delta);
    }
    if (initTime != 0) {
      long waitTime = System.currentTimeMillis() - initTime;
      maxReplyWaitTime.recordMax(waitTime);
    }
    stats.incInt(replyWaitsInProgressId, -1);
    stats.incInt(replyWaitsCompletedId, 1);

    Breadcrumbs.setSendSide(null); // clear any recipient breadcrumbs set by the message
    Breadcrumbs.setProblem(null); // clear out reply-wait errors
  }

  @Override
  public void incReplyTimeouts() {
    stats.incLong(replyTimeoutsId, 1L);
  }

  @Override
  public long getReplyTimeouts() {
    return stats.getLong(replyTimeoutsId);
  }

  @Override
  public void incReceivers() {
    stats.incInt(receiverConnectionsId, 1);
  }

  @Override
  public void decReceivers() {
    stats.incInt(receiverConnectionsId, -1);
  }

  @Override
  public void incFailedAccept() {
    stats.incInt(failedAcceptsId, 1);
  }

  @Override
  public void incFailedConnect() {
    stats.incInt(failedConnectsId, 1);
  }

  @Override
  public void incReconnectAttempts() {
    stats.incInt(reconnectAttemptsId, 1);
  }

  @Override
  public int getReconnectAttempts() {
    return stats.getInt(reconnectAttemptsId);
  }

  @Override
  public void incLostLease() {
    stats.incInt(lostConnectionLeaseId, 1);
  }

  @Override
  public long startSenderCreate() {
    stats.incLong(senderCreatesInProgressId, 1);
    return getTime();
  }

  @Override
  public void incSenders(boolean shared, boolean preserveOrder, long start) {
    if (shared) {
      if (preserveOrder) {
        stats.incInt(sharedOrderedSenderConnectionsId, 1);
      } else {
        stats.incInt(sharedUnorderedSenderConnectionsId, 1);
      }
    } else {
      if (preserveOrder) {
        stats.incInt(threadOrderedSenderConnectionsId, 1);
      } else {
        stats.incInt(threadUnorderedSenderConnectionsId, 1);
      }
    }
    if (enableClockStats) {
      stats.incLong(senderCreateTimeId, getTime() - start);
    }
    stats.incLong(senderCreatesInProgressId, -1);
  }

  @Override
  public int getSendersSU() {
    return stats.getInt(sharedUnorderedSenderConnectionsId);
  }

  @Override
  public void decSenders(boolean shared, boolean preserveOrder) {
    if (shared) {
      if (preserveOrder) {
        stats.incInt(sharedOrderedSenderConnectionsId, -1);
      } else {
        stats.incInt(sharedUnorderedSenderConnectionsId, -1);
      }
    } else {
      if (preserveOrder) {
        stats.incInt(threadOrderedSenderConnectionsId, -1);
      } else {
        stats.incInt(threadUnorderedSenderConnectionsId, -1);
      }
    }
  }

  @Override
  public int getAsyncSocketWritesInProgress() {
    return stats.getInt(asyncSocketWritesInProgressId);
  }

  @Override
  public int getAsyncSocketWrites() {
    return stats.getInt(asyncSocketWritesId);
  }

  @Override
  public int getAsyncSocketWriteRetries() {
    return stats.getInt(asyncSocketWriteRetriesId);
  }

  @Override
  public long getAsyncSocketWriteBytes() {
    return stats.getLong(asyncSocketWriteBytesId);
  }

  @Override
  public long getAsyncSocketWriteTime() {
    return stats.getLong(asyncSocketWriteTimeId);
  }

  @Override
  public long getAsyncQueueAddTime() {
    return stats.getLong(asyncQueueAddTimeId);
  }

  @Override
  public void incAsyncQueueAddTime(long inc) {
    if (enableClockStats) {
      stats.incLong(asyncQueueAddTimeId, inc);
    }
  }

  @Override
  public long getAsyncQueueRemoveTime() {
    return stats.getLong(asyncQueueRemoveTimeId);
  }

  @Override
  public void incAsyncQueueRemoveTime(long inc) {
    if (enableClockStats) {
      stats.incLong(asyncQueueRemoveTimeId, inc);
    }
  }

  @Override
  public int getAsyncQueues() {
    return stats.getInt(asyncQueuesId);
  }

  @Override
  public void incAsyncQueues(int inc) {
    stats.incInt(asyncQueuesId, inc);
  }

  @Override
  public int getAsyncQueueFlushesInProgress() {
    return stats.getInt(asyncQueueFlushesInProgressId);
  }

  @Override
  public int getAsyncQueueFlushesCompleted() {
    return stats.getInt(asyncQueueFlushesCompletedId);
  }

  @Override
  public long getAsyncQueueFlushTime() {
    return stats.getLong(asyncQueueFlushTimeId);
  }

  @Override
  public long startAsyncQueueFlush() {
    stats.incInt(asyncQueueFlushesInProgressId, 1);
    return getTime();
  }

  @Override
  public void endAsyncQueueFlush(long start) {
    stats.incInt(asyncQueueFlushesInProgressId, -1);
    stats.incInt(asyncQueueFlushesCompletedId, 1);
    if (enableClockStats) {
      stats.incLong(asyncQueueFlushTimeId, getTime() - start);
    }
  }

  @Override
  public int getAsyncQueueTimeouts() {
    return stats.getInt(asyncQueueTimeoutExceededId);
  }

  @Override
  public void incAsyncQueueTimeouts(int inc) {
    stats.incInt(asyncQueueTimeoutExceededId, inc);
  }

  @Override
  public int getAsyncQueueSizeExceeded() {
    return stats.getInt(asyncQueueSizeExceededId);
  }

  @Override
  public void incAsyncQueueSizeExceeded(int inc) {
    stats.incInt(asyncQueueSizeExceededId, inc);
  }

  @Override
  public int getAsyncDistributionTimeoutExceeded() {
    return stats.getInt(asyncDistributionTimeoutExceededId);
  }

  @Override
  public void incAsyncDistributionTimeoutExceeded() {
    stats.incInt(asyncDistributionTimeoutExceededId, 1);
  }

  @Override
  public long getAsyncQueueSize() {
    return stats.getLong(asyncQueueSizeId);
  }

  @Override
  public void incAsyncQueueSize(long inc) {
    stats.incLong(asyncQueueSizeId, inc);
  }

  @Override
  public long getAsyncQueuedMsgs() {
    return stats.getLong(asyncQueuedMsgsId);
  }

  @Override
  public void incAsyncQueuedMsgs() {
    stats.incLong(asyncQueuedMsgsId, 1);
  }

  @Override
  public long getAsyncDequeuedMsgs() {
    return stats.getLong(asyncDequeuedMsgsId);
  }

  @Override
  public void incAsyncDequeuedMsgs() {
    stats.incLong(asyncDequeuedMsgsId, 1);
  }

  @Override
  public long getAsyncConflatedMsgs() {
    return stats.getLong(asyncConflatedMsgsId);
  }

  @Override
  public void incAsyncConflatedMsgs() {
    stats.incLong(asyncConflatedMsgsId, 1);
  }

  @Override
  public int getAsyncThreads() {
    return stats.getInt(asyncThreadsId);
  }

  @Override
  public void incAsyncThreads(int inc) {
    stats.incInt(asyncThreadsId, inc);
  }

  @Override
  public int getAsyncThreadInProgress() {
    return stats.getInt(asyncThreadInProgressId);
  }

  @Override
  public int getAsyncThreadCompleted() {
    return stats.getInt(asyncThreadCompletedId);
  }

  @Override
  public long getAsyncThreadTime() {
    return stats.getLong(asyncThreadTimeId);
  }

  @Override
  public long startAsyncThread() {
    stats.incInt(asyncThreadInProgressId, 1);
    return getTime();
  }

  @Override
  public void endAsyncThread(long start) {
    stats.incInt(asyncThreadInProgressId, -1);
    stats.incInt(asyncThreadCompletedId, 1);
    if (enableClockStats) {
      stats.incLong(asyncThreadTimeId, getTime() - start);
    }
  }

  /**
   * Returns a helper object so that the overflow queue can record its stats to the proper
   * distribution stats.
   *
   * @since GemFire 3.5
   */
  public ThrottledQueueStatHelper getOverflowQueueHelper() {
    return new ThrottledQueueStatHelper() {
      @Override
      public void incThrottleCount() {
        incOverflowQueueThrottleCount(1);
      }

      @Override
      public void throttleTime(long nanos) {
        incOverflowQueueThrottleTime(nanos);
      }

      @Override
      public void add() {
        incOverflowQueueSize(1);
      }

      @Override
      public void remove() {
        incOverflowQueueSize(-1);
      }

      @Override
      public void remove(int count) {
        incOverflowQueueSize(-count);
      }
    };
  }

  /**
   * Returns a helper object so that the waiting queue can record its stats to the proper
   * distribution stats.
   *
   * @since GemFire 3.5
   */
  public QueueStatHelper getWaitingQueueHelper() {
    return new QueueStatHelper() {
      @Override
      public void add() {
        incWaitingQueueSize(1);
      }

      @Override
      public void remove() {
        incWaitingQueueSize(-1);
      }

      @Override
      public void remove(int count) {
        incWaitingQueueSize(-count);
      }
    };
  }

  /**
   * Returns a helper object so that the high priority queue can record its stats to the proper
   * distribution stats.
   *
   * @since GemFire 3.5
   */
  public ThrottledQueueStatHelper getHighPriorityQueueHelper() {
    return new ThrottledQueueStatHelper() {
      @Override
      public void incThrottleCount() {
        incHighPriorityQueueThrottleCount(1);
      }

      @Override
      public void throttleTime(long nanos) {
        incHighPriorityQueueThrottleTime(nanos);
      }

      @Override
      public void add() {
        incHighPriorityQueueSize(1);
      }

      @Override
      public void remove() {
        incHighPriorityQueueSize(-1);
      }

      @Override
      public void remove(int count) {
        incHighPriorityQueueSize(-count);
      }
    };
  }

  /**
   * Returns a helper object so that the partitioned region queue can record its stats to the proper
   * distribution stats.
   *
   * @since GemFire 5.0
   */
  public ThrottledQueueStatHelper getPartitionedRegionQueueHelper() {
    return new ThrottledQueueStatHelper() {
      @Override
      public void incThrottleCount() {
        incPartitionedRegionQueueThrottleCount(1);
      }

      @Override
      public void throttleTime(long nanos) {
        incPartitionedRegionQueueThrottleTime(nanos);
      }

      @Override
      public void add() {
        incPartitionedRegionQueueSize(1);
      }

      @Override
      public void remove() {
        incPartitionedRegionQueueSize(-1);
      }

      @Override
      public void remove(int count) {
        incPartitionedRegionQueueSize(-count);
      }
    };
  }

  /**
   * Returns a helper object so that the partitioned region pool can record its stats to the proper
   * distribution stats.
   *
   * @since GemFire 5.0.2
   */
  public PoolStatHelper getPartitionedRegionPoolHelper() {
    return new PoolStatHelper() {
      @Override
      public void startJob() {
        incPartitionedRegionThreadJobs(1);
      }

      @Override
      public void endJob() {
        incPartitionedRegionThreadJobs(-1);
      }
    };
  }

  /**
   * Returns a helper object so that the function execution queue can record its stats to the proper
   * distribution stats.
   *
   * @since GemFire 6.0
   */
  public ThrottledQueueStatHelper getFunctionExecutionQueueHelper() {
    return new ThrottledQueueStatHelper() {
      @Override
      public void incThrottleCount() {
        incFunctionExecutionQueueThrottleCount(1);
      }

      @Override
      public void throttleTime(long nanos) {
        incFunctionExecutionQueueThrottleTime(nanos);
      }

      @Override
      public void add() {
        incFunctionExecutionQueueSize(1);
      }

      @Override
      public void remove() {
        incFunctionExecutionQueueSize(-1);
      }

      @Override
      public void remove(int count) {
        incFunctionExecutionQueueSize(-count);
      }
    };
  }

  /**
   * Returns a helper object so that the function execution pool can record its stats to the proper
   * distribution stats.
   *
   * @since GemFire 6.0
   */
  public PoolStatHelper getFunctionExecutionPoolHelper() {
    return new PoolStatHelper() {
      @Override
      public void startJob() {
        incFunctionExecutionThreadJobs(1);
      }

      @Override
      public void endJob() {
        incFunctionExecutionThreadJobs(-1);
      }
    };
  }

  /**
   * Returns a helper object so that the serial queue can record its stats to the proper
   * distribution stats.
   *
   * @since GemFire 3.5
   */
  public ThrottledMemQueueStatHelper getSerialQueueHelper() {
    return new ThrottledMemQueueStatHelper() {
      @Override
      public void incThrottleCount() {
        incSerialQueueThrottleCount(1);
      }

      @Override
      public void throttleTime(long nanos) {
        incSerialQueueThrottleTime(nanos);
      }

      @Override
      public void add() {
        incSerialQueueSize(1);
      }

      @Override
      public void remove() {
        incSerialQueueSize(-1);
      }

      @Override
      public void remove(int count) {
        incSerialQueueSize(-count);
      }

      @Override
      public void addMem(int amount) {
        incSerialQueueBytes(amount);
      }

      @Override
      public void removeMem(int amount) {
        incSerialQueueBytes(amount * (-1));
      }
    };
  }

  /**
   * Returns a helper object so that the normal pool can record its stats to the proper distribution
   * stats.
   *
   * @since GemFire 3.5
   */
  public PoolStatHelper getNormalPoolHelper() {
    return new PoolStatHelper() {
      @Override
      public void startJob() {
        incNormalPoolThreadJobs(1);
      }

      @Override
      public void endJob() {
        incNormalPoolThreadJobs(-1);
      }
    };
  }

  /**
   * Returns a helper object so that the waiting pool can record its stats to the proper
   * distribution stats.
   *
   * @since GemFire 3.5
   */
  public PoolStatHelper getWaitingPoolHelper() {
    return new PoolStatHelper() {
      @Override
      public void startJob() {
        incWaitingPoolThreadJobs(1);
      }

      @Override
      public void endJob() {
        incWaitingPoolThreadJobs(-1);
      }
    };
  }

  /**
   * Returns a helper object so that the highPriority pool can record its stats to the proper
   * distribution stats.
   *
   * @since GemFire 3.5
   */
  public PoolStatHelper getHighPriorityPoolHelper() {
    return new PoolStatHelper() {
      @Override
      public void startJob() {
        incHighPriorityThreadJobs(1);
      }

      @Override
      public void endJob() {
        incHighPriorityThreadJobs(-1);
      }
    };
  }

  @Override
  public void incBatchSendTime(long start) {
    if (enableClockStats) {
      stats.incLong(batchSendTimeId, getTime() - start);
    }
  }

  @Override
  public void incBatchCopyTime(long start) {
    if (enableClockStats) {
      stats.incLong(batchCopyTimeId, getTime() - start);
    }
  }

  @Override
  public void incBatchWaitTime(long start) {
    if (enableClockStats) {
      stats.incLong(batchWaitTimeId, getTime() - start);
    }
  }

  @Override
  public void incBatchFlushTime(long start) {
    if (enableClockStats) {
      stats.incLong(batchFlushTimeId, getTime() - start);
    }
  }

  @Override
  public void incUcastRetransmits() {
    stats.incInt(ucastRetransmitsId, 1);
  }

  @Override
  public void incMcastRetransmits() {
    stats.incInt(mcastRetransmitsId, 1);
  }

  @Override
  public void incMcastRetransmitRequests() {
    stats.incInt(mcastRetransmitRequestsId, 1);
  }

  @Override
  public int getMcastRetransmits() {
    return stats.getInt(mcastRetransmitsId);
  }

  @Override
  public void incThreadOwnedReceivers(long value, int dominoCount) {
    if (dominoCount < 2) {
      stats.incLong(threadOwnedReceiversId, value);
    } else {
      stats.incLong(threadOwnedReceiversId2, value);
    }
  }

  /**
   * @since GemFire 5.0.2.4
   */
  @Override
  public void incReceiverBufferSize(int inc, boolean direct) {
    if (direct) {
      stats.incLong(receiverDirectBufferSizeId, inc);
    } else {
      stats.incLong(receiverHeapBufferSizeId, inc);
    }
  }

  /**
   * @since GemFire 5.0.2.4
   */
  @Override
  public void incSenderBufferSize(int inc, boolean direct) {
    if (direct) {
      stats.incLong(senderDirectBufferSizeId, inc);
    } else {
      stats.incLong(senderHeapBufferSizeId, inc);
    }
  }

  @Override
  public void incMessagesBeingReceived(boolean newMsg, int bytes) {
    if (newMsg) {
      stats.incInt(messagesBeingReceivedId, 1);
    }
    stats.incLong(messageBytesBeingReceivedId, bytes);
  }

  @Override
  public void decMessagesBeingReceived(int bytes) {
    stats.incInt(messagesBeingReceivedId, -1);
    stats.incLong(messageBytesBeingReceivedId, -bytes);
  }

  public void incSerialThreadStarts() {
    stats.incLong(serialThreadStartsId, 1);
  }

  public void incProcessingThreadStarts() {
    stats.incLong(processingThreadStartsId, 1);
  }

  public void incHighPriorityThreadStarts() {
    stats.incLong(highPriorityThreadStartsId, 1);
  }

  public void incWaitingThreadStarts() {
    stats.incLong(waitingThreadStartsId, 1);
  }

  public void incPartitionedRegionThreadStarts() {
    stats.incLong(partitionedRegionThreadStartsId, 1);
  }

  public void incFunctionExecutionThreadStarts() {
    stats.incLong(functionExecutionThreadStartsId, 1);
  }

  public void incSerialPooledThreadStarts() {
    stats.incLong(serialPooledThreadStartsId, 1);
  }

  @Override
  public void incReplyHandOffTime(long start) {
    if (enableClockStats) {
      long delta = getTime() - start;
      stats.incLong(replyHandoffTimeId, delta);
      // this.replyHandoffHistogram.endOp(delta);
    }
  }

  protected void incPartitionedRegionThreadJobs(int i) {
    this.stats.incInt(partitionedRegionThreadJobsId, i);
  }

  protected void incFunctionExecutionThreadJobs(int i) {
    this.stats.incInt(functionExecutionThreadJobsId, i);
  }

  public PoolStatHelper getSerialProcessorHelper() {
    return new PoolStatHelper() {
      @Override
      public void startJob() {
        incNumSerialThreadJobs(1);
        if (logger.isTraceEnabled()) {
          logger.trace("[DM.SerialQueuedExecutor.execute] numSerialThreads={}",
              getNumSerialThreads());
        }
      }

      @Override
      public void endJob() {
        incNumSerialThreadJobs(-1);
      }
    };
  }

  protected void incNumSerialThreadJobs(int jobs) {
    this.stats.incInt(serialThreadJobsId, jobs);
  }

  public PoolStatHelper getSerialPooledProcessorHelper() {
    return new PoolStatHelper() {
      @Override
      public void startJob() {
        incSerialPooledProcessorThreadJobs(1);
      }

      @Override
      public void endJob() {
        incSerialPooledProcessorThreadJobs(-1);
      }
    };
  }

  protected void incSerialPooledProcessorThreadJobs(int jobs) {
    this.stats.incInt(serialPooledThreadJobsId, jobs);
  }

  protected void incNormalPoolThreadJobs(int jobs) {
    this.stats.incInt(pooledMessageThreadJobsId, jobs);
  }

  protected void incHighPriorityThreadJobs(int jobs) {
    this.stats.incInt(highPriorityThreadJobsId, jobs);
  }

  protected void incWaitingPoolThreadJobs(int jobs) {
    this.stats.incInt(waitingPoolThreadJobsId, jobs);
  }

  @Override
  public int getElders() {
    return this.stats.getInt(eldersId);
  }

  @Override
  public void incElders(int val) {
    this.stats.incInt(eldersId, val);
  }

  @Override
  public int getInitialImageMessagesInFlight() {
    return this.stats.getInt(initialImageMessagesInFlightId);
  }

  @Override
  public void incInitialImageMessagesInFlight(int val) {
    this.stats.incInt(initialImageMessagesInFlightId, val);
  }

  @Override
  public int getInitialImageRequestsInProgress() {
    return this.stats.getInt(initialImageRequestsInProgressId);
  }

  @Override
  public void incInitialImageRequestsInProgress(int val) {
    this.stats.incInt(initialImageRequestsInProgressId, val);
  }

  public Statistics getStats() {
    return stats;
  }

  // For GMSHealthMonitor
  @Override
  public long getHeartbeatRequestsSent() {
    return this.stats.getLong(heartbeatRequestsSentId);
  }

  @Override
  public void incHeartbeatRequestsSent() {
    this.stats.incLong(heartbeatRequestsSentId, 1L);
  }

  @Override
  public long getHeartbeatRequestsReceived() {
    return this.stats.getLong(heartbeatRequestsReceivedId);
  }

  @Override
  public void incHeartbeatRequestsReceived() {
    this.stats.incLong(heartbeatRequestsReceivedId, 1L);
  }

  @Override
  public long getHeartbeatsSent() {
    return this.stats.getLong(heartbeatsSentId);
  }

  @Override
  public void incHeartbeatsSent() {
    this.stats.incLong(heartbeatsSentId, 1L);
  }

  @Override
  public long getHeartbeatsReceived() {
    return this.stats.getLong(heartbeatsReceivedId);
  }

  @Override
  public void incHeartbeatsReceived() {
    this.stats.incLong(heartbeatsReceivedId, 1L);
  }

  @Override
  public long getSuspectsSent() {
    return this.stats.getLong(suspectsSentId);
  }

  @Override
  public void incSuspectsSent() {
    this.stats.incLong(suspectsSentId, 1L);
  }

  @Override
  public long getSuspectsReceived() {
    return this.stats.getLong(suspectsReceivedId);
  }

  @Override
  public void incSuspectsReceived() {
    this.stats.incLong(suspectsReceivedId, 1L);
  }

  @Override
  public long getFinalCheckRequestsSent() {
    return this.stats.getLong(finalCheckRequestsSentId);
  }

  @Override
  public void incFinalCheckRequestsSent() {
    this.stats.incLong(finalCheckRequestsSentId, 1L);
  }

  @Override
  public long getFinalCheckRequestsReceived() {
    return this.stats.getLong(finalCheckRequestsReceivedId);
  }

  @Override
  public void incFinalCheckRequestsReceived() {
    this.stats.incLong(finalCheckRequestsReceivedId, 1L);
  }

  @Override
  public long getFinalCheckResponsesSent() {
    return this.stats.getLong(finalCheckResponsesSentId);
  }

  @Override
  public void incFinalCheckResponsesSent() {
    this.stats.incLong(finalCheckResponsesSentId, 1L);
  }

  @Override
  public long getFinalCheckResponsesReceived() {
    return this.stats.getLong(finalCheckResponsesReceivedId);
  }

  @Override
  public void incFinalCheckResponsesReceived() {
    this.stats.incLong(finalCheckResponsesReceivedId, 1L);
  }

  ///
  @Override
  public long getTcpFinalCheckRequestsSent() {
    return this.stats.getLong(tcpFinalCheckRequestsSentId);
  }

  @Override
  public void incTcpFinalCheckRequestsSent() {
    this.stats.incLong(tcpFinalCheckRequestsSentId, 1L);
  }

  @Override
  public long getTcpFinalCheckRequestsReceived() {
    return this.stats.getLong(tcpFinalCheckRequestsReceivedId);
  }

  @Override
  public void incTcpFinalCheckRequestsReceived() {
    this.stats.incLong(tcpFinalCheckRequestsReceivedId, 1L);
  }

  @Override
  public long getTcpFinalCheckResponsesSent() {
    return this.stats.getLong(tcpFinalCheckResponsesSentId);
  }

  @Override
  public void incTcpFinalCheckResponsesSent() {
    this.stats.incLong(tcpFinalCheckResponsesSentId, 1L);
  }

  @Override
  public long getTcpFinalCheckResponsesReceived() {
    return this.stats.getLong(tcpFinalCheckResponsesReceivedId);
  }

  @Override
  public void incTcpFinalCheckResponsesReceived() {
    this.stats.incLong(tcpFinalCheckResponsesReceivedId, 1L);
  }

  ///
  @Override
  public long getUdpFinalCheckRequestsSent() {
    return this.stats.getLong(udpFinalCheckRequestsSentId);
  }

  @Override
  public void incUdpFinalCheckRequestsSent() {
    this.stats.incLong(udpFinalCheckRequestsSentId, 1L);
  }

  // UDP final check is implemented using HeartbeatRequestMessage and HeartbeatMessage
  // So the following code is commented out
  // public long getUdpFinalCheckRequestsReceived() {
  // return this.stats.getLong(udpFinalCheckRequestsReceivedId);
  // }
  //
  // public void incUdpFinalCheckRequestsReceived() {
  // this.stats.incLong(udpFinalCheckRequestsReceivedId, 1L);
  // }
  //
  // public long getUdpFinalCheckResponsesSent() {
  // return this.stats.getLong(udpFinalCheckResponsesSentId);
  // }
  //
  // public void incUdpFinalCheckResponsesSent() {
  // this.stats.incLong(udpFinalCheckResponsesSentId, 1L);
  // }

  @Override
  public long getUdpFinalCheckResponsesReceived() {
    return this.stats.getLong(udpFinalCheckResponsesReceivedId);
  }

  @Override
  public void incUdpFinalCheckResponsesReceived() {
    this.stats.incLong(udpFinalCheckResponsesReceivedId, 1L);
  }

}
