/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed.internal;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.StatisticDescriptor;
import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsFactory;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.StatisticsTypeFactory;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.NanoTimer;
import com.gemstone.gemfire.internal.StatisticsTypeFactoryImpl;
import com.gemstone.gemfire.internal.logging.LogService;
//import java.io.*;
import com.gemstone.gemfire.internal.tcp.Buffers;
import com.gemstone.gemfire.internal.util.Breadcrumbs;

/**
 * This class maintains statistics in GemFire about the distribution
 * manager and distribution in general.
 *
 * @author Darrel Schneider
 * @author Bruce Schuchardt
 *
 */
public class DistributionStats implements DMStats {
  private static final Logger logger = LogService.getLogger();
  
  public static boolean enableClockStats = false;


  //////////////////  Statistic "Id" Fields  //////////////////

  private final static StatisticsType type;
  private final static int sentMessagesId;
  private final static int sentCommitMessagesId;
  private final static int commitWaitsId;
  private final static int sentMessagesTimeId;
  private final static int sentMessagesMaxTimeId;
  private final static int broadcastMessagesId;
  private final static int broadcastMessagesTimeId;
  private final static int receivedMessagesId;
  private final static int receivedBytesId;
  private final static int sentBytesId;
  private final static int processedMessagesId;
  private final static int processedMessagesTimeId;
  private final static int messageProcessingScheduleTimeId;
  private final static int messageChannelTimeId;
  private final static int replyMessageTimeId;
  private final static int distributeMessageTimeId;
  private final static int nodesId;
  private final static int overflowQueueSizeId;
  private final static int processingThreadsId;
  private final static int serialThreadsId;
  private final static int waitingThreadsId;
  private final static int highPriorityThreadsId;
  private final static int partitionedRegionThreadsId;
  private final static int functionExecutionThreadsId;
  private final static int partitionedRegionThreadJobsId;
  private final static int functionExecutionThreadJobsId;
  private final static int waitingQueueSizeId;
  private final static int overflowQueueThrottleTimeId;
  private final static int overflowQueueThrottleCountId;
  private final static int highPriorityQueueSizeId;
  private final static int highPriorityQueueThrottleTimeId;
  private final static int highPriorityQueueThrottleCountId;
  private final static int partitionedRegionQueueSizeId;
  private final static int partitionedRegionQueueThrottleTimeId;
  private final static int partitionedRegionQueueThrottleCountId;
  private final static int functionExecutionQueueSizeId;
  private final static int functionExecutionQueueThrottleCountId;
  private final static int functionExecutionQueueThrottleTimeId;
  private final static int serialQueueSizeId;
  private final static int serialQueueBytesId;
  private final static int serialPooledThreadId;
  private final static int serialQueueThrottleTimeId;
  private final static int serialQueueThrottleCountId;
  private final static int replyWaitsInProgressId;
  private final static int replyWaitsCompletedId;
  private final static int replyWaitTimeId;
  private final static int replyTimeoutsId;
  private final static int replyWaitMaxTimeId;
  private final static int receiverConnectionsId;
  private final static int failedAcceptsId;
  private final static int failedConnectsId;
  private final static int reconnectAttemptsId;
  private final static int lostConnectionLeaseId;
  private final static int sharedOrderedSenderConnectionsId;
  private final static int sharedUnorderedSenderConnectionsId;
  private final static int threadOrderedSenderConnectionsId;
  private final static int threadUnorderedSenderConnectionsId;

  private final static int syncSocketWritesInProgressId;
  private final static int syncSocketWriteTimeId;
  private final static int syncSocketWritesId;
  private final static int syncSocketWriteBytesId;

  private final static int ucastReadsId;
  private final static int ucastReadBytesId;
  private final static int ucastWriteTimeId;
  private final static int ucastWritesId;
  private final static int ucastWriteBytesId;
  private final static int ucastRetransmitsId;

  private final static int mcastReadsId;
  private final static int mcastReadBytesId;
  private final static int mcastWriteTimeId;
  private final static int mcastWritesId;
  private final static int mcastWriteBytesId;
  private final static int mcastRetransmitsId;
  private final static int mcastRetransmitRequestsId;

  private final static int serializationTimeId;
  private final static int serializationsId;
  private final static int serializedBytesId;

  private final static int pdxSerializationsId;
  private final static int pdxSerializedBytesId;

  private final static int deserializationTimeId;
  private final static int deserializationsId;
  private final static int deserializedBytesId;
  private final static int pdxDeserializationsId;
  private final static int pdxDeserializedBytesId;
  private final static int pdxInstanceDeserializationsId;
  private final static int pdxInstanceDeserializationTimeId;
  private final static int pdxInstanceCreationsId;

  private final static int msgSerializationTimeId;
  private final static int msgDeserializationTimeId;

  private final static int batchSendTimeId;
  private final static int batchCopyTimeId;
  private final static int batchWaitTimeId;
  private final static int batchFlushTimeId;

  private final static int ucastFlushesId;
  private final static int ucastFlushTimeId;

  private final static int flowControlWaitsInProgressId;
  private final static int flowControlThrottleWaitsInProgressId;
  private final static int flowControlRequestsId;
  private final static int flowControlResponsesId;

  private final static int jgUNICASTdataReceivedTimeId;

  private final static int jgReceivedMessagesSizeId;
  private final static int jgQueuedMessagesSizeId;
  private final static int jgSentMessagesPoolSizeId;

  private final static int jgUcastReceivedMessagesSizeId;
  private final static int jgUcastSentMessagesSizeId;
  private final static int jgUcastSentHighPriorityMessagesSizeId;

  private final static int jgSTABLEsuspendTimeId;
  private final static int jgSTABLEmessagesId;
  private final static int jgSTABLEmessagesSentId;
  private final static int jgSTABILITYmessagesId;

  private final static int jgDownTimeId;
  private final static int jgUpTimeId;
  private final static int jChannelUpTimeId;

  private final static int jgFCsendBlocksId;
  private final static int jgFCautoRequestsId;
  private final static int jgFCreplenishId;
  private final static int jgFCresumesId;
  private final static int jgFCsentCreditsId;
  private final static int jgFCsentThrottleRequestsId;
  private final static int jgNAKACKwaitsId;

  private final static int threadOwnedReceiversId;
  private final static int threadOwnedReceiversId2;

  private final static int jgFragmentationsPerformedId;
  private final static int jgFragmentsCreatedId;

  private final static int asyncSocketWritesInProgressId;
  private final static int asyncSocketWritesId;
  private final static int asyncSocketWriteRetriesId;
  private final static int asyncSocketWriteTimeId;
  private final static int asyncSocketWriteBytesId;

  private final static int socketLocksInProgressId;
  private final static int socketLocksId;
  private final static int socketLockTimeId;

  private final static int bufferAcquiresInProgressId;
  private final static int bufferAcquiresId;
  private final static int bufferAcquireTimeId;

  private final static int asyncQueueAddTimeId;
  private final static int asyncQueueRemoveTimeId;

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
  private static final int viewThreadStartsId;
  private static final int processingThreadStartsId;
  private static final int highPriorityThreadStartsId;
  private static final int waitingThreadStartsId;
  private static final int partitionedRegionThreadStartsId;
  private static final int functionExecutionThreadStartsId;
  private static final int serialPooledThreadStartsId;
  private static final int TOSentMsgId;

  private static final int replyHandoffTimeId;

  private final static int viewThreadsId;
  private final static int serialThreadJobsId;
  private final static int viewProcessorThreadJobsId;
  private final static int serialPooledThreadJobsId;
  private final static int pooledMessageThreadJobsId;
  private final static int highPriorityThreadJobsId;
  private final static int waitingPoolThreadJobsId;

  private final static int eldersId;
  private final static int initialImageMessagesInFlightId;
  private final static int initialImageRequestsInProgressId;

  static {
    String statName = "DistributionStats";
    String statDescription = "Statistics on the gemfire distribution layer.";

    final String sentMessagesDesc = "The number of distribution messages that this GemFire system has sent. This includes broadcastMessages.";
    final String sentCommitMessagesDesc = "The number of transaction commit messages that this GemFire system has created to be sent. Note that it is possible for a commit to only create one message even though it will end up being sent to multiple recipients.";
    final String commitWaitsDesc = "The number of transaction commits that had to wait for a response before they could complete.";
    final String sentMessagesTimeDesc = "The total amount of time this distribution manager has spent sending messages. This includes broadcastMessagesTime.";
    final String sentMessagesMaxTimeDesc = "The highest amount of time this distribution manager has spent distributing a single message to the network.";
    final String broadcastMessagesDesc = "The number of distribution messages that this GemFire system has broadcast. A broadcast message is one sent to every other manager in the group.";
    final String broadcastMessagesTimeDesc = "The total amount of time this distribution manager has spent broadcasting messages. A broadcast message is one sent to every other manager in the group.";
    final String receivedMessagesDesc = "The number of distribution messages that this GemFire system has received.";
    final String receivedBytesDesc = "The number of distribution message bytes that this GemFire system has received.";
    final String sentBytesDesc = "The number of distribution message bytes that this GemFire system has sent.";
    final String processedMessagesDesc = "The number of distribution messages that this GemFire system has processed.";
    final String processedMessagesTimeDesc = "The amount of time this distribution manager has spent in message.process().";
    final String messageProcessingScheduleTimeDesc = "The amount of time this distribution manager has spent dispatching message to processor threads.";
    final String overflowQueueSizeDesc = "The number of normal distribution messages currently waiting to be processed.";
    final String waitingQueueSizeDesc = "The number of distribution messages currently waiting for some other resource before they can be processed.";
    final String overflowQueueThrottleTimeDesc = "The total amount of time, in nanoseconds, spent delayed by the overflow queue throttle.";
    final String overflowQueueThrottleCountDesc = "The total number of times a thread was delayed in adding a normal message to the overflow queue.";
    final String highPriorityQueueSizeDesc = "The number of high priority distribution messages currently waiting to be processed.";
    final String highPriorityQueueThrottleTimeDesc = "The total amount of time, in nanoseconds, spent delayed by the high priority queue throttle.";
    final String highPriorityQueueThrottleCountDesc = "The total number of times a thread was delayed in adding a normal message to the high priority queue.";
    final String serialQueueSizeDesc = "The number of serial distribution messages currently waiting to be processed.";
    final String serialQueueBytesDesc = "The approximate number of bytes consumed by serial distribution messages currently waiting to be processed.";
    final String serialPooledThreadDesc = "The number of threads created in the SerialQueuedExecutorPool.";
    final String serialQueueThrottleTimeDesc = "The total amount of time, in nanoseconds, spent delayed by the serial queue throttle.";
    final String serialQueueThrottleCountDesc = "The total number of times a thread was delayed in adding a ordered message to the serial queue.";
    final String serialThreadsDesc = "The number of threads currently processing serial/ordered messages.";
    final String processingThreadsDesc = "The number of threads currently processing normal messages.";
    final String highPriorityThreadsDesc = "The number of threads currently processing high priority messages.";
    final String partitionedRegionThreadsDesc = "The number of threads currently processing partitioned region messages.";
    final String functionExecutionThreadsDesc = "The number of threads currently processing function execution messages.";
    final String waitingThreadsDesc = "The number of threads currently processing messages that had to wait for a resource.";
    final String messageChannelTimeDesc = "The total amount of time received messages spent in the distribution channel";
    final String replyMessageTimeDesc = "The amount of time spent processing reply messages. This includes both processedMessagesTime and messageProcessingScheduleTime.";
    final String distributeMessageTimeDesc = "The amount of time it takes to prepare a message and send it on the network.  This includes sentMessagesTime.";
    final String nodesDesc = "The current number of nodes in this distributed system.";
    final String replyWaitsInProgressDesc = "Current number of threads waiting for a reply.";
    final String replyWaitsCompletedDesc = "Total number of times waits for a reply have completed.";
    final String replyWaitTimeDesc = "Total time spent waiting for a reply to a message.";
    final String replyWaitMaxTimeDesc = "Maximum time spent transmitting and then waiting for a reply to a message. See sentMessagesMaxTime for related information";
    final String replyTimeoutsDesc =
      "Total number of message replies that have timed out.";
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
    final String asyncQueueFlushesInProgressDesc = "Current number of asynchronous queues being flushed.";
    final String asyncQueueFlushesCompletedDesc = "Total number of asynchronous queue flushes completed.";
    final String asyncQueueFlushTimeDesc = "Total time spent flushing asynchronous queues.";
    final String asyncQueueTimeoutExceededDesc = "Total number of asynchronous queues that have timed out by being blocked for more than async-queue-timeout milliseconds.";
    final String asyncQueueSizeExceededDesc = "Total number of asynchronous queues that have exceeded max size.";
    final String asyncDistributionTimeoutExceededDesc = "Total number of times the async-distribution-timeout has been exceeded during a socket write.";
    final String asyncQueueSizeDesc = "The current size in bytes used for asynchronous queues.";
    final String asyncQueuedMsgsDesc = "The total number of queued messages used for asynchronous queues.";
    final String asyncDequeuedMsgsDesc = "The total number of queued messages that have been removed from the queue and successfully sent.";
    final String asyncConflatedMsgsDesc = "The total number of queued conflated messages used for asynchronous queues.";

    final String asyncThreadsDesc = "Total number of asynchronous message queue threads.";
    final String asyncThreadInProgressDesc = "Current iterations of work performed by asynchronous message queue threads.";
    final String asyncThreadCompletedDesc = "Total number of iterations of work performed by asynchronous message queue threads.";
    final String asyncThreadTimeDesc = "Total time spent by asynchronous message queue threads performing iterations.";
    final String receiverDirectBufferSizeDesc = "Current number of bytes allocated from direct memory as buffers for incoming messages.";
    final String receiverHeapBufferSizeDesc = "Current number of bytes allocated from Java heap memory as buffers for incoming messages.";
    final String senderDirectBufferSizeDesc = "Current number of bytes allocated from direct memory as buffers for outgoing messages.";
    final String senderHeapBufferSizeDesc = "Current number of bytes allocated from Java heap memory as buffers for outoing messages.";

    final String replyHandoffTimeDesc = "Total number of seconds to switch thread contexts from processing thread to application thread.";

    final String partitionedRegionThreadJobsDesc = "The number of messages currently being processed by partitioned region threads";
    final String functionExecutionThreadJobsDesc = "The number of messages currently being processed by function execution threads";
    final String viewThreadsDesc = "The number of threads currently processing view messages.";
    final String serialThreadJobsDesc = "The number of messages currently being processed by serial threads.";
    final String viewThreadJobsDesc = "The number of messages currently being processed by view threads.";
    final String serialPooledThreadJobsDesc = "The number of messages currently being processed by pooled serial processor threads.";
    final String processingThreadJobsDesc = "The number of messages currently being processed by pooled message processor threads.";
    final String highPriorityThreadJobsDesc = "The number of messages currently being processed by high priority processor threads.";
    final String waitingThreadJobsDesc = "The number of messages currently being processed by waiting pooly processor threads.";

    final String eldersDesc = "Current number of system elders hosted in this member.";
    final String initialImageMessagesInFlightDesc = "The number of messages with initial image data sent from this member that have not yet been acknowledged.";
    final String initialImageRequestsInProgressDesc = "The number of initial images this member is currently receiving.";


    StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();

    type = f.createType(
      statName,
      statDescription,
      new StatisticDescriptor[] {
        f.createLongCounter("sentMessages", sentMessagesDesc, "messages"),
        f.createLongCounter("commitMessages", sentCommitMessagesDesc, "messages"),
        f.createLongCounter("commitWaits", commitWaitsDesc, "messages"),
        f.createLongCounter("sentMessagesTime", sentMessagesTimeDesc, "nanoseconds", false),
        f.createLongGauge("sentMessagesMaxTime", sentMessagesMaxTimeDesc, "milliseconds", false),
        f.createLongCounter("broadcastMessages", broadcastMessagesDesc, "messages"),
        f.createLongCounter("broadcastMessagesTime", broadcastMessagesTimeDesc, "nanoseconds", false),
        f.createLongCounter("receivedMessages", receivedMessagesDesc, "messages"),
        f.createLongCounter("receivedBytes", receivedBytesDesc, "bytes"),
        f.createLongCounter("sentBytes", sentBytesDesc, "bytes"),
        f.createLongCounter("processedMessages", processedMessagesDesc, "messages"),
        f.createLongCounter("processedMessagesTime", processedMessagesTimeDesc, "nanoseconds", false),
        f.createLongCounter("messageProcessingScheduleTime", messageProcessingScheduleTimeDesc, "nanoseconds", false),
        f.createIntGauge("overflowQueueSize", overflowQueueSizeDesc, "messages"),
        f.createIntGauge("waitingQueueSize", waitingQueueSizeDesc, "messages"),
        f.createIntGauge("overflowQueueThrottleCount", overflowQueueThrottleCountDesc, "delays"),
        f.createLongCounter("overflowQueueThrottleTime", overflowQueueThrottleTimeDesc, "nanoseconds", false),
        f.createIntGauge("highPriorityQueueSize", highPriorityQueueSizeDesc, "messages"),
        f.createIntGauge("highPriorityQueueThrottleCount", highPriorityQueueThrottleCountDesc, "delays"),
        f.createLongCounter("highPriorityQueueThrottleTime", highPriorityQueueThrottleTimeDesc, "nanoseconds", false),
        f.createIntGauge("partitionedRegionQueueSize", highPriorityQueueSizeDesc, "messages"),
        f.createIntGauge("partitionedRegionQueueThrottleCount", highPriorityQueueThrottleCountDesc, "delays"),
        f.createLongCounter("partitionedRegionQueueThrottleTime", highPriorityQueueThrottleTimeDesc, "nanoseconds", false),
        f.createIntGauge("functionExecutionQueueSize", highPriorityQueueSizeDesc, "messages"),
        f.createIntGauge("functionExecutionQueueThrottleCount", highPriorityQueueThrottleCountDesc, "delays"),
        f.createLongCounter("functionExecutionQueueThrottleTime", highPriorityQueueThrottleTimeDesc, "nanoseconds", false),
        f.createIntGauge("serialQueueSize", serialQueueSizeDesc, "messages"),
        f.createIntGauge("serialQueueBytes", serialQueueBytesDesc, "bytes"),
        f.createIntCounter("serialPooledThread", serialPooledThreadDesc, "threads"),
        f.createIntGauge("serialQueueThrottleCount", serialQueueThrottleCountDesc, "delays"),
        f.createLongCounter("serialQueueThrottleTime", serialQueueThrottleTimeDesc, "nanoseconds", false),
        f.createIntGauge("serialThreads", serialThreadsDesc, "threads"),
        f.createIntGauge("processingThreads", processingThreadsDesc, "threads"),
        f.createIntGauge("highPriorityThreads", highPriorityThreadsDesc, "threads"),
        f.createIntGauge("partitionedRegionThreads", partitionedRegionThreadsDesc, "threads"),
        f.createIntGauge("functionExecutionThreads", functionExecutionThreadsDesc, "threads"),
        f.createIntGauge("waitingThreads", waitingThreadsDesc, "threads"),
        f.createLongCounter("messageChannelTime", messageChannelTimeDesc, "nanoseconds", false),
        f.createLongCounter("replyMessageTime", replyMessageTimeDesc, "nanoseconds", false),
        f.createLongCounter("distributeMessageTime", distributeMessageTimeDesc, "nanoseconds", false),
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

        f.createIntGauge("syncSocketWritesInProgress", "Current number of synchronous/blocking socket write calls in progress.", "writes"),
        f.createLongCounter("syncSocketWriteTime", "Total amount of time, in nanoseconds, spent in synchronous/blocking socket write calls.", "nanoseconds"),
        f.createIntCounter("syncSocketWrites", "Total number of completed synchronous/blocking socket write calls.", "writes"),
        f.createLongCounter("syncSocketWriteBytes", "Total number of bytes sent out in synchronous/blocking mode on sockets.", "bytes"),

        f.createIntCounter("ucastReads", "Total number of unicast datagrams received", "datagrams"),
        f.createLongCounter("ucastReadBytes", "Total number of bytes received in unicast datagrams", "bytes"),
        f.createLongCounter("ucastWriteTime", "Total amount of time, in nanoseconds, spent in unicast datagram socket write calls.", "nanoseconds"),
        f.createIntCounter("ucastWrites", "Total number of unicast datagram socket write calls.", "writes"),
        f.createLongCounter("ucastWriteBytes", "Total number of bytes sent out on unicast datagram sockets.", "bytes"),
        f.createIntCounter("ucastRetransmits", "Total number of unicast datagram socket retransmissions", "writes"),

        f.createIntCounter("mcastReads", "Total number of multicast datagrams received", "datagrams"),
        f.createLongCounter("mcastReadBytes", "Total number of bytes received in multicast datagrams", "bytes"),
        f.createLongCounter("mcastWriteTime", "Total amount of time, in nanoseconds, spent in multicast datagram socket write calls.", "nanoseconds"),
        f.createIntCounter("mcastWrites", "Total number of multicast datagram socket write calls.", "writes"),
        f.createLongCounter("mcastWriteBytes", "Total number of bytes sent out on multicast datagram sockets.", "bytes"),
        f.createIntCounter("mcastRetransmits", "Total number of multicast datagram socket retransmissions", "writes"),
        f.createIntCounter("mcastRetransmitRequests", "Total number of multicast datagram socket retransmission requests sent to other processes", "requests"),

        f.createLongCounter("serializationTime", "Total amount of time, in nanoseconds, spent serializing objects. This includes pdx serializations.", "nanoseconds"),
        f.createIntCounter("serializations", "Total number of object serialization calls. This includes pdx serializations.", "ops"),
        f.createLongCounter("serializedBytes", "Total number of bytes produced by object serialization. This includes pdx serializations.", "bytes"),
        f.createIntCounter("pdxSerializations", "Total number of pdx serializations.", "ops"),
        f.createLongCounter("pdxSerializedBytes", "Total number of bytes produced by pdx serialization.", "bytes"),
        f.createLongCounter("deserializationTime", "Total amount of time, in nanoseconds, spent deserializing objects. This includes deserialization that results in a PdxInstance.", "nanoseconds"),
        f.createIntCounter("deserializations", "Total number of object deserialization calls. This includes deserialization that results in a PdxInstance.", "ops"),
        f.createLongCounter("deserializedBytes", "Total number of bytes read by object deserialization. This includes deserialization that results in a PdxInstance.", "bytes"),
        f.createIntCounter("pdxDeserializations", "Total number of pdx deserializations.", "ops"),
        f.createLongCounter("pdxDeserializedBytes", "Total number of bytes read by pdx deserialization.", "bytes"),
        f.createLongCounter("msgSerializationTime", "Total amount of time, in nanoseconds, spent serializing messages.", "nanoseconds"),
        f.createLongCounter("msgDeserializationTime", "Total amount of time, in nanoseconds, spent deserializing messages.", "nanoseconds"),
        f.createIntCounter("pdxInstanceDeserializations", "Total number of times getObject has been called on a PdxInstance.", "ops"),
        f.createLongCounter("pdxInstanceDeserializationTime", "Total amount of time, in nanoseconds, spent deserializing PdxInstances by calling getObject.", "nanoseconds"),
        f.createIntCounter("pdxInstanceCreations", "Total number of times a deserialization created a PdxInstance.", "ops"),

        f.createLongCounter("batchSendTime", "Total amount of time, in nanoseconds, spent queueing and flushing message batches", "nanoseconds"),
        f.createLongCounter("batchWaitTime", "Reserved for future use", "nanoseconds"),
        f.createLongCounter("batchCopyTime", "Total amount of time, in nanoseconds, spent copying messages for batched transmission", "nanoseconds"),
        f.createLongCounter("batchFlushTime", "Total amount of time, in nanoseconds, spent flushing batched messages to the network", "nanoseconds"),

        f.createIntCounter("ucastFlushes", "Total number of flushes of the unicast datagram protocol, prior to sending a multicast message", "flushes"),
        f.createLongCounter("ucastFlushTime", "Total amount of time, in nanoseconds, spent waiting for acknowledgements for outstanding unicast datagram messages", "nanoseconds"),

        f.createIntCounter("flowControlRequests", "Total number of flow control credit requests sent to other processes", "messages"),
        f.createIntCounter("flowControlResponses", "Total number of flow control credit responses sent to a requestor", "messages"),
        f.createIntGauge("flowControlWaitsInProgress", "Number of threads blocked waiting for flow-control recharges from other processes", "threads"),
        f.createLongCounter("flowControlWaitTime", "Total amount of time, in nanoseconds, spent waiting for other processes to recharge the flow of control meter", "nanoseconds"),
        f.createIntGauge("flowControlThrottleWaitsInProgress", "Number of threads blocked waiting due to flow-control throttle requests from other members", "threads"),

        f.createLongGauge("jgNAKACKreceivedMessages", "Number of received messages awaiting stability in NAKACK", "messages"),
        f.createLongGauge("jgNAKACKsentMessages", "Number of sent messages awaiting stability in NAKACK", "messages"),

        f.createLongGauge("jgQueuedMessages", "Number of messages queued by transport and awaiting processing", "messages"),

        f.createLongGauge("jgUNICASTreceivedMessages", "Number of received messages awaiting receipt of prior messages", "messages"),
        f.createLongGauge("jgUNICASTsentMessages", "Number of un-acked normal priority messages", "messages"),
        f.createLongGauge("jgUNICASTsentHighPriorityMessages", "Number of un-acked high priority messages", "messages"),

        f.createLongCounter("jgUNICASTdataReceivedTime", "Amount of time spent in JGroups UNICAST send", "nanoseconds"),
        f.createLongCounter("jgSTABLEsuspendTime", "Amount of time JGroups STABLE is suspended", "nanoseconds"),
        f.createLongCounter("jgSTABLEmessages", "Number of STABLE messages received by JGroups", "messages"),
        f.createLongCounter("jgSTABLEmessagesSent", "Number of STABLE messages sent by JGroups", "messages"),
        f.createLongCounter("jgSTABILITYmessages", "Number of STABILITY messages received by JGroups", "messages"),

//        f.createLongCounter("jgUDPupTime", "Time spent in JGroups UDP processing up events", "nanoseconds"),
//        f.createLongCounter("jgUDPdownTime", "Time spent in JGroups UDP processing down events", "nanoseconds"),
//        f.createLongCounter("jgNAKACKupTime", "Time spent in JGroups NAKACK processing up events", "nanoseconds"),
//        f.createLongCounter("jgNAKACKdownTime", "Time spent in JGroups NAKACK processing down events", "nanoseconds"),
//        f.createLongCounter("jgUNICASTupTime", "Time spent in JGroups UNICAST processing up events", "nanoseconds"),
//        f.createLongCounter("jgUNICASTdownTime", "Time spent in JGroups UNICAST processing down events", "nanoseconds"),
//        f.createLongCounter("jgSTABLEupTime", "Time spent in JGroups STABLE processing up events", "nanoseconds"),
//        f.createLongCounter("jgSTABLEdownTime", "Time spent in JGroups STABLE processing down events", "nanoseconds"),
//        f.createLongCounter("jgFRAG2upTime", "Time spent in JGroups FRAG2 processing up events", "nanoseconds"),
//        f.createLongCounter("jgFRAG2downTime", "Time spent in JGroups FRAG2 processing down events", "nanoseconds"),
//        f.createLongCounter("jgGMSupTime", "Time spent in JGroups GMS processing up events", "nanoseconds"),
//        f.createLongCounter("jgGMSdownTime", "Time spent in JGroups GMS processing down events", "nanoseconds"),
//        f.createLongCounter("jgFCupTime", "Time spent in JGroups FC processing up events", "nanoseconds"),
//        f.createLongCounter("jgFCdownTime", "Time spent in JGroups FC processing down events", "nanoseconds"),
//        f.createLongCounter("jgDirAckupTime", "Time spent in JGroups DirAck processing up events", "nanoseconds"),
//        f.createLongCounter("jgDirAckdownTime", "Time spent in JGroups DirAck processing down events", "nanoseconds"),
//
//        f.createLongCounter("jgVIEWSYNCdownTime", "Time spent in JGroups VIEWSYNC processing down events", "nanoseconds"),
//        f.createLongCounter("jgVIEWSYNCupTime", "Time spent in JGroups VIEWSYNC processing up events", "nanoseconds"),
//        f.createLongCounter("jgFDdownTime", "Time spent in JGroups FD processing down events", "nanoseconds"),
//        f.createLongCounter("jgFDupTime", "Time spent in JGroups FD processing up events", "nanoseconds"),
//        f.createLongCounter("jgTCPGOSSIPdownTime", "Time spent in JGroups TCPGOSSIP processing down events", "nanoseconds"),
//        f.createLongCounter("jgTCPGOSSIPupTime", "Time spent in JGroups TCPGOSSIP processing up events", "nanoseconds"),
//        f.createLongCounter("jgDISCOVERYdownTime", "Time spent in JGroups DISCOVERY processing down events", "nanoseconds"),
//        f.createLongCounter("jgDISCOVERYupTime", "Time spent in JGroups DISCOVERY processing up events", "nanoseconds"),

        f.createLongCounter("jgDownTime", "Down Time spent in JGroups stacks", "nanoseconds"),
        f.createLongCounter("jgUpTime", "Up Time spent in JGroups stacks", "nanoseconds"),
        f.createLongCounter("jChannelUpTime", "Up Time spent in JChannel including jgroup stack", "nanoseconds"),

        f.createLongCounter("jgFCsendBlocks", "Number of times JGroups FC halted sends due to backpressure", "events"),
        f.createLongCounter("jgFCautoRequests", "Number of times JGroups FC automatically sent replenishment requests", "events"),
        f.createLongCounter("jgFCreplenish", "Number of times JGroups FC received replenishments from receivers", "messages"),
        f.createLongCounter("jgFCresumes", "Number of times JGroups FC resumed sends due to backpressure", "events"),
        f.createLongCounter("jgFCsentCredits", "Number of times JGroups FC sent credits to a sender", "events"),
        f.createLongCounter("jgFCsentThrottleRequests","Number of times JGroups FC sent throttle requests to a sender", "events"),

        f.createIntGauge("asyncSocketWritesInProgress", "Current number of non-blocking socket write calls in progress.", "writes"),
        f.createIntCounter("asyncSocketWrites", "Total number of non-blocking socket write calls completed.", "writes"),
        f.createIntCounter("asyncSocketWriteRetries", "Total number of retries needed to write a single block of data using non-blocking socket write calls.", "writes"),
        f.createLongCounter("asyncSocketWriteTime", "Total amount of time, in nanoseconds, spent in non-blocking socket write calls.", "nanoseconds"),
        f.createLongCounter("asyncSocketWriteBytes", "Total number of bytes sent out on non-blocking sockets.", "bytes"),

        f.createLongCounter("asyncQueueAddTime", "Total amount of time, in nanoseconds, spent in adding messages to async queue.", "nanoseconds"),
        f.createLongCounter("asyncQueueRemoveTime", "Total amount of time, in nanoseconds, spent in removing messages from async queue.", "nanoseconds"),

        f.createIntGauge("asyncQueues", asyncQueuesDesc, "queues"),
        f.createIntGauge("asyncQueueFlushesInProgress", asyncQueueFlushesInProgressDesc, "operations"),
        f.createIntCounter("asyncQueueFlushesCompleted", asyncQueueFlushesCompletedDesc, "operations"),
        f.createLongCounter("asyncQueueFlushTime", asyncQueueFlushTimeDesc, "nanoseconds", false),
        f.createIntCounter("asyncQueueTimeoutExceeded", asyncQueueTimeoutExceededDesc, "timeouts"),
        f.createIntCounter("asyncQueueSizeExceeded", asyncQueueSizeExceededDesc, "operations"),
        f.createIntCounter("asyncDistributionTimeoutExceeded", asyncDistributionTimeoutExceededDesc, "operations"),
        f.createLongGauge("asyncQueueSize", asyncQueueSizeDesc, "bytes"),
        f.createLongCounter("asyncQueuedMsgs", asyncQueuedMsgsDesc, "msgs"),
        f.createLongCounter("asyncDequeuedMsgs", asyncDequeuedMsgsDesc, "msgs"),
        f.createLongCounter("asyncConflatedMsgs", asyncConflatedMsgsDesc, "msgs"),

        f.createIntGauge("asyncThreads", asyncThreadsDesc, "threads"),
        f.createIntGauge("asyncThreadInProgress", asyncThreadInProgressDesc, "operations"),
        f.createIntCounter("asyncThreadCompleted", asyncThreadCompletedDesc, "operations"),
        f.createLongCounter("asyncThreadTime", asyncThreadTimeDesc, "nanoseconds", false),
        f.createLongCounter("jgNAKACKwaits", "Number of delays created by NAKACK sent_msgs overflow", "events"),

        f.createLongGauge("receiversTO", "Number of receiver threads owned by non-receiver threads in other members.", "threads"),
        f.createLongGauge("receiversTO2", "Number of receiver threads owned in turn by receiver threads in other members", "threads"),

        f.createLongCounter("jgFragmentationsPerformed", "Number of message fragmentation operations performed", "operations"),
        f.createLongCounter("jgFragmentsCreated", "Number of message fragments created", "fragments"),

        f.createLongGauge("receiverDirectBufferSize", receiverDirectBufferSizeDesc, "bytes"),
        f.createLongGauge("receiverHeapBufferSize", receiverHeapBufferSizeDesc, "bytes"),
        f.createLongGauge("senderDirectBufferSize", senderDirectBufferSizeDesc, "bytes"),
        f.createLongGauge("senderHeapBufferSize", senderHeapBufferSizeDesc, "bytes"),
        f.createIntGauge("socketLocksInProgress", "Current number of threads waiting to lock a socket", "threads", false),
        f.createIntCounter("socketLocks", "Total number of times a socket has been locked.", "locks"),
        f.createLongCounter("socketLockTime", "Total amount of time, in nanoseconds, spent locking a socket", "nanoseconds", false),
        f.createIntGauge("bufferAcquiresInProgress", "Current number of threads waiting to acquire a buffer", "threads", false),
        f.createIntCounter("bufferAcquires", "Total number of times a buffer has been acquired.", "operations"),
        f.createLongCounter("bufferAcquireTime", "Total amount of time, in nanoseconds, spent acquiring a socket", "nanoseconds", false),

        f.createIntGauge("messagesBeingReceived", "Current number of message being received off the network or being processed after reception.", "messages"),
        f.createLongGauge("messageBytesBeingReceived", "Current number of bytes consumed by messages being received or processed.", "bytes"),

        f.createLongCounter("serialThreadStarts", "Total number of times a thread has been created for the serial message executor.", "starts", false),
        f.createLongCounter("viewThreadStarts", "Total number of times a thread has been created for the view message executor.", "starts", false),
        f.createLongCounter("processingThreadStarts", "Total number of times a thread has been created for the pool processing normal messages.", "starts", false),
        f.createLongCounter("highPriorityThreadStarts", "Total number of times a thread has been created for the pool handling high priority messages.", "starts", false),
        f.createLongCounter("waitingThreadStarts", "Total number of times a thread has been created for the waiting pool.", "starts", false),
        f.createLongCounter("partitionedRegionThreadStarts", "Total number of times a thread has been created for the pool handling partitioned region messages.", "starts", false),
        f.createLongCounter("functionExecutionThreadStarts", "Total number of times a thread has been created for the pool handling function execution messages.", "starts", false),
        f.createLongCounter("serialPooledThreadStarts", "Total number of times a thread has been created for the serial pool(s).", "starts", false),
        f.createLongCounter("TOSentMsgs", "Total number of messages sent on thread owned senders", "messages", false),
        f.createLongCounter("replyHandoffTime", replyHandoffTimeDesc, "nanoseconds"),

        f.createIntGauge("partitionedRegionThreadJobs", partitionedRegionThreadJobsDesc, "messages"),
        f.createIntGauge("functionExecutionThreadJobs", functionExecutionThreadJobsDesc, "messages"),
        f.createIntGauge("viewThreads", viewThreadsDesc, "threads"),
        f.createIntGauge("serialThreadJobs", serialThreadJobsDesc, "messages"),
        f.createIntGauge("viewThreadJobs", viewThreadJobsDesc, "messages"),
        f.createIntGauge("serialPooledThreadJobs", serialPooledThreadJobsDesc, "messages"),
        f.createIntGauge("processingThreadJobs", processingThreadJobsDesc, "messages"),
        f.createIntGauge("highPriorityThreadJobs", highPriorityThreadJobsDesc, "messages"),
        f.createIntGauge("waitingThreadJobs", waitingThreadJobsDesc, "messages"),

        f.createIntGauge("elders", eldersDesc, "elders"),
        f.createIntGauge("initialImageMessagesInFlight", initialImageMessagesInFlightDesc, "messages"),
        f.createIntGauge("initialImageRequestsInProgress", initialImageRequestsInProgressDesc, "requests"),
      }
    );

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
    messageProcessingScheduleTimeId =
      type.nameToId("messageProcessingScheduleTime");
    messageChannelTimeId = type.nameToId("messageChannelTime");
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

    syncSocketWritesInProgressId = type.nameToId("syncSocketWritesInProgress");
    syncSocketWriteTimeId = type.nameToId("syncSocketWriteTime");
    syncSocketWritesId = type.nameToId("syncSocketWrites");
    syncSocketWriteBytesId = type.nameToId("syncSocketWriteBytes");

    ucastReadsId = type.nameToId("ucastReads");
    ucastReadBytesId = type.nameToId("ucastReadBytes");
    ucastWriteTimeId = type.nameToId("ucastWriteTime");
    ucastWritesId = type.nameToId("ucastWrites");
    ucastWriteBytesId = type.nameToId("ucastWriteBytes");
    ucastRetransmitsId = type.nameToId("ucastRetransmits");

    mcastReadsId = type.nameToId("mcastReads");
    mcastReadBytesId = type.nameToId("mcastReadBytes");
    mcastWriteTimeId = type.nameToId("mcastWriteTime");
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

    batchSendTimeId = type.nameToId("batchSendTime");
    batchCopyTimeId = type.nameToId("batchCopyTime");
    batchWaitTimeId = type.nameToId("batchWaitTime");
    batchFlushTimeId = type.nameToId("batchFlushTime");

    ucastFlushesId = type.nameToId("ucastFlushes");
    ucastFlushTimeId = type.nameToId("ucastFlushTime");

    flowControlRequestsId = type.nameToId("flowControlRequests");
    flowControlResponsesId = type.nameToId("flowControlResponses");
    flowControlWaitsInProgressId = type.nameToId("flowControlWaitsInProgress");
    flowControlThrottleWaitsInProgressId = type.nameToId("flowControlThrottleWaitsInProgress");

    jgUNICASTdataReceivedTimeId = type.nameToId("jgUNICASTdataReceivedTime");

    jgReceivedMessagesSizeId = type.nameToId("jgNAKACKreceivedMessages");
    jgSentMessagesPoolSizeId = type.nameToId("jgNAKACKsentMessages");
    jgQueuedMessagesSizeId = type.nameToId("jgQueuedMessages");
    jgSTABLEsuspendTimeId = type.nameToId("jgSTABLEsuspendTime");
    jgSTABLEmessagesId = type.nameToId("jgSTABLEmessages");
    jgSTABLEmessagesSentId = type.nameToId("jgSTABLEmessagesSent");
    jgSTABILITYmessagesId = type.nameToId("jgSTABILITYmessages");
    jgFragmentationsPerformedId = type.nameToId("jgFragmentationsPerformed");
    jgFragmentsCreatedId = type.nameToId("jgFragmentsCreated");

    jgUcastReceivedMessagesSizeId = type.nameToId("jgUNICASTreceivedMessages");
    jgUcastSentMessagesSizeId = type.nameToId("jgUNICASTsentMessages");
    jgUcastSentHighPriorityMessagesSizeId = type.nameToId("jgUNICASTsentHighPriorityMessages");

    jgDownTimeId = type.nameToId("jgDownTime");
    jgUpTimeId = type.nameToId("jgUpTime");
    jChannelUpTimeId = type.nameToId("jChannelUpTime");

    jgFCsendBlocksId = type.nameToId("jgFCsendBlocks");
    jgFCautoRequestsId = type.nameToId("jgFCautoRequests");
    jgFCreplenishId = type.nameToId("jgFCreplenish");
    jgFCresumesId = type.nameToId("jgFCresumes");
    jgFCsentCreditsId = type.nameToId("jgFCsentCredits");
    jgFCsentThrottleRequestsId = type.nameToId("jgFCsentThrottleRequests");

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

    jgNAKACKwaitsId = type.nameToId("jgNAKACKwaits");

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
    viewThreadStartsId = type.nameToId("viewThreadStarts");
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
    viewThreadsId = type.nameToId("viewThreads");
    serialThreadJobsId = type.nameToId("serialThreadJobs");
    viewProcessorThreadJobsId = type.nameToId("viewThreadJobs");
    serialPooledThreadJobsId = type.nameToId("serialPooledThreadJobs");
    pooledMessageThreadJobsId = type.nameToId("processingThreadJobs");
    highPriorityThreadJobsId = type.nameToId("highPriorityThreadJobs");
    waitingPoolThreadJobsId = type.nameToId("waitingThreadJobs");

    eldersId = type.nameToId("elders");
    initialImageMessagesInFlightId = type.nameToId("initialImageMessagesInFlight");
    initialImageRequestsInProgressId = type.nameToId("initialImageRequestsInProgress");
  }

  /** The Statistics object that we delegate most behavior to */
  private final Statistics stats;

//  private final HistogramStats replyHandoffHistogram;
//  private final HistogramStats replyWaitHistogram;

  ////////////////////////  Constructors  ////////////////////////

  /**
   * Creates a new <code>DistributionStats</code> and registers itself
   * with the given statistics factory.
   */
  public DistributionStats(StatisticsFactory f, long statId) {
    this.stats = f.createAtomicStatistics(type, "distributionStats", statId);
//    this.replyHandoffHistogram = new HistogramStats("ReplyHandOff", "nanoseconds", f,
//        new long[] {100000, 200000, 300000, 400000, 500000, 600000, 700000, 800000, 900000, 1000000}, false);
//    this.replyWaitHistogram = new HistogramStats("ReplyWait", "nanoseconds", f,
//        new long[] {100000, 200000, 300000, 400000, 500000, 600000, 700000, 800000, 900000, 1000000}, false);
    Buffers.initBufferStats(this);
  }
  /**
   * Used by tests to create an instance given its already existings stats.
   */
  public DistributionStats(Statistics stats) {
    this.stats = stats;
//    this.replyHandoffHistogram = null;
//    this.replyWaitHistogram = null;
  }

  /**
   * Returns the current NanoTime or, if clock stats are disabled, zero.
   * @since 5.0
   */
  public static long getStatTime() {
    return enableClockStats? NanoTimer.getTime() : 0;
  }

  //////////////////////  Instance Methods  //////////////////////

  public void close() {
    this.stats.close();
  }

 /**
   * Returns the total number of messages sent by the distribution
   * manager
   */
  public long getSentMessages() {
    return this.stats.getLong(sentMessagesId);
  }
  public void incTOSentMsg() {
    this.stats.incLong(TOSentMsgId, 1);
  }

  public long getSentCommitMessages() {
    return this.stats.getLong(sentCommitMessagesId);
  }

  public long getCommitWaits() {
    return this.stats.getLong(commitWaitsId);
  }

  /**
   * Increments the total number of messages sent by the distribution
   * manager
   */
  public void incSentMessages(long messages) {
    this.stats.incLong(sentMessagesId, messages);
  }
  /**
   * Increments the total number of transactino commit messages
   * sent by the distribution manager
   */
  public void incSentCommitMessages(long messages) {
    this.stats.incLong(sentCommitMessagesId, messages);
  }
  public void incCommitWaits() {
    this.stats.incLong(commitWaitsId, 1);
  }

  /**
   * Returns the total number of nanoseconds spent sending messages.
   */
  public long getSentMessagesTime() {
    return this.stats.getLong(sentMessagesTimeId);
  }

  /**
   * Increments the total number of nanoseconds spend sending messages.<p>
   * This also sets the sentMessagesMaxTime, if appropriate
   */
  public void incSentMessagesTime(long nanos) {
    if (enableClockStats) {
      this.stats.incLong(sentMessagesTimeId, nanos);
      long millis = nanos / 1000000;
      if (getSentMessagesMaxTime() < millis) {
        this.stats.setLong(sentMessagesMaxTimeId, millis);
      }
    }
  }

  /**
   * Returns the longest time required to distribute a message, in nanos
   */
  public long getSentMessagesMaxTime() {
    return this.stats.getLong(sentMessagesMaxTimeId);
  }


  /**
   * Returns the total number of messages broadcast by the distribution
   * manager
   */
  public long getBroadcastMessages() {
    return this.stats.getLong(broadcastMessagesId);
  }

  /**
   * Increments the total number of messages broadcast by the distribution
   * manager
   */
  public void incBroadcastMessages(long messages) {
    this.stats.incLong(broadcastMessagesId, messages);
  }

  /**
   * Returns the total number of nanoseconds spent sending messages.
   */
  public long getBroadcastMessagesTime() {
    return this.stats.getLong(broadcastMessagesTimeId);
  }

  /**
   * Increments the total number of nanoseconds spend sending messages.
   */
  public void incBroadcastMessagesTime(long nanos) {
    if (enableClockStats) {
      this.stats.incLong(broadcastMessagesTimeId, nanos);
    }
  }

  /**
   * Returns the total number of messages received by the distribution
   * manager
   */
  public long getReceivedMessages() {
    return this.stats.getLong(receivedMessagesId);
  }

  /**
   * Increments the total number of messages received by the distribution
   * manager
   */
  public void incReceivedMessages(long messages) {
    this.stats.incLong(receivedMessagesId, messages);
  }

  /**
   * Returns the total number of bytes received by the distribution
   * manager
   */
  public long getReceivedBytes() {
    return this.stats.getLong(receivedBytesId);
  }

  public void incReceivedBytes(long bytes) {
    this.stats.incLong(receivedBytesId, bytes);
  }

  public void incSentBytes(long bytes) {
    this.stats.incLong(sentBytesId, bytes);
  }

  /**
   * Returns the total number of messages processed by the distribution
   * manager
   */
  public long getProcessedMessages() {
    return this.stats.getLong(processedMessagesId);
  }

  /**
   * Increments the total number of messages processed by the distribution
   * manager
   */
  public void incProcessedMessages(long messages) {
    this.stats.incLong(processedMessagesId, messages);
  }

  /**
   * Returns the total number of nanoseconds spent processing messages.
   */
  public long getProcessedMessagesTime() {
    return this.stats.getLong(processedMessagesTimeId);
  }

  /**
   * Increments the total number of nanoseconds spend processing messages.
   */
  public void incProcessedMessagesTime(long start) {
    if (enableClockStats) {
      this.stats.incLong(processedMessagesTimeId, getStatTime()-start);
    }
  }

  /**
   * Returns the total number of nanoseconds spent scheduling messages to be processed.
   */
  public long getMessageProcessingScheduleTime() {
    return this.stats.getLong(messageProcessingScheduleTimeId);
  }

  /**
   * Increments the total number of nanoseconds spent scheduling messages to be processed.
   */
  public void incMessageProcessingScheduleTime(long elapsed) {
    if (enableClockStats) {
      this.stats.incLong(messageProcessingScheduleTimeId, elapsed);
    }
  }

  public int getOverflowQueueSize() {
    return this.stats.getInt(overflowQueueSizeId);
  }

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
    this.stats.incInt(serialQueueBytesId, amount);
  }
  public int getSerialQueueBytes() {
    return this.stats.getInt(serialQueueBytesId);
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
  public int getNumProcessingThreads() {
    return this.stats.getInt(processingThreadsId);
  }

  public void incNumProcessingThreads(int threads) {
    this.stats.incInt(processingThreadsId, threads);
  }

  public int getNumSerialThreads() {
    return this.stats.getInt(serialThreadsId);
  }

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

  public void incMessageChannelTime(long delta) {
    if (enableClockStats) {
      this.stats.incLong(messageChannelTimeId, delta);
    }
  }

  public long getReplyMessageTime() {
    return this.stats.getLong(replyMessageTimeId);
  }
  public void incReplyMessageTime(long val) {
    if (enableClockStats) {
      this.stats.incLong(replyMessageTimeId, val);
    }
  }

  public long getDistributeMessageTime() {
    return this.stats.getLong(distributeMessageTimeId);
  }
  public void incDistributeMessageTime(long val) {
    if (enableClockStats) {
      this.stats.incLong(distributeMessageTimeId, val);
    }
  }

  public int getNodes() {
    return this.stats.getInt(nodesId);
  }
  public void setNodes(int val) {
    this.stats.setInt(nodesId, val);
  }
  public void incNodes(int val) {
    this.stats.incInt(nodesId, val);
  }
  public int getReplyWaitsInProgress() {
    return stats.getInt(replyWaitsInProgressId);
  }
  public int getReplyWaitsCompleted() {
    return stats.getInt(replyWaitsCompletedId);
  }
  public long getReplyWaitTime() {
    return stats.getLong(replyWaitTimeId);
  }
  public long getReplyWaitMaxTime() {
    return stats.getLong(replyWaitMaxTimeId);
  }
  public long startSocketWrite(boolean sync) {
    if (sync) {
      stats.incInt(syncSocketWritesInProgressId, 1);
    } else {
      stats.incInt(asyncSocketWritesInProgressId, 1);
    }
    return getStatTime();
  }
  public void endSocketWrite(boolean sync, long start, int bytesWritten, int retries) {
    final long now = getStatTime();
    if (sync) {
      stats.incInt(syncSocketWritesInProgressId, -1);
      stats.incInt(syncSocketWritesId, 1);
      stats.incLong(syncSocketWriteBytesId, bytesWritten);
      if (enableClockStats) {
        stats.incLong(syncSocketWriteTimeId, now-start);
      }
    } else {
      stats.incInt(asyncSocketWritesInProgressId, -1);
      stats.incInt(asyncSocketWritesId, 1);
      if (retries != 0) {
        stats.incInt(asyncSocketWriteRetriesId, retries);
      }
      stats.incLong(asyncSocketWriteBytesId, bytesWritten);
      if (enableClockStats) {
        stats.incLong(asyncSocketWriteTimeId, now-start);
      }
    }
  }
  public long startSocketLock() {
    stats.incInt(socketLocksInProgressId, 1);
    return getStatTime();
  }
  public void endSocketLock(long start) {
    long ts = getStatTime();
    stats.incInt(socketLocksInProgressId, -1);
    stats.incInt(socketLocksId, 1);
    stats.incLong(socketLockTimeId, ts-start);
  }
  public long startBufferAcquire() {
    stats.incInt(bufferAcquiresInProgressId, 1);
    return getStatTime();
  }
  public void endBufferAcquire(long start) {
    long ts = getStatTime();
    stats.incInt(bufferAcquiresInProgressId, -1);
    stats.incInt(bufferAcquiresId, 1);
    stats.incLong(bufferAcquireTimeId, ts-start);
  }

  public long startUcastWrite() {
    return getStatTime();
  }
  public void endUcastWrite(long start, int bytesWritten) {
    if (enableClockStats) {
      stats.incLong(ucastWriteTimeId, getStatTime()-start);
    }
    stats.incInt(ucastWritesId, 1);
    stats.incLong(ucastWriteBytesId, bytesWritten);
  }

  public long startMcastWrite() {
    return getStatTime();
  }
  public void endMcastWrite(long start, int bytesWritten) {
    stats.incInt(mcastWritesId, 1);
    if (enableClockStats) {
      stats.incLong(mcastWriteTimeId, getStatTime()-start);
    }
    stats.incLong(mcastWriteBytesId, bytesWritten);
  }

  public int getMcastWrites() {
    return stats.getInt(mcastWritesId);
  }
  public void incMcastReadBytes(long amount) {
    stats.incInt(mcastReadsId, 1);
    stats.incLong(mcastReadBytesId, amount);
  }
  public void incUcastReadBytes(long amount) {
    stats.incInt(ucastReadsId, 1);
    stats.incLong(ucastReadBytesId, amount);
  }
  public long startSerialization() {
    return getStatTime();
  }
  public void endSerialization(long start, int bytes) {
    if (enableClockStats) {
      stats.incLong(serializationTimeId, getStatTime()-start);
    }
    stats.incInt(serializationsId, 1);
    stats.incLong(serializedBytesId, bytes);
  }
  public long startPdxInstanceDeserialization() {
    return getStatTime();
  }
  public void endPdxInstanceDeserialization(long start) {
    if (enableClockStats) {
      stats.incLong(pdxInstanceDeserializationTimeId, getStatTime()-start);
    }
    stats.incInt(pdxInstanceDeserializationsId, 1);
  }
  public void incPdxSerialization(int bytes) {
    stats.incInt(pdxSerializationsId, 1);
    stats.incLong(pdxSerializedBytesId, bytes);
  }
  public void incPdxDeserialization(int bytes) {
    stats.incInt(pdxDeserializationsId, 1);
    stats.incLong(pdxDeserializedBytesId, bytes);
  }
  public void incPdxInstanceCreations() {
    stats.incInt(pdxInstanceCreationsId, 1);
  }
  public long startDeserialization() {
    return getStatTime();
  }
  public void endDeserialization(long start, int bytes) {
    if (enableClockStats) {
      stats.incLong(deserializationTimeId, getStatTime()-start);
    }
    stats.incInt(deserializationsId, 1);
    stats.incLong(deserializedBytesId, bytes);
  }
  public long startMsgSerialization() {
    return getStatTime();
  }
  public void endMsgSerialization(long start) {
    if (enableClockStats) {
      stats.incLong(msgSerializationTimeId, getStatTime()-start);
    }
  }
  public long startMsgDeserialization() {
    return getStatTime();
  }
  public void endMsgDeserialization(long start) {
    if (enableClockStats) {
      stats.incLong(msgDeserializationTimeId, getStatTime()-start);
    }
  }
  /**
   * @return the timestamp that marks the start of the operation
   */
  public long startReplyWait() {
    stats.incInt(replyWaitsInProgressId, 1);
    return getStatTime();
  }
  public void endReplyWait(long startNanos, long initTime) {
    if (enableClockStats) {
      stats.incLong(replyWaitTimeId, getStatTime()-startNanos);
//      this.replyWaitHistogram.endOp(delta);
    }
    if (initTime != 0) {
      long mswait = System.currentTimeMillis() - initTime;
      if (mswait > getReplyWaitMaxTime()) {
        stats.setLong(replyWaitMaxTimeId, mswait);
      }
    }
    stats.incInt(replyWaitsInProgressId, -1);
    stats.incInt(replyWaitsCompletedId, 1);
    
    Breadcrumbs.setSendSide(null); // clear any recipient breadcrumbs set by the message
    Breadcrumbs.setProblem(null); // clear out reply-wait errors
  }

  public void incReplyTimeouts() {
    stats.incLong(replyTimeoutsId, 1L);
  }

  public long getReplyTimeouts() {
    return stats.getLong(replyTimeoutsId);
  }

  public void incReceivers() {
    stats.incInt(receiverConnectionsId, 1);
  }
  public void decReceivers() {
    stats.incInt(receiverConnectionsId, -1);
  }
  public void incFailedAccept() {
    stats.incInt(failedAcceptsId, 1);
  }
  public void incFailedConnect() {
    stats.incInt(failedConnectsId, 1);
  }
  public void incReconnectAttempts() {
    stats.incInt(reconnectAttemptsId, 1);
  }
  public void incLostLease() {
    stats.incInt(lostConnectionLeaseId, 1);
  }
  public void incSenders(boolean shared, boolean preserveOrder) {
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
  }
  public int getSendersSU() {
    return stats.getInt(sharedUnorderedSenderConnectionsId);
  }
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

  public int getAsyncSocketWritesInProgress() {
     return stats.getInt(asyncSocketWritesInProgressId);
  }
  public int getAsyncSocketWrites() {
     return stats.getInt(asyncSocketWritesId);
  }
  public int getAsyncSocketWriteRetries() {
    return stats.getInt(asyncSocketWriteRetriesId);
  }
  public long getAsyncSocketWriteBytes() {
     return stats.getLong(asyncSocketWriteBytesId);
  }
  public long getAsyncSocketWriteTime() {
     return stats.getLong(asyncSocketWriteTimeId);
  }
  public long getAsyncQueueAddTime() {
     return stats.getLong(asyncQueueAddTimeId);
  }
  public void incAsyncQueueAddTime(long inc) {
    if (enableClockStats) {
      stats.incLong(asyncQueueAddTimeId, inc);
    }
  }
  public long getAsyncQueueRemoveTime() {
     return stats.getLong(asyncQueueRemoveTimeId);
  }
  public void incAsyncQueueRemoveTime(long inc) {
    if (enableClockStats) {
      stats.incLong(asyncQueueRemoveTimeId, inc);
    }
  }

  public int getAsyncQueues() {
     return stats.getInt(asyncQueuesId);
  }
  public void incAsyncQueues(int inc) {
    stats.incInt(asyncQueuesId, inc);
  }
  public int getAsyncQueueFlushesInProgress() {
     return stats.getInt(asyncQueueFlushesInProgressId);
  }
  public int getAsyncQueueFlushesCompleted() {
     return stats.getInt(asyncQueueFlushesCompletedId);
  }
  public long getAsyncQueueFlushTime() {
     return stats.getLong(asyncQueueFlushTimeId);
  }
  public long startAsyncQueueFlush() {
    stats.incInt(asyncQueueFlushesInProgressId, 1);
    return getStatTime();
  }
  public void endAsyncQueueFlush(long start) {
    stats.incInt(asyncQueueFlushesInProgressId, -1);
    stats.incInt(asyncQueueFlushesCompletedId, 1);
    if (enableClockStats) {
      stats.incLong(asyncQueueFlushTimeId, getStatTime()-start);
    }
  }
  public int getAsyncQueueTimeouts() {
     return stats.getInt(asyncQueueTimeoutExceededId);
  }
  public void incAsyncQueueTimeouts(int inc) {
    stats.incInt(asyncQueueTimeoutExceededId, inc);
  }
  public int getAsyncQueueSizeExceeded() {
     return stats.getInt(asyncQueueSizeExceededId);
  }
  public void incAsyncQueueSizeExceeded(int inc) {
    stats.incInt(asyncQueueSizeExceededId, inc);
  }
  public int getAsyncDistributionTimeoutExceeded() {
     return stats.getInt(asyncDistributionTimeoutExceededId);
  }
  public void incAsyncDistributionTimeoutExceeded() {
    stats.incInt(asyncDistributionTimeoutExceededId, 1);
  }

  public long getAsyncQueueSize() {
     return stats.getLong(asyncQueueSizeId);
  }
  public void incAsyncQueueSize(long inc) {
    stats.incLong(asyncQueueSizeId, inc);
  }
  public long getAsyncQueuedMsgs() {
     return stats.getLong(asyncQueuedMsgsId);
  }
  public void incAsyncQueuedMsgs() {
    stats.incLong(asyncQueuedMsgsId, 1);
  }
  public long getAsyncDequeuedMsgs() {
     return stats.getLong(asyncDequeuedMsgsId);
  }
  public void incAsyncDequeuedMsgs() {
    stats.incLong(asyncDequeuedMsgsId, 1);
  }
  public long getAsyncConflatedMsgs() {
     return stats.getLong(asyncConflatedMsgsId);
  }
  public void incAsyncConflatedMsgs() {
    stats.incLong(asyncConflatedMsgsId, 1);
  }

  public int getAsyncThreads() {
     return stats.getInt(asyncThreadsId);
  }
  public void incAsyncThreads(int inc) {
    stats.incInt(asyncThreadsId, inc);
  }
  public int getAsyncThreadInProgress() {
     return stats.getInt(asyncThreadInProgressId);
  }
  public int getAsyncThreadCompleted() {
     return stats.getInt(asyncThreadCompletedId);
  }
  public long getAsyncThreadTime() {
     return stats.getLong(asyncThreadTimeId);
  }
  public long startAsyncThread() {
    stats.incInt(asyncThreadInProgressId, 1);
    return getStatTime();
  }
  public void endAsyncThread(long start) {
    stats.incInt(asyncThreadInProgressId, -1);
    stats.incInt(asyncThreadCompletedId, 1);
    if (enableClockStats) {
      stats.incLong(asyncThreadTimeId, getStatTime()-start);
    }
  }

  /**
   * Returns a helper object so that the overflow queue can record its
   * stats to the proper distribution stats.
   * @since 3.5
   */
  public ThrottledQueueStatHelper getOverflowQueueHelper() {
    return new ThrottledQueueStatHelper() {
        public void incThrottleCount() {
          incOverflowQueueThrottleCount(1);
        }
        public void throttleTime(long nanos) {
          incOverflowQueueThrottleTime(nanos);
        }
        public void add() {
          incOverflowQueueSize(1);
        }
        public void remove() {
          incOverflowQueueSize(-1);
        }
        public void remove(int count) {
          incOverflowQueueSize(-count);
        }
      };
  }

  /**
   * Returns a helper object so that the waiting queue can record its
   * stats to the proper distribution stats.
   * @since 3.5
   */
  public QueueStatHelper getWaitingQueueHelper() {
    return new QueueStatHelper() {
        public void add() {
          incWaitingQueueSize(1);
        }
        public void remove() {
          incWaitingQueueSize(-1);
        }
        public void remove(int count) {
          incWaitingQueueSize(-count);
        }
      };
  }

  /**
   * Returns a helper object so that the high priority queue can record its
   * stats to the proper distribution stats.
   * @since 3.5
   */
  public ThrottledQueueStatHelper getHighPriorityQueueHelper() {
    return new ThrottledQueueStatHelper() {
        public void incThrottleCount() {
          incHighPriorityQueueThrottleCount(1);
        }
        public void throttleTime(long nanos) {
          incHighPriorityQueueThrottleTime(nanos);
        }
        public void add() {
          incHighPriorityQueueSize(1);
        }
        public void remove() {
          incHighPriorityQueueSize(-1);
        }
        public void remove(int count) {
          incHighPriorityQueueSize(-count);
        }
      };
  }

  /**
   * Returns a helper object so that the partitioned region queue can record its
   * stats to the proper distribution stats.
   * @since 5.0
   */
  public ThrottledQueueStatHelper getPartitionedRegionQueueHelper() {
    return new ThrottledQueueStatHelper() {
        public void incThrottleCount() {
          incPartitionedRegionQueueThrottleCount(1);
        }
        public void throttleTime(long nanos) {
          incPartitionedRegionQueueThrottleTime(nanos);
        }
        public void add() {
          incPartitionedRegionQueueSize(1);
        }
        public void remove() {
          incPartitionedRegionQueueSize(-1);
        }
        public void remove(int count) {
          incPartitionedRegionQueueSize(-count);
        }
      };
  }
  /**
   * Returns a helper object so that the partitioned region pool can record its
   * stats to the proper distribution stats.
   * @since 5.0.2
   */
  public PoolStatHelper getPartitionedRegionPoolHelper() {
    return new PoolStatHelper() {
        public void startJob() {
          incPartitionedRegionThreadJobs(1);
        }
        public void endJob() {
          incPartitionedRegionThreadJobs(-1);
        }
      };
  }

  /**
   * Returns a helper object so that the function execution queue can record its
   * stats to the proper distribution stats.
   * @since 6.0
   */
  public ThrottledQueueStatHelper getFunctionExecutionQueueHelper() {
    return new ThrottledQueueStatHelper() {
        public void incThrottleCount() {
          incFunctionExecutionQueueThrottleCount(1);
        }
        public void throttleTime(long nanos) {
          incFunctionExecutionQueueThrottleTime(nanos);
        }
        public void add() {
          incFunctionExecutionQueueSize(1);
        }
        public void remove() {
          incFunctionExecutionQueueSize(-1);
        }
        public void remove(int count) {
          incFunctionExecutionQueueSize(-count);
        }
      };
  }
  /**
   * Returns a helper object so that the function execution pool can record its
   * stats to the proper distribution stats.
   * @since 6.0
   */
  public PoolStatHelper getFunctionExecutionPoolHelper() {
    return new PoolStatHelper() {
        public void startJob() {
          incFunctionExecutionThreadJobs(1);
        }
        public void endJob() {
          incFunctionExecutionThreadJobs(-1);
        }
      };
  }

  /**
   * Returns a helper object so that the serial queue can record its
   * stats to the proper distribution stats.
   * @since 3.5
   */
  public ThrottledMemQueueStatHelper getSerialQueueHelper() {
    return new ThrottledMemQueueStatHelper() {
        public void incThrottleCount() {
          incSerialQueueThrottleCount(1);
        }
        public void throttleTime(long nanos) {
          incSerialQueueThrottleTime(nanos);
        }
        public void add() {
          incSerialQueueSize(1);
        }
        public void remove() {
          incSerialQueueSize(-1);
        }
        public void remove(int count) {
          incSerialQueueSize(-count);
        }
        public void addMem(int amount) {
          incSerialQueueBytes(amount);
        }
        public void removeMem(int amount) {
          incSerialQueueBytes(amount * (-1));
        }
      };
  }

  /**
   * Returns a helper object so that the normal pool can record its
   * stats to the proper distribution stats.
   * @since 3.5
   */
  public PoolStatHelper getNormalPoolHelper() {
    return new PoolStatHelper() {
        public void startJob() {
          incNormalPoolThreadJobs(1);
        }
        public void endJob() {
          incNormalPoolThreadJobs(-1);
        }
      };
  }

  /**
   * Returns a helper object so that the waiting pool can record its
   * stats to the proper distribution stats.
   * @since 3.5
   */
  public PoolStatHelper getWaitingPoolHelper() {
    return new PoolStatHelper() {
        public void startJob() {
          incWaitingPoolThreadJobs(1);
        }
        public void endJob() {
          incWaitingPoolThreadJobs(-1);
        }
      };
  }

  /**
   * Returns a helper object so that the highPriority pool can record its
   * stats to the proper distribution stats.
   * @since 3.5
   */
  public PoolStatHelper getHighPriorityPoolHelper() {
    return new PoolStatHelper() {
        public void startJob() {
          incHighPriorityThreadJobs(1);
        }
        public void endJob() {
          incHighPriorityThreadJobs(-1);
        }
      };
  }

  public void incBatchSendTime(long start) {
    if (enableClockStats) {
      stats.incLong(batchSendTimeId, getStatTime()-start);
    }
  }
  public void incBatchCopyTime(long start) {
    if (enableClockStats) {
      stats.incLong(batchCopyTimeId, getStatTime()-start);
    }
  }
  public void incBatchWaitTime(long start) {
    if (enableClockStats) {
      stats.incLong(batchWaitTimeId, getStatTime()-start);
    }
  }
  public void incBatchFlushTime(long start) {
    if (enableClockStats) {
      stats.incLong(batchFlushTimeId, getStatTime()-start);
    }
  }
  public void incUcastRetransmits() {
    stats.incInt(ucastRetransmitsId, 1);
  }
  public void incMcastRetransmits() {
    stats.incInt(mcastRetransmitsId, 1);
  }
  public void incMcastRetransmitRequests() {
    stats.incInt(mcastRetransmitRequestsId, 1);
  }
  public int getMcastRetransmits() {
    return stats.getInt(mcastRetransmitsId);
  }
  public long startUcastFlush() {
    stats.incInt(ucastFlushesId, 1);
    return getStatTime();
  }
  public void endUcastFlush(long start) {
    if (enableClockStats) {
      stats.incLong(ucastFlushTimeId, getStatTime()-start);
    }
  }
  public void incFlowControlRequests() {
    stats.incInt(flowControlRequestsId, 1);
  }
  public void incFlowControlResponses() {
    stats.incInt(flowControlResponsesId, 1);
  }
  public long startFlowControlWait() {
    stats.incInt(flowControlWaitsInProgressId, 1);
    return 1;
  }
  public void endFlowControlWait(long start) {
    stats.incInt(flowControlWaitsInProgressId, -1);
  }
  public long startFlowControlThrottleWait() {
    stats.incInt(flowControlThrottleWaitsInProgressId, 1);
    return 1;
  }

  public void endFlowControlThrottleWait(long start) {
    stats.incInt(flowControlThrottleWaitsInProgressId, -1);
  }
  public void incJgUNICASTdataReceived(long amount) {
    stats.incLong(jgUNICASTdataReceivedTimeId, amount);
  }

//  public void incjgUDPup(long amount) {
//    stats.incLong(jgUDPupId, amount);
//  }
//  public void incjgUDPdown(long amount) {
//    stats.incLong(jgUDPdownId, amount);
//  }
//  public void incjgNAKACKup(long amount) {
//    stats.incLong(jgNAKACKupId, amount);
//  }
//  public void incjgNAKACKdown(long amount) {
//    stats.incLong(jgNAKACKdownId, amount);
//  }
//  public void incjgUNICASTup(long amount) {
//    stats.incLong(jgUNICASTupId, amount);
//  }
//  public void incjgUNICASTdown(long amount) {
//    stats.incLong(jgUNICASTdownId, amount);
//  }
//  public void incjgSTABLEup(long amount) {
//    stats.incLong(jgSTABLEupId, amount);
//  }
//  public void incjgSTABLEdown(long amount) {
//    stats.incLong(jgSTABLEdownId, amount);
//  }
//  public void incjgFRAG2up(long amount) {
//    stats.incLong(jgFRAG2upId, amount);
//  }
//  public void incjgFRAG2down(long amount) {
//    stats.incLong(jgFRAG2downId, amount);
//  }
//  public void incjgGMSup(long amount) {
//    stats.incLong(jgGMSupId, amount);
//  }
//  public void incjgGMSdown(long amount) {
//    stats.incLong(jgGMSdownId, amount);
//  }
//  public void incjgFCup(long amount) {
//    stats.incLong(jgFCupId, amount);
//  }
//  public void incjgFCdown(long amount) {
//    stats.incLong(jgFCdownId, amount);
//  }

  public void setJgQueuedMessagesSize(long amount) {
    stats.setLong(jgQueuedMessagesSizeId, amount);
  }

  public void setJgSTABLEreceivedMessagesSize(long amount) {
    stats.setLong(jgReceivedMessagesSizeId, amount);
  }
  public void setJgSTABLEsentMessagesSize(long amount) {
    stats.setLong(jgSentMessagesPoolSizeId, amount);
  }
  public void incJgSTABLEsuspendTime(long value)
  {
    stats.incLong(jgSTABLEsuspendTimeId, value);
  }
  public void incJgSTABLEmessages(long value)
  {
    stats.incLong(jgSTABLEmessagesId, value);
  }
  public void incJgSTABLEmessagesSent(long value)
  {
    stats.incLong(jgSTABLEmessagesSentId, value);
  }
  public void incJgSTABILITYmessages(long value)
  {
    stats.incLong(jgSTABILITYmessagesId, value);
  }
  public void incJgFragmentationsPerformed() {
    stats.incLong(jgFragmentationsPerformedId, 1);
  }
  public void incJgFragmentsCreated(long numFrags) {
    stats.incLong(jgFragmentsCreatedId, numFrags);
  }
  public void setJgUNICASTreceivedMessagesSize(long amount) {
    stats.setLong(jgUcastReceivedMessagesSizeId, amount);
  }
  public void setJgUNICASTsentMessagesSize(long amount) {
    stats.setLong(jgUcastSentMessagesSizeId, amount);
  }
  public void setJgUNICASTsentHighPriorityMessagesSize(long amount) {
    stats.setLong(jgUcastSentHighPriorityMessagesSizeId, amount);
  }


  public void incjgDownTime(long value)
  {
    stats.incLong(jgDownTimeId, value);
  }

  public void incjgUpTime(long value)
  {
    stats.incLong(jgUpTimeId, value);
  }

  public void incjChannelUpTime(long value)
  {
    stats.incLong(jChannelUpTimeId, value);
  }

  public void incJgFCsendBlocks(long value)
  {
   stats.incLong(jgFCsendBlocksId, value);

  }
  public void incJgFCautoRequests(long value)
  {
    stats.incLong(jgFCautoRequestsId, value);

  }
  public void incJgFCreplenish(long value)
  {
    stats.incLong(jgFCreplenishId, value);

  }
  public void incJgFCresumes(long value)
  {
   stats.incLong(jgFCresumesId, value);
  }
  public void incJgFCsentCredits(long value)
  {
    stats.incLong(jgFCsentCreditsId, value);
  }
  public void incJgFCsentThrottleRequests(long value)
  {
    stats.incLong(jgFCsentThrottleRequestsId, value);
  }

  public void incJgNAKACKwaits(long value) {
    stats.incLong(jgNAKACKwaitsId, value);
  }

  public void incThreadOwnedReceivers(long value, int dominoCount)
  {
    if (dominoCount < 2) {
      stats.incLong(threadOwnedReceiversId, value);
    } else {
      stats.incLong(threadOwnedReceiversId2, value);
    }
  }

  /**
   * @since 5.0.2.4
   */
  public void incReceiverBufferSize(int inc, boolean direct) {
    if (direct) {
      stats.incLong(receiverDirectBufferSizeId, inc);
    } else {
      stats.incLong(receiverHeapBufferSizeId, inc);
    }
  }
  /**
   * @since 5.0.2.4
   */
  public void incSenderBufferSize(int inc, boolean direct) {
    if (direct) {
      stats.incLong(senderDirectBufferSizeId, inc);
    } else {
      stats.incLong(senderHeapBufferSizeId, inc);
    }
  }
  public void incMessagesBeingReceived(boolean newMsg, int bytes) {
    if (newMsg) {
      stats.incInt(messagesBeingReceivedId, 1);
    }
    stats.incLong(messageBytesBeingReceivedId, bytes);
  }
  public void decMessagesBeingReceived(int bytes) {
    stats.incInt(messagesBeingReceivedId, -1);
    stats.incLong(messageBytesBeingReceivedId, -bytes);
  }
  public void incSerialThreadStarts() {
    stats.incLong(serialThreadStartsId, 1);
  }
  public void incViewThreadStarts() {
    stats.incLong(viewThreadStartsId, 1);
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

  public void incReplyHandOffTime(long start) {
    if (enableClockStats) {
      long delta = getStatTime() - start;
      stats.incLong(replyHandoffTimeId, delta);
//      this.replyHandoffHistogram.endOp(delta);
    }
  }

  protected void incPartitionedRegionThreadJobs(int i) {
    this.stats.incInt(partitionedRegionThreadJobsId, i);
  }

  protected void incFunctionExecutionThreadJobs(int i) {
    this.stats.incInt(functionExecutionThreadJobsId, i);
  }

  public void incNumViewThreads(int threads) {
    this.stats.incInt(viewThreadsId, threads);
  }

  public PoolStatHelper getSerialProcessorHelper() {
    return new PoolStatHelper() {
      public void startJob() {
        incNumSerialThreadJobs(1);
        if (logger.isTraceEnabled()) {
          logger.trace("[DM.SerialQueuedExecutor.execute] numSerialThreads={}", getNumSerialThreads());
        }
      }
      public void endJob() {
        incNumSerialThreadJobs(-1);
      }
    };
  }

  protected void incNumSerialThreadJobs(int jobs) {
    this.stats.incInt(serialThreadJobsId, jobs);
  }

  public PoolStatHelper getViewProcessorHelper() {
    return new PoolStatHelper() {
      public void startJob() {
        incViewProcessorThreadJobs(1);
        if (logger.isTraceEnabled()) {
          logger.trace("[DM.SerialQueuedExecutor.execute] numViewThreads={}", getNumViewThreads());
        }
      }
      public void endJob() {
        incViewProcessorThreadJobs(-1);
      }
    };
  }

  public int getNumViewThreads() {
    return this.stats.getInt(viewThreadsId);
  }

  protected void incViewProcessorThreadJobs(int jobs) {
    this.stats.incInt(viewProcessorThreadJobsId, jobs);
  }

  public PoolStatHelper getSerialPooledProcessorHelper() {
    return new PoolStatHelper() {
      public void startJob() {
        incSerialPooledProcessorThreadJobs(1);
      }
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

  public int getElders() {
    return this.stats.getInt(eldersId);
  }
  public void incElders(int val) {
    this.stats.incInt(eldersId, val);
  }
  
  public int getInitialImageMessagesInFlight() {
    return this.stats.getInt(initialImageMessagesInFlightId);
  }
  public void incInitialImageMessagesInFlight(int val) {
    this.stats.incInt(initialImageMessagesInFlightId, val);
  }
  
  public int getInitialImageRequestsInProgress() {
    return this.stats.getInt(initialImageRequestsInProgressId);
  }
  public void incInitialImageRequestsInProgress(int val) {
    this.stats.incInt(initialImageRequestsInProgressId, val);
  }
  
  public Statistics getStats(){
    return stats;
  }
}
