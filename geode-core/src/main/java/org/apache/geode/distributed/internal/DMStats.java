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

import org.apache.geode.distributed.internal.membership.api.MembershipStatistics;

/**
 * Defines the interface used to access and modify DM statistics.
 *
 *
 */
public interface DMStats extends MembershipStatistics {

  /**
   * Returns the total number of messages sent by the distribution manager
   */
  long getSentMessages();

  /**
   * Increments the total number of messages sent by the distribution manager
   */
  void incSentMessages(long messages);

  void incTOSentMsg();

  /**
   * Returns the total number of transaction commit messages sent by the distribution manager
   */
  long getSentCommitMessages();

  /**
   * Increments the total number of transaction commit messages sent by the distribution manager
   */
  void incSentCommitMessages(long messages);

  /**
   * Returns the total number commits that had to wait for an ack before completion.
   */
  long getCommitWaits();

  /**
   * Increments the number of commit waits by one.
   */
  void incCommitWaits();

  /**
   * Returns the total number of nanoseconds spent sending messages.
   */
  long getSentMessagesTime();

  /**
   * Increments the total number of nanoseconds spent sending messages.
   */
  void incSentMessagesTime(long nanos);

  /**
   * Returns the total number of messages broadcast by the distribution manager
   */
  long getBroadcastMessages();

  /**
   * Increments the total number of messages broadcast by the distribution manager
   */
  void incBroadcastMessages(long messages);

  /**
   * Returns the total number of nanoseconds spent sending messages.
   */
  long getBroadcastMessagesTime();

  /**
   * Increments the total number of nanoseconds spend sending messages.
   */
  void incBroadcastMessagesTime(long nanos);

  /**
   * Returns the total number of messages received by the distribution manager
   */
  long getReceivedMessages();

  /**
   * Increments the total number of messages received by the distribution manager
   */
  void incReceivedMessages(long messages);

  /**
   * Returns the total number of message bytes received by the distribution manager
   */
  long getReceivedBytes();

  /**
   * Increments the total number of message bytes received by the distribution manager
   */
  void incReceivedBytes(long bytes);

  /**
   * Returns the total number of messages processed by the distribution manager
   */
  long getProcessedMessages();

  /**
   * Increments the total number of messages processed by the distribution manager
   */
  void incProcessedMessages(long messages);

  /**
   * Returns the total number of nanoseconds spent processing messages.
   */
  long getProcessedMessagesTime();

  /**
   * Increments the total number of nanoseconds spent processing messages.
   */
  void incProcessedMessagesTime(long nanos);

  /**
   * Returns the total number of nanoseconds spent scheduling messages to be processed.
   */
  long getMessageProcessingScheduleTime();

  void incBatchSendTime(long start);

  void incBatchCopyTime(long start);

  void incBatchWaitTime(long start);

  void incBatchFlushTime(long start);

  /**
   * Increments the total number of nanoseconds spent scheduling messages to be processed.
   */
  void incMessageProcessingScheduleTime(long nanos);

  long getOverflowQueueSize();

  void incOverflowQueueSize(long messages);

  long getNumProcessingThreads();

  void incNumProcessingThreads(long threads);

  long getNumSerialThreads();

  void incNumSerialThreads(long threads);

  void incMessageChannelTime(long val);

  long getUDPDispatchRequestTime();

  long getReplyMessageTime();

  void incReplyMessageTime(long val);

  long getDistributeMessageTime();

  void incDistributeMessageTime(long val);

  long startSocketWrite(boolean sync);

  void endSocketWrite(boolean sync, long start, long bytesWritten, long retries);

  /**
   * returns the current value of the mcastWrites statistic
   */
  long getMcastWrites();

  long getMcastReads();

  long startSerialization();

  void endSerialization(long start, int bytes);

  long startDeserialization();

  void endDeserialization(long start, int bytes);

  long getUDPMsgEncryptionTime();

  long getUDPMsgDecryptionTime();

  long getNodes();

  void setNodes(long val);

  void incNodes(long val);

  long getReplyWaitsInProgress();

  long getReplyWaitsCompleted();

  long getReplyWaitTime();

  /**
   * @return the timestamp that marks the start of the operation
   */
  long startReplyWait();

  /**
   * @param startNanos the timestamp taken when the operation started
   * @param initTime the time the operation begain (before msg transmission)
   */
  void endReplyWait(long startNanos, long initTime);

  /**
   * Increments the number of message replies that have timed out
   *
   * @since GemFire 3.5
   */
  void incReplyTimeouts();

  /**
   * Returns the number of message replies that have timed out
   *
   * @since GemFire 3.5
   */
  long getReplyTimeouts();

  /**
   * @since GemFire 4.1
   */
  void incReceivers();

  /**
   * @since GemFire 4.1
   */
  void decReceivers();

  /**
   * @since GemFire 4.1
   */
  void incFailedAccept();

  /**
   * @since GemFire 4.1
   */
  void incFailedConnect();

  /**
   * @since GemFire 4.1.1
   */
  void incReconnectAttempts();


  long getReconnectAttempts();

  /**
   * @since GemFire 4.1
   */
  void incLostLease();

  /**
   * @since GemFire 4.1
   */
  void incSenders(boolean shared, boolean preserveOrder, long start);

  /**
   * @since GemFire 4.1
   */
  void decSenders(boolean shared, boolean preserveOrder);

  /**
   * @since GemFire 4.1
   */
  long getSendersSU();

  /**
   * returns the current number of multicast retransmission requests processed
   */
  long getMcastRetransmits();

  /**
   * @since GemFire 4.2.2
   */
  long getAsyncSocketWritesInProgress();

  /**
   * @since GemFire 4.2.2
   */
  long getAsyncSocketWrites();

  /**
   * @since GemFire 4.2.2
   */
  long getAsyncSocketWriteRetries();

  /**
   * @since GemFire 4.2.2
   */
  long getAsyncSocketWriteBytes();

  /**
   * @since GemFire 4.2.2
   */
  long getAsyncSocketWriteTime();

  /**
   * @since GemFire 4.2.2
   */
  long getAsyncQueues();

  /**
   * @since GemFire 4.2.2
   */
  void incAsyncQueues(long inc);

  /**
   * @since GemFire 4.2.2
   */
  long getAsyncQueueFlushesInProgress();

  /**
   * @since GemFire 4.2.2
   */
  long getAsyncQueueFlushesCompleted();

  /**
   * @since GemFire 4.2.2
   */
  long getAsyncQueueFlushTime();

  /**
   * @since GemFire 4.2.2
   */
  long startAsyncQueueFlush();

  /**
   * @since GemFire 4.2.2
   */
  void endAsyncQueueFlush(long start);

  /**
   * @since GemFire 4.2.2
   */
  long getAsyncQueueTimeouts();

  /**
   * @since GemFire 4.2.2
   */
  void incAsyncQueueTimeouts(long inc);

  /**
   * @since GemFire 4.2.2
   */
  long getAsyncQueueSizeExceeded();

  /**
   * @since GemFire 4.2.2
   */
  void incAsyncQueueSizeExceeded(long inc);

  /**
   * @since GemFire 4.2.2
   */
  long getAsyncDistributionTimeoutExceeded();

  /**
   * @since GemFire 4.2.2
   */
  void incAsyncDistributionTimeoutExceeded();

  /**
   * @since GemFire 4.2.2
   */
  long getAsyncQueueSize();

  /**
   * @since GemFire 4.2.2
   */
  void incAsyncQueueSize(long inc);

  /**
   * @since GemFire 4.2.2
   */
  long getAsyncQueuedMsgs();

  /**
   * @since GemFire 4.2.2
   */
  void incAsyncQueuedMsgs();

  /**
   * @since GemFire 4.2.2
   */
  long getAsyncDequeuedMsgs();

  /**
   * @since GemFire 4.2.2
   */
  void incAsyncDequeuedMsgs();

  /**
   * @since GemFire 4.2.2
   */
  long getAsyncConflatedMsgs();

  /**
   * @since GemFire 4.2.2
   */
  void incAsyncConflatedMsgs();

  /**
   * @since GemFire 4.2.2
   */
  long getAsyncThreads();

  /**
   * @since GemFire 4.2.2
   */
  void incAsyncThreads(long inc);

  /**
   * @since GemFire 4.2.2
   */
  long getAsyncThreadInProgress();

  /**
   * @since GemFire 4.2.2
   */
  long getAsyncThreadCompleted();

  /**
   * @since GemFire 4.2.2
   */
  long getAsyncThreadTime();

  /**
   * @since GemFire 4.2.2
   */
  long startAsyncThread();

  /**
   * @since GemFire 4.2.2
   */
  void endAsyncThread(long start);

  /**
   * @since GemFire 4.2.2
   */
  long getAsyncQueueAddTime();

  /**
   * @since GemFire 4.2.2
   */
  void incAsyncQueueAddTime(long inc);

  /**
   * @since GemFire 4.2.2
   */
  long getAsyncQueueRemoveTime();

  /**
   * @since GemFire 4.2.2
   */
  void incAsyncQueueRemoveTime(long inc);

  /**
   * @since GemFire 5.0.2.4
   */
  void incReceiverBufferSize(long inc, boolean direct);

  /**
   * @since GemFire 5.0.2.4
   */
  void incSenderBufferSize(long inc, boolean direct);

  /**
   * @since GemFire 5.0.2.4
   */
  long startSocketLock();

  /**
   * @since GemFire 5.0.2.4
   */
  void endSocketLock(long start);

  /**
   * @since GemFire 5.0.2.4
   */
  long startBufferAcquire();

  /**
   * @since GemFire 5.0.2.4
   */
  void endBufferAcquire(long start);

  /**
   * increment/decrement the number of thread-owned receivers with the given domino count
   *
   * @param dominoCount thread-owned connection chain count
   */
  void incThreadOwnedReceivers(long value, long dominoCount);

  /**
   * Called when a new message is received.
   *
   * @param newMsg true if a new message being received was detected; false if this is just
   *        additional data for a message already detected.
   * @param bytes the number of bytes read, so far, for the message being received.
   * @since GemFire 5.0.2
   */
  void incMessagesBeingReceived(boolean newMsg, long bytes);

  /**
   * Called when we finish processing a received message.
   *
   * @param bytes the number of bytes read off the wire for the message we have finished with.
   * @since GemFire 5.0.2
   */
  void decMessagesBeingReceived(long bytes);

  void incReplyHandOffTime(long start);

  /**
   * Returns 1 if the system elder is this member, else returns 0.
   *
   * @return 1 if the system elder is this member, else returns 0
   */
  long getElders();

  void incElders(long val);

  /**
   * Returns the number of initial image reply messages sent from this member which have not yet
   * been acked.
   */
  long getInitialImageMessagesInFlight();

  void incInitialImageMessagesInFlight(long val);

  /**
   * Returns the number of initial images this member is currently requesting.
   */
  long getInitialImageRequestsInProgress();

  void incInitialImageRequestsInProgress(long val);

  void incPdxSerialization(long bytesWritten);

  void incPdxDeserialization(long i);

  long startPdxInstanceDeserialization();

  void endPdxInstanceDeserialization(long start);

  void incPdxInstanceCreations();

  long getHeartbeatsReceived();

  long getUdpFinalCheckResponsesReceived();

  long startSenderCreate();
}
