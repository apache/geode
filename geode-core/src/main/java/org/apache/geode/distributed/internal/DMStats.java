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

  int getOverflowQueueSize();

  void incOverflowQueueSize(int messages);

  int getNumProcessingThreads();

  void incNumProcessingThreads(int threads);

  int getNumSerialThreads();

  void incNumSerialThreads(int threads);

  void incMessageChannelTime(long val);

  long getUDPDispatchRequestTime();

  long getReplyMessageTime();

  void incReplyMessageTime(long val);

  long getDistributeMessageTime();

  void incDistributeMessageTime(long val);

  long startSocketWrite(boolean sync);

  void endSocketWrite(boolean sync, long start, int bytesWritten, int retries);

  /**
   * returns the current value of the mcastWrites statistic
   */
  int getMcastWrites();

  int getMcastReads();

  long startSerialization();

  void endSerialization(long start, int bytes);

  long startDeserialization();

  void endDeserialization(long start, int bytes);

  long getUDPMsgEncryptionTiime();

  long getUDPMsgDecryptionTime();

  int getNodes();

  void setNodes(int val);

  void incNodes(int val);

  int getReplyWaitsInProgress();

  int getReplyWaitsCompleted();

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


  int getReconnectAttempts();

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
  int getSendersSU();

  /**
   * returns the current number of multicast retransmission requests processed
   */
  int getMcastRetransmits();

  /**
   * @since GemFire 4.2.2
   */
  int getAsyncSocketWritesInProgress();

  /**
   * @since GemFire 4.2.2
   */
  int getAsyncSocketWrites();

  /**
   * @since GemFire 4.2.2
   */
  int getAsyncSocketWriteRetries();

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
  int getAsyncQueues();

  /**
   * @since GemFire 4.2.2
   */
  void incAsyncQueues(int inc);

  /**
   * @since GemFire 4.2.2
   */
  int getAsyncQueueFlushesInProgress();

  /**
   * @since GemFire 4.2.2
   */
  int getAsyncQueueFlushesCompleted();

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
  int getAsyncQueueTimeouts();

  /**
   * @since GemFire 4.2.2
   */
  void incAsyncQueueTimeouts(int inc);

  /**
   * @since GemFire 4.2.2
   */
  int getAsyncQueueSizeExceeded();

  /**
   * @since GemFire 4.2.2
   */
  void incAsyncQueueSizeExceeded(int inc);

  /**
   * @since GemFire 4.2.2
   */
  int getAsyncDistributionTimeoutExceeded();

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
  int getAsyncThreads();

  /**
   * @since GemFire 4.2.2
   */
  void incAsyncThreads(int inc);

  /**
   * @since GemFire 4.2.2
   */
  int getAsyncThreadInProgress();

  /**
   * @since GemFire 4.2.2
   */
  int getAsyncThreadCompleted();

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
  void incReceiverBufferSize(int inc, boolean direct);

  /**
   * @since GemFire 5.0.2.4
   */
  void incSenderBufferSize(int inc, boolean direct);

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
  void incThreadOwnedReceivers(long value, int dominoCount);

  /**
   * Called when a new message is received.
   *
   * @param newMsg true if a new message being received was detected; false if this is just
   *        additional data for a message already detected.
   * @param bytes the number of bytes read, so far, for the message being received.
   * @since GemFire 5.0.2
   */
  void incMessagesBeingReceived(boolean newMsg, int bytes);

  /**
   * Called when we finish processing a received message.
   *
   * @param bytes the number of bytes read off the wire for the message we have finished with.
   * @since GemFire 5.0.2
   */
  void decMessagesBeingReceived(int bytes);

  void incReplyHandOffTime(long start);

  /**
   * Returns 1 if the system elder is this member, else returns 0.
   *
   * @return 1 if the system elder is this member, else returns 0
   */
  int getElders();

  void incElders(int val);

  /**
   * Returns the number of initial image reply messages sent from this member which have not yet
   * been acked.
   */
  int getInitialImageMessagesInFlight();

  void incInitialImageMessagesInFlight(int val);

  /**
   * Returns the number of initial images this member is currently requesting.
   */
  int getInitialImageRequestsInProgress();

  void incInitialImageRequestsInProgress(int val);

  void incPdxSerialization(int bytesWritten);

  void incPdxDeserialization(int i);

  long startPdxInstanceDeserialization();

  void endPdxInstanceDeserialization(long start);

  void incPdxInstanceCreations();

  long getHeartbeatsReceived();

  long getUdpFinalCheckResponsesReceived();

  long startSenderCreate();
}
