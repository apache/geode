/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.distributed.internal;

/**
 * Defines the interface used to access and modify DM statistics.
 *
 *
 */
public interface DMStats {

  /**
   * Returns the total number of messages sent by the distribution
   * manager 
   */
  public long getSentMessages();

  /**
   * Increments the total number of messages sent by the distribution
   * manager 
   */
  public void incSentMessages(long messages);

  public void incTOSentMsg();
  /**
   * Returns the total number of transaction commit messages
   * sent by the distribution manager 
   */
  public long getSentCommitMessages();

  /**
   * Increments the total number of transaction commit messages
   * sent by the distribution manager 
   */
  public void incSentCommitMessages(long messages);

  /**
   * Returns the total number commits that had to wait for an ack
   * before completion.
   */
  public long getCommitWaits();

  /**
   * Increments the number of commit waits by one.
   */
  public void incCommitWaits();

  /**
   * Returns the total number of nanoseconds spent sending messages.
   */
  public long getSentMessagesTime();

  /**
   * Increments the total number of nanoseconds spent sending messages.
   */
  public void incSentMessagesTime(long nanos);

  /**
   * Returns the total number of messages broadcast by the distribution
   * manager 
   */
  public long getBroadcastMessages();

  /**
   * Increments the total number of messages broadcast by the distribution
   * manager 
   */
  public void incBroadcastMessages(long messages);

  /**
   * Returns the total number of nanoseconds spent sending messages.
   */
  public long getBroadcastMessagesTime();

  /**
   * Increments the total number of nanoseconds spend sending messages.
   */
  public void incBroadcastMessagesTime(long nanos);

  /**
   * Returns the total number of messages received by the distribution
   * manager
   */
  public long getReceivedMessages();

  /**
   * Increments the total number of messages received by the distribution
   * manager 
   */
  public void incReceivedMessages(long messages);

  /**
   * Returns the total number of message bytes received by the distribution
   * manager
   */
  public long getReceivedBytes();

  /**
   * Increments the total number of message bytes received by the distribution
   * manager 
   */
  public void incReceivedBytes(long bytes);

  /**
   * Increments the total number of message bytes sent by the distribution
   * manager 
   */
  public void incSentBytes(long bytes);

  /**
   * Returns the total number of messages processed by the distribution
   * manager
   */
  public long getProcessedMessages();

  /**
   * Increments the total number of messages processed by the distribution
   * manager 
   */
  public void incProcessedMessages(long messages);

  /**
   * Returns the total number of nanoseconds spent processing messages.
   */
  public long getProcessedMessagesTime();

  /**
   * Increments the total number of nanoseconds spent processing messages.
   */
  public void incProcessedMessagesTime(long nanos);

  /**
   * Returns the total number of nanoseconds spent scheduling messages to be processed.
   */
  public long getMessageProcessingScheduleTime();

  public void incBatchSendTime(long start);
  public void incBatchCopyTime(long start);
  public void incBatchWaitTime(long start);
  public void incBatchFlushTime(long start);
  /**
   * Increments the total number of nanoseconds spent scheduling messages to be processed.
   */
  public void incMessageProcessingScheduleTime(long nanos);

  public int getOverflowQueueSize();
  
  public void incOverflowQueueSize(int messages);
  
  public int getNumProcessingThreads();
  
  public void incNumProcessingThreads(int threads);

  public int getNumSerialThreads();
  
  public void incNumSerialThreads(int threads);

  public void incMessageChannelTime(long val);

  public void incUDPDispatchRequestTime(long val);
  public long getUDPDispatchRequestTime();
  
  public long getReplyMessageTime();
  public void incReplyMessageTime(long val);

  public long getDistributeMessageTime();
  public void incDistributeMessageTime(long val);
  
  public long startSocketWrite(boolean sync);
  public void endSocketWrite(boolean sync, long start, int bytesWritten, int retries);
  /**
   * increments
   * the number of unicast writes performed and the number of bytes written
   * @since GemFire 5.0
   */
  public void incUcastWriteBytes(int bytesWritten);
  /**
   * increment the number of unicast datagram payload bytes received and
   * the number of unicast reads performed
   */
  public void incUcastReadBytes(int amount);
  /**
   * increment the number of multicast datagrams sent and
   * the number of multicast bytes transmitted
   */
  public void incMcastWriteBytes(int bytesWritten);
  /**
   * increment the number of multicast datagram payload bytes received, and
   * the number of mcast messages read
   */
  public void incMcastReadBytes(int amount);
  /**
   * returns the current value of the mcastWrites statistic
   */
  public int getMcastWrites();
  public int getMcastReads();

  public long startSerialization();
  public void endSerialization(long start, int bytes);
  public long startDeserialization();
  public void endDeserialization(long start, int bytes);
  public long startMsgSerialization();
  public void endMsgSerialization(long start);
  public long startUDPMsgEncryption();
  public void endUDPMsgEncryption(long start);
  public long startUDPMsgDecryption();
  public void endUDPMsgDecryption(long start);
  public long startMsgDeserialization();
  public void endMsgDeserialization(long start);
  
  public long getUDPMsgEncryptionTiime();
  public long getUDPMsgDecryptionTime();

  public int getNodes();
  public void setNodes(int val);
  public void incNodes(int val);
  public int getReplyWaitsInProgress();
  public int getReplyWaitsCompleted();
  public long getReplyWaitTime();
  /**
   * @return the timestamp that marks the start of the operation
   */
  public long startReplyWait();
  /**
   * @param startNanos the timestamp taken when the operation started
   * @param initTime the time the operation begain (before msg transmission) 
   */
  public void endReplyWait(long startNanos, long initTime);

  /**
   * Increments the number of message replies that have timed out
   *
   * @since GemFire 3.5
   */
  public void incReplyTimeouts();

  /**
   * Returns the number of message replies that have timed out
   *
   * @since GemFire 3.5
   */
  public long getReplyTimeouts();

  /**
   * @since GemFire 4.1
   */
  public void incReceivers();
  /**
   * @since GemFire 4.1
   */
  public void decReceivers();
  /**
   * @since GemFire 4.1
   */
  public void incFailedAccept();
  /**
   * @since GemFire 4.1
   */
  public void incFailedConnect();
  /**
   * @since GemFire 4.1.1
   */
  public void incReconnectAttempts();
  /**
   * @since GemFire 4.1
   */
  public void incLostLease();
  /**
   * @since GemFire 4.1
   */
  public void incSenders(boolean shared, boolean preserveOrder);
  /**
   * @since GemFire 4.1
   */
  public void decSenders(boolean shared, boolean preserveOrder);
  /**
   * @since GemFire 4.1
   */
  public int getSendersSU();
  /**
   * increment the number of unicast UDP retransmission requests received from
   * other processes
   * @since GemFire 5.0
   */
  public void incUcastRetransmits();
  /**
   * increment the number of multicast UDP retransmissions sent to
   * other processes
   * @since GemFire 5.0
   */
  public void incMcastRetransmits();
  /**
   * returns the current number of multicast retransmission requests
   * processed
   */
  public int getMcastRetransmits();
  /**
   * increment the number of multicast UDP retransmission requests sent to
   * other processes
   * @since GemFire 5.0
   */
  public void incMcastRetransmitRequests();

  /**
   * @since GemFire 4.2.2
   */
  public int getAsyncSocketWritesInProgress();
  /**
   * @since GemFire 4.2.2
   */
  public int getAsyncSocketWrites();
  /**
   * @since GemFire 4.2.2
   */
  public int getAsyncSocketWriteRetries();
  /**
   * @since GemFire 4.2.2
   */
  public long getAsyncSocketWriteBytes();
  /**
   * @since GemFire 4.2.2
   */
  public long getAsyncSocketWriteTime();
  /**
   * @since GemFire 4.2.2
   */
  public int getAsyncQueues();
  /**
   * @since GemFire 4.2.2
   */
  public void incAsyncQueues(int inc);
  /**
   * @since GemFire 4.2.2
   */
  public int getAsyncQueueFlushesInProgress();
  /**
   * @since GemFire 4.2.2
   */
  public int getAsyncQueueFlushesCompleted();
  /**
   * @since GemFire 4.2.2
   */
  public long getAsyncQueueFlushTime();
  /**
   * @since GemFire 4.2.2
   */
  public long startAsyncQueueFlush();
  /**
   * @since GemFire 4.2.2
   */
  public void endAsyncQueueFlush(long start);
  /**
   * @since GemFire 4.2.2
   */
  public int getAsyncQueueTimeouts();
  /**
   * @since GemFire 4.2.2
   */
  public void incAsyncQueueTimeouts(int inc);
  /**
   * @since GemFire 4.2.2
   */
  public int getAsyncQueueSizeExceeded();
  /**
   * @since GemFire 4.2.2
   */
  public void incAsyncQueueSizeExceeded(int inc);
  /**
   * @since GemFire 4.2.2
   */
  public int getAsyncDistributionTimeoutExceeded();
  /**
   * @since GemFire 4.2.2
   */
  public void incAsyncDistributionTimeoutExceeded();
  /**
   * @since GemFire 4.2.2
   */
  public long getAsyncQueueSize();
  /**
   * @since GemFire 4.2.2
   */
  public void incAsyncQueueSize(long inc);
  /**
   * @since GemFire 4.2.2
   */
  public long getAsyncQueuedMsgs();
  /**
   * @since GemFire 4.2.2
   */
  public void incAsyncQueuedMsgs();
  /**
   * @since GemFire 4.2.2
   */
  public long getAsyncDequeuedMsgs();
  /**
   * @since GemFire 4.2.2
   */
  public void incAsyncDequeuedMsgs();
  /**
   * @since GemFire 4.2.2
   */
  public long getAsyncConflatedMsgs();
  /**
   * @since GemFire 4.2.2
   */
  public void incAsyncConflatedMsgs();
  /**
   * @since GemFire 4.2.2
   */
  public int getAsyncThreads();
  /**
   * @since GemFire 4.2.2
   */
  public void incAsyncThreads(int inc);
  /**
   * @since GemFire 4.2.2
   */
  public int getAsyncThreadInProgress();
  /**
   * @since GemFire 4.2.2
   */
  public int getAsyncThreadCompleted();
  /**
   * @since GemFire 4.2.2
   */
  public long getAsyncThreadTime();
  /**
   * @since GemFire 4.2.2
   */
  public long startAsyncThread();
  /**
   * @since GemFire 4.2.2
   */
  public void endAsyncThread(long start);
  /**
   * @since GemFire 4.2.2
   */
  public long getAsyncQueueAddTime();
  /**
   * @since GemFire 4.2.2
   */
  public void incAsyncQueueAddTime(long inc);
  /**
   * @since GemFire 4.2.2
   */
  public long getAsyncQueueRemoveTime();
  /**
   * @since GemFire 4.2.2
   */
  public void incAsyncQueueRemoveTime(long inc);

  /**
   * @since GemFire 5.0.2.4
   */
  public void incReceiverBufferSize(int inc, boolean direct);
  /**
   * @since GemFire 5.0.2.4
   */
  public void incSenderBufferSize(int inc, boolean direct);
  /**
   * @since GemFire 5.0.2.4
   */
  public long startSocketLock();
  /**
   * @since GemFire 5.0.2.4
   */
  public void endSocketLock(long start);

  /**
   * @since GemFire 5.0.2.4
   */
  public long startBufferAcquire();
  /**
   * @since GemFire 5.0.2.4
   */
  public void endBufferAcquire(long start);

  /**
   * increment/decrement the number of thread-owned receivers with the given
   * domino count
   * @param value
   * @param dominoCount thread-owned connection chain count
   */
  public void incThreadOwnedReceivers(long value, int dominoCount);
  
  /**
   * Called when a new message is received.
   * @param newMsg true if a new message being received was detected; false if
   *   this is just additional data for a message already detected.
   * @param bytes the number of bytes read, so far, for the message being received. 
   * @since GemFire 5.0.2
   */
  public void incMessagesBeingReceived(boolean newMsg, int bytes);
  /**
   * Called when we finish processing a received message.
   * @param bytes the number of bytes read off the wire for the message we have finished with.
   * @since GemFire 5.0.2
   */
  public void decMessagesBeingReceived(int bytes);

  public void incReplyHandOffTime(long start);
  
  /**
   * Returns 1 if the system elder is this member, else returns 0.
   * @return 1 if the system elder is this member, else returns 0
   */
  public int getElders();
  public void incElders(int val);
  
  /**
   * Returns the number of initial image reply messages sent from this member
   * which have not yet been acked.  
   */
  public int getInitialImageMessagesInFlight();

  public void incInitialImageMessagesInFlight(int val);
  
  /**
   * Returns the number of initial images this member is currently requesting. 
   */
  public int getInitialImageRequestsInProgress();
  
  public void incInitialImageRequestsInProgress(int val);

  public void incPdxSerialization(int bytesWritten);

  public void incPdxDeserialization(int i);

  public long startPdxInstanceDeserialization();

  public void endPdxInstanceDeserialization(long start);
  public void incPdxInstanceCreations();
  
  //Stats for GMSHealthMonitor
  public long getHeartbeatRequestsSent();
  
  public void incHeartbeatRequestsSent();
  
  public long getHeartbeatRequestsReceived();
  
  public void incHeartbeatRequestsReceived();
  
  public long getHeartbeatsSent();
  
  public void incHeartbeatsSent();

  public long getHeartbeatsReceived();
  
  public void incHeartbeatsReceived();
  

  public long getSuspectsSent();
  
  public void incSuspectsSent();

  public long getSuspectsReceived();
  
  public void incSuspectsReceived();
  
  
  public long getFinalCheckRequestsSent();
  
  public void incFinalCheckRequestsSent();
  
  public long getFinalCheckRequestsReceived();
  
  public void incFinalCheckRequestsReceived();
  
  public long getFinalCheckResponsesSent();
  
  public void incFinalCheckResponsesSent();
  
  public long getFinalCheckResponsesReceived();
  
  public void incFinalCheckResponsesReceived();
  
  
  public long getTcpFinalCheckRequestsSent();
  
  public void incTcpFinalCheckRequestsSent();

  public long getTcpFinalCheckRequestsReceived();
  
  public void incTcpFinalCheckRequestsReceived();
  
  public long getTcpFinalCheckResponsesSent();
  
  public void incTcpFinalCheckResponsesSent();

  public long getTcpFinalCheckResponsesReceived();
  
  public void incTcpFinalCheckResponsesReceived();

  
  public long getUdpFinalCheckRequestsSent();
  
  public void incUdpFinalCheckRequestsSent();
  
//  UDP final check is implemented using HeartbeatRequestMessage and HeartbeatMessage
//  So the following code is commented out.
  
//  public long getUdpFinalCheckRequestsReceived();
//  
//  public void incUdpFinalCheckRequestsReceived();
//  
//  public long getUdpFinalCheckResponsesSent();
//  
//  public void incUdpFinalCheckResponsesSent();

  public long getUdpFinalCheckResponsesReceived();
  
  public void incUdpFinalCheckResponsesReceived();
}
