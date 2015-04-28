/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed.internal;

/**
 * Defines the interface used to access and modify DM statistics.
 *
 * @author Darrel Schneider
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
  
  public long getReplyMessageTime();
  public void incReplyMessageTime(long val);

  public long getDistributeMessageTime();
  public void incDistributeMessageTime(long val);
  
  public long startSocketWrite(boolean sync);
  public void endSocketWrite(boolean sync, long start, int bytesWritten, int retries);
  /**
   * begin a unicast datagram write operation.  Use the result of this operation
   * when calling endUcastWrite
   * @since 5.0
   */
  public long startUcastWrite();
  /**
   * record the end of a unicast datagram write operation and increment
   * the number of writes performed and the number of bytes written
   * @since 5.0
   */
  public void endUcastWrite(long start, int bytesWritten);
  /**
   * increment the number of unicast datagram payload bytes received and
   * the number of unicast reads performed
   */
  public void incUcastReadBytes(long amount);
  /**
   * begin a multicast write operation.  Use the result of this operation
   * when calling endMcastWrite
   * @since 5.0
   */
  public long startMcastWrite();
  /**
   * record the end of a multicast datagram write operation
   * @since 5.0
   */
  public void endMcastWrite(long start, int bytesWritten);
  /**
   * increment the number of multicast datagram payload bytes received, and
   * the number of mcast messages read
   */
  public void incMcastReadBytes(long amount);
  /**
   * returns the current value of the mcastWrites statistic
   */
  public int getMcastWrites();

  public long startSerialization();
  public void endSerialization(long start, int bytes);
  public long startDeserialization();
  public void endDeserialization(long start, int bytes);
  public long startMsgSerialization();
  public void endMsgSerialization(long start);
  public long startMsgDeserialization();
  public void endMsgDeserialization(long start);

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
   * @since 3.5
   */
  public void incReplyTimeouts();

  /**
   * Returns the number of message replies that have timed out
   *
   * @since 3.5
   */
  public long getReplyTimeouts();

  /**
   * @since 4.1
   */
  public void incReceivers();
  /**
   * @since 4.1
   */
  public void decReceivers();
  /**
   * @since 4.1
   */
  public void incFailedAccept();
  /**
   * @since 4.1
   */
  public void incFailedConnect();
  /**
   * @since 4.1.1
   */
  public void incReconnectAttempts();
  /**
   * @since 4.1
   */
  public void incLostLease();
  /**
   * @since 4.1
   */
  public void incSenders(boolean shared, boolean preserveOrder);
  /**
   * @since 4.1
   */
  public void decSenders(boolean shared, boolean preserveOrder);
  /**
   * @since 4.1
   */
  public int getSendersSU();
  /**
   * increment the number of unicast UDP retransmissions sent to
   * other processes
   * @since 5.0
   */
  public void incUcastRetransmits();
  /**
   * increment the number of multicast UDP retransmissions sent to
   * other processes
   * @since 5.0
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
   * @since 5.0
   */
  public void incMcastRetransmitRequests();

  /**
   * start a period of suspension of message transmission while we
   * wait for acknowledgement of unicast messages this process has
   * transmitted to other processes.  This returns a timestamp to be
   * used when calling endUcastFlush()
   * @since 5.0
   */
  public long startUcastFlush();
  
  /**
   * end a period of suspension of message transmission while waiting
   * for acknowledgment of unicast messages
   * @since 5.0
   */
  public void endUcastFlush(long start);
  
  /**
   * increment the number of flow control requests sent to other processes
   */
  public void incFlowControlRequests();
  
  /**
   * increment the number of flow control responses sent to other processes
   */
  public void incFlowControlResponses();
  
  /**
   * start a period of suspension of message transmission while waiting
   * for flow-control recharge from another process.  This returns a
   * timestamp to be used when calling endFlowControlWait();
   * @since 5.0
   */
  public long startFlowControlWait();
  
  /**
   * end a period of suspension of message transmission while waiting for
   * flow-control recharge from another process.
   */
  public void endFlowControlWait(long start);

  /**
   * start a period of suspension of message transmission based on throttle
   * request from another process.  
   * This returns a timestamp to be used when calling endFlowControlWait();
   * @since 5.0
   */
  public long startFlowControlThrottleWait();
  
  /**
   * end a period of suspension of message transmission based on throttle 
   * request from another process.
   */
  public void endFlowControlThrottleWait(long start);

  /**
   * this statistic measures travel of messages up the jgroups stack
   * for tuning purposes
   */
  public void incJgUNICASTdataReceived(long value);

  public void incjgDownTime(long value);
  public void incjgUpTime(long value);
  public void incjChannelUpTime(long value);

  public void setJgQueuedMessagesSize(long value);
  
  public void setJgSTABLEreceivedMessagesSize(long value);
  public void setJgSTABLEsentMessagesSize(long value);
  
  public void incJgSTABLEsuspendTime(long value);
  public void incJgSTABLEmessages(long value);
  public void incJgSTABLEmessagesSent(long value);
  public void incJgSTABILITYmessages(long value);
  
  public void incJgFCsendBlocks(long value);
  public void incJgFCautoRequests(long value);
  public void incJgFCreplenish(long value);
  public void incJgFCresumes(long value);
  public void incJgFCsentCredits(long value);
  public void incJgFCsentThrottleRequests(long value);
  
  public void setJgUNICASTreceivedMessagesSize(long amount);
  public void setJgUNICASTsentMessagesSize(long amount);
  public void setJgUNICASTsentHighPriorityMessagesSize(long amount);

  /** increment the number of javagroups fragmentations performed */
  public void incJgFragmentationsPerformed();
  
  /** increment the number of fragments created during javagroups fragmentation */
  public void incJgFragmentsCreated(long value);
  
  /**
   * @since 4.2.2
   */
  public int getAsyncSocketWritesInProgress();
  /**
   * @since 4.2.2
   */
  public int getAsyncSocketWrites();
  /**
   * @since 4.2.2
   */
  public int getAsyncSocketWriteRetries();
  /**
   * @since 4.2.2
   */
  public long getAsyncSocketWriteBytes();
  /**
   * @since 4.2.2
   */
  public long getAsyncSocketWriteTime();
  /**
   * @since 4.2.2
   */
  public int getAsyncQueues();
  /**
   * @since 4.2.2
   */
  public void incAsyncQueues(int inc);
  /**
   * @since 4.2.2
   */
  public int getAsyncQueueFlushesInProgress();
  /**
   * @since 4.2.2
   */
  public int getAsyncQueueFlushesCompleted();
  /**
   * @since 4.2.2
   */
  public long getAsyncQueueFlushTime();
  /**
   * @since 4.2.2
   */
  public long startAsyncQueueFlush();
  /**
   * @since 4.2.2
   */
  public void endAsyncQueueFlush(long start);
  /**
   * @since 4.2.2
   */
  public int getAsyncQueueTimeouts();
  /**
   * @since 4.2.2
   */
  public void incAsyncQueueTimeouts(int inc);
  /**
   * @since 4.2.2
   */
  public int getAsyncQueueSizeExceeded();
  /**
   * @since 4.2.2
   */
  public void incAsyncQueueSizeExceeded(int inc);
  /**
   * @since 4.2.2
   */
  public int getAsyncDistributionTimeoutExceeded();
  /**
   * @since 4.2.2
   */
  public void incAsyncDistributionTimeoutExceeded();
  /**
   * @since 4.2.2
   */
  public long getAsyncQueueSize();
  /**
   * @since 4.2.2
   */
  public void incAsyncQueueSize(long inc);
  /**
   * @since 4.2.2
   */
  public long getAsyncQueuedMsgs();
  /**
   * @since 4.2.2
   */
  public void incAsyncQueuedMsgs();
  /**
   * @since 4.2.2
   */
  public long getAsyncDequeuedMsgs();
  /**
   * @since 4.2.2
   */
  public void incAsyncDequeuedMsgs();
  /**
   * @since 4.2.2
   */
  public long getAsyncConflatedMsgs();
  /**
   * @since 4.2.2
   */
  public void incAsyncConflatedMsgs();
  /**
   * @since 4.2.2
   */
  public int getAsyncThreads();
  /**
   * @since 4.2.2
   */
  public void incAsyncThreads(int inc);
  /**
   * @since 4.2.2
   */
  public int getAsyncThreadInProgress();
  /**
   * @since 4.2.2
   */
  public int getAsyncThreadCompleted();
  /**
   * @since 4.2.2
   */
  public long getAsyncThreadTime();
  /**
   * @since 4.2.2
   */
  public long startAsyncThread();
  /**
   * @since 4.2.2
   */
  public void endAsyncThread(long start);
  /**
   * @since 4.2.2
   */
  public long getAsyncQueueAddTime();
  /**
   * @since 4.2.2
   */
  public void incAsyncQueueAddTime(long inc);
  /**
   * @since 4.2.2
   */
  public long getAsyncQueueRemoveTime();
  /**
   * @since 4.2.2
   */
  public void incAsyncQueueRemoveTime(long inc);

  /**
   * @since 5.0.2.4 
   */
  public void incReceiverBufferSize(int inc, boolean direct);
  /**
   * @since 5.0.2.4 
   */
  public void incSenderBufferSize(int inc, boolean direct);
  /**
   * @since 5.0.2.4 
   */
  public long startSocketLock();
  /**
   * @since 5.0.2.4 
   */
  public void endSocketLock(long start);

  /**
   * @since 5.0.2.4 
   */
  public long startBufferAcquire();
  /**
   * @since 5.0.2.4 
   */
  public void endBufferAcquire(long start);

  public void incJgNAKACKwaits(long value);

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
   * @since 5.0.2
   */
  public void incMessagesBeingReceived(boolean newMsg, int bytes);
  /**
   * Called when we finish processing a received message.
   * @param bytes the number of bytes read off the wire for the message we have finished with.
   * @since 5.0.2
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
}
