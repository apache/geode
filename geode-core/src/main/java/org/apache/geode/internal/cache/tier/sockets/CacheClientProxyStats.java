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

package org.apache.geode.internal.cache.tier.sockets;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.StatisticsType;
import org.apache.geode.StatisticsTypeFactory;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.internal.statistics.StatisticsTypeFactoryImpl;

/**
 * Class <code>CacheClientProxyStats</code> tracks GemFire statistics related to a
 * {@link CacheClientProxy}. These statistics are related to cache server client notifications for
 * each client.
 *
 *
 * @since GemFire 4.2
 */
public class CacheClientProxyStats implements MessageStats {

  /** The <code>StatisticsType</code> of the statistics */
  @Immutable
  private static final StatisticsType _type;

  /** Name of the messages received statistic */
  private static final String MESSAGES_RECEIVED = "messagesReceived";
  /** Name of the messages queued statistic */
  private static final String MESSAGES_QUEUED = "messagesQueued";
  /** Name of the messages not queued because originator statistic */
  private static final String MESSAGES_NOT_QUEUED_ORIGINATOR = "messagesNotQueuedOriginator";
  /** Name of the messages not queued because not interested statistic */
  private static final String MESSAGES_NOT_QUEUED_NOT_INTERESTED = "messagesNotQueuedNotInterested";
  /** Name of the messages failed to be queued statistic */
  private static final String MESSAGES_FAILED_QUEUED = "messagesFailedQueued";
  /** Name of the message queue size statistic */
  private static final String MESSAGE_QUEUE_SIZE = "messageQueueSize";
  /** Name of the messages removed statistic */
  private static final String MESSAGES_PROCESSED = "messagesProcessed";
  /** Name of the message processing time statistic */
  private static final String MESSAGE_PROCESSING_TIME = "messageProcessingTime";
  /** Name of the delta messages sent statistic */
  private static final String DELTA_MESSAGES_SENT = "deltaMessagesSent";
  /** Name of the delta full messages sent statistic */
  private static final String DELTA_FULL_MESSAGES_SENT = "deltaFullMessagesSent";
  /** Name of the CQ count statistic */
  private static final String CQ_COUNT = "cqCount";

  /** Id of the messages received statistic */
  private static final int _messagesReceivedId;
  /** Id of the messages queued statistic */
  private static final int _messagesQueuedId;
  /** Id of the messages not queued because originator statistic */
  private static final int _messagesNotQueuedOriginatorId;
  /** Id of the messages not queued because not interested statistic */
  private static final int _messagesNotQueuedNotInterestedId;
  /** Id of the messages failed to be queued statistic */
  private static final int _messagesFailedQueuedId;
  /** Id of the message queue size statistic */
  private static final int _messageQueueSizeId;
  /** Id of the messages removed statistic */
  private static final int _messagesProcessedId;
  /** Id of the message processing time statistic */
  private static final int _messageProcessingTimeId;
  /** Id of the prepared delta messages statistic */
  private static final int _deltaMessagesSentId;
  /** Id of the prepared delta messages statistic */
  private static final int _deltaFullMessagesSentId;
  /** Id of the CQ count statistic */
  private static final int _cqCountId;
  private static final int _sentBytesId;

  /*
   * Static initializer to create and initialize the <code>StatisticsType</code>
   */
  static {
    String statName = "CacheClientProxyStatistics";

    StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();

    _type = f.createType(statName, statName, new StatisticDescriptor[] {
        f.createIntCounter(MESSAGES_RECEIVED, "Number of client messages received.", "operations"),

        f.createIntCounter(MESSAGES_QUEUED, "Number of client messages added to the message queue.",
            "operations"),

        f.createIntCounter(MESSAGES_FAILED_QUEUED,
            "Number of client messages attempted but failed to be added to the message queue.",
            "operations"),

        f.createIntCounter(MESSAGES_NOT_QUEUED_ORIGINATOR,
            "Number of client messages received but not added to the message queue because the receiving proxy represents the client originating the message.",
            "operations"),

        f.createIntCounter(MESSAGES_NOT_QUEUED_NOT_INTERESTED,
            "Number of client messages received but not added to the message queue because the client represented by the receiving proxy was not interested in the message's key.",
            "operations"),

        f.createIntGauge(MESSAGE_QUEUE_SIZE, "Size of the message queue.", "operations"),

        f.createIntCounter(MESSAGES_PROCESSED,
            "Number of client messages removed from the message queue and sent.", "operations"),

        f.createLongCounter(MESSAGE_PROCESSING_TIME,
            "Total time spent sending messages to clients.", "nanoseconds"),

        f.createIntCounter(DELTA_MESSAGES_SENT,
            "Number of client messages containing only delta bytes dispatched to the client.",
            "operations"),

        f.createIntCounter(DELTA_FULL_MESSAGES_SENT,
            "Number of client messages dispatched in reponse to failed delta at client.",
            "operations"),

        f.createLongCounter(CQ_COUNT, "Number of CQs on the client.", "operations"),
        f.createLongCounter("sentBytes", "Total number of bytes sent to client.", "bytes"),});

    // Initialize id fields
    _messagesReceivedId = _type.nameToId(MESSAGES_RECEIVED);
    _messagesQueuedId = _type.nameToId(MESSAGES_QUEUED);
    _messagesNotQueuedOriginatorId = _type.nameToId(MESSAGES_NOT_QUEUED_ORIGINATOR);
    _messagesNotQueuedNotInterestedId = _type.nameToId(MESSAGES_NOT_QUEUED_NOT_INTERESTED);
    _messagesFailedQueuedId = _type.nameToId(MESSAGES_FAILED_QUEUED);
    _messageQueueSizeId = _type.nameToId(MESSAGE_QUEUE_SIZE);
    _messagesProcessedId = _type.nameToId(MESSAGES_PROCESSED);
    _messageProcessingTimeId = _type.nameToId(MESSAGE_PROCESSING_TIME);
    _deltaMessagesSentId = _type.nameToId(DELTA_MESSAGES_SENT);
    _deltaFullMessagesSentId = _type.nameToId(DELTA_FULL_MESSAGES_SENT);
    _cqCountId = _type.nameToId(CQ_COUNT);
    _sentBytesId = _type.nameToId("sentBytes");
  }

  ////////////////////// Instance Fields //////////////////////

  /** The <code>Statistics</code> instance to which most behavior is delegated */
  private final Statistics _stats;

  /////////////////////// Constructors ///////////////////////

  /**
   * Constructor.
   *
   * @param factory The <code>StatisticsFactory</code> which creates the <code>Statistics</code>
   *        instance
   * @param name The name of the <code>Statistics</code>
   */
  public CacheClientProxyStats(StatisticsFactory factory, String name) {
    this._stats = factory.createAtomicStatistics(_type, "cacheClientProxyStats-" + name);
  }

  ///////////////////// Instance Methods /////////////////////

  /**
   * Closes the <code>CacheClientProxyStats</code>.
   */
  public void close() {
    this._stats.close();
  }

  /**
   * Returns the current value of the "messagesReceived" stat.
   *
   * @return the current value of the "messagesReceived" stat
   */
  public int getMessagesReceived() {
    return this._stats.getInt(_messagesReceivedId);
  }

  /**
   * Returns the current value of the "messagesQueued" stat.
   *
   * @return the current value of the "messagesQueued" stat
   */
  public int getMessagesQueued() {
    return this._stats.getInt(_messagesQueuedId);
  }

  /**
   * Returns the current value of the "messagesNotQueuedOriginator" stat.
   *
   * @return the current value of the "messagesNotQueuedOriginator" stat
   */
  public int getMessagesNotQueuedOriginator() {
    return this._stats.getInt(_messagesNotQueuedOriginatorId);
  }

  /**
   * Returns the current value of the "messagesNotQueuedNotInterested" stat.
   *
   * @return the current value of the "messagesNotQueuedNotInterested" stat
   */
  public int getMessagesNotQueuedNotInterested() {
    return this._stats.getInt(_messagesNotQueuedNotInterestedId);
  }

  /**
   * Returns the current value of the "messagesFailedQueued" stat.
   *
   * @return the current value of the "messagesFailedQueued" stat
   */
  public int getMessagesFailedQueued() {
    return this._stats.getInt(_messagesFailedQueuedId);
  }

  /**
   * Returns the current value of the "messageQueueSize" stat.
   *
   * @return the current value of the "messageQueueSize" stat
   */
  public int getMessageQueueSize() {
    return this._stats.getInt(_messageQueueSizeId);
  }

  /**
   * Returns the current value of the messagesProcessed" stat.
   *
   * @return the current value of the messagesProcessed" stat
   */
  public int getMessagesProcessed() {
    return this._stats.getInt(_messagesProcessedId);
  }

  /**
   * Returns the current value of the "messageProcessingTime" stat.
   *
   * @return the current value of the "messageProcessingTime" stat
   */
  public long getMessageProcessingTime() {
    return this._stats.getLong(_messageProcessingTimeId);
  }

  /**
   * Returns the current value of the "deltaMessagesSent" stat.
   *
   * @return the current value of the "deltaMessagesSent" stat
   */
  public int getDeltaMessagesSent() {
    return this._stats.getInt(_deltaMessagesSentId);
  }

  /**
   * Returns the current value of the "deltaFullMessagesSent" stat.
   *
   * @return the current value of the "deltaFullMessagesSent" stat
   */
  public int getDeltaFullMessagesSent() {
    return this._stats.getInt(_deltaFullMessagesSentId);
  }

  /**
   * Returns the current value of the "cqCount" stat.
   *
   * @return the current value of the "cqCount" stat
   */
  public int getCqCount() {
    return this._stats.getInt(_cqCountId);
  }

  /**
   * Increments the "messagesReceived" stat.
   */
  public void incMessagesReceived() {
    this._stats.incInt(_messagesReceivedId, 1);
  }

  /**
   * Increments the "messagesQueued" stat.
   */
  public void incMessagesQueued() {
    this._stats.incInt(_messagesQueuedId, 1);
  }

  /**
   * Increments the "messagesNotQueuedOriginator" stat.
   */
  public void incMessagesNotQueuedOriginator() {
    this._stats.incInt(_messagesNotQueuedOriginatorId, 1);
  }

  /**
   * Increments the "messagesNotQueuedNotInterested" stat.
   */
  public void incMessagesNotQueuedNotInterested() {
    this._stats.incInt(_messagesNotQueuedNotInterestedId, 1);
  }

  /**
   * Increments the "messagesFailedQueued" stat.
   */
  public void incMessagesFailedQueued() {
    this._stats.incInt(_messagesFailedQueuedId, 1);
  }

  /**
   * Increments the "cqCount" stat.
   */
  public void incCqCount() {
    this._stats.incInt(_cqCountId, 1);
  }

  /**
   * Decrements the "cqCount" stat.
   */
  public void decCqCount() {
    this._stats.incInt(_cqCountId, -1);
  }

  /**
   * Sets the "messageQueueSize" stat.
   *
   * @param size The size of the queue
   */
  public void setQueueSize(int size) {
    this._stats.setInt(_messageQueueSizeId, size);
  }

  /**
   * Returns the current time (ns).
   *
   * @return the current time (ns)
   */
  public long startTime() {
    return DistributionStats.getStatTime();
  }

  /**
   * Increments the "messagesProcessed" and "messageProcessingTime" stats.
   *
   * @param start The start of the message (which is decremented from the current time to determine
   *        the message processing time).
   */
  public void endMessage(long start) {
    long ts = DistributionStats.getStatTime();

    // Increment number of notifications
    this._stats.incInt(_messagesProcessedId, 1);

    // Increment notification time
    long elapsed = ts - start;
    this._stats.incLong(_messageProcessingTimeId, elapsed);
  }

  /**
   * Increments the "deltaMessagesSent" stats.
   */
  public void incDeltaMessagesSent() {
    this._stats.incInt(_deltaMessagesSentId, 1);
  }

  /**
   * Increments the "deltaFullMessagesSent" stats.
   */
  public void incDeltaFullMessagesSent() {
    this._stats.incInt(_deltaFullMessagesSentId, 1);
  }

  @Override
  public void incReceivedBytes(long v) {
    // noop since we never receive
  }

  @Override
  public void incSentBytes(long v) {
    this._stats.incLong(_sentBytesId, v);
  }

  @Override
  public void incMessagesBeingReceived(int bytes) {
    // noop since we never receive
  }

  @Override
  public void decMessagesBeingReceived(int bytes) {
    // noop since we never receive
  }
}
