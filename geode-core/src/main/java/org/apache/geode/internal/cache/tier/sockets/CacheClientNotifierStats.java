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
 * GemFire statistics about a {@link CacheClientNotifier}. These statistics are related to cache
 * server client notifications.
 *
 *
 * @since GemFire 4.1.2
 */
public class CacheClientNotifierStats {

  @Immutable
  private static final StatisticsType _type;

  //////////////////// Statistic "Id" Fields ////////////////////

  private static final String EVENTS = "events";
  private static final String EVENT_PROCESSING_TIME = "eventProcessingTime";
  private static final String CLIENT_REGISTRATIONS = "clientRegistrations";
  private static final String DURABLE_RECONNECTION_COUNT = "durableReconnectionCount";
  private static final String QUEUE_DROPPED_COUNT = "queueDroppedCount";
  private static final String EVENTS_ENQUEUED_WHILE_CLIENT_AWAY_COUNT =
      "eventsEnqueuedWhileClientAwayCount";
  private static final String CLIENT_REGISTRATION_TIME = "clientRegistrationTime";
  private static final String CQ_PROCESSING_TIME = "cqProcessingTime";
  private static final String COMPILED_QUERY_COUNT = "compiledQueryCount";
  private static final String COMPILED_QUERY_USED_COUNT = "compiledQueryUsedCount";

  private static final int _eventsId;
  private static final int _eventProcessingTimeId;
  private static final int _clientRegistrationsId;
  private static final int _clientRegistrationTimeId;

  // Register and Unregister stats.
  private static final int _clientHealthMonitorRegisterId;
  private static final int _durableReconnectionCount;
  private static final int _queueDroppedCount;
  private static final int _eventEnqueuedWhileClientAwayCount;
  private static final int _clientHealthMonitorUnRegisterId;

  // CQ process stat.
  private static final int _cqProcessingTimeId;

  // Compiled query count.
  private static final int _compiledQueryCount;

  private static final int _compiledQueryUsedCount;

  static {
    String statName = "CacheClientNotifierStatistics";

    StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();

    _type = f.createType(statName, statName, new StatisticDescriptor[] {f.createIntCounter(EVENTS,
        "Number of events processed by the cache client notifier.", "operations"),

        f.createLongCounter(EVENT_PROCESSING_TIME,
            "Total time spent by the cache client notifier processing events.", "nanoseconds"),

        f.createIntCounter(CLIENT_REGISTRATIONS,
            "Number of clients that have registered for updates.", "operations"),

        f.createLongCounter(CLIENT_REGISTRATION_TIME,
            "Total time spent doing client registrations.", "nanoseconds"),

        f.createIntGauge("clientHealthMonitorRegister", "Number of client Register.", "registered"),

        f.createIntGauge("clientHealthMonitorUnRegister", "Number of client UnRegister.",
            "unregistered"),

        f.createIntCounter(DURABLE_RECONNECTION_COUNT,
            "Number of times the same durable client connects to the server", "operations"),

        f.createIntCounter(QUEUE_DROPPED_COUNT,
            "Number of times client queue for a particular durable client is dropped",
            "operations"),

        f.createIntCounter(EVENTS_ENQUEUED_WHILE_CLIENT_AWAY_COUNT,
            "Number of events enqueued in queue for a durable client ", "operations"),

        f.createLongCounter(CQ_PROCESSING_TIME,
            "Total time spent by the cache client notifier processing cqs.", "nanoseconds"),

        f.createLongGauge(COMPILED_QUERY_COUNT, "Number of compiled queries maintained.",
            "maintained"),

        f.createLongCounter(COMPILED_QUERY_USED_COUNT, "Number of times compiled queries are used.",
            "used"),

    });

    // Initialize id fields
    _eventsId = _type.nameToId(EVENTS);
    _eventProcessingTimeId = _type.nameToId(EVENT_PROCESSING_TIME);
    _clientRegistrationsId = _type.nameToId(CLIENT_REGISTRATIONS);
    _clientRegistrationTimeId = _type.nameToId(CLIENT_REGISTRATION_TIME);

    _clientHealthMonitorRegisterId = _type.nameToId("clientHealthMonitorRegister");
    _clientHealthMonitorUnRegisterId = _type.nameToId("clientHealthMonitorUnRegister");


    _durableReconnectionCount = _type.nameToId(DURABLE_RECONNECTION_COUNT);
    _queueDroppedCount = _type.nameToId(QUEUE_DROPPED_COUNT);
    _eventEnqueuedWhileClientAwayCount = _type.nameToId(EVENTS_ENQUEUED_WHILE_CLIENT_AWAY_COUNT);

    _cqProcessingTimeId = _type.nameToId(CQ_PROCESSING_TIME);
    _compiledQueryCount = _type.nameToId(COMPILED_QUERY_COUNT);
    _compiledQueryUsedCount = _type.nameToId(COMPILED_QUERY_USED_COUNT);
  }

  ////////////////////// Instance Fields //////////////////////

  /** The Statistics object that we delegate most behavior to */
  private final Statistics _stats;

  /////////////////////// Constructors ///////////////////////

  /**
   * Creates a new <code>CacheClientNotifierStats</code>.
   */
  public CacheClientNotifierStats(StatisticsFactory f) {
    _stats = f.createAtomicStatistics(_type, "cacheClientNotifierStats");
  }

  ///////////////////// Instance Methods /////////////////////

  public void close() {
    _stats.close();
  }

  /**
   * Returns the current value of the "events" stat.
   */
  public int getEvents() {
    return _stats.getInt(_eventsId);
  }

  /**
   * Returns the current value of the "eventProcessingTime" stat.
   */
  public long getEventProcessingTime() {
    return _stats.getLong(_eventProcessingTimeId);
  }

  public long startTime() {
    return DistributionStats.getStatTime();
  }

  public void endEvent(long start) {
    long ts = DistributionStats.getStatTime();
    // Increment number of notifications
    _stats.incInt(_eventsId, 1);

    if (start != 0L && ts != 0L) {
      // Increment notification time
      long elapsed = ts - start;
      _stats.incLong(_eventProcessingTimeId, elapsed);
    }
  }

  public void endClientRegistration(long start) {
    long ts = DistributionStats.getStatTime();

    // Increment number of notifications
    _stats.incInt(_clientRegistrationsId, 1);

    if (start != 0L && ts != 0L) {
      // Increment notification time
      long elapsed = ts - start;
      _stats.incLong(_clientRegistrationTimeId, elapsed);
    }
  }

  public void endCqProcessing(long start) {
    long ts = DistributionStats.getStatTime();
    if (start != 0L && ts != 0L) {
      _stats.incLong(_cqProcessingTimeId, (ts - start));
    }
  }

  public void incClientRegisterRequests() {
    _stats.incInt(_clientHealthMonitorRegisterId, 1);
  }

  public int getClientRegisterRequests() {
    return _stats.getInt(_clientHealthMonitorRegisterId);
  }

  public int get_durableReconnectionCount() {
    return _stats.getInt(_durableReconnectionCount);
  }

  public int get_queueDroppedCount() {
    return _stats.getInt(_queueDroppedCount);
  }

  public int get_eventEnqueuedWhileClientAwayCount() {
    return _stats.getInt(_eventEnqueuedWhileClientAwayCount);
  }

  public long getCqProcessingTime() {
    return _stats.getLong(_cqProcessingTimeId);
  }

  public long getCompiledQueryCount() {
    return _stats.getLong(_compiledQueryCount);
  }

  public long getCompiledQueryUsedCount() {
    return _stats.getLong(_compiledQueryUsedCount);
  }

  public void incDurableReconnectionCount() {
    _stats.incInt(_durableReconnectionCount, 1);
  }

  public void incQueueDroppedCount() {
    _stats.incInt(_queueDroppedCount, 1);
  }

  public void incEventEnqueuedWhileClientAwayCount() {
    _stats.incInt(_eventEnqueuedWhileClientAwayCount, 1);
  }

  public void incClientUnRegisterRequests() {
    _stats.incInt(_clientHealthMonitorUnRegisterId, 1);
  }

  public void incCompiledQueryCount(long count) {
    _stats.incLong(_compiledQueryCount, count);
  }

  public void incCompiledQueryUsedCount(long count) {
    _stats.incLong(_compiledQueryUsedCount, count);
  }

  public int getClientUnRegisterRequests() {
    return _stats.getInt(_clientHealthMonitorUnRegisterId);
  }

}
