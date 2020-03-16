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
package org.apache.geode.cache.query.internal;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.StatisticsType;
import org.apache.geode.StatisticsTypeFactory;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.query.CqEvent;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.statistics.StatisticsTypeFactoryImpl;

/**
 * This class tracks GemFire statistics related to a {@link org.apache.geode.cache.query.CqQuery}.
 *
 * @since GemFire 5.5
 */
public class CqQueryVsdStats {
  /** The <code>StatisticsType</code> of the statistics */
  @Immutable
  private static final StatisticsType _type;

  /** Name of the created CQs statistic */
  protected static final String CQ_INITIAL_RESULTS_TIME = "cqInitialResultsTime";

  /** Name of the created CQs statistic */
  protected static final String CQ_INSERTS = "numInserts";

  /** Name of the active CQs statistic */
  protected static final String CQ_UPDATES = "numUpdates";

  /** Name of the stopped CQs statistic */
  protected static final String CQ_DELETES = "numDeletes";

  /** Name of the closed CQs statistic */
  protected static final String CQ_EVENTS = "numEvents";

  /** Name of the number of queued events CQ statistic */
  protected static final String NUM_HA_QUEUED_CQ_EVENTS = "numQueuedEvents";

  /** Name of the number CqListeners invoked statistic */
  protected static final String CQ_LISTENER_INVOCATIONS = "numCqListenerInvocations";

  /** Name of the number CqListeners invoked statistic */
  protected static final String QUEUED_CQ_LISTENER_EVENTS = "queuedCqListenerEvents";

  /** Id of the initial results time statistic */
  private static final int _cqInitialResultsTimeId;

  /** Id of the num inserts statistic */
  private static final int _numInsertsId;

  /** Id of the num updates statistic */
  private static final int _numUpdatesId;

  /** Id of the num deletes statistic */
  private static final int _numDeletesId;

  /** Id of the num events statistic */
  private static final int _numEventsId;

  /** Id of the num queued events in the ha queue for the cq statistic */
  private static final int _numHAQueuedEventsId;

  /** Id of the num cqListener invocation statistic */
  private static final int _numCqListenerInvocationsId;

  /** Id for the queued CQ events size during execute with initial results */
  private static final int _queuedCqListenerEventsId;

  /*
   * Static initializer to create and initialize the <code>StatisticsType</code>
   */
  static {
    String statName = "CqQueryStats";

    StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();

    _type = f.createType(statName, statName,
        new StatisticDescriptor[] {f.createLongCounter(CQ_INITIAL_RESULTS_TIME,
            "The total amount of time, in nanoseconds, it took to do this initial query and send the results to the client.",
            "nanoseconds"),

            f.createLongCounter(CQ_INSERTS, "Total number of inserts done on this cq.",
                "operations"),

            f.createLongCounter(CQ_UPDATES, "Total number of updates done on this cq.",
                "operations"),

            f.createLongCounter(CQ_DELETES, "Total number of deletes done on this cq.",
                "operations"),

            f.createLongCounter(CQ_EVENTS,
                "Total number of inserts, updates, and deletes done on this cq.", "operations"),

            f.createLongGauge(NUM_HA_QUEUED_CQ_EVENTS, "Number of events in this cq.", "events"),

            f.createLongCounter(CQ_LISTENER_INVOCATIONS, "Total number of CqListener invocations.",
                "operations"),

            f.createLongGauge(QUEUED_CQ_LISTENER_EVENTS,
                "Number of events queued while CQ registration is in progress. This is not the main cq queue but a temporary internal one used while the cq is starting up.",
                "events"),});

    // Initialize id fields
    _cqInitialResultsTimeId = _type.nameToId(CQ_INITIAL_RESULTS_TIME);
    _numInsertsId = _type.nameToId(CQ_INSERTS);
    _numUpdatesId = _type.nameToId(CQ_UPDATES);
    _numDeletesId = _type.nameToId(CQ_DELETES);
    _numEventsId = _type.nameToId(CQ_EVENTS);
    _numHAQueuedEventsId = _type.nameToId(NUM_HA_QUEUED_CQ_EVENTS);
    _numCqListenerInvocationsId = _type.nameToId(CQ_LISTENER_INVOCATIONS);
    _queuedCqListenerEventsId = _type.nameToId(QUEUED_CQ_LISTENER_EVENTS);
  }

  /** The <code>Statistics</code> instance to which most behavior is delegated */
  private final Statistics _stats;

  /**
   * Constructor.
   *
   * @param factory The <code>StatisticsFactory</code> which creates the <code>Statistics</code>
   *        instance
   * @param name The name of the <code>Statistics</code>
   */
  public CqQueryVsdStats(StatisticsFactory factory, String name) {
    this._stats = factory.createAtomicStatistics(_type, "CqQueryStats-" + name);
  }

  // /////////////////// Instance Methods /////////////////////

  /**
   * Closes the <code>CqQueryVSDStats</code>.
   */
  public void close() {
    this._stats.close();
  }

  /**
   * Returns the current value of the "cqInitialResultsTime" stat.
   *
   * @return the current value of the "cqInitialResultsTime" stat
   */
  public long getCqInitialResultsTime() {
    return this._stats.getLong(_cqInitialResultsTimeId);
  }

  /**
   * Set the "cqInitialResultsTime" stat.
   */
  public void setCqInitialResultsTime(long time) {
    this._stats.setLong(_cqInitialResultsTimeId, time);
  }

  /**
   * Returns the current value of the "numInserts" stat.
   *
   * @return the current value of the "numInserts" stat
   */
  public long getNumInserts() {
    return this._stats.getLong(_numInsertsId);
  }

  /**
   * Increments the "numInserts" stat by 1.
   */
  public void incNumInserts() {
    this._stats.incLong(_numInsertsId, 1);
  }

  /**
   * Returns the current value of the "numUpdates" stat.
   *
   * @return the current value of the "numUpdates" stat
   */
  public long getNumUpdates() {
    return this._stats.getLong(_numUpdatesId);
  }

  /**
   * Increments the "numUpdates" stat by 1.
   */
  public void incNumUpdates() {
    this._stats.incLong(_numUpdatesId, 1);
  }

  /**
   * Returns the current value of the "numDeletes" stat.
   *
   * @return the current value of the "numDeletes" stat
   */
  public long getNumDeletes() {
    return this._stats.getLong(_numDeletesId);
  }

  /**
   * Increments the "numDeletes" stat by 1.
   */
  public void incNumDeletes() {
    this._stats.incLong(_numDeletesId, 1);
  }

  /**
   * Returns the current value of the "numEvents" stat.
   *
   * @return the current value of the "numEvents" stat
   */
  public long getNumEvents() {
    return this._stats.getLong(_numEventsId);
  }

  /**
   * Increments the "numEvents" stat by 1.
   */
  public void incNumEvents() {
    this._stats.incLong(_numEventsId, 1);
  }

  /**
   * Returns the current value of the "numQueuedEvents" stat.
   *
   * @return the current value of the "numQueuedEvents" stat
   */
  public long getNumHAQueuedEvents() {
    return this._stats.getLong(_numHAQueuedEventsId);
  }

  /**
   * Increments the "numQueuedEvents" stat by incAmount.
   */
  public void incNumHAQueuedEvents(long incAmount) {
    this._stats.incLong(_numHAQueuedEventsId, incAmount);
  }

  /**
   * Returns the current value of the "numCqListenerInvocations" stat.
   *
   * @return the current value of the "numCqListenerInvocations" stat
   */
  public long getNumCqListenerInvocations() {
    return this._stats.getLong(_numCqListenerInvocationsId);
  }

  public long getQueuedCqListenerEvents() {
    return this._stats.getLong(_queuedCqListenerEventsId);
  }

  /**
   * Increments the "numCqListenerInvocations" stat by 1.
   */
  public void incNumCqListenerInvocations() {
    this._stats.incLong(_numCqListenerInvocationsId, 1);
  }

  public void incQueuedCqListenerEvents() {
    this._stats.incLong(_queuedCqListenerEventsId, 1);
  }

  public void decQueuedCqListenerEvents() {
    this._stats.incLong(_queuedCqListenerEventsId, -1);
  }

  /**
   * Update stats for a CQ for VSD
   *
   * @param cqEvent object containing info on the newly qualified CQ event
   */
  public void updateStats(CqEvent cqEvent) {
    if (cqEvent.getQueryOperation() == null)
      return;
    this.incNumEvents();
    if (cqEvent.getQueryOperation().isCreate()) {
      this.incNumInserts();
    }
    if (cqEvent.getQueryOperation().isUpdate()) {
      this.incNumUpdates();
    }
    if (cqEvent.getQueryOperation().isDestroy()) {
      this.incNumDeletes();
    }
  }

  /**
   * Update stats for a CQ for VSD
   *
   * @param cqEvent object the type of CQ event
   */
  public void updateStats(Integer cqEvent) {
    if (cqEvent == null) {
      return;
    }
    this.incNumEvents();
    switch (cqEvent.intValue()) {
      case MessageType.LOCAL_CREATE:
        this.incNumInserts();
        return;
      case MessageType.LOCAL_UPDATE:
        this.incNumUpdates();
        return;
      case MessageType.LOCAL_DESTROY:
        this.incNumDeletes();
        return;
      default:
        return;
    }
  }
}
