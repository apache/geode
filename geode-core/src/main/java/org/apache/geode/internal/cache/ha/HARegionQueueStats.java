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
package org.apache.geode.internal.cache.ha;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.StatisticsType;
import org.apache.geode.StatisticsTypeFactory;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.internal.statistics.StatisticsTypeFactoryImpl;

/**
 * This class tracks GemFire statistics related to a {@link HARegionQueue}.
 *
 *
 */
public class HARegionQueueStats {
  /** The <code>StatisticsType</code> of the statistics */
  @Immutable
  private static final StatisticsType _type;

  /** Name of the events queued statistic */
  protected static final String EVENTS_QUEUED = "eventsQueued";

  /** Name of the events conflated statistic */
  protected static final String EVENTS_CONFLATED = "eventsConflated";

  /** Name of the marker events conflated statistic */
  protected static final String MARKER_EVENTS_CONFLATED = "markerEventsConflated";

  /** Name of the events removed statistic */
  protected static final String EVENTS_REMOVED = "eventsRemoved";

  /** Name of the events taken statistic */
  protected static final String EVENTS_TAKEN = "eventsTaken";

  /** Name of the events expired statistic */
  protected static final String EVENTS_EXPIRED = "eventsExpired";

  /** Name of the events removed by QRM statistic */
  protected static final String EVENTS_REMOVED_BY_QRM = "eventsRemovedByQrm";

  /** Name of the thread identifiers statistic */
  protected static final String THREAD_IDENTIFIERS = "threadIdentifiers";

  /** Name of the events dispatched statistic */
  protected static final String EVENTS_DISPATCHED = "eventsDispatched";

  /**
   * Name of the num void removals statistic. This refers to the events which were supposed to be
   * destroyed from queue through remove() but were removed by some other operation like conflation
   * or expiration.
   */
  protected static final String NUM_VOID_REMOVALS = "numVoidRemovals";

  /**
   * Name of the number of sequence violated events statistic. This refers to the events which has
   * sequence id less than lastSequnceId hence not put in the region queue
   */
  protected static final String NUM_SEQUENCE_VIOLATED = "numSequenceViolated";

  /** Id of the events queued statistic */
  private static final int _eventsQueuedId;

  /** Id of the events conflated statistic */
  private static final int _eventsConflatedId;

  /** Id of the marker events conflated statistic */
  private static final int _markerEventsConflatedId;

  /** Id of the events removed statistic */
  private static final int _eventsRemovedId;

  /** Id of the events taken statistic */
  private static final int _eventsTakenId;

  /** Id of the events expired statistic */
  private static final int _eventsExpiredId;

  /** Id of the events removed by qrm statistic */
  private static final int _eventsRemovedByQrmId;

  /** Id of the thread identifiers statistic */
  private static final int _threadIdentifiersId;

  /** Id of the num events dispatched statistic */
  private static final int _eventsDispatched;

  /** Id of the num void removal statistic */
  private static final int _numVoidRemovals;

  /** Id of the num sequence violated statistic */
  private static final int _numSequenceViolated;

  /*
   * Static initializer to create and initialize the <code>StatisticsType</code>
   */
  static {
    String statName = "ClientSubscriptionStats";

    StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();

    _type = f.createType(statName, statName, new StatisticDescriptor[] {
        f.createLongCounter(EVENTS_QUEUED, "Number of events added to queue.", "operations"),

        f.createLongCounter(EVENTS_CONFLATED, "Number of events conflated for the queue.",
            "operations"),

        f.createLongCounter(MARKER_EVENTS_CONFLATED,
            "Number of marker events conflated for the queue.", "operations"),

        f.createLongCounter(EVENTS_REMOVED, "Number of events removed from the queue.",
            "operations"),

        f.createLongCounter(EVENTS_TAKEN, "Number of events taken from the queue.", "operations"),

        f.createLongCounter(EVENTS_EXPIRED, "Number of events expired from the queue.",
            "operations"),

        f.createLongCounter(EVENTS_REMOVED_BY_QRM, "Number of events removed by QRM message.",
            "operations"),

        f.createIntCounter(THREAD_IDENTIFIERS, "Number of ThreadIdenfier objects for the queue.",
            "units"),

        f.createLongCounter(EVENTS_DISPATCHED, "Number of events that have been dispatched.",
            "operations"),

        f.createLongCounter(NUM_VOID_REMOVALS, "Number of void removals from the queue.",
            "operations"),

        f.createLongCounter(NUM_SEQUENCE_VIOLATED, "Number of events that has violated sequence.",
            "operations")});

    // Initialize id fields
    _eventsQueuedId = _type.nameToId(EVENTS_QUEUED);
    _eventsConflatedId = _type.nameToId(EVENTS_CONFLATED);
    _markerEventsConflatedId = _type.nameToId(MARKER_EVENTS_CONFLATED);
    _eventsRemovedId = _type.nameToId(EVENTS_REMOVED);
    _eventsTakenId = _type.nameToId(EVENTS_TAKEN);
    _eventsExpiredId = _type.nameToId(EVENTS_EXPIRED);
    _eventsRemovedByQrmId = _type.nameToId(EVENTS_REMOVED_BY_QRM);
    _threadIdentifiersId = _type.nameToId(THREAD_IDENTIFIERS);
    _eventsDispatched = _type.nameToId(EVENTS_DISPATCHED);
    _numVoidRemovals = _type.nameToId(NUM_VOID_REMOVALS);
    _numSequenceViolated = _type.nameToId(NUM_SEQUENCE_VIOLATED);
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
  public HARegionQueueStats(StatisticsFactory factory, String name) {
    this._stats = factory.createAtomicStatistics(_type, "ClientSubscriptionStats-" + name);
  }

  // /////////////////// Instance Methods /////////////////////

  /**
   * Closes the <code>HARegionQueueStats</code>.
   */
  public void close() {
    this._stats.close();
  }

  /**
   * Returns the current value of the "eventsQueued" stat.
   *
   * @return the current value of the "eventsQueued" stat
   */
  public long getEventsEnqued() {
    return this._stats.getLong(_eventsQueuedId);
  }

  /**
   * Increments the "eventsQueued" stat by 1.
   */
  public void incEventsEnqued() {
    this._stats.incLong(_eventsQueuedId, 1);
  }

  /**
   * Returns the current value of the "eventsConflated" stat.
   *
   * @return the current value of the "eventsConflated" stat
   */
  public long getEventsConflated() {
    return this._stats.getLong(_eventsConflatedId);
  }

  /**
   * Increments the "eventsConflated" stat by 1.
   */
  public void incEventsConflated() {
    this._stats.incLong(_eventsConflatedId, 1);
  }

  /**
   * Returns the current value of the "markerEventsConflated" stat.
   *
   * @return the current value of the "markerEventsConflated" stat
   */
  public long getMarkerEventsConflated() {
    return this._stats.getLong(_markerEventsConflatedId);
  }

  /**
   * Increments the "markerEventsConflated" stat by 1.
   */
  public void incMarkerEventsConflated() {
    this._stats.incLong(_markerEventsConflatedId, 1);
  }

  /**
   * Returns the current value of the "eventsRemoved" stat.
   *
   * @return the current value of the "eventsRemoved" stat
   */
  public long getEventsRemoved() {
    return this._stats.getLong(_eventsRemovedId);
  }

  /**
   * Increments the "eventsRemoved" stat by 1.
   */
  public void incEventsRemoved() {
    this._stats.incLong(_eventsRemovedId, 1);
  }

  /**
   * Returns the current value of the "eventsTaken" stat.
   *
   * @return the current value of the "eventsTaken" stat
   */
  public long getEventsTaken() {
    return this._stats.getLong(_eventsTakenId);
  }

  /**
   * Increments the "eventsTaken" stat by 1.
   */
  public void incEventsTaken() {
    this._stats.incLong(_eventsTakenId, 1);
  }

  /**
   * Returns the current value of the "eventsExpired" stat.
   *
   * @return the current value of the "eventsExpired" stat
   */
  public long getEventsExpired() {
    return this._stats.getLong(_eventsExpiredId);
  }

  /**
   * Increments the "eventsExpired" stat by 1.
   */
  public void incEventsExpired() {
    this._stats.incLong(_eventsExpiredId, 1);
  }

  /**
   * Returns the current value of the "eventsRemovedByQrm" stat.
   *
   * @return the current value of the "eventsRemovedByQrm" stat
   */
  public long getEventsRemovedByQrm() {
    return this._stats.getLong(_eventsRemovedByQrmId);
  }

  /**
   * Increments the "eventsRemovedByQrm" stat by 1.
   */
  public void incEventsRemovedByQrm() {
    this._stats.incLong(_eventsRemovedByQrmId, 1);
  }

  /**
   * Returns the current value of the "threadIdentifiers" stat.
   *
   * @return the current value of the "threadIdentifiers" stat
   */
  public int getThreadIdentiferCount() {
    return this._stats.getInt(_threadIdentifiersId);
  }

  /**
   * Increments the "threadIdentifiers" stat by 1.
   */
  public void incThreadIdentifiers() {
    this._stats.incInt(_threadIdentifiersId, 1);
  }

  /**
   * Decrements the "threadIdentifiers" stat by 1.
   */
  public void decThreadIdentifiers() {
    this._stats.incInt(_threadIdentifiersId, -1);
  }

  /**
   * Returns the current value of the "eventsDispatched" stat.
   *
   * @return the current value of the "eventsDispatched" stat
   */
  public long getEventsDispatched() {
    return this._stats.getLong(_eventsDispatched);
  }

  /**
   * Increments the "eventsDispatched" stat by 1.
   */
  public void incEventsDispatched() {
    this._stats.incLong(_eventsDispatched, 1);
  }

  /**
   * Returns the current value of the "numVoidRemovals" stat.
   *
   * @return the current value of the "numVoidRemovals" stat
   */
  public long getNumVoidRemovals() {
    return this._stats.getLong(_numVoidRemovals);
  }

  /**
   * Increments the "numVoidRemovals" stat by 1.
   */
  public void incNumVoidRemovals() {
    this._stats.incLong(_numVoidRemovals, 1);
  }

  /**
   * Returns the current value of the "numSequenceViolated" stat.
   *
   * @return the current value of the "numSequenceViolated" stat
   */
  public long getNumSequenceViolated() {
    return this._stats.getLong(_numSequenceViolated);
  }

  /**
   * Increments the "numSequenceViolated" stat by 1.
   */
  public void incNumSequenceViolated() {
    this._stats.incLong(_numSequenceViolated, 1);
  }

  /**
   * Returns true if the stats instance has been closed.
   *
   * @return true if the stats instance has been closed.
   */
  public boolean isClosed() {
    return this._stats.isClosed();
  }

}
