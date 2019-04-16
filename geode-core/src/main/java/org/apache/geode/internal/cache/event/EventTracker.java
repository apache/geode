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
package org.apache.geode.internal.cache.event;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.InternalCacheEvent;
import org.apache.geode.internal.cache.ha.ThreadIdentifier;
import org.apache.geode.internal.cache.versions.VersionTag;

/**
 * EventTracker tracks the last sequence number for a particular memberID:threadID. It is used to
 * avoid replaying events in client/server and partitioned-region configurations.
 *
 * @since GemFire 6.0
 */
public interface EventTracker {
  /** start this event tracker */
  void start();

  /** stop this event tracker */
  void stop();

  /**
   * retrieve a deep copy of the state of the event tracker. Synchronization is not used while
   * copying the tracker's state.
   */
  Map<ThreadIdentifier, EventSequenceNumberHolder> getState();

  /**
   * record the given state in the tracker.
   *
   * @param provider the member that provided this state
   * @param state a Map obtained from getState();
   */
  void recordState(InternalDistributedMember provider,
      Map<ThreadIdentifier, EventSequenceNumberHolder> state);

  /**
   * Use this method to ensure that the tracker is put in an initialized state
   */
  void setInitialized();

  /**
   * Wait for the tracker to finish being initialized
   */
  void waitOnInitialization() throws InterruptedException;

  /** record the event's threadid/sequenceid to prevent replay */
  void recordEvent(InternalCacheEvent event);

  /**
   * Determines if an event has already been seen by the tracker
   *
   * @param event The event to determine if it has been seen by the tracker already
   * @return if the event provided has already been seen
   */
  boolean hasSeenEvent(InternalCacheEvent event);

  /**
   * Determines if an event has already been seen by the tracker
   *
   * @param eventID The id of the event to determine if it has been seen by the tracker already
   * @return if the event provided has already been seen
   */
  boolean hasSeenEvent(EventID eventID);

  /**
   * Determines if an event has already been seen by the tracker
   *
   * @param eventID The id of the event to determine if it has been seen by the tracker already
   * @param tagHolder Event to update version tag with that of eventID, if event was seen before
   * @return if the event provided has already been seen
   */
  boolean hasSeenEvent(EventID eventID, InternalCacheEvent tagHolder);

  VersionTag findVersionTagForSequence(EventID eventID);

  VersionTag findVersionTagForBulkOp(EventID eventID);

  /**
   * The name of the event tracker for logging purposes
   *
   * @return the name of the tracker
   */
  String getName();

  ConcurrentMap<ThreadIdentifier, BulkOperationHolder> getRecordedBulkOpVersionTags();

  ConcurrentMap<ThreadIdentifier, EventSequenceNumberHolder> getRecordedEvents();

  /**
   * A routine to provide synchronization running based on <memberShipID, threadID> of the
   * requesting client
   *
   * @param r - a Runnable to wrap the processing of the bulk op
   * @param eventID - the base event ID of the bulk op
   *
   * @since GemFire 5.7
   */
  void syncBulkOp(Runnable r, EventID eventID, boolean partOfTransaction);

  /**
   * Called when a new bulkOp is started on the local region. Used to clear event tracker state from
   * the last bulkOp.
   */
  void recordBulkOpStart(EventID eventID, ThreadIdentifier tid);

  /**
   * @return the initialization state of the tracker
   */
  boolean isInitialized();

  /**
   * @param mbr the member in question
   * @return true if the given member provided the initial image event state for this tracker
   */
  boolean isInitialImageProvider(DistributedMember mbr);

  /**
   * clear the tracker
   */
  void clear();

}
