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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.InternalCacheEvent;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.ha.ThreadIdentifier;
import org.apache.geode.internal.cache.versions.RegionVersionVector;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.util.concurrent.StoppableCountDownLatch;
import org.apache.geode.logging.internal.log4j.api.LogService;


public class DistributedEventTracker implements EventTracker {
  private static final Logger logger = LogService.getLogger();

  @VisibleForTesting
  protected static final String EVENT_HAS_PREVIOUSLY_BEEN_SEEN_PREFIX =
      "Event has previously been seen";

  protected static final String IGNORING_PREVIOUSLY_SEEN_EVENT_PREFIX =
      "Ignoring previously seen event due to {}";

  private static final String EVENT_HAS_PREVIOUSLY_BEEN_SEEN_PARAMETERS =
      " for region={}; operation={}; key={}; eventId={}; highestSequenceNumberSeen={}";

  private static final String EVENT_HAS_PREVIOUSLY_BEEN_SEEN =
      EVENT_HAS_PREVIOUSLY_BEEN_SEEN_PREFIX + EVENT_HAS_PREVIOUSLY_BEEN_SEEN_PARAMETERS;

  private static final String IGNORING_PREVIOUSLY_SEEN_EVENT =
      IGNORING_PREVIOUSLY_SEEN_EVENT_PREFIX + EVENT_HAS_PREVIOUSLY_BEEN_SEEN_PARAMETERS;
  /**
   * a mapping of originator to the last event applied to this cache
   *
   * Keys are instances of {@link ThreadIdentifier}, values are instances of
   * {@link EventSequenceNumberHolder}.
   */
  private final ConcurrentMap<ThreadIdentifier, EventSequenceNumberHolder> recordedEvents =
      new ConcurrentHashMap<>(100);

  /**
   * a mapping of originator to bulkOps
   *
   * Keys are instances of @link {@link ThreadIdentifier}
   */
  private final ConcurrentMap<ThreadIdentifier, Object> recordedBulkOps =
      new ConcurrentHashMap<>(100);

  /**
   * a mapping of originator to bulkOperation's last version tags. This map differs from
   * {@link #recordedBulkOps} in that the thread identifier used here is the base member id and
   * thread id of the bulk op, as opposed to the fake thread id which is assigned for each bucket.
   *
   * recordedBulkOps are also only tracked on the secondary for partitioned regions
   * recordedBulkOpVersionTags are tracked on both the primary and secondary.
   *
   * Keys are instances of @link {@link ThreadIdentifier}, values are instances of
   * {@link BulkOperationHolder}.
   */
  private final ConcurrentMap<ThreadIdentifier, BulkOperationHolder> recordedBulkOpVersionTags =
      new ConcurrentHashMap<>(100);

  /**
   * The member that the region corresponding to this tracker (if any) received its initial image
   * from (if a replicate)
   */
  private volatile InternalDistributedMember initialImageProvider;

  /**
   * The region associated with this tracker
   */
  private final DistributedRegion region;

  /**
   * The name of this tracker
   */
  private String name;

  /**
   * whether or not this tracker has been initialized to allow entry operation. replicate region
   * does not initiate event tracker from its replicates.
   */
  private volatile boolean initialized;

  /**
   * object used to wait for initialization
   */
  private final StoppableCountDownLatch initializationLatch;

  /**
   * Create an event tracker
   *
   * @param region the region
   */
  public DistributedEventTracker(DistributedRegion region) {
    this.region = region;
    name = "Event Tracker for " + region.getName();
    initializationLatch = new StoppableCountDownLatch(region.getCancelCriterion(), 1);
  }

  @Override
  public void start() {
    if (region.getCache().getEventTrackerTask() != null) {
      region.getCache().getEventTrackerTask().addTracker(this);
    }
  }

  @Override
  public void stop() {
    if (region.getCache().getEventTrackerTask() != null) {
      region.getCache().getEventTrackerTask().removeTracker(this);
    }
  }

  @Override
  public void clear() {
    recordedEvents.clear();
    recordedBulkOps.clear();
    recordedBulkOpVersionTags.clear();
  }

  @Override
  public Map<ThreadIdentifier, EventSequenceNumberHolder> getState() {
    Map<ThreadIdentifier, EventSequenceNumberHolder> result = new HashMap<>(recordedEvents.size());
    for (Map.Entry<ThreadIdentifier, EventSequenceNumberHolder> entry : recordedEvents.entrySet()) {
      EventSequenceNumberHolder holder = entry.getValue();
      // don't transfer version tags - adds too much bulk just so we can do client tag recovery
      result.put(entry.getKey(),
          new EventSequenceNumberHolder(holder.getLastSequenceNumber(), null));
    }
    return result;
  }

  @Override
  public void recordState(InternalDistributedMember provider,
      Map<ThreadIdentifier, EventSequenceNumberHolder> state) {
    this.initialImageProvider = provider;
    StringBuffer sb = null;
    if (logger.isDebugEnabled()) {
      sb = new StringBuffer(200);
      sb.append("Recording initial state for ").append(this.name).append(": ");
    }
    for (Map.Entry<ThreadIdentifier, EventSequenceNumberHolder> entry : state.entrySet()) {
      if (sb != null) {
        sb.append("\n  ").append(entry.getKey().expensiveToString()).append("; sequenceID=")
            .append(entry.getValue());
      }
      // record only if we haven't received an event that is newer
      recordSequenceNumber(entry.getKey(), entry.getValue(), true);
    }
    if (sb != null) {
      logger.debug(sb);
    }
    // fix for bug 41622 - hang in GII. This keeps ops from waiting for the
    // full GII to complete
    setInitialized();
  }

  @Override
  public void setInitialized() {
    initializationLatch.countDown();
    initialized = true;
  }

  @Override
  public void waitOnInitialization() throws InterruptedException {
    initializationLatch.await();
  }

  /**
   * Record an event sequence id if it is higher than what we currently have. This is intended for
   * use during initial image transfer.
   *
   * @param membershipID the key of an entry in the map obtained from getEventState()
   * @param evhObj the value of an entry in the map obtained from getEventState()
   */
  protected void recordSequenceNumber(ThreadIdentifier membershipID,
      EventSequenceNumberHolder evhObj) {
    recordSequenceNumber(membershipID, evhObj, false);
  }

  /**
   * Record an event sequence id if it is higher than what we currently have. This is intended for
   * use during initial image transfer.
   *
   * @param threadID the key of an entry in the map obtained from getEventState()
   * @param evh the value of an entry in the map obtained from getEventState()
   * @param ifAbsent only record this state if there's not already an entry for this memberID
   */
  private void recordSequenceNumber(ThreadIdentifier threadID, EventSequenceNumberHolder evh,
      boolean ifAbsent) {
    boolean removed;
    if (logger.isDebugEnabled()) {
      logger.debug("recording {} {}", threadID.expensiveToString(), evh.toString());
    }
    do {
      removed = false;
      EventSequenceNumberHolder oldEvh = recordedEvents.putIfAbsent(threadID, evh);
      if (oldEvh != null) {
        synchronized (oldEvh) {
          if (oldEvh.isRemoved()) {
            // need to wait for an entry being removed by the sweeper to go away
            removed = true;
            continue;
          } else {
            if (ifAbsent) {
              break;
            }
            oldEvh.setEndOfLifeTimestamp(0);
            if (oldEvh.getLastSequenceNumber() < evh.getLastSequenceNumber()) {
              oldEvh.setLastSequenceNumber(evh.getLastSequenceNumber());
              oldEvh.setVersionTag(evh.getVersionTag());
            }
          }
        }
      } else {
        evh.setEndOfLifeTimestamp(0);
      }
    } while (removed);
  }

  @Override
  public void recordEvent(InternalCacheEvent event) {
    EventID eventID = event.getEventId();
    if (ignoreEvent(event, eventID)) {
      return; // not tracked
    }

    LocalRegion lr = (LocalRegion) event.getRegion();
    ThreadIdentifier membershipID = createThreadIDFromEvent(eventID);

    VersionTag tag = null;
    if (lr.getServerProxy() == null) {
      tag = event.getVersionTag();
      RegionVersionVector v = ((LocalRegion) event.getRegion()).getVersionVector();
      canonicalizeIDs(tag, v);
    }

    EventSequenceNumberHolder newEvh = new EventSequenceNumberHolder(eventID.getSequenceID(), tag);
    if (logger.isTraceEnabled()) {
      logger.trace("region event tracker recording {}", event);
    }
    recordSequenceNumber(membershipID, newEvh);

    // If this is a bulkOp, and concurrency checks are enabled, we need to
    // save the version tag in case we retry.
    // Make recordBulkOp version tag after recordSequenceNumber, so that recordBulkOpStart
    // in a retry bulk op would not incorrectly remove the saved version tag in
    // recordedBulkOpVersionTags
    if (lr.getConcurrencyChecksEnabled()
        && (event.getOperation().isPutAll() || event.getOperation().isRemoveAll())
        && lr.getServerProxy() == null) {
      recordBulkOpEvent(event, membershipID);
    }
  }

  private ThreadIdentifier createThreadIDFromEvent(EventID eventID) {
    return new ThreadIdentifier(eventID.getMembershipID(), eventID.getThreadID());
  }

  /**
   * Record a version tag for a bulk operation.
   */
  private void recordBulkOpEvent(InternalCacheEvent event, ThreadIdentifier threadID) {
    EventID eventID = event.getEventId();

    VersionTag tag = event.getVersionTag();
    if (tag == null) {
      return;
    }

    if (logger.isDebugEnabled()) {
      logger.debug("recording bulkOp event {} {} {} op={}", threadID.expensiveToString(), eventID,
          tag, event.getOperation());
    }

    RegionVersionVector versionVector = ((LocalRegion) event.getRegion()).getVersionVector();
    canonicalizeIDs(tag, versionVector);

    // Loop until we can successfully update the recorded bulk operations
    // For this thread id.
    boolean retry = false;
    do {
      BulkOperationHolder bulkOpTracker = recordedBulkOpVersionTags.get(threadID);
      if (bulkOpTracker == null) {
        bulkOpTracker = new BulkOperationHolder();
        BulkOperationHolder old = recordedBulkOpVersionTags.putIfAbsent(threadID, bulkOpTracker);
        if (old != null) {
          retry = true;
          continue;
        }
      }
      synchronized (bulkOpTracker) {
        if (bulkOpTracker.isRemoved()) {
          retry = true;
          continue;
        }

        // Add the version tag for bulkOp event.
        bulkOpTracker.putVersionTag(eventID, event.getVersionTag());
        retry = false;
      }
    } while (retry);
  }

  private void canonicalizeIDs(VersionTag tag, RegionVersionVector versionVector) {
    if (tag != null && versionVector != null) {
      tag.setMemberID(versionVector.getCanonicalId(tag.getMemberID()));
      if (tag.getPreviousMemberID() != null) {
        tag.setPreviousMemberID(versionVector.getCanonicalId(tag.getPreviousMemberID()));
      }
    }
  }

  @Override
  public boolean hasSeenEvent(InternalCacheEvent event) {
    EventID eventID = event.getEventId();
    if (ignoreEvent(event, eventID)) {
      return false; // not tracked
    }
    return hasSeenEvent(eventID, event);
  }

  @Override
  public boolean hasSeenEvent(EventID eventID) {
    return hasSeenEvent(eventID, null);
  }

  @Override
  public boolean hasSeenEvent(EventID eventID, InternalCacheEvent tagHolder) {
    if (eventID == null) {
      return false;
    }

    EventSequenceNumberHolder evh = getSequenceHolderForEvent(eventID);
    if (evh == null) {
      return false;
    }

    synchronized (evh) {
      if (evh.isRemoved() || evh.getLastSequenceNumber() < eventID.getSequenceID()) {
        return false;
      }
      Pair<Boolean, String> shouldLogPreviouslySeenEvent =
          shouldLogPreviouslySeenEvent(tagHolder, evh);
      if (shouldLogPreviouslySeenEvent.getLeft()) {
        logger.info(EVENT_HAS_PREVIOUSLY_BEEN_SEEN, region.getName(),
            tagHolder == null ? "unknown" : ((EntryEventImpl) tagHolder).getKey(),
            tagHolder == null ? "unknown" : tagHolder.getOperation(), eventID.expensiveToString(),
            evh.getLastSequenceNumber());
      } else {
        if (logger.isDebugEnabled()) {
          if (tagHolder == null) {
            logger.debug(IGNORING_PREVIOUSLY_SEEN_EVENT_PREFIX,
                shouldLogPreviouslySeenEvent.getRight());
          } else {
            logger.debug(IGNORING_PREVIOUSLY_SEEN_EVENT, shouldLogPreviouslySeenEvent.getRight(),
                region.getName(), ((EntryEventImpl) tagHolder).getKey(), tagHolder.getOperation(),
                tagHolder.getEventId().expensiveToString(), evh.getLastSequenceNumber());
          }
        }
      }
      // bug #44956 - recover version tag for duplicate event
      if (evh.getLastSequenceNumber() == eventID.getSequenceID() && tagHolder != null
          && evh.getVersionTag() != null) {
        ((EntryEventImpl) tagHolder).setVersionTag(evh.getVersionTag());
      }

      // Increment the previously seen events statistic
      region.getCachePerfStats().incPreviouslySeenEvents();

      return true;
    }
  }

  private Pair<Boolean, String> shouldLogPreviouslySeenEvent(InternalCacheEvent event,
      EventSequenceNumberHolder evh) {
    boolean shouldLogSeenEvent = true;
    String message = null;
    if (event != null && ((EntryEventImpl) event).isPossibleDuplicate()) {
      // Ignore the previously seen event if it is a possible duplicate
      message = "possible duplicate";
      shouldLogSeenEvent = false;
    } else if (region instanceof BucketRegion) {
      BucketRegion br = (BucketRegion) region;
      if (br.hasLowRedundancy()) {
        // Ignore the previously seen event while the bucket has low redundancy
        message = "low redundancy";
        shouldLogSeenEvent = false;
      } else if (br.getPartitionedRegion().areRecoveriesInProgress()) {
        // Ignore the previously seen event while recoveries are in progress
        message = "recoveries in progress";
        shouldLogSeenEvent = false;
      }
    }
    return Pair.of(shouldLogSeenEvent, message);
  }

  private EventSequenceNumberHolder getSequenceHolderForEvent(EventID eventID) {
    ThreadIdentifier membershipID = createThreadIDFromEvent(eventID);
    return recordedEvents.get(membershipID);
  }

  @Override
  public VersionTag findVersionTagForSequence(EventID eventID) {
    EventSequenceNumberHolder evh = getSequenceHolderForEvent(eventID);
    if (evh == null) {
      if (logger.isDebugEnabled()) {
        logger.debug("search for version tag failed as no event is recorded for {}",
            createThreadIDFromEvent(eventID).expensiveToString());
      }
      return null;
    }

    synchronized (evh) {
      if (logger.isDebugEnabled()) {
        logger.debug("search for version tag located last event for {}: {}",
            createThreadIDFromEvent(eventID).expensiveToString(), evh);
      }
      if (evh.getLastSequenceNumber() != eventID.getSequenceID()) {
        return null;
      }
      // log at fine because partitioned regions can send event multiple times
      // during normal operation during bucket region initialization
      if (logger.isTraceEnabled(LogMarker.DISTRIBUTION_BRIDGE_SERVER_VERBOSE)
          && evh.getVersionTag() == null) {
        logger.trace(LogMarker.DISTRIBUTION_BRIDGE_SERVER_VERBOSE,
            "Could not recover version tag.  Found event holder with no version tag for {}",
            eventID);
      }
      return evh.getVersionTag();
    }
  }

  @Override
  public VersionTag findVersionTagForBulkOp(EventID eventID) {
    if (eventID == null) {
      return null;
    }
    ThreadIdentifier threadID = createThreadIDFromEvent(eventID);
    BulkOperationHolder evh = recordedBulkOpVersionTags.get(threadID);
    if (evh == null) {
      if (logger.isDebugEnabled()) {
        logger.debug("search for version tag failed as no events are recorded for {}",
            threadID.expensiveToString());
      }
      return null;
    }

    synchronized (evh) {
      if (logger.isDebugEnabled()) {
        logger.debug("search for version tag located event holder for {}: {}",
            threadID.expensiveToString(), evh);
      }
      return evh.getEntryVersionTags().get(eventID);
    }
  }

  @Override
  public String getName() {
    return name;
  }

  /**
   * @return true if the event should not be tracked, false otherwise
   */
  private boolean ignoreEvent(InternalCacheEvent event, EventID eventID) {
    if (eventID == null) {
      return true;
    } else {
      boolean isVersioned = (event.getVersionTag() != null);
      boolean isClient = event.hasClientOrigin();
      if (isVersioned && isClient) {
        return false; // version tags for client events are kept for retries by the client
      }
      boolean isEntry = event.getOperation().isEntry();
      boolean isPr = event.getRegion().getAttributes().getDataPolicy().withPartitioning()
          || ((LocalRegion) event.getRegion()).isUsedForPartitionedRegionBucket();
      return (!isClient && // ignore if it originated on a server, and
          isEntry && // it affects an entry and
          !isPr); // is not on a PR
    }
  }

  @Override
  public void syncBulkOp(Runnable r, EventID eventID, boolean partOfTransaction) {
    if (partOfTransaction) {
      r.run();
      return;
    }
    Assert.assertTrue(eventID != null);
    ThreadIdentifier membershipID = createThreadIDFromEvent(eventID);
    Object opSyncObj = null;
    do {
      opSyncObj = recordedBulkOps.putIfAbsent(membershipID, new Object());
      if (opSyncObj == null) {
        opSyncObj = recordedBulkOps.get(membershipID);
      }
    } while (opSyncObj == null);

    synchronized (opSyncObj) {
      try {
        recordBulkOpStart(eventID, membershipID);
        // Perform the bulk op
        r.run();
      } finally {
        recordedBulkOps.remove(membershipID);
      }
    }
  }

  @Override
  public void recordBulkOpStart(EventID eventID, ThreadIdentifier tid) {
    if (logger.isDebugEnabled()) {
      logger.debug("recording bulkOp start for {}", tid.expensiveToString());
    }
    EventSequenceNumberHolder evh = recordedEvents.get(tid);
    if (evh == null) {
      return;
    }
    synchronized (evh) {
      // only remove it when a new bulk op occurs
      if (eventID.getSequenceID() > evh.getLastSequenceNumber()) {
        this.recordedBulkOpVersionTags.remove(tid);
      }
    }
  }

  @Override
  public boolean isInitialized() {
    return this.initialized;
  }

  @Override
  public boolean isInitialImageProvider(DistributedMember mbr) {
    return (this.initialImageProvider != null) && (mbr != null)
        && this.initialImageProvider.equals(mbr);
  }

  @Override
  public ConcurrentMap<ThreadIdentifier, BulkOperationHolder> getRecordedBulkOpVersionTags() {
    return recordedBulkOpVersionTags;
  }

  @Override
  public ConcurrentMap<ThreadIdentifier, EventSequenceNumberHolder> getRecordedEvents() {
    return recordedEvents;
  }

  @Override
  public String toString() {
    return "" + this.name + "(initialized=" + this.initialized + ")";
  }

}
