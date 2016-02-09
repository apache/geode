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
/**
 * 
 */
package com.gemstone.gemfire.internal.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.client.PoolFactory;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.SystemTimer.SystemTimerTask;
import com.gemstone.gemfire.internal.cache.ha.ThreadIdentifier;
import com.gemstone.gemfire.internal.cache.versions.RegionVersionVector;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LogMarker;
import com.gemstone.gemfire.internal.util.concurrent.StoppableCountDownLatch;

/**
 * EventTracker tracks the last sequence number for a particular
 * memberID:threadID.  It is used to avoid replaying events in
 * client/server and partitioned-region configurations.
 * 
 * @author bruce
 * @since 6.0
 */
public class EventTracker
{
  private static final Logger logger = LogService.getLogger();
  
  /**
   * a mapping of originator to the last event applied to this cache 
   *
   * Keys are instances of {@link ThreadIdentifier}, values are instances
   * of {@link com.gemstone.gemfire.internal.cache.EventTracker.EventSeqnoHolder}.
   */
  protected final ConcurrentMap<ThreadIdentifier, EventSeqnoHolder> recordedEvents 
      = new ConcurrentHashMap<ThreadIdentifier, EventSeqnoHolder>(100);
 
  /** a mapping of originator to bulkOp's last status (true means
   * finished processing) applied to this cache. 
   *
   * Keys are instances of @link {@link ThreadIdentifier}, values are instances
   * of {@link BulkOpProcessed}.
   */
  private final ConcurrentMap<ThreadIdentifier, BulkOpProcessed> recordedBulkOps 
      = new ConcurrentHashMap<ThreadIdentifier, BulkOpProcessed>(100);
  
  /** a mapping of originator to bulkOperation's last version tags. This map
   * differs from {@link #recordedBulkOps} in that the thread identifier used
   * here is the base member id and thread id of the bulk op, as opposed to the fake
   * thread id which is assigned for each bucket.
   * 
   * recordedBulkOps are also only tracked on the secondary for partitioned regions
   * recordedBulkOpVersionTags are tracked on both the primary and secondary.
   *
   * Keys are instances of @link {@link ThreadIdentifier}, values are instances
   * of {@link BulkOpHolder}.
   */
  private final ConcurrentMap<ThreadIdentifier, BulkOpHolder> recordedBulkOpVersionTags 
      = new ConcurrentHashMap<ThreadIdentifier, BulkOpHolder>(100);
  
  /**
   * The member that the region corresponding to this tracker (if any)
   * received its initial image from (if a replicate)
   */
  private volatile InternalDistributedMember initialImageProvider;

 
  /**
   * The cache associated with this tracker
   */
  GemFireCacheImpl cache;
 
  /**
   * The name of this tracker
   */
  String name;
  
  /**
   * whether or not this tracker has been initialized with state from
   * another process
   */
  volatile boolean initialized;
 
  /**
   * object used to wait for initialization 
   */
  final StoppableCountDownLatch initializationLatch;
  
  /**
  * Initialize the EventTracker's timer task.  This is stored in the cache
  * for tracking and shutdown purposes
  * @param cache the cache to schedule tasks with
  */
  public static ExpiryTask startTrackerServices(GemFireCacheImpl cache) {
    long expiryTime = Long.getLong("gemfire.messageTrackingTimeout",
        PoolFactory.DEFAULT_SUBSCRIPTION_MESSAGE_TRACKING_TIMEOUT / 3).longValue();
    ExpiryTask result = new ExpiryTask(cache, expiryTime);
    cache.getCCPTimer().scheduleAtFixedRate(result,
        expiryTime, expiryTime);
    //schedule(result, expiryTime);
    return result;
  }
  
  /**
   * Terminate the tracker's timer task
   * @param cache the cache holding the tracker task
   */
  public static void stopTrackerServices(GemFireCacheImpl cache) {
    cache.getEventTrackerTask().cancel();
  }
 
 /**
  * Create an event tracker
  * @param region the cache region to associate with this tracker
  */
  public EventTracker(LocalRegion region) {
   this.cache = region.cache;
   this.name = "Event Tracker for " + region.getName();
   this.initializationLatch = new StoppableCountDownLatch(region.stopper, 1);
 }

  /** start this event tracker */
  public void start() {
    if (this.cache.getEventTrackerTask() != null) {
      this.cache.getEventTrackerTask().addTracker(this);
    }
  }
  
  /** stop this event tracker */
  public void stop() {
    if (this.cache.getEventTrackerTask() != null) {
      this.cache.getEventTrackerTask().removeTracker(this);
    }
  }
  
  /**
   * retrieve a deep copy of the state of the event tracker.  Synchronization
   * is not used while copying the tracker's state.
   */
  public Map<ThreadIdentifier, EventSeqnoHolder> getState() {
    Map<ThreadIdentifier, EventSeqnoHolder> result = new HashMap<ThreadIdentifier, EventSeqnoHolder>(recordedEvents.size());
    for (Iterator<Map.Entry<ThreadIdentifier, EventSeqnoHolder>> it=recordedEvents.entrySet().iterator(); it.hasNext(); ) {
      Map.Entry<ThreadIdentifier, EventSeqnoHolder> entry = it.next();
      EventSeqnoHolder holder = entry.getValue();
      result.put(entry.getKey(), new EventSeqnoHolder(
          holder.lastSeqno, null)); // don't transfer version tags - adds too much bulk just so we can do client tag recovery
    }
    return result;
  }
  
  /**
   * record the given state in the tracker.
   * @param provider the member that provided this state
   * @param state a Map obtained from getState();
   */
  public void recordState(InternalDistributedMember provider, Map<ThreadIdentifier, EventSeqnoHolder> state) {
    this.initialImageProvider = provider;
    StringBuffer sb = null;
    if (logger.isDebugEnabled()) {
      sb = new StringBuffer(200);
      sb.append("Recording initial state for ")
        .append(this.name)
        .append(": ");
    }
    for (Iterator<Map.Entry<ThreadIdentifier, EventSeqnoHolder>> it=state.entrySet().iterator(); it.hasNext(); ) {
      Map.Entry<ThreadIdentifier, EventSeqnoHolder> entry = it.next();
      if (sb != null) {
        sb.append("\n  ")
          .append(entry.getKey().expensiveToString())
          .append("; sequenceID=")
          .append(entry.getValue()); 
      }
      // record only if we haven't received an event that is newer
      recordSeqno(entry.getKey(), entry.getValue(), true);
    }
    if (sb != null) {
      logger.debug(sb);
    }
    // fix for bug 41622 - hang in GII.  This keeps ops from waiting for the
    // full GII to complete
    setInitialized();
  }
  
  /**
   * Use this method to ensure that the tracker is put in an initialized state
   */
  public void setInitialized() {
    this.initializationLatch.countDown();
    this.initialized = true;
  }
  
  /**
   * Wait for the tracker to finishe being initialized
   */
  public void waitOnInitialization() throws InterruptedException {
    this.initializationLatch.await();
  }
  
  /**
   * Record an event sequence id if it is higher than what we currently have.
   * This is intended for use during initial image transfer.
   * @param membershipID the key of an entry in the map obtained from getEventState()
   * @param evhObj the value of an entry in the map obtained from getEventState()
   */
  protected void recordSeqno(ThreadIdentifier membershipID, EventSeqnoHolder evhObj){
    recordSeqno(membershipID, evhObj, false);
  }
  
  /**
   * Record an event sequence id if it is higher than what we currently have.
   * This is intended for use during initial image transfer.
   * @param threadID the key of an entry in the map obtained from getEventState()
   * @param evh the value of an entry in the map obtained from getEventState()
   * @param ifAbsent only record this state if there's not already an entry for this memberID
   */
  private void recordSeqno(ThreadIdentifier threadID, EventSeqnoHolder evh, boolean ifAbsent) {
    boolean removed;
    if (logger.isDebugEnabled()) {
      logger.debug("recording {} {}", threadID.expensiveToString(), evh.toString());
    }
    do {
      removed = false;
      EventSeqnoHolder oldEvh = recordedEvents.putIfAbsent(
          threadID, evh);
      if (oldEvh != null) {
        synchronized(oldEvh) {
          if (oldEvh.removed) {
            // need to wait for an entry being removed by the sweeper to go away
            removed = true;
            continue;
          }
          else {
            if (ifAbsent) {
              break;
            }
            oldEvh.endOfLifeTimer = 0;
            if (oldEvh.lastSeqno < evh.lastSeqno) {
              oldEvh.lastSeqno = evh.lastSeqno;
              oldEvh.versionTag = evh.versionTag;
//              Exception e = oldEvh.context;
//              oldEvh.context = new Exception("stack trace");
//              oldEvh.context.initCause(e);
            }
          }
        }
      }
      else {
        evh.endOfLifeTimer = 0;
//        evh.context = new Exception("stack trace");
      }
    } while (removed);
  }
  
  /** record the event's threadid/sequenceid to prevent replay */
  public void recordEvent(InternalCacheEvent event) {
    EventID eventID = event.getEventId();
    if (ignoreEvent(event, eventID)) {
      return; // not tracked
    }

    LocalRegion lr = (LocalRegion)event.getRegion();

    ThreadIdentifier membershipID = new ThreadIdentifier(eventID.getMembershipID(),
        eventID.getThreadID());

    VersionTag tag = null;
    if (lr.getServerProxy() == null/* && event.hasClientOrigin()*/) { // clients do not need to store version tags for replayed events
      tag = event.getVersionTag();
      RegionVersionVector v = ((LocalRegion)event.getRegion()).getVersionVector();
      // bug #46453 - make sure ID references are canonical before storing
      if (v != null && tag != null) {
        tag.setMemberID(v.getCanonicalId(tag.getMemberID()));
        if (tag.getPreviousMemberID() != null) {
          tag.setPreviousMemberID(v.getCanonicalId(tag.getPreviousMemberID()));
        }
      }
    }
    EventSeqnoHolder newEvh = new EventSeqnoHolder(eventID.getSequenceID(), tag);
    if (logger.isTraceEnabled()){
      logger.trace("region event tracker recording {}", event);
    }
    recordSeqno(membershipID, newEvh);
    
    //If this is a bulkOp, and concurrency checks are enabled, we need to
    //save the version tag in case we retry.
    if (lr.concurrencyChecksEnabled 
        && (event.getOperation().isPutAll() || event.getOperation().isRemoveAll()) && lr.getServerProxy() == null) {
      recordBulkOpEvent(event, membershipID);
    }
  }

  /**
   * Record a version tag for a bulk operation
   */
  private void recordBulkOpEvent(InternalCacheEvent event, ThreadIdentifier tid) {
    EventID eventID = event.getEventId();
    
    VersionTag tag = event.getVersionTag();
    if (tag == null) {
      return;
    }
    
    if (logger.isDebugEnabled()) {
      logger.debug("recording bulkOp event {} {} {} op={}", tid.expensiveToString(),
          eventID, tag, event.getOperation());
    }

    RegionVersionVector v = ((LocalRegion)event.getRegion()).getVersionVector();
    // bug #46453 - make sure ID references are canonical before storing
    if (v != null) {
      tag.setMemberID(v.getCanonicalId(tag.getMemberID()));
      if (tag.getPreviousMemberID() != null) {
        tag.setPreviousMemberID(v.getCanonicalId(tag.getPreviousMemberID()));
      }
    }

    //Loop until we can successfully update the recorded bulk operations
    //For this thread id.
    boolean retry = false;
    do {
      BulkOpHolder bulkOpTracker = recordedBulkOpVersionTags.get(tid);
      if(bulkOpTracker == null) {
        bulkOpTracker = new BulkOpHolder();
        BulkOpHolder old = recordedBulkOpVersionTags.putIfAbsent(tid, bulkOpTracker);
        if(old != null) {
          retry = true;
          continue;
        }
      }
      synchronized(bulkOpTracker) {
        if(bulkOpTracker.removed) {
          retry = true;
          continue;
        }
        
        //Add the version tag for bulkOp event.
        bulkOpTracker.putVersionTag(eventID, event.getVersionTag());
        retry = false;
      }
    } while(retry);
  }
  
  public boolean hasSeenEvent(InternalCacheEvent event) {
//  ClientProxyMembershipID membershipID = event.getContext();
    EventID eventID = event.getEventId();
    if (ignoreEvent(event, eventID)) {
      return false; // not tracked
    }
    return hasSeenEvent(eventID, event);
  }
  
  public boolean hasSeenEvent(EventID eventID) {
    return hasSeenEvent(eventID, null);
  }

  public boolean hasSeenEvent(EventID eventID, InternalCacheEvent tagHolder) {
    ThreadIdentifier membershipID = new ThreadIdentifier(
        eventID.getMembershipID(), eventID.getThreadID());
//  if (membershipID == null || eventID == null) {
//    return false;
//  }
        
    EventSeqnoHolder evh = recordedEvents.get(membershipID);
    if (evh == null) {
      return false;
    }
    
    synchronized (evh) {
      if (evh.removed || evh.lastSeqno < eventID.getSequenceID()) {
        return false;
      }
      // log at fine because partitioned regions can send event multiple times
      // during normal operation during bucket region initialization
      if (logger.isTraceEnabled(LogMarker.DISTRIBUTION_BRIDGE_SERVER)) {
        logger.trace(LogMarker.DISTRIBUTION_BRIDGE_SERVER, "Cache encountered replay of event with ID {}.  Highest recorded for this source is {}",
            eventID, evh.lastSeqno);
      }
      // bug #44956 - recover version tag for duplicate event
      if (evh.lastSeqno == eventID.getSequenceID() && tagHolder != null && evh.versionTag != null) {
        ((EntryEventImpl)tagHolder).setVersionTag(evh.versionTag);
      }
      return true;
    } // synchronized
  }

  public VersionTag findVersionTag(EventID eventID) {
    ThreadIdentifier threadID = new ThreadIdentifier(
        eventID.getMembershipID(), eventID.getThreadID());
        
    EventSeqnoHolder evh = recordedEvents.get(threadID);
    if (evh == null) {
      if (logger.isDebugEnabled()) {
        logger.debug("search for version tag failed as no event is recorded for {}", threadID.expensiveToString());
      }
      return null;
    }
    
    synchronized (evh) {
      if (logger.isDebugEnabled()) {
        logger.debug("search for version tag located last event for {}: {}",threadID.expensiveToString(), evh);
      }
      if (evh.lastSeqno != eventID.getSequenceID()) {
        return null;
      }
      // log at fine because partitioned regions can send event multiple times
      // during normal operation during bucket region initialization
      if (logger.isTraceEnabled(LogMarker.DISTRIBUTION_BRIDGE_SERVER) && evh.versionTag == null) {
        logger.trace(LogMarker.DISTRIBUTION_BRIDGE_SERVER, "Could not recover version tag.  Found event holder with no version tag for {}", eventID);
      }
      return evh.versionTag;
    } // synchronized
  }
  
  public VersionTag findVersionTagForGateway(EventID eventID) {
    ThreadIdentifier threadID = new ThreadIdentifier(
        eventID.getMembershipID(), eventID.getThreadID());
        
    EventSeqnoHolder evh = recordedEvents.get(threadID);
    if (evh == null) {
      if (logger.isDebugEnabled()) {
        logger.debug("search for version tag failed as no event is recorded for {}", threadID.expensiveToString());
      }
      return null;
    }
    
    synchronized (evh) {
      if (logger.isDebugEnabled()) {
        logger.debug("search for version tag located last event for {}: {} {}",threadID.expensiveToString(), evh, eventID.getSequenceID() );
      }
      
      if (evh.lastSeqno < eventID.getSequenceID()) {
        return null;
      }
      // log at fine because partitioned regions can send event multiple times
      // during normal operation during bucket region initialization
      if (logger.isTraceEnabled(LogMarker.DISTRIBUTION_BRIDGE_SERVER) && evh.versionTag == null) {
        logger.trace(LogMarker.DISTRIBUTION_BRIDGE_SERVER, "Could not recover version tag.  Found event holder with no version tag for {}", eventID);
      }
      return evh.versionTag;
    } // synchronized
  }
  
  
  public VersionTag findVersionTagForBulkOp(EventID eventID) {
    ThreadIdentifier threadID = new ThreadIdentifier(
        eventID.getMembershipID(), eventID.getThreadID());
        
    BulkOpHolder evh = recordedBulkOpVersionTags.get(threadID);
    if (evh == null) {
      if (logger.isDebugEnabled()) {
        logger.debug("search for version tag failed as no events are recorded for {}", threadID.expensiveToString());
      }
      return null;
    }
    
    synchronized (evh) {
      if (logger.isDebugEnabled()) {
        logger.debug("search for version tag located event holder for {}: {}", threadID.expensiveToString(), evh);
      }
      return evh.entryVersionTags.get(eventID);
    } // synchronized
  }

  /**
   * @param event
   * @param eventID
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
                   || ((LocalRegion)event.getRegion()).isUsedForPartitionedRegionBucket();
      return (!isClient &&   // ignore if it originated on a server, and
           isEntry &&        // it affects an entry and
          !isPr);            // is not on a PR
    }
  }

  /**
   * A routine to provide synchronization running based on <memberShipID, threadID> 
   * of the requesting client
   * @param r - a Runnable to wrap the processing of the bulk op
   * @param eventID - the base event ID of the bulk op
   *
   * @since 5.7
   */
  public void syncBulkOp(Runnable r, EventID eventID) {
    Assert.assertTrue(eventID != null);
    ThreadIdentifier membershipID = new ThreadIdentifier(
      eventID.getMembershipID(), eventID.getThreadID());

    BulkOpProcessed opSyncObj = recordedBulkOps.putIfAbsent(membershipID, new BulkOpProcessed(false));
    if (opSyncObj == null) {
      opSyncObj = recordedBulkOps.get(membershipID);
    }
    synchronized (opSyncObj) {
      try {
        if (opSyncObj.getStatus() && logger.isDebugEnabled()) {
          logger.debug("SyncBulkOp: The operation was performed by another thread.");
        }
        else {
          recordBulkOpStart(membershipID);
          
          //Perform the bulk op
          r.run();
          // set to true in case another thread is waiting at sync 
          opSyncObj.setStatus(true);
          recordedBulkOps.remove(membershipID);
        }
      }
      finally {
        recordedBulkOps.remove(membershipID);
      }
    }
  }
  
  /**
   * Called when a bulkOp is started on the local region. Used to clear
   * event tracker state from the last bulkOp.
   */
  public void recordBulkOpStart(ThreadIdentifier tid) {
    if (logger.isDebugEnabled()) {
      logger.debug("recording bulkOp start for {}", tid.expensiveToString());
    }
    this.recordedBulkOpVersionTags.remove(tid);
  }
  
  /**
   * @return the initialized
   */
  public boolean isInitialized() {
    return this.initialized;
  }
  
  /**
   * @param mbr the member in question
   * @return true if the given member provided the initial image event state for this tracker
   */
  public boolean isInitialImageProvider(DistributedMember mbr) {
    return (this.initialImageProvider != null)
      && (mbr != null)
      && this.initialImageProvider.equals(mbr);
  }
  
  /**
   * Test method for getting the set of recorded version tags.
   */
  protected ConcurrentMap<ThreadIdentifier, BulkOpHolder> getRecordedBulkOpVersionTags() {
    return recordedBulkOpVersionTags;
  }
  
  @Override
  public String toString() {
    return ""+this.name+"(initialized=" + this.initialized+")";
  }
  
  /**
   * A sequence number tracker to keep events from clients from being
   * re-applied to the cache if they've already been seen.
   * @author bruce
   * @since 5.5
   */
  static class EventSeqnoHolder implements DataSerializable {
    private static final long serialVersionUID = 8137262960763308046L;

    /** event sequence number.  These  */
    long lastSeqno = -1;
    
    /** millisecond timestamp */
    transient long endOfLifeTimer;
    
    /** whether this entry is being removed */
    transient boolean removed;
    
    /**
     * version tag, if any, for the operation
     */
    VersionTag versionTag;
    
    // for debugging
//    transient Exception context;
    
    EventSeqnoHolder(long id, VersionTag versionTag) {
      this.lastSeqno = id;
      this.versionTag = versionTag;
    }
    
    public EventSeqnoHolder() {
    }
    
    @Override
    public String toString() {
      StringBuilder result = new StringBuilder();
      result.append("seqNo").append(this.lastSeqno);
      if (this.versionTag != null) {
        result.append(",").append(this.versionTag);
      }
      return result.toString();
    }

    public void fromData(DataInput in) throws IOException,
        ClassNotFoundException {
      lastSeqno = in.readLong();
      versionTag = (VersionTag)DataSerializer.readObject(in);
    }

    public void toData(DataOutput out) throws IOException {
      out.writeLong(lastSeqno);
      DataSerializer.writeObject(versionTag, out);
    }
  }

  /**
   * A status tracker for each bulk operation (putAll or removeAll) from originators specified by
   * membershipID and threadID in the cache
   * processed is true means the bulk op is processed by one thread 
   * no need to redo it by other threads.
   * @author Gester
   * @since 5.7
   */
  static class BulkOpProcessed {
    /** whether the op is processed */
    private boolean processed;
  
    /**
     * creates a new instance to save status of a bulk op 
     * @param status true if the op has been processed 
     */
    BulkOpProcessed(boolean status) {
      this.processed = status;
    }
    
    /**
     * setter method to change the status
     * @param status true if the op has been processed 
     */
    void setStatus(boolean status) {
      this.processed = status;
    }
    
    /**
     * getter method to peek the current status
     * @return current status
     */
    boolean getStatus() {
      return this.processed;
    }
    
    @Override
    public String toString() {
      return "BULKOP("+this.processed+")";
    }
  }
  
  /**
   * A holder for the version tags generated for a bulk operation (putAll or removeAll). These
   * version tags are retrieved when a bulk op is retried.
   * @author Dan
   * @since 7.0
   * protected for test purposes only.
   */
  protected static class BulkOpHolder {
    /**
     * Whether this object was removed by the cleanup thread.
     */
    public boolean removed;
    /**
     * public for tests only
     */
    public Map<EventID, VersionTag> entryVersionTags = new HashMap<EventID, VersionTag>();
    /** millisecond timestamp */
    transient long endOfLifeTimer;
  
    /**
     * creates a new instance to save status of a putAllOperation 
     */
    BulkOpHolder() {
    }
    
    public void putVersionTag(EventID eventId, VersionTag versionTag) {
      entryVersionTags.put(eventId, versionTag);
      this.endOfLifeTimer = 0;
    }

    
    @Override
    public String toString() {
      return "BulkOpHolder tags=" + this.entryVersionTags;
    }
  }
  
  static class ExpiryTask extends SystemTimerTask {
    
    GemFireCacheImpl cache;
    long expiryTime;
    List trackers = new LinkedList();
    
    public ExpiryTask(GemFireCacheImpl cache, long expiryTime) {
      this.cache = cache;
      this.expiryTime = expiryTime;
    }
    void addTracker(EventTracker tracker) {
      synchronized(trackers) {
        trackers.add(tracker);
      }
    }
    void removeTracker(EventTracker tracker) {
      synchronized(trackers) {
        trackers.remove(tracker);
      }
    }
    int getNumberOfTrackers() {
      return trackers.size();
    }
    @Override
    public void run2() {
      long now = System.currentTimeMillis();
      long timeout = now - expiryTime;
      final boolean traceEnabled = logger.isTraceEnabled();
      synchronized(trackers) {
        for (Iterator it=trackers.iterator(); it.hasNext(); ) {
          EventTracker tracker = (EventTracker)it.next();
          if (traceEnabled) {
            logger.trace("{} sweeper: starting", tracker.name);
          }
          for (Iterator it2 = tracker.recordedEvents.entrySet().iterator(); it2.hasNext();) {
            Map.Entry e = (Map.Entry)it2.next();
            EventSeqnoHolder evh = (EventSeqnoHolder)e.getValue();
            synchronized(evh) {
              if (evh.endOfLifeTimer == 0) {
                evh.endOfLifeTimer = now; // a new holder - start the timer 
              }
              if (evh.endOfLifeTimer <= timeout) {
                evh.removed = true;
                evh.lastSeqno = -1;
                if (traceEnabled) {
                  logger.trace("{} sweeper: removing {}", tracker.name, e.getKey());
                }
                it2.remove();
              }
            }
          }
          
          //Remove bulk operations we're tracking
          for (Iterator<Map.Entry<ThreadIdentifier, BulkOpHolder>> it2 = tracker.recordedBulkOpVersionTags.entrySet().iterator(); it2.hasNext();) {
            Map.Entry<ThreadIdentifier, BulkOpHolder> e = it2.next();
            BulkOpHolder evh = e.getValue();
            synchronized(evh) {
              if (evh.endOfLifeTimer == 0) {
                evh.endOfLifeTimer = now; // a new holder - start the timer 
              }
              //Remove the PutAll tracker only if the put all is complete
              //and it has expired.
              if (evh.endOfLifeTimer <= timeout) {
                evh.removed = true;
                if (logger.isTraceEnabled()) {
                  logger.trace("{} sweeper: removing bulkOp {}", tracker.name, e.getKey());
                }
                it2.remove();
              }
            }
          }
          if (traceEnabled) {
            logger.trace("{} sweeper: done", tracker.name);
          }
        }
      }
    }
    
  }
}
