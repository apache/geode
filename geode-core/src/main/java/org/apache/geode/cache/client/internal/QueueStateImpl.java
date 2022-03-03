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
package org.apache.geode.cache.client.internal;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.Logger;

import org.apache.geode.SystemFailure;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.client.internal.PoolImpl.PoolTask;
import org.apache.geode.internal.cache.ClientServerObserver;
import org.apache.geode.internal.cache.ClientServerObserverHolder;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.ha.ThreadIdentifier;
import org.apache.geode.logging.internal.log4j.api.LogService;

public class QueueStateImpl implements QueueState {
  private static final Logger logger = LogService.getLogger();

  protected QueueManager qManager = null;
  private boolean processedMarker = false;
  private final AtomicInteger invalidateCount = new AtomicInteger();

  /**
   * This will store the ThreadId to latest received sequence Id
   *
   * Keys are instances of {@link ThreadIdentifier} Values are instances of
   * {@link SequenceIdAndExpirationObject}
   */
  protected final Map threadIdToSequenceId = new LinkedHashMap();

  public QueueStateImpl(QueueManager qm) {
    qManager = qm;
  }

  @Override
  public void processMarker() {
    if (!processedMarker) {
      handleMarker();
      processedMarker = true;
    } else {
      if (logger.isDebugEnabled()) {
        logger.debug("{}: extra marker received", this);
      }
    }
  }

  @Override
  public boolean getProcessedMarker() {
    return processedMarker;
  }

  public void handleMarker() {
    ArrayList regions = new ArrayList();
    Cache cache = GemFireCacheImpl.getInstance();
    if (cache == null) {
      return;
    }

    Set rootRegions = cache.rootRegions();

    for (final Object value : rootRegions) {
      Region rootRegion = (Region) value;
      regions.add(rootRegion);
      try {
        Set subRegions = rootRegion.subregions(true); // throws RDE
        for (final Object subRegion : subRegions) {
          regions.add(subRegion);
        }
      } catch (RegionDestroyedException e) {
        continue; // region is gone go to the next one bug 38705
      }
    }

    for (final Object o : regions) {
      LocalRegion region = (LocalRegion) o;
      try {
        if (region.getAttributes().getPoolName() != null
            && region.getAttributes().getPoolName().equals(qManager.getPool().getName())) {
          region.handleMarker(); // can this throw RDE??
        }
      } catch (RegionDestroyedException e) {
        continue; // region is gone go to the next one bug 38705
      }
    }
  }

  @Override
  public void incrementInvalidatedStats() {
    invalidateCount.incrementAndGet();

  }

  public int getInvalidateCount() {
    return invalidateCount.get();
  }

  /**
   * test hook - access to this map should be synchronized on the map to avoid concurrent
   * modification exceptions
   */
  @Override
  public Map getThreadIdToSequenceIdMap() {
    return threadIdToSequenceId;
  }

  @Override
  public boolean verifyIfDuplicate(EventID eid) {
    return verifyIfDuplicate(eid, true);
  }

  @Override
  public boolean verifyIfDuplicate(EventID eid, boolean addToMap) {
    ThreadIdentifier tid = new ThreadIdentifier(eid.getMembershipID(), eid.getThreadID());
    long seqId = eid.getSequenceID();
    SequenceIdAndExpirationObject seo = null;

    // Fix 36930: save the max sequence id for each non-putAll operation's thread
    // There're totally 3 cases to consider:
    // check the tid:
    // 1) if duplicated, (both putall or non-putall): reject
    // 2) if not duplicate
    // 2.1)if putAll, check via real thread id again,
    // if duplicate, reject (because one non-putall operation with bigger
    // seqno has happened)
    // otherwise save the putAllSeqno for real thread id
    // and save seqno for tid
    // 2.2) if not putAll,
    // check putAllSequenceId with real thread id
    // if request's seqno is smaller, reject (because one putAll operation
    // with bigger seqno has happened)
    // otherwise, update the seqno for tid
    // lock taken to avoid concurrentModification
    // while the objects are being expired
    synchronized (threadIdToSequenceId) {
      seo = (SequenceIdAndExpirationObject) threadIdToSequenceId.get(tid);
      if (seo != null && seo.getSequenceId() >= seqId) {
        if (logger.isDebugEnabled()) {
          logger.debug(" got a duplicate entry with EventId {}. Ignoring the entry", eid);
        }
        seo.setAckSend(false);
        return true;
      } else if (addToMap) {
        ThreadIdentifier real_tid = new ThreadIdentifier(eid.getMembershipID(),
            ThreadIdentifier.getRealThreadIDIncludingWan(eid.getThreadID()));
        if (ThreadIdentifier.isPutAllFakeThreadID(eid.getThreadID())) {
          // it's a putAll
          seo = (SequenceIdAndExpirationObject) threadIdToSequenceId.get(real_tid);
          if (seo != null && seo.getSequenceId() >= seqId) {
            if (logger.isDebugEnabled()) {
              logger.debug(
                  "got a duplicate putAll entry with eventId {}. Other operation with same thread id and bigger seqno {} has happened. Ignoring the entry",
                  eid, seo.getSequenceId());
            }
            seo.setAckSend(false); // bug #41289: send ack to servers that send old events
            return true;
          } else {
            // save the seqno for real thread id into a putAllSequenceId
            threadIdToSequenceId.remove(real_tid);
            threadIdToSequenceId.put(real_tid,
                seo == null ? new SequenceIdAndExpirationObject(-1, seqId)
                    : new SequenceIdAndExpirationObject(seo.getSequenceId(), seqId));
            // save seqno for tid
            // here tid!=real_tid, for fake tid, putAllSeqno should be 0
            threadIdToSequenceId.remove(tid);
            threadIdToSequenceId.put(tid, new SequenceIdAndExpirationObject(seqId, -1));
          }
        } else {
          // non-putAll operation:
          // check putAllSeqno for real thread id
          // if request's seqno is smaller, reject
          // otherwise, update the seqno for tid
          seo = (SequenceIdAndExpirationObject) threadIdToSequenceId.get(real_tid);
          if (seo != null && seo.getPutAllSequenceId() >= seqId) {
            if (logger.isDebugEnabled()) {
              logger.debug(
                  "got a duplicate non-putAll entry with eventId {}. One putAll operation with same real thread id and bigger seqno {} has happened. Ignoring the entry",
                  eid, seo.getPutAllSequenceId());
            }
            seo.setAckSend(false); // bug #41289: send ack to servers that send old events
            return true;
          } else {
            // here tid==real_tid
            threadIdToSequenceId.remove(tid);
            threadIdToSequenceId.put(tid,
                seo == null ? new SequenceIdAndExpirationObject(seqId, -1)
                    : new SequenceIdAndExpirationObject(seqId, seo.getPutAllSequenceId()));
          }
        }
      }
    }
    return false;
  }

  @Override
  public void start(ScheduledExecutorService timer, int interval) {
    timer.scheduleWithFixedDelay(new ThreadIdToSequenceIdExpiryTask(), interval, interval,
        TimeUnit.MILLISECONDS);
  }

  /**
   *
   * Thread which will iterate over threadIdToSequenceId map
   *
   * 1)It will send an ack primary server for all threadIds for which it has not send an ack. 2)It
   * will expire the entries which have exceeded the specified expiry time and for which ack has
   * been alerady sent.
   *
   * @since GemFire 5.1
   *
   */

  private class ThreadIdToSequenceIdExpiryTask extends PoolTask {
    /**
     * The expiry time of the entries in the map
     */
    private final long expiryTime;

    /**
     * constructs the Thread and initializes the expiry time
     *
     */
    public ThreadIdToSequenceIdExpiryTask() {
      expiryTime = qManager.getPool().getSubscriptionMessageTrackingTimeout();
    }

    @Override
    public void run2() {
      SystemFailure.checkFailure();
      if (qManager.getPool().getCancelCriterion().isCancelInProgress()) {
        return;
      }
      if (PoolImpl.BEFORE_SENDING_CLIENT_ACK_CALLBACK_FLAG) {
        ClientServerObserver bo = ClientServerObserverHolder.getInstance();
        bo.beforeSendingClientAck();
      }
      sendPeriodicAck();
      checkForExpiry();
    }

    void checkForExpiry() {
      synchronized (threadIdToSequenceId) {
        Iterator iterator = threadIdToSequenceId.entrySet().iterator();
        long currentTime = System.currentTimeMillis();
        Map.Entry entry;
        SequenceIdAndExpirationObject seo;

        while (iterator.hasNext()) {
          entry = (Map.Entry) iterator.next();
          seo = (SequenceIdAndExpirationObject) entry.getValue();
          if ((currentTime - seo.getCreationTime() > expiryTime)) {
            if (seo.getAckSend() || (qManager.getPool().getSubscriptionRedundancy() == 0
                && !qManager.getPool().isDurableClient())) {
              iterator.remove();
            }
          } else {
            break;
          }
        }
      }
    }

    /**
     * Sends Periodic ack to the primary server for all threadIds for which it has not send an ack.
     */
    void sendPeriodicAck() {
      List events = new ArrayList();
      boolean success = false;
      synchronized (threadIdToSequenceId) {
        for (final Object o : threadIdToSequenceId.entrySet()) {
          Map.Entry entry = (Map.Entry) o;
          SequenceIdAndExpirationObject seo = (SequenceIdAndExpirationObject) entry.getValue();
          if (!seo.getAckSend()) {
            ThreadIdentifier tid = (ThreadIdentifier) entry.getKey();
            events.add(new EventID(tid.getMembershipID(), tid.getThreadID(), seo.getSequenceId()));
            seo.setAckSend(true);
          } // if ends
        } // while ends
      } // synchronized ends

      if (events.size() > 0) {
        try {
          PrimaryAckOp.execute(qManager.getAllConnections().getPrimary(), qManager.getPool(),
              events);
          success = true;
        } catch (Exception ex) {
          if (logger.isDebugEnabled()) {
            logger.debug("Exception while sending an ack to the primary server: {}", ex);
          }
        } finally {
          if (!success) {
            for (final Object event : events) {
              EventID eid = (EventID) event;
              ThreadIdentifier tid = new ThreadIdentifier(eid.getMembershipID(), eid.getThreadID());
              synchronized (threadIdToSequenceId) {
                SequenceIdAndExpirationObject seo =
                    (SequenceIdAndExpirationObject) threadIdToSequenceId.get(tid);
                if (seo != null && seo.getAckSend()) {
                  seo = (SequenceIdAndExpirationObject) threadIdToSequenceId.remove(tid);
                  if (seo != null) {
                    // put back the old seqId with a new time stamp
                    SequenceIdAndExpirationObject siaeo = new SequenceIdAndExpirationObject(
                        seo.getSequenceId(), seo.getPutAllSequenceId());
                    threadIdToSequenceId.put(tid, siaeo);
                  }
                } // if ends
              } // synchronized ends
            } // while ends
          } // if(!success) ends
        } // finally ends
      } // if(events.size() > 0)ends
    }// method ends
  }

  /**
   * A class to store sequenceId and the creation time of the object to be used for expiring the
   * entry
   *
   * @since GemFire 5.1
   *
   */
  public static class SequenceIdAndExpirationObject {
    /** The sequence Id of the entry * */
    private final long sequenceId;
    /** The sequence Id of the putAll operations * */
    private final long putAllSequenceId;
    /** The time of creation of the object* */
    private final long creationTime;
    /** Client ack is send to server or not* */
    private boolean ackSend;

    SequenceIdAndExpirationObject(long sequenceId, long putAllSequenceId) {
      this.sequenceId = sequenceId;
      this.putAllSequenceId = putAllSequenceId;
      creationTime = System.currentTimeMillis();
      ackSend = false;
    }

    /**
     * @return Returns the creationTime.
     */
    public long getCreationTime() {
      return creationTime;
    }

    /**
     * @return Returns the sequenceId.
     */
    public long getSequenceId() {
      return sequenceId;
    }

    /**
     * @return Returns the putAllSequenceId.
     */
    public long getPutAllSequenceId() {
      return putAllSequenceId;
    }

    /**
     *
     * @return Returns the ackSend
     */
    public boolean getAckSend() {
      return ackSend;
    }

    /**
     * Sets the ackSend
     *
     */
    public void setAckSend(boolean ackSend) {
      this.ackSend = ackSend;
    }

    @Override
    public String toString() {
      return "SequenceIdAndExpirationObject["
          + "ackSend = " + ackSend
          + "; creation = " + creationTime
          + "; seq = " + sequenceId
          + "; putAll seq = " + putAllSequenceId
          + "]";
    }
  }

}
