/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.client.internal;

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

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.client.internal.PoolImpl.PoolTask;
import com.gemstone.gemfire.internal.cache.ClientServerObserver;
import com.gemstone.gemfire.internal.cache.ClientServerObserverHolder;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.ha.ThreadIdentifier;
import com.gemstone.gemfire.internal.logging.LogService;

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
    this.qManager = qm;
  }

  public void processMarker() {
    if (!this.processedMarker) {     
      handleMarker();
      this.processedMarker = true;
    } else {
      if (logger.isDebugEnabled()) {
        logger.debug("{}: extra marker received", this);
      }
    }
  }

  public boolean getProcessedMarker() {
    return this.processedMarker;
  }

  public void handleMarker() {
    ArrayList regions = new ArrayList();
    Cache cache = GemFireCacheImpl.getInstance();
    if (cache == null) {
      return;
    }

    Set rootRegions = cache.rootRegions();

    
    for (Iterator iter1 = rootRegions.iterator(); iter1.hasNext();) {
      Region rootRegion = (Region) iter1.next();
      regions.add(rootRegion);
      try {
        Set subRegions = rootRegion.subregions(true); // throws RDE
        for (Iterator iter2 = subRegions.iterator(); iter2.hasNext();) {
          regions.add(iter2.next());
        }
      } catch (RegionDestroyedException e) {
        continue; // region is gone go to the next one bug 38705
      }
    }

    for (Iterator iter = regions.iterator(); iter.hasNext();) {
      LocalRegion region = (LocalRegion) iter.next();
      try {
        if (region.getAttributes().getPoolName()!=null && region.getAttributes().getPoolName().equals(qManager.getPool().getName())) {
          region.handleMarker(); // can this throw RDE??
        }
      }
      catch (RegionDestroyedException e) {
        continue; // region is gone go to the next one bug 38705
      }
    }
  }

  public void incrementInvalidatedStats() {
    this.invalidateCount.incrementAndGet();

  }
  public int getInvalidateCount() {
    return this.invalidateCount.get();
  }
  
  /** test hook - access to this map should be synchronized on the
   * map to avoid concurrent modification exceptions
   */
  public Map getThreadIdToSequenceIdMap() {
    return this.threadIdToSequenceId;
  }
  
  public boolean verifyIfDuplicate(EventID eid) {
    return verifyIfDuplicate(eid, true);  
  }
  
  public boolean verifyIfDuplicate(EventID eid, boolean addToMap) {
    ThreadIdentifier tid = new ThreadIdentifier(eid.getMembershipID(), eid
        .getThreadID());
    long seqId = eid.getSequenceID();
    SequenceIdAndExpirationObject seo = null;

    // Fix 36930: save the max sequence id for each non-putAll operation's thread
    // There're totally 3 cases to consider:
    // check the tid:
    // 1) if duplicated, (both putall or non-putall): reject
    // 2) if not duplicate
    //    2.1)if putAll, check via real thread id again, 
    //        if duplicate, reject (because one non-putall operation with bigger 
    //        seqno has happened) 
    //        otherwise save the putAllSeqno for real thread id 
    //        and save seqno for tid
    //    2.2) if not putAll, 
    //        check putAllSequenceId with real thread id
    //        if request's seqno is smaller, reject (because one putAll operation
    //         with bigger seqno has happened)
    //        otherwise, update the seqno for tid
    // lock taken to avoid concurrentModification
    // while the objects are being expired
    synchronized (this.threadIdToSequenceId) {
      seo = (SequenceIdAndExpirationObject) this.threadIdToSequenceId.get(tid);
      if (seo != null && seo.getSequenceId() >= seqId) {
        if (logger.isDebugEnabled()) {
          logger.debug(" got a duplicate entry with EventId {}. Ignoring the entry", eid);
        }
        seo.setAckSend(false); // bug #41289: send ack to this server since it's sending old events
        // this.threadIdToSequenceId.put(tid, new SequenceIdAndExpirationObject(
        // seo.getSequenceId()));
        return true;
      }
      else if (addToMap) {
        ThreadIdentifier real_tid = new ThreadIdentifier(eid.getMembershipID(), 
            ThreadIdentifier.getRealThreadIDIncludingWan(eid.getThreadID()));
        if  (ThreadIdentifier.isPutAllFakeThreadID(eid.getThreadID())) {
          // it's a putAll
          seo = (SequenceIdAndExpirationObject) this.threadIdToSequenceId.get(real_tid);
          if (seo != null && seo.getSequenceId() >= seqId) {
            if (logger.isDebugEnabled()) {
              logger.debug("got a duplicate putAll entry with eventId {}. Other operation with same thread id and bigger seqno {} has happened. Ignoring the entry", eid, seo.getSequenceId());
            }
            seo.setAckSend(false); // bug #41289: send ack to servers that send old events
            return true;
          }
          else {
            // save the seqno for real thread id into a putAllSequenceId 
            this.threadIdToSequenceId.remove(real_tid);
            this.threadIdToSequenceId.put(real_tid, seo == null? 
                new SequenceIdAndExpirationObject(-1, seqId): 
                new SequenceIdAndExpirationObject(seo.getSequenceId(), seqId));
            // save seqno for tid
            // here tid!=real_tid, for fake tid, putAllSeqno should be 0
            this.threadIdToSequenceId.remove(tid);
            this.threadIdToSequenceId.put(tid, new SequenceIdAndExpirationObject(seqId, -1));
          }
        } else {
          // non-putAll operation:
          // check putAllSeqno for real thread id
          // if request's seqno is smaller, reject
          // otherwise, update the seqno for tid
          seo = (SequenceIdAndExpirationObject) this.threadIdToSequenceId.get(real_tid);
          if (seo != null && seo.getPutAllSequenceId() >= seqId) {
            if (logger.isDebugEnabled()) {
              logger.debug("got a duplicate non-putAll entry with eventId {}. One putAll operation with same real thread id and bigger seqno {} has happened. Ignoring the entry", eid, seo.getPutAllSequenceId());
            }
            seo.setAckSend(false); // bug #41289: send ack to servers that send old events
            return true;
          }
          else {
            // here tid==real_tid
            this.threadIdToSequenceId.remove(tid);
            this.threadIdToSequenceId.put(tid, seo == null? 
                new SequenceIdAndExpirationObject(seqId, -1):
                new SequenceIdAndExpirationObject(seqId, seo.getPutAllSequenceId()));
          }
        }
      }
    }
    return false;
  }
  public void start(ScheduledExecutorService timer, int interval) {
    timer.scheduleWithFixedDelay(new ThreadIdToSequenceIdExpiryTask(),
                              interval, interval, TimeUnit.MILLISECONDS);
  }
  
  /**
   * 
   * Thread which will iterate over threadIdToSequenceId map
   * 
   * 1)It will send an ack primary server for all threadIds for which it has not
   * send an ack. 2)It will expire the entries which have exceeded the specified
   * expiry time and for which ack has been alerady sent.
   * 
   * @author darrel
   * @author Mitul Bid
   * @author Suyog Bhokare
   * @since 5.1
   * 
   */

  private class ThreadIdToSequenceIdExpiryTask extends PoolTask {
    /**
     * The expiry time of the entries in the map
     */
    private final long expiryTime;

    /**
     * The peridic ack interval for client
     */
//     private final long ackTime;
//       ackTime = QueueStateImpl.this.qManager.getPool().getQueueAckInterval();

//     /**
//      * boolean to specify if the thread should continue running
//      */
//     private volatile boolean continueRunning = true;

    /**
     * constructs the Thread and initializes the expiry time
     * 
     */
    public ThreadIdToSequenceIdExpiryTask() {
      expiryTime = QueueStateImpl.this.qManager.getPool()
          .getSubscriptionMessageTrackingTimeout();
    }
    
    @Override
    public void run2() {
      SystemFailure.checkFailure();
      if (qManager.getPool().getCancelCriterion().cancelInProgress() != null) {
        return;
      }
      if (PoolImpl.BEFORE_SENDING_CLIENT_ACK_CALLBACK_FLAG) {
        ClientServerObserver bo = ClientServerObserverHolder.getInstance();
        bo.beforeSendingClientAck();
      }
      //if ((qManager.getPool().getSubscriptionRedundancy() != 0) || (qManager.getPool().isDurableClient())) {
        sendPeriodicAck();
      //}
      checkForExpiry();
    }

//     void shutdown() {
//       synchronized (this) {
//         continueRunning = false;
//         this.notify();
//         // Since the wait is timed, it is not necessary to interrupt
//         // the thread; it will wake up of its own accord.
//         // this.interrupt();
//       }
//       try {
//         this.join();
//       } catch (InterruptedException e) {
//         Thread.currentThread().interrupt();
//         // TODO:
//       }
//     }

    void checkForExpiry() {
      synchronized (threadIdToSequenceId) {
        Iterator iterator = threadIdToSequenceId.entrySet().iterator();
        long currentTime = System.currentTimeMillis();
        Map.Entry entry;
        SequenceIdAndExpirationObject seo;

        while (iterator.hasNext()) {
          entry = (Map.Entry) iterator.next();
          seo = (SequenceIdAndExpirationObject) entry.getValue();
          if ((currentTime - seo.getCreationTime() > this.expiryTime)) {
            if (seo.getAckSend()
                || (qManager.getPool().getSubscriptionRedundancy() == 0 && !qManager.getPool().isDurableClient())) {
              iterator.remove();
            }
          } else {
            break;
          }
        }
      }
    }

    /**
     * Sends Periodic ack to the primary server for all threadIds for which it
     * has not send an ack.
     */
    void sendPeriodicAck() {
      List events = new ArrayList();
      boolean success = false;
      synchronized (threadIdToSequenceId) {
        Iterator iterator = threadIdToSequenceId.entrySet().iterator();
        while (iterator.hasNext()) {
          Map.Entry entry = (Map.Entry) iterator.next();
          SequenceIdAndExpirationObject seo = (SequenceIdAndExpirationObject) entry
              .getValue();
          if (!seo.getAckSend()) {
            ThreadIdentifier tid = (ThreadIdentifier) entry.getKey();
            events.add(new EventID(tid.getMembershipID(), tid.getThreadID(),
                seo.getSequenceId()));
            seo.setAckSend(true);
            // entry.setValue(entry);
          }// if ends
        }// while ends
      }// synchronized ends

      if (events.size() > 0) {
        try {
          PrimaryAckOp.execute(qManager.getAllConnections().getPrimary(), qManager.getPool(), events);
          success = true;
        } catch (Exception ex) {
          if (logger.isDebugEnabled())
            logger.debug("Exception while sending an ack to the primary server: {}", ex);
        } finally {
          if (!success) {
            Iterator iter = events.iterator();
            while (iter.hasNext()) {
              EventID eid = (EventID) iter.next();
              ThreadIdentifier tid = new ThreadIdentifier(
                  eid.getMembershipID(), eid.getThreadID());
              synchronized (threadIdToSequenceId) {
                SequenceIdAndExpirationObject seo = (SequenceIdAndExpirationObject) threadIdToSequenceId
                    .get(tid);
                if (seo != null && seo.getAckSend()) {
                  seo = (SequenceIdAndExpirationObject) threadIdToSequenceId
                      .remove(tid);
                  if (seo != null) {
                    // put back the old seqId with a new time stamp
                    SequenceIdAndExpirationObject siaeo = new SequenceIdAndExpirationObject(
                        seo.getSequenceId(), seo.getPutAllSequenceId());
                    threadIdToSequenceId.put(tid, siaeo);
                  }
                }// if ends
              }// synchronized ends
            }// while ends
          }// if(!success) ends
        }// finally ends
      }// if(events.size() > 0)ends
    }// method ends
  }

  /**
   * A class to store sequenceId and the creation time of the object to be used
   * for expiring the entry
   * 
   * @author Mitul Bid
   * @since 5.1
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
      this.creationTime = System.currentTimeMillis();
      this.ackSend = false;
    }

    /**
     * @return Returns the creationTime.
     */
    public final long getCreationTime() {
      return creationTime;
    }

    /**
     * @return Returns the sequenceId.
     */
    public final long getSequenceId() {
      return sequenceId;
    }

    /**
     * @return Returns the putAllSequenceId.
     */
    public final long getPutAllSequenceId() {
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
     * @param ackSend
     */
    public void setAckSend(boolean ackSend) {
      this.ackSend = ackSend;
    }

    @Override
    public String toString() {
      StringBuffer sb = new StringBuffer();
      sb.append("SequenceIdAndExpirationObject[");
      sb.append("ackSend = " + this.ackSend);
      sb.append("; creation = " + creationTime);
      sb.append("; seq = " + sequenceId);
      sb.append("; putAll seq = " + putAllSequenceId);
      sb.append("]");
      return sb.toString();
    }
  }

}
