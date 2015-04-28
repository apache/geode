/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.SystemTimer;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;

/**
 * ExpirationScheduler uses a single instance of java.util.Timer (and
 * therefore a single thread) per VM to schedule and execute region and 
 * entry expiration tasks.
 */

public class ExpirationScheduler
  {
  private static final Logger logger = LogService.getLogger();

  private final SystemTimer timer;
  private final AtomicInteger pendingCancels = new AtomicInteger();
  private static final int MAX_PENDING_CANCELS = Integer.getInteger("gemfire.MAX_PENDING_CANCELS", 10000).intValue();

  public ExpirationScheduler(InternalDistributedSystem ds) {
    this.timer = new SystemTimer(ds, true);
  }
  
  public void forcePurge() {
    pendingCancels.getAndSet(0);
    this.timer.timerPurge();
  }
  /**
   * Called when we have cancelled a scheduled timer task.
   * Do work, if possible to fix bug 37574.
   */
  public void incCancels() {
    int pc = pendingCancels.incrementAndGet();
    if (pc > MAX_PENDING_CANCELS) {
      pc = pendingCancels.getAndSet(0);
      if (pc > MAX_PENDING_CANCELS) {
        this.timer.timerPurge();
//        int purgedCancels = CFactory.timerPurge(this.timer);
        // we could try to do some fancy stuff here but the value
        // of the atomic is just a hint so don't bother adjusting it
//         // take the diff between the number of actual cancels we purged
//         // "purgedCancels" and the number we said we would purge "pc".
//         int diff = purgedCancels - pc;
      } else {
        // some other thread beat us to it so add back in the cancels
        // we just removed by setting it to 0
        pendingCancels.addAndGet(pc);
      }
    }
  }
  
  /** schedules the given expiration task */
  public ExpiryTask addExpiryTask(ExpiryTask task) {
    try {
      if(logger.isTraceEnabled()) {
        logger.trace(LocalizedMessage.create(LocalizedStrings.ExpirationScheduler_SCHEDULING__0__TO_FIRE_IN__1__MS, new Object[] {task, Long.valueOf(task.getExpiryMillis())}));
      }
      // By using getExpirationTime and passing a Date to schedule
      // we get rid of two calls of System.currentTimeMillis().
      // The Date object creation is very simple and has a very short life.
      timer.schedule(task, new Date(task.getExpirationTime()));
    }
    catch (EntryNotFoundException e) {
      // ignore - there are unsynchronized paths that allow an entry to
      // be destroyed out from under us.
      return null;
    }
    catch (IllegalStateException e) {
      // task must have been cancelled by another thread so don't schedule it
      return null;
    }
    return task;
  }

  /** schedules the given entry expiration task */
  public boolean addEntryExpiryTask(EntryExpiryTask task) {
    try {
      if(logger.isTraceEnabled()) {
        logger.trace(LocalizedMessage.create(LocalizedStrings.ExpirationScheduler_SCHEDULING__0__TO_FIRE_IN__1__MS, new Object[] {task, Long.valueOf(task.getExpiryMillis())}));
      }
      // By using getExpirationTime and passing a Date to schedule
      // we get rid of two calls of System.currentTimeMillis().
      // The Date object creation is very simple and has a very short life.
      timer.schedule(task, new Date(task.getExpirationTime()));
    }
    catch (EntryNotFoundException e) {
      // ignore - there are unsynchronized paths that allow an entry to
      // be destroyed out from under us.
      return false;
    }
    catch (IllegalStateException e) {
      // task must have been cancelled by another thread so don't schedule it
      return false;
    }
    return true;
  }

  /** schedule a java.util.TimerTask for execution */
  public void schedule(SystemTimer.SystemTimerTask task, long when) {
    timer.schedule(task, when);
  }

  /** @see java.util.Timer#cancel() */
  public void cancel() {
    timer.cancel();
  }
}
