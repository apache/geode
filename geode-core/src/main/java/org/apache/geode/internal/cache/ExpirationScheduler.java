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
package org.apache.geode.internal.cache;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.SystemTimer;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * ExpirationScheduler uses a single instance of java.util.Timer (and therefore a single thread) per
 * VM to schedule and execute region and entry expiration tasks.
 */

public class ExpirationScheduler {
  private static final Logger logger = LogService.getLogger();

  private final SystemTimer timer;
  private final AtomicInteger pendingCancels = new AtomicInteger();
  private static final int MAX_PENDING_CANCELS = Integer
      .getInteger(GeodeGlossary.GEMFIRE_PREFIX + "MAX_PENDING_CANCELS", 10000).intValue();

  public ExpirationScheduler(InternalDistributedSystem ds) {
    this.timer = new SystemTimer(ds);
  }

  public void forcePurge() {
    pendingCancels.getAndSet(0);
    this.timer.timerPurge();
  }

  /**
   * Called when we have cancelled a scheduled timer task. Do work, if possible to fix bug 37574.
   */
  public void incCancels() {
    int pc = pendingCancels.incrementAndGet();
    if (pc > MAX_PENDING_CANCELS) {
      pc = pendingCancels.getAndSet(0);
      if (pc > MAX_PENDING_CANCELS) {
        this.timer.timerPurge();
        // int purgedCancels = CFactory.timerPurge(this.timer);
        // we could try to do some fancy stuff here but the value
        // of the atomic is just a hint so don't bother adjusting it
        // // take the diff between the number of actual cancels we purged
        // // "purgedCancels" and the number we said we would purge "pc".
        // int diff = purgedCancels - pc;
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
      if (logger.isTraceEnabled()) {
        logger.trace("Scheduling  {}  to fire in  {}  ms",
            new Object[] {task, task.getExpiryMillis()});
      }
      // To fix bug 52267 do not create a Date here; instead calculate the relative duration.
      timer.schedule(task, task.getExpiryMillis());
    } catch (EntryNotFoundException e) {
      // ignore - there are unsynchronized paths that allow an entry to
      // be destroyed out from under us.
      return null;
    } catch (IllegalStateException e) {
      // task must have been cancelled by another thread so don't schedule it
      return null;
    }
    return task;
  }

  /** schedules the given entry expiration task and returns true; returns false if not scheduled */
  public boolean addEntryExpiryTask(EntryExpiryTask task) {
    return addExpiryTask(task) != null;
  }

  /** @see java.util.Timer#cancel() */
  public void cancel() {
    timer.cancel();
  }
}
