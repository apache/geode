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
package org.apache.geode.internal;

import java.lang.ref.WeakReference;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Instances of this class are like {@link Timer}, but are associated with a DistributedSystem,
 * which can be
 * cancelled as a group with {@link #cancelTimers(DistributedSystem)}.
 *
 * @see Timer
 * @see TimerTask
 *
 */
public class SystemTimer {
  private static final Logger logger = LogService.getLogger();

  private static final boolean isIBM =
      "IBM Corporation".equals(System.getProperty("java.vm.vendor"));

  /**
   * the underlying {@link Timer}
   */
  private final Timer timer;

  /**
   * True if this timer has been cancelled
   */
  private volatile boolean cancelled = false;

  /**
   * the DistributedSystem to which this timer belongs
   */
  private final DistributedSystem distributedSystem;

  @Override
  public String toString() {
    return "SystemTimer["
        + "system = " + distributedSystem
        + "]";
  }

  /**
   * Map of all of the timers in the system
   */
  @MakeNotStatic
  private static final HashMap<DistributedSystem, Set<WeakReference<SystemTimer>>> distributedSystemTimers =
      new HashMap<>();

  /**
   * Add the given timer is in the given DistributedSystem. Used only by constructors.
   *
   * @param system DistributedSystem to add the timer to
   * @param systemTimer timer to add
   */
  private static void addTimer(DistributedSystem system, SystemTimer systemTimer) {
    Set<WeakReference<SystemTimer>> timers;
    synchronized (distributedSystemTimers) {
      timers = distributedSystemTimers.get(system);
      if (timers == null) {
        timers = new HashSet<>();
        distributedSystemTimers.put(system, timers);
      }
    }

    WeakReference<SystemTimer> wr = new WeakReference<>(systemTimer);
    synchronized (timers) {
      timers.add(wr);
    }
  }

  /**
   * Return the current number of DistributedSystems with timers
   */
  public static int distributedSystemCount() {
    synchronized (distributedSystemTimers) {
      return distributedSystemTimers.size();
    }
  }

  /**
   * time that the last sweep was done
   *
   * @see #sweepAllTimers
   */
  @MakeNotStatic
  private static long lastSweepAllTime = 0;

  /**
   * Interval, in milliseconds, to sweep all timers, measured from when the last sweep finished
   *
   * @see #sweepAllTimers
   */
  private static final long SWEEP_ALL_INTERVAL = 2 * 60 * 1000; // 2 minutes

  /**
   * Manually garbage collect {@link #distributedSystemTimers}, if it hasn't happened in a while.
   *
   * @see #lastSweepAllTime
   */
  private static void sweepAllTimers() {
    if (System.currentTimeMillis() < lastSweepAllTime + SWEEP_ALL_INTERVAL) {
      // Too soon.
      return;
    }
    final boolean isDebugEnabled = logger.isTraceEnabled();
    synchronized (distributedSystemTimers) {
      Iterator<Map.Entry<DistributedSystem, Set<WeakReference<SystemTimer>>>> allSystemsIterator =
          distributedSystemTimers.entrySet().iterator();
      while (allSystemsIterator.hasNext()) {
        Map.Entry<DistributedSystem, Set<WeakReference<SystemTimer>>> entry =
            allSystemsIterator.next();
        Set<WeakReference<SystemTimer>> timers = entry.getValue();
        synchronized (timers) {
          Iterator<WeakReference<SystemTimer>> timersIterator = timers.iterator();
          while (timersIterator.hasNext()) {
            WeakReference<SystemTimer> wr = timersIterator.next();
            SystemTimer st = wr.get();
            if (st == null || st.isCancelled()) {
              timersIterator.remove();
            }
          }
          if (timers.size() == 0) {
            allSystemsIterator.remove();
          }
        }
      }
    }

    // Collect time at END of sweep. It means an extra call to the system
    // timer, but makes this potentially less active.
    lastSweepAllTime = System.currentTimeMillis();
  }

  /**
   * Remove given timer.
   *
   * @param timerToRemove timer to remove
   *
   * @see #cancel()
   */
  private static void removeTimer(SystemTimer timerToRemove) {
    synchronized (distributedSystemTimers) {
      // Get the timers for the distributed system
      Set<WeakReference<SystemTimer>> timers =
          distributedSystemTimers.get(timerToRemove.distributedSystem);
      if (timers == null) {
        return; // already gone
      }

      synchronized (timers) {
        Iterator<WeakReference<SystemTimer>> timersIterator = timers.iterator();
        while (timersIterator.hasNext()) {
          WeakReference<SystemTimer> ref = timersIterator.next();
          SystemTimer timer = ref.get();
          if (timer == null) {
            timersIterator.remove();
          } else if (timer == timerToRemove) {
            timersIterator.remove();
            break;
          } else if (timer.isCancelled()) {
            timersIterator.remove();
          }
        }
        if (timers.size() == 0) {
          distributedSystemTimers.remove(timerToRemove.distributedSystem); // last reference
        }
      }
    }

    sweepAllTimers(); // Occasionally check global list
  }

  /**
   * Cancel all outstanding timers
   *
   * @param system the DistributedSystem whose timers should be cancelled
   */
  public static void cancelTimers(DistributedSystem system) {
    Set<WeakReference<SystemTimer>> timers;
    synchronized (distributedSystemTimers) {
      timers = distributedSystemTimers.get(system);
      if (timers == null) {
        return; // already cancelled
      }
      // Remove before releasing synchronization, so any fresh timer ends up
      // in a new set with same key
      distributedSystemTimers.remove(system);
    } // synchronized

    // cancel all of the timers
    synchronized (timers) {
      for (WeakReference<SystemTimer> wr : timers) {
        SystemTimer st = wr.get();
        // it.remove(); Not necessary, we're emptying the list...
        if (st != null) {
          st.cancelled = true; // for safety :-)
          st.timer.cancel(); // st.cancel() would just search for it again
        }
      }
    }
  }

  public int timerPurge() {
    // Fix 39585, IBM's java.util.timer's purge() has stack overflow issue
    if (isIBM) {
      return 0;
    }
    return this.timer.purge();
  }

  /**
   * @see Timer#Timer(boolean)
   * @param distributedSystem the DistributedSystem to which this timer belongs
   */
  public SystemTimer(DistributedSystem distributedSystem) {
    this.timer = new Timer(true);
    this.distributedSystem = distributedSystem;
    addTimer(distributedSystem, this);
  }

  private void checkCancelled() throws IllegalStateException {
    if (this.cancelled) {
      throw new IllegalStateException("This timer has been cancelled.");
    }
  }

  /**
   * @see Timer#schedule(TimerTask, long)
   */
  public void schedule(SystemTimerTask task, long delay) {
    checkCancelled();
    timer.schedule(task, delay);
  }

  /**
   * @see Timer#schedule(TimerTask, Date)
   */
  public void schedule(SystemTimerTask task, Date time) {
    checkCancelled();
    timer.schedule(task, time);
  }

  /**
   * @see Timer#scheduleAtFixedRate(TimerTask, long, long)
   */
  public void scheduleAtFixedRate(SystemTimerTask task, long delay, long period) {
    checkCancelled();
    timer.scheduleAtFixedRate(task, delay, period);
  }

  /**
   * @see Timer#schedule(TimerTask, long, long)
   */
  public void schedule(SystemTimerTask task, long delay, long period) {
    checkCancelled();
    timer.schedule(task, delay, period);
  }

  /**
   * @see Timer#cancel()
   */
  public void cancel() {
    this.cancelled = true;
    timer.cancel();
    removeTimer(this);
  }

  /**
   * has this timer been cancelled?
   */
  public boolean isCancelled() {
    return cancelled;
  }

  /**
   * Cover class to track behavior of scheduled tasks
   *
   * @see TimerTask
   */
  public abstract static class SystemTimerTask extends TimerTask {
    protected static final Logger logger = LogService.getLogger();
    private volatile boolean cancelled;

    public boolean isCancelled() {
      return cancelled;
    }

    @Override
    public boolean cancel() {
      cancelled = true;
      return super.cancel();
    }

    /**
     * This is your executed action
     */
    public abstract void run2();

    /**
     * Does debug logging, catches critical errors, then delegates to {@link #run2()}
     */
    @Override
    public void run() {
      try {
        this.run2();
      } catch (CancelException ignore) {
        // ignore: TimerThreads can fire during or near cache closure
      } catch (Throwable t) {
        logger.warn(String.format("Timer task <%s> encountered exception", this), t);
        // Don't rethrow, it will just get eaten and kill the timer
      }
    }
  }

}
