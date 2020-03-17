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
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Instances of this class are like {@link Timer}, but are associated with a "swarm", which can be
 * cancelled as a group with {@link #cancelSwarm(DistributedSystem)}.
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
   * the swarm to which this timer belongs
   */
  private final DistributedSystem swarm;

  @Override
  public String toString() {
    return "SystemTimer["
        + "swarm = " + swarm
        + "]";
  }

  /**
   * List of all of the swarms in the system
   */
  @MakeNotStatic
  private static final HashMap<DistributedSystem, List<WeakReference<SystemTimer>>> allSwarms =
      new HashMap<>();

  /**
   * Add the given timer is in the given swarm. Used only by constructors.
   *
   * @param swarm swarm to add the timer to
   * @param systemTimer timer to add
   */
  private static void addToSwarm(DistributedSystem swarm, SystemTimer systemTimer) {
    // Get or add list of timers for this swarm...
    List<WeakReference<SystemTimer>> swarmSet;
    synchronized (allSwarms) {
      swarmSet = allSwarms.get(swarm);
      if (swarmSet == null) {
        swarmSet = new ArrayList<>();
        allSwarms.put(swarm, swarmSet);
      }
    }

    // Add the timer to the swarm's list
    WeakReference<SystemTimer> wr = new WeakReference<>(systemTimer);
    synchronized (swarmSet) {
      swarmSet.add(wr);
    }
  }

  /**
   * Return the current number of swarms of timers
   */
  public static int swarmCount() {
    synchronized (allSwarms) {
      return allSwarms.size();
    }
  }

  /**
   * time that the last sweep was done
   *
   * @see #sweepAllSwarms
   */
  @MakeNotStatic
  private static long lastSweepAllTime = 0;

  /**
   * Interval, in milliseconds, to sweep all swarms, measured from when the last sweep finished
   *
   * @see #sweepAllSwarms
   */
  private static final long SWEEP_ALL_INTERVAL = 2 * 60 * 1000; // 2 minutes

  /**
   * Manually garbage collect {@link #allSwarms}, if it hasn't happened in a while.
   *
   * @see #lastSweepAllTime
   */
  private static void sweepAllSwarms() {
    if (System.currentTimeMillis() < lastSweepAllTime + SWEEP_ALL_INTERVAL) {
      // Too soon.
      return;
    }
    final boolean isDebugEnabled = logger.isTraceEnabled();
    synchronized (allSwarms) {
      Iterator<Map.Entry<DistributedSystem, List<WeakReference<SystemTimer>>>> allSwarmsIterator =
          allSwarms.entrySet().iterator();
      while (allSwarmsIterator.hasNext()) { // iterate over allSwarms
        Map.Entry<DistributedSystem, List<WeakReference<SystemTimer>>> entry =
            allSwarmsIterator.next();
        List<WeakReference<SystemTimer>> swarm = entry.getValue();
        synchronized (swarm) {
          Iterator<WeakReference<SystemTimer>> swarmIterator = swarm.iterator();
          while (swarmIterator.hasNext()) { // iterate over current swarm
            WeakReference<SystemTimer> wr = swarmIterator.next();
            SystemTimer st = wr.get();
            if (st == null || st.isCancelled()) {
              swarmIterator.remove();
            }
          } // iterate over current swarm
          if (swarm.size() == 0) { // Remove unused swarm
            allSwarmsIterator.remove();
            if (isDebugEnabled) {
              logger.trace("SystemTimer#sweepAllSwarms: removed unused swarm {}", entry.getKey());
            }
          }
        }
      }
    }

    // Collect time at END of sweep. It means an extra call to the system
    // timer, but makes this potentially less active.
    lastSweepAllTime = System.currentTimeMillis();
  }

  /**
   * Remove given timer from the swarm.
   *
   * @param timerToRemove timer to remove
   *
   * @see #cancel()
   */
  private static void removeFromSwarm(SystemTimer timerToRemove) {
    synchronized (allSwarms) {
      // Get timer's swarm
      List<WeakReference<SystemTimer>> swarmSet = allSwarms.get(timerToRemove.swarm);
      if (swarmSet == null) {
        return; // already gone
      }

      // Remove timer from swarm
      synchronized (swarmSet) {
        Iterator<WeakReference<SystemTimer>> swarmIterator = swarmSet.iterator();
        while (swarmIterator.hasNext()) {
          WeakReference<SystemTimer> ref = swarmIterator.next();
          SystemTimer timer = ref.get();
          if (timer == null || timer == timerToRemove) {
            swarmIterator.remove();
            // Don't keep sweeping once we've found it; just quit.
            break;
          }
          if (timer.isCancelled()) {
            swarmIterator.remove();
          }
        }

        // While we're here, if the swarm has gone to zero size,
        // we should remove it.
        if (swarmSet.size() == 0) {
          allSwarms.remove(timerToRemove.swarm); // last reference
        }
      }
    }

    sweepAllSwarms(); // Occasionally check global list
  }

  /**
   * Cancel all outstanding timers
   *
   * @param swarm the swarm to cancel
   */
  public static void cancelSwarm(DistributedSystem swarm) {
    // Find the swarmSet and remove it
    List<WeakReference<SystemTimer>> swarmSet;
    synchronized (allSwarms) {
      swarmSet = allSwarms.get(swarm);
      if (swarmSet == null) {
        return; // already cancelled
      }
      // Remove before releasing synchronization, so any fresh timer ends up
      // in a new set with same key
      allSwarms.remove(swarm);
    } // synchronized

    // Empty the swarmSet
    synchronized (swarmSet) {
      for (WeakReference<SystemTimer> wr : swarmSet) {
        SystemTimer st = wr.get();
        // it.remove(); Not necessary, we're emptying the list...
        if (st != null) {
          st.cancelled = true; // for safety :-)
          st.timer.cancel(); // st.cancel() would just search for it again
        }
      } // while
    } // synchronized
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
   * @param swarm the swarm this timer belongs to, currently must be a DistributedSystem
   */
  public SystemTimer(DistributedSystem swarm) {
    this.timer = new Timer(true);
    this.swarm = swarm;
    addToSwarm(swarm, this);
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
    removeFromSwarm(this);
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
