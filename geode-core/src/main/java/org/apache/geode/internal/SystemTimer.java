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
    StringBuffer sb = new StringBuffer();
    sb.append("SystemTimer[");
    sb.append("swarm = " + swarm);
    sb.append("]");
    return sb.toString();
  }

  /**
   * List of all of the swarms in the system
   */
  @MakeNotStatic
  private static final HashMap<DistributedSystem, List> allSwarms = new HashMap();

  /**
   * Add the given timer is in the given swarm. Used only by constructors.
   *
   * @param swarm swarm to add the timer to
   * @param t timer to add
   */
  private static void addToSwarm(DistributedSystem swarm, SystemTimer t) {
    // Get or add list of timers for this swarm...
    ArrayList /* ArrayList<WeakReference<SystemTimer>> */ swarmSet;
    synchronized (allSwarms) {
      swarmSet = (ArrayList) allSwarms.get(swarm);
      if (swarmSet == null) {
        swarmSet = new ArrayList();
        allSwarms.put(swarm, swarmSet);
      }
    } // synchronized

    // Add the timer to the swarm's list
    WeakReference /* WeakReference<SystemTimer> */ wr = new WeakReference(t);
    synchronized (swarmSet) {
      swarmSet.add(wr);
    } // synchronized
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
      Iterator it = allSwarms.entrySet().iterator();
      while (it.hasNext()) { // iterate over allSwarms
        Map.Entry entry = (Map.Entry) it.next();
        ArrayList swarm = (ArrayList) entry.getValue();
        synchronized (swarm) {
          Iterator it2 = swarm.iterator();
          while (it2.hasNext()) { // iterate over current swarm
            WeakReference wr = (WeakReference) it2.next();
            SystemTimer st = (SystemTimer) wr.get();
            if (st == null) {
              // Remove stale reference
              it2.remove();
              continue;
            }
            // Get rid of a cancelled timer; it's not interesting.
            if (st.cancelled) {
              it2.remove();
              continue;
            }
          } // iterate over current swarm
          if (swarm.size() == 0) { // Remove unused swarm
            it.remove();
            if (isDebugEnabled) {
              logger.trace("SystemTimer#sweepAllSwarms: removed unused swarm {}", entry.getKey());
            }
          } // Remove unused swarm
        } // synchronized swarm
      } // iterate over allSwarms
    } // synchronized allSwarms

    // Collect time at END of sweep. It means an extra call to the system
    // timer, but makes this potentially less active.
    lastSweepAllTime = System.currentTimeMillis();
  }

  /**
   * Remove given timer from the swarm.
   *
   * @param t timer to remove
   *
   * @see #cancel()
   */
  private static void removeFromSwarm(SystemTimer t) {
    synchronized (allSwarms) {
      // Get timer's swarm
      List swarmSet = (ArrayList) allSwarms.get(t.swarm);
      if (swarmSet == null) {
        return; // already gone
      }

      // Remove timer from swarm
      synchronized (swarmSet) {
        Iterator it = swarmSet.iterator();
        while (it.hasNext()) {
          WeakReference ref = (WeakReference) it.next();
          SystemTimer t2 = (SystemTimer) ref.get();
          if (t2 == null) {
            // Since we've discovered an empty reference, we should remove it.
            it.remove();
            continue;
          }
          if (t2 == t) {
            it.remove();
            // Don't keep sweeping once we've found it; just quit.
            break;
          }
          if (t2.cancelled) {
            // But if we happen to run across a cancelled timer,
            // remove it.
            it.remove();
            continue;
          }
        } // while

        // While we're here, if the swarm has gone to zero size,
        // we should remove it.
        if (swarmSet.size() == 0) {
          allSwarms.remove(t.swarm); // last reference
        }
      } // synchronized swarmSet
    } // synchronized allSwarms

    sweepAllSwarms(); // Occasionally check global list
  }

  /**
   * Cancel all outstanding timers
   *
   * @param swarm the swarm to cancel
   */
  public static void cancelSwarm(DistributedSystem swarm) {
    // Find the swarmSet and remove it
    List<WeakReference> swarmSet;
    synchronized (allSwarms) {
      swarmSet = allSwarms.get(swarm);
      if (swarmSet == null) {
        return; // already cancelled
      }
      // Remove before releasing synchronization, so any fresh timer ends up
      // in a new set with same key
      allSwarms.remove(swarmSet);
    } // synchronized

    // Empty the swarmSet
    synchronized (swarmSet) {
      for (WeakReference wr : swarmSet) {
        SystemTimer st = (SystemTimer) wr.get();
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
