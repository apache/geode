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
package org.apache.geode.internal.statistics;

import java.util.List;
import java.util.concurrent.SynchronousQueue;

import org.apache.logging.log4j.Logger;

import org.apache.geode.SystemFailure;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.concurrent.ConcurrentHashSet;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LogMarker;

/**
 * @since GemFire 7.0
 */
public class StatMonitorHandler implements SampleHandler {

  private static final Logger logger = LogService.getLogger();

  protected static final String ENABLE_MONITOR_THREAD =
      DistributionConfig.GEMFIRE_PREFIX + "stats.enableMonitorThread";

  private final boolean enableMonitorThread;

  /** The registered monitors */
  private final ConcurrentHashSet<StatisticsMonitor> monitors =
      new ConcurrentHashSet<StatisticsMonitor>();

  /** Protected by synchronization on this handler instance */
  private volatile StatMonitorNotifier notifier;

  /** Constructs a new StatMonitorHandler instance */
  public StatMonitorHandler() {
    this.enableMonitorThread = Boolean.getBoolean(ENABLE_MONITOR_THREAD);
  }

  /** Adds a monitor which will be notified of samples */
  public boolean addMonitor(StatisticsMonitor monitor) {
    synchronized (this) {
      boolean added = false;
      if (!this.monitors.contains(monitor)) {
        added = this.monitors.add(monitor);
      }
      if (!this.monitors.isEmpty()) {
        startNotifier_IfEnabledAndNotRunning();
      }
      return added;
    }
  }

  /** Removes a monitor that will no longer be used */
  public boolean removeMonitor(StatisticsMonitor monitor) {
    synchronized (this) {
      boolean removed = false;
      if (this.monitors.contains(monitor)) {
        removed = this.monitors.remove(monitor);
      }
      if (this.monitors.isEmpty()) {
        stopNotifier_IfEnabledAndRunning();
      }
      return removed;
    }
  }

  /**
   * Stops the notifier thread if one exists.
   */
  public void close() {
    synchronized (this) {
      stopNotifier_IfEnabledAndRunning();
    }
  }

  @Override
  public void sampled(long nanosTimeStamp, List<ResourceInstance> resourceInstances) {
    synchronized (this) {
      if (this.enableMonitorThread) {
        final StatMonitorNotifier thread = this.notifier;
        if (thread != null) {
          try {
            thread.monitor(new MonitorTask(System.currentTimeMillis(), resourceInstances));
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
      } else {
        monitor(System.currentTimeMillis(), resourceInstances);
      }
    }
  }

  private void monitor(final long sampleTimeMillis, final List<ResourceInstance> resourceInstance) {
    for (StatisticsMonitor monitor : StatMonitorHandler.this.monitors) {
      try {
        monitor.monitor(sampleTimeMillis, resourceInstance);
      } catch (VirtualMachineError e) {
        SystemFailure.initiateFailure(e);
        throw e;
      } catch (Error e) {
        SystemFailure.checkFailure();
        logger.warn(LogMarker.STATISTICS_MARKER, "StatisticsMonitor {} threw {}", monitor,
            e.getClass().getSimpleName(), e);
      } catch (RuntimeException e) {
        logger.warn(LogMarker.STATISTICS_MARKER, "StatisticsMonitor {} threw {}", monitor,
            e.getClass().getSimpleName(), e);
      }
    }
  }

  @Override
  public void allocatedResourceType(ResourceType resourceType) {}

  @Override
  public void allocatedResourceInstance(ResourceInstance resourceInstance) {}

  @Override
  public void destroyedResourceInstance(ResourceInstance resourceInstance) {}

  /** For testing only */
  ConcurrentHashSet<StatisticsMonitor> getMonitorsSnapshot() {
    return this.monitors;
  }

  /** For testing only */
  StatMonitorNotifier getStatMonitorNotifier() {
    return this.notifier;
  }

  private void startNotifier_IfEnabledAndNotRunning() {
    if (this.enableMonitorThread && this.notifier == null) {
      this.notifier = new StatMonitorNotifier();
      this.notifier.start();
    }
  }

  private void stopNotifier_IfEnabledAndRunning() {
    if (this.enableMonitorThread && this.notifier != null) {
      this.notifier.stop();
      this.notifier = null;
    }
  }

  /**
   * @since GemFire 7.0
   */
  class StatMonitorNotifier implements Runnable {

    /** True while this notifier's thread is running */
    private volatile boolean alive;

    /** Protected by synchronization on this notifier instance */
    private Thread consumer;

    /** Protected by synchronization on this notifier instance */
    private boolean waiting;

    /** Protected by synchronization on this notifier instance */
    private Thread producer;

    /** Used to hand-off from producer to consumer */
    private final SynchronousQueue<MonitorTask> task = new SynchronousQueue<MonitorTask>();

    StatMonitorNotifier() {}

    @Override
    public void run() {
      final boolean isDebugEnabled_STATISTICS = logger.isTraceEnabled(LogMarker.STATISTICS_VERBOSE);
      if (isDebugEnabled_STATISTICS) {
        logger.trace(LogMarker.STATISTICS_VERBOSE, "StatMonitorNotifier is starting {}", this);
      }
      try {
        work();
      } finally {
        synchronized (this) {
          this.alive = false;
          if (this.producer != null) {
            this.producer.interrupt();
          }
        }
      }
      if (isDebugEnabled_STATISTICS) {
        logger.trace(LogMarker.STATISTICS_VERBOSE, "StatMonitorNotifier is stopping {}", this);
      }
    }

    private void work() {
      boolean working = true;
      while (working) {
        try {
          MonitorTask latestTask = null;
          synchronized (this) {
            working = this.alive;
            if (working) {
              this.waiting = true;
            }
          }
          if (working) {
            try {
              latestTask = this.task.take(); // blocking
            } finally {
              synchronized (this) {
                this.waiting = false;
                working = this.alive;
              }
            }
          }
          if (working && latestTask != null) {
            for (StatisticsMonitor monitor : StatMonitorHandler.this.monitors) {
              try {
                monitor.monitor(latestTask.getSampleTimeMillis(),
                    latestTask.getResourceInstances());
              } catch (VirtualMachineError e) {
                SystemFailure.initiateFailure(e);
                throw e;
              } catch (Error e) {
                SystemFailure.checkFailure();
                logger.warn(LogMarker.STATISTICS_MARKER, "StatisticsMonitor {} threw {}", monitor,
                    e.getClass().getSimpleName(), e);
              } catch (RuntimeException e) {
                logger.warn(LogMarker.STATISTICS_MARKER, "StatisticsMonitor {} threw {}", monitor,
                    e.getClass().getSimpleName(), e);
              }
            }
          }
        } catch (InterruptedException e) {
          synchronized (this) {
            working = false;
          }
        }
      }
    }

    void start() {
      synchronized (this) {
        if (this.consumer == null) {
          this.consumer = new Thread(this, toString());
          this.consumer.setDaemon(true);
          this.alive = true;
          this.consumer.start();
        }
      }
    }

    void stop() {
      synchronized (this) {
        if (this.consumer != null) {
          this.alive = false;
          this.consumer.interrupt();
          this.consumer = null;
        }
      }
    }

    void monitor(MonitorTask task) throws InterruptedException {
      boolean isAlive = false;
      synchronized (this) {
        if (this.alive) {
          isAlive = true;
          this.producer = Thread.currentThread();
        }
      }
      if (isAlive) {
        try {
          this.task.put(task);
        } catch (InterruptedException e) {
          // fall through and return
        } finally {
          synchronized (this) {
            this.producer = null;
          }
        }
      }
    }

    boolean isWaiting() {
      synchronized (this) {
        return this.waiting;
      }
    }

    boolean isAlive() {
      synchronized (this) {
        return this.alive;
      }
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder(getClass().getSimpleName());
      sb.append(" Thread").append(" #").append(System.identityHashCode(this));
      return sb.toString();
    }
  }

  /**
   * @since GemFire 7.0
   */
  static class MonitorTask {
    private final long sampleTimeMillis;
    private final List<ResourceInstance> resourceInstances;

    MonitorTask(long sampleTimeMillis, List<ResourceInstance> resourceInstances) {
      this.sampleTimeMillis = sampleTimeMillis;
      this.resourceInstances = resourceInstances;
    }

    long getSampleTimeMillis() {
      return this.sampleTimeMillis;
    }

    List<ResourceInstance> getResourceInstances() {
      return this.resourceInstances;
    }
  }
}
