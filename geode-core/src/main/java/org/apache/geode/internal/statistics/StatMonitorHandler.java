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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.SynchronousQueue;

import org.apache.logging.log4j.Logger;

import org.apache.geode.SystemFailure;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.logging.internal.executors.LoggingThread;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * @since GemFire 7.0
 */
public class StatMonitorHandler implements SampleHandler {

  private static final Logger logger = LogService.getLogger();

  protected static final String ENABLE_MONITOR_THREAD =
      GeodeGlossary.GEMFIRE_PREFIX + "stats.enableMonitorThread";

  private final boolean enableMonitorThread;

  /** The registered monitors */
  private final Set<StatisticsMonitor> monitors = ConcurrentHashMap.newKeySet();

  /** Protected by synchronization on this handler instance */
  private volatile StatMonitorNotifier notifier;

  /** Constructs a new StatMonitorHandler instance */
  public StatMonitorHandler() {
    enableMonitorThread = Boolean.getBoolean(ENABLE_MONITOR_THREAD);
  }

  /** Adds a monitor which will be notified of samples */
  public boolean addMonitor(StatisticsMonitor monitor) {
    synchronized (this) {
      boolean added = false;
      if (!monitors.contains(monitor)) {
        added = monitors.add(monitor);
      }
      if (!monitors.isEmpty()) {
        startNotifier_IfEnabledAndNotRunning();
      }
      return added;
    }
  }

  /** Removes a monitor that will no longer be used */
  public boolean removeMonitor(StatisticsMonitor monitor) {
    synchronized (this) {
      boolean removed = false;
      if (monitors.contains(monitor)) {
        removed = monitors.remove(monitor);
      }
      if (monitors.isEmpty()) {
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
      if (enableMonitorThread) {
        final StatMonitorNotifier thread = notifier;
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
    for (StatisticsMonitor monitor : monitors) {
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

  /**
   * For testing only
   *
   */
  Set<StatisticsMonitor> getMonitorsSnapshot() {
    return monitors;
  }

  /** For testing only */
  StatMonitorNotifier getStatMonitorNotifier() {
    return notifier;
  }

  private void startNotifier_IfEnabledAndNotRunning() {
    if (enableMonitorThread && notifier == null) {
      notifier = new StatMonitorNotifier();
      notifier.start();
    }
  }

  private void stopNotifier_IfEnabledAndRunning() {
    if (enableMonitorThread && notifier != null) {
      notifier.stop();
      notifier = null;
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
          alive = false;
          if (producer != null) {
            producer.interrupt();
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
            working = alive;
            if (working) {
              waiting = true;
            }
          }
          if (working) {
            try {
              latestTask = task.take(); // blocking
            } finally {
              synchronized (this) {
                waiting = false;
                working = alive;
              }
            }
          }
          if (working && latestTask != null) {
            for (StatisticsMonitor monitor : monitors) {
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
        if (consumer == null) {
          consumer = new LoggingThread(toString(), this);
          alive = true;
          consumer.start();
        }
      }
    }

    void stop() {
      synchronized (this) {
        if (consumer != null) {
          alive = false;
          consumer.interrupt();
          consumer = null;
        }
      }
    }

    void monitor(MonitorTask task) throws InterruptedException {
      boolean isAlive = false;
      synchronized (this) {
        if (alive) {
          isAlive = true;
          producer = Thread.currentThread();
        }
      }
      if (isAlive) {
        try {
          this.task.put(task);
        } catch (InterruptedException e) {
          // fall through and return
        } finally {
          synchronized (this) {
            producer = null;
          }
        }
      }
    }

    boolean isWaiting() {
      synchronized (this) {
        return waiting;
      }
    }

    boolean isAlive() {
      synchronized (this) {
        return alive;
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
      return sampleTimeMillis;
    }

    List<ResourceInstance> getResourceInstances() {
      return resourceInstances;
    }
  }
}
