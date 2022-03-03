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

import java.io.File;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelCriterion;
import org.apache.geode.CancelException;
import org.apache.geode.Statistics;
import org.apache.geode.SystemFailure;
import org.apache.geode.internal.NanoTimer;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.internal.io.MainWithChildrenRollingFileHandler;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.process.UncheckedPidUnavailableException;
import org.apache.geode.internal.statistics.platform.OsStatisticsFactory;
import org.apache.geode.internal.util.concurrent.StoppableCountDownLatch;
import org.apache.geode.logging.internal.executors.LoggingThread;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.logging.internal.spi.LogFile;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * HostStatSampler implements a thread which will monitor, sample, and archive statistics. It only
 * has the common functionality that any sampler needs.
 *
 */
public abstract class HostStatSampler
    implements Runnable, StatisticsSampler, StatArchiveHandlerConfig {

  private static final Logger logger = LogService.getLogger();

  public static final String TEST_FILE_SIZE_LIMIT_IN_KB_PROPERTY =
      GeodeGlossary.GEMFIRE_PREFIX + "stats.test.fileSizeLimitInKB";
  public static final String OS_STATS_DISABLED_PROPERTY = "osStatsDisabled";

  protected static final String INITIALIZATION_TIMEOUT_PROPERTY =
      GeodeGlossary.GEMFIRE_PREFIX + "statSamplerInitializationTimeout";
  protected static final int INITIALIZATION_TIMEOUT_DEFAULT = 30000;
  protected static final long INITIALIZATION_TIMEOUT_MILLIS =
      Long.getLong(INITIALIZATION_TIMEOUT_PROPERTY, INITIALIZATION_TIMEOUT_DEFAULT);

  /**
   * Used to check if the sampler thread wake-up is delayed, and log a warning if it is delayed by
   * longer than the amount of milliseconds specified by this property. The value of 0 disables the
   * check.
   */
  private static final long STAT_SAMPLER_DELAY_THRESHOLD =
      Long.getLong(GeodeGlossary.GEMFIRE_PREFIX + "statSamplerDelayThreshold", 3000);
  private static final long STAT_SAMPLER_DELAY_THRESHOLD_NANOS =
      NanoTimer.millisToNanos(STAT_SAMPLER_DELAY_THRESHOLD);

  private static final int MIN_MS_SLEEP = 1;

  private static final int WAIT_FOR_SLEEP_INTERVAL = 10;

  private Thread statThread = null;

  private volatile boolean stopRequested = false;

  private final boolean osStatsDisabled = Boolean.getBoolean(OS_STATS_DISABLED_PROPERTY);
  private final boolean fileSizeLimitInKB;
  private final StatSamplerStats samplerStats;

  private VMStatsContract vmStats;
  private SampleCollector sampleCollector;

  /**
   * Used to signal thread that are waiting for the stat sampler to be initialized.
   */
  private final StoppableCountDownLatch statSamplerInitializedLatch;

  private final CancelCriterion stopper;
  private final CallbackSampler callbackSampler;
  private final NanoTimer timer;
  private final LogFile logFile;

  protected HostStatSampler(CancelCriterion stopper, StatSamplerStats samplerStats) {
    this(stopper, samplerStats, new NanoTimer());
  }

  protected HostStatSampler(CancelCriterion stopper, StatSamplerStats samplerStats,
      NanoTimer timer) {
    this(stopper, samplerStats, timer, null);
  }

  protected HostStatSampler(CancelCriterion stopper, StatSamplerStats samplerStats,
      LogFile logFile) {
    this(stopper, samplerStats, new NanoTimer(), logFile);
  }

  HostStatSampler(CancelCriterion stopper, StatSamplerStats samplerStats, NanoTimer timer,
      LogFile logFile) {
    this.stopper = stopper;
    statSamplerInitializedLatch = new StoppableCountDownLatch(this.stopper, 1);
    this.samplerStats = samplerStats;
    fileSizeLimitInKB = Boolean.getBoolean(TEST_FILE_SIZE_LIMIT_IN_KB_PROPERTY);
    callbackSampler = new CallbackSampler(stopper, samplerStats);
    this.timer = timer;
    this.logFile = logFile;
  }

  public StatSamplerStats getStatSamplerStats() {
    return samplerStats;
  }

  /**
   * Returns the number of times a statistics resource has been add or deleted.
   */
  @Override
  public int getStatisticsModCount() {
    return getStatisticsManager().getStatListModCount();
  }

  /**
   * Returns an array of all the current statistic resource instances.
   */
  @Override
  public Statistics[] getStatistics() {
    return getStatisticsManager().getStatistics();
  }

  /**
   * Returns the time this sampler's system was started.
   */
  @Override
  public long getSystemStartTime() {
    return getStatisticsManager().getStartTime();
  }

  /**
   * Returns the path to this sampler's system directory; if it has one.
   */
  @Override
  public String getSystemDirectoryPath() {
    try {
      return LocalHostUtil.getLocalHostName();
    } catch (UnknownHostException ignore) {
      return "";
    }
  }

  @Override
  public Optional<LogFile> getLogFile() {
    return Optional.ofNullable(logFile);
  }

  @Override
  public boolean waitForSample(long timeout) throws InterruptedException {
    final long endTime = System.currentTimeMillis() + timeout;
    final int startSampleCount = samplerStats.getSampleCount();
    while (System.currentTimeMillis() < endTime
        && samplerStats.getSampleCount() <= startSampleCount) {
      Thread.sleep(WAIT_FOR_SLEEP_INTERVAL);
    }
    return samplerStats.getSampleCount() > startSampleCount;
  }

  @Override
  public SampleCollector waitForSampleCollector(long timeout) throws InterruptedException {
    final long endTime = System.currentTimeMillis() + timeout;
    while (System.currentTimeMillis() < endTime && sampleCollector == null
        || !sampleCollector.isInitialized()) {
      Thread.sleep(WAIT_FOR_SLEEP_INTERVAL);
    }
    return sampleCollector;
  }

  /**
   * This service's main loop
   */
  @Override
  public void run() {
    final boolean isDebugEnabled_STATISTICS = logger.isTraceEnabled(LogMarker.STATISTICS_VERBOSE);
    if (isDebugEnabled_STATISTICS) {
      logger.trace(LogMarker.STATISTICS_VERBOSE, "HostStatSampler started");
    }
    boolean latchCountedDown = false;
    try {
      initSpecialStats();

      sampleCollector = new SampleCollector(this);
      sampleCollector.initialize(this, NanoTimer.getTime(),
          new MainWithChildrenRollingFileHandler());

      statSamplerInitializedLatch.countDown();
      latchCountedDown = true;

      timer.reset();
      // subtract getNanoRate from lastTS to force a quick initial sample
      long nanosLastTimeStamp = timer.getLastResetTime() - getNanoRate();
      while (!stopRequested()) {
        SystemFailure.checkFailure();
        if (Thread.currentThread().isInterrupted()) {
          break;
        }
        final long nanosBeforeSleep = timer.getLastResetTime();
        final long nanosToDelay = nanosLastTimeStamp + getNanoRate();
        delay(nanosToDelay);
        nanosLastTimeStamp = timer.getLastResetTime();
        if (!stopRequested() && isSamplingEnabled()) {
          final long nanosTimeStamp = timer.getLastResetTime();
          final long nanosElapsedSleeping = nanosTimeStamp - nanosBeforeSleep;
          checkElapsedSleepTime(nanosElapsedSleeping);
          if (stopRequested()) {
            break;
          }
          sampleSpecialStats(false);
          if (stopRequested()) {
            break;
          }
          checkListeners();
          if (stopRequested()) {
            break;
          }

          sampleCollector.sample(nanosTimeStamp);

          final long nanosSpentWorking = timer.reset();
          accountForTimeSpentWorking(nanosSpentWorking, nanosElapsedSleeping);
        } else if (!stopRequested() && !isSamplingEnabled()) {
          sampleSpecialStats(true); // fixes bug 42527
        }
      }
    } catch (InterruptedException | CancelException ex) {
      // Silently exit
    } catch (RuntimeException ex) {
      logger.fatal(LogMarker.STATISTICS_MARKER, ex.getMessage(), ex);
      throw ex;
    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error. We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (Error ex) {
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above). However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      logger.fatal(LogMarker.STATISTICS_MARKER, ex.getMessage(), ex);
      throw ex;
    } finally {
      try {
        closeSpecialStats();
        if (sampleCollector != null) {
          sampleCollector.close();
        }
      } finally {
        if (!latchCountedDown) {
          // Make sure the latch gets counted down since
          // other threads wait for this to indicate that
          // the sampler is initialized.
          statSamplerInitializedLatch.countDown();
        }
      }
      if (isDebugEnabled_STATISTICS) {
        logger.trace(LogMarker.STATISTICS_VERBOSE, "HostStatSampler stopped");
      }
    }
  }

  /**
   * Starts the main thread for this service.
   *
   * @throws IllegalStateException if an instance of the {@link #statThread} is still running from a
   *         previous DistributedSystem.
   */
  public void start() {
    synchronized (HostStatSampler.class) {
      if (statThread != null) {
        try {
          int msToWait = getSampleRate() + 100;
          statThread.join(msToWait);
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
        }
        if (statThread.isAlive()) {
          throw new IllegalStateException(
              "Statistics sampling thread is already running, indicating an incomplete shutdown of a previous cache.");
        }
      }
      callbackSampler.start(getStatisticsManager(), getSampleRate(), TimeUnit.MILLISECONDS);
      statThread = new LoggingThread("StatSampler", this);
      statThread.setPriority(Thread.MAX_PRIORITY);
      statThread.start();
      // fix #46310 (race between management and sampler init) by waiting for init here
      try {
        waitForInitialization(INITIALIZATION_TIMEOUT_MILLIS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Tell this service's main thread to terminate.
   */
  public void stop() {
    stop(true);
  }

  private void stop(boolean interruptIfAlive) {
    synchronized (HostStatSampler.class) {
      callbackSampler.stop();
      if (statThread == null) {
        return;
      }

      stopRequested = true;
      synchronized (this) {
        notifyAll();
      }
      try {
        statThread.join(5000);
      } catch (InterruptedException ignore) {
        // It is important that we shutdown so we'll continue trying for another 2 seconds
        try {
          statThread.join(2000);
        } catch (InterruptedException ignored) {
        } finally {
          Thread.currentThread().interrupt();
        }
      } finally {
        if (statThread.isAlive()) {
          if (interruptIfAlive) {
            // It is still alive so interrupt the thread
            statThread.interrupt();
            stop(false);
          } else {
            logger.warn(LogMarker.STATISTICS_MARKER,
                "HostStatSampler thread could not be stopped during shutdown.");
          }
        } else {
          stopRequested = false;
          statThread = null;
        }
      }
    }
  }

  public boolean isAlive() {
    synchronized (HostStatSampler.class) {
      return statThread != null && statThread.isAlive();
    }
  }

  /**
   * Waits for the special statistics to be initialized. For tests, please use
   * {@link #waitForInitialization(long)} instead.
   *
   * @see #initSpecialStats
   * @since GemFire 3.5
   */
  public void waitForInitialization() throws InterruptedException {
    statSamplerInitializedLatch.await();
  }

  /**
   * Waits for the special statistics to be initialized. This overridden version of
   * {@link #waitForInitialization()} should always be used within tests.
   *
   * @see #initSpecialStats
   * @since GemFire 7.0
   */
  public boolean waitForInitialization(long ms) throws InterruptedException {
    return awaitInitialization(ms, TimeUnit.MILLISECONDS);
  }

  /**
   * Awaits the initialization of special statistics.
   *
   * @see #initSpecialStats
   */
  public boolean awaitInitialization(final long timeout, final TimeUnit unit)
      throws InterruptedException {
    return statSamplerInitializedLatch.await(timeout, unit);
  }

  public void changeArchive(File newFile) {
    sampleCollector.changeArchive(newFile, NanoTimer.getTime());
  }

  /**
   * Returns the <code>VMStatsContract</code> for this VM.
   *
   * @since GemFire 3.5
   */
  public VMStatsContract getVMStats() {
    return vmStats;
  }

  @Override
  public String toString() {
    return getClass().getName() + "@" + System.identityHashCode(this);
  }

  protected abstract void checkListeners();

  /**
   * Gets the sample rate in milliseconds
   */
  protected abstract int getSampleRate();

  /**
   * Returns true if sampling is enabled.
   */
  public abstract boolean isSamplingEnabled();

  /**
   * Returns the statistics manager using this sampler.
   */
  protected abstract StatisticsManager getStatisticsManager();

  protected OsStatisticsFactory getOsStatisticsFactory() {
    return null;
  }

  protected void initProcessStats(long id) {
    // do nothing by default
  }

  protected void sampleProcessStats(boolean prepareOnly) {
    // do nothing by default
  }

  protected void closeProcessStats() {
    // do nothing by default
  }

  protected long getSpecialStatsId() {
    try {
      int pid = getStatisticsManager().getPid();
      if (pid > 0) {
        return pid;
      }
    } catch (UncheckedPidUnavailableException ignored) {
      // ignore and fall through
    }
    return getSystemId();
  }

  protected boolean fileSizeLimitInKB() {
    return fileSizeLimitInKB;
  }

  protected boolean osStatsDisabled() {
    return osStatsDisabled;
  }

  protected boolean stopRequested() {
    return stopper.isCancelInProgress() || stopRequested;
  }

  public SampleCollector getSampleCollector() {
    return sampleCollector;
  }

  /**
   * Initialize any special sampler stats
   */
  private synchronized void initSpecialStats() {
    // add a vm resource
    long id = getSpecialStatsId();
    vmStats = VMStatsContractFactory.create(getStatisticsManager(), id);
    initProcessStats(id);
  }

  /**
   * Closes down anything initialied by initSpecialStats.
   */
  private synchronized void closeSpecialStats() {
    if (vmStats != null) {
      vmStats.close();
    }
    closeProcessStats();
  }

  /**
   * Called when this sampler has spent some time working and wants it to be accounted for.
   */
  private void accountForTimeSpentWorking(long nanosSpentWorking, long nanosSpentSleeping) {
    samplerStats.tookSample(nanosSpentWorking, getStatisticsManager().getStatisticsCount(),
        nanosSpentSleeping);
  }

  /**
   * @param nanosToDelay the timestamp to delay until it is the current time
   */
  private void delay(final long nanosToDelay) throws InterruptedException {
    timer.reset();
    long now = timer.getLastResetTime();
    long remainingNanos = nanosToDelay - now;
    if (remainingNanos <= 0) {
      remainingNanos = NanoTimer.millisToNanos(MIN_MS_SLEEP);
    }
    while (remainingNanos > 0 && !stopRequested()) {
      long ms = NanoTimer.nanosToMillis(remainingNanos);
      if (ms <= 0) {
        Thread.yield();
      } else {
        if (ms > MIN_MS_SLEEP) {
          ms -= MIN_MS_SLEEP;
        }
        synchronized (this) {
          if (stopRequested()) {
            // check stopRequested inside the sync to prevent a race in which the wait misses the
            // stopper's notify.
            return;
          }
          wait(ms); // spurious wakeup ok
        }
      }
      timer.reset();
      now = timer.getLastResetTime();
      remainingNanos = nanosToDelay - now;
    }
  }

  private long getNanoRate() {
    return NanoTimer.millisToNanos(getSampleRate());
  }

  /**
   * Collect samples of any operating system statistics
   *
   * @param prepareOnly set to true if you only want to call prepareForSample
   */
  private void sampleSpecialStats(boolean prepareOnly) {
    List<Statistics> statsList = getStatisticsManager().getStatsList();
    for (Statistics s : statsList) {
      if (stopRequested()) {
        return;
      }
      if (s instanceof StatisticsImpl) {
        ((StatisticsImpl) s).prepareForSample();
      }
    }

    if (!prepareOnly && vmStats != null) {
      if (stopRequested()) {
        return;
      }
      vmStats.refresh();
    }
    sampleProcessStats(prepareOnly);
  }

  /**
   * Check the elapsed sleep time upon wakeup, and log a warning if it is longer than the delay
   * threshold.
   *
   * @param elapsedSleepTime duration of sleep in nanoseconds
   */
  private void checkElapsedSleepTime(long elapsedSleepTime) {
    if (STAT_SAMPLER_DELAY_THRESHOLD > 0) {
      final long wakeupDelay = elapsedSleepTime - getNanoRate();
      if (wakeupDelay > STAT_SAMPLER_DELAY_THRESHOLD_NANOS) {
        samplerStats.incJvmPauses();
        logger.warn(LogMarker.STATISTICS_MARKER,
            "Statistics sampling thread detected a wakeup delay of {} ms, indicating a possible resource issue. Check the GC, memory, and CPU statistics.",
            NanoTimer.nanosToMillis(wakeupDelay));
      }
    }
  }
}
