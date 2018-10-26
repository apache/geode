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
package org.apache.geode.internal.logging;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.util.StopWatch;

/**
 * Tests performance of logging when level is OFF.
 */
public abstract class LoggingPerformanceTestCase {

  protected static final boolean TIME_BASED = Boolean
      .getBoolean(DistributionConfig.GEMFIRE_PREFIX + "test.LoggingPerformanceTestCase.TIME_BASED");
  protected static final long TIME_TO_RUN = 1000 * 60 * 10; // ten minutes
  protected static final int LOG_SETS = 1000;
  protected static final int LOG_REPETITIONS_PER_SET = 1000;
  protected static final String message = "This is a log message";

  protected File configDirectory = new File(getUniqueName());// null;
  protected File logFile = new File(configDirectory, getUniqueName() + ".log");

  @Rule
  public TestName testName = new TestName();

  @After
  public void tearDownLoggingPerformanceTestCase() throws Exception {
    this.configDirectory = null; // leave this directory in place for now
  }

  protected String getUniqueName() {
    return getClass().getSimpleName() + "_" + testName.getMethodName();
  }

  protected long performLoggingTest(final PerformanceLogger perfLogger) {
    if (TIME_BASED) {
      return performTimeBasedLoggingTest(perfLogger);
    } else {
      return performCountBasedLoggingTest(perfLogger);
    }
  }

  protected long performIsEnabledTest(final PerformanceLogger perfLogger) {
    if (TIME_BASED) {
      return performTimeBasedIsEnabledTest(perfLogger);
    } else {
      return performCountBasedIsEnabledTest(perfLogger);
    }
  }

  protected long performTimeBasedLoggingTest(final PerformanceLogger perfLogger) {
    System.out.println("\nBeginning " + getUniqueName());

    final StopWatch stopWatch = new StopWatch(true);
    long count = 0;
    while (stopWatch.elapsedTimeMillis() < TIME_TO_RUN) {
      perfLogger.log(message);
      count++;
    }
    stopWatch.stop();

    final long millis = stopWatch.elapsedTimeMillis();
    final long seconds = millis / 1000;
    final long minutes = seconds / 60;

    System.out.println(getUniqueName() + " performTimeBasedLoggingTest");
    System.out.println("Number of log statements: " + count);
    System.out.println("Total elapsed time in millis: " + millis);
    System.out.println("Total elapsed time in seconds: " + seconds);
    System.out.println("Total elapsed time in minutes: " + minutes);

    return millis;
  }

  protected long performCountBasedLoggingTest(final PerformanceLogger perfLogger) {
    System.out.println("\nBeginning " + getUniqueName());

    final StopWatch stopWatch = new StopWatch(true);
    for (int sets = 0; sets < LOG_SETS; sets++) {
      for (int count = 0; count < LOG_REPETITIONS_PER_SET; count++) {
        perfLogger.log(message);
      }
    }
    stopWatch.stop();

    final long millis = stopWatch.elapsedTimeMillis();
    final long seconds = millis / 1000;
    final long minutes = seconds / 60;

    System.out.println(getUniqueName() + " performCountBasedLoggingTest");
    System.out.println("Number of log statements: " + LOG_SETS * LOG_REPETITIONS_PER_SET);
    System.out.println("Total elapsed time in millis: " + millis);
    System.out.println("Total elapsed time in seconds: " + seconds);
    System.out.println("Total elapsed time in minutes: " + minutes);

    return millis;
  }

  protected long performTimeBasedIsEnabledTest(final PerformanceLogger perfLogger) {
    System.out.println("\nBeginning " + getUniqueName());

    final StopWatch stopWatch = new StopWatch(true);
    long count = 0;
    while (stopWatch.elapsedTimeMillis() < TIME_TO_RUN) {
      perfLogger.isEnabled();
      count++;
    }
    stopWatch.stop();

    final long millis = stopWatch.elapsedTimeMillis();
    final long seconds = millis / 1000;
    final long minutes = seconds / 60;

    System.out.println(getUniqueName() + " performTimeBasedIsEnabledTest");
    System.out.println("Number of isEnabled statements: " + count);
    System.out.println("Total elapsed time in millis: " + millis);
    System.out.println("Total elapsed time in seconds: " + seconds);
    System.out.println("Total elapsed time in minutes: " + minutes);

    return millis;
  }

  protected long performCountBasedIsEnabledTest(final PerformanceLogger perfLogger) {
    System.out.println("\nBeginning " + getUniqueName());

    final StopWatch stopWatch = new StopWatch(true);
    for (int sets = 0; sets < LOG_SETS; sets++) {
      for (int count = 0; count < LOG_REPETITIONS_PER_SET; count++) {
        perfLogger.isEnabled();
      }
    }
    stopWatch.stop();

    final long millis = stopWatch.elapsedTimeMillis();
    final long seconds = millis / 1000;
    final long minutes = seconds / 60;

    System.out.println(getUniqueName() + " performCountBasedIsEnabledTest");
    System.out.println("Number of isEnabled statements: " + LOG_SETS * LOG_REPETITIONS_PER_SET);
    System.out.println("Total elapsed time in millis: " + millis);
    System.out.println("Total elapsed time in seconds: " + seconds);
    System.out.println("Total elapsed time in minutes: " + minutes);

    return millis;
  }

  protected abstract PerformanceLogger createPerformanceLogger() throws IOException;

  @Test
  public void testCountBasedLogging() throws Exception {
    performCountBasedLoggingTest(createPerformanceLogger());
    assertTrue(this.logFile.exists());
  }

  @Test
  public void testTimeBasedLogging() throws Exception {
    performTimeBasedLoggingTest(createPerformanceLogger());
    assertTrue(this.logFile.exists());
  }

  @Test
  public void testCountBasedIsEnabled() throws Exception {
    performCountBasedIsEnabledTest(createPerformanceLogger());
    assertTrue(this.logFile.exists());
  }

  @Test
  public void testTimeBasedIsEnabled() throws Exception {
    performTimeBasedIsEnabledTest(createPerformanceLogger());
    assertTrue(this.logFile.exists());
  }

  public static interface PerformanceLogger {
    public void log(final String message);

    public boolean isEnabled();
  }
}
