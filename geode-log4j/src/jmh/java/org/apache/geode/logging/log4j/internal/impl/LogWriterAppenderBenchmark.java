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
package org.apache.geode.logging.log4j.internal.impl;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.geode.logging.log4j.internal.impl.Log4jLoggingProvider.GEODE_CONSOLE_APPENDER_NAME;
import static org.apache.geode.logging.log4j.internal.impl.Log4jLoggingProvider.LOGWRITER_APPENDER_NAME;
import static org.apache.logging.log4j.Level.INFO;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.logging.internal.spi.LogConfig;
import org.apache.geode.logging.internal.spi.LogConfigSupplier;
import org.apache.geode.logging.internal.spi.SessionContext;
import org.apache.geode.test.assertj.LogFileAssert;
import org.apache.geode.test.junit.rules.accessible.AccessibleTemporaryFolder;

@Measurement(iterations = 1, time = 1, timeUnit = MINUTES)
@Warmup(iterations = 1, time = 1, timeUnit = MINUTES)
@Fork(1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(SECONDS)
@State(Scope.Benchmark)
@SuppressWarnings("unused")
public class LogWriterAppenderBenchmark {

  private Logger logger;
  private String message;

  private AccessibleTemporaryFolder temporaryFolder;
  private LogWriterAppender logWriterAppender;

  @Setup(Level.Trial)
  public void setUp() throws Throwable {
    temporaryFolder = new AccessibleTemporaryFolder();
    temporaryFolder.before();

    String name = getClass().getSimpleName();
    File logFile = new File(temporaryFolder.getRoot(), name + ".log");

    LogConfig config = mock(LogConfig.class);
    when(config.getName()).thenReturn(name);
    when(config.getLogFile()).thenReturn(logFile);

    LogConfigSupplier logConfigSupplier = mock(LogConfigSupplier.class);
    when(logConfigSupplier.getLogConfig()).thenReturn(config);

    SessionContext sessionContext = mock(SessionContext.class);
    when(sessionContext.getLogConfigSupplier()).thenReturn(logConfigSupplier);

    org.apache.logging.log4j.core.Logger coreLogger =
        (org.apache.logging.log4j.core.Logger) LogManager.getRootLogger();
    LoggerContext context = coreLogger.getContext();
    Configuration configuration = context.getConfiguration();

    LoggerConfig loggerConfig = configuration.getLoggerConfig(coreLogger.getName());
    loggerConfig.removeAppender(GEODE_CONSOLE_APPENDER_NAME);
    context.updateLoggers();

    assertThat(logFile).doesNotExist();
    logWriterAppender =
        (LogWriterAppender) configuration.getAppenders().get(LOGWRITER_APPENDER_NAME);
    logWriterAppender.createSession(sessionContext);
    logWriterAppender.startSession();
    assertThat(logFile).exists();

    logger = LogService.getLogger();
    assertThat(logger.getLevel()).isEqualTo(INFO);

    message = "message";

    LogFileAssert.assertThat(logFile).doesNotContain(message);
    logger.info(message);
    LogFileAssert.assertThat(logFile).contains(message);
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    logWriterAppender.stopSession();
    temporaryFolder.after();
  }

  @Benchmark
  public void infoLogStatement() {
    logger.info(message);
  }
}
