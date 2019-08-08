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
package org.apache.geode.logging.log4j.internal;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.logging.log4j.Level.INFO;
import static org.assertj.core.api.Assertions.assertThat;

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
import org.openjdk.jmh.annotations.Warmup;

@Measurement(iterations = 1, time = 1, timeUnit = MINUTES)
@Warmup(iterations = 1, time = 1, timeUnit = MINUTES)
@Fork(1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(SECONDS)
@State(Scope.Benchmark)
@SuppressWarnings("unused")
public class FastLoggerDisabledLevelBenchmark {

  private Logger logger;
  private FastLogger fastLogger;

  private String message;

  @Setup(Level.Trial)
  public void setUp() {
    logger = LogManager.getLogger();
    fastLogger = new FastLogger(LogManager.getLogger());

    FastLogger.setDelegating(false);

    org.apache.logging.log4j.core.Logger coreLogger =
        (org.apache.logging.log4j.core.Logger) LogManager.getRootLogger();
    LoggerContext context = coreLogger.getContext();

    Configuration configuration = context.getConfiguration();
    LoggerConfig loggerConfig = configuration.getLoggerConfig(coreLogger.getName());

    loggerConfig.removeAppender("STDOUT");
    loggerConfig.setLevel(INFO);

    context.updateLoggers();

    assertThat(logger.getLevel()).isEqualTo(INFO);

    message = "message";
  }

  @Benchmark
  public void logger_debugLogStatement_isDebugEnabled_benchmark_01() {
    if (logger.isDebugEnabled()) {
      logger.debug(message);
    }
  }

  @Benchmark
  public void fastLogger_debugLogStatement_isDebugEnabled_benchmark_01() {
    if (fastLogger.isDebugEnabled()) {
      fastLogger.debug(message);
    }
  }

  @Benchmark
  public void logger_debugLogStatement_benchmark_02() {
    logger.debug(message);
  }

  @Benchmark
  public void fastLogger_debugLogStatement_benchmark_02() {
    fastLogger.debug(message);
  }

  @Benchmark
  public void logger_debugLogStatement_lambda_benchmark_03() {
    logger.debug(() -> message);
  }

  @Benchmark
  public void fastLogger_debugLogStatement_lambda_benchmark_03() {
    fastLogger.debug(() -> message);
  }
}
