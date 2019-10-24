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

import java.util.HashMap;
import java.util.Map;

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

import org.apache.geode.internal.logging.log4j.FastLogger;

@Measurement(iterations = 1, time = 1, timeUnit = MINUTES)
@Warmup(iterations = 1, time = 1, timeUnit = MINUTES)
@Fork(1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(SECONDS)
@State(Scope.Benchmark)
@SuppressWarnings("unused")
public class FastLoggerParameterTypeBenchmark {

  private Logger logger;
  private FastLogger fastLogger;

  private String messageForConcat;
  private String messageForParams;
  private String string1;
  private String string2;
  private Map<String, String> map1;
  private Map<String, String> map2;

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

    messageForConcat = "message: ";
    messageForParams = "message: {}, {}";
    string1 = "string1";
    string2 = "string2";
    map1 = new HashMap<>();
    map2 = new HashMap<>();
    for (int i = 1; i <= 100; i++) {
      map1.put("key" + i, "value" + i);
    }
    for (int i = 1; i <= 100; i++) {
      map2.put("key" + i, "value" + i);
    }
  }

  @Benchmark
  public void logger_infoLogStatement_stringConcat_benchmark_01() {
    logger.info(messageForConcat + string1 + ", " + string2);
  }

  @Benchmark
  public void fastLogger_infoLogStatement_stringConcat_benchmark_01() {
    fastLogger.info(messageForConcat + string1 + ", " + string2);
  }

  @Benchmark
  public void logger_infoLogStatement_complexConcat_benchmark_02() {
    logger.info(messageForConcat + map1 + ", " + map2);
  }

  @Benchmark
  public void fastLogger_infoLogStatement_complexConcat_benchmark_02() {
    fastLogger.info(messageForConcat + map1 + ", " + map2);
  }

  @Benchmark
  public void logger_infoLogStatement_lambda_stringConcat_benchmark_03() {
    logger.info(() -> messageForConcat + string1 + ", " + string2);
  }

  @Benchmark
  public void fastLogger_infoLogStatement_lambda_stringConcat_benchmark_03() {
    fastLogger.info(() -> messageForConcat + string1 + ", " + string2);
  }

  @Benchmark
  public void logger_infoLogStatement_lambda_complexConcat_benchmark_04() {
    logger.info(() -> messageForConcat + map1 + ", " + map2);
  }

  @Benchmark
  public void fastLogger_infoLogStatement_lambda_complexConcat_benchmark_04() {
    fastLogger.info(() -> messageForConcat + map1 + ", " + map2);
  }

  @Benchmark
  public void logger_infoLogStatement_params_benchmark_05() {
    logger.info(messageForParams, string1, string2);
  }

  @Benchmark
  public void fastLogger_infoLogStatement_params_benchmark_05() {
    fastLogger.info(messageForParams, string1, string2);
  }

  @Benchmark
  public void logger_infoLogStatement_complexParams_benchmark_06() {
    logger.info(messageForParams, map1, map2);
  }

  @Benchmark
  public void fastLogger_infoLogStatement_complexParams_benchmark_06() {
    fastLogger.info(messageForParams, map1, map2);
  }

  @Benchmark
  public void logger_infoLogStatement_lambda_params_benchmark_07() {
    logger.info(messageForParams, () -> string1, () -> string2);
  }

  @Benchmark
  public void fastLogger_infoLogStatement_lambda_params_benchmark_07() {
    fastLogger.info(messageForParams, () -> string1, () -> string2);
  }

  @Benchmark
  public void logger_infoLogStatement_lambda_complexParams_benchmark_08() {
    logger.info(messageForParams, () -> map1, () -> map2);
  }

  @Benchmark
  public void fastLogger_infoLogStatement_lambda_complexParams_benchmark_08() {
    fastLogger.info(messageForParams, () -> map1, () -> map2);
  }
}
