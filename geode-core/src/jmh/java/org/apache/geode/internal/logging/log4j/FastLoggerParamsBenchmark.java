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
package org.apache.geode.internal.logging.log4j;

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

@Measurement(iterations = 1, time = 1, timeUnit = SECONDS)
@Warmup(iterations = 1, time = 1, timeUnit = SECONDS)
@Fork(1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(SECONDS)
@State(Scope.Benchmark)
@SuppressWarnings("unused")
public class FastLoggerParamsBenchmark {

  private Logger logger;
  private FastLogger fastLogger;

  private String messageForConcat;
  private String messageForParams;
  private String string1;
  private String string2;
  private Map<String, String> map1;
  private Map<String, String> map2;

  @Setup(Level.Trial)
  public void setUp() throws Exception {
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
  public void loggerDebugLogStatementWithConcat() {
    logger.debug(messageForConcat + string1 + ", " + string2);
  }

  @Benchmark
  public void fastLoggerDebugLogStatementWithConcat() {
    fastLogger.debug(messageForConcat + string1 + ", " + string2);
  }

  @Benchmark
  public void loggerDebugLogStatementWithComplexConcat() {
    logger.debug(messageForConcat + map1 + ", " + map2);
  }

  @Benchmark
  public void fastLoggerDebugLogStatementWithComplexConcat() {
    fastLogger.debug(messageForConcat + map1 + ", " + map2);
  }

  @Benchmark
  public void loggerIsDebugEnabledLogStatementWithConcat() {
    if (logger.isDebugEnabled()) {
      logger.debug(messageForConcat + string1 + ", " + string2);
    }
  }

  @Benchmark
  public void fastLoggerIsDebugEnabledLogStatementWithConcat() {
    if (fastLogger.isDebugEnabled()) {
      fastLogger.debug(messageForConcat + string1 + ", " + string2);
    }
  }

  @Benchmark
  public void loggerIsDebugEnabledLogStatementWithComplexConcat() {
    if (logger.isDebugEnabled()) {
      logger.debug(messageForConcat + map1 + ", " + map2);
    }
  }

  @Benchmark
  public void fastLoggerIsDebugEnabledLogStatementWithComplexConcat() {
    if (fastLogger.isDebugEnabled()) {
      fastLogger.debug(messageForConcat + map1 + ", " + map2);
    }
  }

  @Benchmark
  public void loggerDebugLambdaLogStatementWithConcat() {
    logger.debug(() -> messageForConcat + string1 + ", " + string2);
  }

  @Benchmark
  public void fastLoggerDebugLambdaLogStatementWithConcat() {
    fastLogger.debug(() -> messageForConcat + string1 + ", " + string2);
  }

  @Benchmark
  public void loggerDebugLambdaLogStatementWithComplexConcat() {
    logger.debug(() -> messageForConcat + map1 + ", " + map2);
  }

  @Benchmark
  public void fastLoggerDebugLambdaLogStatementWithComplexConcat() {
    fastLogger.debug(() -> messageForConcat + map1 + ", " + map2);
  }

  @Benchmark
  public void loggerDebugLogStatementWithParams() {
    logger.debug(messageForParams, string1, string2);
  }

  @Benchmark
  public void fastLoggerDebugLogStatementWithParams() {
    fastLogger.debug(messageForParams, string1, string2);
  }

  @Benchmark
  public void loggerDebugLogStatementWithComplexParams() {
    logger.debug(messageForParams, map1, map2);
  }

  @Benchmark
  public void fastLoggerDebugLogStatementWithComplexParams() {
    fastLogger.debug(messageForParams, map1, map2);
  }

  @Benchmark
  public void loggerDebugLogStatementWithLambdaParams() {
    logger.debug(messageForParams, () -> string1, () -> string2);
  }

  @Benchmark
  public void fastLoggerDebugLogStatementWithLambdaParams() {
    fastLogger.debug(messageForParams, () -> string1, () -> string2);
  }

  @Benchmark
  public void loggerDebugLogStatementWithComplexLambdaParams() {
    logger.debug(messageForParams, () -> map1, () -> map2);
  }

  @Benchmark
  public void fastLoggerDebugLogStatementWithComplexLambdaParams() {
    fastLogger.debug(messageForParams, () -> map1, () -> map2);
  }

  @Benchmark
  public void loggerIsDebugEnabledLogStatementWithParams() {
    if (logger.isDebugEnabled()) {
      logger.debug(messageForParams, string1, string2);
    }
  }

  @Benchmark
  public void fastLoggerIsDebugEnabledLogStatementWithParams() {
    if (fastLogger.isDebugEnabled()) {
      fastLogger.debug(messageForParams, string1, string2);
    }
  }

  @Benchmark
  public void loggerIsDebugEnabledLogStatementWithComplexParams() {
    if (logger.isDebugEnabled()) {
      logger.debug(messageForParams, map1, map2);
    }
  }

  @Benchmark
  public void fastLoggerIsDebugEnabledLogStatementWithComplexParams() {
    if (fastLogger.isDebugEnabled()) {
      fastLogger.debug(messageForParams, map1, map2);
    }
  }

  @Benchmark
  public void loggerIsDebugEnabledLogStatementWithLambdaParams() {
    if (logger.isDebugEnabled()) {
      logger.debug(messageForParams, () -> string1, () -> string2);
    }
  }

  @Benchmark
  public void fastLoggerIsDebugEnabledLogStatementWithLambdaParams() {
    if (fastLogger.isDebugEnabled()) {
      fastLogger.debug(messageForParams, () -> string1, () -> string2);
    }
  }

  @Benchmark
  public void loggerIsDebugEnabledLogStatementWithComplexLambdaParams() {
    if (logger.isDebugEnabled()) {
      logger.debug(messageForParams, () -> map1, () -> map2);
    }
  }

  @Benchmark
  public void fastLoggerIsDebugEnabledLogStatementWithComplexLambdaParams() {
    if (fastLogger.isDebugEnabled()) {
      fastLogger.debug(messageForParams, () -> map1, () -> map2);
    }
  }

  @Benchmark
  public void loggerInfoLogStatementWithConcat() {
    logger.info(messageForConcat + string1 + ", " + string2);
  }

  @Benchmark
  public void fastLoggerInfoLogStatementWithConcat() {
    fastLogger.info(messageForConcat + string1 + ", " + string2);
  }

  @Benchmark
  public void loggerInfoLogStatementWithComplexConcat() {
    logger.info(messageForConcat + map1 + ", " + map2);
  }

  @Benchmark
  public void fastLoggerInfoLogStatementWithComplexConcat() {
    fastLogger.info(messageForConcat + map1 + ", " + map2);
  }

  @Benchmark
  public void loggerInfoLambdaLogStatementWithConcat() {
    logger.info(() -> messageForConcat + string1 + ", " + string2);
  }

  @Benchmark
  public void fastLoggerInfoLambdaLogStatementWithConcat() {
    fastLogger.info(() -> messageForConcat + string1 + ", " + string2);
  }

  @Benchmark
  public void loggerInfoLambdaLogStatementWithComplexConcat() {
    logger.info(() -> messageForConcat + map1 + ", " + map2);
  }

  @Benchmark
  public void fastLoggerInfoLambdaLogStatementWithComplexConcat() {
    fastLogger.info(() -> messageForConcat + map1 + ", " + map2);
  }

  @Benchmark
  public void loggerIsInfoEnabledLogStatementWithConcat() {
    if (logger.isInfoEnabled()) {
      logger.info(messageForConcat + string1 + ", " + string2);
    }
  }

  @Benchmark
  public void fastLoggerIsInfoEnabledLogStatementWithConcat() {
    if (fastLogger.isInfoEnabled()) {
      fastLogger.info(messageForConcat + string1 + ", " + string2);
    }
  }

  @Benchmark
  public void loggerIsInfoEnabledLogStatementWithComplexConcat() {
    if (logger.isInfoEnabled()) {
      logger.info(messageForConcat + map1 + ", " + map2);
    }
  }

  @Benchmark
  public void fastLoggerIsInfoEnabledLogStatementWithComplexConcat() {
    if (fastLogger.isInfoEnabled()) {
      fastLogger.info(messageForConcat + map1 + ", " + map2);
    }
  }

  @Benchmark
  public void loggerIsInfoEnabledLambdaLogStatementWithConcat() {
    if (logger.isInfoEnabled()) {
      logger.info(() -> messageForConcat + string1 + ", " + string2);
    }
  }

  @Benchmark
  public void fastLoggerIsInfoEnabledLambdaLogStatementWithConcat() {
    if (fastLogger.isInfoEnabled()) {
      fastLogger.info(() -> messageForConcat + string1 + ", " + string2);
    }
  }

  @Benchmark
  public void loggerIsInfoEnabledLambdaLogStatementWithComplexConcat() {
    if (logger.isInfoEnabled()) {
      logger.info(() -> messageForConcat + map1 + ", " + map2);
    }
  }

  @Benchmark
  public void fastLoggerIsInfoEnabledLambdaLogStatementWithComplexConcat() {
    if (fastLogger.isInfoEnabled()) {
      fastLogger.info(() -> messageForConcat + map1 + ", " + map2);
    }
  }

  @Benchmark
  public void loggerInfoLogStatementWithParams() {
    logger.info(messageForParams, string1, string2);
  }

  @Benchmark
  public void fastLoggerInfoLogStatementWithParams() {
    fastLogger.info(messageForParams, string1, string2);
  }

  @Benchmark
  public void loggerInfoLogStatementWithComplexParams() {
    logger.info(messageForParams, map1, map2);
  }

  @Benchmark
  public void fastLoggerInfoLogStatementWithComplexParams() {
    fastLogger.info(messageForParams, map1, map2);
  }

  @Benchmark
  public void loggerInfoLogStatementWithLambdaParams() {
    logger.info(messageForParams, () -> string1, () -> string2);
  }

  @Benchmark
  public void fastLoggerInfoLogStatementWithLambdaParams() {
    fastLogger.info(messageForParams, () -> string1, () -> string2);
  }

  @Benchmark
  public void loggerInfoLogStatementWithComplexLambdaParams() {
    logger.info(messageForParams, () -> map1, () -> map2);
  }

  @Benchmark
  public void fastLoggerInfoLogStatementWithComplexLambdaParams() {
    fastLogger.info(messageForParams, () -> map1, () -> map2);
  }

  @Benchmark
  public void loggerIsInfoEnabledLogStatementWithParams() {
    if (logger.isInfoEnabled()) {
      logger.info(messageForParams, string1, string2);
    }
  }

  @Benchmark
  public void fastLoggerIsInfoEnabledLogStatementWithParams() {
    if (fastLogger.isInfoEnabled()) {
      fastLogger.info(messageForParams, string1, string2);
    }
  }

  @Benchmark
  public void loggerIsInfoEnabledLogStatementWithComplexParams() {
    if (logger.isInfoEnabled()) {
      logger.info(messageForParams, map1, map2);
    }
  }

  @Benchmark
  public void fastLoggerIsInfoEnabledLogStatementWithComplexParams() {
    if (fastLogger.isInfoEnabled()) {
      fastLogger.info(messageForParams, map1, map2);
    }
  }

  @Benchmark
  public void loggerIsInfoEnabledLogStatementWithLambdaParams() {
    if (logger.isInfoEnabled()) {
      logger.info(messageForParams, () -> string1, () -> string2);
    }
  }

  @Benchmark
  public void fastLoggerIsInfoEnabledLogStatementWithLambdaParams() {
    if (fastLogger.isInfoEnabled()) {
      fastLogger.info(messageForParams, () -> string1, () -> string2);
    }
  }

  @Benchmark
  public void loggerIsInfoEnabledLogStatementWithComplexLambdaParams() {
    if (logger.isInfoEnabled()) {
      logger.info(messageForParams, () -> map1, () -> map2);
    }
  }

  @Benchmark
  public void fastLoggerIsInfoEnabledLogStatementWithComplexLambdaParams() {
    if (fastLogger.isInfoEnabled()) {
      fastLogger.info(messageForParams, () -> map1, () -> map2);
    }
  }
}
