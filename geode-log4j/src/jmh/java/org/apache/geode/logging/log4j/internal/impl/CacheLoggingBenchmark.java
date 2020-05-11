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
import static org.apache.geode.cache.RegionShortcut.LOCAL;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.logging.log4j.Level.INFO;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.Properties;

import org.apache.logging.log4j.Logger;
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

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.logging.LogFileAssert;
import org.apache.geode.test.junit.rules.accessible.AccessibleTemporaryFolder;

@Measurement(iterations = 1, time = 1, timeUnit = MINUTES)
@Warmup(iterations = 1, time = 1, timeUnit = MINUTES)
@Fork(1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(SECONDS)
@State(Scope.Benchmark)
@SuppressWarnings("unused")
public class CacheLoggingBenchmark {

  private Logger logger;
  private String message;

  private AccessibleTemporaryFolder temporaryFolder;
  private InternalCache cache;
  private Region<String, String> region;
  private String key;
  private String value;

  @Setup(Level.Trial)
  public void setUp() throws Throwable {
    temporaryFolder = new AccessibleTemporaryFolder();
    temporaryFolder.before();

    String name = getClass().getSimpleName();
    File logFile = new File(temporaryFolder.getRoot(), name + ".log");

    Properties config = new Properties();
    config.setProperty(LOCATORS, "");
    config.setProperty(LOG_LEVEL, "INFO");
    config.setProperty(LOG_FILE, logFile.getAbsolutePath());

    assertThat(logFile).doesNotExist();
    cache = (InternalCache) new CacheFactory(config).create();
    assertThat(logFile).exists();

    region = cache.<String, String>createRegionFactory(LOCAL).create(name + "-region");

    logger = LogService.getLogger();
    assertThat(logger.getLevel()).isEqualTo(INFO);

    message = "message";
    key = "key";
    value = "value";

    LogFileAssert.assertThat(logFile).doesNotContain(message);
    logger.info(message);
    LogFileAssert.assertThat(logFile).contains(message);
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    cache.close();
    temporaryFolder.after();
  }

  @Benchmark
  public void infoLogStatement() {
    logger.info(message);
  }

  @Benchmark
  public void putStatement() {
    region.put(key, value);
  }
}
