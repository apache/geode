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
package org.apache.geode.internal.cache.partitioned.colocation;

import static org.apache.geode.internal.cache.partitioned.colocation.SingleThreadColocationLoggerFactory.DEFAULT_LOG_INTERVAL;
import static org.apache.geode.internal.cache.partitioned.colocation.SingleThreadColocationLoggerFactory.LOG_INTERVAL_PROPERTY;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.quality.Strictness.STRICT_STUBS;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import org.apache.geode.internal.cache.PartitionedRegion;

public class SingleThreadColocationLoggerFactoryTest {

  private SingleThreadColocationLogger restartableColocationLogger;
  private SingleThreadColocationLoggerConstructor constructor;
  private PartitionedRegion region;

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(STRICT_STUBS);

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Before
  public void setUp() {
    restartableColocationLogger = mock(SingleThreadColocationLogger.class);
    constructor = mock(SingleThreadColocationLoggerConstructor.class);
    region = mock(PartitionedRegion.class);

    when(constructor.create(any(), anyLong(), anyLong()))
        .thenReturn(restartableColocationLogger);
  }

  @Test
  public void startColocationLoggerStartsNewColocationLogger() {
    ColocationLoggerFactory factory = new SingleThreadColocationLoggerFactory(constructor);

    factory.startColocationLogger(region);

    verify(restartableColocationLogger).start();
  }

  @Test
  public void startColocationLoggerUsesSpecifiedRegion() {
    ColocationLoggerFactory factory = new SingleThreadColocationLoggerFactory(constructor);

    factory.startColocationLogger(region);

    verify(constructor).create(same(region), anyLong(), anyLong());
  }

  @Test
  public void startColocationLoggerUsesHalfDefaultLogIntervalAsDelayMillis() {
    ColocationLoggerFactory factory = new SingleThreadColocationLoggerFactory(constructor);

    factory.startColocationLogger(region);

    long expectedDelayMillis = DEFAULT_LOG_INTERVAL / 2;
    verify(constructor).create(any(), eq(expectedDelayMillis), anyLong());
  }

  @Test
  public void startColocationLoggerUsesDefaultLogIntervalAsIntervalMillis() {
    ColocationLoggerFactory factory = new SingleThreadColocationLoggerFactory(constructor);

    factory.startColocationLogger(region);

    verify(constructor).create(any(), anyLong(), eq(DEFAULT_LOG_INTERVAL));
  }

  @Test
  public void startColocationLoggerOverridesDefaultIntervalMillisIfSystemPropertyIsSet() {
    long logIntervalValue = 40_001;
    System.setProperty(LOG_INTERVAL_PROPERTY, String.valueOf(logIntervalValue));
    ColocationLoggerFactory factory = new SingleThreadColocationLoggerFactory(constructor);

    factory.startColocationLogger(region);

    verify(constructor).create(any(), anyLong(), eq(logIntervalValue));
  }

  @Test
  public void startColocationLoggerOverridesDefaultDelayMillisIfSystemPropertyIsSet() {
    long logIntervalValue = 50_001;
    System.setProperty(LOG_INTERVAL_PROPERTY, String.valueOf(logIntervalValue));
    ColocationLoggerFactory factory = new SingleThreadColocationLoggerFactory(constructor);

    factory.startColocationLogger(region);

    verify(constructor).create(any(), eq(logIntervalValue / 2), anyLong());
  }
}
