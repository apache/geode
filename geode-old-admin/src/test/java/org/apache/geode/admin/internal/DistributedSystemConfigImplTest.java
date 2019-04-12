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
package org.apache.geode.admin.internal;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.logging.LogConfig;
import org.apache.geode.internal.logging.LogWriterLevel;
import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Unit tests for {@link DistributedSystemConfigImpl}.
 */
@Category(LoggingTest.class)
public class DistributedSystemConfigImplTest {

  private DistributedSystemConfigImpl distributedSystemConfigImpl;

  @Before
  public void setUp() {
    distributedSystemConfigImpl = new DistributedSystemConfigImpl();
  }

  @Test
  public void createLogConfigCreatesLogConfigWithUsableLogLevel() {
    LogConfig logConfig = distributedSystemConfigImpl.createLogConfig();
    int logLevel = logConfig.getLogLevel();

    assertThat(logLevel).isNotEqualTo(0);
    assertThat(LogWriterLevel.find(logLevel)).isInstanceOf(LogWriterLevel.class);
  }

  @Test
  public void createLogConfigCreatesLogConfigWithUsableSecurityLogLevel() {
    LogConfig logConfig = distributedSystemConfigImpl.createLogConfig();
    int securityLogLevel = logConfig.getSecurityLogLevel();

    assertThat(securityLogLevel).isNotEqualTo(0);
    assertThat(LogWriterLevel.find(securityLogLevel)).isInstanceOf(LogWriterLevel.class);
  }
}
