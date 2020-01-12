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

import static org.apache.geode.logging.internal.LoggingProviderLoader.LOGGING_PROVIDER_NAME_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;

import org.apache.geode.logging.internal.LoggingProviderLoader;
import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Unit tests for {@link LoggingProviderLoader}.
 */
@Category(LoggingTest.class)
public class LoggingProviderLoaderTest {

  private LoggingProviderLoader loggingProviderLoader;

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Before
  public void setUp() {
    loggingProviderLoader = new LoggingProviderLoader();
  }

  @Test
  public void createProviderAgent_defaultsTo_Log4jAgent() {
    assertThat(loggingProviderLoader.load()).isInstanceOf(Log4jLoggingProvider.class);
  }

  @Test
  public void createProviderAgent_usesSystemPropertySetTo_Log4jAgent() {
    System.setProperty(LOGGING_PROVIDER_NAME_PROPERTY, Log4jLoggingProvider.class.getName());

    assertThat(loggingProviderLoader.load()).isInstanceOf(Log4jLoggingProvider.class);
  }

  @Test
  public void findProviderAgent_defaultsTo_createProviderAgent() {
    assertThat(loggingProviderLoader.load()).isInstanceOf(Log4jLoggingProvider.class);
  }
}
