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
package org.apache.geode.logging.internal;

import static org.apache.geode.logging.internal.LoggingProviderLoader.LOGGING_PROVIDER_NAME_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;

import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.logging.internal.spi.LogConfig;
import org.apache.geode.logging.internal.spi.LogLevelUpdateOccurs;
import org.apache.geode.logging.internal.spi.LogLevelUpdateScope;
import org.apache.geode.logging.internal.spi.LoggingProvider;
import org.apache.geode.services.module.internal.impl.ServiceLoaderModuleService;
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
    loggingProviderLoader = new LoggingProviderLoader(new ServiceLoaderModuleService(
        LogService.getLogger()));
  }

  @Test
  public void createProviderAgent_usesSystemPropertySetTo_NullProviderAgent() {
    System.setProperty(LOGGING_PROVIDER_NAME_PROPERTY, SimpleLoggingProvider.class.getName());

    LoggingProvider value = loggingProviderLoader.load();

    assertThat(value).isInstanceOf(SimpleLoggingProvider.class);
  }

  @Test
  public void createProviderAgent_usesSystemPropertySetTo_SimpleProviderAgent() {
    System.setProperty(LOGGING_PROVIDER_NAME_PROPERTY, TestLoggingProvider.class.getName());

    LoggingProvider value = loggingProviderLoader.load();

    assertThat(value).isInstanceOf(TestLoggingProvider.class);
  }

  @Test
  public void createProviderAgent_usesNullProviderAgent_whenClassNotFoundException() {
    System.setProperty(LOGGING_PROVIDER_NAME_PROPERTY, TestLoggingProvider.class.getSimpleName());

    LoggingProvider value = loggingProviderLoader.load();

    assertThat(value).isInstanceOf(SimpleLoggingProvider.class);
  }

  @Test
  public void createProviderAgent_usesNullProviderAgent_whenClassCastException() {
    System.setProperty(LOGGING_PROVIDER_NAME_PROPERTY, NotProviderAgent.class.getName());

    LoggingProvider value = loggingProviderLoader.load();

    assertThat(value).isInstanceOf(SimpleLoggingProvider.class);
  }

  @Test
  public void getLoggingProviderReturnsSimpleLoggingProviderByDefault() {
    LoggingProvider loggingProvider =
        new LoggingProviderLoader(new ServiceLoaderModuleService(LogService.getLogger())).load();

    assertThat(loggingProvider).isInstanceOf(SimpleLoggingProvider.class);
  }

  static class TestLoggingProvider implements LoggingProvider {

    @Override
    public void configure(LogConfig logConfig,
        LogLevelUpdateOccurs logLevelUpdateOccurs,
        LogLevelUpdateScope logLevelUpdateScope) {
      // nothing
    }

    @Override
    public void cleanup() {
      // nothing
    }

    @Override
    public int getPriority() {
      return Integer.MAX_VALUE;
    }
  }

  @SuppressWarnings("all")
  static class NotProviderAgent {

  }
}
