/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.logging.internal;

import static org.apache.geode.logging.internal.Configuration.LOG_LEVEL_UPDATE_OCCURS_PROPERTY;
import static org.apache.geode.logging.internal.Configuration.LOG_LEVEL_UPDATE_SCOPE_PROPERTY;
import static org.apache.geode.logging.spi.LogWriterLevel.INFO;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;

import org.apache.geode.logging.spi.LogConfig;
import org.apache.geode.logging.spi.LogConfigSupplier;
import org.apache.geode.logging.spi.LogLevelUpdateOccurs;
import org.apache.geode.logging.spi.LogLevelUpdateScope;
import org.apache.geode.logging.spi.LoggingProvider;
import org.apache.geode.test.junit.categories.LoggingTest;

/** Unit tests for {@link org.apache.geode.logging.internal.Configuration}. */
@Category(LoggingTest.class)
public class ConfigurationTest {

  private LoggingProvider loggingProvider;
  private LogConfig logConfig;
  private LogConfigSupplier logConfigSupplier;
  private Configuration configuration;

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Before
  public void setUp() {
    loggingProvider = mock(LoggingProvider.class);
    logConfig = mock(LogConfig.class);
    logConfigSupplier = mockLogConfigSupplier();
    configuration = Configuration.create(loggingProvider);
  }

  @Test
  public void logConfigSupplierIsNullByDefault() {
    assertThat(configuration.getLogConfigSupplier()).isNull();
  }

  @Test
  public void initializeSetsLogConfigSupplier() {
    configuration.initialize(logConfigSupplier);

    assertThat(configuration.getLogConfigSupplier()).isSameAs(logConfigSupplier);
  }

  @Test
  public void initializeAddsSelfAsListenerToLogConfigSupplier() {
    configuration.initialize(logConfigSupplier);

    verify(logConfigSupplier).addLogConfigListener(eq(configuration));
  }

  @Test
  public void initializeReplacesLogConfigSupplier() {
    LogConfigSupplier logConfigSupplier1 = mockLogConfigSupplier();
    LogConfigSupplier logConfigSupplier2 = mockLogConfigSupplier();
    LogConfigSupplier logConfigSupplier3 = mockLogConfigSupplier();

    configuration.initialize(logConfigSupplier1);
    configuration.initialize(logConfigSupplier2);
    configuration.initialize(logConfigSupplier3);

    assertThat(configuration.getLogConfigSupplier()).isSameAs(logConfigSupplier3);
  }

  @Test
  public void initializeAddsSelfAsListenerOnlyOnceToEachLogConfigSupplier() {
    LogConfigSupplier logConfigSupplier1 = mockLogConfigSupplier();
    LogConfigSupplier logConfigSupplier2 = mockLogConfigSupplier();
    LogConfigSupplier logConfigSupplier3 = mockLogConfigSupplier();

    configuration.initialize(logConfigSupplier1);
    configuration.initialize(logConfigSupplier2);
    configuration.initialize(logConfigSupplier3);

    verify(logConfigSupplier1).addLogConfigListener(eq(configuration));
    verify(logConfigSupplier2).addLogConfigListener(eq(configuration));
    verify(logConfigSupplier3).addLogConfigListener(eq(configuration));
  }

  @Test
  public void initializeWithNullThrowsIllegalArgumentException() {
    assertThatThrownBy(() -> configuration.initialize(null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void initializeConfiguresProviderAgent() {
    configuration.initialize(logConfigSupplier);

    verify(loggingProvider).configure(eq(logConfig), isA(LogLevelUpdateOccurs.class), isA(
        LogLevelUpdateScope.class));
  }

  @Test
  public void configChangedWithoutLogConfigSupplierThrowsIllegalStateException() {
    assertThatThrownBy(() -> configuration.configChanged())
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void configChangedReconfiguresProviderAgent() {
    configuration.initialize(logConfigSupplier);

    configuration.configChanged();

    verify(loggingProvider, times(2)).configure(eq(logConfig), isA(LogLevelUpdateOccurs.class),
        isA(LogLevelUpdateScope.class));
  }

  @Test
  public void shutdownRemovesCurrentLogConfigSupplier() {
    configuration.initialize(logConfigSupplier);
    configuration.shutdown();

    assertThat(configuration.getLogConfigSupplier()).isNull();
  }

  @Test
  public void shutdownRemovesSelfAsListenerFromCurrentLogConfigSupplier() {
    configuration.initialize(logConfigSupplier);
    configuration.shutdown();

    verify(logConfigSupplier).removeLogConfigListener(eq(configuration));
  }

  @Test
  public void shutdownDoesNothingWithoutCurrentLogConfigSupplier() {
    configuration.shutdown();

    verifyNoMoreInteractions(logConfigSupplier);
  }

  @Test
  public void getLogLevelUpdateOccurs_defaultsTo_ONLY_WHEN_USING_DEFAULT_CONFIG() {
    assertThat(Configuration.getLogLevelUpdateOccurs())
        .isEqualTo(LogLevelUpdateOccurs.ONLY_WHEN_USING_DEFAULT_CONFIG);
  }

  @Test
  public void getLogLevelUpdateOccurs_usesSystemPropertySetTo_ONLY_WHEN_USING_DEFAULT_CONFIG() {
    System.setProperty(LOG_LEVEL_UPDATE_OCCURS_PROPERTY,
        LogLevelUpdateOccurs.ONLY_WHEN_USING_DEFAULT_CONFIG.name());

    assertThat(Configuration.getLogLevelUpdateOccurs())
        .isEqualTo(LogLevelUpdateOccurs.ONLY_WHEN_USING_DEFAULT_CONFIG);
  }

  @Test
  public void getLogLevelUpdateOccurs_usesSystemPropertySetTo_ALWAYS() {
    System.setProperty(LOG_LEVEL_UPDATE_OCCURS_PROPERTY, LogLevelUpdateOccurs.ALWAYS.name());

    assertThat(Configuration.getLogLevelUpdateOccurs()).isEqualTo(LogLevelUpdateOccurs.ALWAYS);
  }

  @Test
  public void getLogLevelUpdateOccurs_usesSystemPropertySetTo_NEVER() {
    System.setProperty(LOG_LEVEL_UPDATE_OCCURS_PROPERTY, LogLevelUpdateOccurs.NEVER.name());

    assertThat(Configuration.getLogLevelUpdateOccurs()).isEqualTo(LogLevelUpdateOccurs.NEVER);
  }

  @Test
  public void getLogLevelUpdateOccurs_usesSystemPropertySetTo_only_when_using_default_config() {
    System.setProperty(LOG_LEVEL_UPDATE_OCCURS_PROPERTY,
        LogLevelUpdateOccurs.ONLY_WHEN_USING_DEFAULT_CONFIG.name().toLowerCase());

    assertThat(Configuration.getLogLevelUpdateOccurs())
        .isEqualTo(LogLevelUpdateOccurs.ONLY_WHEN_USING_DEFAULT_CONFIG);
  }

  @Test
  public void getLogLevelUpdateOccurs_usesSystemPropertySetTo_always() {
    System.setProperty(LOG_LEVEL_UPDATE_OCCURS_PROPERTY,
        LogLevelUpdateOccurs.ALWAYS.name().toLowerCase());

    assertThat(Configuration.getLogLevelUpdateOccurs()).isEqualTo(LogLevelUpdateOccurs.ALWAYS);
  }

  @Test
  public void getLogLevelUpdateOccurs_usesSystemPropertySetTo_never() {
    System.setProperty(LOG_LEVEL_UPDATE_OCCURS_PROPERTY,
        LogLevelUpdateOccurs.NEVER.name().toLowerCase());

    assertThat(Configuration.getLogLevelUpdateOccurs()).isEqualTo(LogLevelUpdateOccurs.NEVER);
  }

  @Test
  public void getLogLevelUpdateOccurs_usesDefaultWhenSystemPropertySetTo_gibberish() {
    System.setProperty(LOG_LEVEL_UPDATE_OCCURS_PROPERTY, "gibberish");

    assertThat(Configuration.getLogLevelUpdateOccurs())
        .isEqualTo(LogLevelUpdateOccurs.ONLY_WHEN_USING_DEFAULT_CONFIG);
  }

  @Test
  public void getLogLevelUpdateScope_defaultsTo_GEODE_LOGGERS() {
    assertThat(Configuration.getLogLevelUpdateScope()).isEqualTo(LogLevelUpdateScope.GEODE_LOGGERS);
  }

  @Test
  public void getLogLevelUpdateScope_usesSystemPropertySetTo_GEODE_LOGGERS() {
    System.setProperty(LOG_LEVEL_UPDATE_SCOPE_PROPERTY, LogLevelUpdateScope.GEODE_LOGGERS.name());

    assertThat(Configuration.getLogLevelUpdateScope()).isEqualTo(LogLevelUpdateScope.GEODE_LOGGERS);
  }

  @Test
  public void getLogLevelUpdateScope_usesSystemPropertySetTo_GEODE_AND_SECURITY_LOGGERS() {
    System.setProperty(LOG_LEVEL_UPDATE_SCOPE_PROPERTY,
        LogLevelUpdateScope.GEODE_AND_SECURITY_LOGGERS.name());

    assertThat(Configuration.getLogLevelUpdateScope())
        .isEqualTo(LogLevelUpdateScope.GEODE_AND_SECURITY_LOGGERS);
  }

  @Test
  public void getLogLevelUpdateScope_usesSystemPropertySetTo_GEODE_AND_APPLICATION_LOGGERS() {
    System.setProperty(LOG_LEVEL_UPDATE_SCOPE_PROPERTY,
        LogLevelUpdateScope.GEODE_AND_APPLICATION_LOGGERS.name());

    assertThat(Configuration.getLogLevelUpdateScope())
        .isEqualTo(LogLevelUpdateScope.GEODE_AND_APPLICATION_LOGGERS);
  }

  @Test
  public void getLogLevelUpdateScope_usesSystemPropertySetTo_ALL_LOGGERS() {
    System.setProperty(LOG_LEVEL_UPDATE_SCOPE_PROPERTY, LogLevelUpdateScope.ALL_LOGGERS.name());

    assertThat(Configuration.getLogLevelUpdateScope()).isEqualTo(LogLevelUpdateScope.ALL_LOGGERS);
  }

  @Test
  public void getLogLevelUpdateScope_usesSystemPropertySetTo_geode_loggers() {
    System.setProperty(LOG_LEVEL_UPDATE_SCOPE_PROPERTY,
        LogLevelUpdateScope.GEODE_LOGGERS.name().toLowerCase());

    assertThat(Configuration.getLogLevelUpdateScope()).isEqualTo(LogLevelUpdateScope.GEODE_LOGGERS);
  }

  @Test
  public void getLogLevelUpdateScope_usesSystemPropertySetTo_geode_and_security_loggers() {
    System.setProperty(LOG_LEVEL_UPDATE_SCOPE_PROPERTY,
        LogLevelUpdateScope.GEODE_AND_SECURITY_LOGGERS.name().toLowerCase());

    assertThat(Configuration.getLogLevelUpdateScope())
        .isEqualTo(LogLevelUpdateScope.GEODE_AND_SECURITY_LOGGERS);
  }

  @Test
  public void getLogLevelUpdateScope_usesSystemPropertySetTo_geode_and_application_loggers() {
    System.setProperty(LOG_LEVEL_UPDATE_SCOPE_PROPERTY,
        LogLevelUpdateScope.GEODE_AND_APPLICATION_LOGGERS.name().toLowerCase());

    assertThat(Configuration.getLogLevelUpdateScope())
        .isEqualTo(LogLevelUpdateScope.GEODE_AND_APPLICATION_LOGGERS);
  }

  @Test
  public void getLogLevelUpdateScope_usesSystemPropertySetTo_all_loggers() {
    System.setProperty(LOG_LEVEL_UPDATE_SCOPE_PROPERTY,
        LogLevelUpdateScope.ALL_LOGGERS.name().toLowerCase());

    assertThat(Configuration.getLogLevelUpdateScope()).isEqualTo(LogLevelUpdateScope.ALL_LOGGERS);
  }

  @Test
  public void getLogLevelUpdateScope_usesDefaultWhenSystemPropertySetTo_gibberish() {
    System.setProperty(LOG_LEVEL_UPDATE_SCOPE_PROPERTY, "gibberish");

    assertThat(Configuration.getLogLevelUpdateScope()).isEqualTo(LogLevelUpdateScope.GEODE_LOGGERS);
  }

  private LogConfigSupplier mockLogConfigSupplier() {
    LogConfigSupplier logConfigSupplier = mock(LogConfigSupplier.class);

    when(logConfigSupplier.getLogConfig()).thenReturn(logConfig);
    when(logConfig.getLogLevel()).thenReturn(INFO.intLevel());
    when(logConfig.getSecurityLogLevel()).thenReturn(INFO.intLevel());

    return logConfigSupplier;
  }
}
