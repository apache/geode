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

import static org.apache.geode.logging.internal.spi.LogWriterLevel.INFO;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.InOrder;
import org.mockito.Mockito;

import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.logging.internal.spi.LogConfig;
import org.apache.geode.logging.internal.spi.LogConfigSupplier;
import org.apache.geode.services.module.impl.ServiceLoaderModuleService;
import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Unit tests for {@link LoggingSession}.
 */
@Category(LoggingTest.class)
public class LoggingSessionTest {

  private LoggingSessionNotifier loggingSessionNotifier;
  private LogConfigSupplier logConfigSupplier;
  private Configuration configuration;

  private LoggingSession loggingSession;

  @Before
  public void setUp() {
    loggingSessionNotifier = spy(new LoggingSessionRegistryProvider());
    logConfigSupplier = spy(LogConfigSupplier.class);
    configuration =
        spy(Configuration.create(new ServiceLoaderModuleService(LogService.getLogger())));
    LogConfig config = mock(LogConfig.class);

    when(logConfigSupplier.getLogConfig()).thenReturn(config);
    when(config.getLogFile()).thenReturn(new File(""));
    when(config.getLogLevel()).thenReturn(INFO.intLevel());
    when(config.getSecurityLogLevel()).thenReturn(INFO.intLevel());

    loggingSession = LoggingSession.create(configuration, loggingSessionNotifier);
  }

  @Test
  public void createUsesLoggingSessionListenersGetByDefault() {
    loggingSession = LoggingSession.create(new ServiceLoaderModuleService(LogService.getLogger()));

    assertThat(loggingSession.getLoggingSessionNotifier())
        .isEqualTo(LoggingSessionRegistryProvider.get());
  }

  @Test
  public void createSessionInitializesConfiguration() {
    loggingSession.createSession(logConfigSupplier);

    verify(configuration).initialize(eq(logConfigSupplier));
  }

  @Test
  public void createSessionInvokesConfigChangedOnConfiguration() {
    loggingSession.createSession(logConfigSupplier);

    verify(configuration).configChanged();
  }

  @Test
  public void createSessionPublishesConfiguration() {
    loggingSession.createSession(logConfigSupplier);
    loggingSession.startSession();

    verify(configuration).initialize(eq(logConfigSupplier));
    verify(configuration).configChanged();
  }

  @Test
  public void createSessionInvokesDisableLoggingToStandardOutputIfLoggingToFileByDefault() {
    loggingSession.createSession(logConfigSupplier);
    loggingSession.startSession();

    verify(configuration).initialize(eq(logConfigSupplier));
    verify(configuration).configChanged();
    verify(configuration).disableLoggingToStandardOutputIfLoggingToFile();
  }

  @Test
  public void createSessionDoesNotDisableLoggingToStandardOutputIfSystemPropertySet() {
    loggingSession.createSession(logConfigSupplier, true, true, true);
    loggingSession.startSession();

    verify(configuration).initialize(eq(logConfigSupplier));
    verify(configuration).configChanged();
    verify(configuration, Mockito.times(0)).disableLoggingToStandardOutputIfLoggingToFile();
  }


  @Test
  public void createSessionPublishesConfigBeforeCreatingLoggingSession() {
    loggingSession.createSession(logConfigSupplier);

    InOrder inOrder = inOrder(configuration, loggingSessionNotifier);
    inOrder.verify(configuration).initialize(eq(logConfigSupplier));
    inOrder.verify(configuration).configChanged();
    inOrder.verify(loggingSessionNotifier).createSession(eq(loggingSession));
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void createSessionChangesStateToCREATED() {
    loggingSession.createSession(logConfigSupplier);

    assertThat(loggingSession.getState()).isSameAs(LoggingSession.State.CREATED);
  }

  @Test
  public void createSessionNotifiesLoggingSessionListeners() {
    loggingSession.createSession(logConfigSupplier);

    verify(loggingSessionNotifier).createSession(eq(loggingSession));
  }

  @Test
  public void createSessionThrowsIfSessionAlreadyCreated() {
    loggingSession.createSession(logConfigSupplier);

    assertThatThrownBy(() -> loggingSession.createSession(logConfigSupplier))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void createSessionThrowsIfSessionAlreadyStarted() {
    loggingSession.createSession(logConfigSupplier);
    loggingSession.startSession();

    assertThatThrownBy(() -> loggingSession.createSession(logConfigSupplier))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void startSessionNotifiesListeners() {
    loggingSession.createSession(logConfigSupplier);

    loggingSession.startSession();

    verify(loggingSessionNotifier).startSession();
  }

  @Test
  public void startSessionThrowsIfSessionNotCreated() {
    assertThatThrownBy(() -> loggingSession.startSession())
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void startSessionThrowsIfSessionStopped() {
    loggingSession.createSession(logConfigSupplier);
    loggingSession.startSession();
    loggingSession.stopSession();

    assertThatThrownBy(() -> loggingSession.startSession())
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void startSessionThrowsIfSessionAlreadyStarted() {
    loggingSession.createSession(logConfigSupplier);
    loggingSession.startSession();

    assertThatThrownBy(() -> loggingSession.startSession())
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void stopSessionNotifiesListeners() {
    loggingSession.createSession(logConfigSupplier);
    loggingSession.startSession();

    loggingSession.stopSession();

    verify(loggingSessionNotifier).stopSession();
  }

  @Test
  public void stopSessionThrowsIfSessionNotCreated() {
    assertThatThrownBy(() -> loggingSession.stopSession())
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void stopSessionThrowsIfSessionNotStarted() {
    loggingSession.createSession(logConfigSupplier);

    assertThatThrownBy(() -> loggingSession.stopSession())
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void stopSessionThrowsIfSessionAlreadyStopped() {
    loggingSession.createSession(logConfigSupplier);
    loggingSession.startSession();
    loggingSession.stopSession();

    assertThatThrownBy(() -> loggingSession.stopSession())
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void shutdownInvokesConfigurationShutdown() {
    loggingSession.shutdown();

    verify(configuration).shutdown();
  }

  @Test
  public void shutdownCleansUpConfiguration() {
    loggingSession.createSession(logConfigSupplier);

    loggingSession.shutdown();

    verify(configuration).shutdown();
  }
}
