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
package org.apache.geode.distributed.internal;

import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPort;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Path;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

import org.apache.geode.internal.logging.InternalLogWriter;
import org.apache.geode.logging.internal.LoggingSession;

public class InternalLocatorIntegrationTest {

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private int port;
  @Mock
  private LoggingSession loggingSession;
  @Mock
  private File logFile;
  @Mock
  private InternalLogWriter logWriter;
  @Mock
  private InternalLogWriter securityLogWriter;
  private InetAddress bindAddress;
  private String hostnameForClients;
  @Mock
  private Properties distributedSystemProperties;
  @Mock
  private DistributionConfigImpl distributionConfig;
  private Path workingDirectory;
  private InternalLocator internalLocator;

  @Before
  public void setUp() throws IOException {
    port = getRandomAvailableTCPPort();
    hostnameForClients = "";
    bindAddress = null;

    logFile = temporaryFolder.newFile("logfile.log");
    workingDirectory = temporaryFolder.getRoot().toPath();
  }

  @After
  public void tearDown() {
    if (internalLocator != null) {
      internalLocator.stop();
    }
  }

  @Test
  public void constructs() {
    when(distributionConfig.getLogFile()).thenReturn(logFile);

    assertThatCode(() -> {
      internalLocator =
          new InternalLocator(port, loggingSession, logFile, logWriter, securityLogWriter,
              bindAddress, hostnameForClients, distributedSystemProperties, distributionConfig,
              workingDirectory);
    }).doesNotThrowAnyException();
  }

  @Test
  public void startedLocatorIsRunning() throws IOException {
    internalLocator = InternalLocator.startLocator(port, logFile, logWriter,
        securityLogWriter, bindAddress, true,
        distributedSystemProperties, hostnameForClients, workingDirectory);

    assertThat(internalLocator.isStopped()).isFalse();
  }

  @Test
  public void startedLocatorHasLocator() throws IOException {
    internalLocator = InternalLocator.startLocator(port, logFile, logWriter,
        securityLogWriter, bindAddress, true,
        distributedSystemProperties, hostnameForClients, workingDirectory);

    assertThat(InternalLocator.hasLocator()).isTrue();
  }

  @Test
  public void stoppedLocatorIsStopped() throws IOException {
    internalLocator = InternalLocator.startLocator(port, logFile, logWriter,
        securityLogWriter, bindAddress, true,
        distributedSystemProperties, hostnameForClients, workingDirectory);

    internalLocator.stop();

    assertThat(internalLocator.isStopped()).isTrue();
  }

  @Test
  public void stoppedLocatorDoesNotHaveLocator() throws IOException {
    internalLocator = InternalLocator.startLocator(port, logFile, logWriter,
        securityLogWriter, bindAddress, true,
        distributedSystemProperties, hostnameForClients, workingDirectory);

    internalLocator.stop();

    assertThat(InternalLocator.hasLocator()).isFalse();
  }

  @Test
  public void startLocatorFail() throws Exception {
    Properties properties = new Properties();
    // use this property to induce a NPE when calling
    // InternalLocator.startConfigurationPersistenceService
    // so this would demonstrate that we would throw the exception when we encounter an error when
    // calling InternalLocator.startConfigurationPersistenceService
    properties.put("load-cluster-configuration-from-dir", "true");
    assertThatThrownBy(() -> InternalLocator.startLocator(port, logFile, logWriter,
        securityLogWriter, bindAddress, true,
        properties, hostnameForClients, workingDirectory)).isInstanceOf(RuntimeException.class);

    assertThat(InternalLocator.hasLocator()).isFalse();
  }
}
