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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Path;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.SystemOutRule;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

import org.apache.geode.distributed.Locator;
import org.apache.geode.internal.logging.InternalLogWriter;

public class InternalLocatorIntegrationTest2 {

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public SystemOutRule systemOutRule = new SystemOutRule().enableLog();

  private int port;
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
  private Path workingDirectory;
  private InternalLocator internalLocator;

  @Before
  public void setUp() throws IOException {
    port = 0;
    hostnameForClients = "";
    bindAddress = null;

    logFile = temporaryFolder.newFile("logfile.log");
    workingDirectory = temporaryFolder.getRoot().toPath();

    if (Locator.hasLocator()) {
      Locator.getLocator().stop();
    }
  }

  @After
  public void tearDown() {
    if (internalLocator != null) {
      internalLocator.stop();
    }
  }

  @Test
  public void startedLocatorDoesNotAttemptReconnect() throws IOException, InterruptedException {
    // start a locator that's not part of a cluster
    final boolean joinCluster = false;
    internalLocator = InternalLocator.startLocator(port, logFile, logWriter,
        securityLogWriter, bindAddress, joinCluster,
        distributedSystemProperties, hostnameForClients, workingDirectory);
    port = internalLocator.getPort();
    // the locator shouldn't attempt a reconnect because it's not part of a cluster
    internalLocator.stoppedForReconnect = true;
    assertThat(internalLocator.attemptReconnect()).isFalse();
    String output = systemOutRule.getLog();
    assertThat(output).isNotEmpty();
    assertThat(output).contains(InternalLocator.IGNORING_RECONNECT_REQUEST);
  }

}
