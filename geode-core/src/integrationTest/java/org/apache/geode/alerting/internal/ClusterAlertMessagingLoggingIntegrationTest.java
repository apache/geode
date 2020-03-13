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
package org.apache.geode.alerting.internal;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.mockito.stubbing.Answer;

import org.apache.geode.alerting.internal.spi.AlertLevel;
import org.apache.geode.alerting.internal.spi.AlertingIOException;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.test.assertj.LogFileAssert;
import org.apache.geode.test.junit.categories.AlertingTest;
import org.apache.geode.test.junit.categories.LoggingTest;

@Category({LoggingTest.class, AlertingTest.class})
public class ClusterAlertMessagingLoggingIntegrationTest {

  private File mainLogFile;
  private InternalDistributedSystem system;

  private AlertListenerMessageFactory alertListenerMessageFactory;
  private ClusterDistributionManager distributionManager;
  private InternalDistributedMember remoteMember;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() {
    String name = testName.getMethodName();
    mainLogFile = new File(temporaryFolder.getRoot(), name + "-main.log");

    Properties config = new Properties();
    config.setProperty(LOCATORS, "");
    config.setProperty(LOG_FILE, mainLogFile.getAbsolutePath());
    config.setProperty(NAME, getClass().getSimpleName() + "_" + testName.getMethodName());

    system = (InternalDistributedSystem) DistributedSystem.connect(config);

    alertListenerMessageFactory = mock(AlertListenerMessageFactory.class);
    distributionManager = mock(ClusterDistributionManager.class);
    remoteMember = mock(InternalDistributedMember.class);
  }

  @After
  public void tearDown() {
    system.disconnect();
  }

  @Test
  public void sendAlertLogsWarning_withoutExceptionStack() {
    String exceptionMessage = "Cannot form connection to alert listener";
    doThrow(new AlertingIOException(new IOException(exceptionMessage)))
        .when(distributionManager).putOutgoing(any());
    ClusterAlertMessaging clusterAlertMessaging = new ClusterAlertMessaging(system,
        distributionManager, alertListenerMessageFactory, currentThreadExecutorService());

    clusterAlertMessaging.sendAlert(remoteMember, AlertLevel.WARNING, Instant.now(), "threadName",
        Thread.currentThread().getId(), "formattedMessage", "stackTrace");

    LogFileAssert.assertThat(mainLogFile)
        .contains(exceptionMessage)
        .doesNotContain(IOException.class.getSimpleName())
        .doesNotContain(AlertingIOException.class.getSimpleName());
  }

  private ExecutorService currentThreadExecutorService() {
    ExecutorService executor = mock(ExecutorService.class);
    when(executor.submit(isA(Runnable.class)))
        .thenAnswer((Answer<Future<?>>) invocation -> {
          Runnable task = invocation.getArgument(0);
          task.run();
          return CompletableFuture.completedFuture(null);
        });
    return executor;
  }
}
