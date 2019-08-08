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
package org.apache.geode.internal.logging;

import static org.apache.geode.logging.spi.LogWriterLevel.FINE;
import static org.apache.geode.logging.spi.LogWriterLevel.WARNING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.logging.spi.LogConfig;
import org.apache.geode.statistics.StatisticsConfig;

/**
 * Integration tests for {@link ManagerLogWriterFactory}.
 */
public class ManagerLogWriterFactoryIntegrationTest {

  private File mainLogFile;
  private String mainLogFilePath;
  private File securityLogFile;
  private String securityLogFilePath;
  private LogConfig mainLogConfig;
  private LogConfig securityLogConfig;
  private StatisticsConfig statisticsConfig;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() {
    String name = getClass().getSimpleName() + "_" + testName.getMethodName();

    mainLogFile = new File(temporaryFolder.getRoot(), name + "-main.log");
    mainLogFilePath = mainLogFile.getAbsolutePath();

    securityLogFile = new File(temporaryFolder.getRoot(), name + "-security.log");
    securityLogFilePath = securityLogFile.getAbsolutePath();

    mainLogConfig = mock(LogConfig.class);
    securityLogConfig = mock(LogConfig.class);
    statisticsConfig = mock(StatisticsConfig.class);

    when(mainLogConfig.getLogFile()).thenReturn(mainLogFile);
    when(mainLogConfig.getLogLevel()).thenReturn(FINE.intLevel());

    when(securityLogConfig.getLogFile()).thenReturn(mainLogFile);
    when(securityLogConfig.getLogLevel()).thenReturn(FINE.intLevel());
    when(securityLogConfig.getSecurityLogFile()).thenReturn(securityLogFile);
    when(securityLogConfig.getSecurityLogLevel()).thenReturn(WARNING.intLevel());
  }

  @Test
  public void getLogFileReturnsMainLogFileIfSecurityIsFalse() {
    File logFile = new ManagerLogWriterFactory().setSecurity(false).getLogFile(mainLogConfig);

    assertThat(logFile.getAbsolutePath()).isEqualTo(mainLogFilePath);
  }

  @Test
  public void getLogFileReturnsSecurityLogFileIfSecurityIsTrue() {
    File logFile = new ManagerLogWriterFactory().setSecurity(true).getLogFile(securityLogConfig);

    assertThat(logFile.getAbsolutePath()).isEqualTo(securityLogFilePath);
  }

  @Test
  public void getLogLevelReturnsMainLogLevelIfSecurityIsFalse() {
    int level = new ManagerLogWriterFactory().setSecurity(false).getLogLevel(mainLogConfig);

    assertThat(level).isEqualTo(FINE.intLevel());
  }

  @Test
  public void getLogLevelReturnsSecurityLogLevelIfSecurityIsTrue() {
    int level = new ManagerLogWriterFactory().setSecurity(true).getLogLevel(securityLogConfig);

    assertThat(level).isEqualTo(WARNING.intLevel());
  }

  @Test
  public void createReturnsManagerLogWriterIfSecurityIsFalse() {
    ManagerLogWriter logWriter =
        new ManagerLogWriterFactory().setSecurity(false).create(mainLogConfig, statisticsConfig);

    assertThat(logWriter).isNotInstanceOf(SecurityManagerLogWriter.class);
  }

  @Test
  public void createUsesMainLogFileIfSecurityIsFalse() {
    new ManagerLogWriterFactory().setSecurity(false).create(mainLogConfig, statisticsConfig);

    assertThat(mainLogFile).exists();
    assertThat(securityLogFile).doesNotExist();
  }

  @Test
  public void createReturnsSecurityManagerLogWriterIfSecurityIsTrue() {
    ManagerLogWriter logWriter =
        new ManagerLogWriterFactory().setSecurity(true).create(securityLogConfig, statisticsConfig);

    assertThat(logWriter).isInstanceOf(SecurityManagerLogWriter.class);
  }

  @Test
  public void createUsesSecurityLogFileIfSecurityIsTrue() {
    new ManagerLogWriterFactory().setSecurity(true).create(securityLogConfig, statisticsConfig);

    assertThat(mainLogFile).doesNotExist();
    assertThat(securityLogFile).exists();
  }

  @Test
  public void createSetsConfigOnLogWriterIfSecurityIsFalse() {
    ManagerLogWriter logWriter =
        new ManagerLogWriterFactory().setSecurity(false).create(mainLogConfig, statisticsConfig);

    assertThat(logWriter.getConfig()).isNotNull();
    assertThat(logWriter.getConfig().getLogFile().getAbsolutePath()).isSameAs(mainLogFilePath);
  }

  @Test
  public void createSetsConfigWithSecurityLogFileIfSecurityIsTrue() {
    ManagerLogWriter logWriter =
        new ManagerLogWriterFactory().setSecurity(true).create(securityLogConfig, statisticsConfig);

    assertThat(logWriter.getConfig()).isNotNull().isInstanceOf(SecurityLogConfig.class);
    assertThat(logWriter.getConfig().getLogFile().getAbsolutePath()).isSameAs(securityLogFilePath);
  }
}
