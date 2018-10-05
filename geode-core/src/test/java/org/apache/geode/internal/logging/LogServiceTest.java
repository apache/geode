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

import static junitparams.JUnitParamsRunner.$;
import static org.assertj.core.api.Assertions.assertThat;

import java.net.URL;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.logging.log4j.Level;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.logging.log4j.AppenderContext;
import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Unit tests for LogService
 */
@RunWith(JUnitParamsRunner.class)
@Category(LoggingTest.class)
public class LogServiceTest {

  private URL defaultConfigUrl;
  private URL cliConfigUrl;

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Before
  public void setUp() {
    defaultConfigUrl = LogService.class.getResource(LogService.DEFAULT_CONFIG);
    cliConfigUrl = LogService.class.getResource(LogService.CLI_CONFIG);
  }

  @Test
  public void getAppenderContextShouldHaveEmptyName() {
    final AppenderContext appenderContext = LogService.getAppenderContext();

    assertThat(appenderContext.getName()).isEmpty();
  }

  @Test
  public void getAppenderContextWithNameShouldHaveName() {
    final String name = "someName";
    final AppenderContext appenderContext = LogService.getAppenderContext(name);

    assertThat(appenderContext.getName()).isEqualTo(name);
  }

  @Test
  @Parameters(method = "getToLevelParameters")
  public void toLevelShouldReturnMatchingLog4jLevel(final int intLevel, final Level level) {
    assertThat(LogService.toLevel(intLevel)).isSameAs(level);
  }

  @Test
  public void cliConfigLoadsAsResource() {
    assertThat(cliConfigUrl).isNotNull();
    assertThat(cliConfigUrl.toString()).contains(LogService.CLI_CONFIG);
  }

  @Test
  public void defaultConfigLoadsAsResource() {
    assertThat(defaultConfigUrl).isNotNull();
    assertThat(defaultConfigUrl.toString()).contains(LogService.DEFAULT_CONFIG);
  }

  @Test
  public void defaultConfigShouldBeLoadableAsResource() {
    final URL configUrlFromLogService = LogService.class.getResource(LogService.DEFAULT_CONFIG);
    final URL configUrlFromClassLoader =
        getClass().getClassLoader().getResource(LogService.DEFAULT_CONFIG.substring(1));
    final URL configUrlFromClassPathLoader =
        ClassPathLoader.getLatest().getResource(LogService.DEFAULT_CONFIG.substring(1));

    assertThat(configUrlFromLogService).isNotNull();
    assertThat(configUrlFromClassLoader).isNotNull();
    assertThat(configUrlFromClassPathLoader).isNotNull();
    assertThat(configUrlFromLogService).isEqualTo(configUrlFromClassLoader)
        .isEqualTo(configUrlFromClassPathLoader);
  }

  @SuppressWarnings("unused")
  private static Object[] getToLevelParameters() {
    return $(new Object[] {0, Level.OFF}, new Object[] {100, Level.FATAL},
        new Object[] {200, Level.ERROR}, new Object[] {300, Level.WARN},
        new Object[] {400, Level.INFO}, new Object[] {500, Level.DEBUG},
        new Object[] {600, Level.TRACE}, new Object[] {Integer.MAX_VALUE, Level.ALL});
  }
}
