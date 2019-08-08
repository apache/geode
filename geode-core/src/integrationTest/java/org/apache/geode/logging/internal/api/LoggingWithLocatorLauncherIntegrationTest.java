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
package org.apache.geode.logging.internal.api;

import static org.apache.geode.internal.logging.Banner.BannerHeader.displayValues;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.LocatorLauncher;
import org.apache.geode.distributed.LocatorLauncherIntegrationTestCase;
import org.apache.geode.internal.process.ProcessControllerFactory;
import org.apache.geode.internal.process.ProcessType;
import org.apache.geode.test.assertj.LogFileAssert;
import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Integration tests of logging with {@link LocatorLauncher}.
 */
@Category(LoggingTest.class)
public class LoggingWithLocatorLauncherIntegrationTest extends LocatorLauncherIntegrationTestCase {

  @Before
  public void setUp() throws Exception {
    System.setProperty(ProcessType.PROPERTY_TEST_PREFIX, getUniqueName() + "-");
    assertThat(new ProcessControllerFactory().isAttachAPIFound()).isTrue();

    givenRunningLocator();
  }

  @After
  public void tearDown() throws Exception {
    disconnectFromDS();
  }

  @Test
  public void logFileExists() {
    assertThat(getLogFile()).exists();
  }

  @Test
  public void logFileContainsBanner() {
    LogFileAssert.assertThat(getLogFile()).contains(displayValues());
  }

  @Test
  public void logFileContainsBannerOnlyOnce() {
    LogFileAssert.assertThat(getLogFile()).containsOnlyOnce(displayValues());
  }
}
