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
package org.apache.geode.management.internal.cli.commands;

import static org.apache.geode.logging.internal.Configuration.STARTUP_CONFIGURATION;

import java.io.File;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.logging.Banner;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.categories.LoggingTest;
import org.apache.geode.test.junit.rules.gfsh.GfshExecution;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;
import org.apache.geode.test.junit.rules.gfsh.GfshScript;
import org.apache.geode.test.logging.LogFileAssert;

@Category({LoggingTest.class, GfshTest.class})
public class GfshStartLocatorLogAcceptanceTest {

  private File logFile;

  @Rule
  public GfshRule gfshRule = new GfshRule();

  @Before
  public void setUp() {
    GfshExecution gfshExecution = GfshScript.of("start locator").execute(gfshRule);
    File[] files = gfshExecution.getWorkingDir().listFiles();
    String logName = files[0].getAbsolutePath() + "/" + files[0].getName() + ".log";
    logFile = new File(logName);
  }

  @Test
  public void bannerIsLoggedOnlyOnce() {
    LogFileAssert.assertThat(logFile).containsOnlyOnce(Banner.BannerHeader.displayValues());
  }

  @Test
  public void startupConfigIsLoggedOnlyOnce() {
    LogFileAssert.assertThat(logFile).containsOnlyOnce(STARTUP_CONFIGURATION);
  }
}
