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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.nio.charset.StandardCharsets;

import com.google.common.io.Files;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.AbstractConfig;
import org.apache.geode.internal.Banner;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;
import org.apache.geode.test.dunit.rules.gfsh.GfshExecution;
import org.apache.geode.test.dunit.rules.gfsh.GfshRule;
import org.apache.geode.test.dunit.rules.gfsh.GfshScript;
import org.apache.geode.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class GfshStartLocatorLogTest {
  @Rule
  public GfshRule gfshRule = new GfshRule();

  @Test
  public void bannerOnlyLogsOnce() throws Exception {
    final String banner = "Licensed to the Apache Software Foundation (ASF)";
    String lines = getExecutionLogs();
    assertThat(lines.indexOf(banner)).isEqualTo(lines.lastIndexOf(banner));
  }

  @Test
  public void startupConfigsOnlyLogsOnce() throws Exception {
    final String startupConfigs = AbstractConfig.GEM_FIRE_PROPERTIES_USING_DEFAULT_VALUES;
    String lines = getExecutionLogs();
    assertThat(lines.indexOf(startupConfigs)).isEqualTo(lines.lastIndexOf(startupConfigs));
  }

  private String getExecutionLogs() throws Exception {
    GfshExecution gfshExecution = GfshScript.of("start locator").execute(gfshRule);
    File[] files = gfshExecution.getWorkingDir().listFiles();
    String logName = files[0].getAbsolutePath() + "/" + files[0].getName() + ".log";
    return Files.readLines(new File(logName), StandardCharsets.UTF_8).toString();
  }
}
