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
package org.apache.geode.distributed;

import static org.apache.geode.internal.logging.log4j.custom.CustomConfiguration.CONFIG_LAYOUT_PREFIX;
import static org.apache.geode.internal.logging.log4j.custom.CustomConfiguration.createConfigFileIn;
import static org.apache.logging.log4j.core.config.ConfigurationFactory.CONFIGURATION_FILE_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.SystemOutRule;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.LocatorLauncher.Command;
import org.apache.geode.internal.process.ProcessStreamReader.InputListener;
import org.apache.geode.test.junit.categories.IntegrationTest;

/**
 * Integration tests for using {@code LocatorLauncher} as an application main in a forked JVM with
 * custom logging configuration.
 */
@Category(IntegrationTest.class)
public class LocatorLauncherRemoteWithCustomLoggingIntegrationTest
    extends LocatorLauncherRemoteIntegrationTestCase {

  private File customLoggingConfigFile;

  @Rule
  public SystemOutRule systemOutRule = new SystemOutRule().enableLog();

  @Before
  public void setUpLocatorLauncherRemoteWithCustomLoggingIntegrationTest() throws Exception {
    this.customLoggingConfigFile = createConfigFileIn(getWorkingDirectory());
  }

  @Test
  public void startWithCustomLoggingConfiguration() throws Exception {
    startLocator(new LocatorCommand(this)
        .addJvmArgument("-D" + CONFIGURATION_FILE_PROPERTY + "=" + getCustomLoggingConfigFilePath())
        .withCommand(Command.START), new ToSystemOut(), new ToSystemOut());

    assertThat(systemOutRule.getLog()).contains(CONFIG_LAYOUT_PREFIX)
        .contains("log4j.configurationFile = " + getCustomLoggingConfigFilePath());
  }

  private String getCustomLoggingConfigFilePath() {
    try {
      return customLoggingConfigFile.getCanonicalPath();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static class ToSystemOut implements InputListener {
    @Override
    public void notifyInputLine(final String line) {
      System.out.println(line);
    }
  }
}
