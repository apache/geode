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

import static org.apache.geode.test.util.ResourceUtils.createFileFromResource;
import static org.apache.geode.test.util.ResourceUtils.getResource;
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

import org.apache.geode.distributed.ServerLauncher.Command;
import org.apache.geode.internal.process.ProcessStreamReader.InputListener;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Integration tests for launching a Server in a forked process with custom logging configuration
 */
@Category({GfshTest.class, LoggingTest.class})
public class ServerLauncherRemoteWithCustomLoggingIntegrationTest
    extends ServerLauncherRemoteIntegrationTestCase {

  private static final String CONFIG_LAYOUT_PREFIX = "CUSTOM";

  private File customLoggingConfigFile;

  @Rule
  public SystemOutRule systemOutRule = new SystemOutRule().enableLog();

  @Before
  public void setUpServerLauncherRemoteWithCustomLoggingIntegrationTest() throws Exception {
    String configFileName = getClass().getSimpleName() + "_log4j2.xml";
    customLoggingConfigFile = createFileFromResource(getResource(configFileName),
        getWorkingDirectory(), configFileName);
  }

  @Test
  public void startWithCustomLoggingConfiguration() throws Exception {
    startServer(
        new ServerCommand(this).addJvmArgument(customLoggingConfigArgument())
            .disableDefaultServer(true).withCommand(Command.START),
        new ToSystemOut(), new ToSystemOut());

    assertThat(systemOutRule.getLog())
        .contains(CONFIGURATION_FILE_PROPERTY + " = " + getCustomLoggingConfigFilePath());
    assertThat(systemOutRule.getLog()).contains(CONFIG_LAYOUT_PREFIX);
  }

  private String customLoggingConfigArgument() {
    return "-D" + CONFIGURATION_FILE_PROPERTY + "=" + getCustomLoggingConfigFilePath();
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
