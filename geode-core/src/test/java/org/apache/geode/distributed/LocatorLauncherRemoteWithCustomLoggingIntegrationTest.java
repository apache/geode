/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.distributed;

import static org.apache.geode.internal.logging.log4j.custom.CustomConfiguration.CONFIG_LAYOUT_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.SystemOutRule;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.logging.log4j.custom.CustomConfiguration;
import org.apache.geode.internal.process.ProcessStreamReader;
import org.apache.geode.internal.process.ProcessType;
import org.apache.geode.internal.process.ProcessUtils;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Integration tests for launching a Locator in a forked process with custom logging configuration
 */
@Category(IntegrationTest.class)
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class LocatorLauncherRemoteWithCustomLoggingIntegrationTest extends AbstractLocatorLauncherRemoteIntegrationTestCase {

  private File customConfigFile;

  @Rule
  public SystemOutRule systemOutRule = new SystemOutRule().enableLog();

  @Before
  public void setUpLocatorLauncherRemoteWithCustomLoggingIntegrationTest() throws Exception {
    this.customConfigFile = CustomConfiguration.createConfigFileIn(this.temporaryFolder.getRoot());
  }

  @Test
  public void testStartUsesCustomLoggingConfiguration() throws Throwable {
    // build and start the locator
    final List<String> jvmArguments = getJvmArguments();

    final List<String> command = new ArrayList<String>();
    command.add(new File(new File(System.getProperty("java.home"), "bin"), "java").getCanonicalPath());
    for (String jvmArgument : jvmArguments) {
      command.add(jvmArgument);
    }
    command.add("-D" + ConfigurationFactory.CONFIGURATION_FILE_PROPERTY + "=" + this.customConfigFile.getCanonicalPath());
    command.add("-cp");
    command.add(System.getProperty("java.class.path"));
    command.add(LocatorLauncher.class.getName());
    command.add(LocatorLauncher.Command.START.getName());
    command.add(getUniqueName());
    command.add("--port=" + this.locatorPort);
    command.add("--redirect-output");

    this.process = new ProcessBuilder(command).directory(new File(this.workingDirectory)).start();
    this.processOutReader = new ProcessStreamReader.Builder(this.process).inputStream(this.process.getInputStream()).inputListener(new ToSystemOut()).build().start();
    this.processErrReader = new ProcessStreamReader.Builder(this.process).inputStream(this.process.getErrorStream()).inputListener(new ToSystemOut()).build().start();

    int pid = 0;
    this.launcher = new LocatorLauncher.Builder()
            .setWorkingDirectory(workingDirectory)
            .build();
    try {
      waitForLocatorToStart(this.launcher);

      // validate the pid file and its contents
      this.pidFile = new File(this.temporaryFolder.getRoot(), ProcessType.LOCATOR.getPidFileName());
      assertTrue(this.pidFile.exists());
      pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertTrue(ProcessUtils.isProcessAlive(pid));

      final String logFileName = getUniqueName()+".log";
      assertTrue("Log file should exist: " + logFileName, new File(this.temporaryFolder.getRoot(), logFileName).exists());

      // check the status
      final LocatorLauncher.LocatorState locatorState = this.launcher.status();
      assertNotNull(locatorState);
      assertEquals(AbstractLauncher.Status.ONLINE, locatorState.getStatus());

      assertThat(systemOutRule.getLog()).contains("log4j.configurationFile = " + this.customConfigFile.getCanonicalPath());
      assertThat(systemOutRule.getLog()).contains(CONFIG_LAYOUT_PREFIX);

    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }

    // stop the locator
    try {
      assertEquals(AbstractLauncher.Status.STOPPED, this.launcher.stop().getStatus());
      waitForPidToStop(pid);
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
  }

  private static class ToSystemOut implements ProcessStreamReader.InputListener {
    @Override
    public void notifyInputLine(String line) {
      System.out.println(line);
    }
  }

}
