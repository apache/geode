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

import static org.apache.geode.management.internal.i18n.CliStrings.STATUS_LOCATOR;

import java.io.File;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.DUnitLauncher;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.GfshCommandRule.PortType;
import org.apache.geode.test.junit.rules.LocatorLauncherStartupRule;
import org.apache.geode.test.junit.rules.MemberStarterRule;

public class StatusLocatorCommandSSLTest {
  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  private static VM locatorVM;
  private static String workingDir;
  private static int locatorPort;
  private static String memberId;
  private static File gfPropertiesFile;

  @BeforeClass
  public static void beforeClass() throws Exception {
    DUnitLauncher.launchIfNeeded(false);
    locatorVM = VM.getVM(0);
    Properties sslProperties = MemberStarterRule.getSSLProperties("all", false, false);
    Map<String, Object> results = locatorVM.invoke(() -> {
      LocatorLauncherStartupRule launcherStartupRule = new LocatorLauncherStartupRule()
          .withProperties(sslProperties);
      launcherStartupRule.start();
      Map<String, Object> result = new HashMap<>();
      result.put("workingDir", launcherStartupRule.getWorkingDir().getAbsolutePath());
      result.put("port", launcherStartupRule.getLauncher().getPort());
      result.put("id", launcherStartupRule.getLauncher().getMemberId());
      return result;
    });
    workingDir = (String) results.get("workingDir");
    locatorPort = (int) results.get("port");
    memberId = (String) results.get("id");

    gfPropertiesFile = new File(locatorVM.getWorkingDirectory(), "security.properties");
    FileOutputStream out = new FileOutputStream(gfPropertiesFile);
    sslProperties.store(out, null);
    gfsh.connectAndVerify(locatorPort, PortType.locator, "security-properties-file",
        gfPropertiesFile.getAbsolutePath());
  }

  @Test
  public void testWithMemberAddress() throws Exception {
    gfsh.executeAndAssertThat(STATUS_LOCATOR + " --host=localhost --port=" + locatorPort
        + " --security-properties-file=" + gfPropertiesFile.getAbsolutePath())
        .statusIsSuccess();
  }

  @Test
  public void testWithMemberName() throws Exception {
    gfsh.executeAndAssertThat(STATUS_LOCATOR + " --name=locator-0" + " --security-properties-file="
        + gfPropertiesFile.getAbsolutePath()).statusIsSuccess();
  }

  @Test
  public void testWithMemberId() throws Exception {
    gfsh.executeAndAssertThat(STATUS_LOCATOR + " --name=" + memberId
        + " --security-properties-file=" + gfPropertiesFile.getAbsolutePath()).statusIsSuccess();
  }

  @Test
  public void testWithDirOnline() throws Exception {
    gfsh.executeAndAssertThat(STATUS_LOCATOR + " --dir=" + workingDir
        + " --security-properties-file=" + gfPropertiesFile.getAbsolutePath()).statusIsSuccess();
  }

  @Test
  public void testWithDirOffline() throws Exception {
    gfsh.disconnect();
    gfsh.executeAndAssertThat(STATUS_LOCATOR + " --dir=" + workingDir
        + " --security-properties-file=" + gfPropertiesFile.getAbsolutePath()).statusIsSuccess();
  }
}
