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

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.JarFileRule;
import org.apache.geode.test.junit.rules.LocatorStarterRule;


public class ConcurrentDeployDUnitTest {

  @Rule
  public ClusterStartupRule lsRule = new ClusterStartupRule();

  @Rule
  public LocatorStarterRule locator = new LocatorStarterRule().withAutoStart();

  @Rule
  public JarFileRule jar1Rule = new JarFileRule("classOne", "jar1.jar", true);

  // This is a reference used to refer to connections in VM 2 and VM 3
  private static GfshCommandRule gfsh;

  private VM gfsh1, gfsh2, gfsh3;

  @Test
  @SuppressWarnings("deprecation")
  public void testMultipleGfshClientToOneServer() throws Exception {
    lsRule.startServerVM(0, locator.getPort());
    gfsh1 = lsRule.getVM(1);
    gfsh2 = lsRule.getVM(2);
    gfsh3 = lsRule.getVM(3);

    int locatorPort = locator.getPort();

    gfsh1.invoke(() -> connectToLocator(locatorPort));
    gfsh2.invoke(() -> connectToLocator(locatorPort));
    gfsh3.invoke(() -> connectToLocator(locatorPort));

    File jar1 = jar1Rule.getJarFile();
    AsyncInvocation<Void> gfsh1Invocation =
        gfsh1.invokeAsync(() -> loopThroughDeployAndUndeploys(jar1));
    AsyncInvocation<Void> gfsh2Invocation =
        gfsh2.invokeAsync(() -> loopThroughDeployAndUndeploys(jar1));
    AsyncInvocation<Void> gfsh3Invocation =
        gfsh3.invokeAsync(() -> loopThroughDeployAndUndeploys(jar1));

    gfsh1Invocation.await(30, TimeUnit.MINUTES);
    gfsh2Invocation.await(30, TimeUnit.MINUTES);
    gfsh3Invocation.await(30, TimeUnit.MINUTES);
  }

  @After
  public void after() {
    gfsh1.invoke(() -> gfsh.close());
    gfsh2.invoke(() -> gfsh.close());
    gfsh3.invoke(() -> gfsh.close());
  }

  public static void connectToLocator(int locatorPort) throws Exception {
    gfsh = new GfshCommandRule();
    gfsh.connectAndVerify(locatorPort, GfshCommandRule.PortType.locator);
  }

  public static void loopThroughDeployAndUndeploys(File jar1) {
    int numTimesToExecute = 50;
    String command;

    for (int i = 1; i <= numTimesToExecute; i++) {
      command = "deploy --jar=" + jar1.getAbsolutePath();
      gfsh.executeAndAssertThat(command).statusIsSuccess();

      command = "list deployed";
      gfsh.executeAndAssertThat(command).statusIsSuccess();

      command = "undeploy --jar=" + jar1.getName();
      gfsh.executeAndAssertThat(command).statusIsSuccess();

    }
  }

}
