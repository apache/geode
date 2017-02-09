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
 *
 */

package org.apache.geode.test.dunit.rules;

import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.test.dunit.Host.getHost;

import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


/**
 * A rule to help you start locators and servers inside of a
 * <a href="https://cwiki.apache.org/confluence/display/GEODE/Distributed-Unit-Tests">DUnit
 * test</a>. This rule will start Servers and Locators inside of the four remote {@link VM}s created
 * by the DUnit framework.
 */
public class LocatorServerStartupRule extends ExternalResource implements Serializable {

  /**
   * This is only available in each Locator/Server VM, not in the controller (test) VM.
   */
  public static ServerStarterRule serverStarter;

  /**
   * This is only available in each Locator/Server VM, not in the controller (test) VM.
   */
  public static LocatorStarterRule locatorStarter;

  private DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  private TemporaryFolder temporaryFolder = new SerializableTemporaryFolder();
  private List<Member> members;

  public LocatorServerStartupRule() {
    DUnitLauncher.launchIfNeeded();
  }

  @Override
  protected void before() throws Throwable {
    restoreSystemProperties.before();
    temporaryFolder.create();
    Invoke.invokeInEveryVM("Stop each VM", this::stopServerOrLocatorInThisVM);
    members = new ArrayList<>(4);
  }

  @Override
  protected void after() {
    restoreSystemProperties.after();
    temporaryFolder.delete();
    Invoke.invokeInEveryVM("Stop each VM", this::stopServerOrLocatorInThisVM);
  }

  /**
   * Starts a locator instance with the given configuration properties inside
   * {@code getHost(0).getVM(index)}.
   *
   * @return VM locator vm
   */
  public Locator startLocatorVM(int index, Properties locatorProperties) throws IOException {
    String name = "locator-" + index;
    locatorProperties.setProperty(NAME, name);
    File workingDir = createWorkingDirForMember(name);

    VM locatorVM = getHost(0).getVM(index);
    int locatorPort = locatorVM.invoke(() -> {
      System.setProperty("user.dir", workingDir.getCanonicalPath());
      locatorStarter = new LocatorStarterRule(locatorProperties);
      locatorStarter.startLocator();
      return locatorStarter.locator.getPort();
    });
    Locator locator = new Locator(locatorVM, locatorPort, workingDir, name);
    members.add(index, locator);
    return locator;
  }

  public Locator startLocatorVMWithPulse(int index, Properties locatorProperties)
      throws IOException {
    String name = "locator-" + index;
    locatorProperties.setProperty(NAME, name);
    File workingDir = createWorkingDirForMember(name);

    // Setting gemfire.home to this value allows locators started by the rule to run Pulse
    String geodeInstallDir = new File(".").getAbsoluteFile().getParentFile().getParentFile()
        .toPath().resolve("geode-assembly").resolve("build").resolve("install")
        .resolve("apache-geode").toString();

    System.out.println("Current dir is " + new File(".").getCanonicalPath());
    System.out.println("Setting gemfire.home to " + geodeInstallDir);

    VM locatorVM = getHost(0).getVM(index);
    int locatorPort = locatorVM.invoke(() -> {
      System.setProperty("user.dir", workingDir.getCanonicalPath());
      System.setProperty("gemfire.home", geodeInstallDir);
      locatorStarter = new LocatorStarterRule(locatorProperties);
      locatorStarter.startLocator();
      return locatorStarter.locator.getPort();
    });
    Locator locator = new Locator(locatorVM, locatorPort, workingDir, name);
    members.add(index, locator);
    return locator;
  }


  public Locator startLocatorVM(int index) throws IOException {
    return startLocatorVM(index, new Properties());
  }

  /**
   * starts a cache server that does not connect to a locator
   * 
   * @return VM node vm
   */
  public Server startServerVM(int index, Properties properties) throws IOException {
    return startServerVM(index, properties, 0);
  }

  public Server startServerVM(int index, int locatorPort) throws IOException {
    return startServerVM(index, new Properties(), locatorPort);
  }

  /**
   * Starts a cache server that connect to the locator running at the given port.
   */
  public Server startServerVM(int index, Properties properties, int locatorPort)
      throws IOException {
    String name = "server-" + index;
    properties.setProperty(NAME, name);
    File workingDir = createWorkingDirForMember(name);

    VM serverVM = getHost(0).getVM(index);
    int port = serverVM.invoke(() -> {
      System.setProperty("user.dir", workingDir.getCanonicalPath());
      serverStarter = new ServerStarterRule(properties);
      serverStarter.startServer(locatorPort);
      return serverStarter.server.getPort();
    });
    Server server = new Server(serverVM, port, workingDir, name);
    members.add(index, server);
    return server;
  }

  /**
   * Returns the {@link Member} running inside the VM with the specified {@code index}
   */
  public Member getMember(int index) {
    return members.get(index);
  }

  public TemporaryFolder getTempFolder() {
    return temporaryFolder;
  }

  private void stopServerOrLocatorInThisVM() {
    if (serverStarter != null) {
      serverStarter.after();
      serverStarter = null;
    }
    if (locatorStarter != null) {
      locatorStarter.after();
      locatorStarter = null;
    }
  }

  private File createWorkingDirForMember(String dirName) throws IOException {
    File workingDir = new File(temporaryFolder.getRoot(), dirName);
    if (!workingDir.exists()) {
      temporaryFolder.newFolder(dirName);
    }

    return workingDir;
  }
}
