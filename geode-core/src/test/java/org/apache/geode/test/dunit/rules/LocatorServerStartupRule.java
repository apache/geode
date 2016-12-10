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
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;
import org.junit.After;
import org.junit.Before;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
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
  private Member[] members;

  @Before
  public void before() throws Throwable {
    restoreSystemProperties.before();
    temporaryFolder.create();
    Invoke.invokeInEveryVM("Stop each VM", this::stop);
    members = new Member[4];
  }

  @After
  public void after() {
    restoreSystemProperties.after();
    temporaryFolder.delete();
    Invoke.invokeInEveryVM("Stop each VM", this::stop);
  }

  /**
   * Starts a locator instance with the given configuration properties inside
   * {@code getHost(0).getVM(index)}.
   *
   * @return VM locator vm
   */
  public Member startLocatorVM(int index, Properties locatorProperties) throws IOException {
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
    members[index] = new Member(locatorVM, locatorPort, workingDir);
    return members[index];
  }

  /**
   * starts a cache server that does not connect to a locator
   * 
   * @return VM node vm
   */
  public Member startServerVM(int index, Properties properties) throws IOException {
    return startServerVM(index, properties, 0);
  }

  /**
   * Starts a cache server that connect to the locator running at the given port.
   */
  public Member startServerVM(int index, Properties properties, int locatorPort)
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
    members[index] = new Member(serverVM, port, workingDir);
    return members[index];
  }

  /**
   * Returns the {@link Member} running inside the VM with the specified {@code index}
   */
  public Member getMember(int index) {
    return members[index];
  }

  public TemporaryFolder getRootFolder() {
    return temporaryFolder;
  }

  public final void stop() {
    if (serverStarter != null) {
      serverStarter.after();
    }
    if (locatorStarter != null) {
      locatorStarter.after();
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
