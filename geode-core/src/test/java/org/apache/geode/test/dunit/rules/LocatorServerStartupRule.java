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

import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.test.dunit.Host.getHost;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;
import java.util.stream.IntStream;

import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.rules.Locator;
import org.apache.geode.test.junit.rules.LocatorStarterRule;
import org.apache.geode.test.junit.rules.Member;
import org.apache.geode.test.junit.rules.MemberStarterRule;
import org.apache.geode.test.junit.rules.Server;
import org.apache.geode.test.junit.rules.ServerStarterRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;


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

  private TemporaryFolder tempWorkingDir;
  private MemberVM[] members;

  private boolean logFile = false;

  public LocatorServerStartupRule() {
    DUnitLauncher.launchIfNeeded();
  }

  /**
   * This will use a temporary folder to hold all the vm directories instead of using dunit folder.
   * It will set each VM's working dir to its respective sub-directories.
   *
   * use this if you want to examine each member's file system without worrying about it's being
   * contaminated with DUnitLauncher's log files that exists in each dunit/vm folder such as
   * locator0View.dat and locator0views.log and other random log files. This will cause the VMs to
   * be bounced after test is done, because it dynamically changes the user.dir system property.
   */
  public LocatorServerStartupRule withTempWorkingDir() {
    tempWorkingDir = new SerializableTemporaryFolder();
    return this;
  }

  public boolean useTempWorkingDir() {
    return tempWorkingDir != null;
  }

  /**
   * this will allow all the logs go into log files instead of going into the console output
   */
  public LocatorServerStartupRule withLogFile() {
    this.logFile = true;
    return this;
  }

  @Override
  protected void before() throws Throwable {
    restoreSystemProperties.before();
    if (useTempWorkingDir()) {
      tempWorkingDir.create();
    }
    members = new MemberVM[DUnitLauncher.NUM_VMS];
  }

  @Override
  protected void after() {
    try {
      DUnitLauncher.closeAndCheckForSuspects();
    } finally {
      MemberStarterRule.disconnectDSIfAny();
      IntStream.range(0, DUnitLauncher.NUM_VMS).forEach(this::stopVM);

      if (useTempWorkingDir()) {
        tempWorkingDir.delete();
      }
      restoreSystemProperties.after();
    }
  }

  public MemberVM<Locator> startLocatorVM(int index) throws Exception {
    return startLocatorVM(index, new Properties());
  }

  /**
   * Starts a locator instance with the given configuration properties inside
   * {@code getHost(0).getVM(index)}.
   * 
   * @return VM locator vm
   */
  public MemberVM<Locator> startLocatorVM(int index, Properties specifiedProperties)
      throws Exception {
    Properties properties = new Properties();
    properties.putAll(specifiedProperties);

    String defaultName = "locator-" + index;
    properties.putIfAbsent(NAME, defaultName);
    String name = properties.getProperty(NAME);

    VM locatorVM = getVM(index);
    Locator locator = locatorVM.invoke(() -> {
      locatorStarter = new LocatorStarterRule();
      if (useTempWorkingDir()) {
        File workingDirFile = createWorkingDirForMember(name);
        locatorStarter.withWorkingDir(workingDirFile);
      }
      if (logFile) {
        locatorStarter.withLogFile();
      }
      locatorStarter.withProperties(properties).withAutoStart();
      locatorStarter.before();
      return locatorStarter;
    });
    members[index] = new MemberVM(locator, locatorVM, useTempWorkingDir());
    return members[index];
  }

  public MemberVM startServerVM(int index) throws IOException {
    return startServerVM(index, new Properties(), -1);
  }

  public MemberVM startServerVM(int index, int locatorPort) throws IOException {
    return startServerVM(index, new Properties(), locatorPort);
  }

  public MemberVM startServerVM(int index, Properties properties) throws IOException {
    return startServerVM(index, properties, -1);
  }

  /**
   * Starts a cache server with given properties
   */
  public MemberVM startServerVM(int index, Properties specifiedProperties, int locatorPort)
      throws IOException {
    Properties properties = new Properties();
    properties.putAll(specifiedProperties);

    String defaultName = "server-" + index;
    properties.putIfAbsent(NAME, defaultName);
    String name = properties.getProperty(NAME);

    VM serverVM = getVM(index);
    Server server = serverVM.invoke(() -> {
      serverStarter = new ServerStarterRule();
      if (useTempWorkingDir()) {
        File workingDirFile = createWorkingDirForMember(name);
        serverStarter.withWorkingDir(workingDirFile);
      }
      if (logFile) {
        serverStarter.withLogFile();
      }
      serverStarter.withProperties(properties).withConnectionToLocator(locatorPort).withAutoStart();
      serverStarter.before();
      return serverStarter;
    });
    members[index] = new MemberVM(server, serverVM, useTempWorkingDir());
    return members[index];
  }

  public MemberVM startServerAsJmxManager(int index) throws IOException {
    return startServerAsJmxManager(index, new Properties());
  }

  public MemberVM startServerAsJmxManager(int index, Properties properties) throws IOException {
    properties.setProperty(JMX_MANAGER_PORT, AvailablePortHelper.getRandomAvailableTCPPort() + "");
    return startServerVM(index, properties, -1);
  }

  public MemberVM startServerAsEmbededLocator(int index) throws IOException {
    return startServerAsEmbededLocator(index, new Properties());
  }

  public MemberVM startServerAsEmbededLocator(int index, Properties properties) throws IOException {
    String name = "server-" + index;

    VM serverVM = getVM(index);
    Server server = serverVM.invoke(() -> {
      serverStarter = new ServerStarterRule();
      if (useTempWorkingDir()) {
        File workingDirFile = createWorkingDirForMember(name);
        serverStarter.withWorkingDir(workingDirFile);
      }
      if (logFile) {
        serverStarter.withLogFile();
      }
      serverStarter.withEmbeddedLocator().withProperties(properties).withName(name).withJMXManager()
          .withAutoStart();
      serverStarter.before();
      return serverStarter;
    });
    members[index] = new MemberVM(server, serverVM, useTempWorkingDir());
    return members[index];
  }

  public void stopVM(int index) {
    MemberVM member = members[index];
    // user has started a server/locator in this VM
    if (member != null) {
      member.stopMember();
    }
    // user may have used this VM as a client VM
    else {
      getVM(index).invoke(() -> MemberStarterRule.disconnectDSIfAny());
    }
  }

  /**
   * Returns the {@link Member} running inside the VM with the specified {@code index}
   */
  public MemberVM getMember(int index) {
    return members[index];
  }

  public VM getVM(int index) {
    return getHost(0).getVM(index);
  }

  public TemporaryFolder getTempWorkingDir() {
    return tempWorkingDir;
  }

  public File getWorkingDirRoot() {
    if (useTempWorkingDir())
      return tempWorkingDir.getRoot();

    // return the dunit folder
    return new File(DUnitLauncher.DUNIT_DIR);
  }

  public static void stopMemberInThisVM() {
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
    File workingDir = new File(tempWorkingDir.getRoot(), dirName).getAbsoluteFile();
    if (!workingDir.exists()) {
      tempWorkingDir.newFolder(dirName);
    }

    return workingDir;
  }
}
