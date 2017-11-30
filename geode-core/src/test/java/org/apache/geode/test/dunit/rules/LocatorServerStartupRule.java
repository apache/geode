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

import static org.apache.geode.distributed.ConfigurationProperties.GROUPS;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.test.dunit.Host.getHost;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.InternalCache;
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
  public static MemberStarterRule memberStarter;

  public static InternalCache getCache() {
    return memberStarter.getCache();
  }

  public static InternalLocator getLocator() {
    return ((LocatorStarterRule) memberStarter).getLocator();
  }

  public static CacheServer getServer() {
    return ((ServerStarterRule) memberStarter).getServer();
  }

  private DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  private TemporaryFolder tempWorkingDir;
  private ArrayList<MemberVM> members;

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
    members = new ArrayList<>();
  }

  @Override
  protected void after() {
    try {
      DUnitLauncher.closeAndCheckForSuspects();
    } finally {
      MemberStarterRule.disconnectDSIfAny();
      IntStream.range(0, members.size()).forEach(this::stopVM);

      if (useTempWorkingDir()) {
        tempWorkingDir.delete();
      }
      restoreSystemProperties.after();
    }
  }

  public MemberVM startLocatorVM(int index) throws Exception {
    return startLocatorVM(index, new Properties());
  }

  public MemberVM startLocatorVM(int index, int... locatorPort) throws Exception {
    Properties properties = new Properties();
    String locators = Arrays.stream(locatorPort).mapToObj(i -> "localhost[" + i + "]")
        .collect(Collectors.joining(","));
    properties.setProperty(LOCATORS, locators);
    return startLocatorVM(index, properties);
  }

  /**
   * Starts a locator instance with the given configuration properties inside
   * {@code getHost(0).getVM(index)}.
   *
   * @return VM locator vm
   */
  public MemberVM startLocatorVM(int index, Properties specifiedProperties) throws Exception {
    Properties properties = new Properties();
    properties.putAll(specifiedProperties);

    String defaultName = "locator-" + index;
    properties.putIfAbsent(NAME, defaultName);
    String name = properties.getProperty(NAME);

    VM locatorVM = getVM(index);
    Locator locator = locatorVM.invoke(() -> {
      memberStarter = new LocatorStarterRule();
      LocatorStarterRule locatorStarter = (LocatorStarterRule) memberStarter;
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

    return setMember(index, new MemberVM(locator, locatorVM, useTempWorkingDir()));
  }

  private MemberVM setMember(int index, MemberVM element) {
    while (members.size() <= index) {
      members.add(null);
    }
    members.set(index, element);
    return members.get(index);
  }

  public MemberVM startServerVM(int index) throws IOException {
    return startServerVM(index, new Properties(), -1);
  }

  public MemberVM startServerVM(int index, int locatorPort) throws IOException {
    return startServerVM(index, new Properties(), locatorPort);
  }

  public MemberVM startServerVM(int index, String group, int locatorPort) throws IOException {
    Properties properties = new Properties();
    properties.put(GROUPS, group);
    return startServerVM(index, properties, locatorPort);
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
      memberStarter = new ServerStarterRule();
      ServerStarterRule serverStarter = (ServerStarterRule) memberStarter;
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
    return setMember(index, new MemberVM(server, serverVM, useTempWorkingDir()));
  }

  public void startServerVMAsync(int index) {
    startServerVMAsync(index, new Properties(), -1);
  }

  public void startServerVMAsync(int index, int locatorPort) {
    startServerVMAsync(index, new Properties(), locatorPort);
  }

  public void startServerVMAsync(int index, Properties specifiedProperties, int locatorPort) {
    assert members.get(index) != null;

    Properties properties = new Properties();
    properties.putAll(specifiedProperties);

    String defaultName = "server-" + index;
    properties.putIfAbsent(NAME, defaultName);
    String name = properties.getProperty(NAME);

    VM serverVM = getVM(index);
    serverVM.invokeAsync(() -> {
      memberStarter = new ServerStarterRule();
      ServerStarterRule serverStarter = (ServerStarterRule) memberStarter;
      if (useTempWorkingDir()) {
        File workingDirFile = createWorkingDirForMember(name);
        serverStarter.withWorkingDir(workingDirFile);
      }
      if (logFile) {
        serverStarter.withLogFile();
      }
      serverStarter.withProperties(properties).withConnectionToLocator(locatorPort).withAutoStart();
      serverStarter.before();
    });
  }

  public MemberVM startServerAsJmxManager(int index) throws IOException {
    return startServerAsJmxManager(index, new Properties());
  }

  public MemberVM startServerAsJmxManager(int index, Properties properties) throws IOException {
    properties.setProperty(JMX_MANAGER_PORT, AvailablePortHelper.getRandomAvailableTCPPort() + "");
    return startServerVM(index, properties, -1);
  }

  public MemberVM startServerAsJmxManager(int index, Properties properties, int locatorPort)
      throws IOException {
    properties.setProperty(JMX_MANAGER_PORT, AvailablePortHelper.getRandomAvailableTCPPort() + "");
    return startServerVM(index, properties, locatorPort);
  }

  public MemberVM startServerAsEmbededLocator(int index) throws IOException {
    return startServerAsEmbededLocator(index, new Properties());
  }

  public MemberVM startServerAsEmbededLocator(int index, Properties properties) throws IOException {
    String name = "server-" + index;

    VM serverVM = getVM(index);
    Server server = serverVM.invoke(() -> {
      memberStarter = new ServerStarterRule();
      ServerStarterRule serverStarter = (ServerStarterRule) memberStarter;
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
    return setMember(index, new MemberVM(server, serverVM, useTempWorkingDir()));
  }

  public void stopVM(int index) {
    stopVM(index, true);
  }

  public void stopVM(int index, boolean cleanWorkingDir) {
    MemberVM member = members.get(index);
    // user has started a server/locator in this VM
    if (member != null) {
      member.stopMember(cleanWorkingDir);
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
    return members.get(index);
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
    if (memberStarter != null) {
      memberStarter.after();
      memberStarter = null;
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
