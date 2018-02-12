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
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_AUTH_INIT;
import static org.apache.geode.test.dunit.Host.getHost;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.security.templates.UserPasswordAuthInit;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.dunit.standalone.VersionManager;
import org.apache.geode.test.junit.rules.ClientCacheRule;
import org.apache.geode.test.junit.rules.Locator;
import org.apache.geode.test.junit.rules.LocatorStarterRule;
import org.apache.geode.test.junit.rules.Member;
import org.apache.geode.test.junit.rules.MemberStarterRule;
import org.apache.geode.test.junit.rules.Server;
import org.apache.geode.test.junit.rules.ServerStarterRule;
import org.apache.geode.test.junit.rules.VMProvider;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;


/**
 * A rule to help you start locators and servers inside of a
 * <a href="https://cwiki.apache.org/confluence/display/GEODE/Distributed-Unit-Tests">DUnit
 * test</a>. This rule will start Servers and Locators inside of the four remote {@link VM}s created
 * by the DUnit framework.
 */
public class ClusterStartupRule extends ExternalResource implements Serializable {
  /**
   * This is only available in each Locator/Server VM, not in the controller (test) VM.
   */
  public static MemberStarterRule memberStarter;
  public static ClientCacheRule clientCacheRule;

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
  private Map<Integer, VMProvider> occupiedVMs;

  private boolean logFile = false;

  public ClusterStartupRule() {
    DUnitLauncher.launchIfNeeded();
  }

  public static ClientCache getClientCache() {
    return clientCacheRule.getCache();
  }

  /**
   * This will use a temporary folder to hold all the vm directories instead of using dunit folder.
   * It will set each VM's working dir to its respective sub-directories.
   *
   * use this if you want to examine each member's file system without worrying about it's being
   * contaminated with DUnitLauncher's log files that exists in each dunit/vm folder such as
   * locatorxxxView.dat and locatorxxxviews.log and other random log files.
   *
   * If the product code is doing new File(".") or new File("relative-path.log"), it will still
   * pointing to the a File under the old CWD. So avoid using relative path and always use absolute
   * path or with a parent dir when creating new File object.
   *
   * But this will cause the VMs to be bounced after test is done, because it dynamically changes
   * the user.dir system property, causing slow running tests. Use with discretion.
   */
  public ClusterStartupRule withTempWorkingDir() {
    tempWorkingDir = new SerializableTemporaryFolder();
    return this;
  }

  public boolean useTempWorkingDir() {
    return tempWorkingDir != null;
  }

  /**
   * this will allow all the logs go into log files instead of going into the console output
   */
  public ClusterStartupRule withLogFile() {
    this.logFile = true;
    return this;
  }

  @Override
  protected void before() throws Throwable {
    restoreSystemProperties.before();
    if (useTempWorkingDir()) {
      tempWorkingDir.create();
    }
    occupiedVMs = new HashMap<>();
  }

  @Override
  protected void after() {
    try {
      DUnitLauncher.closeAndCheckForSuspects();
    } finally {
      MemberStarterRule.disconnectDSIfAny();

      // stop all the clientsVM before stop all the memberVM
      occupiedVMs.values().forEach(x -> x.stopVM(true));

      if (useTempWorkingDir()) {
        tempWorkingDir.delete();
      }
      restoreSystemProperties.after();
    }
  }

  /**
   * Starts a locator instance with the given configuration properties inside
   * {@code getHost(0).getVM(index)}.
   *
   * @return VM locator vm
   */
  public MemberVM startLocatorVM(int index, Properties specifiedProperties, String version)
      throws Exception {
    Properties properties = new Properties();
    properties.putAll(specifiedProperties);

    String defaultName = "locator-" + index;
    properties.putIfAbsent(NAME, defaultName);
    String name = properties.getProperty(NAME);

    VM locatorVM = getVM(index, version);
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
    MemberVM memberVM = new MemberVM(locator, locatorVM, useTempWorkingDir());
    occupiedVMs.put(index, memberVM);
    return memberVM;
  }

  public MemberVM startLocatorVM(int index, Properties specifiedProperties) throws Exception {
    return startLocatorVM(index, specifiedProperties, VersionManager.CURRENT_VERSION);
  }

  public MemberVM startLocatorVM(int index, int... locatorPort) throws Exception {
    Properties properties = new Properties();
    String locators = Arrays.stream(locatorPort).mapToObj(i -> "localhost[" + i + "]")
        .collect(Collectors.joining(","));
    properties.setProperty(LOCATORS, locators);
    return startLocatorVM(index, properties);
  }

  public MemberVM startLocatorVM(int index) throws Exception {
    return startLocatorVM(index, new Properties());
  }

  /**
   * Starts a cache server with given properties
   */
  public MemberVM startServerVM(int index, Properties specifiedProperties, int locatorPort,
      String version) throws IOException {
    Properties properties = new Properties();
    properties.putAll(specifiedProperties);

    String defaultName = "server-" + index;
    properties.putIfAbsent(NAME, defaultName);
    String name = properties.getProperty(NAME);

    VM serverVM = getVM(index, version);
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

    MemberVM memberVM = new MemberVM(server, serverVM, useTempWorkingDir());
    occupiedVMs.put(index, memberVM);
    return memberVM;
  }

  public MemberVM startServerVM(int index, Properties specifiedProperties, int locatorPort)
      throws IOException {
    return startServerVM(index, specifiedProperties, locatorPort, VersionManager.CURRENT_VERSION);
  }

  public MemberVM startServerVM(int index, String group, int locatorPort) throws IOException {
    Properties properties = new Properties();
    properties.put(GROUPS, group);
    return startServerVM(index, properties, locatorPort);
  }

  public MemberVM startServerVM(int index, int locatorPort) throws IOException {
    return startServerVM(index, new Properties(), locatorPort);
  }

  public MemberVM startServerVM(int index, Properties properties) throws IOException {
    return startServerVM(index, properties, -1);
  }

  public MemberVM startServerVM(int index) throws IOException {
    return startServerVM(index, new Properties(), -1);
  }

  /**
   * Starts a cache server with given properties, plus an available port for a JMX manager
   */
  public MemberVM startServerAsJmxManager(int index, Properties properties) throws IOException {
    properties.setProperty(JMX_MANAGER_PORT, AvailablePortHelper.getRandomAvailableTCPPort() + "");
    return startServerVM(index, properties, -1);
  }

  public MemberVM startServerAsJmxManager(int index) throws IOException {
    return startServerAsJmxManager(index, new Properties());
  }

  /**
   * Starts a cache server with given properties. Additionally, start the server with a JMX manager
   * and embedded locator.
   */
  public MemberVM startServerAsEmbeddedLocator(int index, Properties specifiedProperties,
      String version) throws IOException {
    Properties properties = new Properties();
    properties.putAll(specifiedProperties);

    String defaultName = "server-" + index;
    properties.putIfAbsent(NAME, defaultName);
    String name = properties.getProperty(NAME);

    VM serverVM = getVM(index, version);
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

    MemberVM memberVM = new MemberVM(server, serverVM, useTempWorkingDir());
    occupiedVMs.put(index, memberVM);
    return memberVM;
  }

  public MemberVM startServerAsEmbeddedLocator(int index, Properties properties)
      throws IOException {
    return startServerAsEmbeddedLocator(index, properties, VersionManager.CURRENT_VERSION);
  }

  public MemberVM startServerAsEmbeddedLocator(int index) throws IOException {
    return startServerAsEmbeddedLocator(index, new Properties());
  }

  /**
   * Starts a client with the given properties, configuring the cacheFactory with the provided
   * Consumer
   */
  public ClientVM startClientVM(int index, Properties properties,
      Consumer<ClientCacheFactory> cacheFactorySetup, String clientVersion) throws Exception {
    return startClientVM(index, properties, cacheFactorySetup, clientVersion,
        (Runnable & Serializable) () -> {
        });
  }

  public ClientVM startClientVM(int index, Properties properties,
      Consumer<ClientCacheFactory> cacheFactorySetup, String clientVersion,
      Runnable clientCacheHook) throws Exception {
    VM client = getVM(index, clientVersion);
    Exception error = client.invoke(() -> {
      clientCacheRule =
          new ClientCacheRule().withProperties(properties).withCacheSetup(cacheFactorySetup);
      try {
        clientCacheRule.before();
        clientCacheHook.run();
        return null;
      } catch (Exception e) {
        return e;
      }
    });
    if (error != null) {
      throw error;
    }
    ClientVM clientVM = new ClientVM(client);
    occupiedVMs.put(index, clientVM);
    return clientVM;
  }

  public ClientVM startClientVM(int index, Properties properties,
      Consumer<ClientCacheFactory> cacheFactorySetup) throws Exception {
    return startClientVM(index, properties, cacheFactorySetup, VersionManager.CURRENT_VERSION);
  }

  public ClientVM startClientVM(int index, String username, String password,
      boolean subscriptionEnabled, int... serverPorts) throws Exception {
    Properties props = new Properties();
    props.setProperty(UserPasswordAuthInit.USER_NAME, username);
    props.setProperty(UserPasswordAuthInit.PASSWORD, password);
    props.setProperty(SECURITY_CLIENT_AUTH_INIT, UserPasswordAuthInit.class.getName());

    Consumer<ClientCacheFactory> consumer =
        (Serializable & Consumer<ClientCacheFactory>) ((cacheFactory) -> {
          cacheFactory.setPoolSubscriptionEnabled(subscriptionEnabled);
          for (int serverPort : serverPorts) {
            cacheFactory.addPoolServer("localhost", serverPort);
          }
        });
    return startClientVM(index, props, consumer);
  }

  public ClientVM startClientVM(int index, String username, String password,
      boolean subscriptionEnabled, int serverPort, Runnable clientCacheHook) throws Exception {
    Properties props = new Properties();
    props.setProperty(UserPasswordAuthInit.USER_NAME, username);
    props.setProperty(UserPasswordAuthInit.PASSWORD, password);
    props.setProperty(SECURITY_CLIENT_AUTH_INIT, UserPasswordAuthInit.class.getName());

    Consumer<ClientCacheFactory> consumer =
        (Serializable & Consumer<ClientCacheFactory>) ((cacheFactory) -> {
          cacheFactory.setPoolSubscriptionEnabled(subscriptionEnabled);
          cacheFactory.addPoolServer("localhost", serverPort);
        });
    return startClientVM(index, props, consumer, VersionManager.CURRENT_VERSION, clientCacheHook);
  }

  /**
   * Returns the {@link Member} running inside the VM with the specified {@code index}
   */
  public MemberVM getMember(int index) {
    return (MemberVM) occupiedVMs.get(index);
  }

  public VM getVM(int index, String version) {
    return getHost(0).getVM(version, index);
  }

  public VM getVM(int index) {
    return getHost(0).getVM(index);
  }

  public void stopVM(int index) {
    stopVM(index, true);
  }

  public void stopVM(int index, boolean cleanWorkingDir) {
    VMProvider member = occupiedVMs.get(index);

    if (member == null)
      return;

    member.stopVM(cleanWorkingDir);
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

  public static void stopElementInsideVM() {
    if (memberStarter != null) {
      memberStarter.after();
      memberStarter = null;
    }
    if (clientCacheRule != null) {
      clientCacheRule.after();
      clientCacheRule = null;
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
