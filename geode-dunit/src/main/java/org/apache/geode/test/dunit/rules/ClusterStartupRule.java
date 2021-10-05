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

import static org.apache.commons.lang3.SystemUtils.isJavaVersionAtLeast;
import static org.apache.geode.distributed.ConfigurationProperties.GROUPS;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.apache.geode.test.dunit.internal.DUnitLauncher.NUM_VMS;
import static org.apache.geode.test.dunit.internal.DUnitLauncher.closeAndCheckForSuspects;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.JavaVersion;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.SerializableConsumerIF;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.DUnitLauncher;
import org.apache.geode.test.junit.rules.ClientCacheRule;
import org.apache.geode.test.junit.rules.Locator;
import org.apache.geode.test.junit.rules.LocatorStarterRule;
import org.apache.geode.test.junit.rules.Member;
import org.apache.geode.test.junit.rules.MemberStarterRule;
import org.apache.geode.test.junit.rules.Server;
import org.apache.geode.test.junit.rules.ServerStarterRule;
import org.apache.geode.test.junit.rules.VMProvider;
import org.apache.geode.test.junit.rules.serializable.SerializableTestRule;
import org.apache.geode.test.version.VersionManager;

/**
 * A rule to help you start locators and servers or clients inside of a
 * <a href="https://cwiki.apache.org/confluence/display/GEODE/Distributed-Unit-Tests">DUnit
 * test</a>. This rule will start Servers and Locators inside of the four remote {@link VM}s created
 * by the DUnit framework. Using this rule will eliminate the need to extends
 * JUnit4DistributedTestCase when writing a Dunit test
 *
 * <p>
 * If you use this Rule in any test that uses more than the default of 4 VMs in DUnit, then
 * you must specify the total number of VMs via the {@link #ClusterStartupRule(int)} constructor.
 */
public class ClusterStartupRule implements SerializableTestRule {
  /**
   * This is only available in each Locator/Server VM, not in the controller (test) VM.
   */
  public static MemberStarterRule<?> memberStarter;
  public static ClientCacheRule clientCacheRule;

  private boolean skipLocalDistributedSystemCleanup;

  public static InternalCache getCache() {
    if (memberStarter == null) {
      throw new RuntimeException("no cache.");
    }
    return memberStarter.getCache();
  }

  public static InternalLocator getLocator() {
    if (!(memberStarter instanceof LocatorStarterRule)) {
      throw new RuntimeException("No locator");
    }
    return ((LocatorStarterRule) memberStarter).getLocator();
  }

  public static CacheServer getServer() {
    if (!(memberStarter instanceof ServerStarterRule)) {
      throw new RuntimeException("no server");
    }
    return ((ServerStarterRule) memberStarter).getServer();
  }

  private final int vmCount;

  private final DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  private Map<Integer, VMProvider> occupiedVMs;

  private boolean logFile = false;

  public ClusterStartupRule() {
    this(NUM_VMS);
  }

  public ClusterStartupRule(final int vmCount) {
    this.vmCount = vmCount;
  }

  public static ClientCache getClientCache() {
    if (clientCacheRule == null) {
      throw new RuntimeException("no client cache");
    }
    return clientCacheRule.getCache();
  }

  /**
   * this will allow all the logs go into log files instead of going into the console output
   */
  public ClusterStartupRule withLogFile() {
    this.logFile = true;
    return this;
  }

  @Override
  public Statement apply(Statement base, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        before(description);
        try {
          base.evaluate();
        } finally {
          after(description);
        }
      }
    };
  }

  private void before(Description description) throws Throwable {
    if (isJavaVersionAtLeast(JavaVersion.JAVA_11)) {
      // GEODE-6247: JDK 11 has an issue where native code is reporting committed is 2MB > max.
      IgnoredException.addIgnoredException("committed = 538968064 should be < max = 536870912");
    }
    DUnitLauncher.launchIfNeeded(false);
    for (int i = 0; i < vmCount; i++) {
      Host.getHost(0).getVM(i);
    }
    restoreSystemProperties.beforeDistributedTest(description);
    occupiedVMs = new HashMap<>();
  }

  private void after(Description description) throws Throwable {

    if (!skipLocalDistributedSystemCleanup) {
      MemberStarterRule.disconnectDSIfAny();
    }

    // stop all the members in the order of clients, servers and locators
    List<VMProvider> vms = new ArrayList<>();
    vms.addAll(
        occupiedVMs.values().stream().filter(x -> x.isClient()).collect(Collectors.toSet()));
    vms.addAll(
        occupiedVMs.values().stream().filter(x -> x.isServer()).collect(Collectors.toSet()));
    vms.addAll(
        occupiedVMs.values().stream().filter(x -> x.isLocator()).collect(Collectors.toSet()));
    vms.forEach(x -> x.stop());

    // delete any file under root dir
    Arrays.stream(getWorkingDirRoot().listFiles()).filter(File::isFile)
        .forEach(FileUtils::deleteQuietly);

    restoreSystemProperties.afterDistributedTest(description);

    // close suspect string at the end of tear down
    // any background thread can fill the dunit_suspect.log
    // after its been truncated if we do it before closing cache
    IgnoredException.removeAllExpectedExceptions();
    closeAndCheckForSuspects();
  }


  /**
   * In some weird situations you may not want to do local DS cleanup as that lifecyle is deferred
   * elsewhere - see {@code LocatorCleanupEventListener} and any test that uses {@code
   * PlainLocatorContextLoader} or {@code LocatorWithSecurityManagerContextLoader}
   */
  public void setSkipLocalDistributedSystemCleanup(boolean skipLocalDistributedSystemCleanup) {
    this.skipLocalDistributedSystemCleanup = skipLocalDistributedSystemCleanup;
  }

  public MemberVM startLocatorVM(int index, int... locatorPort) {
    return startLocatorVM(index, x -> x.withConnectionToLocator(locatorPort));
  }

  public MemberVM startLocatorVM(int index, Properties properties, int... locatorPort) {
    return startLocatorVM(index,
        x -> x.withProperties(properties).withConnectionToLocator(locatorPort));
  }

  public MemberVM startLocatorVM(int index, int port, Properties properties, int... locatorPort) {
    return startLocatorVM(index, port, VersionManager.CURRENT_VERSION,
        x -> x.withProperties(properties).withConnectionToLocator(locatorPort));
  }

  public MemberVM startLocatorVM(int index, String version) {
    return startLocatorVM(index, 0, version, x -> x);
  }

  public MemberVM startLocatorVM(int index,
      SerializableFunction<LocatorStarterRule> ruleOperator) {
    return startLocatorVM(index, 0, VersionManager.CURRENT_VERSION, ruleOperator);
  }

  public MemberVM startLocatorVM(int index, int port, String version,
      SerializableFunction<LocatorStarterRule> ruleOperator) {
    final String defaultName = "locator-" + index;
    VM locatorVM = getVM(index, version);
    LocatorStarterRule locatorStarter = new LocatorStarterRule();
    Locator server = locatorVM.invoke("start locator in vm" + index, () -> {
      memberStarter = locatorStarter;
      if (logFile) {
        locatorStarter.withLogFile();
      }
      ruleOperator.apply(locatorStarter);
      locatorStarter.withName(defaultName);
      locatorStarter.withAutoStart();
      if (port != 0) {
        locatorStarter.withPort(port);
      }
      locatorStarter.before();
      return locatorStarter;
    });

    MemberVM memberVM = new MemberVM(server, locatorVM);
    occupiedVMs.put(index, memberVM);
    return memberVM;
  }

  public MemberVM startServerVM(int index, int... locatorPort) {
    return startServerVM(index, x -> x.withConnectionToLocator(locatorPort));
  }

  public MemberVM startServerVM(int index, String group, int... locatorPort) {
    return startServerVM(index,
        x -> x.withConnectionToLocator(locatorPort).withProperty(GROUPS, group));
  }

  public MemberVM startServerVM(int index, Properties properties, int... locatorPort) {
    return startServerVM(index,
        x -> x.withProperties(properties).withConnectionToLocator(locatorPort));
  }

  public MemberVM startServerVM(int index, SerializableFunction<ServerStarterRule> ruleOperator) {
    return startServerVM(index, VersionManager.CURRENT_VERSION, ruleOperator);
  }

  public MemberVM startServerVM(int index, String version,
      SerializableFunction<ServerStarterRule> ruleOperator) {
    final String defaultName = "server-" + index;
    VM serverVM = getVM(index, version);
    ServerStarterRule serverStarter = new ServerStarterRule();
    Server server = serverVM.invoke("startServerVM", () -> {
      memberStarter = serverStarter;
      if (logFile) {
        serverStarter.withLogFile();
      }
      ruleOperator.apply(serverStarter);
      serverStarter.withName(defaultName);
      serverStarter.withAutoStart();
      serverStarter.before();
      return serverStarter;
    });

    MemberVM memberVM = new MemberVM(server, serverVM);
    occupiedVMs.put(index, memberVM);
    return memberVM;
  }

  public ClientVM startClientVM(int index, String clientVersion,
      SerializableConsumerIF<ClientCacheRule> clientCacheRuleSetUp) throws Exception {
    VM client = getVM(index, clientVersion);
    Exception error = client.invoke(() -> {
      clientCacheRule = new ClientCacheRule();
      try {
        clientCacheRuleSetUp.accept(clientCacheRule);
        clientCacheRule.createCache();
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

  public ClientVM startClientVM(int index,
      SerializableConsumerIF<ClientCacheRule> clientCacheRuleSetUp) throws Exception {
    return startClientVM(index, VersionManager.CURRENT_VERSION, clientCacheRuleSetUp);
  }

  public ClientVM startClientVM(int index, String clientVersion, Properties properties,
      SerializableConsumerIF<ClientCacheFactory> cacheFactorySetup)
      throws Exception {
    return startClientVM(index, clientVersion,
        c -> c.withProperties(properties).withCacheSetup(cacheFactorySetup));
  }


  public ClientVM startClientVM(int index, Properties properties,
      SerializableConsumerIF<ClientCacheFactory> cacheFactorySetup) throws Exception {
    return startClientVM(index,
        c -> c.withProperties(properties).withCacheSetup(cacheFactorySetup));
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

  /**
   * gracefully stop the member/client inside this vm
   *
   * if this vm is a server/locator, it stops them and cleans the working dir if this vm is a
   * client, it closes the client cache.
   *
   * @param index vm index
   */
  public void stop(int index) {
    stop(index, true);
  }

  public void stop(int index, boolean cleanWorkingDir) {
    closeAndCheckForSuspects(index);
    occupiedVMs.get(index).stop(cleanWorkingDir);
  }

  /**
   * this crashes the VM hosting the member/client.
   */
  public void crashVM(int index) {
    VMProvider member = occupiedVMs.get(index);
    member.getVM().bounceForcibly();
  }

  public File getWorkingDirRoot() {
    // return the dunit folder
    return new File(DUnitLauncher.DUNIT_DIR);
  }

  public static void stopElementInsideVM() {
    if (memberStarter != null) {
      memberStarter.setCleanWorkingDir(false);
      memberStarter.after();
      memberStarter = null;
    }
    if (clientCacheRule != null) {
      clientCacheRule.after();
      clientCacheRule = null;
    }
  }
}
