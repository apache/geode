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
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_AUTH_INIT;
import static org.apache.geode.test.dunit.Host.getHost;

import java.io.File;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

import org.apache.commons.io.FileUtils;
import org.junit.rules.ExternalResource;

import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.internal.InternalLocator;
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

  private Map<Integer, VMProvider> occupiedVMs;

  private boolean logFile = false;

  public ClusterStartupRule() {
    DUnitLauncher.launchIfNeeded();
  }

  public static ClientCache getClientCache() {
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
  protected void before() throws Throwable {
    restoreSystemProperties.before();
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

      // delete any file under root dir
      Arrays.stream(getWorkingDirRoot().listFiles()).filter(File::isFile)
          .forEach(FileUtils::deleteQuietly);

      restoreSystemProperties.after();
    }
  }

  public MemberVM startLocatorVM(int index, int... locatorPort) {
    return startLocatorVM(index, x -> x.withConnectionToLocator(locatorPort));
  }

  public MemberVM startLocatorVM(int index, Properties properties, int... locatorPort) {
    return startLocatorVM(index,
        x -> x.withProperties(properties).withConnectionToLocator(locatorPort));
  }

  public MemberVM startLocatorVM(int index, String version) {
    return startLocatorVM(index, version, x -> x);
  }

  public MemberVM startLocatorVM(int index,
      SerializableFunction1<LocatorStarterRule> ruleOperator) {
    return startLocatorVM(index, VersionManager.CURRENT_VERSION, ruleOperator);
  }

  public MemberVM startLocatorVM(int index, String version,
      SerializableFunction1<LocatorStarterRule> ruleOperator) {
    final String defaultName = "locator-" + index;
    VM locatorVM = getVM(index, version);
    Locator server = locatorVM.invoke(() -> {
      memberStarter = new LocatorStarterRule();
      LocatorStarterRule locatorStarter = (LocatorStarterRule) memberStarter;
      if (logFile) {
        locatorStarter.withLogFile();
      }
      ruleOperator.apply(locatorStarter);
      locatorStarter.withName(defaultName);
      locatorStarter.withAutoStart();
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

  public MemberVM startServerVM(int index, SerializableFunction1<ServerStarterRule> ruleOperator) {
    return startServerVM(index, VersionManager.CURRENT_VERSION, ruleOperator);
  }

  public MemberVM startServerVM(int index, String version,
      SerializableFunction1<ServerStarterRule> ruleOperator) {
    final String defaultName = "server-" + index;
    VM serverVM = getVM(index, version);
    Server server = serverVM.invoke(() -> {
      memberStarter = new ServerStarterRule();
      ServerStarterRule serverStarter = (ServerStarterRule) memberStarter;
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

  public File getWorkingDirRoot() {
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

  public interface SerializableFunction1<T> extends UnaryOperator<T>, Serializable {
  }
}
