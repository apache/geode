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
package org.apache.geode.internal.cache.wan;

import static org.apache.geode.distributed.ConfigurationProperties.DISTRIBUTED_SYSTEM_ID;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.REMOTE_LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_AUTHENTICATOR;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_PEER_AUTHENTICATOR;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_PEER_AUTH_INIT;
import static org.apache.geode.distributed.ConfigurationProperties.START_LOCATOR;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.security.Principal;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.LogWriter;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.cache.wan.GatewayReceiverFactory;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.cache.wan.GatewaySenderFactory;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.security.AuthInitialize;
import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.security.Authenticator;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.categories.WanTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;

/**
 * Gateway authentication should not throw NullPointerException.
 *
 * <p>
 * GEODE-3117: "Gateway authentication throws NullPointerException"
 */
@Category({SecurityTest.class, WanTest.class})
public class GatewayLegacyAuthenticationRegressionTest implements Serializable {

  private static final String REGION_NAME = "TheRegion";
  private static final String USER_NAME = "security-username";
  private static final String PASSWORD = "security-password";

  private static final AtomicInteger AUTHENTICATE_COUNT = new AtomicInteger();

  private VM londonLocatorVM;
  private VM newYorkLocatorVM;
  private VM londonServerVM;
  private VM newYorkServerVM;

  private String londonName;
  private String newYorkName;

  private int londonId;
  private int newYorkId;

  private int londonLocatorPort;
  private int newYorkLocatorPort;
  private int londonReceiverPort;
  private int newYorkReceiverPort;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Before
  public void before() {
    AUTHENTICATE_COUNT.set(0);

    londonLocatorVM = getVM(0);
    newYorkLocatorVM = getVM(1);
    londonServerVM = getVM(2);
    newYorkServerVM = getVM(3);

    londonName = "ln";
    newYorkName = "ny";

    londonId = 1;
    newYorkId = 2;

    int[] ports = getRandomAvailableTCPPorts(4);
    londonLocatorPort = ports[0];
    newYorkLocatorPort = ports[1];
    londonReceiverPort = ports[2];
    newYorkReceiverPort = ports[3];
  }

  /**
   * Use of SECURITY_CLIENT_AUTHENTICATOR should result in the servers performing authentication
   * during HandShake of Gateway Sender/Receiver connecting.
   */
  @Test
  public void gatewayHandShakeShouldAuthenticate() {
    londonLocatorVM.invoke("start London locator", () -> {
      Properties config = createLocatorConfig(londonId, londonLocatorPort, newYorkLocatorPort);
      cacheRule.createCache(config);
    });

    newYorkLocatorVM.invoke("start New York locator", () -> {
      Properties config = createLocatorConfig(newYorkId, newYorkLocatorPort, londonLocatorPort);
      cacheRule.createCache(config);
    });

    londonServerVM.invoke("create London server", () -> {
      assertThat(AUTHENTICATE_COUNT.get()).isEqualTo(0);
      startServer(londonId, londonLocatorPort, newYorkId, newYorkName, londonReceiverPort);
    });

    newYorkServerVM.invoke("create New York server", () -> {
      assertThat(AUTHENTICATE_COUNT.get()).isEqualTo(0);
      startServer(newYorkId, newYorkLocatorPort, londonId, londonName, newYorkReceiverPort);
    });

    londonServerVM.invoke(() -> {
      GatewaySender sender = cacheRule.getCache().getGatewaySender(newYorkName);
      await().untilAsserted(() -> assertThat(isRunning(sender)).isTrue());
    });

    newYorkServerVM.invoke(() -> {
      GatewaySender sender = cacheRule.getCache().getGatewaySender(londonName);
      await().untilAsserted(() -> assertThat(isRunning(sender)).isTrue());
    });

    newYorkServerVM.invoke(() -> {
      Region<Integer, Integer> region = cacheRule.getCache().getRegion(REGION_NAME);
      assertThat(region).isNotNull();
      assertThat(region.isEmpty()).isTrue();
    });

    londonServerVM.invoke(() -> {
      Region<Integer, Integer> region = cacheRule.getCache().getRegion(REGION_NAME);
      region.put(0, 0);
    });

    newYorkServerVM.invoke(() -> {
      Region<Integer, Integer> region = cacheRule.getCache().getRegion(REGION_NAME);
      assertThat(region).isNotNull();
      await()
          .untilAsserted(() -> assertThat(region.isEmpty()).isFalse());
    });

    newYorkLocatorVM.invoke(() -> {
      assertThat(AUTHENTICATE_COUNT.get()).isEqualTo(0);
    });
    londonLocatorVM.invoke(() -> {
      assertThat(AUTHENTICATE_COUNT.get()).isEqualTo(0);
    });
    newYorkServerVM.invoke(() -> {
      assertThat(AUTHENTICATE_COUNT.get()).isGreaterThanOrEqualTo(1);
    });
    londonServerVM.invoke(() -> {
      assertThat(AUTHENTICATE_COUNT.get()).isGreaterThanOrEqualTo(1);
    });
  }

  private boolean isRunning(GatewaySender sender) {
    return sender != null && sender.isRunning();
  }

  private Properties createLocatorConfig(int systemId, int locatorPort, int remoteLocatorPort) {
    Properties config = new Properties();
    config.setProperty(MCAST_PORT, "0");
    config.setProperty(DISTRIBUTED_SYSTEM_ID, String.valueOf(systemId));
    config.setProperty(LOCATORS, "localhost[" + locatorPort + ']');
    config.setProperty(REMOTE_LOCATORS, "localhost[" + remoteLocatorPort + ']');
    config.setProperty(START_LOCATOR,
        "localhost[" + locatorPort + "],server=true,peer=true,hostname-for-clients=localhost");
    config.setProperty(SECURITY_PEER_AUTH_INIT, TestPeerAuthInitialize.class.getName() + ".create");
    config.setProperty(SECURITY_PEER_AUTHENTICATOR,
        TestPeerAuthenticator.class.getName() + ".create");
    config.setProperty(USER_NAME, "user");
    config.setProperty(PASSWORD, "user");
    return config;
  }

  private Properties createServerConfig(int locatorPort) {
    Properties config = new Properties();
    config.setProperty(MCAST_PORT, "0");
    config.setProperty(LOCATORS, "localhost[" + locatorPort + ']');
    config.setProperty(SECURITY_PEER_AUTH_INIT, TestPeerAuthInitialize.class.getName() + ".create");
    config.setProperty(SECURITY_PEER_AUTHENTICATOR,
        TestPeerAuthenticator.class.getName() + ".create");
    config.setProperty(SECURITY_CLIENT_AUTHENTICATOR,
        TestClientOrReceiverAuthenticator.class.getName() + ".create");
    config.setProperty(USER_NAME, "user");
    config.setProperty(PASSWORD, "user");
    return config;
  }

  private void startServer(int systemId, int locatorPort, int remoteSystemId, String remoteName,
      int receiverPort) throws IOException {
    cacheRule.createCache(createServerConfig(locatorPort));

    String uniqueName = "server-" + systemId;
    File[] dirs = new File[] {temporaryFolder.newFolder(uniqueName)};

    GatewaySenderFactory senderFactory = createGatewaySenderFactory(dirs, uniqueName);
    GatewaySender sender = senderFactory.create(remoteName, remoteSystemId);
    sender.start();

    GatewayReceiverFactory receiverFactory = createGatewayReceiverFactory(receiverPort);
    GatewayReceiver receiver = receiverFactory.create();
    receiver.start();

    RegionFactory<Integer, Integer> regionFactory =
        cacheRule.getCache().createRegionFactory(RegionShortcut.REPLICATE);
    regionFactory.addGatewaySenderId(remoteName);

    regionFactory.create(REGION_NAME);
  }

  private GatewayReceiverFactory createGatewayReceiverFactory(int receiverPort) {
    GatewayReceiverFactory receiverFactory = cacheRule.getCache().createGatewayReceiverFactory();

    receiverFactory.setStartPort(receiverPort);
    receiverFactory.setEndPort(receiverPort);
    receiverFactory.setManualStart(true);
    return receiverFactory;
  }

  private GatewaySenderFactory createGatewaySenderFactory(File[] dirs, String diskStoreName) {
    InternalGatewaySenderFactory senderFactory =
        (InternalGatewaySenderFactory) cacheRule.getCache().createGatewaySenderFactory();

    senderFactory.setMaximumQueueMemory(100);
    senderFactory.setBatchSize(10);
    senderFactory.setBatchConflationEnabled(false);
    senderFactory.setManualStart(true);
    senderFactory.setDispatcherThreads(1);
    senderFactory.setOrderPolicy(GatewaySender.DEFAULT_ORDER_POLICY);

    DiskStoreFactory dsf = cacheRule.getCache().createDiskStoreFactory();
    DiskStore store = dsf.setDiskDirs(dirs).create(diskStoreName);
    senderFactory.setDiskStoreName(store.getName());

    return senderFactory;
  }

  private static class TestPrincipal implements Principal, Serializable {

    private final String userName;
    private final UUID uuid;

    TestPrincipal(String userName) {
      this.userName = userName;
      uuid = UUID.randomUUID();
    }

    @Override
    public String getName() {
      return userName;
    }

    @Override
    public String toString() {
      return userName + "->" + uuid;
    }
  }

  public static class TestPeerAuthenticator implements Authenticator {

    public static Authenticator create() {
      return new TestPeerAuthenticator();
    }

    @Override
    public void init(Properties securityProps, LogWriter systemLogger, LogWriter securityLogger)
        throws AuthenticationFailedException {
      // nothing
    }

    @Override
    public Principal authenticate(Properties props, DistributedMember member)
        throws AuthenticationFailedException {
      System.out
          .println(Thread.currentThread().getName() + ": TestPeerAuthenticator authenticating "
              + member + " at " + System.currentTimeMillis());

      // Get the user name and password
      String userName = props.getProperty(USER_NAME);
      String password = props.getProperty(PASSWORD);

      // If they are not equal, throw an exception
      if (!userName.equals(password)) {
        String msg = "Invalid user name and password combination supplied for user " + userName;
        throw new AuthenticationFailedException(msg);
      }
      return new TestPrincipal(userName);
    }
  }

  public static class TestPeerAuthInitialize implements AuthInitialize {

    public static AuthInitialize create() {
      return new TestPeerAuthInitialize();
    }

    @Override
    public void init(LogWriter systemLogger, LogWriter securityLogger)
        throws AuthenticationFailedException {
      // nothing
    }

    @Override
    public Properties getCredentials(Properties securityProps, DistributedMember server,
        boolean isPeer) throws AuthenticationFailedException {
      String userName = securityProps.getProperty(USER_NAME);
      if (userName == null) {
        throw new AuthenticationFailedException(
            "TestPeerAuthInitialize: The user name property [" + USER_NAME + "] not set");
      }

      Properties newProps = new Properties();
      newProps.setProperty(USER_NAME, userName);

      String passwd = securityProps.getProperty(PASSWORD);
      if (passwd == null) {
        throw new AuthenticationFailedException(
            "TestPeerAuthInitialize: The password property [" + PASSWORD + "] not set");
      }
      newProps.setProperty(PASSWORD, passwd);

      System.out.println(Thread.currentThread().getName()
          + ": TestPeerAuthInitialize providing credentials for " + (isPeer ? "peer " : "client ")
          + server + ": " + newProps + " at " + System.currentTimeMillis());

      return newProps;
    }
  }

  public static class TestClientOrReceiverAuthenticator implements Authenticator {

    public static Authenticator create() {
      return new TestClientOrReceiverAuthenticator();
    }

    @Override
    public void init(Properties securityProps, LogWriter systemLogger, LogWriter securityLogger)
        throws AuthenticationFailedException {
      // nothing
    }

    @Override
    public Principal authenticate(Properties props, DistributedMember member)
        throws AuthenticationFailedException {
      AUTHENTICATE_COUNT.incrementAndGet();

      System.out.println(
          Thread.currentThread().getName() + ": TestClientOrReceiverAuthenticator authenticating "
              + member + " at " + System.currentTimeMillis());

      // Get the user name and password
      String userName = props.getProperty(USER_NAME);
      String password = props.getProperty(PASSWORD);

      // If they are not equal, throw an exception
      if (!userName.equals(password)) {
        String msg = "Invalid user name and password combination supplied for user " + userName;
        throw new AuthenticationFailedException(msg);
      }
      return new TestPrincipal(userName);
    }
  }
}
