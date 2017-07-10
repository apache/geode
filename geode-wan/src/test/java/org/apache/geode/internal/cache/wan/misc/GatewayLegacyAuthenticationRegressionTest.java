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
package org.apache.geode.internal.cache.wan.misc;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.geode.distributed.ConfigurationProperties.DISTRIBUTED_SYSTEM_ID;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.REMOTE_LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_AUTHENTICATOR;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_PEER_AUTHENTICATOR;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_PEER_AUTH_INIT;
import static org.apache.geode.distributed.ConfigurationProperties.START_LOCATOR;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Awaitility.waitAtMost;

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
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.cache.wan.GatewayReceiverFactory;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.cache.wan.GatewaySenderFactory;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.wan.InternalGatewaySenderFactory;
import org.apache.geode.security.AuthInitialize;
import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.security.Authenticator;
import org.apache.geode.test.dunit.DistributedTestCase;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.categories.WanTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;

/**
 * Reproduces bug GEODE-3117: "Gateway authentication throws NullPointerException" and validates the
 * fix.
 */
@Category({DistributedTest.class, SecurityTest.class, WanTest.class})
public class GatewayLegacyAuthenticationRegressionTest extends DistributedTestCase {

  private static final String USER_NAME = "security-username";
  private static final String PASSWORD = "security-password";

  private static final AtomicInteger invokeAuthenticateCount = new AtomicInteger();

  private static Cache cache;
  private static GatewaySender sender;

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

  private String regionName;

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Before
  public void before() {
    invokeAuthenticateCount.set(0);

    this.londonLocatorVM = getHost(0).getVM(0);
    this.newYorkLocatorVM = getHost(0).getVM(1);
    this.londonServerVM = getHost(0).getVM(2);
    this.newYorkServerVM = getHost(0).getVM(3);

    this.londonName = "ln";
    this.newYorkName = "ny";

    this.londonId = 1;
    this.newYorkId = 2;

    this.regionName = getTestMethodName() + "_RR";

    int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    this.londonLocatorPort = ports[0];
    this.newYorkLocatorPort = ports[1];
  }

  /**
   * Use of SECURITY_CLIENT_AUTHENTICATOR should result in the servers performing authentication
   * during HandShake of Gateway Sender/Receiver connecting.
   */
  @Test
  public void gatewayHandShakeShouldAuthenticate() {
    this.londonLocatorVM.invoke("start London locator", () -> {
      getSystem(
          createLocatorConfig(this.londonId, this.londonLocatorPort, this.newYorkLocatorPort));
    });

    this.newYorkLocatorVM.invoke("start New York locator", () -> {
      getSystem(
          createLocatorConfig(this.newYorkId, this.newYorkLocatorPort, this.londonLocatorPort));
    });

    this.londonServerVM.invoke("create London server", () -> {
      assertThat(invokeAuthenticateCount.get()).isEqualTo(0);
      startServer(this.londonId, this.londonLocatorPort, this.newYorkId, this.newYorkName);
    });

    this.newYorkServerVM.invoke("create New York server", () -> {
      assertThat(invokeAuthenticateCount.get()).isEqualTo(0);
      startServer(this.newYorkId, this.newYorkLocatorPort, this.londonId, this.londonName);
    });

    this.londonServerVM.invoke(() -> {
      await().atMost(1, MINUTES).until(() -> assertThat(isRunning(sender)).isTrue());
    });

    this.newYorkServerVM.invoke(() -> {
      await().atMost(1, MINUTES).until(() -> assertThat(isRunning(sender)).isTrue());
    });

    this.newYorkServerVM.invoke(() -> {
      Region region = cache.getRegion(Region.SEPARATOR + this.regionName);
      assertThat(region).isNotNull();
      assertThat(region.isEmpty()).isTrue();
    });

    this.londonServerVM.invoke(() -> {
      Region region = cache.getRegion(Region.SEPARATOR + this.regionName);
      region.put(0, 0);
    });

    this.newYorkServerVM.invoke(() -> {
      Region region = cache.getRegion(Region.SEPARATOR + this.regionName);
      assertThat(region).isNotNull();
      waitAtMost(1, MINUTES).until(() -> assertThat(region.isEmpty()).isFalse());
    });

    this.newYorkLocatorVM.invoke(() -> {
      assertThat(invokeAuthenticateCount.get()).isEqualTo(0);
    });
    this.londonLocatorVM.invoke(() -> {
      assertThat(invokeAuthenticateCount.get()).isEqualTo(0);
    });
    this.newYorkServerVM.invoke(() -> {
      assertThat(invokeAuthenticateCount.get()).isGreaterThanOrEqualTo(1);
    });
    this.londonServerVM.invoke(() -> {
      assertThat(invokeAuthenticateCount.get()).isGreaterThanOrEqualTo(1);
    });
  }

  private boolean isRunning(GatewaySender sender) {
    return sender != null && sender.isRunning();
  }

  private Properties createLocatorConfig(int systemId, int locatorPort, int remoteLocatorPort) {
    Properties config = getDistributedSystemProperties();
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

  private void startServer(int systemId, int locatorPort, int remoteSystemId, String remoteName)
      throws IOException {
    DistributedSystem system = getSystem(createServerConfig(locatorPort));
    cache = CacheFactory.create(system);

    String uniqueName = "server-" + systemId;
    File[] dirs = new File[] {this.temporaryFolder.newFolder(uniqueName)};

    GatewaySenderFactory senderFactory = createGatewaySenderFactory(dirs, uniqueName);
    sender = senderFactory.create(remoteName, remoteSystemId);
    sender.start();

    GatewayReceiverFactory receiverFactory = createGatewayReceiverFactory();
    GatewayReceiver receiver = receiverFactory.create();
    receiver.start();

    AttributesFactory attributesFactory = new AttributesFactory();
    attributesFactory.addGatewaySenderId(remoteName);
    attributesFactory.setDataPolicy(DataPolicy.REPLICATE);
    attributesFactory.setScope(Scope.DISTRIBUTED_ACK);
    cache.createRegionFactory(attributesFactory.create()).create(this.regionName);
  }

  private GatewayReceiverFactory createGatewayReceiverFactory() {
    GatewayReceiverFactory receiverFactory = cache.createGatewayReceiverFactory();

    int port = AvailablePortHelper.getRandomAvailableTCPPort();

    receiverFactory.setStartPort(port);
    receiverFactory.setEndPort(port);
    receiverFactory.setManualStart(true);
    return receiverFactory;
  }

  private GatewaySenderFactory createGatewaySenderFactory(File[] dirs, String diskStoreName) {
    InternalGatewaySenderFactory senderFactory =
        (InternalGatewaySenderFactory) cache.createGatewaySenderFactory();

    senderFactory.setMaximumQueueMemory(100);
    senderFactory.setBatchSize(10);
    senderFactory.setBatchConflationEnabled(false);
    senderFactory.setManualStart(true);
    senderFactory.setDispatcherThreads(1);
    senderFactory.setOrderPolicy(GatewaySender.DEFAULT_ORDER_POLICY);

    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    DiskStore store = dsf.setDiskDirs(dirs).create(diskStoreName);
    senderFactory.setDiskStoreName(store.getName());

    return senderFactory;
  }

  static class TestPrincipal implements Principal, Serializable {

    private final String userName;
    private final UUID uuid;

    TestPrincipal(String userName) {
      this.userName = userName;
      this.uuid = UUID.randomUUID();
    }

    @Override
    public String getName() {
      return this.userName;
    }

    public UUID getUUID() {
      return this.uuid;
    }

    @Override
    public String toString() {
      return this.userName + "->" + this.uuid;
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

    @Override
    public void close() {
      // nothing
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

    @Override
    public void close() {
      // nothing
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
      invokeAuthenticateCount.incrementAndGet();

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

    @Override
    public void close() {
      // nothing
    }
  }

}
