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
package org.apache.geode;

import static org.apache.geode.distributed.ConfigurationProperties.CONSERVE_SOCKETS;
import static org.apache.geode.distributed.ConfigurationProperties.DISABLE_TCP;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.distributed.ConfigurationProperties.SOCKET_BUFFER_SIZE;
import static org.apache.geode.distributed.ConfigurationProperties.SOCKET_LEASE_TIME;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENABLED_COMPONENTS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_REQUIRE_AUTHENTICATION;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.USE_CLUSTER_CONFIGURATION;
import static org.apache.geode.internal.serialization.DataSerializableFixedID.SERIAL_ACKED_MESSAGE;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.awaitility.GeodeAwaitility.getTimeout;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.util.ResourceUtils.createTempFileFromResource;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DMStats;
import org.apache.geode.distributed.internal.DirectReplyProcessor;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.MessageWithReply;
import org.apache.geode.distributed.internal.OperationExecutors;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.SerialAckedMessage;
import org.apache.geode.distributed.internal.membership.gms.membership.GMSJoinLeave;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.DirectReplyMessage;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.BackwardCompatibilityTest;
import org.apache.geode.test.junit.categories.MembershipTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;
import org.apache.geode.test.version.VersionManager;

/**
 * This class tests cluster tcp/ip communications both with and without SSL enabled
 */
@Category({MembershipTest.class, BackwardCompatibilityTest.class})
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class ClusterCommunicationsDUnitTest implements Serializable {

  private static final int NUM_SERVERS = 2;
  private static final int SMALL_BUFFER_SIZE = 8000;

  private static final long serialVersionUID = -3438183140385150550L;

  private static Cache cache;

  private final String regionName = "clusterTestRegion";

  private final boolean disableTcp;
  private boolean conserveSockets;
  private boolean useSSL;

  @Parameters(name = "{0}")
  public static Collection<RunConfiguration> data() {
    return Arrays.asList(RunConfiguration.values());
  }

  @Rule
  public DistributedRule distributedRule =
      DistributedRule.builder().withVMCount(NUM_SERVERS + 1).build();

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  public ClusterCommunicationsDUnitTest(RunConfiguration runConfiguration) {
    useSSL = runConfiguration.useSSL;
    conserveSockets = runConfiguration.conserveSockets;
    disableTcp = runConfiguration.disableTcp;
  }

  @Before
  public void setUp() throws Exception {
    addIgnoredException("Socket Closed");
    addIgnoredException("Remote host closed connection during handshake");
  }

  @Test
  public void createEntryAndVerifyUpdate() {
    int locatorPort = createLocator(getVM(0));
    for (int i = 1; i <= NUM_SERVERS; i++) {
      createCacheAndRegion(getVM(i), locatorPort);
    }
    performCreate(getVM(1));
    for (int i = 1; i <= NUM_SERVERS; i++) {
      verifyCreatedEntry(getVM(i));
    }
    for (int iteration = 1; iteration < 6; iteration++) {
      performUpdate(getVM(1));
    }
    for (int i = 1; i <= NUM_SERVERS; i++) {
      verifyUpdatedEntry(getVM(i));
    }
  }

  @Test
  public void createEntryWithBigMessage() {
    int locatorPort = createLocator(getVM(0));
    for (int i = 1; i <= NUM_SERVERS; i++) {
      createCacheAndRegion(getVM(i), locatorPort);
    }
    performCreateWithLargeValue(getVM(1));
    // fault the value into an empty cache - forces use of message chunking
    for (int i = 1; i <= NUM_SERVERS - 1; i++) {
      verifyCreatedEntry(getVM(i));
    }
  }

  @Test
  public void receiveBigResponse() {
    invokeInEveryVM(
        () -> InternalDataSerializer.getDSFIDSerializer().registerDSFID(SERIAL_ACKED_MESSAGE,
            SerialAckedMessageWithBigReply.class));
    try {
      int locatorPort = createLocator(getVM(0));
      for (int i = 1; i <= NUM_SERVERS; i++) {
        createCacheAndRegion(getVM(i), locatorPort);
      }
      DistributedMember vm2ID =
          getVM(2).invoke(() -> cache.getDistributedSystem().getDistributedMember());
      getVM(1).invoke("receive a large direct-reply message", () -> {
        SerialAckedMessageWithBigReply messageWithBigReply = new SerialAckedMessageWithBigReply();
        await().until(() -> {
          messageWithBigReply.send(Collections.singleton(vm2ID));
          return true;
        });
      });
    } finally {
      invokeInEveryVM(
          () -> InternalDataSerializer.getDSFIDSerializer().registerDSFID(SERIAL_ACKED_MESSAGE,
              SerialAckedMessage.class));
    }
  }

  @Test
  public void performARollingUpgrade() {
    List<String> testVersions = VersionManager.getInstance().getVersionsWithoutCurrent();
    Collections.sort(testVersions);
    String testVersion = testVersions.get(testVersions.size() - 1);

    // create a cluster with the previous version of Geode
    VM locatorVM = Host.getHost(0).getVM(testVersion, 0);
    VM server1VM = Host.getHost(0).getVM(testVersion, 1);
    int locatorPort = createLocator(locatorVM, true);
    createCacheAndRegion(server1VM, locatorPort);
    performCreate(getVM(1));

    // roll the locator to the current version
    locatorVM.invoke("stop locator", () -> Locator.getLocator().stop());
    locatorVM = Host.getHost(0).getVM(VersionManager.CURRENT_VERSION, 0);
    locatorVM.invoke("roll locator to current version", () -> {
      // if you need to debug SSL communications use this property:
      // System.setProperty("javax.net.debug", "all");
      Properties props = getDistributedSystemProperties();
      // locator must restart with the same port so that it reconnects to the server
      await().atMost(getTimeout().getValueInMS(), TimeUnit.MILLISECONDS)
          .until(() -> Locator.startLocatorAndDS(locatorPort, new File(""), props) != null);
      assertThat(Locator.getLocator().getDistributedSystem().getAllOtherMembers().size())
          .isGreaterThan(0);
    });

    // start server2 with current version
    VM server2VM = Host.getHost(0).getVM(VersionManager.CURRENT_VERSION, 2);
    createCacheAndRegion(server2VM, locatorPort);

    // roll server1 to the current version
    server1VM.invoke("stop server1", () -> cache.close());
    server1VM = Host.getHost(0).getVM(VersionManager.CURRENT_VERSION, 1);
    createCacheAndRegion(server1VM, locatorPort);

    verifyCreatedEntry(server1VM);
    verifyCreatedEntry(server2VM);
  }

  private void createCacheAndRegion(VM memberVM, int locatorPort) {
    memberVM.invoke("start cache and create region", () -> {
      cache = createCache(locatorPort);
      cache.createRegionFactory(RegionShortcut.REPLICATE).create(regionName);
    });
  }

  private void performCreate(VM memberVM) {
    memberVM.invoke("perform create", () -> cache
        .getRegion(regionName).put("testKey", "testValue"));
  }

  private void performUpdate(VM memberVM) {
    memberVM.invoke("perform update", () -> {
      DMStats stats = ((InternalDistributedSystem) cache.getDistributedSystem())
          .getDistributionManager().getStats();
      int reconnectAttempts = stats.getReconnectAttempts();
      cache.getRegion(regionName).put("testKey", "updatedTestValue");
      assertThat(stats.getReconnectAttempts()).isEqualTo(reconnectAttempts);
    });
  }

  private void performCreateWithLargeValue(VM memberVM) {
    memberVM.invoke("perform create", () -> {
      byte[] value = new byte[SMALL_BUFFER_SIZE * 20];
      Arrays.fill(value, (byte) 1);
      cache.getRegion(regionName).put("testKey", value);
    });
  }

  private void verifyCreatedEntry(VM memberVM) {
    memberVM.invoke("verify entry created", () -> Assert.assertTrue(cache
        .getRegion(regionName).containsKey("testKey")));
  }

  private void verifyUpdatedEntry(VM memberVM) {
    memberVM.invoke("verify entry updated", () -> Assert.assertTrue(cache
        .getRegion(regionName).containsValue("updatedTestValue")));
  }

  private int createLocator(VM memberVM) {
    return createLocator(memberVM, false);
  }

  private int createLocator(VM memberVM, boolean usingOldVersion) {
    return memberVM.invoke("create locator", () -> {
      // if you need to debug SSL communications use this property:
      // System.setProperty("javax.net.debug", "all");
      System.setProperty(GMSJoinLeave.BYPASS_DISCOVERY_PROPERTY, "true");
      Properties dsProperties = getDistributedSystemProperties();
      try {
        int port = 0;
        // for stress-tests make sure that an older-version locator doesn't try
        // to read state persisted by another run's newer-version locator
        if (usingOldVersion) {
          port = AvailablePortHelper.getRandomAvailableTCPPort();
          DistributedTestUtils.deleteLocatorStateFile(port);
        }
        return Locator.startLocatorAndDS(port, new File(""), getDistributedSystemProperties())
            .getPort();
      } finally {
        System.clearProperty(GMSJoinLeave.BYPASS_DISCOVERY_PROPERTY);
      }
    });
  }

  private Cache createCache(int locatorPort) {
    // if you need to debug SSL communications use this property:
    // System.setProperty("javax.net.debug", "all");
    Properties properties = getDistributedSystemProperties();
    properties.setProperty(LOCATORS, "localhost[" + locatorPort + "]");
    return new CacheFactory(properties).create();
  }

  public Properties getDistributedSystemProperties() {
    Properties properties = new Properties();
    properties.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");
    properties.setProperty(USE_CLUSTER_CONFIGURATION, "false");
    properties.setProperty(NAME, "vm" + VM.getCurrentVMNum());
    properties.setProperty(CONSERVE_SOCKETS, "" + conserveSockets);
    properties.setProperty(DISABLE_TCP, "" + disableTcp);
    properties.setProperty(SOCKET_LEASE_TIME, "10000");
    properties.setProperty(SOCKET_BUFFER_SIZE, "" + SMALL_BUFFER_SIZE);

    if (useSSL) {
      properties.setProperty(SSL_ENABLED_COMPONENTS, "cluster,locator");
      properties
          .setProperty(SSL_KEYSTORE, createTempFileFromResource(getClass(), "server.keystore")
              .getAbsolutePath());
      properties.setProperty(SSL_TRUSTSTORE,
          createTempFileFromResource(getClass(), "server.keystore")
              .getAbsolutePath());
      properties.setProperty(SSL_PROTOCOLS, "TLSv1.2");
      properties.setProperty(SSL_KEYSTORE_PASSWORD, "password");
      properties.setProperty(SSL_TRUSTSTORE_PASSWORD, "password");
      properties.setProperty(SSL_REQUIRE_AUTHENTICATION, "true");
    }
    return properties;
  }

  enum RunConfiguration {
    SHARED_CONNECTIONS(true, false, false),
    SHARED_CONNECTIONS_WITH_SSL(true, true, false),
    UNSHARED_CONNECTIONS(false, false, false),
    UNSHARED_CONNECTIONS_WITH_SSL(false, true, false),
    UDP_CONNECTIONS(true, false, true);

    boolean useSSL;
    boolean conserveSockets;
    boolean disableTcp;

    RunConfiguration(boolean conserveSockets, boolean useSSL, boolean disableTcp) {
      this.useSSL = useSSL;
      this.conserveSockets = conserveSockets;
      this.disableTcp = disableTcp;
    }
  }

  /**
   * SerialAckedMessageWithBigReply requires conserve-sockets=false and acts to send
   * a large reply message to the sender. You must have already created a cache in the
   * sender and receiver VMs and registered this class with the DataSerializableFixedID
   * of SERIAL_ACKED_MESSAGE. Don't forget to reset the registration to
   * SerialAckedMessage at the end of the test.
   */
  private static class SerialAckedMessageWithBigReply extends DistributionMessage
      implements MessageWithReply, DirectReplyMessage {
    static final int DSFID = SERIAL_ACKED_MESSAGE;

    private int processorId;
    private ClusterDistributionManager originDm;
    private DirectReplyProcessor replyProcessor;

    public SerialAckedMessageWithBigReply() {
      InternalDistributedSystem ds = InternalDistributedSystem.getAnyInstance();
      // this constructor is used in serialization as well as when sending to others
      if (ds != null) {
        originDm = (ClusterDistributionManager) ds.getDistributionManager();
      }
    }

    public void send(Set<DistributedMember> recipients)
        throws InterruptedException, ReplyException {
      // this message is only used by battery tests so we can log info level debug
      // messages
      replyProcessor = new DirectReplyProcessor(originDm, recipients);
      processorId = replyProcessor.getProcessorId();
      setRecipients(recipients);
      Set failures = originDm.putOutgoing(this);
      if (failures != null && !failures.isEmpty()) {
        for (Object failure : failures) {
          System.err.println("Unable to send serial acked message to " + failure);
        }
      }

      replyProcessor.waitForReplies();
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      out.writeInt(processorId);
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      processorId = in.readInt();
    }

    @Override
    protected void process(ClusterDistributionManager dm) {
      ReplyMessage reply = new ReplyMessage();
      reply.setProcessorId(processorId);
      reply.setRecipient(getSender());
      byte[] returnValue = new byte[SMALL_BUFFER_SIZE * 6];
      reply.setReturnValue(returnValue);
      System.out.println("<" + Thread.currentThread().getName() +
          "> sending reply with return value size "
          + returnValue.length + " using " + getReplySender(dm));
      getReplySender(dm).putOutgoing(reply);
    }

    @Override
    public int getProcessorId() {
      return processorId;
    }

    @Override
    public int getProcessorType() {
      return OperationExecutors.SERIAL_EXECUTOR;
    }

    @Override
    public int getDSFID() {
      return DSFID;
    }

    @Override
    public DirectReplyProcessor getDirectReplyProcessor() {
      return replyProcessor;
    }

    @Override
    public boolean supportsDirectAck() {
      return processorId == 0;
    }

    @Override
    public void registerProcessor() {
      if (replyProcessor != null) {
        processorId = replyProcessor.register();
      }
    }
  }
}
