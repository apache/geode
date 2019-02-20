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
import static org.apache.geode.internal.DataSerializableFixedID.SERIAL_ACKED_MESSAGE;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.awaitility.GeodeAwaitility.getTimeout;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
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

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DirectReplyProcessor;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.MessageWithReply;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.SerialAckedMessage;
import org.apache.geode.distributed.internal.membership.gms.membership.GMSJoinLeave;
import org.apache.geode.internal.DSFIDFactory;
import org.apache.geode.internal.cache.DirectReplyMessage;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.BackwardCompatibilityTest;
import org.apache.geode.test.junit.categories.MembershipTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;
import org.apache.geode.test.version.VersionManager;
import org.apache.geode.util.test.TestUtil;


/**
 * This class tests cluster tcp/ip communications both with and without SSL enabled
 */
@Category({MembershipTest.class, BackwardCompatibilityTest.class})
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class ClusterCommunicationsDUnitTest implements java.io.Serializable {

  private boolean conserveSockets;
  private boolean useSSL;

  enum RunConfiguration {
    SHARED_CONNECTIONS(true, false),
    SHARED_CONNECTIONS_WITH_SSL(true, true),
    UNSHARED_CONNECTIONS(false, false),
    UNSHARED_CONNECTIONS_WITH_SSL(false, true);

    boolean useSSL;
    boolean conserveSockets;

    RunConfiguration(boolean conserveSockets, boolean useSSL) {
      this.useSSL = useSSL;
      this.conserveSockets = conserveSockets;
    }
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<RunConfiguration> data() {
    return Arrays.asList(RunConfiguration.values());
  }

  private static final int NUM_SERVERS = 2;
  private static final int SMALL_BUFFER_SIZE = 8000;

  private static final long serialVersionUID = -3438183140385150550L;

  private static Cache cache;

  @Rule
  public DistributedRule distributedRule =
      DistributedRule.builder().withVMCount(NUM_SERVERS + 1).build();

  @Rule
  public final SerializableTestName testName = new SerializableTestName();

  final String regionName = "clusterTestRegion";

  public ClusterCommunicationsDUnitTest(RunConfiguration runConfiguration) {
    this.useSSL = runConfiguration.useSSL;
    this.conserveSockets = runConfiguration.conserveSockets;
  }

  @Before
  public void setUp() throws Exception {
    final Boolean testWithSSL = useSSL;
    final Boolean testWithConserveSocketsTrue = conserveSockets;
    Invoke.invokeInEveryVM(() -> {
      this.useSSL = testWithSSL;
      this.conserveSockets = testWithConserveSocketsTrue;
    });
  }

  @Test
  public void createEntryAndVerifyUpdate() {
    int locatorPort = createLocator(VM.getVM(0));
    for (int i = 1; i <= NUM_SERVERS; i++) {
      createCacheAndRegion(VM.getVM(i), locatorPort);
    }
    performCreate(VM.getVM(1));
    for (int i = 1; i <= NUM_SERVERS; i++) {
      verifyCreatedEntry(VM.getVM(i));
    }
    performUpdate(VM.getVM(1));
    for (int i = 1; i <= NUM_SERVERS; i++) {
      verifyUpdatedEntry(VM.getVM(i));
    }
  }

  @Test
  public void createEntryWithBigMessage() {
    int locatorPort = createLocator(VM.getVM(0));
    for (int i = 1; i <= NUM_SERVERS; i++) {
      createCacheAndRegion(VM.getVM(i), locatorPort);
    }
    performCreateWithLargeValue(VM.getVM(1));
    // fault the value into an empty cache - forces use of message chunking
    for (int i = 1; i <= NUM_SERVERS - 1; i++) {
      verifyCreatedEntry(VM.getVM(i));
    }
  }

  @Test
  public void receiveBigResponse() {
    Invoke.invokeInEveryVM(() -> DSFIDFactory.registerDSFID(SERIAL_ACKED_MESSAGE,
        SerialAckedMessageWithBigReply.class));
    try {
      int locatorPort = createLocator(VM.getVM(0));
      for (int i = 1; i <= NUM_SERVERS; i++) {
        createCacheAndRegion(VM.getVM(i), locatorPort);
      }
      final DistributedMember vm2ID =
          VM.getVM(2).invoke(() -> cache.getDistributedSystem().getDistributedMember());
      VM.getVM(1).invoke("receive a large direct-reply message", () -> {
        SerialAckedMessageWithBigReply messageWithBigReply = new SerialAckedMessageWithBigReply();
        await().until(() -> {
          messageWithBigReply.send(Collections.<DistributedMember>singleton(vm2ID));
          return true;
        });
      });
    } finally {
      Invoke.invokeInEveryVM(
          () -> DSFIDFactory.registerDSFID(SERIAL_ACKED_MESSAGE, SerialAckedMessage.class));
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
    int locatorPort = createLocator(locatorVM);
    createCacheAndRegion(server1VM, locatorPort);
    performCreate(VM.getVM(1));

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
    server1VM.invoke("stop server1", () -> {
      cache.close();
    });
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
    memberVM.invoke("perform update", () -> cache
        .getRegion(regionName).put("testKey", "updatedTestValue"));
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
    return memberVM.invoke("create locator", () -> {
      // if you need to debug SSL communications use this property:
      // System.setProperty("javax.net.debug", "all");
      System.setProperty(GMSJoinLeave.BYPASS_DISCOVERY_PROPERTY, "true");
      try {
        return Locator.startLocatorAndDS(0, new File(""), getDistributedSystemProperties())
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
    properties.put(LOCATORS, "localhost[" + locatorPort + "]");
    return new CacheFactory(properties).create();
  }

  public Properties getDistributedSystemProperties() {
    Properties properties = new Properties();
    properties.put(ENABLE_CLUSTER_CONFIGURATION, "false");
    properties.put(USE_CLUSTER_CONFIGURATION, "false");
    properties.put(NAME, "vm" + VM.getCurrentVMNum());
    properties.put(CONSERVE_SOCKETS, "" + conserveSockets);
    properties.put(SOCKET_LEASE_TIME, "10000");
    properties.put(SOCKET_BUFFER_SIZE, "" + SMALL_BUFFER_SIZE);

    if (useSSL) {
      properties.put(SSL_ENABLED_COMPONENTS, "cluster,locator");
      properties.put(SSL_KEYSTORE, TestUtil.getResourcePath(this.getClass(), "server.keystore"));
      properties.put(SSL_TRUSTSTORE, TestUtil.getResourcePath(this.getClass(), "server.keystore"));
      properties.put(SSL_PROTOCOLS, "TLSv1.2");
      properties.put(SSL_KEYSTORE_PASSWORD, "password");
      properties.put(SSL_TRUSTSTORE_PASSWORD, "password");
      properties.put(SSL_REQUIRE_AUTHENTICATION, "true");
    }
    return properties;
  }

  /**
   * SerialAckedMessageWithBigReply requires conserve-sockets=false and acts to send
   * a large reply message to the sender. You must have already created a cache in the
   * sender and receiver VMs and registered this class with the DataSerializableFixedID
   * of SERIAL_ACKED_MESSAGE. Don't forget to reset the registration to
   * SerialAckedMessage at the end of the test.
   */
  public static class SerialAckedMessageWithBigReply extends DistributionMessage
      implements MessageWithReply,
      DirectReplyMessage {
    static final int DSFID = SERIAL_ACKED_MESSAGE;

    private int processorId;
    private transient ClusterDistributionManager originDm;
    private transient DirectReplyProcessor replyProcessor;

    public SerialAckedMessageWithBigReply() {
      super();
      InternalDistributedSystem ds = InternalDistributedSystem.getAnyInstance();
      if (ds != null) { // this constructor is used in serialization as well as when sending to
                        // others
        this.originDm = (ClusterDistributionManager) ds.getDistributionManager();
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
      if (failures != null && failures.size() > 0) {
        for (Object failure : failures) {
          System.err.println("Unable to send serial acked message to " + failure);
        }
      }

      replyProcessor.waitForReplies();
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      out.writeInt(processorId);
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      super.fromData(in);
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
      return ClusterDistributionManager.SERIAL_EXECUTOR;
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
        this.processorId = this.replyProcessor.register();
      }
    }
  }

}
