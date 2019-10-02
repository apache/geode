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
package org.apache.geode.internal.cache;

import static org.apache.geode.distributed.ConfigurationProperties.DISABLE_AUTO_RECONNECT;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_NETWORK_PARTITION_DETECTION;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.USE_CLUSTER_CONFIGURATION;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.DistributedTestUtils.crashDistributedSystem;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.apache.geode.test.dunit.VM.getHostName;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.ForcedDisconnectException;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.DistributionMessageObserver;
import org.apache.geode.internal.cache.partitioned.ManageBucketMessage;
import org.apache.geode.internal.cache.partitioned.ManageBucketMessage.ManageBucketReplyMessage;
import org.apache.geode.test.dunit.RMIException;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.dunit.rules.SharedErrorCollector;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * Verifies that new bucket does not hang after requester crashes.
 *
 * <p>
 * TRAC #41733: Hang in BucketAdvisor.waitForPrimaryMember
 */
@SuppressWarnings("serial")
public class BucketCreationCrashRegressionTest implements Serializable {

  private String uniqueName;
  private String hostName;
  private int locatorPort;
  private File locatorLog;

  private VM server1;
  private VM server2;
  private VM locator;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Rule
  public SharedErrorCollector errorCollector = new SharedErrorCollector();

  @Before
  public void setUp() throws Exception {
    server1 = getVM(0);
    server2 = getVM(1);
    locator = getVM(2);

    uniqueName = getClass().getSimpleName() + "_" + testName.getMethodName();
    hostName = getHostName();
    locatorLog = new File(temporaryFolder.newFolder(uniqueName), "locator.log");

    locatorPort = locator.invoke(() -> startLocator());
    assertThat(locatorPort).isGreaterThan(0);

    server1.invoke(() -> createServerCache());
    server2.invoke(() -> createServerCache());

    // cluster should ONLY have 3 members (our 2 servers and 1 locator)
    assertThat(server1.invoke(() -> cacheRule.getCache().getDistributionManager()
        .getDistributionManagerIdsIncludingAdmin())).hasSize(3);

    addIgnoredException(ForcedDisconnectException.class);
  }

  @After
  public void tearDown() {
    DistributionMessageObserver.setInstance(null);
    invokeInEveryVM(() -> {
      DistributionMessageObserver.setInstance(null);
    });
  }

  /**
   * Test the we can handle a member departing after creating a bucket on the remote node but before
   * we choose a primary
   */
  @Test
  public void putShouldNotHangAfterBucketCrashesBeforePrimarySelection() {
    server1.invoke(
        () -> handleBeforeProcessMessage(ManageBucketReplyMessage.class, () -> crashServer()));
    server1.invoke(() -> createPartitionedRegion());

    // Create a couple of buckets in VM0. This will make sure
    // the next bucket we create will be created in VM 1.
    server1.invoke(() -> putData(0, 2, "a"));

    server2.invoke(() -> createPartitionedRegion());

    // Trigger a bucket creation in VM1, which should cause server1 to close it's cache.
    assertThatThrownBy(() -> server1.invoke(() -> putData(3, 4, "a")))
        .isInstanceOf(RMIException.class)
        .hasCauseInstanceOf(DistributedSystemDisconnectedException.class);

    assertThat(server2.invoke(() -> getBucketList())).containsExactly(3);

    // This shouldn't hang, because the bucket creation should finish,.
    server2.invoke(() -> putData(3, 4, "a"));
  }

  /**
   * Test the we can handle a member departing while we are in the process of creating the bucket on
   * the remote node.
   */
  @Test
  public void putShouldNotHangAfterServerWithBucketCrashes() {
    server2.invoke(() -> handleBeforeProcessMessage(ManageBucketMessage.class,
        () -> server1.invoke(() -> crashServer())));
    server1.invoke(() -> createPartitionedRegion());

    // Create a couple of buckets in VM0. This will make sure
    // the next bucket we create will be created in VM 1.
    server1.invoke(() -> putData(0, 2, "a"));

    server2.invoke(() -> createPartitionedRegion());

    // Trigger a bucket creation in VM1, which should cause server1 to close it's cache.
    assertThatThrownBy(() -> server1.invoke(() -> putData(3, 4, "a")))
        .isInstanceOf(RMIException.class)
        .hasCauseInstanceOf(DistributedSystemDisconnectedException.class);

    await()
        .untilAsserted(() -> assertThat(server2.invoke(() -> getBucketList())).containsExactly(3));

    // This shouldn't hang, because the bucket creation should finish.
    server2.invoke(() -> putData(3, 4, "a"));
  }

  private Properties createLocatorConfig() {
    Properties config = new Properties();
    config.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");
    config.setProperty(ENABLE_NETWORK_PARTITION_DETECTION, "false");
    config.setProperty(USE_CLUSTER_CONFIGURATION, "false");
    config.setProperty(DISABLE_AUTO_RECONNECT, "true");
    return config;
  }

  private Properties createServerConfig() {
    Properties config = createLocatorConfig();
    config.setProperty(LOCATORS, hostName + "[" + locatorPort + "]");
    return config;
  }

  private int startLocator() throws IOException {
    Properties config = createLocatorConfig();
    InetAddress bindAddress = InetAddress.getByName(hostName);
    Locator locator = Locator.startLocatorAndDS(locatorPort, locatorLog, bindAddress, config);
    return locator.getPort();
  }

  private void createServerCache() {
    cacheRule.createCache(createServerConfig());
  }

  private void createPartitionedRegion() {
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(0);

    AttributesFactory af = new AttributesFactory();
    af.setDataPolicy(DataPolicy.PARTITION);
    af.setPartitionAttributes(paf.create());

    cacheRule.getCache().createRegion(uniqueName, af.create());
  }

  private void putData(final int startKey, final int endKey, final String value) {
    Region<Integer, String> region = cacheRule.getCache().getRegion(uniqueName);

    for (int i = startKey; i < endKey; i++) {
      region.put(i, value);
    }
  }

  private Set<Integer> getBucketList() {
    PartitionedRegion region = (PartitionedRegion) cacheRule.getCache().getRegion(uniqueName);
    return new TreeSet<>(region.getDataStore().getAllLocalBucketIds());
  }

  private void handleBeforeProcessMessage(final Class<? extends DistributionMessage> messageClass,
      final SerializableRunnableIF runnable) {
    DistributionMessageObserver
        .setInstance(new RunnableBeforeProcessMessageObserver(messageClass, runnable));
  }

  private void crashServer() {
    crashDistributedSystem(cacheRule.getSystem());
  }

  private class RunnableBeforeProcessMessageObserver extends DistributionMessageObserver {

    private final Class<? extends DistributionMessage> messageClass;
    private final SerializableRunnableIF runnable;

    RunnableBeforeProcessMessageObserver(final Class<? extends DistributionMessage> messageClass,
        final SerializableRunnableIF runnable) {
      this.messageClass = messageClass;
      this.runnable = runnable;
    }

    @Override
    public void beforeProcessMessage(ClusterDistributionManager dm, DistributionMessage message) {
      if (messageClass.isInstance(message)) {
        try {
          runnable.run();
        } catch (Exception e) {
          errorCollector.addError(e);
        }
      }
    }
  }
}
