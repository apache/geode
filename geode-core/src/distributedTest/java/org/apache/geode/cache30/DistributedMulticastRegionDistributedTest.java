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
package org.apache.geode.cache30;

import static org.apache.geode.distributed.ConfigurationProperties.DISABLE_AUTO_RECONNECT;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_TIME_STATISTICS;
import static org.apache.geode.distributed.ConfigurationProperties.MAX_WAIT_TIME_RECONNECT;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_TTL;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_SAMPLING_ENABLED;

import java.io.Serializable;
import java.util.Properties;

import org.assertj.core.api.Assertions;
import org.jetbrains.annotations.NotNull;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.api.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.Scope;
import org.apache.geode.distributed.internal.membership.api.MembershipManagerHelper;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.CachedDeserializableFactory;
import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxSerializationException;
import org.apache.geode.pdx.PdxWriter;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

@SuppressWarnings("serial")
public class DistributedMulticastRegionDistributedTest implements Serializable {

  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule();

  protected void setUDPDHAlgo(Properties properties) {
    // Override in child classes.
  }

  @Test
  public void testMulticastEnabled() {
    final String ROOT_REGION_NAME = "root";
    final String SUB_REGION_NAME = "subregion";
    final String mcastPort = String.valueOf(AvailablePortHelper.getRandomAvailableUDPPort());
    final String mcastTTL = "0";
    Properties properties = getMemberProperties(mcastPort, mcastTTL);
    MemberVM locator = clusterStartupRule.startLocatorVM(0, l -> l.withProperties(properties));
    int locatorPort = locator.getPort();
    MemberVM server1 =
        clusterStartupRule.startServerVM(1,
            s -> s.withConnectionToLocator(locatorPort).withProperties(properties));
    MemberVM server2 =
        clusterStartupRule.startServerVM(2,
            s -> s.withConnectionToLocator(locatorPort).withProperties(properties));

    server1.invoke("Creating root region in server 1",
        () -> createRootAndSubRegion(ROOT_REGION_NAME, SUB_REGION_NAME));
    server2.invoke("Creating root region in server 2",
        () -> createRootAndSubRegion(ROOT_REGION_NAME, SUB_REGION_NAME));

    server1.invoke("Do puts in server 1", () -> doPutOperations(ROOT_REGION_NAME, SUB_REGION_NAME));
    server1.invoke("Validating multicast operation in server 1",
        this::validateMulticastOpsAfterRegionOps);
    server2.invoke("Validating multicast operation in server 2",
        this::validateMulticastOpsAfterRegionOps);
  }

  @NotNull
  private Properties getMemberProperties(String mcastPort, String mcastTTL) {
    Properties properties = new Properties();
    properties.setProperty(MCAST_PORT, mcastPort);
    properties.setProperty(MCAST_TTL, mcastTTL);
    properties.setProperty(DISABLE_AUTO_RECONNECT, "false");
    properties.setProperty(MAX_WAIT_TIME_RECONNECT, "20");
    properties.setProperty(STATISTIC_SAMPLING_ENABLED, "true");
    properties.setProperty(ENABLE_TIME_STATISTICS, "true");
    setUDPDHAlgo(properties);
    return properties;
  }

  private void createRootAndSubRegion(final String rootRegionName, final String subRegionName) {
    RegionFactory<Object, Object> regionFactory =
        ClusterStartupRule.getCache().createRegionFactory().setScope(Scope.DISTRIBUTED_ACK)
            .setDataPolicy(DataPolicy.PRELOADED).setConcurrencyChecksEnabled(false)
            .setMulticastEnabled(true).setPartitionAttributes(null);
    Region<Object, Object> rootRegion = regionFactory.create(rootRegionName);
    regionFactory.createSubregion(rootRegion, subRegionName);
  }

  @Test
  public void testMulticastAfterReconnect() throws InterruptedException {
    final String ROOT_REGION_NAME = "root";
    final String SUB_REGION_NAME = "subregion";
    final String mcastPort = String.valueOf(AvailablePortHelper.getRandomAvailableUDPPort());
    final String mcastTTL = "0";
    Properties properties = getMemberProperties(mcastPort, mcastTTL);
    MemberVM locator = clusterStartupRule.startLocatorVM(0, l -> l.withProperties(properties));
    int locatorPort = locator.getPort();
    MemberVM server1 =
        clusterStartupRule.startServerVM(1,
            s -> s.withConnectionToLocator(locatorPort).withProperties(properties));
    MemberVM server2 =
        clusterStartupRule.startServerVM(2,
            s -> s.withConnectionToLocator(locatorPort).withProperties(properties));

    server1.invoke("Creating root region in server 1",
        () -> createRootAndSubRegion(ROOT_REGION_NAME, SUB_REGION_NAME));
    server2.invoke("Creating root region in server 2",
        () -> createRootAndSubRegion(ROOT_REGION_NAME, SUB_REGION_NAME));

    server1.invoke("Do puts in server 1", () -> doPutOperations(ROOT_REGION_NAME, SUB_REGION_NAME));

    // Disconnect server 2 and do put operations in server 1
    server2.invoke(() -> MembershipManagerHelper.crashDistributedSystem(
        ClusterStartupRule.getCache().getInternalDistributedSystem()));
    server1.invoke("Do puts in server 1", () -> doPutOperations(ROOT_REGION_NAME, SUB_REGION_NAME));
    server2.waitTilFullyReconnected();

    server2.invoke("Creating root region in server 2",
        () -> createRootAndSubRegion(ROOT_REGION_NAME, SUB_REGION_NAME));
    // after server 2 reconnects it should have the correct multicast digest and operations
    // on the cache region should be acknowledged.
    AsyncInvocation<?> asyncInvocation =
        server1.invokeAsync(() -> doPutOperations(ROOT_REGION_NAME, SUB_REGION_NAME));
    GeodeAwaitility.await().until(asyncInvocation::isDone);
    asyncInvocation.get();

    // after server 2 reconnects it should respond to multicast destroy-region messages
    asyncInvocation = server1.invokeAsync(() -> GeodeAwaitility.await().until(() -> {
      ClusterStartupRule.getCache().close();
      return true;
    }));
    GeodeAwaitility.await().until(asyncInvocation::isDone);
    asyncInvocation.get();

    server2.invoke("Validating multicast operation in server 2",
        this::validateMulticastOpsAfterRegionOps);

  }

  private void doPutOperations(String rootRegionName, String subRegionName) {
    final Region<Object, Object> region =
        ClusterStartupRule.getCache().getRegion(rootRegionName).getSubregion(subRegionName);
    for (int i = 0; i < 50; i++) {
      region.put(i, i);
    }
  }

  private static class TestObjectThrowsException implements PdxSerializable {

    final String name = "TestObjectThrowsException";

    @Override
    public void toData(PdxWriter writer) {
      writer.writeString("name", name);
    }

    @Override
    public void fromData(PdxReader reader) {
      throw new RuntimeException("Unable to deserialize message ");
    }
  }

  @Test
  public void testMulticastWithRegionOpsException() {
    final String ROOT_REGION_NAME = "root";
    final String SUB_REGION_NAME = "subregion";
    final String mcastPort = String.valueOf(AvailablePortHelper.getRandomAvailableUDPPort());
    final String mcastTTL = "0";
    Properties properties = getMemberProperties(mcastPort, mcastTTL);
    MemberVM locator = clusterStartupRule.startLocatorVM(0, l -> l.withProperties(properties));
    int locatorPort = locator.getPort();
    MemberVM server1 =
        clusterStartupRule.startServerVM(1,
            s -> s.withConnectionToLocator(locatorPort).withProperties(properties));
    MemberVM server2 =
        clusterStartupRule.startServerVM(2,
            s -> s.withConnectionToLocator(locatorPort).withProperties(properties));

    server1.invoke(() -> {
      CachedDeserializableFactory.STORE_ALL_VALUE_FORMS = true;
    });
    server2.invoke(() -> {
      CachedDeserializableFactory.STORE_ALL_VALUE_FORMS = true;
    });

    server1.invoke("Creating root region in server 1",
        () -> createRootAndSubRegion(ROOT_REGION_NAME, SUB_REGION_NAME));
    server2.invoke("Creating root region in server 2",
        () -> createRootAndSubRegion(ROOT_REGION_NAME, SUB_REGION_NAME));


    server1.invoke("do Put() with exception test", () -> {
      final Region<Object, Object> region =
          ClusterStartupRule.getCache().getRegion(ROOT_REGION_NAME).getSubregion(SUB_REGION_NAME);
      boolean gotReplyException = false;
      for (int i = 0; i < 1; i++) {
        try {
          region.put(i, new TestObjectThrowsException());
        } catch (PdxSerializationException e) {
          gotReplyException = true;
        } catch (Exception e) {
          ClusterStartupRule.getCache().getLogger()
              .info("Got exception of type " + e.getClass().toString());
        }
      }
      Assertions.assertThat(gotReplyException).as("ReplyException is expected").isTrue();
    });

    server1.invoke("Validating multicast operation in server 1",
        this::validateMulticastOpsAfterRegionOps);
    server2.invoke("Validating multicast operation in server 2",
        this::validateMulticastOpsAfterRegionOps);
  }

  protected void validateMulticastOpsAfterRegionOps() {
    long writes =
        ClusterStartupRule.getCache().getDistributionManager().getStats().getMcastWrites();
    long reads = ClusterStartupRule.getCache().getDistributionManager().getStats().getMcastReads();
    Assertions.assertThat(writes > 0 || reads > 0)
        .as("Should have multicast writes or reads. Writes=  " + writes + " ,read= " + reads)
        .isTrue();

    validateUDPEncryptionStats();
  }

  protected void validateUDPEncryptionStats() {
    long encryptTime =
        ClusterStartupRule.getCache().getDistributionManager().getStats().getUDPMsgEncryptionTime();
    long decryptTime =
        ClusterStartupRule.getCache().getDistributionManager().getStats().getUDPMsgDecryptionTime();
    Assertions.assertThat(encryptTime == 0 && decryptTime == 0)
        .as("Should have multicast writes or reads. encryptTime=  " + encryptTime
            + " ,decryptTime= " + decryptTime)
        .isTrue();
  }
}
