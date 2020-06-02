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

package org.apache.geode.management.internal.configuration;


import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.distributed.ConfigurationProperties.MAX_WAIT_TIME_RECONNECT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.GemFireConfigException;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.persistence.PersistentMemberID;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;


public class ClusterConfigLocatorRestartDUnitTest {

  @Rule
  public ClusterStartupRule rule = new ClusterStartupRule(5);

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  public static class TestDisconnectListener
      implements InternalDistributedSystem.DisconnectListener {
    static int disconnectCount;

    public TestDisconnectListener() {
      disconnectCount = 0;
    }

    @Override
    public void onDisconnect(InternalDistributedSystem sys) {
      disconnectCount += 1;
    }
  }

  @Test
  public void serverRestartsAfterLocatorReconnects() throws Exception {
    IgnoredException.addIgnoredException("org.apache.geode.ForcedDisconnectException: for testing");
    IgnoredException.addIgnoredException("cluster configuration service not available");
    IgnoredException.addIgnoredException("This thread has been stalled");
    IgnoredException
        .addIgnoredException("member unexpectedly shut down shared, unordered connection");
    IgnoredException.addIgnoredException("Connection refused");

    Properties properties = new Properties();
    properties.setProperty(MAX_WAIT_TIME_RECONNECT, "15000");

    MemberVM locator0 = rule.startLocatorVM(0, properties);

    rule.startServerVM(1, properties, locator0.getPort());
    MemberVM server2 = rule.startServerVM(2, properties, locator0.getPort());

    addDisconnectListener(locator0);

    server2.forceDisconnect();
    locator0.forceDisconnect();

    waitForLocatorToReconnect(locator0);

    rule.startServerVM(3, locator0.getPort());

    gfsh.connectAndVerify(locator0);

    await()
        .untilAsserted(() -> gfsh.executeAndAssertThat("list members").statusIsSuccess()
            .tableHasColumnOnlyWithValues("Name", "locator-0", "server-1", "server-2", "server-3"));
  }

  @Test
  public void serverRestartsAfterOneLocatorDies() throws Exception {
    IgnoredException.addIgnoredException("This member is no longer in the membership view");
    IgnoredException.addIgnoredException("This node is no longer in the membership view");
    IgnoredException.addIgnoredException("org.apache.geode.ForcedDisconnectException: for testing");
    IgnoredException.addIgnoredException("Connection refused");

    Properties properties = new Properties();
    properties.setProperty(MAX_WAIT_TIME_RECONNECT, "30000");

    MemberVM locator0 = rule.startLocatorVM(0, properties);
    MemberVM locator1 = rule.startLocatorVM(1, properties, locator0.getPort());

    rule.startServerVM(2, properties, locator0.getPort(), locator1.getPort());
    MemberVM server3 = rule.startServerVM(3, properties, locator0.getPort(), locator1.getPort());

    // Shut down hard
    rule.crashVM(0);

    server3.forceDisconnect();

    rule.startServerVM(4, properties, locator1.getPort(), locator0.getPort());

    gfsh.connectAndVerify(locator1);

    await()
        .untilAsserted(() -> gfsh.executeAndAssertThat("list members").statusIsSuccess()
            .tableHasColumnOnlyWithValues("Name", "locator-1", "server-2", "server-3", "server-4"));
  }

  @Test(timeout = 300_000)
  public void serverRestartHangsWaitingForStartupMessageResponse() throws Exception {
    IgnoredException.addIgnoredException("This member is no longer in the membership view");
    IgnoredException.addIgnoredException("This node is no longer in the membership view");
    IgnoredException.addIgnoredException("ForcedDisconnectException");
    IgnoredException.addIgnoredException("Possible loss of quorum due to the loss");
    IgnoredException.addIgnoredException("Membership service failure:");
    IgnoredException.addIgnoredException("Failed to send message:");
    IgnoredException.addIgnoredException("Cannot form connection to alert listener");
    IgnoredException.addIgnoredException("Received invalid result");
    IgnoredException.addIgnoredException("cluster configuration service not available");
    // With the following steps, a locator can get into state where it is stuck in the middle of
    // reconnecting.
    // It allows members to join the system, but they timeout sending it startup messages and
    // start up without cluster configuration, resulting not being able to restart the cluster.
    // A- Start 2 locators and some number of servers
    // B- Kill one locator and trigger a force disconnect in the remaining locators and servers at
    // the same time
    // C- Have one of the members take a little bit of time before reconnecting, to let the locator
    // get to
    // recovering the _ConfigurationRegion before that remaining member joins.

    // A
    MemberVM locator0 = rule.startLocatorVM(0);
    MemberVM locator1 = rule.startLocatorVM(1, locator0.getPort());

    MemberVM server2 = rule.startServerVM(2, locator0.getPort(), locator1.getPort());
    Properties properties = new Properties();
    properties.setProperty(MAX_WAIT_TIME_RECONNECT, "10000");

    MemberVM server3 =
        rule.startServerVM(3, properties, locator0.getPort(), locator1.getPort());

    gfsh.connectAndVerify(locator1);
    gfsh.executeAndAssertThat("create region --name=region --type=REPLICATE").statusIsSuccess();

    // B
    server2.forceDisconnect();
    server3.forceDisconnect();
    locator1.forceDisconnect();

    rule.crashVM(0); // Shut down hard

    // Wait until locator1 gets stuck waiting for locator0 to start up in order to recover the
    // cluster configuration region
    locator1.invoke(() -> {
      await().untilAsserted(() -> {
        InternalCache cache = GemFireCacheImpl.getInstance();
        assertThat(cache).isNotNull();
        Map<String, Set<PersistentMemberID>> waitingRegions = cache
            .getPersistentMemberManager()
            .getWaitingRegions();

        assertThat(waitingRegions).isNotEmpty();
      });
    });

    // Start another member. This should fail because it should require cluster configuration in
    // order to startup
    assertThatThrownBy(
        () -> this.rule.startServerVM(4, properties, locator1.getPort(), locator0.getPort()))
            .hasCauseInstanceOf(
                GemFireConfigException.class);


    // Restart locator 0, which allows the locators to recover cluster configuration
    rule.startLocatorVM(0, locator1.getPort());

    // Member4 should now be able to startup and get cluster configuartion
    MemberVM server4 =
        rule.startServerVM(4, properties, locator1.getPort(), locator0.getPort());

    // Make sure server4 actually gets the cluster configuration
    server4.invoke(() -> {
      Cache cache = CacheFactory.getAnyInstance();
      assertThat(cache.getRegion(SEPARATOR + "region")).isNotNull();
    });
  }


  private void addDisconnectListener(MemberVM member) {
    member.invoke(() -> {
      InternalDistributedSystem ds =
          (InternalDistributedSystem) InternalLocator.getLocator().getDistributedSystem();
      ds.addDisconnectListener(new TestDisconnectListener());
    });
  }

  private void waitForLocatorToReconnect(MemberVM locator) {
    // Ensure that disconnect/reconnect sequence starts otherwise in the next await we might end up
    // with the initial locator instead of a newly created one.
    locator.invoke(() -> await()
        .until(() -> TestDisconnectListener.disconnectCount > 0));

    locator.invoke(() -> {
      await().until(() -> {
        InternalLocator intLocator = InternalLocator.getLocator();
        return intLocator != null
            && intLocator.getDistributedSystem() != null
            && intLocator.getDistributedSystem().isConnected()
            && intLocator.isSharedConfigurationRunning();
      });
    });
  }
}
