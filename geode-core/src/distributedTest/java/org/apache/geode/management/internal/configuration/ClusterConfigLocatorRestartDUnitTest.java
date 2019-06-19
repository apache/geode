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



import static org.apache.geode.distributed.ConfigurationProperties.MAX_WAIT_TIME_RECONNECT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Properties;

import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.GemFireConfigException;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.VM;
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



    rule.startServerVM(4, locator1.getPort(), locator0.getPort());

    gfsh.connectAndVerify(locator1);

    await()
        .untilAsserted(() -> gfsh.executeAndAssertThat("list members").statusIsSuccess()
            .tableHasColumnOnlyWithValues("Name", "locator-1", "server-2", "server-3", "server-4"));
  }

  /**
   * This test demonstrates that killing a locator after join but before getting cluster
   * configuration results in a lack of cluster configuration.
   */
  @Test
  public void memberStartupFailsIfNoClusterConfigAvailable() {
    IgnoredException.addIgnoredException("This member is no longer in the membership view");
    IgnoredException.addIgnoredException("This node is no longer in the membership view");
    IgnoredException
        .addIgnoredException("org.apache.geode.ForcedDisconnectException: for testing");
    IgnoredException.addIgnoredException("Connection refused");
    IgnoredException.addIgnoredException("cluster configuration service not available");

    Properties properties = new Properties();
    properties.setProperty(MAX_WAIT_TIME_RECONNECT, "30000");

    MemberVM locator1 = rule.startLocatorVM(0, properties);
    VM locator1VM = locator1.getVM();

    MemberVM server2 = rule.startServerVM(2, properties, locator1.getPort());

    VM server3 = VM.getVM(3);

    server3.invoke(() -> {
      InternalDistributedSystem.addConnectListener(sys -> {
        locator1VM.invoke(() -> InternalDistributedSystem.getAnyInstance().disconnect());
      });
    });

    assertThatThrownBy(() -> this.rule.startServerVM(3, properties, locator1.getPort()))
        .hasCauseInstanceOf(
            GemFireConfigException.class);

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
