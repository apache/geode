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

package org.apache.geode.management;

import static java.util.stream.Collectors.toList;
import static org.apache.geode.management.ManagementService.getExistingManagementService;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase.getBlackboard;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import javax.management.ObjectName;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.JMXTest;
import org.apache.geode.test.junit.rules.ConcurrencyRule;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.MBeanServerConnectionRule;

@Category({JMXTest.class})
public class JMXMBeanReconnectDUnitTest {
  private static final String REGION_PATH = "/test-region-1";
  private static final String RECONNECT_MAILBOX = "reconnectReady";
  private static final int SERVER_COUNT = 2;
  private static final int NUM_REMOTE_BEANS = 19;
  private static final int NUM_LOCATOR_BEANS = 8;
  private static final int NUM_SERVER_BEANS = 3;
  private static final long TIMEOUT = GeodeAwaitility.getTimeout().getValueInMS();

  private MemberVM locator1, locator2, server1;

  @Rule
  public ClusterStartupRule lsRule = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  private MBeanServerConnectionRule jmxConToLocator1;
  private MBeanServerConnectionRule jmxConToLocator2;

  @Rule
  public ConcurrencyRule concurrencyRule = new ConcurrencyRule();

  @Before
  public void before() throws Exception {
    locator1 = lsRule.startLocatorVM(0);
    locator2 = lsRule.startLocatorVM(1, locator1.getPort());

    server1 = lsRule.startServerVM(2, locator1.getPort());
    // start an extra server to have more MBeans, but we don't need to use it in these tests
    lsRule.startServerVM(3, locator1.getPort());

    gfsh.connectAndVerify(locator1);
    gfsh.executeAndAssertThat("create region --type=REPLICATE --name=" + REGION_PATH
        + " --enable-statistics=true").statusIsSuccess();

    locator1.waitUntilRegionIsReadyOnExactlyThisManyServers(REGION_PATH, SERVER_COUNT);

    jmxConToLocator1 = new MBeanServerConnectionRule();
    jmxConToLocator1.connect(locator1.getJmxPort());
    jmxConToLocator2 = new MBeanServerConnectionRule();
    jmxConToLocator2.connect(locator2.getJmxPort());

    await("Locators must agree on the state of the system")
        .untilAsserted(() -> assertThat(jmxConToLocator1.getGemfireFederatedBeans())
            .containsExactlyElementsOf(jmxConToLocator2.getGemfireFederatedBeans())
            .hasSize(NUM_REMOTE_BEANS));
  }

  @After
  public void after() throws Exception {
    jmxConToLocator1.disconnect();
    jmxConToLocator2.disconnect();
  }

  /**
   * Test that a server's local MBeans are not affected by a locator crashing
   */
  @Test
  public void testLocalBeans_MaintainServerAndCrashLocator() {
    List<String> initialServerBeans = server1.invoke(() -> getLocalCanonicalBeanNames());
    assertThat(initialServerBeans).hasSize(NUM_SERVER_BEANS);

    locator1.forceDisconnect();

    List<String> intermediateServerBeans = server1.invoke(() -> getLocalCanonicalBeanNames());
    assertThat(intermediateServerBeans)
        .containsExactlyElementsOf(initialServerBeans)
        .hasSize(NUM_SERVER_BEANS);

    locator1.waitTilFullyReconnected();

    List<String> finalServerBeans = server1.invoke(() -> getLocalCanonicalBeanNames());
    assertThat(finalServerBeans)
        .containsExactlyElementsOf(initialServerBeans)
        .hasSize(NUM_SERVER_BEANS);
  }

  /**
   * Test that a locator's local MBeans are not affected by a server crashing
   */
  @Test
  public void testLocalBeans_MaintainLocatorAndCrashServer() {
    List<String> initialLocatorBeans = locator1.invoke(() -> getLocalCanonicalBeanNames());
    assertThat(initialLocatorBeans).hasSize(NUM_LOCATOR_BEANS);

    server1.forceDisconnect();

    List<String> intermediateLocatorBeans = locator1.invoke(() -> getLocalCanonicalBeanNames());
    assertThat(intermediateLocatorBeans)
        .containsExactlyElementsOf(initialLocatorBeans)
        .hasSize(NUM_LOCATOR_BEANS);

    server1.waitTilFullyReconnected();
    locator1.waitUntilRegionIsReadyOnExactlyThisManyServers(REGION_PATH, SERVER_COUNT);

    List<String> finalLocatorBeans = locator1.invoke(() -> getLocalCanonicalBeanNames());
    assertThat(finalLocatorBeans)
        .containsExactlyElementsOf(initialLocatorBeans)
        .hasSize(NUM_LOCATOR_BEANS);
  }

  /**
   * Test MBean consistency when disconnecting and reconnecting the lead locator. MBeans should
   * remain the same after a member reconnects as they were before the disconnect. MBeans (other
   * than local MBeans, which are filtered for this test) should be consistent between locators.
   * All MBeans not related to the killed member should remain the same when a member is killed.
   */
  @Test
  public void testRemoteBeanKnowledge_MaintainServerAndCrashLocator() throws IOException {
    // check that the initial state is good
    List<ObjectName> initialL1Beans = jmxConToLocator1.getGemfireFederatedBeans();
    List<ObjectName> initialL2Beans = jmxConToLocator2.getGemfireFederatedBeans();
    assertThat(initialL1Beans).containsExactlyElementsOf(initialL2Beans).hasSize(NUM_REMOTE_BEANS);

    // calculate the expected list for use once the locator has crashed
    List<ObjectName> expectedIntermediateBeanList = initialL1Beans.stream()
        .filter(excludingBeansFor("locator-0")).collect(toList());

    // crash the locator
    locator1.forceDisconnect(TIMEOUT, TimeUnit.MILLISECONDS, RECONNECT_MAILBOX);

    // wait for the locator's crash to federate to the remaining locator
    List<ObjectName> intermediateL2Beans = new ArrayList<>();
    await().untilAsserted(() -> {
      intermediateL2Beans.clear();
      intermediateL2Beans.addAll(jmxConToLocator2.getGemfireFederatedBeans());

      assertThat(intermediateL2Beans)
          .containsExactlyElementsOf(expectedIntermediateBeanList)
          .hasSameSizeAs(expectedIntermediateBeanList);
    });

    // allow locator 1 to start reconnecting
    locator1.invoke(() -> getBlackboard().setMailbox(RECONNECT_MAILBOX, true));

    // wait for the locator's restart to federate to the other locator
    List<ObjectName> finalL2Beans = new ArrayList<>();
    await().untilAsserted(() -> {
      finalL2Beans.clear();
      finalL2Beans.addAll(jmxConToLocator2.getGemfireFederatedBeans());

      assertThat(finalL2Beans).hasSize(NUM_REMOTE_BEANS);
    });

    // check that the final state is the same as the initial state
    assertThat(jmxConToLocator1.getGemfireFederatedBeans())
        .containsExactlyElementsOf(finalL2Beans)
        .containsExactlyElementsOf(initialL1Beans)
        .hasSize(NUM_REMOTE_BEANS);
  }

  /**
   * Test MBean consistency when disconnecting and reconnecting a server. MBeans should
   * remain the same after a member reconnects as they were before the disconnect. MBeans (other
   * than local MBeans, which are filtered for this test) should be consistent between locators.
   * All MBeans not related to the killed member should remain the same when a member is killed.
   */
  @Test
  public void testRemoteBeanKnowledge_MaintainLocatorAndCrashServer() throws IOException {
    // check that the initial state is correct
    List<ObjectName> initialL1Beans = jmxConToLocator1.getGemfireFederatedBeans();
    List<ObjectName> initialL2Beans = jmxConToLocator2.getGemfireFederatedBeans();
    assertThat(initialL1Beans).containsExactlyElementsOf(initialL2Beans).hasSize(NUM_REMOTE_BEANS);

    // calculate the expected list of MBeans when the server has crashed
    List<ObjectName> expectedIntermediateBeanList = initialL1Beans.stream()
        .filter(excludingBeansFor("server-2")).collect(toList());

    // crash the server
    server1.forceDisconnect(TIMEOUT, TimeUnit.MILLISECONDS, RECONNECT_MAILBOX);

    // wait for the server's crash to federate to the locators
    List<ObjectName> intermediateL1Beans = new ArrayList<>();
    List<ObjectName> intermediateL2Beans = new ArrayList<>();

    await().untilAsserted(() -> {
      intermediateL1Beans.clear();
      intermediateL2Beans.clear();

      intermediateL1Beans.addAll(jmxConToLocator1.getGemfireFederatedBeans());
      intermediateL2Beans.addAll(jmxConToLocator2.getGemfireFederatedBeans());

      assertThat(intermediateL1Beans)
          .containsExactlyElementsOf(expectedIntermediateBeanList)
          .hasSameSizeAs(expectedIntermediateBeanList);

      assertThat(intermediateL2Beans)
          .containsExactlyElementsOf(expectedIntermediateBeanList)
          .hasSameSizeAs(expectedIntermediateBeanList);
    });

    // allow the server to start reconnecting
    server1.invoke(() -> getBlackboard().setMailbox(RECONNECT_MAILBOX, true));

    // wait for the server's restart to federate to the locators and check final state
    List<ObjectName> finalL1Beans = new ArrayList<>();
    List<ObjectName> finalL2Beans = new ArrayList<>();
    await().untilAsserted(() -> {
      finalL1Beans.clear();
      finalL2Beans.clear();

      finalL1Beans.addAll(jmxConToLocator1.getGemfireFederatedBeans());
      finalL2Beans.addAll(jmxConToLocator2.getGemfireFederatedBeans());

      // check that the final state eventually matches the initial state
      assertThat(finalL1Beans)
          .containsExactlyElementsOf(finalL2Beans)
          .containsExactlyElementsOf(initialL1Beans)
          .hasSize(NUM_REMOTE_BEANS);
    });
  }

  /**
   * Gets a list of local MBeans from the JVM this is invoked from. This list of MBeans does not
   * include beans for members other than the member this method is invoked on.
   */
  private static List<String> getLocalCanonicalBeanNames() {
    Cache cache = ClusterStartupRule.getCache();
    SystemManagementService service = (SystemManagementService) getExistingManagementService(cache);
    Set<ObjectName> keySet = service.getJMXAdapter().getLocalGemFireMBean().keySet();
    return keySet.stream().map(ObjectName::getCanonicalName).sorted().collect(toList());
  }

  private static Predicate<ObjectName> excludingBeansFor(String memberName) {
    return b -> !b.getCanonicalName().contains("member=" + memberName);
  }
}
