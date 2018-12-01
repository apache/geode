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
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.distributed.ConfigurationProperties;
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
  private static final String LOCATOR_1_NAME = "locator-one";
  private static final String LOCATOR_2_NAME = "locator-two";
  private static final String REGION_PATH = "/test-region-1";
  private static final String RECONNECT_MAILBOX = "reconnectReady";
  private static final int LOCATOR_1_VM_INDEX = 0;
  private static final int LOCATOR_2_VM_INDEX = 1;
  private static final int SERVER_1_VM_INDEX = 2;
  private static final int SERVER_2_VM_INDEX = 3;
  private static final int SERVER_COUNT = 2;
  private static final int NUM_REMOTE_BEANS = 19;
  private static final int NUM_LOCATOR_BEANS = 8;
  private static final int NUM_SERVER_BEANS = 3;
  private static final long TIMEOUT = GeodeAwaitility.getTimeout().getValueInMS();

  private MemberVM locator1, locator2, server1;

  private MBeanServerConnection locator1Connection;
  private MBeanServerConnection locator2Connection;

  @Rule
  public ClusterStartupRule lsRule = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public MBeanServerConnectionRule jmxConnectionRule = new MBeanServerConnectionRule();

  @Rule
  public ConcurrencyRule concurrencyRule = new ConcurrencyRule();

  @Before
  public void before() throws Exception {
    locator1 = lsRule.startLocatorVM(LOCATOR_1_VM_INDEX, locator1Properties());

    locator1.waitTilLocatorFullyStarted();

    locator2 = lsRule.startLocatorVM(LOCATOR_2_VM_INDEX, locator2Properties(), locator1.getPort());

    server1 = lsRule.startServerVM(SERVER_1_VM_INDEX, locator1.getPort());
    // start an extra server to have more MBeans, but we don't need to use it in these tests
    lsRule.startServerVM(SERVER_2_VM_INDEX, locator1.getPort());

    gfsh.connectAndVerify(locator1);
    gfsh.executeAndAssertThat("create region --type=REPLICATE --name=" + REGION_PATH
        + " --enable-statistics=true").statusIsSuccess();

    locator1.waitUntilRegionIsReadyOnExactlyThisManyServers(REGION_PATH, SERVER_COUNT);

    locator1Connection = connectToMBeanServerFor(locator1.getJmxPort());
    locator2Connection = connectToMBeanServerFor(locator2.getJmxPort());

    await("Locators must agree on the state of the system")
        .untilAsserted(() -> assertThat(getFederatedGemfireBeansFrom(locator1Connection))
            .containsExactlyElementsOf(getFederatedGemfireBeansFrom(locator2Connection))
            .hasSize(NUM_REMOTE_BEANS));
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

    locator1.waitTilLocatorFullyReconnected();

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

    server1.waitTilServerFullyReconnected();
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
    List<ObjectName> initialL1Beans = getFederatedGemfireBeansFrom(locator1Connection);
    List<ObjectName> initialL2Beans = getFederatedGemfireBeansFrom(locator2Connection);
    assertThat(initialL1Beans).containsExactlyElementsOf(initialL2Beans).hasSize(NUM_REMOTE_BEANS);

    // calculate the expected list for use once the locator has crashed
    List<ObjectName> expectedIntermediateBeanList = initialL1Beans.stream()
        .filter(excludingBeansFor(LOCATOR_1_NAME)).collect(toList());

    // crash the locator
    locator1.forceDisconnect(TIMEOUT, TimeUnit.MILLISECONDS, RECONNECT_MAILBOX);

    // wait for the locator's crash to federate to the remaining locator
    List<ObjectName> intermediateL2Beans = new ArrayList<>();
    await().untilAsserted(() -> {
      intermediateL2Beans.clear();
      intermediateL2Beans.addAll(getFederatedGemfireBeansFrom(locator2Connection));

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
      finalL2Beans.addAll(getFederatedGemfireBeansFrom(locator2Connection));

      assertThat(finalL2Beans).hasSize(NUM_REMOTE_BEANS);
    });

    // check that the final state is the same as the initial state
    assertThat(getFederatedGemfireBeansFrom(locator1Connection))
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
    List<ObjectName> initialL1Beans = getFederatedGemfireBeansFrom(locator1Connection);
    List<ObjectName> initialL2Beans = getFederatedGemfireBeansFrom(locator2Connection);
    assertThat(initialL1Beans).containsExactlyElementsOf(initialL2Beans).hasSize(NUM_REMOTE_BEANS);

    // calculate the expected list of MBeans when the server has crashed
    List<ObjectName> expectedIntermediateBeanList = initialL1Beans.stream()
        .filter(excludingBeansFor("server-" + SERVER_1_VM_INDEX)).collect(toList());

    // crash the server
    server1.forceDisconnect(TIMEOUT, TimeUnit.MILLISECONDS, RECONNECT_MAILBOX);

    // wait for the server's crash to federate to the locators
    List<ObjectName> intermediateL1Beans = new ArrayList<>();
    List<ObjectName> intermediateL2Beans = new ArrayList<>();

    await().untilAsserted(() -> {
      intermediateL1Beans.clear();
      intermediateL2Beans.clear();

      intermediateL1Beans.addAll(getFederatedGemfireBeansFrom(locator1Connection));
      intermediateL2Beans.addAll(getFederatedGemfireBeansFrom(locator2Connection));

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

      finalL1Beans.addAll(getFederatedGemfireBeansFrom(locator1Connection));
      finalL2Beans.addAll(getFederatedGemfireBeansFrom(locator2Connection));

      // check that the final state eventually matches the initial state
      assertThat(finalL1Beans)
          .containsExactlyElementsOf(finalL2Beans)
          .containsExactlyElementsOf(initialL1Beans)
          .hasSize(NUM_REMOTE_BEANS);
    });
  }

  /**
   * Returns a list of remote MBeans from the given member. The MBeans are filtered to exclude the
   * member's local MBeans. The resulting list includes only MBeans that all locators in the system
   * should have.
   *
   * @param remoteMBS - the connection to the locator's MBean server, created using
   *        connectToMBeanServerFor(MemberVM member).
   * @return List<ObjectName> - a filtered and sorted list of MBeans from the given member
   */
  private static List<ObjectName> getFederatedGemfireBeansFrom(MBeanServerConnection remoteMBS)
      throws IOException {
    Set<ObjectName> allBeans = remoteMBS.queryNames(null, null);
    // Each locator will have a "Manager" bean that is a part of the above query,
    // representing the ManagementAdapter.
    // This bean is registered (and so included in its own queries),
    // but *not* federated (and so is not included in another locator's bean queries).
    // For the scope of this test, we do not consider these "service=Manager" beans.
    return allBeans.stream()
        .filter(b -> b.toString().contains("GemFire"))
        .filter(b -> !b.toString().contains("service=Manager,type=Member,member=locator"))
        .sorted()
        .collect(toList());
  }

  private static MBeanServerConnection connectToMBeanServerFor(int jmxPort) throws IOException {
    String url = "service:jmx:rmi:///jndi/rmi://localhost" + ":" + jmxPort + "/jmxrmi";
    final JMXServiceURL serviceURL = new JMXServiceURL(url);
    return JMXConnectorFactory.connect(serviceURL).getMBeanServerConnection();
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

  private Properties locator1Properties() {
    Properties props = new Properties();
    props.setProperty(ConfigurationProperties.JMX_MANAGER_HOSTNAME_FOR_CLIENTS, "localhost");
    props.setProperty(ConfigurationProperties.NAME, LOCATOR_1_NAME);
    props.setProperty(ConfigurationProperties.MAX_WAIT_TIME_RECONNECT, "5000");
    return props;
  }

  private Properties locator2Properties() {
    Properties props = new Properties();
    props.setProperty(ConfigurationProperties.JMX_MANAGER_HOSTNAME_FOR_CLIENTS, "localhost");
    props.setProperty(ConfigurationProperties.NAME, LOCATOR_2_NAME);
    props.setProperty(ConfigurationProperties.LOCATORS, "localhost[" + locator1.getPort() + "]");
    return props;
  }
}
