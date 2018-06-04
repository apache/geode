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

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.awaitility.Awaitility.waitAtMost;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Predicate;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.JMXTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.MBeanServerConnectionRule;

@Category({DistributedTest.class, JMXTest.class})
public class JMXMBeanReconnectDUnitTest {
  private static final String LOCATOR_1_NAME = "locator-one";
  private static final String LOCATOR_2_NAME = "locator-two";
  private static final String REGION_PATH = "/test-region-1";
  private static final int LOCATOR_1_VM_INDEX = 0;
  private static final int LOCATOR_2_VM_INDEX = 1;
  private static final int LOCATOR_COUNT = 2;
  private static final int SERVER_1_VM_INDEX = 2;
  private static final int SERVER_2_VM_INDEX = 3;
  private static final int SERVER_COUNT = 2;

  private int locator1JmxPort, locator2JmxPort;

  private MemberVM locator1, locator2, server1, server2;

  @Rule
  public ClusterStartupRule lsRule = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public MBeanServerConnectionRule jmxConnectionRule = new MBeanServerConnectionRule();

  @Before
  public void before() throws Exception {
    int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(LOCATOR_COUNT);
    locator1JmxPort = ports[0];
    locator2JmxPort = ports[1];

    locator1 = lsRule.startLocatorVM(LOCATOR_1_VM_INDEX, locator1Properties());
    locator2 = lsRule.startLocatorVM(LOCATOR_2_VM_INDEX, locator2Properties(), locator1.getPort());

    server1 = lsRule.startServerVM(SERVER_1_VM_INDEX, locator1.getPort());
    server2 = lsRule.startServerVM(SERVER_2_VM_INDEX, locator1.getPort());

    gfsh.connectAndVerify(locator1);
    gfsh.executeAndAssertThat(
        "create region --type=REPLICATE --name=" + REGION_PATH + " --enable-statistics=true")
        .statusIsSuccess();

    locator1.waitTillRegionsAreReadyOnServers(REGION_PATH, SERVER_COUNT);
    waitForLocatorsToAgreeOnMembership();
  }

  @Test
  public void testLocalBeans_MaintainServerAndCrashLocator() {
    List<String> initialServerBeans = canonicalBeanNamesFor(server1);

    locator1.forceDisconnectMember();

    List<String> intermediateServerBeans = canonicalBeanNamesFor(server1);

    assertThat(intermediateServerBeans)
        .containsExactlyElementsOf(initialServerBeans);

    locator1.waitTilLocatorFullyReconnected();

    List<String> finalServerBeans = canonicalBeanNamesFor(server1);

    assertThat(finalServerBeans)
        .containsExactlyElementsOf(initialServerBeans);
  }

  @Test
  public void testLocalBeans_MaintainLocatorAndCrashServer() {
    List<String> initialLocatorBeans = canonicalBeanNamesFor(locator1);

    server1.forceDisconnectMember();

    List<String> intermediateLocatorBeans = canonicalBeanNamesFor(locator1);

    assertThat(intermediateLocatorBeans)
        .containsExactlyElementsOf(initialLocatorBeans);

    server1.waitTilServerFullyReconnected();
    locator1.waitTillRegionsAreReadyOnServers(REGION_PATH, SERVER_COUNT);

    List<String> finalLocatorBeans = canonicalBeanNamesFor(locator1);

    assertThat(finalLocatorBeans)
        .containsExactlyElementsOf(initialLocatorBeans);
  }

  @Test
  public void testRemoteBeanKnowledge_MaintainServerAndCrashLocator() throws IOException {
    List<ObjectName> initialLocator1GemfireBeans =
        getFederatedGemfireBeansFrom(locator1);
    List<ObjectName> initialLocator2GemfireBeans =
        getFederatedGemfireBeansFrom(locator2);

    assertThat(initialLocator1GemfireBeans)
        .containsExactlyElementsOf(initialLocator2GemfireBeans);

    locator1.forceDisconnectMember();

    List<ObjectName> intermediateLocator2GemfireBeans =
        getFederatedGemfireBeansFrom(locator2);

    List<ObjectName> locatorBeansExcludingStoppedMember = initialLocator2GemfireBeans.stream()
        .filter(excludingBeansFor(LOCATOR_1_NAME)).collect(toList());

    assertThat(intermediateLocator2GemfireBeans)
        .containsExactlyElementsOf(locatorBeansExcludingStoppedMember);

    locator1.waitTilLocatorFullyReconnected();
    waitForLocatorsToAgreeOnMembership();

    List<ObjectName> finalLocator1GemfireBeans =
        getFederatedGemfireBeansFrom(locator1);
    List<ObjectName> finalLocator2GemfireBeans =
        getFederatedGemfireBeansFrom(locator2);

    assertSoftly(softly -> {
      softly.assertThat(finalLocator1GemfireBeans)
          .containsExactlyElementsOf(finalLocator2GemfireBeans);
      softly.assertThat(finalLocator1GemfireBeans)
          .containsExactlyElementsOf(initialLocator2GemfireBeans);
    });
  }

  @Test
  public void testRemoteBeanKnowledge_MaintainLocatorAndCrashServer()
      throws IOException {
    List<ObjectName> initialLocator1GemfireBeans =
        getFederatedGemfireBeansFrom(locator1);
    List<ObjectName> initialLocator2GemfireBeans =
        getFederatedGemfireBeansFrom(locator2);

    assertThat(initialLocator1GemfireBeans)
        .containsExactlyElementsOf(initialLocator2GemfireBeans);

    server1.forceDisconnectMember();

    List<ObjectName> intermediateLocator1GemfireBeans =
        getFederatedGemfireBeansFrom(locator1);
    List<ObjectName> intermediateLocator2GemfireBeans =
        getFederatedGemfireBeansFrom(locator2);

    List<ObjectName> locatorBeansExcludingStoppedMember = initialLocator1GemfireBeans.stream()
        .filter(excludingBeansFor("server-" + SERVER_1_VM_INDEX))
        .collect(toList());

    assertSoftly(softly -> {
      softly.assertThat(intermediateLocator2GemfireBeans)
          .containsExactlyElementsOf(intermediateLocator1GemfireBeans);
      softly.assertThat(intermediateLocator2GemfireBeans)
          .containsExactlyElementsOf(locatorBeansExcludingStoppedMember);
    });

    server1.waitTilServerFullyReconnected();
    locator1.waitTillRegionsAreReadyOnServers(REGION_PATH, SERVER_COUNT);
    waitForLocatorsToAgreeOnMembership();

    List<ObjectName> finalLocator1GemfireBeans =
        getFederatedGemfireBeansFrom(locator1);
    List<ObjectName> finalLocator2GemfireBeans =
        getFederatedGemfireBeansFrom(locator2);

    assertSoftly(softly -> {
      softly.assertThat(finalLocator1GemfireBeans)
          .containsExactlyElementsOf(finalLocator2GemfireBeans);
      softly.assertThat(finalLocator1GemfireBeans)
          .containsExactlyElementsOf(initialLocator2GemfireBeans);
    });
  }

  private static List<ObjectName> getFederatedGemfireBeansFrom(MemberVM member)
      throws IOException {
    String url = jmxBeanLocalhostUrlString(member.getJmxPort());
    MBeanServerConnection remoteMBS = connectToMBeanServer(url);
    return getFederatedGemfireBeanObjectNames(remoteMBS);
  }

  private static MBeanServerConnection connectToMBeanServer(String url) throws IOException {
    final JMXServiceURL serviceURL = new JMXServiceURL(url);
    JMXConnector conn = JMXConnectorFactory.connect(serviceURL);
    return conn.getMBeanServerConnection();
  }

  private static List<ObjectName> getFederatedGemfireBeanObjectNames(
      MBeanServerConnection remoteMBS)
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

  private static List<String> canonicalBeanNamesFor(MemberVM member) {
    return member.invoke(JMXMBeanReconnectDUnitTest::getLocalCanonicalBeanNames);
  }

  private static List<String> getLocalCanonicalBeanNames() {
    Cache cache = ClusterStartupRule.getCache();
    SystemManagementService service =
        (SystemManagementService) ManagementService.getExistingManagementService(cache);
    Map<ObjectName, Object> gfBeanMap = service.getJMXAdapter().getLocalGemFireMBean();
    return gfBeanMap.keySet().stream()
        .map(ObjectName::getCanonicalName)
        .sorted()
        .collect(toList());
  }

  private void waitForLocatorsToAgreeOnMembership() {
    waitAtMost(1, MINUTES)
        .until(
            () -> {
              int locator1BeanCount =
                  getFederatedGemfireBeansFrom(locator1)
                      .size();
              int locator2BeanCount =
                  getFederatedGemfireBeansFrom(locator2)
                      .size();
              return locator1BeanCount == locator2BeanCount;
            });
  }

  private static Predicate<ObjectName> excludingBeansFor(String memberName) {
    return b -> !b.getCanonicalName().contains("member=" + memberName);
  }

  private static String jmxBeanLocalhostUrlString(int port) {
    return "service:jmx:rmi:///jndi/rmi://localhost"
        + ":" + port + "/jmxrmi";
  }

  private Properties locator1Properties() {
    Properties props = new Properties();
    props.setProperty(ConfigurationProperties.JMX_MANAGER_HOSTNAME_FOR_CLIENTS, "localhost");
    props.setProperty(ConfigurationProperties.JMX_MANAGER_PORT, "" + locator1JmxPort);
    props.setProperty(ConfigurationProperties.NAME, LOCATOR_1_NAME);
    props.setProperty(ConfigurationProperties.MAX_WAIT_TIME_RECONNECT, "5000");
    return props;
  }

  private Properties locator2Properties() {
    Properties props = new Properties();
    props.setProperty(ConfigurationProperties.JMX_MANAGER_HOSTNAME_FOR_CLIENTS, "localhost");
    props.setProperty(ConfigurationProperties.JMX_MANAGER_PORT, "" + locator2JmxPort);
    props.setProperty(ConfigurationProperties.NAME, LOCATOR_2_NAME);
    props.setProperty(ConfigurationProperties.LOCATORS, "localhost[" + locator1.getPort() + "]");
    return props;
  }
}
