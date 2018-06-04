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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.awaitility.Awaitility;
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
  private MemberVM locator1, locator2, server1, server2;

  private int jmxPort1;
  private String locator1Name = "locator-one";

  private int jmxPort2;
  private String locator2Name = "locator-two";

  private String regionName1 = "test-region-1";

  @Rule
  public ClusterStartupRule lsRule = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public MBeanServerConnectionRule jmxConnectionRule = new MBeanServerConnectionRule();

  private int locator1VMIndex = 0;
  private int locator2VMIndex = 1;
  private int server1VMIndex = 2;
  private int server2VMIndex = 3;
  private int restoredMemberVMIndex = 4;

  @Before
  public void before() throws Exception {
    int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    jmxPort1 = ports[0];
    jmxPort2 = ports[1];

    // Start a locator1, a server1, another locator1, and a region
    locator1 = lsRule.startLocatorVM(locator1VMIndex, getLocator1Properties());
    locator2 = lsRule.startLocatorVM(locator2VMIndex, getLocator2Properties(), locator1.getPort());

    server1 = lsRule.startServerVM(server1VMIndex, locator1.getPort());
    server2 = lsRule.startServerVM(server2VMIndex, locator1.getPort());

    gfsh.connectAndVerify(locator1);
    gfsh.executeAndAssertThat("create region --type=REPLICATE --name=/" + regionName1)
        .statusIsSuccess();

    // Avoid a minor race condition if locators haven't come to terms yet.
    Awaitility.waitAtMost(1, TimeUnit.MINUTES)
        .until(
            () -> getAllFederatedGemfireBeanObjectNamesFromRemoteBeanServer(locator1.getJmxPort())
                .size() == getAllFederatedGemfireBeanObjectNamesFromRemoteBeanServer(
                    locator2.getJmxPort())
                        .size());
    locator1.waitTillRegionsAreReadyOnServers("/" + regionName1, 2);
  }

  @Test
  public void testLocalBeans_MaintainServerAndCrashLocator() {
    List<String> initialServerBeans = server1.invoke(
        JMXMBeanReconnectDUnitTest::getLocalCanonicalBeanNames);

    int portOfCrashedMember = locator1.getPort();
    lsRule.stopVM(locator1VMIndex);

    List<String> intermediateServerBeans = server1.invoke(
        JMXMBeanReconnectDUnitTest::getLocalCanonicalBeanNames);

    Properties newLocator1Props = getLocator1Properties();
    newLocator1Props.setProperty(ConfigurationProperties.LOCATORS,
        "localhost[" + locator2.getPort() + "]");

    locator1 = lsRule.startLocatorVM(restoredMemberVMIndex,
        member -> member.withPort(portOfCrashedMember).withProperties(newLocator1Props));
    locator1.waitTillRegionsAreReadyOnServers("/" + regionName1, 2);

    List<String> finalServerBeans = server1.invoke(
        JMXMBeanReconnectDUnitTest::getLocalCanonicalBeanNames);

    assertThat(initialServerBeans)
        .containsExactlyElementsOf(intermediateServerBeans)
        .containsExactlyElementsOf(finalServerBeans);
  }

  @Test
  public void testLocalBeans_MaintainLocatorAndCrashServer() {
    List<String> initialLocatorBeans = locator1.invoke(
        JMXMBeanReconnectDUnitTest::getLocalCanonicalBeanNames);

    int portOfCrashedMember = server1.getPort();
    lsRule.stopVM(server1VMIndex);

    List<String> intermediateLocatorBeans = locator1.invoke(
        JMXMBeanReconnectDUnitTest::getLocalCanonicalBeanNames);

    int locator1Port = locator1.getPort();
    server1 = lsRule.startServerVM(restoredMemberVMIndex,
        member -> member.withPort(portOfCrashedMember).withConnectionToLocator(locator1Port));
    locator1.waitTillRegionsAreReadyOnServers("/" + regionName1, 2);

    List<String> finalLocatorBeans = locator1.invoke(
        JMXMBeanReconnectDUnitTest::getLocalCanonicalBeanNames);

    assertThat(initialLocatorBeans)
        .containsExactlyElementsOf(intermediateLocatorBeans)
        .containsExactlyElementsOf(finalLocatorBeans);
  }

  @Test
  public void testRemoteBeanKnowledge_MaintainServerAndCrashLocator() throws IOException {
    List<ObjectName> initialLocator1GemfireBeanList =
        getAllFederatedGemfireBeanObjectNamesFromRemoteBeanServer(locator1.getJmxPort());
    List<ObjectName> initialLocator2GemfireBeanList =
        getAllFederatedGemfireBeanObjectNamesFromRemoteBeanServer(locator2.getJmxPort());

    assertThat(initialLocator1GemfireBeanList)
        .containsExactlyElementsOf(initialLocator2GemfireBeanList);

    int portOfCrashedMember = locator1.getPort();
    lsRule.stopVM(locator1VMIndex);

    List<ObjectName> intermediateLocator2GemfireBeanList =
        getAllFederatedGemfireBeanObjectNamesFromRemoteBeanServer(locator2.getJmxPort());

    // We expect all the same beans, excepting those of the recently-crashed member.
    assertThat(intermediateLocator2GemfireBeanList)
        .containsExactlyElementsOf(
            initialLocator2GemfireBeanList.stream()
                .filter(b -> !b.getCanonicalName().contains(locator1Name))
                .collect(Collectors.toList()));

    Properties newLocator1Props = getLocator1Properties();
    newLocator1Props.setProperty(ConfigurationProperties.LOCATORS,
        "localhost[" + locator2.getPort() + "]");

    locator1 = lsRule.startLocatorVM(restoredMemberVMIndex,
        member -> member.withProperties(newLocator1Props).withPort(portOfCrashedMember));
    locator1.waitTillRegionsAreReadyOnServers("/" + regionName1, 2);

    // Give the locators time to communicate membership.
    Awaitility.waitAtMost(1, TimeUnit.MINUTES)
        .until(
            () -> getAllFederatedGemfireBeanObjectNamesFromRemoteBeanServer(locator1.getJmxPort())
                .size() == getAllFederatedGemfireBeanObjectNamesFromRemoteBeanServer(
                    locator2.getJmxPort())
                        .size());

    List<ObjectName> finalLocator1GemfireBeanList =
        getAllFederatedGemfireBeanObjectNamesFromRemoteBeanServer(locator1.getJmxPort());
    List<ObjectName> finalLocator2GemfireBeanList =
        getAllFederatedGemfireBeanObjectNamesFromRemoteBeanServer(locator2.getJmxPort());

    assertThat(finalLocator1GemfireBeanList)
        .containsExactlyElementsOf(finalLocator2GemfireBeanList)
        .containsExactlyElementsOf(initialLocator2GemfireBeanList);
  }

  @Test
  public void testRemoteBeanKnowledge_MaintainLocatorAndCrashServer() throws IOException {
    List<ObjectName> initialLocator1GemfireBeanList =
        getAllFederatedGemfireBeanObjectNamesFromRemoteBeanServer(locator1.getJmxPort());
    List<ObjectName> initialLocator2GemfireBeanList =
        getAllFederatedGemfireBeanObjectNamesFromRemoteBeanServer(locator2.getJmxPort());

    assertThat(initialLocator1GemfireBeanList)
        .containsExactlyElementsOf(initialLocator2GemfireBeanList);

    int portOfCrashedMember = server1.getPort();
    lsRule.stopVM(server1VMIndex);

    List<ObjectName> intermediateLocator1GemfireBeanList =
        getAllFederatedGemfireBeanObjectNamesFromRemoteBeanServer(locator1.getJmxPort());
    List<ObjectName> intermediateLocator2GemfireBeanList =
        getAllFederatedGemfireBeanObjectNamesFromRemoteBeanServer(locator2.getJmxPort());

    assertThat(intermediateLocator2GemfireBeanList)
        .containsExactlyElementsOf(intermediateLocator1GemfireBeanList)
        .containsExactlyElementsOf(
            initialLocator2GemfireBeanList.stream()
                .filter(b -> !b.getCanonicalName().contains("member=server-1"))
                .collect(Collectors.toList()))
        .containsExactlyElementsOf(
            initialLocator1GemfireBeanList.stream()
                .filter(b -> !b.getCanonicalName().contains("member=server-1"))
                .collect(Collectors.toList()));


    Properties spoofServer1Properties = new Properties();
    spoofServer1Properties.setProperty("name", "server-1");

    int locator1Port = locator1.getPort();
    server1 = lsRule.startServerVM(restoredMemberVMIndex,
        member -> member.withPort(portOfCrashedMember).withProperties(spoofServer1Properties)
            .withConnectionToLocator(locator1Port));

    locator1.waitTillRegionsAreReadyOnServers("/" + regionName1, 2);

    List<ObjectName> finalLocator1GemfireBeanList =
        getAllFederatedGemfireBeanObjectNamesFromRemoteBeanServer(locator1.getJmxPort());
    List<ObjectName> finalLocator2GemfireBeanList =
        getAllFederatedGemfireBeanObjectNamesFromRemoteBeanServer(locator2.getJmxPort());

    assertThat(finalLocator1GemfireBeanList)
        .containsExactlyElementsOf(finalLocator2GemfireBeanList)
        .containsExactlyElementsOf(initialLocator2GemfireBeanList);
  }

  private MBeanServerConnection getmBeanServerConnection(String url) throws IOException {
    final JMXServiceURL serviceURL = new JMXServiceURL(url);
    JMXConnector conn = JMXConnectorFactory.connect(serviceURL);
    return conn.getMBeanServerConnection();
  }

  private List<ObjectName> getFederatedGemfireBeanObjectNames(MBeanServerConnection remoteMBS)
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
        .collect(Collectors.toList());
  }

  private List<ObjectName> getAllFederatedGemfireBeanObjectNamesFromRemoteBeanServer(int port)
      throws IOException {
    String url = getJmxBeanLocalhostUrlString(port);
    MBeanServerConnection remoteMBS = getmBeanServerConnection(url);
    return getFederatedGemfireBeanObjectNames(remoteMBS);
  }

  private static List<String> getLocalCanonicalBeanNames() {
    Cache cache = ClusterStartupRule.getCache();
    SystemManagementService service =
        (SystemManagementService) ManagementService.getExistingManagementService(cache);
    Map<ObjectName, Object> gfBeanMap = service.getJMXAdapter().getLocalGemFireMBean();
    return gfBeanMap.keySet().stream().map(ObjectName::getCanonicalName).sorted()
        .collect(Collectors.toList());
  }

  private String getJmxBeanLocalhostUrlString(int port) {
    return "service:jmx:rmi:///jndi/rmi://localhost"
        + ":" + port + "/jmxrmi";
  }

  private Properties getLocator1Properties() {
    Properties props = new Properties();
    props.setProperty(ConfigurationProperties.JMX_MANAGER_HOSTNAME_FOR_CLIENTS, "localhost");
    props.setProperty(ConfigurationProperties.JMX_MANAGER_PORT, "" + jmxPort1);
    props.setProperty(ConfigurationProperties.NAME, locator1Name);
    return props;
  }

  private Properties getLocator2Properties() {
    Properties props = new Properties();
    props.setProperty(ConfigurationProperties.JMX_MANAGER_HOSTNAME_FOR_CLIENTS, "localhost");
    props.setProperty(ConfigurationProperties.JMX_MANAGER_PORT, "" + jmxPort2);
    props.setProperty(ConfigurationProperties.NAME, locator2Name);
    props.setProperty(ConfigurationProperties.LOCATORS, "localhost[" + locator1.getPort() + "]");
    return props;
  }
}
