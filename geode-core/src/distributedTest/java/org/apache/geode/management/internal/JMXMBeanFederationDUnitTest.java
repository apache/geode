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
package org.apache.geode.management.internal;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.dunit.internal.InternalBlackboard;
import org.apache.geode.test.dunit.internal.InternalBlackboardImpl;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.JMXTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.MBeanServerConnectionRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

@Category({JMXTest.class})
public class JMXMBeanFederationDUnitTest {
  private static final String LOCATOR_1_NAME = "locator-one";
  private static final String LOCATOR_2_NAME = "locator-two";
  private static final String REGION_PATH = "/test-region-1";
  private static final int LOCATOR_1_VM_INDEX = 0;
  private static final int LOCATOR_2_VM_INDEX = 4;
  private static final int LOCATOR_COUNT = 1;
  private static final int SERVER_1_VM_INDEX = 1;
  private static final int SERVER_2_VM_INDEX = 2;
  private static final int SERVER_3_VM_INDEX = 3;
  private static int SERVER_COUNT = 2;

  private int locator1JmxPort;
  private int locator2JmxPort;

  private MemberVM locator1, locator2, server1, server2, server3;

  private InternalBlackboard bb;

  @Rule
  public ClusterStartupRule lsRule = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public MBeanServerConnectionRule jmxConnectionRule = new MBeanServerConnectionRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Before
  public void before() throws Exception {
    locator1JmxPort = AvailablePortHelper.getRandomAvailableTCPPorts(LOCATOR_COUNT)[0];
    locator1 = lsRule.startLocatorVM(LOCATOR_1_VM_INDEX, locator1Properties());

    server1 = lsRule.startServerVM(SERVER_1_VM_INDEX, locator1.getPort());
    server2 = lsRule.startServerVM(SERVER_2_VM_INDEX, locator1.getPort());

    gfsh.connectAndVerify(locator1);
    gfsh.executeAndAssertThat(
        "create region --type=REPLICATE --name=" + REGION_PATH + " --enable-statistics=true")
        .statusIsSuccess();
    gfsh.disconnect();

    locator1.waitUntilRegionIsReadyOnExactlyThisManyServers(REGION_PATH, SERVER_COUNT);

    bb = InternalBlackboardImpl.getInstance();
  }

  @Test
  public void MBeanFederationAddRemoveServer() throws IOException {
    List<String> initialMBeans = getFederatedGemfireBeansFrom(locator1);

    server3 = lsRule.startServerVM(SERVER_3_VM_INDEX, locator1.getPort());
    SERVER_COUNT++;
    locator1.waitUntilRegionIsReadyOnExactlyThisManyServers(REGION_PATH, SERVER_COUNT);
    List keyset = server3.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      InternalDistributedMember member =
          InternalDistributedSystem.getConnectedInstance().getDistributedMember();
      String appender = MBeanJMXAdapter.getUniqueIDForMember(member);
      Region monitoringRegion =
          cache.getRegion(ManagementConstants.MONITORING_REGION + "_" + appender);
      List l = (List<String>) monitoringRegion.keySet().stream().collect(Collectors.toList());
      return l;
    });

    List<String> intermediateMBeans = getFederatedGemfireBeansFrom(locator1);
    List<String> expectedMBeans = new ArrayList<>();
    expectedMBeans.addAll(initialMBeans);
    expectedMBeans.addAll(keyset);
    expectedMBeans = expectedMBeans.stream().sorted().collect(Collectors.toList());
    intermediateMBeans = intermediateMBeans.stream().sorted().collect(Collectors.toList());
    assertThat(intermediateMBeans).containsExactlyElementsOf(expectedMBeans);

    lsRule.stop(SERVER_3_VM_INDEX);
    SERVER_COUNT--;
    locator1.waitUntilRegionIsReadyOnExactlyThisManyServers(REGION_PATH, SERVER_COUNT);

    List<String> finalMBeans = getFederatedGemfireBeansFrom(locator1);

    assertThat(finalMBeans).containsExactlyElementsOf(initialMBeans);
  }

  private static List<String> getFederatedGemfireBeansFrom(MemberVM member)
      throws IOException {
    String url = jmxBeanLocalhostUrlString(member.getJmxPort());
    MBeanServerConnection remoteMBS = connectToMBeanServer(url);
    Set<ObjectName> allBeanNames = remoteMBS.queryNames(null, null);
    // Each locator will have a "Manager" bean that is a part of the above query,
    // representing the ManagementAdapter.
    // This bean is registered (and so included in its own queries),
    // but *not* federated (and so is not included in another locator's bean queries).
    // For the scope of this test, we do not consider these "service=Manager" beans.
    Set<String> allBeans = new HashSet<>();
    for (ObjectName bean : allBeanNames) {
      allBeans.add(bean.toString());
    }

    return allBeans.stream()
        .filter(b -> b.contains("GemFire"))
        .sorted()
        .collect(toList());
  }

  private static MBeanServerConnection connectToMBeanServer(String url) throws IOException {
    final JMXServiceURL serviceURL = new JMXServiceURL(url);
    JMXConnector conn = JMXConnectorFactory.connect(serviceURL);
    return conn.getMBeanServerConnection();
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
    return props;
  }

  private Properties locator2Properties() {
    locator2JmxPort = AvailablePortHelper.getRandomAvailableTCPPorts(LOCATOR_COUNT)[0];
    Properties props = new Properties();
    props.setProperty(ConfigurationProperties.JMX_MANAGER_HOSTNAME_FOR_CLIENTS, "localhost");
    props.setProperty(ConfigurationProperties.JMX_MANAGER_PORT, "" + locator2JmxPort);
    props.setProperty(ConfigurationProperties.NAME, LOCATOR_2_NAME);
    return props;
  }
}
