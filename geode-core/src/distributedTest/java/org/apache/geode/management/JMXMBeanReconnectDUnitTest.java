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
import static org.apache.geode.internal.Assert.fail;
import static org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase.getBlackboard;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.SoftAssertions.assertSoftly;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import com.google.common.base.Stopwatch;
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
  private static final int NUM_BEANS = 19;
  private static final int TIMEOUT = 300;
  private int locator1JmxPort, locator2JmxPort;

  private MemberVM locator1, locator2, server1;

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
    locator1JmxPort = AvailablePortHelper.getRandomAvailableTCPPort();
    locator1 = lsRule.startLocatorVM(LOCATOR_1_VM_INDEX, locator1Properties());

    locator2JmxPort = AvailablePortHelper.getRandomAvailableTCPPort();
    locator2 = lsRule.startLocatorVM(LOCATOR_2_VM_INDEX, locator2Properties(), locator1.getPort());

    server1 = lsRule.startServerVM(SERVER_1_VM_INDEX, locator1.getPort());
    // start an extra server to have more MBeans, but we don't need to use it in these tests
    lsRule.startServerVM(SERVER_2_VM_INDEX, locator1.getPort());

    gfsh.connectAndVerify(locator1);
    gfsh.executeAndAssertThat(
        "create region --type=REPLICATE --name=" + REGION_PATH + " --enable-statistics=true")
        .statusIsSuccess();

    locator1.waitUntilRegionIsReadyOnExactlyThisManyServers(REGION_PATH, SERVER_COUNT);

    waitForLocatorsToAgreeOnMembership();
  }

  /**
   * Test that a server's local MBeans are not affected by a locator crashing
   */
  @Test
  public void testLocalBeans_MaintainServerAndCrashLocator() {
    List<String> initialServerBeans = canonicalBeanNamesFor(server1);

    locator1.forceDisconnect();

    List<String> intermediateServerBeans = canonicalBeanNamesFor(server1);

    // check the beans (must be checked twice as assertion is not commutative)
    assertThat(intermediateServerBeans).containsExactlyElementsOf(initialServerBeans);
    assertThat(initialServerBeans).containsExactlyElementsOf(intermediateServerBeans);

    locator1.waitTilLocatorFullyReconnected();

    List<String> finalServerBeans = canonicalBeanNamesFor(server1);

    // check the beans (must be checked twice as assertion is not commutative)
    assertThat(finalServerBeans).containsExactlyElementsOf(initialServerBeans);
    assertThat(initialServerBeans).containsExactlyElementsOf(finalServerBeans);
  }

  /**
   * Test that a locator's local MBeans are not affected by a server crashing
   */
  @Test
  public void testLocalBeans_MaintainLocatorAndCrashServer() {
    List<String> initialLocatorBeans = canonicalBeanNamesFor(locator1);

    server1.forceDisconnect();

    List<String> intermediateLocatorBeans = canonicalBeanNamesFor(locator1);

    // check the beans (must be checked twice as assertion is not commutative)
    assertThat(intermediateLocatorBeans).containsExactlyElementsOf(initialLocatorBeans);
    assertThat(initialLocatorBeans).containsExactlyElementsOf(intermediateLocatorBeans);

    server1.waitTilServerFullyReconnected();
    locator1.waitUntilRegionIsReadyOnExactlyThisManyServers(REGION_PATH, SERVER_COUNT);

    List<String> finalLocatorBeans = canonicalBeanNamesFor(locator1);

    // check the beans (must be checked twice as assertion is not commutative)
    assertThat(finalLocatorBeans).containsExactlyElementsOf(initialLocatorBeans);
    assertThat(initialLocatorBeans).containsExactlyElementsOf(finalLocatorBeans);
  }

  /**
   * Test MBean consistency when disconnecting and reconnecting the lead locator. MBeans should
   * remain the same after a member reconnects as they were before the disconnect. MBeans (other
   * than local MBeans, which are filtered for this test) should be consistent between locators.
   * All MBeans not related to the killed member should remain the same when a member is killed.
   */
  @Test
  public void testRemoteBeanKnowledge_MaintainServerAndCrashLocator()
      throws IOException, InterruptedException {
    Stopwatch stopwatch = Stopwatch.createStarted();

    // check that the initial state is good
    List<ObjectName> initialL1Beans = getFederatedGemfireBeansFrom(locator1);
    List<ObjectName> initialL2Beans = getFederatedGemfireBeansFrom(locator2);
    checkBeanListConsistency(initialL1Beans, initialL2Beans);

    // calculate the expected list for use once the locator has crashed
    List<ObjectName> expectedIntermediateBeanList = initialL1Beans.stream()
        .filter(excludingBeansFor(LOCATOR_1_NAME)).collect(toList());

    // crash the locator
    locator1.forceDisconnect(TIMEOUT, RECONNECT_MAILBOX);

    // wait for the locator's crash to federate to the remaining locator
    Boolean foundMember;
    List<ObjectName> intermediateL2Beans = new ArrayList<>();

    stopwatch.reset().start();
    do {
      foundMember = false;
      intermediateL2Beans.clear();

      intermediateL2Beans.addAll(getFederatedGemfireBeansFrom(locator2));

      // check that the timeout has not been reached or exceeded
      if (stopwatch.elapsed(TimeUnit.SECONDS) >= TIMEOUT) {
        fail("Condition was not true within " + TIMEOUT + " seconds.\n"
            + " Member's stop did not federate to other members.\n"
            + "Locator2 had beans:\n" + intermediateL2Beans);
      }

      if (intermediateL2Beans.size() == NUM_BEANS) {
        foundMember = true;
        Thread.sleep(1000); // make the busy wait slightly less busy
      } else {
        if (checkForMemberBeans(LOCATOR_1_NAME, intermediateL2Beans)) {
          foundMember = true;
          Thread.sleep(1000); // make the busy wait slightly less busy
        }
      }
    } while (foundMember);

    // allow locator 1 to start reconnecting
    locator1.invoke(() -> getBlackboard().setMailbox(RECONNECT_MAILBOX, true));

    // check that we got the MBeans that we were expecting while the locator was crashed
    checkCurrentAgainstExpectedBeanLists(expectedIntermediateBeanList.size(),
        expectedIntermediateBeanList, intermediateL2Beans);

    // wait for the locator's restart to federate to the other locator
    Boolean restartFederated;
    List<ObjectName> finalL1Beans = new ArrayList<>();
    List<ObjectName> finalL2Beans = new ArrayList<>();

    stopwatch.reset().start();
    do {
      restartFederated = true;
      finalL1Beans.clear();
      finalL2Beans.clear();

      finalL2Beans.addAll(getFederatedGemfireBeansFrom(locator2));

      // check that the timeout has not been reached or exceeded
      if (stopwatch.elapsed(TimeUnit.SECONDS) >= TIMEOUT) {
        fail("Condition was not true within " + TIMEOUT + " seconds.\n"
            + " Member's restart did not federate to members.\n"
            + "Locator2 had beans:\n" + intermediateL2Beans);
      }

      if (finalL2Beans.size() != NUM_BEANS) {
        restartFederated = false;
        Thread.sleep(1000); // make the busy wait slightly less busy
      } else {
        // the locator's restart is complete, collect its MBeans to check
        finalL1Beans.addAll(getFederatedGemfireBeansFrom(locator1));
      }
    } while (!restartFederated);

    // check that our final state is correct
    checkCurrentAgainstExpectedBeanLists(NUM_BEANS, initialL1Beans, finalL1Beans, finalL2Beans);
  }

  /**
   * Test MBean consistency when disconnecting and reconnecting a server. MBeans should
   * remain the same after a member reconnects as they were before the disconnect. MBeans (other
   * than local MBeans, which are filtered for this test) should be consistent between locators.
   * All MBeans not related to the killed member should remain the same when a member is killed.
   */
  @Test
  public void testRemoteBeanKnowledge_MaintainLocatorAndCrashServer()
      throws IOException, InterruptedException {
    Stopwatch stopwatch = Stopwatch.createStarted();

    // check that the initial state is correct
    List<ObjectName> initialL1Beans = getFederatedGemfireBeansFrom(locator1);
    List<ObjectName> initialL2Beans = getFederatedGemfireBeansFrom(locator2);
    checkBeanListConsistency(initialL1Beans, initialL2Beans);

    // calculate the expected list of MBeans when the server has crashed
    List<ObjectName> expectedIntermediateBeanList = initialL1Beans.stream()
        .filter(excludingBeansFor("server-" + SERVER_1_VM_INDEX)).collect(toList());

    // crash the server
    server1.forceDisconnect(TIMEOUT, RECONNECT_MAILBOX);

    // wait for the server's crash to federate to the locators
    Boolean foundMember;
    String name = "server-" + SERVER_1_VM_INDEX;
    List<ObjectName> intermediateL1Beans = new ArrayList<>();
    List<ObjectName> intermediateL2Beans = new ArrayList<>();

    stopwatch.reset().start();
    do {
      foundMember = false;
      intermediateL1Beans.clear();
      intermediateL2Beans.clear();

      intermediateL1Beans.addAll(getFederatedGemfireBeansFrom(locator1));
      intermediateL2Beans.addAll(getFederatedGemfireBeansFrom(locator2));

      // check if the timeout has been reached or exceeded
      if (stopwatch.elapsed(TimeUnit.SECONDS) >= TIMEOUT) {
        fail("Condition was not true within " + TIMEOUT + " seconds.\n"
            + " Member's stop did not federate to other members.\n"
            + "Locator1 had beans:\n" + intermediateL1Beans + "\n"
            + "Locator2 had beans:\n" + intermediateL2Beans);
      }

      if (intermediateL1Beans.size() != intermediateL2Beans.size()) {
        foundMember = true;
        Thread.sleep(1000); // make the busy wait slightly less busy
      } else {
        if (checkForMemberBeans(name, intermediateL1Beans)
            || checkForMemberBeans(name, intermediateL2Beans)) {
          foundMember = true;
          Thread.sleep(1000); // make the busy wait slightly less busy
        }
      }
    } while (foundMember);

    // allow the server to start reconnecting
    server1.invoke(() -> getBlackboard().setMailbox(RECONNECT_MAILBOX, true));

    // check that the beans were correct when the server is crashed
    checkCurrentAgainstExpectedBeanLists(expectedIntermediateBeanList.size(),
        expectedIntermediateBeanList, intermediateL1Beans, intermediateL2Beans);

    // wait for the server's restart to federate to the locators
    Boolean restartFederated;
    List<ObjectName> finalL1Beans = new ArrayList<>();
    List<ObjectName> finalL2Beans = new ArrayList<>();

    stopwatch.reset().start();
    do {
      restartFederated = true;
      finalL1Beans.clear();
      finalL2Beans.clear();

      finalL1Beans.addAll(getFederatedGemfireBeansFrom(locator1));
      finalL2Beans.addAll(getFederatedGemfireBeansFrom(locator2));

      // check if the timeout has been reached or exceeded
      if (stopwatch.elapsed(TimeUnit.SECONDS) >= TIMEOUT) {
        fail("Condition was not true within " + TIMEOUT + " seconds.\n"
            + " Member's restart did not federate to members.\n"
            + "Locator1 had beans:\n" + intermediateL1Beans + "\n"
            + "Locator2 had beans:\n" + intermediateL2Beans);
      }

      if (finalL1Beans.size() != NUM_BEANS && finalL2Beans.size() != NUM_BEANS) {
        restartFederated = false;
        Thread.sleep(1000); // make the busy wait slightly less busy
      }
    } while (!restartFederated);

    // check that our final state is good
    checkCurrentAgainstExpectedBeanLists(NUM_BEANS, initialL1Beans, finalL1Beans, finalL2Beans);
  }

  private boolean checkForMemberBeans(String memberName, List<ObjectName> beans) {
    for (ObjectName bean : beans) {
      if (bean.getKeyProperty("member") != null
          && bean.getKeyProperty("member").equals(memberName)) {
        return true;
      }
    }

    return false;
  }

  /**
   * Compares two lists. Both lists must have the same elements and the same size
   */
  private void checkBeanListConsistency(List<ObjectName> list1, List<ObjectName> list2) {
    assertSoftly(softly -> {
      // check the beans (must be checked twice as assertion is not commutative)
      softly.assertThat(list1).containsExactlyElementsOf(list2);
      softly.assertThat(list2).containsExactlyElementsOf(list1);
      softly.assertThat(list1).hasSameSizeAs(list2);
    });
  }

  /**
   * Checks three lists using soft assertions. All lists must have the same elements and have size
   * equal to the number of expected MBeans.
   */
  private void checkCurrentAgainstExpectedBeanLists(int numBeans,
      List<ObjectName> expectedBeans,
      List<ObjectName> currentL1Beans,
      List<ObjectName> currentL2Beans) {
    assertSoftly(softly -> {
      // check that the locators agree on the beans
      // check must be done twice as assertion is not commutative
      softly.assertThat(currentL1Beans).containsExactlyElementsOf(currentL2Beans);
      softly.assertThat(currentL2Beans).containsExactlyElementsOf(currentL1Beans);
      // check that there are no unexpected MBeans
      softly.assertThat(currentL1Beans).containsExactlyElementsOf(expectedBeans);
      softly.assertThat(currentL1Beans).hasSize(numBeans);
    });
  }

  private void checkCurrentAgainstExpectedBeanLists(int numBeans,
      List<ObjectName> expectedBeans,
      List<ObjectName> currentBeans) {
    assertSoftly(softly -> {
      // check that the lists are the same
      // check must be done twice as assertion is not commutative
      softly.assertThat(currentBeans).containsExactlyElementsOf(expectedBeans);
      softly.assertThat(currentBeans).containsExactlyElementsOf(expectedBeans);
      softly.assertThat(currentBeans).hasSize(numBeans);
    });
  }

  /**
   * Returns a list of remote MBeans from the given member. The MBeans are filtered to exclude the
   * member's local MBeans. The resulting list includes only MBeans that all locators in the system
   * should have.
   *
   * @param member - the locator to get the MBeans from
   * @return List<ObjectName> - a filtered and sorted list of MBeans from the given member
   */
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

  /**
   * Gets a list of local MBeans from the given member. This list of MBeans does not include beans
   * for members other than the given member
   */
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

  /**
   * Waits until the two locators have the same MBeans, and have NUM_BEANS beans.
   *
   * This can't be simply achieved with assertions and awaitilities. The simple implementation
   * causes out of memory errors. This implementation slows down the checks so that GC has time to
   * clean up, and uses only one thread for checking.
   *
   * This method will return when the locators have the same beans and the expected number of beans.
   * If it does not complete within GeodeAwaitility.DEFAULT_TIMEOUT seconds, a TimeoutException will
   * be thrown.
   */
  private void waitForLocatorsToAgreeOnMembership()
      throws InterruptedException, IOException {
    List<ObjectName> l1Beans = new ArrayList<>();
    List<ObjectName> l2Beans = new ArrayList<>();
    AtomicBoolean consistent = new AtomicBoolean();

    Stopwatch stopwatch = Stopwatch.createStarted();
    do {
      // reset everything
      consistent.set(true);
      l1Beans.clear();
      l2Beans.clear();

      // get the MBeans and add them to lists to check them
      l1Beans.addAll(getFederatedGemfireBeansFrom(locator1));
      l2Beans.addAll(getFederatedGemfireBeansFrom(locator2));

      if (stopwatch.elapsed(TimeUnit.SECONDS) >= TIMEOUT) {
        fail("Locators could not agree on the state of the system within " + TIMEOUT + " seconds.\n"
            + "Locator1 had MBeans:\n" + l1Beans
            + "Locator2 had MBeans:\n" + l2Beans);
      }

      // if there aren't enough beans, wait then loop again
      if (l1Beans.size() != NUM_BEANS || l2Beans.size() != NUM_BEANS) {
        consistent.set(false);
        Thread.sleep(1000);
        // if any beans aren't the same between the two lists, wait then loop again
      } else {
        for (int i = 0; i < NUM_BEANS; i++) {
          if (!l1Beans.get(i).equals(l2Beans.get(i))) {
            consistent.set(false);
            Thread.sleep(1000);
            break;
          }
        }
      }

      if (stopwatch.elapsed(TimeUnit.SECONDS) >= TIMEOUT) {
        fail("Locators could not agree on the state of the system within " + TIMEOUT + " seconds.\n"
            + "Locator1 had MBeans:\n" + l1Beans
            + "Locator2 had MBeans:\n" + l2Beans);
      }
    } while (!consistent.get());
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
