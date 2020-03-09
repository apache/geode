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
package org.apache.geode.session.tests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.IntSupplier;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.geode.internal.UniquePortSupplier;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.modules.session.functions.GetMaxInactiveInterval;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.SessionTest;

/**
 * Base class for test of session replication.
 *
 * This class contains all of the tests of session replication functionality. Subclasses of this
 * class configure different containers in order to run these tests against specific containers.
 */
@Category({SessionTest.class})
public abstract class CargoTestBase {
  private final UniquePortSupplier portSupplier = new UniquePortSupplier();

  @Rule
  public TestName testName = new TestName();

  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule(2);

  protected Client client;
  protected ContainerManager manager;
  protected ContainerInstall install;
  protected MemberVM locatorVM;
  protected boolean dumpLogs = true;
  protected int numberOfContainers = 2;

  /**
   * Should only be called once per test.
   *
   * @return a newly initialized ContainerInstall
   */
  public abstract ContainerInstall getInstall(IntSupplier portSupplier) throws Exception;

  /**
   * Sets up the {@link #client} and {@link #manager} variables by creating new instances of each.
   *
   * Adds two new containers to the {@link #manager} based on the subclass's
   * {@link #getInstall(IntSupplier)} method. Also sets the test name in the {@link #manager} to the
   * name of the current test.
   */
  @Before
  public void setup() throws Exception {
    dumpDockerInfo();
    announceTest("START");

    locatorVM = clusterStartupRule.startLocatorVM(0, 0);

    client = new Client();
    manager = new ContainerManager();

    manager.setTestName(testName.getMethodName());

    install = getInstall(portSupplier::getAvailablePort);
    install.setDefaultLocatorPort(locatorVM.getPort());

    manager.addContainers(numberOfContainers, install);

    customizeContainers();
  }

  public void customizeContainers() throws Exception {}

  /**
   * Stops all containers that were previously started and cleans up their configurations
   */
  @After
  public void stop() throws IOException, InterruptedException {
    try {
      manager.stopAllActiveContainers();
    } finally {
      try {
        if (dumpLogs) {
          manager.dumpLogs();
        }
      } finally {
        try {
          manager.cleanUp();
        } finally {
          announceTest("END");
        }
      }
    }
  }

  /**
   * Gets the specified key from all the containers within the container manager and check that each
   * container has the associated expected value
   */
  public void getKeyValueDataOnAllClients(String key, String expectedValue, String expectedCookie)
      throws IOException, URISyntaxException {
    getKeyValueDataOnAllClients(client, key, expectedValue, expectedCookie);
  }

  public void getKeyValueDataOnAllClients(Client client, String key, String expectedValue,
      String expectedCookie)
      throws IOException, URISyntaxException {
    for (int i = 0; i < manager.numContainers(); i++) {
      // Set the port for this server
      client.setPort(Integer.parseInt(manager.getContainerPort(i)));
      // Get the response to a get on the specified key from this server
      Client.Response resp = client.get(key);

      // Null would mean we don't expect the same cookie as before
      if (expectedCookie != null)
        assertEquals("Sessions are not replicating properly", expectedCookie,
            resp.getSessionCookie());

      // Check that the response from this server is correct
      if (install.getConnectionType() == ContainerInstall.ConnectionType.CACHING_CLIENT_SERVER) {
        // There might be delay for other client cache to gets the update through
        // HARegionQueue
        String value = resp.getResponse();
        if (!expectedValue.equals(value)) {
          LogService.getLogger().info("verifying container {} for expected value of {}"
              + " for key {}, but gets response value of {} from client {}. Waiting for update from server.",
              i, expectedValue, key, value, client);
        }
        GeodeAwaitility.await().pollInSameThread().untilAsserted(
            () -> assertThat(getResponseValue(client, key)).isEqualTo(expectedValue));
      } else {
        // either p2p cache or client cache which has proxy/empty region - retrieving session from
        // servers
        assertEquals("Session data is not replicating properly for key " + key, expectedValue,
            resp.getResponse());
      }
    }
  }

  protected void getKeyValueDataOnOperatingClient(Client client, String key, String expectedValue,
      String expectedCookie, int operatingContainer)
      throws IOException, URISyntaxException {
    client.setPort(Integer.parseInt(manager.getContainerPort(operatingContainer)));
    Client.Response resp = client.get(key);
    if (expectedCookie != null)
      assertEquals("Sessions are not replicating properly", expectedCookie,
          resp.getSessionCookie());

    assertEquals("Session data is not replicating properly for key " + key, expectedValue,
        resp.getResponse());

  }

  private String getResponseValue(Client client, String key)
      throws IOException, URISyntaxException {
    Client.Response response = client.get(key);
    String value = response.getResponse();
    return value;
  }

  /**
   * Test that when multiple containers are using session replication, all of the containers will
   * use the same session cookie for the same client.
   */
  @Test
  public void containersShouldReplicateCookies() throws IOException, URISyntaxException {
    manager.startAllInactiveContainers();

    client.setPort(Integer.parseInt(manager.getContainerPort(0)));
    Client.Response resp = client.get(null);

    getKeyValueDataOnAllClients(null, "", resp.getSessionCookie());
  }

  /**
   * Test that when a session attribute is set in one container, it is replicated to other
   * containers
   */
  @Test
  public void containersShouldHavePersistentSessionData() throws IOException, URISyntaxException {
    manager.startAllInactiveContainers();

    String key = "value_testSessionPersists";
    String value = "Foo";

    client.setPort(Integer.parseInt(manager.getContainerPort(0)));
    Client.Response resp = client.set(key, value);

    getKeyValueDataOnAllClients(key, value, resp.getSessionCookie());
  }

  /**
   * Test that when a container fails, session attributes that were previously set in that
   * container are still available in other containers
   */
  @Test
  public void failureShouldStillAllowOtherContainersDataAccess()
      throws IOException, URISyntaxException {
    manager.startAllInactiveContainers();

    String key = "value_testSessionPersists";
    String value = "Foo";

    client.setPort(Integer.parseInt(manager.getContainerPort(0)));
    Client.Response resp = client.set(key, value);

    manager.stopContainer(0);
    manager.removeContainer(0);

    getKeyValueDataOnAllClients(key, value, resp.getSessionCookie());
  }

  /**
   * Test that invalidating a session in one container invalidates the session in all containers.
   */
  @Test
  public void invalidationShouldRemoveValueAccessForAllContainers() throws Exception {
    manager.startAllInactiveContainers();

    String key = "value_testInvalidate";
    String value = "Foo";

    client.setPort(Integer.parseInt(manager.getContainerPort(0)));
    client.set(key, value);

    client.invalidate();

    verifySessionIsRemoved(key);
  }

  protected void verifySessionIsRemoved(String key) throws IOException, URISyntaxException {
    getKeyValueDataOnAllClients(key, "", null);
  }

  /**
   * Test that if a session is not used within the expiration time, it is expired and removed from
   * all containers
   */
  @Test
  public void containersShouldExpireInSetTimeframe()
      throws IOException, URISyntaxException, InterruptedException {
    manager.startAllInactiveContainers();

    String key = "value_testSessionExpiration";
    String value = "Foo";

    client.setPort(Integer.parseInt(manager.getContainerPort(0)));
    Client.Response resp = client.set(key, value);

    getKeyValueDataOnAllClients(key, value, resp.getSessionCookie());

    client.setMaxInactive(1);
    Thread.sleep(5000);

    verifySessionIsRemoved(key);
  }

  /**
   * Test that if a session is not used within the expiration time, it is expired and removed from
   * all containers
   */
  @Test
  public void sessionPicksUpSessionTimeoutConfiguredInWebXml()
      throws IOException, URISyntaxException, InterruptedException {
    manager.startAllInactiveContainers();

    String key = "value_testSessionExpiration";
    String value = "Foo";

    client.setPort(Integer.parseInt(manager.getContainerPort(0)));
    Client.Response resp = client.set(key, value);

    // 59 minutes is the value configured in web.xml
    verifyMaxInactiveInterval(59 * 60);

    client.setMaxInactive(63);
    verifyMaxInactiveInterval(63);

  }

  protected void verifyMaxInactiveInterval(int expected) throws IOException, URISyntaxException {
    for (int i = 0; i < manager.numContainers(); i++) {
      client.setPort(Integer.parseInt(manager.getContainerPort(i)));
      if (install.getConnectionType() == ContainerInstall.ConnectionType.CACHING_CLIENT_SERVER) {
        GeodeAwaitility.await().until(() -> Integer.toString(expected)
            .equals(client.executionFunction(GetMaxInactiveInterval.class).getResponse()));
      } else {
        assertEquals(Integer.toString(expected),
            client.executionFunction(GetMaxInactiveInterval.class).getResponse());
      }
    }
  }


  /**
   * Test that if one container is accessing a session, that will prevent the session from
   * expiring in all containers.
   */
  @Test
  public void containersShouldShareSessionExpirationReset()
      throws URISyntaxException, IOException, InterruptedException {
    manager.startAllInactiveContainers();

    int timeToExp = 30;
    String key = "value_testSessionExpiration";
    String value = "Foo";

    client.setPort(Integer.parseInt(manager.getContainerPort(0)));
    Client.Response resp = client.set(key, value);
    String cookie = resp.getSessionCookie();

    resp = client.setMaxInactive(timeToExp);
    assertEquals(cookie, resp.getSessionCookie());

    long startTime = System.currentTimeMillis();
    long curTime = System.currentTimeMillis();
    // Run for 2 times the set expiration time
    while (curTime - startTime < timeToExp * 2000) {
      resp = client.get(key);
      Thread.sleep(500);
      curTime = System.currentTimeMillis();

      assertEquals("Sessions are not replicating properly", cookie, resp.getSessionCookie());
      assertEquals("Containers are not replicating session expiration reset", value,
          resp.getResponse());
    }

    getKeyValueDataOnAllClients(key, value, resp.getSessionCookie());
  }

  /**
   * Test that if a session attribute is removed in one container, it is removed from all
   * containers
   */
  @Test
  public void containersShouldShareDataRemovals() throws IOException, URISyntaxException {
    manager.startAllInactiveContainers();

    String key = "value_testSessionRemove";
    String value = "Foo";

    client.setPort(Integer.parseInt(manager.getContainerPort(0)));
    Client.Response resp = client.set(key, value);

    getKeyValueDataOnAllClients(key, value, resp.getSessionCookie());

    client.setPort(Integer.parseInt(manager.getContainerPort(0)));
    client.remove(key);

    getKeyValueDataOnAllClients(key, "", resp.getSessionCookie());
  }

  /**
   * Test that a container added to the system after puts still can access the correct sessions
   * and data.
   */
  @Test
  public void newContainersShouldShareDataAccess() throws Exception {
    manager.startAllInactiveContainers();

    String key = "value_testSessionAdd";
    String value = "Foo";

    client.setPort(Integer.parseInt(manager.getContainerPort(0)));
    Client.Response resp = client.set(key, value);

    getKeyValueDataOnAllClients(key, value, resp.getSessionCookie());

    int numContainers = manager.numContainers();
    // Add and start new container
    manager.addContainer(install);
    customizeContainers();

    manager.startAllInactiveContainers();
    // Check that a container was added
    assertEquals(numContainers + 1, manager.numContainers());

    getKeyValueDataOnAllClients(key, value, resp.getSessionCookie());
  }

  private void announceTest(String status) {
    System.out.format("TEST %s %s.%s%n", status, getClass().getSimpleName(),
        testName.getMethodName());
  }

  private static void dumpDockerInfo() throws IOException {
    Path dockerInfoPath = Paths.get("/", "proc", "self", "cgroup");
    if (Files.isReadable(dockerInfoPath)) {
      System.out.println("Docker info:");
      System.out.println("------------------------------------");
      Files.lines(dockerInfoPath).forEach(System.out::println);
      System.out.println("------------------------------------");
    }
  }
}
