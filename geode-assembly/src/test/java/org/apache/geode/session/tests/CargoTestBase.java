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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.URISyntaxException;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

/**
 * Base class for test of session replication.
 *
 * This class contains all of the tests of session replication functionality. Subclasses of this
 * class configure different containers in order to run these tests against specific containers.
 */
@Category(DistributedTest.class)
public abstract class CargoTestBase extends JUnit4CacheTestCase {
  @Rule
  public transient TestName testName = new TestName();

  public transient Client client;
  public transient ContainerManager manager;

  public abstract ContainerInstall getInstall();

  /**
   * Sets up the {@link #client} and {@link #manager} variables by creating new instances of each.
   *
   * Adds two new containers to the {@link #manager} based on the super class' {@link #getInstall()}
   * method. Also sets {@link ContainerManager#testName} for {@link #manager} to the name of the
   * current test.
   */
  @Before
  public void setup() throws IOException {
    client = new Client();
    manager = new ContainerManager();

    manager.setTestName(testName.getMethodName());
    manager.addContainers(2, getInstall());
  }

  /**
   * Stops all containers that were previously started and cleans up their configurations
   */
  @After
  public void stop() throws IOException {
    manager.stopAllActiveContainers();
    manager.cleanUp();
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
    String cookie = resp.getSessionCookie();

    for (int i = 1; i < manager.numContainers(); i++) {
      client.setPort(Integer.parseInt(manager.getContainerPort(i)));
      resp = client.get(null);

      assertEquals("Sessions are not replicating properly", cookie, resp.getSessionCookie());
    }
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
    String cookie = resp.getSessionCookie();

    for (int i = 0; i < manager.numContainers(); i++) {
      client.setPort(Integer.parseInt(manager.getContainerPort(i)));
      resp = client.get(key);

      assertEquals("Sessions are not replicating properly", cookie, resp.getSessionCookie());
      assertEquals("Session data is not replicating properly", value, resp.getResponse());
    }
  }

  /**
   * Test that when a container fails, session attributes that were previously set in that container
   * are still available in other containers
   */
  @Test
  public void failureShouldStillAllowOtherContainersDataAccess()
      throws IOException, URISyntaxException {
    manager.startAllInactiveContainers();

    String key = "value_testSessionPersists";
    String value = "Foo";

    client.setPort(Integer.parseInt(manager.getContainerPort(0)));
    Client.Response resp = client.set(key, value);
    String cookie = resp.getSessionCookie();

    manager.stopContainer(0);

    for (int i = 1; i < manager.numContainers(); i++) {
      client.setPort(Integer.parseInt(manager.getContainerPort(i)));
      resp = client.get(key);

      assertEquals("Sessions are not replicating properly", cookie, resp.getSessionCookie());
      assertEquals("Container failure caused inaccessible data.", value, resp.getResponse());
    }
  }

  /**
   * Test that invalidating a session in one container invalidates the session in all containers.
   */
  @Test
  public void invalidationShouldRemoveValueAccessForAllContainers()
      throws IOException, URISyntaxException {
    manager.startAllInactiveContainers();

    String key = "value_testInvalidate";
    String value = "Foo";

    client.setPort(Integer.parseInt(manager.getContainerPort(0)));
    Client.Response resp = client.set(key, value);
    String cookie = resp.getSessionCookie();

    client.invalidate();

    for (int i = 0; i < manager.numContainers(); i++) {
      client.setPort(Integer.parseInt(manager.getContainerPort(i)));
      resp = client.get(key);

      assertEquals("Data removal is not being replicated properly.", "", resp.getResponse());
    }
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
    String cookie = resp.getSessionCookie();

    for (int i = 0; i < manager.numContainers(); i++) {
      client.setPort(Integer.parseInt(manager.getContainerPort(i)));
      resp = client.get(key);

      assertEquals("Sessions are not replicating properly", cookie, resp.getSessionCookie());
      assertEquals(value, resp.getResponse());
    }

    client.setMaxInactive(1);

    Thread.sleep(5000);

    for (int i = 0; i < manager.numContainers(); i++) {
      client.setPort(Integer.parseInt(manager.getContainerPort(i)));
      resp = client.get(key);

      assertEquals("Session replication is not doing session expiration correctly.", "",
          resp.getResponse());
    }
  }


  /**
   * Test that if one container is accessing a session, that will prevent the session from expiring
   * in all containers.
   */
  @Test
  public void containersShouldShareSessionExpirationReset()
      throws URISyntaxException, IOException, InterruptedException {
    manager.startAllInactiveContainers();

    int timeToExp = 5;
    String key = "value_testSessionExpiration";
    String value = "Foo";

    client.setPort(Integer.parseInt(manager.getContainerPort(0)));
    Client.Response resp = client.set(key, value);
    String cookie = resp.getSessionCookie();

    client.setMaxInactive(timeToExp);

    long startTime = System.currentTimeMillis();
    long curTime = System.currentTimeMillis();
    // Run for 2 times the set expiration time
    while (curTime - startTime < timeToExp * 2000) {
      client.get(key);
      Thread.sleep(500);
      curTime = System.currentTimeMillis();
    }

    for (int i = 0; i < manager.numContainers(); i++) {
      client.setPort(Integer.parseInt(manager.getContainerPort(i)));
      resp = client.get(key);

      assertEquals("Sessions are not replicating properly", cookie, resp.getSessionCookie());
      assertEquals("Containers are not replicating session expiration reset", value,
          resp.getResponse());
    }
  }

  /**
   * Test that if a session attribute is removed in one container, it is removed from all containers
   */
  @Test
  public void containersShouldShareDataRemovals() throws IOException, URISyntaxException {
    manager.startAllInactiveContainers();

    String key = "value_testSessionRemove";
    String value = "Foo";

    client.setPort(Integer.parseInt(manager.getContainerPort(0)));
    Client.Response resp = client.set(key, value);
    String cookie = resp.getSessionCookie();

    for (int i = 0; i < manager.numContainers(); i++) {
      client.setPort(Integer.parseInt(manager.getContainerPort(i)));
      resp = client.get(key);

      assertEquals("Sessions are not replicating properly", cookie, resp.getSessionCookie());
      assertEquals(value, resp.getResponse());
    }

    client.setPort(Integer.parseInt(manager.getContainerPort(0)));
    client.remove(key);

    for (int i = 0; i < manager.numContainers(); i++) {
      client.setPort(Integer.parseInt(manager.getContainerPort(i)));
      resp = client.get(key);

      assertEquals("Sessions are not replicating properly", cookie, resp.getSessionCookie());
      assertEquals(
          "Was expecting an empty response after removal. Double check to make sure that the enableLocalCache cacheProperty is set to false. This test is unreliable on servers which use a local cache.",
          "", resp.getResponse());
    }
  }

  /**
   * Test that a container added to the system after puts still can access the correct sessions and
   * data.
   */
  @Test
  public void newContainersShouldShareDataAccess() throws IOException, URISyntaxException {
    manager.startAllInactiveContainers();

    String key = "value_testSessionAdd";
    String value = "Foo";

    client.setPort(Integer.parseInt(manager.getContainerPort(0)));
    Client.Response resp = client.set(key, value);
    String cookie = resp.getSessionCookie();

    for (int i = 0; i < manager.numContainers(); i++) {
      client.setPort(Integer.parseInt(manager.getContainerPort(i)));
      resp = client.get(key);

      assertEquals("Sessions are not replicating properly", cookie, resp.getSessionCookie());
      assertEquals(value, resp.getResponse());
    }
    int numContainers = manager.numContainers();

    manager.addContainer(getInstall());
    manager.startAllInactiveContainers();

    assertEquals(numContainers + 1, manager.numContainers());

    for (int i = 0; i < manager.numContainers(); i++) {
      client.setPort(Integer.parseInt(manager.getContainerPort(i)));
      resp = client.get(key);

      assertEquals("Sessions are not replicating properly", cookie, resp.getSessionCookie());
      assertEquals("Containers are not properly sharing data with new arrival", value,
          resp.getResponse());
    }
  }
}
