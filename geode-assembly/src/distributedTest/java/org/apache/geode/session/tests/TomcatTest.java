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


import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;

import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public abstract class TomcatTest extends CargoTestBase {
  @Rule
  public ExecutorServiceRule executorServiceRule = new ExecutorServiceRule();

  @Rule
  public ErrorCollector errorCollector = new ErrorCollector();

  protected int numberOfConcurrentClients;
  protected CountDownLatch latch;
  protected CountDownLatch finishLatch;
  protected CountDownLatch checkQueueLatch;
  private final Random random = new Random();
  private final Map<Client, HashMap<String, String>> clientAttributesMap =
      new ConcurrentHashMap<>();
  private int counter = 0;

  @Test
  public void multipleClientsCanMaintainOwnSessions() throws Throwable {
    dumpLogs = false;
    numberOfConcurrentClients = 20;
    latch = new CountDownLatch(numberOfConcurrentClients);
    finishLatch = new CountDownLatch(numberOfConcurrentClients);
    checkQueueLatch = new CountDownLatch(1);
    manager.startAllInactiveContainers();
    for (int i = 0; i < numberOfConcurrentClients; i++) {
      executorServiceRule.submit(() -> doSessionOps(20));
    }
    if (isCachingClient()) {
      waitUntilHARegionQueueAreDrainedOnAllServers();
    }
    finishLatch.await();
  }

  protected void doSessionOps(int maxNumOfOperations) throws Exception {
    String key = "key";
    String value = "Foo55";
    String key1 = "key1";
    String value1 = "Bar1226";

    Client client = new Client();
    int operatingContainer = random.nextInt(numberOfContainers);
    try {
      client.setPort(Integer.parseInt(manager.getContainerPort(operatingContainer)));
      LogService.getLogger().info(
          "doing work for client " + client + " on Container operatingContainer "
              + operatingContainer);
      LogService.getLogger().info("putting key " + key + " value " + value);
      Client.Response resp = client.set(key, value);
      // To reproduce GEODE-7780 uncomment out the following code
      // if (operatingContainer == 0) {
      // getKeyValueDataOnOperatingClient(client, key, value, resp.getSessionCookie(), 1);
      // } else {
      // getKeyValueDataOnOperatingClient(client, key, value, resp.getSessionCookie(), 0);
      // }
      if (isCachingClient()) {
        // assume sticky session for client with caching proxy
        getKeyValueDataOnOperatingClient(client, key, value, resp.getSessionCookie(),
            operatingContainer);
      } else {
        getKeyValueDataOnAllClients(client, key, value, resp.getSessionCookie());
      }
      resp = client.set(key1, value1);
      if (isCachingClient()) {
        getKeyValueDataOnOperatingClient(client, key1, value1, resp.getSessionCookie(),
            operatingContainer);
      } else {
        getKeyValueDataOnAllClients(client, key1, value1, resp.getSessionCookie());
      }
      resp = client.set(key, null);
      if (isCachingClient()) {
        getKeyValueDataOnOperatingClient(client, key, "", resp.getSessionCookie(),
            operatingContainer);
      } else {
        getKeyValueDataOnAllClients(client, key, "", resp.getSessionCookie());
      }
      doSetsAndVerifyGets(client, maxNumOfOperations, operatingContainer);
    } catch (Throwable e) {
      errorCollector.addError(e);
    } finally {
      latch.countDown();
    }

    if (isCachingClient()) {
      checkQueueLatch.await();
    }

    try {
      verifySessionAllAttributes(client, clientAttributesMap.get(client), client.get(key, true));
    } catch (Throwable e) {
      errorCollector.addError(e);
    } finally {
      finishLatch.countDown();
    }
  }

  private void waitUntilHARegionQueueAreDrainedOnAllServers() throws Exception {
    latch.await();
    for (int i = 0; i < manager.numContainers(); i++) {
      client.setPort(Integer.parseInt(manager.getContainerPort(i)));
      LogService.getLogger().info("waitForQueueToDrain on container " + i);
      client.waitForQueueToDrain();
      LogService.getLogger().info("done waitForQueueToDrain on container " + i);
    }
    checkQueueLatch.countDown();
  }

  private synchronized void updateDoneCounter() {
    ++counter;
    LogService.getLogger().info("finished {} of sessions", counter);
  }

  private void doSetsAndVerifyGets(Client client, int maxNumberOfOperations, int operatingContainer)
      throws Exception {
    HashMap<String, String> attributes = new HashMap<>();
    clientAttributesMap.put(client, attributes);

    int numberOfOperations = random.nextInt(maxNumberOfOperations) + 1;
    LogService.getLogger().info("performing {} of operations", numberOfOperations);
    for (int i = 0; i < numberOfOperations; i++) {
      if (i % 2 == 0) {
        doGetsAndSets(client, attributes, operatingContainer);
      } else {
        doSetsAndGetWithUpdate(client, attributes, operatingContainer);
      }
    }
    LogService.getLogger().info("finished doSetsAndVerifyGets ops for {} times",
        numberOfOperations);
    updateDoneCounter();
  }

  private void doGetsAndSets(Client client, HashMap<String, String> attributes,
      int operatingContainer)
      throws IOException, URISyntaxException {
    String key = getKey(client, attributes);
    int maxLength = 100;
    String value = getRandomVarChar(client, attributes, maxLength);
    attributes.put(key, value);
    verifySets(client, attributes, operatingContainer, key, value);
  }

  private void verifySets(Client client, HashMap<String, String> attributes, int operatingContainer,
      String key, String value) throws IOException, URISyntaxException {
    int whichContainer =
        isCachingClient() ? operatingContainer : random.nextInt(numberOfContainers);
    client.setPort(Integer.parseInt(manager.getContainerPort(whichContainer)));
    Client.Response resp = client.set(key, value);
    if (isCachingClient()) {
      getKeyValueDataOnOperatingClient(client, key, value, resp.getSessionCookie(),
          operatingContainer);
      verifySessionGets(client, attributes, resp, operatingContainer);
    } else {
      getKeyValueDataOnAllClients(client, key, value, resp.getSessionCookie());
      verifySessionGets(client, attributes, resp);
    }
  }

  private void doSetsAndGetWithUpdate(Client client, HashMap<String, String> attributes,
      int operatingContainer)
      throws IOException, URISyntaxException {
    String key = getKey(client, attributes);
    if (random.nextInt(10) == 1) {
      // do update on an existing attribute
      key = "key";
    }
    int maxLength = 100;
    String value = getRandomVarChar(client, attributes, maxLength);
    attributes.put(key, value);
    verifySets(client, attributes, operatingContainer, key, value);
  }

  protected String getKey(Client client, Map attributes) {
    return "key" + (attributes.size() + 1) + "_" + client.getCookie();
  }

  protected String getRandomVarChar(Client client, Map attributes, int length) {
    if (length == 0) {
      return "";
    }
    StringBuffer buffer = new StringBuffer();
    buffer.append("value" + (attributes.size() + 1) + "_" + client.getCookie());
    int randomLength = random.nextInt(length) + 1;

    int sp = ' ';
    int tilde = '~';
    for (int j = 0; j < randomLength; j++) {
      buffer.append((char) (random.nextInt(tilde - sp) + sp));
    }
    return buffer.toString();
  }

  private void verifySessionGets(Client client, HashMap<String, String> attributes,
      Client.Response resp) throws IOException, URISyntaxException {
    verifySessionGets(client, attributes, resp, -1);
  }

  private void verifySessionGets(Client client, HashMap<String, String> attributes,
      Client.Response resp, int operatingContainer) throws IOException, URISyntaxException {
    int count = 0;
    int whichOne = random.nextInt(attributes.size());
    Iterator iterator = attributes.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry entry = (Map.Entry) iterator.next();
      if (count == whichOne) {
        String key = (String) entry.getKey();
        String expectedValue = (String) entry.getValue();
        if (isCachingClient()) {
          getKeyValueDataOnOperatingClient(client, key, expectedValue,
              resp.getSessionCookie(), operatingContainer);
        } else {
          getKeyValueDataOnAllClients(client, key, expectedValue,
              resp.getSessionCookie());
        }
        break;
      }
      count++;
    }
  }

  private void verifySessionAllAttributes(Client client, HashMap<String, String> attributes,
      Client.Response resp) throws IOException, URISyntaxException {
    Iterator iterator = attributes.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry entry = (Map.Entry) iterator.next();
      String key = (String) entry.getKey();
      String expectedValue = (String) entry.getValue();
      getKeyValueDataOnAllClients(client, key, expectedValue,
          resp.getSessionCookie());
    }
  }

  protected boolean isCachingClient() {
    return install.getConnectionType() == ContainerInstall.ConnectionType.CACHING_CLIENT_SERVER;
  }
}
