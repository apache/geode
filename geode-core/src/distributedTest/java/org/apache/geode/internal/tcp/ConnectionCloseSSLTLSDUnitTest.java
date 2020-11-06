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

package org.apache.geode.internal.tcp;

import static org.apache.geode.distributed.ConfigurationProperties.CONSERVE_SOCKETS;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.distributed.ConfigurationProperties.SOCKET_BUFFER_SIZE;
import static org.apache.geode.distributed.ConfigurationProperties.SOCKET_LEASE_TIME;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENABLED_COMPONENTS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_REQUIRE_AUTHENTICATION;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.USE_CLUSTER_CONFIGURATION;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.util.ResourceUtils.createTempFileFromResource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Fail.fail;

import java.io.File;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.DistributionMessageObserver;
import org.apache.geode.distributed.internal.membership.gms.membership.GMSJoinLeave;
import org.apache.geode.internal.cache.UpdateOperation.UpdateMessage;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedBlackboard;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.dunit.rules.DistributedRule;

/**
 * It would be nice if this test didn't need to use the cache since the test's purpose is to test
 * that the {@link Connection} class can be closed while readers and writers hold locks on its
 * internal TLS {@link ByteBuffer}s
 *
 * But this test does use the cache (region) because it enabled us to use existing cache messaging
 * and to use the DistributionMessageObserver (observer) hooks.
 *
 * see also ClusterCommunicationsDUnitTest
 */
public class ConnectionCloseSSLTLSDUnitTest implements Serializable {

  private static final int SMALL_BUFFER_SIZE = 8000;
  private static final String UPDATE_ENTERED_GATE = "connectionCloseDUnitTest.regionUpdateEntered";
  private static final String SUSPEND_UPDATE_GATE = "connectionCloseDUnitTest.suspendRegionUpdate";
  private static final String regionName = "connectionCloseDUnitTestRegion";
  private static final Logger logger = LogService.getLogger();

  private static Cache cache;

  @Rule
  public DistributedRule distributedRule =
      DistributedRule.builder().withVMCount(3).build();

  @Rule
  public DistributedBlackboard blackboard = new DistributedBlackboard();

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  private VM locator;
  private VM sender;
  private VM receiver;

  @Before
  public void before() {
    locator = getVM(0);
    sender = getVM(1);
    receiver = getVM(2);
  }

  @After
  public void after() {
    receiver.invoke(() -> {
      DistributionMessageObserver.setInstance(null);
    });
  }

  @Test
  public void connectionWithHungReaderIsCloseableAndUnhangsReader()
      throws InterruptedException, TimeoutException {

    blackboard.clearGate(UPDATE_ENTERED_GATE);
    blackboard.clearGate(SUSPEND_UPDATE_GATE);

    final int locatorPort = createLocator(locator);
    createCacheAndRegion(sender, locatorPort);
    createCacheAndRegion(receiver, locatorPort);

    receiver
        .invoke("set up DistributionMessageObserver to 'hang' sender's put (on receiver)",
            () -> {
              final DistributionMessageObserver observer =
                  new DistributionMessageObserver() {

                    @Override
                    public void beforeProcessMessage(final ClusterDistributionManager dm,
                        final DistributionMessage message) {
                      guardMessageProcessingHook(message, () -> {
                        try {
                          blackboard.signalGate(UPDATE_ENTERED_GATE);
                          blackboard.waitForGate(SUSPEND_UPDATE_GATE);
                        } catch (TimeoutException | InterruptedException e) {
                          fail("message observus interruptus");
                        }
                        logger.info("BGB: got before process message: " + message);
                      });
                    }
                  };
              DistributionMessageObserver.setInstance(observer);
            });

    final AsyncInvocation<Object> putInvocation = sender.invokeAsync("try a put", () -> {
      final Region<Object, Object> region = cache.getRegion(regionName);
      // test is going to close the cache while we are waiting for our ack
      assertThatThrownBy(() -> {
        region.put("hello", "world");
      }).isInstanceOf(DistributedSystemDisconnectedException.class);
    });

    // wait until our message observer is blocked
    blackboard.waitForGate(UPDATE_ENTERED_GATE);

    // at this point our put() is blocked waiting for a direct ack
    assertThat(putInvocation.isAlive()).as("put is waiting for remote region to ack").isTrue();

    /*
     * Now close the cache. The point of calling it is to test that we don't block while trying
     * to close connections. Cache.close() calls DistributedSystem.disconnect() which in turn
     * closes all the connections (and their sockets.) We want the sockets to close because that'll
     * cause our hung put() to see a DistributedSystemDisconnectedException.
     */
    sender.invoke("", () -> cache.close());

    // wait for put task to complete: with an exception, that is!
    putInvocation.get();

    // un-stick our message observer
    blackboard.signalGate(SUSPEND_UPDATE_GATE);
  }

  private void guardMessageProcessingHook(final DistributionMessage message,
      final Runnable runnable) {
    if (message instanceof UpdateMessage) {
      final UpdateMessage updateMessage = (UpdateMessage) message;
      if (updateMessage.getRegionPath().equals("/" + regionName)) {
        runnable.run();
      }
    }
  }

  private int createLocator(VM memberVM) {
    return memberVM.invoke("create locator", () -> {
      // if you need to debug SSL communications use this property:
      // System.setProperty("javax.net.debug", "all");
      System.setProperty(GMSJoinLeave.BYPASS_DISCOVERY_PROPERTY, "true");
      return Locator.startLocatorAndDS(0, new File(""), getDistributedSystemProperties())
          .getPort();
    });
  }

  private void createCacheAndRegion(VM memberVM, int locatorPort) {
    memberVM.invoke("start cache and create region", () -> {
      cache = createCache(locatorPort);
      cache.createRegionFactory(RegionShortcut.REPLICATE).create(regionName);
    });
  }

  private Cache createCache(int locatorPort) {
    // if you need to debug SSL communications use this property:
    // System.setProperty("javax.net.debug", "all");
    Properties properties = getDistributedSystemProperties();
    properties.setProperty(LOCATORS, "localhost[" + locatorPort + "]");
    return new CacheFactory(properties).create();
  }

  private Properties getDistributedSystemProperties() {
    Properties properties = new Properties();
    properties.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");
    properties.setProperty(USE_CLUSTER_CONFIGURATION, "false");
    properties.setProperty(NAME, "vm" + VM.getCurrentVMNum());
    properties.setProperty(CONSERVE_SOCKETS, "false"); // we are testing direct ack
    properties.setProperty(SOCKET_LEASE_TIME, "10000");
    properties.setProperty(SOCKET_BUFFER_SIZE, "" + SMALL_BUFFER_SIZE);

    properties.setProperty(SSL_ENABLED_COMPONENTS, "cluster,locator");
    properties
        .setProperty(SSL_KEYSTORE, createTempFileFromResource(getClass(), "server.keystore")
            .getAbsolutePath());
    properties.setProperty(SSL_TRUSTSTORE,
        createTempFileFromResource(getClass(), "server.keystore")
            .getAbsolutePath());
    properties.setProperty(SSL_PROTOCOLS, "TLSv1.2");
    properties.setProperty(SSL_KEYSTORE_PASSWORD, "password");
    properties.setProperty(SSL_TRUSTSTORE_PASSWORD, "password");
    properties.setProperty(SSL_REQUIRE_AUTHENTICATION, "true");
    return properties;
  }

}
