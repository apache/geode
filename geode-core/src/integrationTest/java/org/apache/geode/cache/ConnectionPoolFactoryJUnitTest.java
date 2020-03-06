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
package org.apache.geode.cache;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Map;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.PoolFactoryImpl;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
public class ConnectionPoolFactoryJUnitTest {

  private Cache cache;
  private DistributedSystem ds;

  @Before
  public void setUp() {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    props.setProperty(LOG_LEVEL, "info"); // to keep diskPerf logs smaller
    this.ds = DistributedSystem.connect(props);
    this.cache = CacheFactory.create(this.ds);
  }

  @After
  public void tearDown() {
    try {
      this.cache.close();
    } catch (Exception e) {
      // do nothing
    }
    this.ds.disconnect();
  }

  @Test
  public void testAddIllegalArgs() {
    PoolFactory cpf = PoolManager.createFactory();
    try {
      cpf.addServer("localhost", 0);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
    }
    try {
      cpf.addServer("localhost", 65536);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
    }
    try {
      cpf.addLocator("localhost", 0);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
    }
    try {
      cpf.addLocator("localhost", 65536);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
    }
    // Fix for #45348
    // try {
    // cpf.addLocator("noWayThisIsAValidHost", 12345);
    // fail("expected IllegalArgumentException");
    // } catch (IllegalArgumentException expected) {
    // if (!(expected.getCause() instanceof java.net.UnknownHostException)) {
    // fail("expected cause to be UnknownHostException but was " + expected.getCause());
    // }
    // }
    cpf.addLocator("localhost", 12345);
    try {
      cpf.addServer("localhost", 12345);
      fail("expected IllegalStateException");
    } catch (IllegalStateException expected) {
    }
    cpf.reset();
    cpf.addServer("localhost", 12345);
    try {
      cpf.addLocator("localhost", 12345);
      fail("expected IllegalStateException");
    } catch (IllegalStateException expected) {
    }
  }

  @Test
  public void testCreateDefaultAndInvalidAndLegitAttributes() {
    PoolFactory cpf = PoolManager.createFactory();
    ((PoolFactoryImpl) cpf).setStartDisabled(true);

    try {
      cpf.create("illegal");
      fail("expected IllegalStateException");
    } catch (IllegalStateException expected) {
    }

    cpf.addServer("localhost", 40907);

    Pool defaultAttr = cpf.create("defaults");
    try {
      // now add a source and try defaults again
      assertEquals(PoolFactory.DEFAULT_FREE_CONNECTION_TIMEOUT,
          defaultAttr.getFreeConnectionTimeout());
      assertEquals(PoolFactory.DEFAULT_SERVER_CONNECTION_TIMEOUT,
          defaultAttr.getServerConnectionTimeout());
      assertEquals(PoolFactory.DEFAULT_THREAD_LOCAL_CONNECTIONS,
          defaultAttr.getThreadLocalConnections());
      assertEquals(PoolFactory.DEFAULT_READ_TIMEOUT, defaultAttr.getReadTimeout());
      assertEquals(PoolFactory.DEFAULT_MIN_CONNECTIONS, defaultAttr.getMinConnections());
      assertEquals(PoolFactory.DEFAULT_MAX_CONNECTIONS, defaultAttr.getMaxConnections());
      assertEquals(PoolFactory.DEFAULT_RETRY_ATTEMPTS, defaultAttr.getRetryAttempts());
      assertEquals(PoolFactory.DEFAULT_IDLE_TIMEOUT, defaultAttr.getIdleTimeout());
      assertEquals(PoolFactory.DEFAULT_PING_INTERVAL, defaultAttr.getPingInterval());
      assertEquals(PoolFactory.DEFAULT_SOCKET_BUFFER_SIZE, defaultAttr.getSocketBufferSize());
    } finally {
      defaultAttr.destroy();
    }

    /*
     * Lets configure each attribute and make sure they are reflected in the attributes
     */

    int connectionTimeout = -1;
    int serverConnectionTimeout = -2;
    int connectionLifetime = -2;
    boolean threadLocalConnections = false;
    int readTimeout = -1;
    int messageTrackingTimeout = -1;
    int ackInterval = -1;
    int minConnections = -1;
    int maxConnections = -2;
    int retryAttempts = -2;
    int pingInterval = -1;
    int idleTimeout = -2;
    int redundancy = -2;
    int bufferSize = -1;

    /* All of these should fail */
    try {
      cpf.setFreeConnectionTimeout(connectionTimeout);
      assertTrue("This should have failed with IllegalArgumentException", false);
    } catch (IllegalArgumentException iae) {
      // this is what we want
    }

    try {
      cpf.setServerConnectionTimeout(serverConnectionTimeout);
      assertTrue("This should have failed with IllegalArgumentException", false);
    } catch (IllegalArgumentException iae) {
      // this is what we want
    }

    try {
      cpf.setLoadConditioningInterval(connectionLifetime);
      assertTrue("This should have failed with IllegalArgumentException", false);
    } catch (IllegalArgumentException iae) {
      // this is what we want
    }

    try {
      cpf.setReadTimeout(readTimeout);
      assertTrue("This should have failed with IllegalArgumentException", false);
    } catch (IllegalArgumentException iae) {
      // this is what we want
    }

    try {
      cpf.setMinConnections(minConnections);
      assertTrue("This should have failed with IllegalArgumentException", false);
    } catch (IllegalArgumentException iae) {
      // this is what we want
    }

    try {
      cpf.setMaxConnections(maxConnections);
      assertTrue("This should have failed with IllegalArgumentException", false);
    } catch (IllegalArgumentException iae) {
      // this is what we want
    }

    try {
      cpf.setRetryAttempts(retryAttempts);
      assertTrue("This should have failed with IllegalArgumentException", false);
    } catch (IllegalArgumentException iae) {
      // this is what we want
    }

    try {
      cpf.setPingInterval(pingInterval);
      assertTrue("This should have failed with IllegalArgumentException", false);
    } catch (IllegalArgumentException iae) {
      // this is what we want
    }

    try {
      cpf.setIdleTimeout(idleTimeout);
      assertTrue("This should have failed with IllegalArgumentException", false);
    } catch (IllegalArgumentException iae) {
      // this is what we want
    }

    try {
      cpf.setIdleTimeout(idleTimeout);
      assertTrue("This should have failed with IllegalArgumentException", false);
    } catch (IllegalArgumentException iae) {
      // this is what we want
    }

    try {
      cpf.setSocketBufferSize(bufferSize);
      assertTrue("This should have failed with IllegalArgumentException", false);
    } catch (IllegalArgumentException iae) {
      // this is what we want
    }

    try {
      cpf.setSubscriptionRedundancy(redundancy);
      assertTrue("This should have failed with IllegalArgumentException", false);
    } catch (IllegalArgumentException iae) {
      // this is what we want
    }
    try {
      cpf.setSubscriptionMessageTrackingTimeout(messageTrackingTimeout);
      assertTrue("This should have failed with IllegalArgumentException", false);
    } catch (IllegalArgumentException iae) {
      // this is what we want
    }
    try {
      cpf.setSubscriptionAckInterval(ackInterval);
      assertTrue("This should have failed with IllegalArgumentException", false);
    } catch (IllegalArgumentException iae) {
      // this is what we want
    }

    /* none of those should take effect so this should still match default */
    defaultAttr = cpf.create("default");
    assertEquals("Attribute should match default, but doesn't",
        defaultAttr.getFreeConnectionTimeout(), PoolFactory.DEFAULT_FREE_CONNECTION_TIMEOUT);
    assertEquals("Attribute should match default, but doesn't",
        defaultAttr.getServerConnectionTimeout(), PoolFactory.DEFAULT_SERVER_CONNECTION_TIMEOUT);
    assertEquals("Attribute should match default, but doesn't",
        defaultAttr.getLoadConditioningInterval(), PoolFactory.DEFAULT_LOAD_CONDITIONING_INTERVAL);
    assertEquals("Attribute should match default, but doesn't",
        defaultAttr.getThreadLocalConnections(), PoolFactory.DEFAULT_THREAD_LOCAL_CONNECTIONS);
    assertEquals("Attribute should match default, but doesn't", defaultAttr.getReadTimeout(),
        PoolFactory.DEFAULT_READ_TIMEOUT);
    assertEquals("Attribute should match default, but doesn't", defaultAttr.getMinConnections(),
        PoolFactory.DEFAULT_MIN_CONNECTIONS);
    assertEquals("Attribute should match default, but doesn't", defaultAttr.getMaxConnections(),
        PoolFactory.DEFAULT_MAX_CONNECTIONS);
    assertEquals("Attribute should match default, but doesn't", defaultAttr.getRetryAttempts(),
        PoolFactory.DEFAULT_RETRY_ATTEMPTS);
    assertEquals("Attribute should match default, but doesn't", defaultAttr.getIdleTimeout(),
        PoolFactory.DEFAULT_IDLE_TIMEOUT);
    assertEquals("Attribute should match default, but doesn't", defaultAttr.getPingInterval(),
        PoolFactory.DEFAULT_PING_INTERVAL);
    assertEquals("Attribute should match default, but doesn't", defaultAttr.getSocketBufferSize(),
        PoolFactory.DEFAULT_SOCKET_BUFFER_SIZE);

    /* Lets do a legitimate one now */

    connectionTimeout = 30;
    serverConnectionTimeout = 0;
    connectionLifetime = -1;
    threadLocalConnections = true;
    readTimeout = 3;
    minConnections = 6;
    maxConnections = 7;
    retryAttempts = 2;
    pingInterval = 23;
    idleTimeout = 14;
    messageTrackingTimeout = 40;
    ackInterval = 33;
    redundancy = 4;
    bufferSize = 1000;


    cpf.setFreeConnectionTimeout(connectionTimeout);
    cpf.setServerConnectionTimeout(serverConnectionTimeout);
    cpf.setLoadConditioningInterval(connectionLifetime);
    cpf.setThreadLocalConnections(threadLocalConnections);
    cpf.setReadTimeout(readTimeout);
    cpf.setSubscriptionEnabled(true);
    cpf.setSubscriptionRedundancy(redundancy);
    cpf.setSubscriptionMessageTrackingTimeout(messageTrackingTimeout);
    cpf.setSubscriptionAckInterval(ackInterval);
    cpf.setMinConnections(minConnections);
    cpf.setMaxConnections(maxConnections);
    cpf.setRetryAttempts(retryAttempts);
    cpf.setPingInterval(pingInterval);
    cpf.setIdleTimeout(idleTimeout);
    cpf.setSocketBufferSize(bufferSize);

    Pool cpa = cpf.create("mypool");
    try {

      assertEquals(connectionTimeout, cpa.getFreeConnectionTimeout());
      assertEquals(serverConnectionTimeout, cpa.getServerConnectionTimeout());
      assertEquals(connectionLifetime, cpa.getLoadConditioningInterval());
      assertEquals(threadLocalConnections, cpa.getThreadLocalConnections());
      assertEquals(true, cpa.getSubscriptionEnabled());
      assertEquals(redundancy, cpa.getSubscriptionRedundancy());
      assertEquals(messageTrackingTimeout, cpa.getSubscriptionMessageTrackingTimeout());
      assertEquals(readTimeout, cpa.getReadTimeout());
      assertEquals(minConnections, cpa.getMinConnections());
      assertEquals(maxConnections, cpa.getMaxConnections());
      assertEquals(ackInterval, cpa.getSubscriptionAckInterval());
      assertEquals(retryAttempts, cpa.getRetryAttempts());
      assertEquals(idleTimeout, cpa.getIdleTimeout());
      assertEquals(pingInterval, cpa.getPingInterval());

      assertEquals(bufferSize, cpa.getSocketBufferSize());
      // validate contacts
      assertEquals(1, cpa.getServers().size());
      assertEquals(0, cpa.getLocators().size());
    } finally {
      cpa.destroy();
    }

    // test reset
    cpf.reset();
    try {
      cpf.create("mypool");
      fail("expected IllegalStateException");
    } catch (IllegalStateException expected) {
      // since reset emptied out the contacts
    }
  }

  @Test
  public void testCreateADirectPool() throws Exception {
    int connectionTimeout = 20;
    boolean threadLocalConnections = true;
    int readTimeout = 20;
    int messageTrackingTimeout = 20;
    int redundancy = 20;
    int bufferSize = 20;
    int ackInterval = 15;

    PoolFactory cpf = PoolManager.createFactory();
    ((PoolFactoryImpl) cpf).setStartDisabled(true);
    cpf.addServer("localhost", 40907).setFreeConnectionTimeout(connectionTimeout)
        .setThreadLocalConnections(threadLocalConnections).setReadTimeout(readTimeout)
        .setSubscriptionEnabled(true).setSubscriptionRedundancy(redundancy)
        .setSubscriptionMessageTrackingTimeout(messageTrackingTimeout)
        .setSubscriptionAckInterval(ackInterval).setSocketBufferSize(bufferSize);

    Pool pool1 = cpf.create("myfriendlypool");

    // @todo validate non default props

    Map pools = PoolManager.getAll();
    assertEquals("there should be one pool", 1, pools.size());
    assertNotNull("pool myfriendlypool should exist and be non null", pools.get("myfriendlypool"));


    /* lets make another with same name - should fail! */

    boolean gotit = false;
    try {
      cpf.create("myfriendlypool");
    } catch (IllegalStateException ise) {
      gotit = true;
    }
    assertTrue("should have gotten an illegal state when creating duplicate pool name", gotit);

    pools = PoolManager.getAll();
    assertEquals("there should be one pool", 1, pools.size());
    assertNotNull("pool myfriendlypool should exist and be non null", pools.get("myfriendlypool"));


    /* create another legit one */

    Pool pool2 = cpf.create("myfriendlypool2");

    pools = PoolManager.getAll();
    assertEquals("there should be two pools", 2, pools.size());
    assertNotNull("pool myfriendlypool should exist and be non null", pools.get("myfriendlypool"));
    assertNotNull("pool myfriendlypool2 should exist and be non null",
        pools.get("myfriendlypool2"));

    /* lets remove them one by one */

    assertEquals(pool1, PoolManager.find("myfriendlypool"));
    pool1.destroy();
    assertEquals(null, PoolManager.find("myfriendlypool"));
    pools = PoolManager.getAll();
    assertEquals("there should be one pool", 1, pools.size());
    assertNull("pool myfriendlypool should NOT exist", pools.get("myfriendlypool"));
    assertNotNull("pool myfriendlypool2 should exist and be non null",
        pools.get("myfriendlypool2"));

    assertEquals(pool2, PoolManager.find("myfriendlypool2"));
    pool2.destroy();
    assertEquals(null, PoolManager.find("myfriendlypool2"));
    pools = PoolManager.getAll();
    assertEquals("there should be 0 pools", 0, pools.size());
    assertNull("pool myfriendlypool should NOT exist", pools.get("myfriendlypool"));
    assertNull("pool myfriendlypool2 should NOT exist and be non null",
        pools.get("myfriendlypool2"));
    cache.close();
  }

}
