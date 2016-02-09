/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.cache.tier.sockets;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;
import junit.framework.TestCase;

import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.*;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.client.internal.Connection;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Make sure max-connections on cache server is enforced
 *
 * @author darrel
 *
 */
@Category(IntegrationTest.class)
public class CacheServerMaxConnectionsJUnitTest

{

  /** connection proxy object for the client */
  PoolImpl proxy = null;

  /** the distributed system instance for the test */
  DistributedSystem system;

  /** the cache instance for the test */
  Cache cache;

  /** name of the region created */
  final String regionName = "region1";

  private static int PORT;

  /**
   * Close the cache and disconnects from the distributed system
   */
  @After
  public void tearDown() throws Exception

  {
    this.cache.close();
    this.system.disconnect();
  }

  /**
   * Default to 0; override in sub tests to add thread pool
   */
  protected int getMaxThreads() {
    return 0;
  }

  /**
   * Initializes proxy object and creates region for client
   *
   */
  private void createProxyAndRegionForClient()
  {
    try {
      //props.setProperty("retryAttempts", "0");
      PoolFactory pf = PoolManager.createFactory();
      pf.addServer("localhost", PORT);
      pf.setMinConnections(0);
      pf.setPingInterval(10000);
      pf.setThreadLocalConnections(true);
      pf.setReadTimeout(2000);
      pf.setSocketBufferSize(32768);
      proxy = (PoolImpl)pf.create("junitPool");
      AttributesFactory factory = new AttributesFactory();
      factory.setScope(Scope.DISTRIBUTED_ACK);
      factory.setPoolName("junitPool");
      RegionAttributes attrs = factory.createRegionAttributes();
      cache.createVMRegion(regionName, attrs);
    }
    catch (Exception ex) {
      ex.printStackTrace();
      fail("Failed to initialize client");
    }
  }

  private final static int MAX_CNXS = 100;

  /**
   * Creates and starts the server instance
   *
   */
  private int createServer()
  {
    CacheServer server = null;
    try {
      Properties p = new Properties();
      // make it a loner
      p.put("mcast-port", "0");
      p.put("locators", "");
      this.system = DistributedSystem.connect(p);
      this.cache = CacheFactory.create(system);
      server = this.cache.addCacheServer();
      int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
      server.setMaxConnections(MAX_CNXS);
      server.setMaxThreads(getMaxThreads());
      server.setPort(port);
      server.start();
    }
    catch (Exception e) {
      e.printStackTrace();
      fail("Failed to create server");
    }
    return server.getPort();
  }

  /**
   * This test performs the following:<br>
   * 1)create server<br>
   * 2)initialize proxy object and create region for client<br>
   * 3)perform a PUT on client by acquiring Connection through proxy<br>
   * 4)stop server monitor threads in client to ensure that server treats this
   * as dead client <br>
   * 5)wait for some time to allow server to clean up the dead client artifacts<br>
   * 6)again perform a PUT on client through same Connection and verify after
   * the put that the Connection object used was new one.
   */
  @Test
  public void testMaxCnxLimit() throws Exception
  {
    PORT = createServer();
    createProxyAndRegionForClient();
    StatisticsType st = this.system.findType("CacheServerStats");
    final Statistics s = this.system.findStatisticsByType(st)[0];
    assertEquals(0, s.getInt("currentClients"));
    assertEquals(0, s.getInt("currentClientConnections"));
    Connection[] cnxs = new Connection[MAX_CNXS];
    for (int i=0; i < MAX_CNXS; i++) {
      cnxs[i] = proxy.acquireConnection();
      this.system.getLogWriter().info("acquired connection[" + i + "]=" + cnxs[i]);
    }
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return s.getInt("currentClientConnections") == MAX_CNXS;
      }
      public String description() {
        return null;
      }
    };
    Wait.waitForCriterion(ev, 1000, 200, true);
    assertEquals(MAX_CNXS, s.getInt("currentClientConnections"));
    assertEquals(1, s.getInt("currentClients"));
    this.system.getLogWriter().info("<ExpectedException action=add>" 
        + "exceeded max-connections" + "</ExpectedException>");
    try {
      Connection cnx = proxy.acquireConnection();
      if (cnx != null) {
        fail("should not have been able to connect more than " + MAX_CNXS + " times but was able to connect " + s.getInt("currentClientConnections") + " times. Last connection=" + cnx);
      }
      this.system.getLogWriter().info("acquire connection returned null which is ok");
    }
    catch (NoAvailableServersException expected) {
      // This is expected but due to race conditions in server handshake
      // we may get null back from acquireConnection instead.
      this.system.getLogWriter().info("received expected " + expected.getMessage());
    }
    catch (Exception ex) {
      fail("expected acquireConnection to throw NoAvailableServersException but instead it threw " + ex);
    }
    finally {
      this.system.getLogWriter().info("<ExpectedException action=remove>" 
          + "exceeded max-connections" + "</ExpectedException>");
    }

    // now lets see what happens we we close our connections
    for (int i=0; i < MAX_CNXS; i++) {
      cnxs[i].close(false);
    }
    ev = new WaitCriterion() {
      public boolean done() {
        return s.getInt("currentClients") == 0;
      }
      public String description() {
        return null;
      }
    };
    Wait.waitForCriterion(ev, 3 * 1000, 200, true);
    this.system.getLogWriter().info("currentClients="
        + s.getInt("currentClients")
        + " currentClientConnections="
        + s.getInt("currentClientConnections"));
    assertEquals(0, s.getInt("currentClientConnections"));
    assertEquals(0, s.getInt("currentClients"));
  }
}
