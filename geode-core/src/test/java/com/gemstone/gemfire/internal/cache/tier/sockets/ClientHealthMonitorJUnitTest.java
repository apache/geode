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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Properties;

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.PoolFactory;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.ServerConnectivityException;
import com.gemstone.gemfire.cache.client.internal.Connection;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.client.internal.ServerRegionProxy;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * This is a functional-test for <code>ClientHealthMonitor</code>.
 * 
 * @author dpatel
 * 
 */
@Category(IntegrationTest.class)
public class ClientHealthMonitorJUnitTest
{
  /**
   * Default to 0; override in sub tests to add thread pool
   */
  protected int getMaxThreads() {
    return 0; 
  }

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
    removeExceptions(); 
    this.cache.close();
    this.system.disconnect();
  }

  /**
   * Initializes proxy object and creates region for client
   * 
   */
  private void createProxyAndRegionForClient()
  {
    try {
      //props.setProperty("retryAttempts", "5");
      PoolFactory pf = PoolManager.createFactory();
      proxy = (PoolImpl)pf.addServer("localhost", PORT)
        .setThreadLocalConnections(true)
        .setReadTimeout(10000)
        .setPingInterval(10000)
        .setMinConnections(0)
        .create("junitPool");
      AttributesFactory factory = new AttributesFactory();
      factory.setScope(Scope.DISTRIBUTED_ACK);
      cache.createVMRegion(regionName, factory.createRegionAttributes());
    }
    catch (Exception ex) {
      ex.printStackTrace();
      fail("Failed to initialize client");
    }
  }

  private final static int TIME_BETWEEN_PINGS = 2500;

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
      server.setMaximumTimeBetweenPings(TIME_BETWEEN_PINGS);
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
  public void testDeadClientRemovalByServer() throws Exception
  {
    PORT = createServer();
    createProxyAndRegionForClient();
//    String connection2String = null;
    StatisticsType st = this.system.findType("CacheServerStats");
    final Statistics s = this.system.findStatisticsByType(st)[0];
    assertEquals(0, s.getInt("currentClients"));
    assertEquals(0, s.getInt("currentClientConnections"));
    this.system.getLogWriter().info("beforeAcquireConnection clients=" + s.getInt("currentClients") + " cnxs=" + s.getInt("currentClientConnections"));
    Connection connection1 = proxy.acquireConnection();
    this.system.getLogWriter().info("afterAcquireConnection clients=" + s.getInt("currentClients") + " cnxs=" + s.getInt("currentClientConnections"));
    this.system.getLogWriter().info("acquired connection " + connection1);
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return s.getInt("currentClients") != 0;
      }
      public String description() {
        return null;
      }
    };
    Wait.waitForCriterion(ev, 20 * 1000, 200, true);
    
    assertEquals(1, s.getInt("currentClients"));
    assertEquals(1, s.getInt("currentClientConnections"));
//    String connection1String = connection1.toString();
    ServerRegionProxy srp = new ServerRegionProxy("region1", proxy);
    srp.putOnForTestsOnly(connection1, "key-1", "value-1", new EventID(new byte[] { 1 },1,1), null);
    this.system.getLogWriter().info("did put 1");
    //proxy.testfinalizeServerConnectionMonitor();
    ev = new WaitCriterion() {
      public boolean done() {
        return s.getInt("currentClients") == 0;
      }
      public String description() {
        return null;
      }
    };
    Wait.waitForCriterion(ev, TIME_BETWEEN_PINGS * 5, 200, true);

    {
      this.system.getLogWriter().info("currentClients="
                                      + s.getInt("currentClients")
                                      + " currentClientConnections="
                                      + s.getInt("currentClientConnections"));
      assertEquals(0, s.getInt("currentClients"));
      assertEquals(0, s.getInt("currentClientConnections"));
    }
    addExceptions();
    // the connection should now fail since the server timed it out
    try {
      srp.putOnForTestsOnly(connection1, "key-1", "fail",new EventID(new byte[] {1},1,2), null);
      fail("expected EOF");
    } catch (ServerConnectivityException expected) {
    }
    // The rest of this test no longer works.
//     connection1.finalizeConnection();
//     proxy.release();
    
//     connection1 = proxy.acquireConnection();
//     connection2String = connection1.toString();
//     this.system.getLogWriter().info("connection is now " + connection2String);

//     if (connection1String.equals(connection2String)) {
//       fail("New connection object was not obtained");
//     }
//     connection1.putObject("region1", "key-1", "value-2", new EventID(new byte[] {1},1,3), null);
//     this.system.getLogWriter().info("did put 2");
//     assertEquals(1, s.getInt("currentClients"));
//     assertEquals(1, s.getInt("currentClientConnections"));

//     // now lets see what happens when we close our connection
//     // note we use a nasty close which just closes the socket instead
//     // of sending a nice message to the server telling him we are going away
//     ((ConnectionImpl)connection1).finalizeConnection();
//     {
//       int retry = (TIME_BETWEEN_PINGS*5) / 100; 
//       while (s.getInt("currentClients") > 0 && retry-- > 0) {
//         Thread.sleep(100);
//       }
//       this.system.getLogWriter().info("currentClients="
//                                       + s.getInt("currentClients")
//                                       + " currentClientConnections="
//                                       + s.getInt("currentClientConnections"));
//       assertEquals(0, s.getInt("currentClients"));
//       assertEquals(0, s.getInt("currentClientConnections"));
//     }
  }
 public void addExceptions() throws Exception {
    if (this.system != null) {
      this.system.getLogWriter().info(
          "<ExpectedException action=add>" + "java.io.EOFException"
              + "</ExpectedException>");
    }
  }

 public void removeExceptions() {
    if (this.system != null) {
      this.system.getLogWriter().info(
          "<ExpectedException action=remove>" + "java.io.EOFException"
              + "</ExpectedException>");
    }
 }
}
