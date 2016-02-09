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
/*
 * Created on Feb 3, 2006
 *
 */
package com.gemstone.gemfire.internal.cache.tier.sockets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Map;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.PoolFactory;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.internal.Connection;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.client.internal.PutOp;
import com.gemstone.gemfire.cache.client.internal.QueueStateImpl.SequenceIdAndExpirationObject;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.cache.ha.ThreadIdentifier;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * @author asif
 *
 * Tests the functionality of operations of AbstractConnectionProxy & its
 * derived classes.
 */
@Category(IntegrationTest.class)
public class ConnectionProxyJUnitTest
{
  private static final String expectedRedundantErrorMsg = "Could not find any server to create redundant client queue on.";
  private static final String expectedPrimaryErrorMsg = "Could not find any server to create primary client queue on.";

  DistributedSystem system;

  Cache cache;

  PoolImpl proxy = null;
  
  SequenceIdAndExpirationObject seo = null;
  
  @Before
  public void setUp() throws Exception
  {

    Properties p = new Properties();
    p.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    p.setProperty(DistributionConfig.LOCATORS_NAME, "");
    this.system = DistributedSystem.connect(p);
    this.cache = CacheFactory.create(system);
    final String addExpectedPEM =
        "<ExpectedException action=add>" + expectedPrimaryErrorMsg + "</ExpectedException>";
    final String addExpectedREM =
        "<ExpectedException action=add>" + expectedRedundantErrorMsg + "</ExpectedException>";
    system.getLogWriter().info(addExpectedPEM);
    system.getLogWriter().info(addExpectedREM);
  }

  @After
  public void tearDown() throws Exception
  {
    this.cache.close();
    
    final String removeExpectedPEM =
        "<ExpectedException action=remove>" + expectedPrimaryErrorMsg + "</ExpectedException>";
    final String removeExpectedREM =
        "<ExpectedException action=remove>" + expectedRedundantErrorMsg + "</ExpectedException>";
    
    system.getLogWriter().info(removeExpectedPEM);
    system.getLogWriter().info(removeExpectedREM);
    
    this.system.disconnect();
    if (proxy != null)
      proxy.destroy();
  }

  /**
   * This test verifies the behaviour of client request when the listener on the
   * server sits forever. This is done in following steps:<br>
   * 1)create server<br>
   * 2)initialize proxy object and create region for client having a
   * CacheListener and make afterCreate in the listener to wait infinitely<br>
   * 3)perform a PUT on client by acquiring Connection through proxy<br>
   * 4)Verify that exception occurs due to infinite wait in the listener<br>
   * 5)Verify that above exception occurs sometime after the readTimeout
   * configured for the client <br>
   *
   * @author Dinesh
   */
  public void DISABLE_testListenerOnServerSitForever()
  {
    int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    Region testRegion = null ;
    try {
      CacheServer server = this.cache.addCacheServer();
      server.setMaximumTimeBetweenPings(10000);
      server.setPort(port3);
      server.start();
    }
    catch (Exception e) {
      e.printStackTrace();
      fail("Failed to create server");
    }
    try {
      PoolFactory pf = PoolManager.createFactory();
      pf.addServer("localhost", port3);
      pf.setSubscriptionEnabled(false);
      pf.setSubscriptionRedundancy(-1);
      pf.setReadTimeout(2000);
      pf.setThreadLocalConnections(true);
      pf.setSocketBufferSize(32768);
      pf.setRetryAttempts(1);
      pf.setPingInterval(10000);
      
      proxy = (PoolImpl) pf.create("clientPool");

      AttributesFactory factory = new AttributesFactory();
      factory.setScope(Scope.DISTRIBUTED_ACK);
      factory.setCacheListener(new CacheListenerAdapter() {
        public void afterCreate(EntryEvent event)
        {
          synchronized (ConnectionProxyJUnitTest.this) {
            try {
              ConnectionProxyJUnitTest.this.wait();
            }
            catch (InterruptedException e) {
              fail("interrupted");
            }
          }
        }
      });
      RegionAttributes attrs = factory.create();
      testRegion = cache.createRegion("testregion", attrs);

    }
    catch (Exception ex) {
      ex.printStackTrace();
      fail("Failed to initialize client");
    }
    Connection conn = (proxy).acquireConnection();
    long t1 = 0;
    try {
      t1 = System.currentTimeMillis();
      EntryEventImpl event = new EntryEventImpl((Object)null);
      try {
      event.setEventId(new EventID(new byte[] { 1 },1,1));
      PutOp.execute(conn, proxy, testRegion.getFullPath(), "key1", "val1", event, null, false);
      } finally {
        event.release();
      }
      fail("Test failed as exception was expected");
    }
    catch (Exception e) {
      long t2 = System.currentTimeMillis();
      long net = (t2 - t1);
      assertTrue(net / 1000 < 5);
    }
    synchronized (ConnectionProxyJUnitTest.this) {
      ConnectionProxyJUnitTest.this.notify();
    }
  }
  /**
   * Tests the DeadServerMonitor when identifying an 
   * Endpoint as alive , does not create a persistent Ping connection
   * ( i.e sends a CLOSE protocol , if the number of connections is zero.
   * @author Asif
   */
  @Test
  public void testDeadServerMonitorPingNature1() {
    int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    
//    final int maxWaitTime = 10000;    
    try {
      PoolFactory pf = PoolManager.createFactory();
      pf.addServer("localhost", port3);
      pf.setSubscriptionEnabled(false);
      pf.setReadTimeout(2000);
      pf.setThreadLocalConnections(true);
      pf.setMinConnections(1);
      pf.setSocketBufferSize(32768);
      pf.setRetryAttempts(1);
      pf.setPingInterval(500);
      
      proxy = (PoolImpl) pf.create("clientPool");
    }catch (Exception ex) {
      ex.printStackTrace();
      fail("Failed to initialize client");
    }
    try {
      (proxy).acquireConnection();
    }catch(Exception ok) {
     ok.printStackTrace(); 
    }
    
    try {
      (proxy).acquireConnection();
    }catch(Exception ok) {
     ok.printStackTrace(); 
    }
    
//    long start = System.currentTimeMillis();
    assertEquals(0, proxy.getConnectedServerCount());
    //start the server
   CacheServer server = null;
   try {
   try {
     server = this.cache.addCacheServer();
      server.setMaximumTimeBetweenPings(15000);
      server.setPort(port3);
      server.start();
    }
    catch (Exception e) {
      e.printStackTrace();
      fail("Failed to create server");
    }
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return proxy.getConnectedServerCount() == 1;
      }
      public String description() {
        return null;
      }
    };
    Wait.waitForCriterion(ev, 90 * 1000, 200, true);
   }
   finally {
     if (server != null) {
       server.stop();
     }
   }
  }
  
  /**
   * Tests the DeadServerMonitor when identifying an 
   * Endpoint as alive , does creates a persistent Ping connection
   * ( i.e sends a PING protocol , if the number of connections is more than 
   * zero.
   * @author Asif
   */
  @Test
  public void testDeadServerMonitorPingNature2() {
    int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    
//    final int maxWaitTime = 10000;    
    try {
      PoolFactory pf = PoolManager.createFactory();
      pf.addServer("localhost", port3);
      pf.setSubscriptionEnabled(false);
      pf.setReadTimeout(2000);
      pf.setThreadLocalConnections(true);
      pf.setMinConnections(1);
      pf.setSocketBufferSize(32768);
      pf.setRetryAttempts(1);
      pf.setPingInterval(500);
      proxy = (PoolImpl) pf.create("clientPool");
    }catch (Exception ex) {
      ex.printStackTrace();
      fail("Failed to initialize client");
    }
    //let LiveServerMonitor detect it as alive as the numConnection is more than zero
    
//    long start = System.currentTimeMillis();
    assertEquals(0, proxy.getConnectedServerCount());
    //start the server
   CacheServer server = null;
   try {
   try {
     server = this.cache.addCacheServer();
      server.setMaximumTimeBetweenPings(15000);
      server.setPort(port3);
      server.start();
    }
    catch (Exception e) {
      e.printStackTrace();
      fail("Failed to create server");
    }
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return proxy.getConnectedServerCount() == 1;
      }
      public String description() {
        return null;
      }
    };
    Wait.waitForCriterion(ev, 90 * 1000, 200, true);
   }
   finally {
     if (server != null) {
       server.stop();
     }
   }
  }

 @Test
  public void testThreadIdToSequenceIdMapCreation()
  {
    int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
   CacheServer server = null;
   try {
   try {
     server = this.cache.addCacheServer();
      server.setMaximumTimeBetweenPings(10000);
      server.setPort(port3);
      server.start();
    }
    catch (Exception e) {
      e.printStackTrace();
      fail("Failed to create server");
    }
    try {
      PoolFactory pf = PoolManager.createFactory();
      pf.addServer("localhost", port3);
      pf.setSubscriptionEnabled(true);
      pf.setSubscriptionRedundancy(-1);
      proxy = (PoolImpl) pf.create("clientPool");
      if (proxy.getThreadIdToSequenceIdMap() == null) {
        fail(" ThreadIdToSequenceIdMap is null. ");
      }
    }
    catch (Exception ex) {
      ex.printStackTrace();
      fail("Failed to initialize client");
    }
   }
   finally {
     if (server != null) {
       try {
         Thread.sleep(500);
       }
       catch (InterruptedException ie) {
         fail("interrupted");
       }
       server.stop();
     }
   }
  }

 @Test
  public void testThreadIdToSequenceIdMapExpiryPositive()
 {
   int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
   CacheServer server = null;
   try {
   try {
     server = this.cache.addCacheServer();
     server.setMaximumTimeBetweenPings(10000);
     server.setPort(port3);
     server.start();
   }
   catch (Exception e) {
     e.printStackTrace();
     fail("Failed to create server");
   }
   try {
     PoolFactory pf = PoolManager.createFactory();
     pf.addServer("localhost", port3);
     pf.setSubscriptionEnabled(true);
     pf.setSubscriptionRedundancy(-1);
     pf.setSubscriptionMessageTrackingTimeout(4000);
     pf.setSubscriptionAckInterval(2000);
     proxy = (PoolImpl) pf.create("clientPool");

     EventID eid = new EventID(new byte[0],1,1);
     if(proxy.verifyIfDuplicate(eid)){
       fail(" eid should not be duplicate as it is a new entry");
     }

     verifyExpiry(60 * 1000);

     if(proxy.verifyIfDuplicate(eid)){
       fail(" eid should not be duplicate as the previous entry should have expired ");
     }

   }
   catch (Exception ex) {
     ex.printStackTrace();
     fail("Failed to initialize client");
   }
   }
   finally {
     if (server != null) {
       server.stop();
     }
   }
 }


 @Test
  public void testThreadIdToSequenceIdMapExpiryNegative()
 {
   int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
   CacheServer server = null;
   try {
   try {
     server = this.cache.addCacheServer();
     server.setMaximumTimeBetweenPings(10000);     
     server.setPort(port3);
     server.start();
   }
   catch (Exception e) {
     e.printStackTrace();
     fail("Failed to create server");
   }
   try {
     PoolFactory pf = PoolManager.createFactory();
     pf.addServer("localhost", port3);
     pf.setSubscriptionEnabled(true);
     pf.setSubscriptionRedundancy(-1);
     pf.setSubscriptionMessageTrackingTimeout(10000);
     
     proxy = (PoolImpl) pf.create("clientPool");

     final EventID eid = new EventID(new byte[0],1,1);
     if(proxy.verifyIfDuplicate(eid)){
       fail(" eid should not be duplicate as it is a new entry");
     }

     WaitCriterion ev = new WaitCriterion() {
       public boolean done() {
         return proxy.verifyIfDuplicate(eid);
       }
       public String description() {
         return null;
       }
     };
     Wait.waitForCriterion(ev, 20 * 1000, 200, true);
   }
   catch (Exception ex) {
     ex.printStackTrace();
     fail("Failed to initialize client");
   }
   }
   finally {
     if (server != null) {
       server.stop();
     }
   }
 }

 @Test
  public void testThreadIdToSequenceIdMapConcurrency()
 {
   int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
   CacheServer server = null;
   try {
   try {
     server = this.cache.addCacheServer();
     server.setMaximumTimeBetweenPings(10000);
     server.setPort(port3);
     server.start();
   }
   catch (Exception e) {
     e.printStackTrace();
     fail("Failed to create server");
   }
   try {
     PoolFactory pf = PoolManager.createFactory();
     pf.addServer("localhost", port3);
     pf.setSubscriptionEnabled(true);
     pf.setSubscriptionRedundancy(-1);
     pf.setSubscriptionMessageTrackingTimeout(5000);
     pf.setSubscriptionAckInterval(2000);
     proxy = (PoolImpl) pf.create("clientPool");
     
     final int EVENT_ID_COUNT = 10000; // why 10,000?
     EventID[] eid = new EventID[EVENT_ID_COUNT];
     for (int i = 0; i < EVENT_ID_COUNT; i++) {
       eid[i] = new EventID(new byte[0],i,i);
       if(proxy.verifyIfDuplicate(eid[i])){
         fail(" eid can never be duplicate, it is being created for the first time! ");
       }
       }
     verifyExpiry(30 * 1000);

     for (int i = 0; i < EVENT_ID_COUNT; i++) {
       if(proxy.verifyIfDuplicate(eid[i])){
         fail(" eid can not be found to be  duplicate since the entry should have expired! "+i);
       }
      }
   }
   catch (Exception ex) {
     ex.printStackTrace();
     fail("Failed to initialize client");
   }
   }
   finally {
     if (server != null) {
       server.stop();
     }
   }
 }



 @Test
  public void testDuplicateSeqIdLesserThanCurrentSeqIdBeingIgnored()
 {
   int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
   CacheServer server = null;
   try {
   try {
     server = this.cache.addCacheServer();
     server.setMaximumTimeBetweenPings(10000);
     server.setPort(port3);
     server.start();
   }
   catch (Exception e) {
     e.printStackTrace();
     fail("Failed to create server");
   }
   try {
     PoolFactory pf = PoolManager.createFactory();
     pf.addServer("localhost", port3);
     pf.setSubscriptionEnabled(true);
     pf.setSubscriptionRedundancy(-1);
     pf.setSubscriptionMessageTrackingTimeout(100000);
     proxy = (PoolImpl) pf.create("clientPool");
     
     EventID eid1 = new EventID(new byte[0],1,5);
     if(proxy.verifyIfDuplicate(eid1)){
       fail(" eid1 can never be duplicate, it is being created for the first time! ");
     }

     EventID eid2 = new EventID(new byte[0],1,2);

     if(!proxy.verifyIfDuplicate(eid2)){
       fail(" eid2 should be duplicate, seqId is less than highest (5)");
     }

     EventID eid3 = new EventID(new byte[0],1,3);

     if(!proxy.verifyIfDuplicate(eid3)){
       fail(" eid3 should be duplicate, seqId is less than highest (5)");
     }

     assertTrue(!proxy.getThreadIdToSequenceIdMap().isEmpty());
     proxy.destroy();
   }
   catch (Exception ex) {
     ex.printStackTrace();
     fail("Failed to initialize client");
   }
   }
   finally {
     if (server != null) {
       server.stop();
     }
   }
 }




 @Test
  public void testCleanCloseOfThreadIdToSeqId()
 {
   int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
   CacheServer server = null;
   try {
   try {
     server = this.cache.addCacheServer();
     server.setMaximumTimeBetweenPings(10000);
     server.setPort(port3);
     server.start();
   }
   catch (Exception e) {
     e.printStackTrace();
     fail("Failed to create server");
   }
   try {
     PoolFactory pf = PoolManager.createFactory();
     pf.addServer("localhost", port3);
     pf.setSubscriptionEnabled(true);
     pf.setSubscriptionRedundancy(-1);
     pf.setSubscriptionMessageTrackingTimeout(100000);
     proxy = (PoolImpl) pf.create("clientPool");
     
     EventID eid1 = new EventID(new byte[0],1,2);
     if(proxy.verifyIfDuplicate(eid1)){
         fail(" eid can never be duplicate, it is being created for the first time! ");
       }
     EventID eid2 = new EventID(new byte[0],1,3);
     if(proxy.verifyIfDuplicate(eid2)){
         fail(" eid can never be duplicate, since sequenceId is greater ");
       }

     if(!proxy.verifyIfDuplicate(eid2)){
       fail(" eid had to be a duplicate, since sequenceId is equal ");
     }
     EventID eid3 = new EventID(new byte[0],1,1);
     if(!proxy.verifyIfDuplicate(eid3)){
         fail(" eid had to be a duplicate, since sequenceId is lesser ");
       }   }
   catch (Exception ex) {
     ex.printStackTrace();
     fail("Failed to initialize client");
   }
   }
   finally {
     if (server != null) {
       try {
          Thread.sleep(500);
        }
        catch (InterruptedException ie) {
          fail("interrupted");
        }
       server.stop();
     }
   }
 }

 @Test
  public void testTwoClientsHavingDifferentThreadIdMaps()
 {
   int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
   CacheServer server = null;
   try {
   try {
     server = this.cache.addCacheServer();
     server.setMaximumTimeBetweenPings(10000);
     server.setPort(port3);
     server.start();
   }
   catch (Exception e) {
     e.printStackTrace();
     fail("Failed to create server");
   }
   try {
     PoolFactory pf = PoolManager.createFactory();
     pf.addServer("localhost", port3);
     pf.setSubscriptionEnabled(true);
     pf.setSubscriptionRedundancy(-1);
     pf.setSubscriptionMessageTrackingTimeout(100000);
     
     PoolImpl proxy1 = (PoolImpl) pf.create("clientPool1");
     try {
     PoolImpl proxy2 = (PoolImpl) pf.create("clientPool2");
     try {

     Map map1 = proxy1.getThreadIdToSequenceIdMap();
     Map map2 = proxy2.getThreadIdToSequenceIdMap();

     assertTrue(!(map1==map2));

     } finally {
       proxy2.destroy();
     }
     } finally {
       proxy1.destroy();
     }
    }
   catch (Exception ex) {
     ex.printStackTrace();
     fail("Failed to initialize client");
   }
   }
   finally {
     if (server != null) {
       server.stop();
     }
   }
 }

 @Test
  public void testPeriodicAckSendByClient()
 {
   int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
   CacheServer server = null;
   try {
   try {
     server = this.cache.addCacheServer();
     server.setPort(port);
     server.start();
   }
   catch (Exception e) {
     e.printStackTrace();
     fail("Failed to create server");
   }
   try {
     PoolFactory pf = PoolManager.createFactory();
     pf.addServer("localhost", port);
     pf.setSubscriptionEnabled(true);
     pf.setSubscriptionRedundancy(1);
     pf.setReadTimeout(20000);
     pf.setSubscriptionMessageTrackingTimeout(15000);
     pf.setSubscriptionAckInterval(5000);
     
     proxy = (PoolImpl) pf.create("clientPool");
     
     EventID eid = new EventID(new byte[0],1,1);
     
     if(proxy.verifyIfDuplicate(eid)){
       fail(" eid should not be duplicate as it is a new entry");
     }        
     
     seo = (SequenceIdAndExpirationObject)proxy.
     getThreadIdToSequenceIdMap().get(new ThreadIdentifier(new byte[0],1));    
     assertFalse(seo.getAckSend());
     
//   should send the ack to server     
      seo = (SequenceIdAndExpirationObject)proxy.
         getThreadIdToSequenceIdMap().get(new ThreadIdentifier(new byte[0],1));
      verifyAckSend(60 * 1000,true);
      
//   New update on same threadId   
      eid = new EventID(new byte[0],1,2);     
     if(proxy.verifyIfDuplicate(eid)){
       fail(" eid should not be duplicate as it is a new entry");
     }     
     seo = (SequenceIdAndExpirationObject)proxy.
     getThreadIdToSequenceIdMap().get(new ThreadIdentifier(new byte[0],1));
     assertFalse(seo.getAckSend());

//   should send another ack to server    
     seo = (SequenceIdAndExpirationObject)proxy.
     getThreadIdToSequenceIdMap().get(new ThreadIdentifier(new byte[0],1));
     verifyAckSend(6000,true);    
     
   // should expire with the this mentioned. 
     verifyExpiry(15 * 1000);
   }
   catch (Exception ex) {
     ex.printStackTrace();
     fail("Test testPeriodicAckSendByClient Failed");
   }
   }
   finally {
     if (server != null) {
       server.stop();
     }
   }
 }

 //No ack will be send if Redundancy level = 0
 @Test
  public void testNoAckSendByClient()
 {
   int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
   CacheServer server = null;
   try {
   try {
     server = this.cache.addCacheServer();
     server.setPort(port);
     server.start();
   }
   catch (Exception e) {
     e.printStackTrace();
     fail("Failed to create server");
   }
   try {
     PoolFactory pf = PoolManager.createFactory();
     pf.addServer("localhost", port);
     pf.setSubscriptionEnabled(true);
     pf.setSubscriptionRedundancy(1);
     pf.setReadTimeout(20000);
     pf.setSubscriptionMessageTrackingTimeout(8000);
     pf.setSubscriptionAckInterval(2000);
     
     proxy = (PoolImpl) pf.create("clientPool");
     
     EventID eid = new EventID(new byte[0],1,1);
     
     if(proxy.verifyIfDuplicate(eid)){
       fail(" eid should not be duplicate as it is a new entry");
     }
     
     seo = (SequenceIdAndExpirationObject)proxy.
     getThreadIdToSequenceIdMap().get(new ThreadIdentifier(new byte[0],1));    
     assertFalse(seo.getAckSend());
    
    //  should not send an ack as redundancy level = 0;
     seo = (SequenceIdAndExpirationObject)proxy.
     getThreadIdToSequenceIdMap().get(new ThreadIdentifier(new byte[0],1));     
     verifyAckSend(30 * 1000, false);
     
     // should expire without sending an ack as redundancy level = 0.       
     verifyExpiry(90 * 1000);      
     }
     
     catch (Exception ex) {
       ex.printStackTrace();
       fail("Test testPeriodicAckSendByClient Failed");
     }
   }
   finally {
     if (server != null) {
       server.stop();
     }
   }
  }
 
  private void verifyAckSend(long timeToWait, final boolean expectedAckSend )
  { 
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        return expectedAckSend == seo.getAckSend();
      }
      public String description() {
        return "ack flag never became " + expectedAckSend;
      }
    };
    Wait.waitForCriterion(wc, timeToWait, 1000, true);
  }  
  
  private void verifyExpiry(long timeToWait)
  {
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        return 0 == proxy.getThreadIdToSequenceIdMap().size();
      }
      public String description() {
        return "Entry never expired";
      }
    };
    Wait.waitForCriterion(wc, timeToWait * 2, 200, true);
  }
 
}
