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
package com.gemstone.gemfire.internal.cache.ha;

import hydra.Log;

import java.util.Iterator;
import java.util.Properties;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionEvent;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.query.CqAttributes;
import com.gemstone.gemfire.cache.query.CqAttributesFactory;
import com.gemstone.gemfire.cache.query.CqListener;
import com.gemstone.gemfire.cache.query.CqQuery;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.cq.dunit.CqQueryTestListener;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.cache30.ClientServerTestCase;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.CacheServerImpl;
import com.gemstone.gemfire.internal.cache.Conflatable;
import com.gemstone.gemfire.internal.cache.HARegion;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientProxy;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheServerTestUtil;
import com.gemstone.gemfire.internal.cache.tier.sockets.ConflationDUnitTest;
import com.gemstone.gemfire.internal.cache.tier.sockets.HAEventWrapper;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.NetworkUtils;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;

/**
 * This Dunit test is to verify that when the dispatcher of CS dispatches the
 * Event , the peer's HARegionQueue should get the events removed from the HA
 * RegionQueues assuming the QRM thread has acted upon by that time
 * This is done in the following steps
 * 1. start server1 and server2
 * 2. start client1 and client2
 * 3. perform put operation from client1
 * 4. check the entry in the regionque of client2 on server2.It should be present.
 * 5. Wait till client2 receives event
 * 6. Make sure that QRM is envoked
 * 7. Again the entry in the regionque of client2 on server2.It should not be present.
 * 8. close client1 and client2
 * 9. close server1 and server2
 *
 *  @author Girish Thombare
 */

public class HADispatcherDUnitTest extends DistributedTestCase
{

  VM server1 = null;

  VM server2 = null;

  VM client1 = null;

  VM client2 = null;

  public static int PORT1;

  public static int PORT2;

  private static final String REGION_NAME = "HADispatcherDUnitTest_region";

  protected static Cache cache = null;

  public static final Object dummyObj = "dummyObject";

  static volatile boolean isObjectPresent = false;

  final static String KEY1 = "KEY1";

  final static String VALUE1 = "VALUE1";

  final static String KEY2 = "KEY2";

  final static String VALUE2 = "VALUE2";

  static volatile boolean waitFlag = true;

  public HADispatcherDUnitTest(String name) {
    super(name);
  }

  @Override
  public void setUp() throws Exception
  {
    super.setUp();
    final Host host = Host.getHost(0);
    
    // Server1 VM
    server1 = host.getVM(0);

    // Server2 VM
    server2 = host.getVM(1);

    // Client 1 VM
    client1 = host.getVM(2);

    // client 2 VM
    client2 = host.getVM(3);

    PORT1 = ((Integer)server1.invoke(HADispatcherDUnitTest.class,
        "createServerCache", new Object[] { new Boolean(false) })).intValue();
    server1.invoke(ConflationDUnitTest.class, "setIsSlowStart");
    server1.invoke(HADispatcherDUnitTest.class, "makeDispatcherSlow");
    server1.invoke(HADispatcherDUnitTest.class, "setQRMslow");
    PORT2 = ((Integer)server2.invoke(HADispatcherDUnitTest.class,
        "createServerCache", new Object[] { new Boolean(true) })).intValue();

    client1.invoke( CacheServerTestUtil.class, "disableShufflingOfEndpoints");
    client2.invoke( CacheServerTestUtil.class, "disableShufflingOfEndpoints");
    client1.invoke(HADispatcherDUnitTest.class, "createClientCache",
        new Object[] {
            NetworkUtils.getServerHostName(Host.getHost(0)),
            new Integer(PORT1), new Integer(PORT2),
            new Boolean(false) });
    client2.invoke(HADispatcherDUnitTest.class, "createClientCache",
        new Object[] {
            NetworkUtils.getServerHostName(Host.getHost(0)),
            new Integer(PORT1), new Integer(PORT2),
            new Boolean(true) });
    //createClientCache(new Integer(PORT1), new Integer(PORT2), new Boolean(true) );

  }

  @Override
  protected final void preTearDown() throws Exception {
    client1.invoke(HADispatcherDUnitTest.class, "closeCache");
    client2.invoke(HADispatcherDUnitTest.class, "closeCache");
    // close server
    server1.invoke(HADispatcherDUnitTest.class, "resetQRMslow");
    server1.invoke(HADispatcherDUnitTest.class, "closeCache");
    server2.invoke(HADispatcherDUnitTest.class, "closeCache");
  }

  public static void closeCache()
  {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }

  public static void setQRMslow()
  {
    int oldMessageSyncInterval = cache.getMessageSyncInterval();
    cache.setMessageSyncInterval(6);
    try {
      Thread.sleep((oldMessageSyncInterval + 1)*1000);
    }
    catch (InterruptedException e) {
      fail("Unexcepted InterruptedException Occurred");
    }
  }

  public static void resetQRMslow()
  {
    cache.setMessageSyncInterval(HARegionQueue.DEFAULT_MESSAGE_SYNC_INTERVAL);
  }


  public static void makeDispatcherSlow()
  {
    System.setProperty("slowStartTimeForTesting", "5000");
  }

  private void clientPut(VM vm, final Object key, final Object value) {
    // performing put from the client1
    vm.invoke(new CacheSerializableRunnable("putFromClient") {
      @Override
      public void run2() throws CacheException
      {
        Region region = cache.getRegion(Region.SEPARATOR + REGION_NAME);
        assertNotNull(region);
        region.put(key, value);
      }
    });
  }

  private void checkFromClient(VM vm) {
    // Waiting in the client till it receives the event for the key.
    vm.invoke(new CacheSerializableRunnable("checkFromClient") {
      @Override
      public void run2() throws CacheException
      {
        Region region = cache.getRegion(Region.SEPARATOR + REGION_NAME);
        assertNotNull(region);
        cache.getLogger().fine("starting the wait");        
        synchronized (dummyObj) {
          while (waitFlag) {
            try {
              dummyObj.wait(30000);
            }
            catch (InterruptedException e) {
              fail("interrupted");
            }
          }
        }
        cache.getLogger().fine("wait over...waitFlag=" + waitFlag);
        if (waitFlag)
          fail("test failed");
      }
    });
  }

  private void checkFromServer(VM vm, final Object key) {
    // Thread.sleep(10000); // why sleep if the invoke will retry?
    //  performing check in the regionqueue of the server2
    vm.invoke(new CacheSerializableRunnable("checkFromServer") {
      @Override
      public void run2() throws CacheException
      {
        Iterator iter = cache.getCacheServers().iterator();
        CacheServerImpl server = (CacheServerImpl)iter.next();
        Iterator iter_prox = server.getAcceptor().getCacheClientNotifier()
          .getClientProxies().iterator();
        isObjectPresent = false;
        while (iter_prox.hasNext()) {
          final CacheClientProxy proxy = (CacheClientProxy)iter_prox.next();
//          ClientProxyMembershipID proxyID = proxy.getProxyID();
/* Conflict from CQ branch ------------------------------------------------------
          Region regionqueue = cache.getRegion(Region.SEPARATOR
              + HARegionQueue.createRegionName(proxyID.toString()));
          assertNotNull(regionqueue);
          Iterator itr = regionqueue.values().iterator();
          while (itr.hasNext()) {
            Object obj = itr.next();
            if (obj
                .getClass()
                .getName()
                .equals(
                "com.gemstone.gemfire.internal.cache.tier.sockets.ClientUpdateMessage")) {
              Conflatable confObj = (Conflatable)obj;
              Log.getLogWriter().info("value of the object ");
              if (key.equals(confObj.getKeyToConflate()))
                isObjectPresent = true;
-------------------------------------------------------------------------------*/
            //HARegion region = (HARegion)cache.getRegion(Region.SEPARATOR
            //    + HAHelper.getRegionQueueName(proxyID.toString()));
            //assertNotNull(region);
            HARegion region = (HARegion) proxy.getHARegion();
            assertNotNull(region);
            final HARegionQueue regionQueue = region.getOwner();

            WaitCriterion wc = new WaitCriterion() {

              public boolean done() {
                int sz = regionQueue.size();
                cache.getLogger().fine("regionQueue.size()::"+ sz);
                return sz == 0 || !proxy.isConnected();
              }

              public String description() {
                return "regionQueue not empty with size " + regionQueue.size()
                    + " for proxy " + proxy;
              }
            };
            Wait.waitForCriterion(wc, 60 * 1000, 1000, true);
            cache.getLogger().fine("processed a proxy");
        }
      }
    });
  }    
  
  public void testDispatcher() throws Exception
  {
    clientPut(client1, KEY1, VALUE1);
    // Waiting in the client2 till it receives the event for the key.
    checkFromClient(client2);

    // performing check in the regionqueue of the server2
    checkFromServer(server2, KEY1);
    
    // For CQ Only.
    // performing put from the client1
    clientPut(client1, KEY2, VALUE2);
    checkFromClient(client2);
    checkFromServer(server2, KEY2);
    
    Log.getLogWriter().info("testDispatcher() completed successfully");
  }

  /*
   * This is to test the serialization mechanism of ClientUpdateMessage.
   * Added after CQ support. 
   * This could be done in different way, by overflowing the HARegion queue.
   * 
   */
  public void /*test*/ClientUpdateMessageSerialization() throws Exception
  {
    // Update Value.
    clientPut(client1, KEY1, VALUE1);
    Log.getLogWriter().fine(">>>>>>>> after clientPut(c1, k1, v1)");
    // Waiting in the client2 till it receives the event for the key.
    checkFromClient(client2);
    Log.getLogWriter().fine("after checkFromClient(c2)");

    // performing check in the regionqueue of the server2
    checkFromServer(server2, KEY1);
    Log.getLogWriter().fine("after checkFromServer(s2, k1)");

    // UPDATE.
    clientPut(client1, KEY1, VALUE1);
    Log.getLogWriter().fine("after clientPut 2 (c1, k1, v1)");
    // Waiting in the client2 till it receives the event for the key.
    checkFromClient(client2);
    Log.getLogWriter().fine("after checkFromClient 2 (c2)");

    // performing check in the regionqueue of the server2
    checkFromServer(server2, KEY1);
    Log.getLogWriter().fine("after checkFromServer 2 (s2, k1)");

    Log.getLogWriter().info("testClientUpdateMessageSerialization() completed successfully");
  }

  private void createCache(Properties props) throws Exception
  {
    DistributedSystem ds = getSystem(props);
    assertNotNull(ds);
    ds.disconnect();
    ds = getSystem(props);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
  }

  public static Integer createServerCache(Boolean isListenerPresent)
      throws Exception
  {
    new HADispatcherDUnitTest("temp").createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    if (isListenerPresent.booleanValue() == true) {
      CacheListener serverListener = new HAServerListener();
      factory.setCacheListener(serverListener);
    }
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);
    CacheServerImpl server = (CacheServerImpl)cache.addCacheServer();
    assertNotNull(server);
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.setPort(port);
    server.setNotifyBySubscription(true);
    server.start();
    return new Integer(server.getPort());
  }

  public static void createClientCache(String hostName, Integer port1, Integer port2,
      Boolean isListenerPresent) throws Exception
  {
    PORT1 = port1.intValue();
    PORT2 = port2.intValue();
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "");
    new HADispatcherDUnitTest("temp").createCache(props);
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    ClientServerTestCase.configureConnectionPool(factory, hostName, new int[] {PORT1,PORT2}, true, -1, 2, null);
    if (isListenerPresent.booleanValue() == true) {
      CacheListener clientListener = new HAClientListener();
      factory.setCacheListener(clientListener);
    }
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);
    Region region = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    assertNotNull(region);

    {
      LocalRegion lr = (LocalRegion)region;
      final PoolImpl pool = (PoolImpl)(lr.getServerProxy().getPool());
      WaitCriterion ev = new WaitCriterion() {
        public boolean done() {
          return pool.getPrimary() != null;
        }
        public String description() {
          return null;
        }
      };
      Wait.waitForCriterion(ev, 30 * 1000, 200, true);
      ev = new WaitCriterion() {
        public boolean done() {
          return pool.getRedundants().size() >= 1;
        }
        public String description() {
          return null;
        }
      };
      Wait.waitForCriterion(ev, 30 * 1000, 200, true);
      
      assertNotNull(pool.getPrimary());
      assertTrue("backups="+pool.getRedundants() + " expected=" + 1,
                 pool.getRedundants().size() >= 1);
      assertEquals(PORT1, pool.getPrimaryPort());
    }

    region.registerInterest(KEY1);

    // Register CQ.
    createCQ(); 
  }

  private static void createCQ(){
    QueryService cqService = null;
    try {
      cqService = cache.getQueryService();
    } catch (Exception cqe) {
      cqe.printStackTrace();
      fail("Failed to getCQService.");
    }
    
    // Create CQ Attributes.
    CqAttributesFactory cqf = new CqAttributesFactory();
    CqListener[] cqListeners = {new CqQueryTestListener(LogWriterUtils.getLogWriter())};    
    cqf.initCqListeners(cqListeners);
    CqAttributes cqa = cqf.create();
    
    String cqName = "CQForHARegionQueueTest";
    String queryStr = "Select * from " + Region.SEPARATOR + REGION_NAME;
    
    // Create CQ.
    try {
      CqQuery cq1 = cqService.newCq(cqName, queryStr, cqa);
      cq1.execute();
    } catch (Exception ex){
      LogWriterUtils.getLogWriter().info("CQService is :" + cqService);
      ex.printStackTrace();
      AssertionError err = new AssertionError("Failed to create/execute CQ " + cqName + " . ");
      err.initCause(ex);
      throw err;
    }
  }


  /**
   * This is the client listener which notifies the waiting thread when it
   * receives the event.
   */
  protected static class HAClientListener extends CacheListenerAdapter implements Declarable  {
    public void afterCreate(EntryEvent event)
    {
      synchronized (HADispatcherDUnitTest.dummyObj) {
        try {
          Object value = event.getNewValue();
          if (value.equals(HADispatcherDUnitTest.VALUE1)) {
            HADispatcherDUnitTest.waitFlag = false;
            HADispatcherDUnitTest.dummyObj.notifyAll();
          }
        }
        catch (Exception e) {
          e.printStackTrace();
        }
      }
    }

    public void afterUpdate(EntryEvent event)
    {

    }

    public void afterInvalidate(EntryEvent event)
    {

    }

    public void afterDestroy(EntryEvent event)
    {

    }

    public void afterRegionInvalidate(RegionEvent event)
    {

    }

    public void afterRegionDestroy(RegionEvent event)
    {

    }

    public void close()
    {

    }

    public void init(Properties props)
    {

    }
    public void afterRegionCreate(RegionEvent event)
    {
      // TODO Auto-generated method stub

    }
    public void afterRegionClear(RegionEvent event)
    {
      // TODO Auto-generated method stub

    }
    public void afterRegionLive(RegionEvent event)
    {
      // TODO NOT Auto-generated method stub, added by vrao

    }
  }

  /**
   * This is the server listener which ensures that regionqueue is properly populated
   */
  protected static class HAServerListener extends CacheListenerAdapter {
    @Override
    public void afterCreate(EntryEvent event)
    {
      Cache cache = event.getRegion().getCache();
      Iterator iter = cache.getCacheServers().iterator();
      CacheServerImpl server = (CacheServerImpl)iter.next();
      HADispatcherDUnitTest.isObjectPresent = false;

      // The event not be there in the region first time; try couple of time.
      // This should have been replaced by listener on the HARegion and doing wait for event arrival in that.
      while (true) { 
        Iterator iter_prox = server.getAcceptor().getCacheClientNotifier()
        .getClientProxies().iterator();
        while (iter_prox.hasNext()) {
          CacheClientProxy proxy = (CacheClientProxy)iter_prox.next();
//          ClientProxyMembershipID proxyID = proxy.getProxyID();
          HARegion regionForQueue = (HARegion)proxy.getHARegion();

          Iterator itr = regionForQueue.values().iterator();
          while (itr.hasNext()) {
            Object obj = itr.next();
            if (obj instanceof HAEventWrapper) {
              Conflatable confObj = (Conflatable)obj;
              if ((HADispatcherDUnitTest.KEY1).equals(confObj.getKeyToConflate()) ||
                  (HADispatcherDUnitTest.KEY2).equals(confObj.getKeyToConflate())) {
                HADispatcherDUnitTest.isObjectPresent = true;
              }
            }
          }
        }
        if (HADispatcherDUnitTest.isObjectPresent == true) {
          break; // From while.
        }
        try {
          Thread.sleep(10);
        } catch (InterruptedException ex) {fail("interrupted");}
      }
    }

    // this test is no longer needed since these
    // messages are not longer Externalizable
//  /*
//  * This is for testing ClientUpdateMessage's serialization code.
//  */
//  public void afterUpdate(EntryEvent event)
//  {
//  Log.getLogWriter().info("In HAServerListener::AfterUpdate::Event=" + event);
//  Cache cache = event.getRegion().getCache();
//  Iterator iter = cache.getCacheServers().iterator();
//  BridgeServerImpl server = (BridgeServerImpl)iter.next();
//  HADispatcherDUnitTest.isObjectPresent = false;

//  // The event not be there in the region first time; try couple of time.
//  // This should have been replaced by listener on the HARegion and doing wait for event arrival in that.
//  while (true) { 

//  Iterator iter_prox = server.getAcceptor().getCacheClientNotifier()
//  .getClientProxies().iterator();
//  while (iter_prox.hasNext()) {
//  CacheClientProxy proxy = (CacheClientProxy)iter_prox.next();
//  ClientProxyMembershipID proxyID = proxy.getProxyID();
//  HARegion regionForQueue = (HARegion)cache.getRegion(Region.SEPARATOR
//  + HARegionQueue.createRegionName(proxyID.toString()));
//  if (regionForQueue == null) {
//  // I observed dunit throwing an NPE here.
//  // I changed it to just keep retrying which caused dunit to hang.
//  // The queue is gone we are shutting down so just return
//  return;
//  }
//  Iterator itr = regionForQueue.values().iterator();
//  while (itr.hasNext()) {
//  Object obj = itr.next();
//  if (obj.getClass().getName().equals(
//  "com.gemstone.gemfire.internal.cache.tier.sockets.ClientUpdateMessage")) {
//  com.gemstone.gemfire.internal.cache.tier.sockets.ClientUpdateMessage clientUpdateMessage = 
//  (com.gemstone.gemfire.internal.cache.tier.sockets.ClientUpdateMessage)obj;
//  try{
//  // Test for readExternal(), writeExternal().
//  ByteArrayOutputStream outStream = new ByteArrayOutputStream();
//  ObjectOutputStream out = new ObjectOutputStream(outStream);
//  clientUpdateMessage.writeExternal(out);
//  out.flush();
//  ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(outStream.toByteArray()));

//  com.gemstone.gemfire.internal.cache.tier.sockets.ClientUpdateMessage newClientUpdateMessage = 
//  new com.gemstone.gemfire.internal.cache.tier.sockets.ClientUpdateMessageImpl();

//  newClientUpdateMessage.readExternal(in);

//  Log.getLogWriter().info("Newly constructed ClientUpdateMessage is :" + newClientUpdateMessage.toString());
//  if (!newClientUpdateMessage.hasCqs() || 
//  ((ClientUpdateMessageImpl)newClientUpdateMessage).getClientCqs() == null ||
//  ((ClientUpdateMessageImpl)newClientUpdateMessage).getClientCqs().size() != 2){
//  throw new Exception("CQ Info not present");
//  }
//  HashMap clientCQs = ((ClientUpdateMessageImpl)newClientUpdateMessage).getClientCqs();

//  // Try to print CQ details - debug.
//  for (Iterator cciter = clientCQs.keySet().iterator(); cciter.hasNext();) {
//  ClientProxyMembershipID proxyId = (ClientProxyMembershipID)cciter.next();

//  HashMap cqs = (HashMap)clientCQs.get(proxyId);
//  Log.getLogWriter().info("Client ID is :" + proxyId); 

//  for (Iterator cqIter = cqs.keySet().iterator();cqIter.hasNext();){
//  // Add CQ Name.
//  String cq = (String)cqIter.next();
//  // Add CQ Op.
//  Log.getLogWriter().info("CQ Name :" + cq + " CQ OP :" + ((Integer)cqs.get(cq)).intValue());
//  }

//  }

//  // Test for toData(), fromData().
//  ByteArrayOutputStream dataOutStream = new ByteArrayOutputStream();
//  DataOutputStream dataout = new DataOutputStream(dataOutStream);
//  clientUpdateMessage.toData(dataout);
//  dataOutStream.flush();
//  DataInputStream datain = new DataInputStream(new ByteArrayInputStream(dataOutStream.toByteArray()));

//  com.gemstone.gemfire.internal.cache.tier.sockets.ClientUpdateMessage newClientUpdateMessage2 = 
//  new com.gemstone.gemfire.internal.cache.tier.sockets.ClientUpdateMessageImpl();

//  newClientUpdateMessage2.fromData(datain);

//  Log.getLogWriter().info("Newly constructed ClientUpdateMessage is :" + newClientUpdateMessage2.toString());
//  if (!newClientUpdateMessage2.hasCqs() || 
//  ((ClientUpdateMessageImpl)newClientUpdateMessage2).getClientCqs() == null ||
//  ((ClientUpdateMessageImpl)newClientUpdateMessage2).getClientCqs().size() != 2){
//  throw new Exception("CQ Info not present");
//  }

//  } catch (Exception ex) {
//  Log.getLogWriter().info("Exception while serializing ClientUpdateMessage.", ex);
//  return;
//  }
//  HADispatcherDUnitTest.isObjectPresent = true;
//  }
//  }
//  }
//  if (HADispatcherDUnitTest.isObjectPresent == true) {
//  break; // From while.
//  }
//  try {
//  Thread.sleep(10);
//  } catch (Exception ex) {}
//  }
//  }
  }
}
