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



import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.InterestResultPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.cache30.ClientServerTestCase;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.CacheServerImpl;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.NetworkUtils;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * This is the Dunit test to verify the duplicates after the fail over
 * The test perorms following operations
 * 1. Create 2 servers and 1 client
 * 2. Perform put operations for knows set of keys directy from the server1.
 * 3. Stop the server1 so that fail over happens
 * 4. Validate the duplicates received by the client1
 *
 * @author Girish Thombare
 *
 */

public class HADuplicateDUnitTest extends DistributedTestCase
{

  VM server1 = null;

  VM server2 = null;

  VM client1 = null;

  VM client2 = null;

  public static int PORT1;

  public static int PORT2;

  private static final String REGION_NAME = "HADuplicateDUnitTest_Region";

  protected static Cache cache = null;

  static boolean isEventDuplicate = true;

  static CacheServerImpl server = null;

  static final int NO_OF_PUTS = 100;

  public static final Object dummyObj = "dummyObject";

  static boolean waitFlag = true;

  static int put_counter = 0;

  static Map storeEvents = new HashMap();

  public HADuplicateDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception
  {
    super.setUp();
    final Host host = Host.getHost(0);

    server1 = host.getVM(0);

    server2 = host.getVM(1);

    client1 = host.getVM(2);

    client2 = host.getVM(3);
  }

  @Override
  protected final void preTearDown() throws Exception {
    client1.invoke(HADuplicateDUnitTest.class, "closeCache");
    // close server
    server1.invoke(HADuplicateDUnitTest.class, "reSetQRMslow");
    server1.invoke(HADuplicateDUnitTest.class, "closeCache");
    server2.invoke(HADuplicateDUnitTest.class, "closeCache");
  }

  public void _testDuplicate() throws Exception
  {
    createClientServerConfiguration();
    server1.invoke(putForKnownKeys());
    server1.invoke(stopServer());

    // wait till all the duplicates are received by client
    client1.invoke(new CacheSerializableRunnable("waitForPutToComplete") {

      public void run2() throws CacheException
      {
        synchronized (dummyObj) {
          while (waitFlag) {
            try {
              dummyObj.wait();
            }
            catch (InterruptedException e) {
              fail("interrupted");
            }
          }
        }

        if (waitFlag)
          fail("test failed");
      }
    });

    // validate the duplicates received by client
    client1.invoke(new CacheSerializableRunnable("validateDuplicates") {
      public void run2() throws CacheException
      {
        if (!isEventDuplicate)
          fail(" Not all duplicates received");

      }
    });

    server1.invoke(HADuplicateDUnitTest.class, "reSetQRMslow");
  }


  public void testSample() throws Exception
  {

    IgnoredException.addIgnoredException("IOException");
    IgnoredException.addIgnoredException("Connection reset");
    createClientServerConfiguration();
    server1.invoke(new CacheSerializableRunnable("putKey") {

    public void run2() throws CacheException
    {
      Region region = cache.getRegion(Region.SEPARATOR + REGION_NAME);
      assertNotNull(region);
      region.put("key1","value1");

    }
    });

  }


  // function to perform put operations for the known set of keys.
  private CacheSerializableRunnable putForKnownKeys()
  {

    CacheSerializableRunnable putforknownkeys = new CacheSerializableRunnable(
        "putforknownkeys") {

      public void run2() throws CacheException
      {
        Region region = cache.getRegion(Region.SEPARATOR + REGION_NAME);
        assertNotNull(region);
        for (int i = 0; i < NO_OF_PUTS; i++) {
          region.put("key" + i, "value" + i);
        }
      }

    };

    return putforknownkeys;
  }

  // function to stop server so that the fail over happens
  private CacheSerializableRunnable stopServer()
  {

    CacheSerializableRunnable stopserver = new CacheSerializableRunnable(
        "stopServer") {
      public void run2() throws CacheException
      {
        server.stop();
      }

    };

    return stopserver;
  }



  // function to create 2servers and 1 clients
  private void createClientServerConfiguration()
  {
    PORT1 = ((Integer)server1.invoke(HADuplicateDUnitTest.class,
        "createServerCache")).intValue();
    server1.invoke(HADuplicateDUnitTest.class, "setQRMslow");
    PORT2 = ((Integer)server2.invoke(HADuplicateDUnitTest.class,
        "createServerCache")).intValue();
    client1.invoke(HADuplicateDUnitTest.class, "createClientCache",
        new Object[] { NetworkUtils.getServerHostName(Host.getHost(0)), new Integer(PORT1), new Integer(PORT2) });

  }

  // function to set QRM slow
  public static void setQRMslow()
  {
    System.setProperty("QueueRemovalThreadWaitTime", "100000");
  }

  public static void reSetQRMslow()
  {
    System.setProperty("QueueRemovalThreadWaitTime", "1000");
  }

  public static Integer createServerCache() throws Exception
  {
    new HADuplicateDUnitTest("temp").createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);
    server = (CacheServerImpl)cache.addCacheServer();
    assertNotNull(server);
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.setPort(port);
    server.setNotifyBySubscription(true);
    server.start();
    return new Integer(server.getPort());
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

  public static void createClientCache(String hostName, Integer port1, Integer port2)
      throws Exception
  {
    PORT1 = port1.intValue();
    PORT2 = port2.intValue();
    Properties props = new Properties();
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    new HADuplicateDUnitTest("temp").createCache(props);
    AttributesFactory factory = new AttributesFactory();
    ClientServerTestCase.configureConnectionPool(factory, hostName, new int[] {PORT1,PORT2}, true, -1, 2, null);
    
    factory.setScope(Scope.DISTRIBUTED_ACK);
    CacheListener clientListener = new HAValidateDuplicateListener();
    factory.setCacheListener(clientListener);

    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);
    Region region = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    assertNotNull(region);
    region.registerInterest("ALL_KEYS", InterestResultPolicy.NONE);

  }

  public static void closeCache()
  {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }

}

// Listener class for the validation purpose
class HAValidateDuplicateListener extends CacheListenerAdapter
{
  public void afterCreate(EntryEvent event)
  {
    System.out.println("After Create");
    HADuplicateDUnitTest.storeEvents.put(event.getKey(), event.getNewValue());

  }

  public void afterUpdate(EntryEvent event)
  {
    Object value = HADuplicateDUnitTest.storeEvents.get(event.getKey());
    if (value == null)
      HADuplicateDUnitTest.isEventDuplicate = false;
    synchronized (HADuplicateDUnitTest.dummyObj) {
      try {
        HADuplicateDUnitTest.put_counter++;
        if (HADuplicateDUnitTest.put_counter == HADuplicateDUnitTest.NO_OF_PUTS) {
          HADuplicateDUnitTest.waitFlag = false;
          HADuplicateDUnitTest.dummyObj.notifyAll();
        }
      }
      catch (Exception e) {
        e.printStackTrace();
      }
    }

  }
}
