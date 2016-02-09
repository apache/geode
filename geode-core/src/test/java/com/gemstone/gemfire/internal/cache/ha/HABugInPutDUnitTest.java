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



import java.util.Properties;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache30.ClientServerTestCase;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.CacheServerImpl;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.NetworkUtils;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * This Dunit test is to verify the bug in put() operation. When the put is invoked on the server
 * and NotifyBySubscription is false then it follows normal path and then again calls put of region
 * on which regionqueue is based. so recurssion is happening.
 *
 * @author Girish Thombare
 */

public class HABugInPutDUnitTest extends DistributedTestCase
{

  VM server1 = null;

  VM server2 = null;

  VM client1 = null;

  VM client2 = null;

  public static int PORT1;

  public static int PORT2;

  private static final String REGION_NAME = "HABugInPutDUnitTest_region";

  final static String KEY1 = "KEY1";

  final static String VALUE1 = "VALUE1";

  protected static Cache cache = null;

  public HABugInPutDUnitTest(String name) {
    super(name);
  }

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

    //System.setProperty())
    PORT1 = ((Integer)server1.invoke(HABugInPutDUnitTest.class, "createServerCache"))
        .intValue();
    PORT2 = ((Integer)server2.invoke(HABugInPutDUnitTest.class, "createServerCache"))
        .intValue();

    client1.invoke(HABugInPutDUnitTest.class, "createClientCache", new Object[] {
        NetworkUtils.getServerHostName(host), new Integer(PORT1), new Integer(PORT2) });
    client2.invoke(HABugInPutDUnitTest.class, "createClientCache", new Object[] {
        NetworkUtils.getServerHostName(host), new Integer(PORT1), new Integer(PORT2) });
    //Boolean.getBoolean("")

  }

  @Override
  protected final void preTearDown() throws Exception {
    client1.invoke(HABugInPutDUnitTest.class, "closeCache");
    client2.invoke(HABugInPutDUnitTest.class, "closeCache");
    // close server
    server1.invoke(HABugInPutDUnitTest.class, "closeCache");
    server2.invoke(HABugInPutDUnitTest.class, "closeCache");
  }

  public static void closeCache()
  {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
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

  public static Integer createServerCache() throws Exception
  {
    new HABugInPutDUnitTest("temp").createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);
    CacheServerImpl server = (CacheServerImpl)cache.addCacheServer();
    assertNotNull(server);
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.setPort(port);
    server.setNotifyBySubscription(false);
    server.start();
    return new Integer(server.getPort());
  }

  public static void createClientCache(String hostName, Integer port1, Integer port2)
      throws Exception
  {
    PORT1 = port1.intValue();
    PORT2 = port2.intValue();
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "");
    new HABugInPutDUnitTest("temp").createCache(props);
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    ClientServerTestCase.configureConnectionPool(factory, hostName, new int[] {PORT1,PORT2}, true, -1, 2, null);
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);
    Region region = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    assertNotNull(region);
    region.registerInterest(KEY1);

  }

  public void testBugInPut() throws Exception
  {
    client1.invoke(new CacheSerializableRunnable("putFromClient1") {

      public void run2() throws CacheException
      {
        Region region = cache.getRegion(Region.SEPARATOR + REGION_NAME);
        assertNotNull(region);
        region.put(KEY1, VALUE1);
        cache.getLogger().info("Put done successfully");

      }
    });
  }
}
