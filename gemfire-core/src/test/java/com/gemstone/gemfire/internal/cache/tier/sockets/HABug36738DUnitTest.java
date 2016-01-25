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

import java.util.Iterator;
import java.util.Properties;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.MirrorType;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.cache.EnumListenerEvent;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.HARegion;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.ha.HAHelper;
import com.gemstone.gemfire.internal.cache.ha.HARegionQueue;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientUpdateMessage;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * This is the bugtest for bug no. 36738. When Object of class
 * ClientUpdateMessage gets deserialized it thows NPE if region mentioned in the
 * ClientUpdateMessage is not present on the node. The test performs following
 * operations 
 * 1. Create server1 and HARegion. 
 * 2. Perform put operations on HARegion with the value as ClientUpdateMessage. 
 * 3. Create server2 and HARegion in it so that GII will happen. 
 * 4. Perform get operations from server2.
 * 
 */

public class HABug36738DUnitTest extends DistributedTestCase
{

  static VM server1 = null;

  static VM server2 = null;

  private static int NO_OF_PUTS = 10;

  private static final String REGION_NAME = "HABug36738DUnitTest_Region";

  protected static Cache cache = null;

  protected static HARegionQueue messageQueue = null;

  static Region haRegion = null;

  final static String HAREGION_NAME = "haRegion";

  public HABug36738DUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception
  {
    super.setUp();
    final Host host = Host.getHost(0);
    server1 = host.getVM(0);
    server2 = host.getVM(1);

  }

  public void tearDown2() throws Exception
  {
    super.tearDown2();
    server1.invoke(HABug36738DUnitTest.class, "closeCache");
    server2.invoke(HABug36738DUnitTest.class, "closeCache");

  }

  public void testBug36768() throws Exception
  {
    createServer1();
    pause(10000);
    server1.invoke(HABug36738DUnitTest.class, "checkRegionQueueSize");
    createServer2();
    server1.invoke(HABug36738DUnitTest.class, "checkRegionQueueSize");
    server2.invoke(HABug36738DUnitTest.class, "checkRegionQueueSize");
    server2.invoke(HABug36738DUnitTest.class, "printRecs");
  }

  public static void printRecs()
  {
    HARegion region = (HARegion)cache.getRegion(Region.SEPARATOR
        + HAHelper.getRegionQueueName(HAREGION_NAME));
    assertNotNull(region);
    Iterator itr = region.keys().iterator();
    while (itr.hasNext()) {
      Object key = itr.next();
      ClientUpdateMessage value = (ClientUpdateMessage)region.get(key);
      getLogWriter().info("key : " + key + "Value " + value.getValue());

    }

  }

  // function to create server and region in it.
  private void createServer1() throws Exception
  {
    server1.invoke(HABug36738DUnitTest.class, "createServerCache",
        new Object[] { new Boolean(true) });
  }

  // function to create server without region.
  private void createServer2() throws Exception
  {
    server2.invoke(HABug36738DUnitTest.class, "createServerCache",
        new Object[] { new Boolean(false) });
  }

  public static void createServerCache(Boolean isRegion) throws Exception
  {
    new HABug36738DUnitTest("temp").createCache(new Properties());
    if (isRegion.booleanValue()) {
      AttributesFactory factory = new AttributesFactory();
      factory.setScope(Scope.DISTRIBUTED_ACK);
      factory.setEnableConflation(true);
      factory.setDataPolicy(DataPolicy.REPLICATE);
      RegionAttributes attrs = factory.createRegionAttributes();
      cache.createVMRegion(REGION_NAME, attrs);

    }

    AttributesFactory factoryForHARegion = new AttributesFactory();
    factoryForHARegion.setMirrorType(MirrorType.KEYS_VALUES);
    factoryForHARegion.setScope(Scope.DISTRIBUTED_ACK);
    RegionAttributes ra = factoryForHARegion.createRegionAttributes();
    haRegion = HARegion.getInstance(HAREGION_NAME, (GemFireCacheImpl)cache, null,
        ra);

    if (isRegion.booleanValue()) {
      for (int i = 0; i < NO_OF_PUTS; i++) {
        ClientUpdateMessage clientMessage = new ClientUpdateMessageImpl(
            EnumListenerEvent.AFTER_UPDATE, (LocalRegion)haRegion, null, ("value" + i)
                .getBytes(), (byte)0x01, null, new ClientProxyMembershipID(),
            new EventID(("memberID" + i).getBytes(), i, i));

        haRegion.put(new Long(i), clientMessage);
        getLogWriter().info("Putting in the message Queue");

      }
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

  public static void closeCache()
  {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }

  public static void checkRegionQueueSize()
  {
    HARegion region = (HARegion)cache.getRegion(Region.SEPARATOR
        + HAHelper.getRegionQueueName(HAREGION_NAME));
    assertNotNull(region);
    getLogWriter().info("Size of the Queue : " + region.size());

  }
}
