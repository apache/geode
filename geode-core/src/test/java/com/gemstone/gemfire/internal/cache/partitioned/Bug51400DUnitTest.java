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
/**
 * 
 */
package com.gemstone.gemfire.internal.cache.partitioned;

import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.*;
import static org.junit.Assert.*;

import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientNotifier;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientProxy;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientProxyStats;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.DistributedTestUtils;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.NetworkUtils;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.internal.JUnit4DistributedTestCase;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class Bug51400DUnitTest extends JUnit4DistributedTestCase {

  private static VM server0 = null;
  private static VM server1 = null;
  private static VM client0 = null;
  private static VM client1 = null;

  private static GemFireCacheImpl cache;

  public static final String REGION_NAME = "Bug51400DUnitTest_region";

  @Override
  public final void postSetUp() throws Exception {
    Host host = Host.getHost(0);
    server0 = host.getVM(0);
    server1 = host.getVM(1);
    client0 = host.getVM(2);
    client1 = host.getVM(3);
  }

  @Override
  public final void preTearDown() throws Exception {
    closeCache();

    client0.invoke(() -> Bug51400DUnitTest.closeCache());
    client1.invoke(() -> Bug51400DUnitTest.closeCache());

    server0.invoke(() -> Bug51400DUnitTest.closeCache());
    server1.invoke(() -> Bug51400DUnitTest.closeCache());
  }

  public static void closeCache() throws Exception {
    if (cache != null) {
      cache.close();
    }
  }

  public static Integer createServerCache(Integer mcastPort,
      Integer maxMessageCount) throws Exception {
    Properties props = new Properties();
    props.setProperty(LOCATORS, "localhost[" + DistributedTestUtils.getDUnitLocatorPort() + "]");

    Bug51400DUnitTest test = new Bug51400DUnitTest();
    DistributedSystem ds = test.getSystem(props);
    ds.disconnect();
    cache = (GemFireCacheImpl)CacheFactory.create(test.getSystem());
//    cache = (GemFireCacheImpl) new CacheFactory(props).create();

    RegionFactory<String, String> rf = cache
        .createRegionFactory(RegionShortcut.REPLICATE);

    rf.setConcurrencyChecksEnabled(false);

    Region<String, String> region = rf.create(REGION_NAME);

    CacheServer server = cache.addCacheServer();
    server.setMaximumMessageCount(maxMessageCount);
    server.setPort(AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET));
    server.start();
    return server.getPort();
  }

  public static void createClientCache(String hostName, Integer[] ports,
      Integer interval) throws Exception {
    Properties props = new Properties();

    DistributedSystem ds = new Bug51400DUnitTest().getSystem(props);
    ds.disconnect();
    ClientCacheFactory ccf = new ClientCacheFactory(props);
    ccf.setPoolSubscriptionEnabled(true);
    ccf.setPoolSubscriptionAckInterval(interval);
    for (int port : ports) {
      ccf.addPoolServer(hostName, port);
    }
    cache = (GemFireCacheImpl) ccf.create();

    ClientRegionFactory<String, String> crf = cache
        .createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY);

    Region<String, String> region = crf.create(REGION_NAME);

    region.registerInterest("ALL_KEYS");
  }

  public static void verifyQueueSize(Boolean isPrimary,
      Integer numOfEvents) throws Exception {
    CacheClientProxyStats stats = ((CacheClientProxy) CacheClientNotifier
        .getInstance().getClientProxies().toArray()[0]).getStatistics();

    if (isPrimary) {
      numOfEvents = numOfEvents + 1; // marker
    }
    long qSize = stats.getMessageQueueSize();
    assertEquals("Expected queue size: " + numOfEvents
        + " but actual size: " + qSize + " at "
        + (isPrimary ? "primary." : "secondary."), numOfEvents.intValue(), qSize);
  }

  @Test
  public void testNothing() {
    // remove when ticket #51932 is fixed
  }
  public void ticket51932_testDeadlock() throws Throwable {
    int maxQSize = 5;
    // Set infinite ack interval so that the queue will not be drained.
    int ackInterval = Integer.MAX_VALUE;

    fail("Invoking bad method");
    int port1 = 0;
//    int port1 = (Integer) server0.invoke(() -> Bug51400DUnitTest.createServerCache( maxQSize));

    client1.invoke(Bug51400DUnitTest.class, "createClientCache",
        new Object[] { NetworkUtils.getServerHostName(Host.getHost(0)), new Integer[]{port1}, ackInterval});

    // Do puts from server as well as from client on the same key.
    AsyncInvocation ai1 = server0.invokeAsync(() -> Bug51400DUnitTest.updateKey( 2 * maxQSize ));
    AsyncInvocation ai2 = client1.invokeAsync(() -> Bug51400DUnitTest.updateKey( 2 * maxQSize ));
    ai1.getResult();
    ai2.getResult();
    // Verify that the queue has crossed its limit of maxQSize
    server0.invoke(() -> Bug51400DUnitTest.verifyQueueSize(
        true, 2 * maxQSize ));
  }

  public static void updateKey(Integer num) {
    try {
      String k = "51400_KEY";
      Region r = cache.getRegion(REGION_NAME);
      for (int i = 0; i < num; ++i) {
        r.put(k, "VALUE_" + i);
      }
    }
    catch (Exception e) {
      fail("Failed in updateKey()" + e);
    }
  }

}
