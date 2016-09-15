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
package org.apache.geode.internal.cache.tier.sockets;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.junit.categories.DistributedTest;

/**
 * Test reconnecting a durable client that is using
 * the locator to discover its servers
 *
 * @since GemFire 5.7
 */
@Category(DistributedTest.class)
public class DurableClientReconnectAutoDUnitTest extends DurableClientReconnectDUnitTest {

  @BeforeClass
  public static void caseSetUp() throws Exception {
    disconnectAllFromDS();
  }
 
  @Ignore("do nothing, this test doesn't make sense with the locator")
  @Override
  @Test
  public void testDurableReconnectSingleServerWithZeroConnPerServer() {
    //do nothing, this test doesn't make sense with the locator
  }

  @Ignore("do nothing, this test doesn't make sense with the locator")
  @Override
  @Test
  public void testDurableReconnectSingleServer() throws Exception {
    //do nothing, this test doesn't make sense with the locator
  }
  
  protected PoolFactory getPoolFactory() {
    Host host = Host.getHost(0);
    PoolFactory factory = PoolManager.createFactory()
    .addLocator(NetworkUtils.getServerHostName(host), DistributedTestUtils.getDUnitLocatorPort());
    return factory;
  }

}
