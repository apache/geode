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
package com.gemstone.gemfire.security;

import static org.junit.Assert.*;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;

import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(DistributedTest.class)
public class IntegratedClientContainsKeyAuthDistributedTest extends AbstractIntegratedClientAuthDistributedTest {

  @Test
  public void testContainsKey() throws InterruptedException {
//    AsyncInvocation ai1 = client1.invokeAsync("containsKey with permission", () -> {
//      ClientCache cache = new ClientCacheFactory(createClientProperties("dataReader", "1234567"))
//          .setPoolSubscriptionEnabled(true)
//          .addPoolServer("localhost", serverPort)
//          .create();
//
    AsyncInvocation ai1 = client1.invokeAsync(() -> {
      Cache cache = SecurityTestUtils.createCacheClient("key1User", "1234567", serverPort, SecurityTestUtils.NO_EXCEPTION);
      final Region region = cache.getRegion(SecurityTestUtils.REGION_NAME);
      assertTrue(region.containsKeyOnServer("key1"));
      assertNotAuthorized(() -> region.containsKeyOnServer("key3"), "DATA:READ:AuthRegion:key3");
    });

    AsyncInvocation ai2 = client2.invokeAsync(() -> {
      Cache cache = SecurityTestUtils.createCacheClient("authRegionReader", "1234567", serverPort, SecurityTestUtils.NO_EXCEPTION);
      final Region region = cache.getRegion(SecurityTestUtils.REGION_NAME);
      region.containsKeyOnServer("key3");
      assertTrue(region.containsKeyOnServer("key1"));

    });

//    AsyncInvocation ai2 = client2.invokeAsync("containsKey without permission", () -> {
//      ClientCache cache = new ClientCacheFactory(createClientProperties("authRegionReader", "1234567"))
//        .setPoolSubscriptionEnabled(true)
//        .addPoolServer("localhost", serverPort)
//        .create();
//
//      Region region = cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(REGION_NAME);
//      assertNotAuthorized(() -> region.containsKeyOnServer("key3"), "DATA:READ");

//    });

    ai1.join();
    ai2.join();
    ai1.checkException();
    ai2.checkException();
  }

}
