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

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;
import com.gemstone.gemfire.test.junit.categories.SecurityTest;

@Category({ DistributedTest.class, SecurityTest.class })
public class IntegratedClientDestroyInvalidateAuthDistributedTest extends AbstractSecureServerDUnitTest {

  @Test
  public void testDestroyInvalidate() throws InterruptedException {

    // Delete one key and invalidate another key with an authorized user.
    AsyncInvocation ai1 = client1.invokeAsync(() -> {
      ClientCache cache = new ClientCacheFactory(createClientProperties("dataUser", "1234567")).setPoolSubscriptionEnabled(true)
                                                                                               .addPoolServer("localhost", serverPort)
                                                                                               .create();

      Region region = cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(REGION_NAME);
      assertTrue(region.containsKeyOnServer("key1"));

      // Destroy key1
      region.destroy("key1");
      assertFalse(region.containsKeyOnServer("key1"));

      // Invalidate key2
      assertNotNull("Value of key2 should not be null", region.get("key2"));
      region.invalidate("key2");
      assertNull("Value of key2 should have been null", region.get("key2"));

    });

    // Delete one key and invalidate another key with an unauthorized user.
    AsyncInvocation ai2 = client2.invokeAsync(() -> {
      ClientCache cache = new ClientCacheFactory(createClientProperties("authRegionReader", "1234567")).setPoolSubscriptionEnabled(true)
                                                                                                       .addPoolServer("localhost", serverPort)
                                                                                                       .create();

      Region region = cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(REGION_NAME);

      assertTrue(region.containsKeyOnServer("key3"));

      // Destroy key1
      assertNotAuthorized(() -> region.destroy("key3"), "DATA:WRITE:AuthRegion");
      assertTrue(region.containsKeyOnServer("key3"));

      // Invalidate key2
      assertNotNull("Value of key4 should not be null", region.get("key4"));
      assertNotAuthorized(() -> region.invalidate("key4"), "DATA:WRITE:AuthRegion");
      assertNotNull("Value of key4 should not be null", region.get("key4"));
    });

    ai1.join();
    ai2.join();
    ai1.checkException();
    ai2.checkException();
  }

}
