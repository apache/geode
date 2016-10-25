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
package org.apache.geode.security;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category({ DistributedTest.class, SecurityTest.class })
public class IntegratedClientRegisterInterestAuthDistributedTest extends AbstractSecureServerDUnitTest {
  @Test
  public void testRegisterInterest() throws InterruptedException {
    // client1 connects to server as a user not authorized to do any operations
    AsyncInvocation ai1 = client1.invokeAsync(() -> {
      ClientCache cache = new ClientCacheFactory(createClientProperties("stranger", "1234567")).setPoolSubscriptionEnabled(true)
                                                                                               .addPoolServer("localhost", serverPort)
                                                                                               .create();

      Region region = cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(REGION_NAME);
      assertNotAuthorized(() -> region.registerInterest("key3"), "DATA:READ:AuthRegion:key3");
    });

    // client2 connects to user as a user authorized to use AuthRegion region
    AsyncInvocation ai2 = client2.invokeAsync(() -> {
      ClientCache cache = new ClientCacheFactory(createClientProperties("authRegionUser", "1234567")).setPoolSubscriptionEnabled(true)
                                                                                                     .addPoolServer("localhost", serverPort)
                                                                                                     .create();

      Region region = cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(REGION_NAME);
      region.registerInterest("key3");  //  DATA:READ:AuthRegion:key3;
    });

    // client3 connects to user as a user authorized to use key1 in AuthRegion region
    AsyncInvocation ai3 = client3.invokeAsync(() -> {
      ClientCache cache = new ClientCacheFactory(createClientProperties("key1User", "1234567")).setPoolSubscriptionEnabled(true)
                                                                                               .addPoolServer("localhost", serverPort)
                                                                                               .create();

      Region region = cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(REGION_NAME);
      assertNotAuthorized(() -> region.registerInterest("key2"), "DATA:READ:AuthRegion:key2");
    });

    ai1.join();
    ai2.join();
    ai3.join();

    ai1.checkException();
    ai2.checkException();
    ai3.checkException();
  }

  @Test
  public void testRegisterInterestRegex() throws InterruptedException {
    //client1 connects to server as a user not authorized to do any operations
    AsyncInvocation ai1 = client1.invokeAsync(() -> {
      ClientCache cache = new ClientCacheFactory(createClientProperties("stranger", "1234567")).setPoolSubscriptionEnabled(true)
                                                                                               .addPoolServer("localhost", serverPort)
                                                                                               .create();

      Region region = cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(REGION_NAME);
      assertNotAuthorized(() -> region.registerInterestRegex("key.*"), "DATA:READ:AuthRegion");
    });

    // client2 connects to user as a user authorized to use AuthRegion region
    AsyncInvocation ai2 = client2.invokeAsync(() -> {
      ClientCache cache = new ClientCacheFactory(createClientProperties("authRegionUser", "1234567")).setPoolSubscriptionEnabled(true)
                                                                                                     .addPoolServer("localhost", serverPort)
                                                                                                     .create();

      Region region = cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(REGION_NAME);
      region.registerInterestRegex("key[0-9]+");  //  DATA:READ:AuthRegion:key3;
    });

    // client3 connects to user as a user authorized to use key1 in AuthRegion region
    AsyncInvocation ai3 = client3.invokeAsync(() -> {
      ClientCache cache = new ClientCacheFactory(createClientProperties("key1User", "1234567")).setPoolSubscriptionEnabled(true)
                                                                                               .addPoolServer("localhost", serverPort)
                                                                                               .create();

      Region region = cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(REGION_NAME);
      assertNotAuthorized(() -> region.registerInterestRegex("key[0-9]+"), "DATA:READ:AuthRegion");
      assertNotAuthorized(() -> region.registerInterestRegex("key1"), "DATA:READ:AuthRegion");
    });

    ai1.join();
    ai2.join();
    ai3.join();

    ai1.checkException();
    ai2.checkException();
    ai3.checkException();
  }

  @Test
  public void testRegisterInterestList() throws InterruptedException {
    List<String> keys = new ArrayList<>();
    keys.add("key1");
    keys.add("key2");

    //client1 connects to server as a user not authorized to do any operations
    AsyncInvocation ai1 = client1.invokeAsync(() -> {
      ClientCache cache = new ClientCacheFactory(createClientProperties("stranger", "1234567")).setPoolSubscriptionEnabled(true)
                                                                                               .addPoolServer("localhost", serverPort)
                                                                                               .create();

      Region region = cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(REGION_NAME);
      assertNotAuthorized(() -> region.registerInterest(keys), "DATA:READ:AuthRegion");
    });

    // client2 connects to user as a user authorized to use AuthRegion region
    AsyncInvocation ai2 = client2.invokeAsync(() -> {
      ClientCache cache = new ClientCacheFactory(createClientProperties("authRegionUser", "1234567")).setPoolSubscriptionEnabled(true)
                                                                                                     .addPoolServer("localhost", serverPort)
                                                                                                     .create();

      Region region = cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(REGION_NAME);
      region.registerInterest(keys);  //  DATA:READ:AuthRegion;
    });

    // client3 connects to user as a user authorized to use key1 in AuthRegion region
    AsyncInvocation ai3 = client3.invokeAsync(() -> {
      ClientCache cache = new ClientCacheFactory(createClientProperties("key1User", "1234567")).setPoolSubscriptionEnabled(true)
                                                                                               .addPoolServer("localhost", serverPort)
                                                                                               .create();

      Region region = cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(REGION_NAME);
      assertNotAuthorized(() -> region.registerInterest(keys), "DATA:READ:AuthRegion");
    });

    ai1.join();
    ai2.join();
    ai3.join();

    ai1.checkException();
    ai2.checkException();
    ai3.checkException();
  }

}
