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

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category({ DistributedTest.class, SecurityTest.class })
public class IntegratedClientRegionClearAuthDistributedTest extends AbstractSecureServerDUnitTest {

  @Test
  public void testRegionClear() throws InterruptedException {
    // Verify that an unauthorized user can't clear the region
    SerializableRunnable clearUnauthorized = new SerializableRunnable() {
      @Override
      public void run() {
        ClientCache cache = new ClientCacheFactory(createClientProperties("stranger", "1234567")).setPoolSubscriptionEnabled(true)
                                                                                                 .addPoolServer("localhost", serverPort)
                                                                                                 .create();

        Region region = cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(REGION_NAME);
        assertNotAuthorized(() -> region.clear(), "DATA:WRITE:AuthRegion");
      }
    };
    client1.invoke(clearUnauthorized);

    // Verify that an authorized user can clear the region
    SerializableRunnable clearAuthorized = new SerializableRunnable() {
      @Override
      public void run() {
        ClientCache cache = new ClientCacheFactory(createClientProperties("authRegionUser", "1234567")).setPoolSubscriptionEnabled(true)
                                                                                                       .addPoolServer("localhost", serverPort)
                                                                                                       .create();

        Region region = cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(REGION_NAME);
        region.clear();
      }
    };
    client2.invoke(clearAuthorized);
  }
}
