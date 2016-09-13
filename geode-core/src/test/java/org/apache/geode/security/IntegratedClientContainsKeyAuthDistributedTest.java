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
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;
import com.gemstone.gemfire.test.junit.categories.SecurityTest;

@Category({ DistributedTest.class, SecurityTest.class })
public class IntegratedClientContainsKeyAuthDistributedTest extends AbstractSecureServerDUnitTest {

  @Test
  public void testContainsKey() throws InterruptedException {
    AsyncInvocation ai1 = client1.invokeAsync(() -> {
      ClientCache cache = createClientCache("key1User", "1234567", serverPort);
      final Region region = cache.getRegion(REGION_NAME);
      assertTrue(region.containsKeyOnServer("key1"));
      assertNotAuthorized(() -> region.containsKeyOnServer("key3"), "DATA:READ:AuthRegion:key3");
    });

    AsyncInvocation ai2 = client2.invokeAsync(() -> {
      ClientCache cache = createClientCache("authRegionReader", "1234567", serverPort);
      final Region region = cache.getRegion(REGION_NAME);
      region.containsKeyOnServer("key3");
      assertTrue(region.containsKeyOnServer("key1"));
    });

    ai1.join();
    ai2.join();
    ai1.checkException();
    ai2.checkException();
  }

}
