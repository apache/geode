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
public class IntegratedClientDestroyInvalidateAuthDistributedTest extends AbstractIntegratedClientAuthDistributedTest {

  @Test
  public void testDestroyInvalidate() throws InterruptedException {

    // Delete one key and invalidate another key with an authorized user.
    AsyncInvocation ai1 = client1.invokeAsync(() -> {
      Cache cache = SecurityTestUtils.createCacheClient("authRegionUser", "1234567", serverPort, SecurityTestUtils.NO_EXCEPTION);
      final Region region = cache.getRegion(SecurityTestUtils.REGION_NAME);

      assertEquals(region.get("key1"), "value1");
      assertTrue(region.containsKey("key1")); // will only be true after we first get it, then it's cached locally

      // Destroy key1
      region.destroy("key1");
      assertFalse(region.containsKey("key1"));

      // Invalidate key2
      assertNotNull("Value of key2 should not be null", region.get("key2"));
      region.invalidate("key2");
      assertNull("Value of key2 should have been null", region.get("key2"));

    });

    // Delete one key and invalidate another key with an unauthorized user.
    AsyncInvocation ai2 = client2.invokeAsync(() -> {
      Cache cache = SecurityTestUtils.createCacheClient("authRegionReader", "1234567", serverPort, SecurityTestUtils.NO_EXCEPTION);
      final Region region = cache.getRegion(SecurityTestUtils.REGION_NAME);

      assertEquals(region.get("key3"), "value3");
      assertTrue(region.containsKey("key3")); // will only be true after we first get it, then it's cached locally

      // Destroy key1
      assertNotAuthorized(() -> region.destroy("key3"), "DATA:WRITE:AuthRegion");
      assertTrue(region.containsKey("key3"));

      // Invalidate key2
      assertNotNull("Value of key4 should not be null", region.get("key4"));
      assertNotAuthorized(() -> region.invalidate("key4"), "DATA:WRITE:AuthRegion");
      assertNotNull("Value of key4 should not be null", region.get("key4"));
      cache.close();
    });
    ai1.join();
    ai2.join();
    ai1.checkException();
    ai2.checkException();
  }

}
