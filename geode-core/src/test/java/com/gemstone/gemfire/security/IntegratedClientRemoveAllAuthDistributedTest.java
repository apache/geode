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

import static org.junit.Assert.assertFalse;

import java.util.Arrays;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class IntegratedClientRemoveAllAuthDistributedTest extends AbstractIntegratedClientAuthDistributedTest {

  @Test
  public void testRemoveAll() throws InterruptedException {

    AsyncInvocation ai1 = client1.invokeAsync(() -> {
      Cache cache = SecurityTestUtils.createCacheClient("dataReader", "1234567", serverPort, SecurityTestUtils.NO_EXCEPTION);
      final Region region = cache.getRegion(SecurityTestUtils.REGION_NAME);
      assertNotAuthorized(() -> region.removeAll(Arrays.asList("key1", "key2", "key3", "key4")), "DATA:WRITE");
    });

    AsyncInvocation ai2 = client2.invokeAsync(() -> {
      Cache cache = SecurityTestUtils.createCacheClient("dataUser", "1234567", serverPort, SecurityTestUtils.NO_EXCEPTION);
      final Region region = cache.getRegion(SecurityTestUtils.REGION_NAME);
      region.removeAll(Arrays.asList("key1", "key2", "key3", "key4"));
      assertFalse(region.containsKey("key1"));
      assertFalse(region.containsKeyOnServer("key1"));
    });
    ai1.join();
    ai2.join();
    ai1.checkException();
    ai2.checkException();
  }

}
