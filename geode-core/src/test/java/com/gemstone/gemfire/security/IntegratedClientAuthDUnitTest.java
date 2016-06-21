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

import static com.gemstone.gemfire.cache.query.CacheUtils.*;
import static org.assertj.core.api.Assertions.*;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.management.internal.security.JSONAuthorization;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.internal.JUnit4DistributedTestCase;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class IntegratedClientAuthDUnitTest extends JUnit4DistributedTestCase {

  private VM server1 = null;
  private VM client1 = null;
  private VM client2 = null;
  private VM client3 = null;
  private int serverPort;

  @Before
  public void before(){
    final Host host = Host.getHost(0);
    server1 = host.getVM(0);
    client1 = host.getVM(1);
    client2 = host.getVM(2);
    client3 = host.getVM(3);
    serverPort = server1.invoke(() -> {
      JSONAuthorization.setUpWithJsonFile("clientServer.json");
      return SecurityTestUtils.createCacheServer(JSONAuthorization.class.getName()+".create");
    });
  }

  @Test
  public void testAuthentication(){
    int port = serverPort;
    client1.invoke("logging in super-user with correct password", () -> {
      SecurityTestUtils.createCacheClient("super-user", "1234567", port, SecurityTestUtils.NO_EXCEPTION);
    });

    client2.invoke("logging in super-user with wrong password", () -> {
      SecurityTestUtils.createCacheClient("super-user", "wrong", port, SecurityTestUtils.AUTHFAIL_EXCEPTION);
    });
  }

  @Test
  public void testGetPutAuthorization() throws InterruptedException {
    int port = serverPort;
    Map<String, String> allValues = new HashMap<String, String>();
    allValues.put("key1", "value1");
    allValues.put("key2", "value2");

    List<String> keys = new ArrayList<>();
    keys.add("key1");
    keys.add("key2");

    // have one client log in as authorized user to put some data in the regions first.
    client2.invoke(()->{
      Cache cache = SecurityTestUtils.createCacheClient("authRegionUser", "1234567", port, SecurityTestUtils.NO_EXCEPTION);
      final Region region = cache.getRegion(SecurityTestUtils.REGION_NAME);
      region.putAll(allValues);
      cache.close();
    });

    // client1 connects to server as a user not authorized to do any operations
    AsyncInvocation ai1 =  client1.invokeAsync(()->{
      Cache cache = SecurityTestUtils.createCacheClient("stranger", "1234567", port, SecurityTestUtils.NO_EXCEPTION);
      final Region region = cache.getRegion(SecurityTestUtils.REGION_NAME);

      assertNotAuthorized(()->region.put("key3", "value3"), "DATA:WRITE:AuthRegion:key3");
      assertNotAuthorized(()->region.get("key3"), "DATA:READ:AuthRegion:key3");

      //putall
      assertNotAuthorized(()->region.putAll(allValues), "DATA:WRITE:AuthRegion");

      // not authorized for either keys, get no record back
      Map keyValues =  region.getAll(keys);
      assertEquals(0, keyValues.size());

      Set keySet = region.keySet();
      assertEquals(0, keySet.size());

//      Query query = cache.getQueryService().newQuery("select * from /AuthRegion");
//      Object result = query.execute();

      cache.close();
    });


    // client2 connects to user as a user authorized to use AuthRegion region
    AsyncInvocation ai2 =  client2.invokeAsync(()->{
      Cache cache = SecurityTestUtils.createCacheClient("authRegionUser", "1234567", port, SecurityTestUtils.NO_EXCEPTION);
      final Region region = cache.getRegion(SecurityTestUtils.REGION_NAME);

      region.put("key3", "value3");
      assertEquals("value3", region.get("key3"));

      // put all
      region.putAll(allValues);

      // get all
      Map keyValues =  region.getAll(keys);
      assertEquals(2, keyValues.size());

      // keyset
      Set keySet = region.keySet();
      assertEquals(3, keySet.size());

      cache.close();
    });

    // client3 connects to user as a user authorized to use key1 in AuthRegion region
    AsyncInvocation ai3 =  client3.invokeAsync(()->{
      Cache cache = SecurityTestUtils.createCacheClient("key1User", "1234567", port, SecurityTestUtils.NO_EXCEPTION);
      final Region region = cache.getRegion(SecurityTestUtils.REGION_NAME);

      assertNotAuthorized(()->region.put("key2", "value1"), "DATA:WRITE:AuthRegion:key2");
      assertNotAuthorized(()->region.get("key2"), "DATA:READ:AuthRegion:key2");

      assertNotAuthorized(()->region.putAll(allValues), "DATA:WRITE:AuthRegion");

      // only authorized for one recrod
      Map keyValues =  region.getAll(keys);
      assertEquals(1, keyValues.size());

      // keyset
      Set keySet = region.keySet();
      assertEquals(1, keySet.size());

      cache.close();
    });

    ai1.join();
    ai2.join();
    ai3.join();

    ai1.checkException();
    ai2.checkException();
    ai3.checkException();
  }

  @Test
  public void testDestroyInvalidate() throws InterruptedException {

    // First, load up 5 keys to work with
    SerializableRunnable putKeys = new SerializableRunnable() {
      @Override
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(SecurityTestUtils.REGION_NAME);
        assertNotNull(region);
        region.clear();
        for (int i = 0; i < 5; i++) {
          String key = "key" + i;
          String value = "value" + i;
          region.put(key, value);
        }
        assertEquals(5, region.size());
      }
    };
    server1.invoke(putKeys);

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
      cache.close();
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

  public static void assertNotAuthorized(ThrowingCallable shouldRaiseThrowable, String permString){
    assertThatThrownBy(shouldRaiseThrowable).hasMessageContaining(permString);
  }

}
