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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;
import com.gemstone.gemfire.test.junit.categories.FlakyTest;
import com.gemstone.gemfire.test.junit.categories.SecurityTest;

@Category({ DistributedTest.class, SecurityTest.class, FlakyTest.class })
public class IntegratedClientGetPutAuthDistributedTest extends AbstractSecureServerDUnitTest {

  @Test
  public void testGetPutAuthorization() throws InterruptedException {
    Map<String, String> allValues = new HashMap<String, String>();
    allValues.put("key1", "value1");
    allValues.put("key2", "value2");

    List<String> keys = new ArrayList<>();
    keys.add("key1");
    keys.add("key2");

    // client1 connects to server as a user not authorized to do any operations
    AsyncInvocation ai1 =  client1.invokeAsync(()->{
      ClientCache cache = createClientCache("stranger", "1234567", serverPort);
      Region region = cache.getRegion(REGION_NAME);

      assertNotAuthorized(() -> region.put("key3", "value3"), "DATA:WRITE:AuthRegion:key3");
      assertNotAuthorized(() -> region.get("key3"), "DATA:READ:AuthRegion:key3");

      //putall
      assertNotAuthorized(() -> region.putAll(allValues), "DATA:WRITE:AuthRegion");

      // not authorized for either keys, get no record back
      Map keyValues = region.getAll(keys);
      assertEquals(0, keyValues.size());

      assertNotAuthorized(() -> region.keySetOnServer(), "DATA:READ:AuthRegion");
    });


    // client2 connects to user as a user authorized to use AuthRegion region
    AsyncInvocation ai2 =  client2.invokeAsync(()->{
      ClientCache cache = createClientCache("authRegionUser", "1234567", serverPort);
      Region region = cache.getRegion(REGION_NAME);

      region.put("key3", "value3");
      assertEquals("value3", region.get("key3"));

      // put all
      region.putAll(allValues);

      // get all
      Map keyValues = region.getAll(keys);
      assertEquals(2, keyValues.size());

      // keyset
      Set keySet = region.keySetOnServer();
      assertEquals(5, keySet.size());
    });

    // client3 connects to user as a user authorized to use key1 in AuthRegion region
    AsyncInvocation ai3 =  client3.invokeAsync(()->{
      ClientCache cache = createClientCache("key1User", "1234567", serverPort);
      Region region = cache.getRegion(REGION_NAME);

      assertNotAuthorized(() -> region.put("key2", "value1"), "DATA:WRITE:AuthRegion:key2");
      assertNotAuthorized(() -> region.get("key2"), "DATA:READ:AuthRegion:key2");

      assertNotAuthorized(() -> region.putAll(allValues), "DATA:WRITE:AuthRegion");

      // only authorized for one recrod
      Map keyValues = region.getAll(keys);
      assertEquals(1, keyValues.size());

      // keyset
      assertNotAuthorized(() -> region.keySetOnServer(), "DATA:READ:AuthRegion");
    });

    ai1.join();
    ai2.join();
    ai3.join();

    ai1.checkException();
    ai2.checkException();
    ai3.checkException();
  }

}
