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

import static com.gemstone.gemfire.internal.Assert.assertTrue;
import static org.jgroups.util.Util.*;

import java.util.Arrays;
import java.util.Map;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;
import com.gemstone.gemfire.test.junit.categories.SecurityTest;

@Category({ DistributedTest.class, SecurityTest.class })
public class IntegratedClientGetAllAuthDistributedTest extends AbstractSecureServerDUnitTest {

  @Test
  public void testGetAll() {
    client1.invoke("logging in Stranger", () -> {
      ClientCache cache = createClientCache("stranger", "1234567", serverPort);

      Region region = cache.getRegion(REGION_NAME);
      Map emptyMap = region.getAll(Arrays.asList("key1", "key2", "key3", "key4"));
      assertTrue(emptyMap.isEmpty());
    });

    client2.invoke("logging in authRegionReader", () -> {
      ClientCache cache = createClientCache("authRegionReader", "1234567", serverPort);

      Region region = cache.getRegion(REGION_NAME);
      Map filledMap = region.getAll(Arrays.asList("key1", "key2", "key3", "key4"));
      assertEquals("Map should contain 4 entries", 4, filledMap.size());
      assertTrue(filledMap.containsKey("key1"));
    });
  }
}


