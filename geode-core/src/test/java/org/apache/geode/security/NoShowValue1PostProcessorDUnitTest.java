/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.security;

import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_POST_PROCESSOR;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category({DistributedTest.class, SecurityTest.class})
public class NoShowValue1PostProcessorDUnitTest extends AbstractSecureServerDUnitTest {

  public Properties getProperties() {
    Properties properties = super.getProperties();
    properties.setProperty(SECURITY_POST_PROCESSOR, NoShowValue1PostProcessor.class.getName());
    return properties;
  }

  @Test
  public void testPostProcess() {
    List<String> keys = new ArrayList<>();
    keys.add("key1");
    keys.add("key2");

    client1.invoke(() -> {
      ClientCache cache = createClientCache("super-user", "1234567", serverPort);
      Region region = cache.getRegion(REGION_NAME);

      // post process for get
      assertEquals("value3", region.get("key3"));

      assertNull(region.get("key1"));

      // post processs for getAll
      Map values = region.getAll(keys);
      assertEquals(2, values.size());
      assertEquals("value2", values.get("key2"));
      assertNull(values.get("key1"));

      // post process for query
      String query = "select * from /AuthRegion";
      SelectResults result = region.query(query);
      System.out.println("query result: " + result);
      assertEquals(5, result.size());
      assertTrue(result.contains("value0"));
      assertFalse(result.contains("value1"));
      assertTrue(result.contains("value2"));
      assertTrue(result.contains("value3"));
      assertTrue(result.contains("value4"));

      Pool pool = PoolManager.find(region);
      result = (SelectResults) pool.getQueryService().newQuery(query).execute();
      System.out.println("query result: " + result);
      assertTrue(result.contains("value0"));
      assertFalse(result.contains("value1"));
      assertTrue(result.contains("value2"));
      assertTrue(result.contains("value3"));
      assertTrue(result.contains("value4"));
    });
  }

}
