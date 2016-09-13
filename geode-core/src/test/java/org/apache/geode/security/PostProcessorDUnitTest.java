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

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.geode.security.templates.SamplePostProcessor;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category({ DistributedTest.class, SecurityTest.class })
public class PostProcessorDUnitTest extends AbstractSecureServerDUnitTest {

  public PostProcessorDUnitTest(){
    this.postProcessor = SamplePostProcessor.class;
  }

  @Test
  public void testPostProcessRegionGet(){
    List<String> keys = new ArrayList<>();
    keys.add("key1");
    keys.add("key2");

    client1.invoke(()->{
      ClientCache cache = createClientCache("super-user", "1234567", serverPort);
      Region region = cache.getRegion(REGION_NAME);

      // post process for get
      Object value = region.get("key3");
      assertEquals("super-user/AuthRegion/key3/value3", value);

      // post processs for getAll
      Map values = region.getAll(keys);
      assertEquals(2, values.size());
      assertEquals("super-user/AuthRegion/key1/value1", values.get("key1"));
      assertEquals("super-user/AuthRegion/key2/value2", values.get("key2"));
    });
  }

  @Test
  public void testPostProcessQuery(){
    client1.invoke(()->{
      ClientCache cache = createClientCache("super-user", "1234567", serverPort);
      Region region = cache.getRegion(REGION_NAME);

      // post process for query
      String query = "select * from /AuthRegion";
      SelectResults result = region.query(query);
      assertEquals(5, result.size());

      assertTrue(result.contains("super-user/null/null/value0"));
      assertTrue(result.contains("super-user/null/null/value1"));
      assertTrue(result.contains("super-user/null/null/value2"));
      assertTrue(result.contains("super-user/null/null/value3"));
      assertTrue(result.contains("super-user/null/null/value4"));

      Pool pool = PoolManager.find(region);
      result =  (SelectResults)pool.getQueryService().newQuery(query).execute();
      assertTrue(result.contains("super-user/null/null/value0"));
      assertTrue(result.contains("super-user/null/null/value1"));
      assertTrue(result.contains("super-user/null/null/value2"));
      assertTrue(result.contains("super-user/null/null/value3"));
      assertTrue(result.contains("super-user/null/null/value4"));
    });
  }

  @Test
  public void testRegisterInterestPostProcess(){
    client1.invoke(()->{
      ClientCache cache = new ClientCacheFactory(createClientProperties("super-user", "1234567"))
        .setPoolSubscriptionEnabled(true)
        .addPoolServer("localhost", serverPort)
        .create();

      ClientRegionFactory factory =  cache.createClientRegionFactory(ClientRegionShortcut.PROXY);
      factory.addCacheListener(new CacheListenerAdapter() {
        @Override
        public void afterUpdate(EntryEvent event) {
          assertEquals("super-user/AuthRegion/key1/value2", event.getSerializedNewValue().getDeserializedValue());
        }
      });

      Region region = factory.create(REGION_NAME);
      region.put("key1", "value1");
      region.registerInterest("key1");
    });

    client2.invoke(()->{
      ClientCache cache = createClientCache("dataUser", "1234567", serverPort);
      Region region = cache.getRegion(REGION_NAME);
      region.put("key1", "value2");
    });
  }

}
