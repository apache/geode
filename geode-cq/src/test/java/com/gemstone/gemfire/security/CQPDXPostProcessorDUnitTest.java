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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import com.jayway.awaitility.Awaitility;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.query.CqAttributes;
import com.gemstone.gemfire.cache.query.CqAttributesFactory;
import com.gemstone.gemfire.cache.query.CqEvent;
import com.gemstone.gemfire.cache.query.CqQuery;
import com.gemstone.gemfire.cache.query.CqResults;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.internal.cq.CqListenerImpl;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.security.GeodeSecurityUtil;
import com.gemstone.gemfire.pdx.SimpleClass;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;
import com.gemstone.gemfire.test.junit.categories.SecurityTest;
import com.gemstone.gemfire.test.junit.runners.CategoryWithParameterizedRunnerFactory;

@Category({ DistributedTest.class, SecurityTest.class })
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class CQPDXPostProcessorDUnitTest extends AbstractSecureServerDUnitTest {
  private static byte[] BYTES = {1,0};

  @Parameterized.Parameters
  public static Collection<Object[]> parameters(){
    Object[][] params = {{true}, {false}};
    return Arrays.asList(params);
  }

  public CQPDXPostProcessorDUnitTest(boolean pdxPersistent){
    this.postProcessor = PDXPostProcessor.class;
    this.pdxPersistent = pdxPersistent;
    this.jmxPort = AvailablePortHelper.getRandomAvailableTCPPort();
    values = new HashMap();
  }

  @Test
  public void testCQ() {
    String query = "select * from /AuthRegion";
    client1.invoke(() -> {
      ClientCache cache = createClientCache("super-user", "1234567", serverPort);
      Region region = cache.getRegion(REGION_NAME);

      Pool pool = PoolManager.find(region);
      QueryService qs = pool.getQueryService();

      CqAttributesFactory factory = new CqAttributesFactory();

      factory.addCqListener(new CqListenerImpl() {
        @Override
        public void onEvent(final CqEvent aCqEvent) {
          Object key = aCqEvent.getKey();
          Object value = aCqEvent.getNewValue();
          if(key.equals("key1")) {
            assertTrue(value instanceof SimpleClass);
          }
          else if(key.equals("key2")){
            assertTrue(Arrays.equals(BYTES, (byte[])value));
          }
        }
      });

      CqAttributes cqa = factory.create();

      // Create the CqQuery
      CqQuery cq = qs.newCq("CQ1", query, cqa);
      CqResults results = cq.executeWithInitialResults();
    });

    client2.invoke(() -> {
      ClientCache cache = createClientCache("authRegionUser", "1234567", serverPort);
      Region region = cache.getRegion(REGION_NAME);
      region.put("key1", new SimpleClass(1, (byte) 1));
      region.put("key2", BYTES);
    });

    // wait for events to fire
    Awaitility.await().atMost(1, TimeUnit.SECONDS);
    PDXPostProcessor pp = (PDXPostProcessor) GeodeSecurityUtil.getPostProcessor();
    assertEquals(pp.getCount(), 2);
  }

}
