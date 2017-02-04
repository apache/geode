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

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.query.CqAttributes;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqEvent;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.query.CqResults;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.internal.cq.CqListenerImpl;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.pdx.SimpleClass;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

@Category({DistributedTest.class, SecurityTest.class})
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class CQPDXPostProcessorDUnitTest extends AbstractSecureServerDUnitTest {
  private static byte[] BYTES = {1, 0};
  private static int jmxPort = AvailablePortHelper.getRandomAvailableTCPPort();

  @Parameterized.Parameters
  public static Collection<Object[]> parameters() {
    Object[][] params = {{true}, {false}};
    return Arrays.asList(params);
  }

  public Properties getProperties() {
    Properties properties = super.getProperties();
    properties.setProperty(SECURITY_POST_PROCESSOR, PDXPostProcessor.class.getName());
    properties.setProperty("security-pdx", pdxPersistent + "");
    properties.setProperty(JMX_MANAGER_PORT, jmxPort + "");
    return properties;
  }

  public Map<String, String> getData() {
    return new HashMap();
  }

  public CQPDXPostProcessorDUnitTest(boolean pdxPersistent) {
    this.pdxPersistent = pdxPersistent;
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
          if (key.equals("key1")) {
            assertTrue(value instanceof SimpleClass);
          } else if (key.equals("key2")) {
            assertTrue(Arrays.equals(BYTES, (byte[]) value));
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
    PDXPostProcessor pp =
        (PDXPostProcessor) SecurityService.getSecurityService().getPostProcessor();
    assertEquals(pp.getCount(), 2);
  }

}
