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

import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_POST_PROCESSOR;
import static org.apache.geode.security.SecurityTestUtil.createClientCache;
import static org.apache.geode.security.SecurityTestUtil.createProxyRegion;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
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
import org.apache.geode.pdx.SimpleClass;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.ServerStarterRule;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

@Category({SecurityTest.class})
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class CQPDXPostProcessorDUnitTest extends JUnit4DistributedTestCase {

  private static String REGION_NAME = "AuthRegion";
  final Host host = Host.getHost(0);
  final VM client1 = host.getVM(1);
  final VM client2 = host.getVM(2);

  private boolean pdxPersistent = false;
  private static byte[] BYTES = {1, 0};

  @Parameterized.Parameters
  public static Collection<Object[]> parameters() {
    Object[][] params = {{true}, {false}};
    return Arrays.asList(params);
  }

  @Rule
  public ServerStarterRule server =
      new ServerStarterRule().withProperty(SECURITY_MANAGER, TestSecurityManager.class.getName())
          .withProperty(TestSecurityManager.SECURITY_JSON,
              "org/apache/geode/management/internal/security/clientServer.json")
          .withProperty(SECURITY_POST_PROCESSOR, PDXPostProcessor.class.getName())
          .withProperty("security-pdx", pdxPersistent + "")
          .withRegion(RegionShortcut.REPLICATE, REGION_NAME);

  public CQPDXPostProcessorDUnitTest(boolean pdxPersistent) {
    this.pdxPersistent = pdxPersistent;
  }

  @Test
  public void testCQ() {
    String query = "select * from /" + REGION_NAME;
    client1.invoke(() -> {
      ClientCache cache = createClientCache("super-user", "1234567", server.getPort());
      Region region = createProxyRegion(cache, REGION_NAME);

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
      ClientCache cache = createClientCache("authRegionUser", "1234567", server.getPort());
      Region region = createProxyRegion(cache, REGION_NAME);
      region.put("key1", new SimpleClass(1, (byte) 1));
      region.put("key2", BYTES);
    });

    // wait for events to fire
    await();
    PDXPostProcessor pp =
        (PDXPostProcessor) server.getCache().getSecurityService().getPostProcessor();
    assertEquals(pp.getCount(), 2);
  }

}
