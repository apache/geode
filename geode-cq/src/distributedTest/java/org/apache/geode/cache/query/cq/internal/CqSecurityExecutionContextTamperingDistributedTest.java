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
package org.apache.geode.cache.query.cq.internal;

import static org.apache.geode.cache.RegionShortcut.PARTITION;
import static org.apache.geode.cache.RegionShortcut.REPLICATE;
import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.internal.ExecutionContext;
import org.apache.geode.cache.query.internal.ExecutionContextTamperer;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.security.query.TestCqListener;
import org.apache.geode.security.query.data.QueryTestObject;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.OQLQueryTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

/**
 * Verifies that users can tamper the {@link ExecutionContext}.
 * It needs to be part of the {@link org.apache.geode.cache.query.cq.internal} package because the
 * method {@link CqQueryImpl#getQueryExecutionContext()} has package access.
 */
@RunWith(Parameterized.class)
@Category({SecurityTest.class, OQLQueryTest.class})
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class CqSecurityExecutionContextTamperingDistributedTest implements Serializable {
  private MemberVM server;
  private ClientVM client;
  protected final String regionName = "region";
  private static TestCqListener cqListener = null;

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Parameterized.Parameters(name = "RegionType:{1}, Accessor:{0}")
  public static Object[] regionTypes() {
    return new Object[][] {
        {"name", REPLICATE}, {"name", PARTITION},
        {"getName", REPLICATE}, {"getName", PARTITION},
    };
  }

  @Parameterized.Parameter
  public String attributeAccessor;

  @Parameterized.Parameter(1)
  public RegionShortcut regionShortcut;

  @Before
  public void setUp() throws Exception {
    server = cluster.startServerVM(1, cf -> cf
        .withSecurityManager(SimpleSecurityManager.class)
        .withProperty(SERIALIZABLE_OBJECT_FILTER, "org.apache.geode.security.query.data.*")
        .withRegion(regionShortcut, regionName));

    client = cluster
        .startClientVM(2, ccf -> ccf.withCredential("dataRead", "dataRead")
            .withPoolSubscription(true)
            .withServerConnection(server.getPort())
            .withProperty(SERIALIZABLE_OBJECT_FILTER, "org.apache.geode.security.query.data.*"));

    client.invoke(() -> {
      assertThat(ClusterStartupRule.getClientCache()).isNotNull();
      ClusterStartupRule.getClientCache().createClientRegionFactory(ClientRegionShortcut.PROXY)
          .create(regionName);
    });
  }

  @Test
  public void executionContextShouldNotBeModifiableForCqQueriesWithMethodInvocations() {
    String query = "SELECT * FROM /" + regionName + " r WHERE r." + attributeAccessor + " = 'Beth'";

    client.invoke(() -> {
      TestCqListener cqListener = new TestCqListener();
      assertThat(ClusterStartupRule.getClientCache()).isNotNull();
      QueryService queryService = ClusterStartupRule.getClientCache().getQueryService();
      CqSecurityExecutionContextTamperingDistributedTest.cqListener = cqListener;
      CqAttributesFactory cqAttributesFactory = new CqAttributesFactory();
      cqAttributesFactory.addCqListener(cqListener);

      CqQuery cq = queryService.newCq(query, cqAttributesFactory.create());
      cq.execute();
    });

    server.invoke(() -> {
      assertThat(ClusterStartupRule.getCache()).isNotNull();
      InternalCache internalCache = ClusterStartupRule.getCache();
      assertThat(internalCache.getCqService().getAllCqs().size()).isEqualTo(1);
      CqQueryImpl cqQueryImpl =
          (CqQueryImpl) internalCache.getCqService().getAllCqs().iterator().next();
      ExecutionContextTamperer.tamperContextCache(cqQueryImpl.getQueryExecutionContext(),
          "org.apache.geode.security.query.data.QueryTestObject.getName", true);

      Region<String, QueryTestObject> region = ClusterStartupRule.getCache().getRegion(regionName);
      region.put("1", new QueryTestObject(1, "Beth"));
    });

    client.invoke(() -> {
      assertThat(CqSecurityExecutionContextTamperingDistributedTest.cqListener.getNumEvents())
          .isEqualTo(0);
      assertThat(CqSecurityExecutionContextTamperingDistributedTest.cqListener.getNumErrors())
          .isEqualTo(1);
    });
  }
}
