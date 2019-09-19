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
package org.apache.geode.security.query;

import static org.assertj.core.api.Assertions.assertThat;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.security.RestrictedMethodAuthorizer;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.security.query.data.PdxTrade;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.SecurityTest;

/**
 * This test verifies client/server distinct order-by queries with integrated security
 */
@Category(SecurityTest.class)
@RunWith(JUnitParamsRunner.class)
public class QuerySecurityDistinctQueryDistributedTest extends QuerySecurityBase {

  private static final int NUM_ENTRIES = 1000;

  @Override
  public RegionShortcut getRegionType() {
    return RegionShortcut.PARTITION;
  }

  @Test
  @Parameters({"99", "100", "499", "500", "999", "1000", "1500"})
  public void verifyDistinctOrderByQueryWithLimits(int limit) {
    // Do puts from the client
    putIntoRegion(superUserClient, NUM_ENTRIES);

    // Execute query from the client and validate the size of the results
    superUserClient.invoke(() -> {
      String query = "<trace> select distinct * from /" + regionName
          + " where cusip = 'PVTL' order by id asc limit " + limit;
      QueryService queryService = getClientCache().getQueryService();
      Object results = queryService.newQuery(query).execute();
      assertThat(results).isInstanceOf(SelectResults.class);
      assertThat(((SelectResults) results).size()).isEqualTo(Math.min(limit, NUM_ENTRIES));
    });
  }

  @Test
  public void verifyDistinctOrderByQueryOnMethodFails() {
    // Do puts from the client
    putIntoRegion(superUserClient, 1);

    // Execute query from the client and validate the size of the results
    superUserClient.invoke(() -> {
      String methodName = "getCusip";
      String query = "<trace> select distinct * from /" + regionName
          + " where " + methodName + " = 'PVTL' order by id asc";
      QueryService queryService = getClientCache().getQueryService();
      try {
        queryService.newQuery(query).execute();
      } catch (Exception e) {
        assertThat(e).isInstanceOf(ServerOperationException.class);
        assertThat(e.getCause()).isInstanceOf(NotAuthorizedException.class);
        assertThat(e.getMessage()).contains(
            RestrictedMethodAuthorizer.UNAUTHORIZED_STRING + methodName);
      }
    });
  }

  private void putIntoRegion(VM clientVM, int numEntries) {
    keys = new Object[numEntries];
    values = new Object[numEntries];
    for (int i = 0; i < numEntries; i++) {
      String key = String.valueOf(i);
      Object value = new PdxTrade(key, "PVTL", 100, 30);
      keys[i] = key;
      values[i] = value;
    }
    putIntoRegion(clientVM, keys, values, regionName);
  }
}
