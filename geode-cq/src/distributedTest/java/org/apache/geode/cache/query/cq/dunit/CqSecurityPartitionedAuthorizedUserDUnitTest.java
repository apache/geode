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
package org.apache.geode.cache.query.cq.dunit;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

@Category({SecurityTest.class})
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class CqSecurityPartitionedAuthorizedUserDUnitTest
    extends CqSecurityAuthorizedUserDUnitTest {
  public RegionShortcut getRegionType() {
    return RegionShortcut.PARTITION;
  }

  @Test
  public void cqExecuteWithMethodInvocationWithUsersWithCqPermissionsWithPrepopulatedRegionIsGettingExceptionInReplicatedRegion()
      throws Exception {
    putIntoRegion(superUserClient, keys, values, regionName);

    String query = "select * from /" + regionName + " r where r.name = 'Beth'";

    specificUserClient.invoke(() -> {
      QueryService queryService = getClientCache().getQueryService();
      CqSecurityTestCqListener cqListener = new CqSecurityTestCqListener();
      CqSecurityAuthorizedUserDUnitTest.cqListener = cqListener;
      CqQuery cq = createCq(queryService, query, cqListener);
      cq.execute();
    });

    putIntoRegion(superUserClient, keys, values, regionName);

    specificUserClient.invoke(() -> {
      await()
          .untilAsserted(() -> assertEquals(1, cqListener.getNumErrors()));
    });
  }
}
