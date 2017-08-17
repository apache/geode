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

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.concurrent.TimeUnit;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.awaitility.Awaitility;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.security.query.QuerySecurityDUnitTest;
import org.apache.geode.security.query.UserPermissions;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category({DistributedTest.class, SecurityTest.class})
@RunWith(JUnitParamsRunner.class)
public class CqSecurityPartitionedDUnitTest extends CqSecurityDUnitTest {
  public RegionShortcut getRegionType() {
    return RegionShortcut.PARTITION;
  }


//  @Test
//  @Parameters(method = "getAllUsersThatCannotInvokeCq")
//  public void cqExecuteWithMethodInvocationWithUsersWithCqPermissionsWithPrepopulatedRegionIsGettingExceptionInReplicatedRegion(
//      String user) throws Exception {
//    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
//    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);
//
//    Object[] keys = {REGION_PUT_KEY};
//    Object[] values = {new QuerySecurityDUnitTest.QueryTestObject(1, "Mary")};
//    putIntoRegion(superUserClient, keys, values, UserPermissions.REGION_NAME);
//
//    String query = UserPermissions.SELECT_FROM_REGION_BY_IMPLICIT_GETTER;
//    List<String> expectedExceptionMessages =
//        userPerms.getMissingUserAuthorizationsForQuery(user, query);
//    String regexForExpectedExceptions = userPerms.regexForExpectedException(expectedExceptionMessages);
//
//    specificUserClient.invoke(() -> {
//      QueryService queryService = getClientCache().getQueryService();
//      CqSecurityTestCqListener cqListener = new CqSecurityTestCqListener();
//      CqSecurityDUnitTest.cqListener = cqListener;
//      CqQuery cq = createCq(queryService, query, cqListener);
//      executeCqButExpectException(cq, user, regexForExpectedExceptions);
//    });
//  }
//
  @Test
  @Parameters(method = "getAllUsersThatCanInvokeCq")
  public void cqExecuteWithMethodInvocationWithUsersWithCqPermissionsWithPrepopulatedRegionIsGettingExceptionInReplicatedRegion(
      String user) throws Exception {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    Object[] keys = {REGION_PUT_KEY};
    Object[] values = {new QuerySecurityDUnitTest.QueryTestObject(1, "Mary")};
    putIntoRegion(superUserClient, keys, values, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_FROM_REGION_BY_IMPLICIT_GETTER;

    specificUserClient.invoke(() -> {
      QueryService queryService = getClientCache().getQueryService();
      CqSecurityTestCqListener cqListener = new CqSecurityTestCqListener();
      CqSecurityDUnitTest.cqListener = cqListener;
      CqQuery cq = createCq(queryService, query, cqListener);
      cq.execute();
    });

    putIntoRegion(superUserClient, keys, values, UserPermissions.REGION_NAME);

    specificUserClient.invoke(() -> {
      Awaitility.await().atMost(30, TimeUnit.SECONDS)
          .until(() -> assertEquals(1, cqListener.getNumErrors()));
    });
  }
}
