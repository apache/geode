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

import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.apache.geode.security.SecurityTestUtil.createClientCache;
import static org.apache.geode.security.SecurityTestUtil.createProxyRegion;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.security.TestSecurityManager;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.dunit.rules.ServerStarterRule;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category({DistributedTest.class, SecurityTest.class})
@RunWith(JUnitParamsRunner.class)
public class IndexSecurityDUnitTest extends QuerySecurityBase {

  // Should be the same as the key specified for the region key specific users in the
  // clientServer.json
  public static final String REGION_PUT_KEY = "key";


  public List<String> getAllUsersOnlyAllowedWrite() {
    return Arrays.asList(
        "dataWriter");
  }

  @Test
  @Parameters(method = "getAllUsers")
  public void indexBehaviorTest(String user) throws Exception {
    String query = UserPermissions.SELECT_ALL_FROM_REGION;
    String region = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + 1, REGION_PUT_KEY + 2};
    Object[] values = {new QuerySecurityDUnitTest.QueryTestObject(1, "Mary"), new QuerySecurityDUnitTest.QueryTestObject(2, "Joe"),
        new QuerySecurityDUnitTest.QueryTestObject(3, "Joe")};
    QueryService queryService = server.getCache().getQueryService();
    Index idIndex = queryService.createIndex("IdIndex", "id", "/" + UserPermissions.REGION_NAME);
    Index errorIndex = queryService.createIndex("TEST", "methodThrowsException",
        "/" + UserPermissions.REGION_NAME);

    try {
      putIntoRegion(superUserClient, keys, values, region);
    } catch (Exception e) {
      e.printStackTrace();
    }

  }


}
