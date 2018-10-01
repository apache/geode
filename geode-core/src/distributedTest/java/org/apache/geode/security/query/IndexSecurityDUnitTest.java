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

import static org.apache.geode.internal.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.IndexInvalidException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.security.query.data.QueryTestObject;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category({SecurityTest.class})
@RunWith(Parameterized.class)
public class IndexSecurityDUnitTest extends QuerySecurityBase {
  public RegionShortcut getRegionType() {
    return RegionShortcut.REPLICATE;
  }

  @Parameterized.Parameters
  public static Object[] usersAllowed() {
    return new Object[] {"dataWriter"};
  }

  @Parameterized.Parameter
  public String user;


  @Before
  public void configureSpecificUserAndKeyAndValues() {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, regionName);

    keys = new Object[] {"key-0", "key-1", "key-2"};
    values = new Object[] {new QueryTestObject(1, "Mary"), new QueryTestObject(2, "Joe"),
        new QueryTestObject(3, "Joe")};
  }

  @Test
  public void indexCreatedButPutWithNoReadCredentialsShouldNotThrowSecurityException()
      throws Exception {
    QueryService queryService = server.getCache().getQueryService();
    Index idIndex = queryService.createIndex("IdIndex", "id", "/" + regionName);
    putIntoRegion(specificUserClient, keys, values, regionName);
  }

  @Test
  public void indexCreatedWithRegionEntriesButPutWithNoReadCredentialsShouldNotThrowSecurityException()
      throws Exception {
    QueryService queryService = server.getCache().getQueryService();
    Index idIndex = queryService.createIndex("IdIndex", "e.id", "/" + regionName + ".entries e");
    putIntoRegion(specificUserClient, keys, values, regionName);
  }

  @Test
  public void indexCreatedWithMethodInvocationOnPrepopulatedRegionShouldThrowSecurityException()
      throws Exception {
    QueryService queryService = server.getCache().getQueryService();
    putIntoRegion(superUserClient, keys, values, regionName);

    try {
      queryService.createIndex("IdIndex", "e.getName()", "/" + regionName + " e");
      fail("Index creation should have failed due to method invocation");
    } catch (IndexInvalidException e) {
      assertTrue(e.getMessage().contains("Unauthorized access to method: getName"));
    }
  }

  @Test
  public void indexCreatedWithMethodInvocationOnUnpopulatedRegionAndPutShouldMarkIndexInvalid()
      throws Exception {
    QueryService queryService = server.getCache().getQueryService();
    Index index = queryService.createIndex("IdIndex", "e.getName()", "/" + regionName + " e");
    putIntoRegion(superUserClient, keys, values, regionName);
    assertFalse(index.isValid());
  }

}
