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

import java.util.Arrays;
import java.util.HashSet;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.security.query.data.QueryTestObject;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category(SecurityTest.class)
public class QuerySecurityAuthorizedUserBindParameterDUnitTest extends QuerySecurityBase {

  @Before
  public void configureCache() {
    String user = "dataReaderRegion";
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, regionName);

    keys = new Object[] {"key-0", "key-1"};
    values = new Object[] {new QueryTestObject(1, "John"), new QueryTestObject(3, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);
  }

  @Test
  public void userWithRegionAccessAndPassingInWrappedBindParameterShouldReturnCorrectResults() {
    String query = "select v from $1 r, r.values() v";
    specificUserClient.invoke(() -> {
      Region region = getClientCache().getRegion(regionName);
      HashSet<Region> hashset = new HashSet<>();
      hashset.add(region);
      assertQueryResults(getClientCache(), query, new Object[] {hashset}, Arrays.asList(values));
    });
  }
}
