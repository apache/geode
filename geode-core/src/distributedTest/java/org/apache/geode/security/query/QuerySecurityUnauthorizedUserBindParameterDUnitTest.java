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

import java.util.HashSet;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.internal.QRegion;
import org.apache.geode.cache.query.internal.index.DummyQRegion;
import org.apache.geode.security.query.data.QueryTestObject;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category({SecurityTest.class})
public class QuerySecurityUnauthorizedUserBindParameterDUnitTest extends QuerySecurityBase {

  @Before
  public void configureCache() {
    String user = "dataReaderRegionKey";
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, regionName);

    keys = new Object[] {"key-0", "key-1"};
    values = new Object[] {new QueryTestObject(1, "John"), new QueryTestObject(3, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);
  }

  @Test
  public void userWithoutRegionAccessAndPassingInWrappedBindParameterShouldThrowException() {
    String query = "select v from $1 r, r.values() v";
    String regexForExpectedException = ".*values.*";
    specificUserClient.invoke(() -> {
      Region region = getClientCache().getRegion(regionName);
      HashSet<Region> hashSet = new HashSet<>();
      hashSet.add(region);
      assertExceptionOccurred(getClientCache().getQueryService(), query, new Object[] {hashSet},
          regexForExpectedException);
    });
  }

  // If DummyQRegion is ever serializable, then this test will fail and a security hole with query
  // will have been opened
  // That means a user could wrap a region in a dummy region and bypass the
  // RestrictedMethodInvocationAuthorizer
  @Test
  public void userWithoutRegionAccessAndPassingInWrappedInDummyQRegionBindParameterShouldThrowSerializationException() {
    String query = "select v from $1 r, r.values() v";
    String regexForExpectedException = ".*failed serializing object.*";
    specificUserClient.invoke(() -> {
      Region region = getClientCache().getRegion(regionName);
      HashSet<QRegion> hashset = new HashSet<>();
      hashset.add(new DummyQRegion(region));
      assertExceptionOccurred(getClientCache().getQueryService(), query, new Object[] {hashset},
          regexForExpectedException);
    });
  }
}
