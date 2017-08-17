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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxWriter;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category({DistributedTest.class, SecurityTest.class})
@RunWith(JUnitParamsRunner.class)
public class PdxQuerySecurityDUnitTest extends QuerySecurityBase {

  // Should be the same as the key specified for the region key specific users in the
  // clientServer.json
  public static final String REGION_PUT_KEY = "key";


  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectOnPublicFieldQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_FROM_REGION_BY_PUBLIC_FIELD;
    String region = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new PdxQueryTestObject(0, "John"), new PdxQueryTestObject(3, "Beth")};
    putIntoRegion(superUserClient, keys, values, region);

    List<Object> expectedResults = new ArrayList<>();
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, region, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectWithPdxFieldNoExistingPublicFieldQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_FROM_REGION_BY_IMPLICIT_GETTER;
    String region = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new PdxQueryTestObject(0, "John"), new PdxQueryTestObject(3, "Beth")};
    putIntoRegion(superUserClient, keys, values, region);

    List<Object> expectedResults = new ArrayList<>();
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, region, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectWhenInvokingMethodOnPdxObjectQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_DEPLOYED_METHOD_FROM_REGION;
    String region = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new PdxQueryTestObject(0, "John"), new PdxQueryTestObject(3, "Beth")};
    putIntoRegion(superUserClient, keys, values, region);

    List<Object> expectedResults = new ArrayList<>();
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, region, expectedResults);
  }

  public static class PdxQueryTestObject implements PdxSerializable , Serializable /*just to pass around in test code*/ {
    public int id = -1;
    private String name;

    private boolean shouldThrowException = true;

    public PdxQueryTestObject(int id, String name) {
      this.id = id;
      this.name = name;
    }

    public String getName() {
      return name;
    }

    @Override
    public String toString() {
      return "Test_Object";
    }

    @Override
    public void toData(PdxWriter writer) {
      writer.writeInt("id", id);
      writer.writeString("getName", name);
    }

    @Override
    public void fromData(PdxReader reader) {
      id = reader.readInt("id");
      name = reader.readString("getName");
    }
  }
}
