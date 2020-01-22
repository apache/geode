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
package org.apache.geode.cache.query.internal;

import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.Serializable;
import java.util.stream.IntStream;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.test.junit.categories.OQLQueryTest;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category(OQLQueryTest.class)
@RunWith(JUnitParamsRunner.class)
public class GroupJunctionIntegrationTest {
  private static final int ENTRIES = 1000;
  private QueryService queryService;

  @Rule
  public TestName testName = new TestName();

  @Rule
  public ServerStarterRule server = new ServerStarterRule()
      .withProperty(SERIALIZABLE_OBJECT_FILTER,
          "org.apache.geode.cache.query.internal.GroupJunctionIntegrationTest$TestObject")
      .withAutoStart();

  @Before
  public void setUp() throws IOException {
    queryService = server.getCache().getQueryService();
  }

  @Test
  @Parameters({"REPLICATE", "PARTITION"})
  public void equiJoinWithBothFieldsIndexedAndMultipleFiltersOnSingleRegionShouldReturnCorrectResults(
      RegionShortcut regionShortcut) throws Exception {
    String regionName = testName.getMethodName();
    Region<String, TestObject> region = server.getCache()
        .<String, TestObject>createRegionFactory(regionShortcut).create(regionName);
    queryService.createIndex("uCode", "utilityCode", "/" + regionName);
    queryService.createIndex("fCode", "functionalCode", "/" + regionName);

    IntStream.range(0, ENTRIES).forEach(value -> {
      String stringValue = "string_" + value;
      TestObject testObject = new TestObject(stringValue, stringValue, stringValue, stringValue);
      region.put(String.valueOf(value), testObject);
    });

    String queryString = "<TRACE>SELECT * FROM /" + testName.getMethodName()
        + " WHERE utilityCode=functionalCode AND managementCode=code";
    Query queryObject = queryService.newQuery(queryString);

    SelectResults<?> results = (SelectResults<?>) queryObject.execute();
    assertThat(results).isNotNull();
    assertThat(results.size()).isEqualTo(ENTRIES);
  }

  @SuppressWarnings("unused")
  public static class TestObject implements Serializable {
    private final String code;
    private final String utilityCode;
    private final String functionalCode;
    private final String managementCode;

    public String getCode() {
      return code;
    }

    public String getUtilityCode() {
      return utilityCode;
    }

    public String getFunctionalCode() {
      return functionalCode;
    }

    public String getManagementCode() {
      return managementCode;
    }

    public TestObject(String code, String utilityCode, String functionalCode,
        String managementCode) {
      this.code = code;
      this.utilityCode = utilityCode;
      this.functionalCode = functionalCode;
      this.managementCode = managementCode;
    }
  }
}
