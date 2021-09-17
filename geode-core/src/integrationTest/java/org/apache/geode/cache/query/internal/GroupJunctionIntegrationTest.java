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

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import junitparams.Parameters;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.test.junit.categories.OQLQueryTest;
import org.apache.geode.test.junit.rules.ServerStarterRule;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;
import org.apache.geode.util.internal.GeodeGlossary;

@Category(OQLQueryTest.class)
@RunWith(GeodeParamsRunner.class)
public class GroupJunctionIntegrationTest {
  private static final int ENTRIES = 1000;
  private static final String REGION_NAME = "testRegion";
  private QueryService queryService;

  @Rule
  public ServerStarterRule server = new ServerStarterRule()
      .withProperty(SERIALIZABLE_OBJECT_FILTER,
          "org.apache.geode.cache.query.internal.GroupJunctionIntegrationTest$TestObject")
      .withAutoStart();

  @ClassRule
  public static RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @BeforeClass
  public static void beforeClass() {
    System.setProperty(GeodeGlossary.GEMFIRE_PREFIX + "Query.VERBOSE", "true");
  }

  @Before
  public void setUp() throws IOException {
    queryService = server.getCache().getQueryService();
  }

  @SuppressWarnings("unused")
  private Object[] regionTypeAndQuery() {
    return new Object[] {
        // The comparison order matters, so we test both combinations (see GEODE-7728).
        new Object[] {"REPLICATE", "SELECT * FROM " + SEPARATOR + REGION_NAME
            + " WHERE code=utilityCode AND functionalCode=managementCode"},
        new Object[] {"REPLICATE", "SELECT * FROM " + SEPARATOR + REGION_NAME
            + " WHERE functionalCode=managementCode AND code=utilityCode"},
        new Object[] {"REPLICATE", "SELECT * FROM " + SEPARATOR + REGION_NAME
            + " WHERE code=utilityCode OR functionalCode=managementCode"},
        new Object[] {"REPLICATE", "SELECT * FROM " + SEPARATOR + REGION_NAME
            + " WHERE functionalCode=managementCode OR code=utilityCode"},

        new Object[] {"PARTITION", "SELECT * FROM " + SEPARATOR + REGION_NAME
            + " WHERE code=utilityCode AND functionalCode=managementCode"},
        new Object[] {"PARTITION", "SELECT * FROM " + SEPARATOR + REGION_NAME
            + " WHERE functionalCode=managementCode AND code=utilityCode"},
        new Object[] {"PARTITION", "SELECT * FROM " + SEPARATOR + REGION_NAME
            + " WHERE code=utilityCode OR functionalCode=managementCode"},
        new Object[] {"PARTITION", "SELECT * FROM " + SEPARATOR + REGION_NAME
            + " WHERE functionalCode=managementCode OR code=utilityCode"},
    };
  }

  private void setUpRegionAndIndexes(RegionShortcut regionShortcut, List<String> indexedFields) {
    Region<String, TestObject> region = server.getCache()
        .<String, TestObject>createRegionFactory(regionShortcut).create(REGION_NAME);
    indexedFields.forEach(fieldName -> {
      try {
        queryService.createIndex(fieldName + "Index", fieldName, SEPARATOR + REGION_NAME);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    IntStream.range(0, ENTRIES).forEach(value -> {
      String stringValue = "string_" + value;
      TestObject testObject = new TestObject(stringValue, stringValue, stringValue, stringValue);
      region.put(String.valueOf(value), testObject);
    });
  }

  @Test
  @Parameters(method = "regionTypeAndQuery")
  public void equiJoinWithBothFieldsRangeIndexedAndOtherFilterOnSingleRegionShouldReturnCorrectResults(
      RegionShortcut regionShortcut, String queryString) throws Exception {
    setUpRegionAndIndexes(regionShortcut, Arrays.asList("code", "utilityCode"));
    Query queryObject = queryService.newQuery(queryString);

    SelectResults<?> results = (SelectResults<?>) queryObject.execute();
    assertThat(results).isNotNull();
    assertThat(results.size()).isEqualTo(ENTRIES);
  }

  @Test
  @Parameters(method = "regionTypeAndQuery")
  public void multipleEquiJoinsWithAllFieldsRangedIndexedOnSingleRegionShouldReturnCorrectResults(
      RegionShortcut regionShortcut, String queryString) throws Exception {
    setUpRegionAndIndexes(regionShortcut,
        Arrays.asList("code", "utilityCode", "functionalCode", "managementCode"));
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
