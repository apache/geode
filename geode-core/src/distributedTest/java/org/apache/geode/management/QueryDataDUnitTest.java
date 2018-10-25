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
package org.apache.geode.management;

import static com.jayway.jsonpath.matchers.JsonPathMatchers.hasJsonPath;
import static com.jayway.jsonpath.matchers.JsonPathMatchers.isJson;
import static com.jayway.jsonpath.matchers.JsonPathMatchers.withJsonPath;
import static org.apache.geode.cache.FixedPartitionAttributes.createFixedPartition;
import static org.apache.geode.cache.query.Utils.createPortfoliosAndPositions;
import static org.apache.geode.management.internal.ManagementConstants.DEFAULT_QUERY_LIMIT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.anything;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import javax.management.ObjectName;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryOperation;
import org.apache.geode.cache.FixedPartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.PartitionResolver;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionHelper;
import org.apache.geode.internal.cache.partitioned.fixed.SingleHopQuarterPartitionResolver;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.management.internal.beans.BeanUtilFuncs;
import org.apache.geode.management.internal.cli.json.TypedJson;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxInstanceFactory;
import org.apache.geode.pdx.internal.PdxInstanceFactoryImpl;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedUseJacksonForJsonPathRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * Distributed tests for {@link DistributedSystemMXBean#queryData(String, String, int)}.
 * </p>
 *
 * <pre>
 * Test Basic Json Strings for Partitioned Regions
 * Test Basic Json Strings for Replicated Regions
 * Test for all Region Types
 * Test for primitive types
 * Test for Nested Objects
 * Test for Enums
 * Test for collections
 * Test for huge collection
 * Test PDX types
 * Test different projects type e.g. SelectResult, normal bean etc..
 * Test Colocated Regions
 * Test for Limit ( both row count and Depth)
 * ORDER by orders
 * Test all attributes are covered in an complex type
 * </pre>
 */

@SuppressWarnings({"serial", "unused"})
public class QueryDataDUnitTest implements Serializable {

  private static final int NUM_OF_BUCKETS = 20;

  // PARTITIONED_REGION_NAME5 is co-located with PARTITIONED_REGION_NAME4
  private static final String PARTITIONED_REGION_NAME1 = "PARTITIONED_REGION_NAME1";
  private static final String PARTITIONED_REGION_NAME2 = "PARTITIONED_REGION_NAME2";
  private static final String PARTITIONED_REGION_NAME3 = "PARTITIONED_REGION_NAME3";
  private static final String PARTITIONED_REGION_NAME4 = "PARTITIONED_REGION_NAME4";
  private static final String PARTITIONED_REGION_NAME5 = "PARTITIONED_REGION_NAME5";

  private static final String REPLICATE_REGION_NAME1 = "REPLICATE_REGION_NAME1";
  private static final String REPLICATE_REGION_NAME2 = "REPLICATE_REGION_NAME2";
  private static final String REPLICATE_REGION_NAME3 = "REPLICATE_REGION_NAME3";
  private static final String REPLICATE_REGION_NAME4 = "REPLICATE_REGION_NAME4";

  private static final String LOCAL_REGION_NAME = "LOCAL_REGION_NAME";

  private static final String BIG_COLLECTION_ELEMENT_ = "BIG_COLLECTION_ELEMENT_";
  private static final String BIG_COLLECTION_ = "BIG_COLLECTION_";

  private static final String[] QUERIES =
      new String[] {"SELECT * FROM /" + PARTITIONED_REGION_NAME1 + " WHERE ID >= 0",
          "SELECT * FROM /" + PARTITIONED_REGION_NAME1 + " r1, /" + PARTITIONED_REGION_NAME2
              + " r2 WHERE r1.ID = r2.ID",
          "SELECT * FROM /" + PARTITIONED_REGION_NAME1 + " r1, /" + PARTITIONED_REGION_NAME2
              + " r2 WHERE r1.ID = r2.ID AND r1.status = r2.status",
          "SELECT * FROM /" + PARTITIONED_REGION_NAME1 + " r1, /" + PARTITIONED_REGION_NAME2
              + " r2, /" + PARTITIONED_REGION_NAME3 + " r3 WHERE r1.ID = r2.ID AND r2.ID = r3.ID",
          "SELECT * FROM /" + PARTITIONED_REGION_NAME1 + " r1, /" + PARTITIONED_REGION_NAME2
              + " r2, /" + PARTITIONED_REGION_NAME3 + " r3, /" + REPLICATE_REGION_NAME1
              + " r4 WHERE r1.ID = r2.ID AND r2.ID = r3.ID AND r3.ID = r4.ID",
          "SELECT * FROM /" + PARTITIONED_REGION_NAME4 + " r4, /" + PARTITIONED_REGION_NAME5
              + " r5 WHERE r4.ID = r5.ID"};

  private static final String[] QUERIES_FOR_REPLICATED =
      new String[] {"<TRACE> SELECT * FROM /" + REPLICATE_REGION_NAME1 + " WHERE ID >= 0",
          "SELECT * FROM /" + REPLICATE_REGION_NAME1 + " r1, /" + REPLICATE_REGION_NAME2
              + " r2 WHERE r1.ID = r2.ID",
          "SELECT * FROM /" + REPLICATE_REGION_NAME3 + " WHERE ID >= 0"};

  private static final String[] QUERIES_FOR_LIMIT =
      new String[] {"SELECT * FROM /" + REPLICATE_REGION_NAME4};

  private DistributedMember member1;
  private DistributedMember member2;
  private DistributedMember member3;

  @Member
  private VM[] memberVMs;

  @Manager
  private VM managerVM;

  @Rule
  public DistributedUseJacksonForJsonPathRule useJacksonForJsonPathRule =
      new DistributedUseJacksonForJsonPathRule();

  @Rule
  public ManagementTestRule managementTestRule =
      ManagementTestRule.builder().defineManagersFirst(false).start(true).build();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Before
  public void before() throws Exception {
    member1 = managementTestRule.getDistributedMember(memberVMs[0]);
    member2 = managementTestRule.getDistributedMember(memberVMs[1]);
    member3 = managementTestRule.getDistributedMember(memberVMs[2]);

    createRegionsInNodes();
    generateValuesInRegions();
  }

  @Test
  public void testQueryOnPartitionedRegion() throws Exception {
    managerVM.invoke(testName.getMethodName(), () -> {
      DistributedSystemMXBean distributedSystemMXBean =
          managementTestRule.getSystemManagementService().getDistributedSystemMXBean();

      String jsonString = distributedSystemMXBean.queryData(QUERIES[0], null, 10);
      assertThat(jsonString).contains("result").doesNotContain("No Data Found");

      for (int i = 0; i < QUERIES.length; i++) {
        jsonString = distributedSystemMXBean.queryData(QUERIES[i], member1.getId(), 10);
        assertThat(jsonString).contains("result");
        assertThat(jsonString).contains("member");
        assertThat("QUERIES[" + i + "]", jsonString, isJson(withJsonPath("$..result", anything())));

        // TODO: create better assertions
        // assertThat("QUERIES[" + i + "]", result,
        // isJson(withJsonPath("$..member",
        // equalTo(JsonPath.compile(result)))));
        // //equalTo(new JSONObject().put(String.class.getName(), member1.getId())))));
        // System.out.println(JsonPath.read(jsonString, "$.result.*"));
        // System.out.println(JsonPath.read(jsonString, "$['result']['member']"));

        verifyJsonIsValid(jsonString);
      }
    });
  }

  @Test
  public void testQueryOnReplicatedRegion() throws Exception {
    managerVM.invoke(testName.getMethodName(), () -> {
      DistributedSystemMXBean distributedSystemMXBean =
          managementTestRule.getSystemManagementService().getDistributedSystemMXBean();

      String jsonString = distributedSystemMXBean.queryData(QUERIES_FOR_REPLICATED[0], null, 10);
      assertThat(jsonString).contains("result").doesNotContain("No Data Found");

      for (int i = 0; i < QUERIES_FOR_REPLICATED.length; i++) {
        assertThat(jsonString).contains("result");
        verifyJsonIsValid(jsonString);
      }
    });
  }

  @Test
  public void testMemberWise() throws Exception {
    managerVM.invoke(testName.getMethodName(), () -> {
      DistributedSystemMXBean distributedSystemMXBean =
          managementTestRule.getSystemManagementService().getDistributedSystemMXBean();

      byte[] bytes = distributedSystemMXBean.queryDataForCompressedResult(QUERIES_FOR_REPLICATED[0],
          member1.getId() + "," + member2.getId(), 2);
      String jsonString = BeanUtilFuncs.decompress(bytes);

      verifyJsonIsValid(jsonString);
    });
  }

  @Test
  public void testLimitForQuery() throws Exception {
    memberVMs[0].invoke("putBigInstances", () -> putBigInstances(REPLICATE_REGION_NAME4));

    managerVM.invoke(testName.getMethodName(), () -> {
      DistributedSystemMXBean distributedSystemMXBean =
          managementTestRule.getSystemManagementService().getDistributedSystemMXBean();

      // Query With Default values
      assertThat(distributedSystemMXBean.getQueryCollectionsDepth())
          .isEqualTo(TypedJson.DEFAULT_COLLECTION_ELEMENT_LIMIT);
      assertThat(distributedSystemMXBean.getQueryResultSetLimit()).isEqualTo(DEFAULT_QUERY_LIMIT);

      String jsonString = distributedSystemMXBean.queryData(QUERIES_FOR_LIMIT[0], null, 0);

      verifyJsonIsValid(jsonString);
      assertThat(jsonString).contains("result").doesNotContain("No Data Found");
      assertThat(jsonString).contains(BIG_COLLECTION_ELEMENT_);

      JSONObject jsonObject = new JSONObject(jsonString);
      JSONArray jsonArray = jsonObject.getJSONArray("result");
      assertThat(jsonArray.length()).isEqualTo(DEFAULT_QUERY_LIMIT);

      // Get the first element
      JSONArray jsonArray1 = jsonArray.getJSONArray(0);

      // Get the ObjectValue
      JSONObject collectionObject = (JSONObject) jsonArray1.get(1);
      assertThat(collectionObject.length()).isEqualTo(100);

      // Query With Override Values
      int newQueryCollectionDepth = 150;
      int newQueryResultSetLimit = 500;

      distributedSystemMXBean.setQueryCollectionsDepth(newQueryCollectionDepth);
      distributedSystemMXBean.setQueryResultSetLimit(newQueryResultSetLimit);

      assertThat(distributedSystemMXBean.getQueryCollectionsDepth())
          .isEqualTo(newQueryCollectionDepth);
      assertThat(distributedSystemMXBean.getQueryResultSetLimit())
          .isEqualTo(newQueryResultSetLimit);

      jsonString = distributedSystemMXBean.queryData(QUERIES_FOR_LIMIT[0], null, 0);

      verifyJsonIsValid(jsonString);
      assertThat(jsonString).contains("result").doesNotContain("No Data Found");

      jsonObject = new JSONObject(jsonString);
      assertThat(jsonString).contains(BIG_COLLECTION_ELEMENT_);

      jsonArray = jsonObject.getJSONArray("result");
      assertThat(jsonArray.length()).isEqualTo(newQueryResultSetLimit);

      // Get the first element
      jsonArray1 = jsonArray.getJSONArray(0);

      // Get the ObjectValue
      collectionObject = (JSONObject) jsonArray1.get(1);
      assertThat(collectionObject.length()).isEqualTo(newQueryCollectionDepth);
    });
  }

  @Test
  public void testErrors() throws Exception {
    managerVM.invoke(testName.getMethodName(), () -> {
      DistributedSystemMXBean distributedSystemMXBean =
          managementTestRule.getSystemManagementService().getDistributedSystemMXBean();

      String invalidQuery = "SELECT * FROM " + PARTITIONED_REGION_NAME1;
      String invalidQueryResult = distributedSystemMXBean.queryData(invalidQuery, null, 2);
      assertThat(invalidQueryResult,
          isJson(
              withJsonPath("$.message", equalTo(String.format("Query is invalid due to error : %s",
                  "Region mentioned in query probably missing /")))));

      String nonexistentRegionName = testName.getMethodName() + "_NONEXISTENT_REGION";
      String regionsNotFoundQuery = "SELECT * FROM /" + nonexistentRegionName
          + " r1, PARTITIONED_REGION_NAME2 r2 WHERE r1.ID = r2.ID";
      String regionsNotFoundResult =
          distributedSystemMXBean.queryData(regionsNotFoundQuery, null, 2);
      assertThat(regionsNotFoundResult, isJson(withJsonPath("$.message",
          equalTo(String.format("Cannot find regions %s in any of the members",
              "/" + nonexistentRegionName)))));

      String regionName = testName.getMethodName() + "_REGION";
      String regionsNotFoundOnMembersQuery = "SELECT * FROM /" + regionName;

      RegionFactory regionFactory =
          managementTestRule.getCache().createRegionFactory(RegionShortcut.REPLICATE);
      regionFactory.create(regionName);

      String regionsNotFoundOnMembersResult =
          distributedSystemMXBean.queryData(regionsNotFoundOnMembersQuery, member1.getId(), 2);
      assertThat(regionsNotFoundOnMembersResult, isJson(withJsonPath("$.message",
          equalTo(
              String.format("Cannot find regions %s in specified members", "/" + regionName)))));

      String joinMissingMembersQuery = QUERIES[1];
      String joinMissingMembersResult =
          distributedSystemMXBean.queryData(joinMissingMembersQuery, null, 2);
      assertThat(joinMissingMembersResult,
          isJson(withJsonPath("$.message", equalTo(
              "Join operation can only be executed on targeted members, please give member input"))));
    });
  }

  @Test
  public void testNormalRegions() throws Exception {
    managerVM.invoke(testName.getMethodName(), () -> {
      DistributedSystemMXBean distributedSystemMXBean =
          managementTestRule.getSystemManagementService().getDistributedSystemMXBean();

      String normalRegionName1 = testName.getMethodName() + "_NORMAL_REGION_1";
      String tempRegionName1 = testName.getMethodName() + "_TEMP_REGION_1";

      // to Reverse order of regions while getting Random region in QueryDataFunction [?]
      String normalRegionName2 = testName.getMethodName() + "_NORMAL_REGION_2";
      String tempRegionName2 = testName.getMethodName() + "_TEMP_REGION_2";

      Cache cache = managementTestRule.getCache();

      RegionFactory regionFactory = cache.createRegionFactory(RegionShortcut.LOCAL_HEAP_LRU);
      regionFactory.create(normalRegionName1);
      regionFactory.create(normalRegionName2);

      Region region = cache.getRegion("/" + normalRegionName1);
      assertThat(region.getAttributes().getDataPolicy()).isEqualTo(DataPolicy.NORMAL);

      RegionFactory regionFactory1 = cache.createRegionFactory(RegionShortcut.REPLICATE);
      regionFactory1.create(tempRegionName1);
      regionFactory1.create(tempRegionName2);

      String query1 = "SELECT * FROM /" + tempRegionName1 + " r1, /" + normalRegionName1
          + " r2 WHERE r1.ID = r2.ID";
      String query2 = "SELECT * FROM /" + normalRegionName2 + " r1, /" + tempRegionName2
          + " r2 WHERE r1.ID = r2.ID";
      String query3 = "SELECT * FROM /" + normalRegionName2;

      distributedSystemMXBean.queryDataForCompressedResult(query1, null, 2);
      distributedSystemMXBean.queryDataForCompressedResult(query2, null, 2);
      distributedSystemMXBean.queryDataForCompressedResult(query3, null, 2);

      // TODO: assert results of queryDataForCompressedResult?
    });
  }

  @Test
  public void testRegionsLocalDataSet() throws Exception {
    String partitionedRegionName = testName.getMethodName() + "_PARTITIONED_REGION";

    String[] values1 = new String[] {"val1", "val2", "val3"};
    String[] values2 = new String[] {"val4", "val5", "val6"};

    memberVMs[0].invoke(testName.getMethodName() + " Create Region", () -> {
      PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory();
      partitionAttributesFactory.setRedundantCopies(2).setTotalNumBuckets(12);

      List<FixedPartitionAttributes> fixedPartitionAttributesList = createFixedPartitionList(1);
      for (FixedPartitionAttributes fixedPartitionAttributes : fixedPartitionAttributesList) {
        partitionAttributesFactory.addFixedPartitionAttributes(fixedPartitionAttributes);
      }
      partitionAttributesFactory.setPartitionResolver(new SingleHopQuarterPartitionResolver());

      RegionFactory regionFactory =
          managementTestRule.getCache().createRegionFactory(RegionShortcut.PARTITION)
              .setPartitionAttributes(partitionAttributesFactory.create());
      Region region = regionFactory.create(partitionedRegionName);

      for (int i = 0; i < values1.length; i++) {
        region.put(getDate(2013, 1, i + 5), values1[i]);
      }
    });

    memberVMs[1].invoke(testName.getMethodName() + " Create Region", () -> {
      PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory();
      partitionAttributesFactory.setRedundantCopies(2).setTotalNumBuckets(12);

      List<FixedPartitionAttributes> fixedPartitionAttributesList = createFixedPartitionList(2);
      for (FixedPartitionAttributes fixedPartitionAttributes : fixedPartitionAttributesList) {
        partitionAttributesFactory.addFixedPartitionAttributes(fixedPartitionAttributes);
      }
      partitionAttributesFactory.setPartitionResolver(new SingleHopQuarterPartitionResolver());

      RegionFactory regionFactory =
          managementTestRule.getCache().createRegionFactory(RegionShortcut.PARTITION)
              .setPartitionAttributes(partitionAttributesFactory.create());
      Region region = regionFactory.create(partitionedRegionName);

      for (int i = 0; i < values2.length; i++) {
        region.put(getDate(2013, 5, i + 5), values2[i]);
      }
    });

    memberVMs[2].invoke(testName.getMethodName() + " Create Region", () -> {
      PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory();
      partitionAttributesFactory.setRedundantCopies(2).setTotalNumBuckets(12);

      List<FixedPartitionAttributes> fixedPartitionAttributesList = createFixedPartitionList(3);
      fixedPartitionAttributesList.forEach(partitionAttributesFactory::addFixedPartitionAttributes);
      partitionAttributesFactory.setPartitionResolver(new SingleHopQuarterPartitionResolver());

      RegionFactory regionFactory =
          managementTestRule.getCache().createRegionFactory(RegionShortcut.PARTITION)
              .setPartitionAttributes(partitionAttributesFactory.create());
      regionFactory.create(partitionedRegionName);
    });

    List<String> member1RealData =
        memberVMs[0].invoke(() -> getLocalDataSet(partitionedRegionName));
    List<String> member2RealData =
        memberVMs[1].invoke(() -> getLocalDataSet(partitionedRegionName));
    List<String> member3RealData =
        memberVMs[2].invoke(() -> getLocalDataSet(partitionedRegionName));

    managerVM.invoke(testName.getMethodName(), () -> {
      DistributedSystemMXBean distributedSystemMXBean =
          managementTestRule.getSystemManagementService().getDistributedSystemMXBean();
      DistributedRegionMXBean distributedRegionMXBean =
          awaitDistributedRegionMXBean("/" + partitionedRegionName, 3);

      String alias = "Waiting for all entries to get reflected at managing node";
      int expectedEntryCount = values1.length + values2.length;
      await(alias)
          .untilAsserted(() -> assertThat(distributedRegionMXBean.getSystemRegionEntryCount())
              .isEqualTo(expectedEntryCount));

      String query = "Select * from /" + partitionedRegionName;

      String member1Result = distributedSystemMXBean.queryData(query, member1.getId(), 0);
      verifyJsonIsValid(member1Result);

      String member2Result = distributedSystemMXBean.queryData(query, member2.getId(), 0);
      verifyJsonIsValid(member2Result);

      String member3Result = distributedSystemMXBean.queryData(query, member3.getId(), 0);
      verifyJsonIsValid(member3Result);

      for (String val : member1RealData) {
        assertThat(member1Result).contains(val);
      }

      for (String val : member2RealData) {
        assertThat(member2Result).contains(val);
      }

      assertThat(member3Result).contains("No Data Found");
    });
  }

  private Date getDate(final int year, final int month, final int date) {
    Calendar calendar = Calendar.getInstance();
    calendar.set(year, month, date);
    return calendar.getTime();
  }

  private void verifyJsonIsValid(final String jsonString) throws JSONException {
    assertThat(jsonString, isJson());
    assertThat(jsonString, hasJsonPath("$.result"));
    assertThat(new JSONObject(jsonString)).isNotNull();
  }

  private void putDataInRegion(final String regionName, final Object[] portfolio, final int from,
      final int to) {
    Region region = managementTestRule.getCache().getRegion(regionName);
    for (int i = from; i < to; i++) {
      region.put(new Integer(i), portfolio[i]);
    }
  }

  private void generateValuesInRegions() {
    int COUNT_DESTINATION = 30;
    int COUNT_FROM = 0;

    // Create common Portfolios and NewPortfolios
    Portfolio[] portfolio = createPortfoliosAndPositions(COUNT_DESTINATION);

    // Fill local region
    memberVMs[0]
        .invoke(() -> putDataInRegion(LOCAL_REGION_NAME, portfolio, COUNT_FROM, COUNT_DESTINATION));

    // Fill replicated region
    memberVMs[0].invoke(
        () -> putDataInRegion(REPLICATE_REGION_NAME1, portfolio, COUNT_FROM, COUNT_DESTINATION));
    memberVMs[1].invoke(
        () -> putDataInRegion(REPLICATE_REGION_NAME2, portfolio, COUNT_FROM, COUNT_DESTINATION));

    // Fill Partition Region
    memberVMs[0].invoke(
        () -> putDataInRegion(PARTITIONED_REGION_NAME1, portfolio, COUNT_FROM, COUNT_DESTINATION));
    memberVMs[0].invoke(
        () -> putDataInRegion(PARTITIONED_REGION_NAME2, portfolio, COUNT_FROM, COUNT_DESTINATION));
    memberVMs[0].invoke(
        () -> putDataInRegion(PARTITIONED_REGION_NAME3, portfolio, COUNT_FROM, COUNT_DESTINATION));
    memberVMs[0].invoke(
        () -> putDataInRegion(PARTITIONED_REGION_NAME4, portfolio, COUNT_FROM, COUNT_DESTINATION));
    memberVMs[0].invoke(
        () -> putDataInRegion(PARTITIONED_REGION_NAME5, portfolio, COUNT_FROM, COUNT_DESTINATION));

    memberVMs[0].invoke(() -> putPdxInstances(REPLICATE_REGION_NAME3));
  }

  private void putPdxInstances(final String regionName) throws CacheException {
    InternalCache cache = (InternalCache) managementTestRule.getCache();
    Region region = cache.getRegion(regionName);

    PdxInstanceFactory pdxInstanceFactory =
        PdxInstanceFactoryImpl.newCreator("Portfolio", false, cache);
    pdxInstanceFactory.writeInt("ID", 111);
    pdxInstanceFactory.writeString("status", "active");
    pdxInstanceFactory.writeString("secId", "IBM");
    PdxInstance pdxInstance = pdxInstanceFactory.create();
    region.put("IBM", pdxInstance);

    pdxInstanceFactory = PdxInstanceFactoryImpl.newCreator("Portfolio", false, cache);
    pdxInstanceFactory.writeInt("ID", 222);
    pdxInstanceFactory.writeString("status", "inactive");
    pdxInstanceFactory.writeString("secId", "YHOO");
    pdxInstance = pdxInstanceFactory.create();
    region.put("YHOO", pdxInstance);

    pdxInstanceFactory = PdxInstanceFactoryImpl.newCreator("Portfolio", false, cache);
    pdxInstanceFactory.writeInt("ID", 333);
    pdxInstanceFactory.writeString("status", "active");
    pdxInstanceFactory.writeString("secId", "GOOGL");
    pdxInstance = pdxInstanceFactory.create();
    region.put("GOOGL", pdxInstance);

    pdxInstanceFactory = PdxInstanceFactoryImpl.newCreator("Portfolio", false, cache);
    pdxInstanceFactory.writeInt("ID", 111);
    pdxInstanceFactory.writeString("status", "inactive");
    pdxInstanceFactory.writeString("secId", "VMW");
    pdxInstance = pdxInstanceFactory.create();
    region.put("VMW", pdxInstance);
  }

  private void putBigInstances(final String regionName) {
    Region region = managementTestRule.getCache().getRegion(regionName);

    for (int i = 0; i < 1200; i++) {
      List<String> bigCollection = new ArrayList<>();
      for (int j = 0; j < 200; j++) {
        bigCollection.add(BIG_COLLECTION_ELEMENT_ + j);
      }
      region.put(BIG_COLLECTION_ + i, bigCollection);
    }
  }

  private void createLocalRegion() {
    managementTestRule.getCache().createRegionFactory(RegionShortcut.LOCAL)
        .create(LOCAL_REGION_NAME);
  }

  private void createReplicatedRegion() {
    managementTestRule.getCache().createRegionFactory(RegionShortcut.REPLICATE)
        .create(REPLICATE_REGION_NAME1);
  }

  private void createColocatedPR() {
    PartitionResolver testKeyBasedResolver = new TestPartitionResolver();
    managementTestRule.getCache().createRegionFactory(RegionShortcut.PARTITION)
        .setPartitionAttributes(new PartitionAttributesFactory().setTotalNumBuckets(NUM_OF_BUCKETS)
            .setPartitionResolver(testKeyBasedResolver).create())
        .create(PARTITIONED_REGION_NAME1);
    managementTestRule.getCache().createRegionFactory(RegionShortcut.PARTITION)
        .setPartitionAttributes(new PartitionAttributesFactory().setTotalNumBuckets(NUM_OF_BUCKETS)
            .setPartitionResolver(testKeyBasedResolver).setColocatedWith(PARTITIONED_REGION_NAME1)
            .create())
        .create(PARTITIONED_REGION_NAME2);
    managementTestRule.getCache().createRegionFactory(RegionShortcut.PARTITION)
        .setPartitionAttributes(new PartitionAttributesFactory().setTotalNumBuckets(NUM_OF_BUCKETS)
            .setPartitionResolver(testKeyBasedResolver).setColocatedWith(PARTITIONED_REGION_NAME2)
            .create())
        .create(PARTITIONED_REGION_NAME3);
    managementTestRule.getCache().createRegionFactory(RegionShortcut.PARTITION)
        .setPartitionAttributes(new PartitionAttributesFactory().setTotalNumBuckets(NUM_OF_BUCKETS)
            .setPartitionResolver(testKeyBasedResolver).create())
        .create(PARTITIONED_REGION_NAME4); // not collocated
    managementTestRule.getCache().createRegionFactory(RegionShortcut.PARTITION)
        .setPartitionAttributes(new PartitionAttributesFactory().setTotalNumBuckets(NUM_OF_BUCKETS)
            .setPartitionResolver(testKeyBasedResolver).setColocatedWith(PARTITIONED_REGION_NAME4)
            .create())
        .create(PARTITIONED_REGION_NAME5); // collocated with 4
  }

  private void createDistributedRegion(final String regionName) {
    managementTestRule.getCache().createRegionFactory(RegionShortcut.REPLICATE).create(regionName);
  }

  private void createRegionsInNodes()
      throws InterruptedException, TimeoutException, ExecutionException {
    // Create local Region on servers
    memberVMs[0].invoke(() -> createLocalRegion());

    // Create ReplicatedRegion on servers
    memberVMs[0].invoke(() -> createReplicatedRegion());
    memberVMs[1].invoke(() -> createReplicatedRegion());
    memberVMs[2].invoke(() -> createReplicatedRegion());

    memberVMs[1].invoke(() -> createDistributedRegion(REPLICATE_REGION_NAME2));
    memberVMs[0].invoke(() -> createDistributedRegion(REPLICATE_REGION_NAME3));
    memberVMs[0].invoke(() -> createDistributedRegion(REPLICATE_REGION_NAME4));

    // Create two co-located PartitionedRegions On Servers.
    memberVMs[0].invoke(() -> createColocatedPR());
    memberVMs[1].invoke(() -> createColocatedPR());
    memberVMs[2].invoke(() -> createColocatedPR());

    managerVM.invoke("Wait for all Region Proxies to get replicated", () -> {
      awaitDistributedRegionMXBean("/" + PARTITIONED_REGION_NAME1, 3);
      awaitDistributedRegionMXBean("/" + PARTITIONED_REGION_NAME2, 3);
      awaitDistributedRegionMXBean("/" + PARTITIONED_REGION_NAME3, 3);
      awaitDistributedRegionMXBean("/" + PARTITIONED_REGION_NAME4, 3);
      awaitDistributedRegionMXBean("/" + PARTITIONED_REGION_NAME5, 3);
      awaitDistributedRegionMXBean("/" + REPLICATE_REGION_NAME1, 3);
      awaitDistributedRegionMXBean("/" + REPLICATE_REGION_NAME2, 1);
      awaitDistributedRegionMXBean("/" + REPLICATE_REGION_NAME3, 1);
      awaitDistributedRegionMXBean("/" + REPLICATE_REGION_NAME4, 1);
    });
  }

  private List<String> getLocalDataSet(final String region) {
    PartitionedRegion partitionedRegion =
        PartitionedRegionHelper.getPartitionedRegion(region, managementTestRule.getCache());
    Set<BucketRegion> localPrimaryBucketRegions =
        partitionedRegion.getDataStore().getAllLocalPrimaryBucketRegions();

    List<String> allPrimaryValues = new ArrayList<>();

    for (BucketRegion bucketRegion : localPrimaryBucketRegions) {
      for (Object value : bucketRegion.values()) {
        allPrimaryValues.add((String) value);
      }
    }

    return allPrimaryValues;
  }

  private List<FixedPartitionAttributes> createFixedPartitionList(final int primaryIndex) {
    List<FixedPartitionAttributes> fixedPartitionAttributesList = new ArrayList<>();
    if (primaryIndex == 1) {
      fixedPartitionAttributesList.add(createFixedPartition("Q1", true, 3));
      fixedPartitionAttributesList.add(createFixedPartition("Q2", 3));
      fixedPartitionAttributesList.add(createFixedPartition("Q3", 3));
    }
    if (primaryIndex == 2) {
      fixedPartitionAttributesList.add(createFixedPartition("Q1", 3));
      fixedPartitionAttributesList.add(createFixedPartition("Q2", true, 3));
      fixedPartitionAttributesList.add(createFixedPartition("Q3", 3));
    }
    if (primaryIndex == 3) {
      fixedPartitionAttributesList.add(createFixedPartition("Q1", 3));
      fixedPartitionAttributesList.add(createFixedPartition("Q2", 3));
      fixedPartitionAttributesList.add(createFixedPartition("Q3", true, 3));
    }
    return fixedPartitionAttributesList;
  }

  private MemberMXBean awaitMemberMXBeanProxy(final DistributedMember member) {
    SystemManagementService service = managementTestRule.getSystemManagementService();
    ObjectName objectName = service.getMemberMBeanName(member);
    String alias = "awaiting MemberMXBean proxy for " + member;

    await(alias)
        .untilAsserted(
            () -> assertThat(service.getMBeanProxy(objectName, MemberMXBean.class)).isNotNull());

    return service.getMBeanProxy(objectName, MemberMXBean.class);
  }

  private DistributedSystemMXBean awaitDistributedSystemMXBean() {
    SystemManagementService service = managementTestRule.getSystemManagementService();

    await().untilAsserted(() -> assertThat(service.getDistributedSystemMXBean()).isNotNull());

    return service.getDistributedSystemMXBean();
  }

  private DistributedRegionMXBean awaitDistributedRegionMXBean(final String name) {
    SystemManagementService service = managementTestRule.getSystemManagementService();

    await().untilAsserted(() -> assertThat(service.getDistributedRegionMXBean(name)).isNotNull());

    return service.getDistributedRegionMXBean(name);
  }

  private DistributedRegionMXBean awaitDistributedRegionMXBean(final String name,
      final int memberCount) {
    SystemManagementService service = managementTestRule.getSystemManagementService();

    await().untilAsserted(() -> assertThat(service.getDistributedRegionMXBean(name)).isNotNull());
    await()
        .untilAsserted(() -> assertThat(service.getDistributedRegionMXBean(name).getMemberCount())
            .isEqualTo(memberCount));

    return service.getDistributedRegionMXBean(name);
  }

  private static class TestPartitionResolver implements PartitionResolver {

    @Override
    public void close() {}

    @Override
    public Serializable getRoutingObject(EntryOperation opDetails) {
      return ((Integer) opDetails.getKey() % NUM_OF_BUCKETS);
    }

    @Override
    public String getName() {
      return getClass().getName();
    }
  }
}
