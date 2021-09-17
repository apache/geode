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
package org.apache.geode.cache.query;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.Calendar;
import java.util.Date;

import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.test.junit.categories.OQLQueryTest;
import org.apache.geode.test.junit.rules.ServerStarterRule;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@Category(OQLQueryTest.class)
@RunWith(GeodeParamsRunner.class)
public class JoinQueriesIntegrationTest {

  @Rule
  public ServerStarterRule serverRule = new ServerStarterRule().withAutoStart();

  @SuppressWarnings("unused")
  public static class Customer implements Serializable {
    int pkid;
    int id;
    int joinId;
    String name;

    public int getPkid() {
      return pkid;
    }

    public int getId() {
      return id;
    }

    public int getJoinId() {
      return joinId;
    }

    public String getName() {
      return name;
    }

    Customer(int pkid, int id, int joinId) {
      this.pkid = pkid;
      this.id = id;
      this.joinId = joinId;
      this.name = "name" + pkid;
    }

    public String toString() {
      return "Customer pkid = " + getPkid() + ", id: " + getId() + " name:" + getName()
          + " joinId: " + getJoinId();
    }
  }

  @SuppressWarnings("unused")
  public static class Order implements Serializable {
    private final String orderId;
    private final Integer version;

    public String getOrderId() {
      return orderId;
    }

    public Integer getVersion() {
      return version;
    }

    Order(String orderId, Integer version) {
      this.orderId = orderId;
      this.version = version;
    }
  }

  @SuppressWarnings("unused")
  public static class ValidationIssue implements Serializable {
    private final String issueId;
    private final Date createdTime;

    public String getIssueId() {
      return issueId;
    }

    public Date getCreatedTime() {
      return createdTime;
    }

    ValidationIssue(String issueId, Date createdTime) {
      this.issueId = issueId;
      this.createdTime = createdTime;
    }
  }

  @SuppressWarnings("unused")
  public static class OrderValidationIssueXRef implements Serializable {
    private final String referenceOrderId;
    private final String referenceIssueId;
    private final String validationIssueXRefID;
    private final Integer referenceOrderVersion;

    public String getReferenceOrderId() {
      return referenceOrderId;
    }

    public String getReferenceIssueId() {
      return referenceIssueId;
    }

    public String getValidationIssueXRefID() {
      return validationIssueXRefID;
    }

    public Integer getReferenceOrderVersion() {
      return referenceOrderVersion;
    }

    OrderValidationIssueXRef(String validationIssueXRefID, String referenceIssueId,
        String referenceOrderId, Integer referenceOrderVersion) {
      this.validationIssueXRefID = validationIssueXRefID;
      this.referenceIssueId = referenceIssueId;
      this.referenceOrderId = referenceOrderId;
      this.referenceOrderVersion = referenceOrderVersion;
    }
  }

  @SuppressWarnings("unused")
  private static Object[] getQueryStrings() {
    return new Object[] {
        new Object[] {
            "<trace>select STA.id as STACID, STA.pkid as STAacctNum, STC.id as STCCID, STC.pkid as STCacctNum from "
                + SEPARATOR + "region1 STA, " + SEPARATOR
                + "region2 STC where STA.pkid = 1 AND STA.joinId = STC.joinId AND STA.id = STC.id",
            20},
        new Object[] {
            "<trace>select STA.id as STACID, STA.pkid as STAacctNum, STC.id as STCCID, STC.pkid as STCacctNum from "
                + SEPARATOR + "region1 STA, " + SEPARATOR
                + "region2 STC where STA.pkid = 1 AND STA.joinId = STC.joinId OR STA.id = STC.id",
            22}};
  }

  private void populateCustomerRegionsWithData(Region<Integer, Customer> region1,
      Region<Integer, Customer> region2) {
    for (int i = 1; i < 11; i++) {
      if (i == 1 || i == 3 || i == 8 || i == 2 || i == 5) {
        region1.put(i, new Customer(1, 1, 1));
      } else {
        region1.put(i, new Customer(i, i, i));
      }
      if (i == 1 || i == 4 || i == 7 || i == 10) {
        region2.put(i, new Customer(1, 1, 1));
      } else {
        region2.put(i, new Customer(i % 5, i, i % 3));
      }
    }
  }

  @Test
  @Parameters(method = "getQueryStrings")
  public void testJoinTwoRegions(String queryString, int expectedResultSize) throws Exception {
    InternalCache cache = serverRule.getCache();
    QueryService queryService = cache.getQueryService();
    Region<Integer, Customer> region1 = cache.<Integer, Customer>createRegionFactory()
        .setDataPolicy(DataPolicy.REPLICATE).create("region1");
    Region<Integer, Customer> region2 = cache.<Integer, Customer>createRegionFactory()
        .setDataPolicy(DataPolicy.REPLICATE).create("region2");
    populateCustomerRegionsWithData(region1, region2);

    SelectResults results = (SelectResults) queryService.newQuery(queryString).execute();
    int resultsWithoutIndex = results.size();
    assertThat(resultsWithoutIndex).isEqualTo(expectedResultSize);

    queryService.createIndex("pkidregion1", "p.pkid", SEPARATOR + "region1 p");
    queryService.createIndex("pkidregion2", "p.pkid", SEPARATOR + "region2 p");
    queryService.createIndex("indexIDRegion2", "p.id", SEPARATOR + "region2 p");
    queryService.createIndex("indexIDRegion1", "p.id", SEPARATOR + "region1 p");
    queryService.createIndex("joinIdregion1", "p.joinId", SEPARATOR + "region1 p");
    queryService.createIndex("joinIdregion2", "p.joinId", SEPARATOR + "region2 p");
    queryService.createIndex("nameIndex", "p.name", SEPARATOR + "region2 p");

    results = (SelectResults) queryService.newQuery(queryString).execute();
    int resultsSizeWithIndex = results.size();
    assertThat(resultsWithoutIndex).isEqualTo(expectedResultSize);
    assertThat(resultsWithoutIndex).isEqualTo(resultsSizeWithIndex);
  }

  private void populateTripleJointRegionsWithSerializables(
      int expectedMatches, int extraEntitiesPerRegion,
      Region<String, Object> orderRegion, Region<String, Object> validationIssueRegion,
      Region<String, Object> validationIssueXRefRegion) {
    for (int i = 0; i < expectedMatches; i++) {
      orderRegion.put("orderId_" + i, new Order("orderId_" + i, i));
      validationIssueRegion.put("validationIssueID_" + i,
          new ValidationIssue("issueId_" + i, Calendar.getInstance().getTime()));
      validationIssueXRefRegion.put("validationIssueXRefID_" + i, new OrderValidationIssueXRef(
          "validationIssueXRefID_" + i, "issueId_" + i, "orderId_" + i, i));
    }

    for (int i = 0; i < extraEntitiesPerRegion; i++) {
      orderRegion.put("orderId#" + i, new Order("orderId#" + i, i));
      validationIssueRegion.put("referenceIssueId#" + i,
          new ValidationIssue("referenceIssueId#" + i, Calendar.getInstance().getTime()));
      validationIssueXRefRegion.put("validationIssueXRefID#" + i, new OrderValidationIssueXRef(
          "validationIssueXRefID#" + i, "validationIssueID2#" + i, "orderId#2" + i, i));
    }
  }

  private void populateTripleJointRegionsWithPdxInstances(
      int expectedMatches, int extraEntitiesPerRegion,
      Cache cache, Region<String, Object> orderRegion, Region<String, Object> validationIssueRegion,
      Region<String, Object> validationIssueXRefRegion) {
    for (int i = 0; i < expectedMatches; i++) {
      PdxInstance orderPdx = cache
          .createPdxInstanceFactory("org.apache.geode.test.Order")
          .writeString("orderId", "orderId_" + i)
          .writeInt("version", i)
          .create();
      PdxInstance validationIssuePdx = cache
          .createPdxInstanceFactory("org.apache.geode.test.ValidationIssue")
          .writeString("issueId", "issueId_" + i)
          .writeDate("createdTime", Calendar.getInstance().getTime())
          .create();
      PdxInstance validationIssueXRefPdx = cache
          .createPdxInstanceFactory("org.apache.geode.test.OrderValidationIssueXRef")
          .writeString("validationIssueXRefID", "validationIssueXRefID_" + i)
          .writeString("referenceIssueId", "issueId_" + i)
          .writeString("referenceOrderId", "orderId_" + i)
          .writeInt("referenceOrderVersion", i)
          .create();

      orderRegion.put("orderId_" + i, orderPdx);
      validationIssueRegion.put("issueId_" + i, validationIssuePdx);
      validationIssueXRefRegion.put("validationIssueXRefID_" + i, validationIssueXRefPdx);
    }

    for (int i = 0; i < extraEntitiesPerRegion; i++) {
      PdxInstance orderPdx = cache
          .createPdxInstanceFactory("org.apache.geode.test.Order")
          .writeString("orderId", "orderId#" + i)
          .writeInt("version", i)
          .create();
      PdxInstance validationIssuePdx = cache
          .createPdxInstanceFactory("org.apache.geode.test.ValidationIssue")
          .writeString("referenceIssueId", "referenceIssueId#" + i)
          .writeDate("createdTime", Calendar.getInstance().getTime())
          .create();
      PdxInstance validationIssueXRefPdx = cache
          .createPdxInstanceFactory("org.apache.geode.test.OrderValidationIssueXRef")
          .writeString("validationIssueXRefID", "validationIssueXRefID#" + i)
          .writeString("referenceIssueId", "validationIssueID2#" + i)
          .writeString("referenceOrderId", "orderId#2" + i)
          .writeInt("referenceOrderVersion", i)
          .create();

      orderRegion.put("orderId#" + i, orderPdx);
      validationIssueRegion.put("issueId#" + i, validationIssuePdx);
      validationIssueXRefRegion.put("validationIssueXRefID#" + i, validationIssueXRefPdx);
    }
  }

  @Test
  @Parameters({"true", "false"})
  @TestCaseName("{method} - Using PDX: {params}")
  public void joiningThreeRegionsWhenIntermediateResultSizeIsHigherThanLimitClauseShouldNotTrimResults(
      boolean usePdx) throws Exception {
    int matches = 10;
    int extraEntitiesPerRegion = 25;
    InternalCache cache = serverRule.getCache();
    QueryService queryService = cache.getQueryService();
    String queryString = "SELECT issue.issueId, issue.createdTime, o.orderId, o.version "
        + "FROM " + SEPARATOR + "ValidationIssue issue, " + SEPARATOR
        + "OrderValidationIssueXRef xRef, " + SEPARATOR + "Order o "
        + "WHERE "
        + "issue.issueId = xRef.referenceIssueId "
        + "AND "
        + "xRef.referenceOrderId = o.orderId "
        + "AND "
        + "xRef.referenceOrderVersion = o.version ";

    // Create Regions
    Region<String, Object> orderRegion = cache.<String, Object>createRegionFactory()
        .setDataPolicy(DataPolicy.REPLICATE).create("Order");
    Region<String, Object> validationIssueRegion = cache.<String, Object>createRegionFactory()
        .setDataPolicy(DataPolicy.REPLICATE).create("ValidationIssue");
    Region<String, Object> validationIssueXRefRegion = cache.<String, Object>createRegionFactory()
        .setDataPolicy(DataPolicy.REPLICATE).create("OrderValidationIssueXRef");

    if (!usePdx) {
      populateTripleJointRegionsWithSerializables(matches, extraEntitiesPerRegion, orderRegion,
          validationIssueRegion, validationIssueXRefRegion);
    } else {
      populateTripleJointRegionsWithPdxInstances(matches, extraEntitiesPerRegion, cache,
          orderRegion,
          validationIssueRegion, validationIssueXRefRegion);
    }

    SelectResults baseResults = (SelectResults) queryService.newQuery(queryString).execute();
    SelectResults resultsWithLimitOne =
        (SelectResults) queryService.newQuery(queryString + "LIMIT 2").execute();
    SelectResults resultsWithLimitTwo =
        (SelectResults) queryService.newQuery(queryString + "LIMIT 5").execute();
    assertThat(baseResults.size()).isEqualTo(matches);
    assertThat(resultsWithLimitOne.size()).isEqualTo(2);
    assertThat(resultsWithLimitTwo.size()).isEqualTo(5);
  }


  @Test
  @Parameters({"true", "false"})
  @TestCaseName("{method} - Using PDX: {params}")
  public void joiningThreeRegionsWithIndexesWhenIntermediateResultSizeIsHigherThanLimitClauseShouldNotTrimResults(
      boolean usePdx) throws Exception {
    int matches = 10;
    int extraEntitiesPerRegion = 25;
    InternalCache cache = serverRule.getCache();
    QueryService queryService = cache.getQueryService();
    String queryString = "SELECT issue.issueId, issue.createdTime, o.orderId, o.version "
        + "FROM " + SEPARATOR + "ValidationIssue issue, " + SEPARATOR
        + "OrderValidationIssueXRef xRef, " + SEPARATOR + "Order o "
        + "WHERE "
        + "issue.issueId = xRef.referenceIssueId "
        + "AND "
        + "xRef.referenceOrderId = o.orderId "
        + "AND "
        + "xRef.referenceOrderVersion = o.version ";

    // Create Regions
    Region<String, Object> orderRegion = cache.<String, Object>createRegionFactory()
        .setDataPolicy(DataPolicy.REPLICATE).create("Order");
    Region<String, Object> validationIssueRegion = cache.<String, Object>createRegionFactory()
        .setDataPolicy(DataPolicy.REPLICATE).create("ValidationIssue");
    Region<String, Object> validationIssueXRefRegion = cache.<String, Object>createRegionFactory()
        .setDataPolicy(DataPolicy.REPLICATE).create("OrderValidationIssueXRef");

    // Populate Regions
    if (!usePdx) {
      populateTripleJointRegionsWithSerializables(matches, extraEntitiesPerRegion, orderRegion,
          validationIssueRegion, validationIssueXRefRegion);
    } else {
      populateTripleJointRegionsWithPdxInstances(matches, extraEntitiesPerRegion, cache,
          orderRegion,
          validationIssueRegion, validationIssueXRefRegion);
    }

    // Create Indexes
    cache.getQueryService().createIndex("order_orderID", "orderId", SEPARATOR + "Order", null);
    cache.getQueryService().createIndex("validationIssue_issueID", "issueId",
        SEPARATOR + "ValidationIssue",
        null);
    cache.getQueryService().createIndex("orderValidationIssueXRef_referenceOrderId",
        "referenceOrderId", SEPARATOR + "OrderValidationIssueXRef", null);
    cache.getQueryService().createIndex("orderValidationIssueXRef_referenceIssueId",
        "referenceIssueId", SEPARATOR + "OrderValidationIssueXRef", null);

    SelectResults baseResultsWithIndexes =
        (SelectResults) queryService.newQuery(queryString).execute();
    SelectResults resultsWithLimitOneWithIndexes =
        (SelectResults) queryService.newQuery(queryString + "LIMIT 2").execute();
    SelectResults resultsWithLimitTwoWithIndexes =
        (SelectResults) queryService.newQuery(queryString + "LIMIT 5").execute();
    assertThat(baseResultsWithIndexes.size()).isEqualTo(matches);
    assertThat(resultsWithLimitOneWithIndexes.size()).isEqualTo(2);
    assertThat(resultsWithLimitTwoWithIndexes.size()).isEqualTo(5);
  }
}
