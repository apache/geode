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
/*
 * Created on Feb 24, 2005
 *
 *
 */
package org.apache.geode.cache.query.functional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.CacheUtils;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.Struct;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.internal.CompiledSelect;
import org.apache.geode.cache.query.internal.DefaultQuery;
import org.apache.geode.cache.query.internal.aggregate.AbstractAggregator;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.cache.query.types.StructType;

/**
 * Tests the group by queries with or without aggreagte functions
 *
 *
 *
 */
public abstract class GroupByTestImpl implements GroupByTestInterface {

  String queries[] = {"select  p.status as status, p.ID from /portfolio p where p.ID > 0 "// ResultSet

  };

  public abstract Region createRegion(String regionName, Class constraint);

  @Test
  public void testConvertibleGroupByQuery_1() throws Exception {
    Region region = this.createRegion("portfolio", Portfolio.class);
    for (int i = 1; i < 200; ++i) {
      Portfolio pf = new Portfolio(i);
      pf.shortID = (short) ((short) i / 5);
      region.put("" + i, pf);
    }
    String queryStr =
        "select  p.status as status, p.ID from /portfolio p where p.ID > 0 group by status, p.ID ";
    QueryService qs = CacheUtils.getQueryService();
    Query query = qs.newQuery(queryStr);
    CompiledSelect cs = ((DefaultQuery) query).getSelect();
    assertTrue(cs.isDistinct());
    assertTrue(cs.isOrderBy());
    assertFalse(cs.isGroupBy());
  }

  @Test
  public void testConvertibleGroupByQuery_refer_column() throws Exception {
    Region region = this.createRegion("portfolio", Portfolio.class);
    for (int i = 1; i < 200; ++i) {
      Portfolio pf = new Portfolio(i);
      pf.shortID = (short) ((short) i / 5);
      region.put("" + i, pf);
    }
    String queryStr = "select  p.shortID  from /portfolio p where p.ID >= 0 group by p.shortID ";
    QueryService qs = CacheUtils.getQueryService();
    Query query = qs.newQuery(queryStr);
    SelectResults<Short> results = (SelectResults<Short>) query.execute();
    Iterator<Short> iter = results.asList().iterator();
    int counter = 0;
    while (iter.hasNext()) {
      Short shortID = iter.next();
      assertEquals(counter++, shortID.intValue());
    }
    assertEquals(39, counter - 1);
  }

  @Test
  public void testConvertibleGroupByQuery_refer_column_alias_Bug520141() throws Exception {
    Region region = this.createRegion("portfolio", Portfolio.class);
    for (int i = 1; i < 200; ++i) {
      Portfolio pf = new Portfolio(i);
      pf.shortID = (short) ((short) i / 5);
      region.put("" + i, pf);
    }
    String queryStr =
        "select  p.shortID as short_id  from /portfolio p where p.ID >= 0 group by short_id ";
    QueryService qs = CacheUtils.getQueryService();
    Query query = qs.newQuery(queryStr);
    SelectResults<Struct> results = (SelectResults<Struct>) query.execute();
    Iterator<Struct> iter = results.asList().iterator();
    int counter = 0;
    while (iter.hasNext()) {
      Struct str = iter.next();
      assertEquals(counter++, ((Short) str.get("short_id")).intValue());
    }
    assertEquals(39, counter - 1);
  }

  @Test
  public void testAggregateFuncCountStar() throws Exception {
    Region region = this.createRegion("portfolio", Portfolio.class);
    for (int i = 1; i < 200; ++i) {
      Portfolio pf = new Portfolio(i);
      pf.shortID = (short) ((short) i % 5);
      region.put("" + i, pf);
    }
    String queryStr =
        "select  p.status as status, Count(*) as countt from /portfolio p where p.ID > 0 group by status ";
    QueryService qs = CacheUtils.getQueryService();
    Query query = qs.newQuery(queryStr);
    CompiledSelect cs = ((DefaultQuery) query).getSelect();
    SelectResults sr = (SelectResults) query.execute();
    assertTrue(sr.getCollectionType().getElementType().isStructType());
    assertEquals(2, sr.size());
    Iterator iter = sr.iterator();
    Region rgn = CacheUtils.getRegion("portfolio");
    int activeCount = 0;
    int inactiveCount = 0;
    for (Object o : rgn.values()) {
      Portfolio pf = (Portfolio) o;
      if (pf.status.equals("active")) {
        ++activeCount;
      } else if (pf.status.equals("inactive")) {
        ++inactiveCount;
      } else {
        fail("unexpected status");
      }
    }

    while (iter.hasNext()) {
      Struct struct = (Struct) iter.next();
      StructType structType = struct.getStructType();
      ObjectType[] fieldTypes = structType.getFieldTypes();
      assertEquals("String", fieldTypes[0].getSimpleClassName());
      assertEquals("Integer", fieldTypes[1].getSimpleClassName());
      if (struct.get("status").equals("active")) {
        assertEquals(activeCount, ((Integer) struct.get("countt")).intValue());
      } else if (struct.get("status").equals("inactive")) {
        assertEquals(inactiveCount, ((Integer) struct.get("countt")).intValue());
      } else {
        fail("unexpected value of status");
      }
    }
    ObjectType elementType = sr.getCollectionType().getElementType();
    assertTrue(elementType.isStructType());
    StructType structType = (StructType) elementType;
    ObjectType[] fieldTypes = structType.getFieldTypes();
    assertEquals("String", fieldTypes[0].getSimpleClassName());
    assertEquals("Integer", fieldTypes[1].getSimpleClassName());

  }

  @Test
  public void testAggregateFuncCountDistinctStar_1() throws Exception {
    Region region = this.createRegion("portfolio", Portfolio.class);
    for (int i = 1; i < 200; ++i) {
      Portfolio pf = new Portfolio(i);
      pf.shortID = (short) ((short) i % 5);
      region.put("" + i, pf);
    }
    String queryStr =
        "select  p.status as status, count(distinct p.shortID) as countt from /portfolio p where p.ID > 0 group by status ";
    QueryService qs = CacheUtils.getQueryService();
    Query query = qs.newQuery(queryStr);
    CompiledSelect cs = ((DefaultQuery) query).getSelect();
    SelectResults sr = (SelectResults) query.execute();
    assertTrue(sr.getCollectionType().getElementType().isStructType());
    assertEquals(2, sr.size());
    Iterator iter = sr.iterator();
    Region rgn = CacheUtils.getRegion("portfolio");
    Set<Object> distinctShortIDActive = new HashSet<Object>();
    Set<Object> distinctShortIDInactive = new HashSet<Object>();
    int inactiveCount = 0;
    for (Object o : rgn.values()) {
      Portfolio pf = (Portfolio) o;
      if (pf.status.equals("active")) {
        distinctShortIDActive.add(pf.shortID);
      } else if (pf.status.equals("inactive")) {
        distinctShortIDInactive.add(pf.shortID);
      } else {
        fail("unexpected status");
      }
    }

    while (iter.hasNext()) {
      Struct struct = (Struct) iter.next();
      StructType structType = struct.getStructType();
      ObjectType[] fieldTypes = structType.getFieldTypes();
      assertEquals("String", fieldTypes[0].getSimpleClassName());
      assertEquals("Integer", fieldTypes[1].getSimpleClassName());
      if (struct.get("status").equals("active")) {
        assertEquals(distinctShortIDActive.size(), ((Integer) struct.get("countt")).intValue());
      } else if (struct.get("status").equals("inactive")) {
        assertEquals(distinctShortIDInactive.size(), ((Integer) struct.get("countt")).intValue());
      } else {
        fail("unexpected value of status");
      }
    }

    ObjectType elementType = sr.getCollectionType().getElementType();
    assertTrue(elementType.isStructType());
    StructType structType = (StructType) elementType;
    ObjectType[] fieldTypes = structType.getFieldTypes();
    assertEquals("String", fieldTypes[0].getSimpleClassName());
    assertEquals("Integer", fieldTypes[1].getSimpleClassName());
  }

  @Test
  public void testAggregateFuncCountDistinctStar_2() throws Exception {
    Region region = this.createRegion("portfolio", Portfolio.class);
    for (int i = 1; i < 200; ++i) {
      Portfolio pf = new Portfolio(i);
      pf.shortID = (short) ((short) i / 5);
      region.put("" + i, pf);
    }
    String queryStr =
        "select  p.status as status, COUNT(distinct p.shortID) as countt from /portfolio p where p.ID > 0 group by status ";
    QueryService qs = CacheUtils.getQueryService();
    Query query = qs.newQuery(queryStr);
    CompiledSelect cs = ((DefaultQuery) query).getSelect();
    SelectResults sr = (SelectResults) query.execute();
    assertTrue(sr.getCollectionType().getElementType().isStructType());
    assertEquals(2, sr.size());
    Iterator iter = sr.iterator();
    Region rgn = CacheUtils.getRegion("portfolio");
    Set<Object> distinctShortIDActive = new HashSet<Object>();
    Set<Object> distinctShortIDInactive = new HashSet<Object>();
    int inactiveCount = 0;
    for (Object o : rgn.values()) {
      Portfolio pf = (Portfolio) o;
      if (pf.status.equals("active")) {
        distinctShortIDActive.add(pf.shortID);
      } else if (pf.status.equals("inactive")) {
        distinctShortIDInactive.add(pf.shortID);
      } else {
        fail("unexpected status");
      }
    }

    while (iter.hasNext()) {
      Struct struct = (Struct) iter.next();
      StructType structType = struct.getStructType();
      ObjectType[] fieldTypes = structType.getFieldTypes();
      assertEquals("String", fieldTypes[0].getSimpleClassName());
      assertEquals("Integer", fieldTypes[1].getSimpleClassName());
      if (struct.get("status").equals("active")) {
        assertEquals(distinctShortIDActive.size(), ((Integer) struct.get("countt")).intValue());
      } else if (struct.get("status").equals("inactive")) {
        assertEquals(distinctShortIDInactive.size(), ((Integer) struct.get("countt")).intValue());
      } else {
        fail("unexpected value of status");
      }
    }

    ObjectType elementType = sr.getCollectionType().getElementType();
    assertTrue(elementType.isStructType());
    StructType structType = (StructType) elementType;
    ObjectType[] fieldTypes = structType.getFieldTypes();
    assertEquals("String", fieldTypes[0].getSimpleClassName());
    assertEquals("Integer", fieldTypes[1].getSimpleClassName());
  }

  @Test
  public void testAggregateFuncSum() throws Exception {
    Region region = this.createRegion("portfolio", Portfolio.class);
    for (int i = 1; i < 200; ++i) {
      Portfolio pf = new Portfolio(i);
      pf.shortID = (short) ((short) i / 5);
      region.put("" + i, pf);
    }
    String queryStr =
        "select  p.status as status, Sum(p.ID) as summ from /portfolio p where p.ID > 0 group by status ";
    QueryService qs = CacheUtils.getQueryService();
    Query query = qs.newQuery(queryStr);
    CompiledSelect cs = ((DefaultQuery) query).getSelect();
    SelectResults sr = (SelectResults) query.execute();
    assertTrue(sr.getCollectionType().getElementType().isStructType());
    assertEquals(2, sr.size());
    Iterator iter = sr.iterator();
    Region rgn = CacheUtils.getRegion("portfolio");
    int activeSum = 0;
    int inactiveSum = 0;
    for (Object o : rgn.values()) {
      Portfolio pf = (Portfolio) o;
      if (pf.status.equals("active")) {
        activeSum += pf.ID;
      } else if (pf.status.equals("inactive")) {
        inactiveSum += pf.ID;
      } else {
        fail("unexpected value of status");
      }
    }

    while (iter.hasNext()) {
      Struct struct = (Struct) iter.next();
      StructType structType = struct.getStructType();
      ObjectType[] fieldTypes = structType.getFieldTypes();
      assertEquals("String", fieldTypes[0].getSimpleClassName());
      assertEquals("Number", fieldTypes[1].getSimpleClassName());

      if (struct.get("status").equals("active")) {
        assertEquals(activeSum, ((Integer) struct.get("summ")).intValue());
      } else if (struct.get("status").equals("inactive")) {
        assertEquals(inactiveSum, ((Integer) struct.get("summ")).intValue());
      } else {
        fail("unexpected value of status");
      }
    }

    ObjectType elementType = sr.getCollectionType().getElementType();
    assertTrue(elementType.isStructType());
    StructType structType = (StructType) elementType;
    ObjectType[] fieldTypes = structType.getFieldTypes();
    assertEquals("String", fieldTypes[0].getSimpleClassName());
    assertEquals("Number", fieldTypes[1].getSimpleClassName());
  }

  @Test
  public void testAggregateFuncSumDistinct() throws Exception {
    Region region = this.createRegion("portfolio", Portfolio.class);
    for (int i = 1; i < 200; ++i) {
      Portfolio pf = new Portfolio(i);
      pf.shortID = (short) ((short) i / 5);
      region.put("" + i, pf);
    }
    String queryStr =
        "select  p.status as status, SUM (distinct p.shortID) as summ from /portfolio p where p.ID > 0 group by status ";
    QueryService qs = CacheUtils.getQueryService();
    Query query = qs.newQuery(queryStr);
    CompiledSelect cs = ((DefaultQuery) query).getSelect();
    SelectResults sr = (SelectResults) query.execute();
    assertTrue(sr.getCollectionType().getElementType().isStructType());
    assertEquals(2, sr.size());
    Iterator iter = sr.iterator();
    Region rgn = CacheUtils.getRegion("portfolio");
    Set<Short> activeSum = new HashSet<Short>();
    Set<Short> inactiveSum = new HashSet<Short>();
    for (Object o : rgn.values()) {
      Portfolio pf = (Portfolio) o;
      if (pf.status.equals("active")) {
        activeSum.add(pf.shortID);
      } else if (pf.status.equals("inactive")) {
        inactiveSum.add(pf.shortID);
      } else {
        fail("unexpected value of status");
      }
    }

    int activeSumm = 0, inactiveSumm = 0;
    for (Short val : activeSum) {
      activeSumm += val.intValue();
    }

    for (Short val : inactiveSum) {
      inactiveSumm += val.intValue();
    }

    while (iter.hasNext()) {
      Struct struct = (Struct) iter.next();
      StructType structType = struct.getStructType();
      ObjectType[] fieldTypes = structType.getFieldTypes();
      assertEquals("String", fieldTypes[0].getSimpleClassName());
      assertEquals("Number", fieldTypes[1].getSimpleClassName());
      if (struct.get("status").equals("active")) {
        assertEquals(activeSumm, ((Integer) struct.get("summ")).intValue());
      } else if (struct.get("status").equals("inactive")) {
        assertEquals(inactiveSumm, ((Integer) struct.get("summ")).intValue());
      } else {
        fail("unexpected value of status");
      }
    }

    ObjectType elementType = sr.getCollectionType().getElementType();
    assertTrue(elementType.isStructType());
    StructType structType = (StructType) elementType;
    ObjectType[] fieldTypes = structType.getFieldTypes();
    assertEquals("String", fieldTypes[0].getSimpleClassName());
    assertEquals("Number", fieldTypes[1].getSimpleClassName());
  }

  @Test
  public void testAggregateFuncNoGroupBy() throws Exception {
    Region region = this.createRegion("portfolio", Portfolio.class);
    for (int i = 1; i < 200; ++i) {
      Portfolio pf = new Portfolio(i);
      pf.shortID = (short) ((short) i / 5);
      region.put("" + i, pf);
    }
    String queryStr = "select   sum(p.ID) as summ , Max(p.ID) as maxx, min(p.ID) as minn,"
        + " avg(p.ID) as average from /portfolio p where p.ID > 0 ";
    QueryService qs = CacheUtils.getQueryService();
    Query query = qs.newQuery(queryStr);
    CompiledSelect cs = ((DefaultQuery) query).getSelect();
    SelectResults sr = (SelectResults) query.execute();
    assertTrue(sr.getCollectionType().getElementType().isStructType());
    assertEquals(1, sr.size());
    Iterator iter = sr.iterator();
    Region rgn = CacheUtils.getRegion("portfolio");
    int sum = 0;
    int max = Integer.MIN_VALUE;
    int min = Integer.MAX_VALUE;
    for (Object o : rgn.values()) {
      Portfolio pf = (Portfolio) o;
      sum += pf.ID;
      if (pf.ID > max) {
        max = pf.ID;
      }
      if (pf.ID < min) {
        min = pf.ID;
      }
    }

    float avg = sum / rgn.size();

    while (iter.hasNext()) {
      Struct struct = (Struct) iter.next();
      assertEquals(sum, ((Integer) struct.get("summ")).intValue());
      assertEquals(max, ((Integer) struct.get("maxx")).intValue());
      assertEquals(min, ((Integer) struct.get("minn")).intValue());
      assertEquals(avg, ((Number) struct.get("average")).floatValue(), 0.0f);
    }
  }

  @Test
  public void testAggregateFuncAvg() throws Exception {
    Region region = this.createRegion("portfolio", Portfolio.class);
    for (int i = 1; i < 200; ++i) {
      Portfolio pf = new Portfolio(i);
      pf.shortID = (short) ((short) i / 5);
      region.put("" + i, pf);
    }
    String queryStr = "select   p.status as status,  Avg(p.ID) as average from "
        + "/portfolio p where p.ID > 0 group by status";
    QueryService qs = CacheUtils.getQueryService();
    Query query = qs.newQuery(queryStr);
    CompiledSelect cs = ((DefaultQuery) query).getSelect();
    SelectResults sr = (SelectResults) query.execute();
    assertTrue(sr.getCollectionType().getElementType().isStructType());
    assertEquals(2, sr.size());
    Iterator iter = sr.iterator();
    Region rgn = CacheUtils.getRegion("portfolio");
    double sumIDActive = 0, sumIDInactive = 0;
    int numActive = 0, numInactive = 0;
    for (Object o : rgn.values()) {
      Portfolio pf = (Portfolio) o;
      if (pf.ID > 0) {
        if (pf.status.equals("active")) {
          sumIDActive += pf.ID;
          ++numActive;
        } else if (pf.status.equals("inactive")) {
          sumIDInactive += pf.ID;
          ++numInactive;
        }
      }
    }

    Number avgActive = AbstractAggregator.downCast(sumIDActive / numActive);
    Number avgInactive = AbstractAggregator.downCast(sumIDInactive / numInactive);

    while (iter.hasNext()) {
      Struct struct = (Struct) iter.next();
      StructType structType = struct.getStructType();
      ObjectType[] fieldTypes = structType.getFieldTypes();
      assertEquals("String", fieldTypes[0].getSimpleClassName());
      assertEquals("Number", fieldTypes[1].getSimpleClassName());
      if (struct.get("status").equals("active")) {
        assertEquals(avgActive, struct.get("average"));
      } else if (struct.get("status").equals("inactive")) {
        assertEquals(avgInactive, struct.get("average"));
      } else {
        fail("unexpected value of status");
      }
    }

    ObjectType elementType = sr.getCollectionType().getElementType();
    assertTrue(elementType.isStructType());
    StructType structType = (StructType) elementType;
    ObjectType[] fieldTypes = structType.getFieldTypes();
    assertEquals("String", fieldTypes[0].getSimpleClassName());
    assertEquals("Number", fieldTypes[1].getSimpleClassName());
  }

  @Test
  public void testAggregateFuncAvgDistinct() throws Exception {
    Region region = this.createRegion("portfolio", Portfolio.class);
    for (int i = 1; i < 200; ++i) {
      Portfolio pf = new Portfolio(i);
      pf.shortID = (short) ((short) i / 5);
      region.put("" + i, pf);
    }
    String queryStr = "select   p.status as status,  avg(distinct p.shortID) as average from "
        + "/portfolio p where p.ID > 0 group by status";
    QueryService qs = CacheUtils.getQueryService();
    Query query = qs.newQuery(queryStr);
    CompiledSelect cs = ((DefaultQuery) query).getSelect();
    SelectResults sr = (SelectResults) query.execute();
    assertTrue(sr.getCollectionType().getElementType().isStructType());
    assertEquals(2, sr.size());
    Iterator iter = sr.iterator();
    Region rgn = CacheUtils.getRegion("portfolio");
    Set<Short> sumIDActiveSet = new HashSet<Short>(), sumIDInactiveSet = new HashSet<Short>();

    for (Object o : rgn.values()) {
      Portfolio pf = (Portfolio) o;
      if (pf.ID > 0) {
        if (pf.status.equals("active")) {
          sumIDActiveSet.add(pf.shortID);

        } else if (pf.status.equals("inactive")) {
          sumIDInactiveSet.add(pf.shortID);
        }
      }
    }
    double sumActive = 0, sumInactive = 0;
    for (Short shortt : sumIDActiveSet) {
      sumActive += shortt.doubleValue();
    }

    for (Short shortt : sumIDInactiveSet) {
      sumInactive += shortt.doubleValue();
    }

    Number avgActive = AbstractAggregator.downCast(sumActive / sumIDActiveSet.size());
    Number avgInactive = AbstractAggregator.downCast(sumInactive / sumIDInactiveSet.size());

    while (iter.hasNext()) {
      Struct struct = (Struct) iter.next();
      StructType structType = struct.getStructType();
      ObjectType[] fieldTypes = structType.getFieldTypes();
      assertEquals("String", fieldTypes[0].getSimpleClassName());
      assertEquals("Number", fieldTypes[1].getSimpleClassName());

      if (struct.get("status").equals("active")) {
        assertEquals(avgActive, struct.get("average"));
      } else if (struct.get("status").equals("inactive")) {
        assertEquals(avgInactive, struct.get("average"));
      } else {
        fail("unexpected value of status");
      }
    }
    ObjectType elementType = sr.getCollectionType().getElementType();
    assertTrue(elementType.isStructType());
    StructType structType = (StructType) elementType;
    ObjectType[] fieldTypes = structType.getFieldTypes();
    assertEquals("String", fieldTypes[0].getSimpleClassName());
    assertEquals("Number", fieldTypes[1].getSimpleClassName());
  }

  @Test
  public void testAggregateFuncWithOrderBy() throws Exception {
    Region region = this.createRegion("portfolio", Portfolio.class);
    for (int i = 1; i < 600; ++i) {
      Portfolio pf = new Portfolio(i);
      if (pf.status.equals("active")) {
        pf.shortID = (short) ((short) i % 5);
      } else {
        pf.shortID = (short) ((short) i % 11);
      }
      region.put("" + i, pf);
    }
    String[] queries = {
        "select   p.status as status,  avg(distinct p.shortID) as average from /portfolio p where p.ID > 0 group by status order by average",
        "select   p.shortID as shid,  avg( p.ID) as average from /portfolio p where p.ID > 0 group by shid order by average desc",
        "select   p.shortID as shid,  avg( p.ID) as average from /portfolio p where p.ID > 0 group by shid order by  avg(p.ID)",
        "select   p.shortID as shid,  avg( p.ID) as average from /portfolio p where p.ID > 0 group by shid order by  avg(p.ID) desc, shid asc",
        "select   p.shortID as shid,  avg( p.ID) as average from /portfolio p where p.ID > 0 group by shid order by  avg(p.ID) desc, shid desc",
        "select   p.status as status,  p.shortID as shid  from /portfolio p where p.ID > 0 group by status, shid order by  shid desc",
        "select   p.shortID as shid,  count(*) as countt  from /portfolio p where p.ID > 0 group by p.shortID order by  count(*) desc"};
    Object[][] r = new Object[queries.length][2];
    QueryService qs = CacheUtils.getQueryService();
    for (int i = 0; i < queries.length; ++i) {
      Query query = qs.newQuery(queries[i]);

      SelectResults sr = (SelectResults) query.execute();
      r[i][0] = sr;
    }

    StructSetOrResultsSet ssOrrs = new StructSetOrResultsSet();

    // Compare each of the query results with queries fired without order by ,
    // but without order by
    ssOrrs.compareExternallySortedQueriesWithOrderBy(queries, r);
  }

  @Test
  public void testComplexValueAggregateFuncAvgDistinct() throws Exception {
    Region region = this.createRegion("portfolio", Portfolio.class);
    for (int i = 1; i < 200; ++i) {
      Portfolio pf = new Portfolio(i);
      pf.shortID = (short) ((short) i / 5);
      region.put("" + i, pf);
    }
    String queryStr =
        "select   p.status as status,  avg(distinct element(select iter.shortID from /portfolio iter where iter.ID = p.ID) ) as average from "
            + "/portfolio p where p.ID > 0 group by status";
    QueryService qs = CacheUtils.getQueryService();
    Query query = qs.newQuery(queryStr);
    CompiledSelect cs = ((DefaultQuery) query).getSelect();
    SelectResults sr = (SelectResults) query.execute();
    assertTrue(sr.getCollectionType().getElementType().isStructType());
    assertEquals(2, sr.size());
    Iterator iter = sr.iterator();
    Region rgn = CacheUtils.getRegion("portfolio");
    Set<Short> sumIDActiveSet = new HashSet<Short>(), sumIDInactiveSet = new HashSet<Short>();

    for (Object o : rgn.values()) {
      Portfolio pf = (Portfolio) o;
      if (pf.ID > 0) {
        if (pf.status.equals("active")) {
          sumIDActiveSet.add(pf.shortID);

        } else if (pf.status.equals("inactive")) {
          sumIDInactiveSet.add(pf.shortID);
        }
      }
    }
    double sumActive = 0, sumInactive = 0;
    for (Short shortt : sumIDActiveSet) {
      sumActive += shortt.doubleValue();
    }

    for (Short shortt : sumIDInactiveSet) {
      sumInactive += shortt.doubleValue();
    }

    Number avgActive = AbstractAggregator.downCast(sumActive / sumIDActiveSet.size());
    Number avgInactive = AbstractAggregator.downCast(sumInactive / sumIDInactiveSet.size());

    while (iter.hasNext()) {
      Struct struct = (Struct) iter.next();
      StructType structType = struct.getStructType();
      ObjectType[] fieldTypes = structType.getFieldTypes();
      assertEquals("String", fieldTypes[0].getSimpleClassName());
      assertEquals("Number", fieldTypes[1].getSimpleClassName());

      if (struct.get("status").equals("active")) {
        assertEquals(avgActive, struct.get("average"));
      } else if (struct.get("status").equals("inactive")) {
        assertEquals(avgInactive, struct.get("average"));
      } else {
        fail("unexpected value of status");
      }
    }
    ObjectType elementType = sr.getCollectionType().getElementType();
    assertTrue(elementType.isStructType());
    StructType structType = (StructType) elementType;
    ObjectType[] fieldTypes = structType.getFieldTypes();
    assertEquals("String", fieldTypes[0].getSimpleClassName());
    assertEquals("Number", fieldTypes[1].getSimpleClassName());
  }

  @Test
  public void testAggregateFuncMax() throws Exception {
    Region region = this.createRegion("portfolio", Portfolio.class);
    for (int i = 1; i < 200; ++i) {
      Portfolio pf = new Portfolio(i);
      pf.shortID = (short) ((short) i / 5);
      region.put("" + i, pf);
    }
    String queryStr =
        "select  p.status as status, Max(p.ID) as Maxx from /portfolio p where p.ID > 0 group by status ";
    QueryService qs = CacheUtils.getQueryService();
    Query query = qs.newQuery(queryStr);
    CompiledSelect cs = ((DefaultQuery) query).getSelect();
    SelectResults sr = (SelectResults) query.execute();
    assertTrue(sr.getCollectionType().getElementType().isStructType());
    assertEquals(2, sr.size());
    Iterator iter = sr.iterator();
    Region rgn = CacheUtils.getRegion("portfolio");
    int activeMaxID = 0;
    int inactiveMaxID = 0;
    for (Object o : rgn.values()) {
      Portfolio pf = (Portfolio) o;
      if (pf.status.equals("active")) {
        if (pf.ID > activeMaxID) {
          activeMaxID = pf.ID;
        }
      } else if (pf.status.equals("inactive")) {
        if (pf.ID > inactiveMaxID) {
          inactiveMaxID = pf.ID;
        }
      } else {
        fail("unexpected value of status");
      }
    }

    while (iter.hasNext()) {
      Struct struct = (Struct) iter.next();
      StructType structType = struct.getStructType();
      ObjectType[] fieldTypes = structType.getFieldTypes();
      assertEquals("String", fieldTypes[0].getSimpleClassName());
      assertEquals("Number", fieldTypes[1].getSimpleClassName());

      if (struct.get("status").equals("active")) {
        assertEquals(activeMaxID, ((Integer) struct.get("Maxx")).intValue());
      } else if (struct.get("status").equals("inactive")) {
        assertEquals(inactiveMaxID, ((Integer) struct.get("Maxx")).intValue());
      } else {
        fail("unexpected value of status");
      }
    }

    ObjectType elementType = sr.getCollectionType().getElementType();
    assertTrue(elementType.isStructType());
    StructType structType = (StructType) elementType;
    ObjectType[] fieldTypes = structType.getFieldTypes();
    assertEquals("String", fieldTypes[0].getSimpleClassName());
    assertEquals("Number", fieldTypes[1].getSimpleClassName());
  }

  @Test
  public void testSumWithMultiColumnGroupBy() throws Exception {
    Region region = this.createRegion("portfolio", Portfolio.class);
    for (int i = 1; i < 200; ++i) {
      Portfolio pf = new Portfolio(i);
      pf.shortID = (short) ((short) i % 5);
      region.put("" + i, pf);
    }

    Map<String, Integer> expectedData = new HashMap<String, Integer>();
    for (Object o : region.values()) {
      Portfolio pf = (Portfolio) o;
      String key = pf.status + "_" + pf.shortID;
      if (expectedData.containsKey(key)) {
        int sum = expectedData.get(key).intValue() + pf.ID;
        expectedData.put(key, sum);
      } else {
        expectedData.put(key, pf.ID);
      }
    }

    String queryStr =
        "select  p.status as status, p.shortID as shortID, sum(p.ID) as summ from /portfolio p"
            + " where p.ID > 0 group by status, shortID ";

    QueryService qs = CacheUtils.getQueryService();
    Query query = qs.newQuery(queryStr);
    CompiledSelect cs = ((DefaultQuery) query).getSelect();
    SelectResults sr = (SelectResults) query.execute();
    assertTrue(sr.getCollectionType().getElementType().isStructType());
    assertEquals(expectedData.size(), sr.size());
    Iterator iter = sr.iterator();

    while (iter.hasNext()) {
      Struct struct = (Struct) iter.next();
      StructType structType = struct.getStructType();
      ObjectType[] fieldTypes = structType.getFieldTypes();
      assertEquals("String", fieldTypes[0].getSimpleClassName());
      assertEquals("Short", fieldTypes[1].getSimpleClassName());
      assertEquals("Number", fieldTypes[2].getSimpleClassName());

      String key = struct.get("status") + "_" + struct.get("shortID");
      int sum = ((Integer) struct.get("summ")).intValue();
      assertTrue(expectedData.containsKey(key));
      assertEquals(expectedData.get(key).intValue(), sum);
    }

    ObjectType elementType = sr.getCollectionType().getElementType();
    assertTrue(elementType.isStructType());
    StructType structType = (StructType) elementType;
    ObjectType[] fieldTypes = structType.getFieldTypes();
    assertEquals("String", fieldTypes[0].getSimpleClassName());
    assertEquals("Short", fieldTypes[1].getSimpleClassName());
    assertEquals("Number", fieldTypes[2].getSimpleClassName());

  }

  @Test
  public void testAggregateFuncMin() throws Exception {
    Region region = this.createRegion("portfolio", Portfolio.class);
    for (int i = 1; i < 200; ++i) {
      Portfolio pf = new Portfolio(i);
      pf.shortID = (short) ((short) i / 5);
      region.put("" + i, pf);
    }
    String queryStr =
        "select  p.status as status, Min(p.ID) as Minn from /portfolio p where p.ID > 0 group by status ";
    QueryService qs = CacheUtils.getQueryService();
    Query query = qs.newQuery(queryStr);
    CompiledSelect cs = ((DefaultQuery) query).getSelect();
    SelectResults sr = (SelectResults) query.execute();
    assertTrue(sr.getCollectionType().getElementType().isStructType());
    assertEquals(2, sr.size());
    Iterator iter = sr.iterator();
    Region rgn = CacheUtils.getRegion("portfolio");
    int activeMinID = Integer.MAX_VALUE;
    int inactiveMinID = Integer.MAX_VALUE;
    for (Object o : rgn.values()) {
      Portfolio pf = (Portfolio) o;
      if (pf.status.equals("active")) {
        if (pf.ID < activeMinID) {
          activeMinID = pf.ID;
        }
      } else if (pf.status.equals("inactive")) {
        if (pf.ID < inactiveMinID) {
          inactiveMinID = pf.ID;
        }
      } else {
        fail("unexpected value of status");
      }
    }

    while (iter.hasNext()) {
      Struct struct = (Struct) iter.next();
      StructType structType = struct.getStructType();
      ObjectType[] fieldTypes = structType.getFieldTypes();
      assertEquals("String", fieldTypes[0].getSimpleClassName());
      assertEquals("Number", fieldTypes[1].getSimpleClassName());

      if (struct.get("status").equals("active")) {
        assertEquals(activeMinID, ((Integer) struct.get("Minn")).intValue());
      } else if (struct.get("status").equals("inactive")) {
        assertEquals(inactiveMinID, ((Integer) struct.get("Minn")).intValue());
      } else {
        fail("unexpected value of status");
      }
    }

    ObjectType elementType = sr.getCollectionType().getElementType();
    assertTrue(elementType.isStructType());
    StructType structType = (StructType) elementType;
    ObjectType[] fieldTypes = structType.getFieldTypes();
    assertEquals("String", fieldTypes[0].getSimpleClassName());
    assertEquals("Number", fieldTypes[1].getSimpleClassName());
  }

  @Test
  public void testCompactRangeIndex() throws Exception {

    Region region = this.createRegion("portfolio", Portfolio.class);
    for (int i = 1; i < 200; ++i) {
      Portfolio pf = new Portfolio(i);
      pf.shortID = (short) ((short) i / 5);
      region.put("" + i, pf);
    }
    QueryService qs = CacheUtils.getQueryService();
    String[] queries = {
        "select pos.secId from  /portfolio  p, p.positions.values pos where NOT (pos.secId IN SET('SUN', 'ORCL')) group by pos.secId ", // 6
        "select pos.secId , count(pos.ID) from /portfolio p, p.positions.values pos where  pos.secId > 'APPL' group by pos.secId ",// 7

    };
    Object r[][] = new Object[queries.length][2];

    // Execute Queries without Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        SelectResults sr = (SelectResults) q.execute();
        r[i][0] = sr;
        assertTrue(sr.size() > 0);
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    // Create Indexes
    qs.createIndex("statusIndex", "status", "/portfolio");
    // Execute Queries with Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        r[i][1] = q.execute();
        SelectResults rcw = (SelectResults) r[i][1];
        int indexLimit = queries[i].indexOf("limit");
        int limit = -1;
        boolean limitQuery = indexLimit != -1;
        if (limitQuery) {
          limit = Integer.parseInt(queries[i].substring(indexLimit + 5).trim());
        }
        assertTrue("Result size is " + rcw.size() + " and limit is " + limit,
            !limitQuery || rcw.size() <= limit);
        String colType = rcw.getCollectionType().getSimpleClassName();
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    StructSetOrResultsSet ssOrrs = new StructSetOrResultsSet();

    ssOrrs.CompareQueryResultsWithoutAndWithIndexes(r, queries.length, true, queries);
  }

  public void testDistinctCountWithoutGroupBy() throws Exception {
    Region region = this.createRegion("portfolio", Portfolio.class);
    for (int i = 1; i < 200; ++i) {
      Portfolio pf = new Portfolio(i);
      pf.shortID = (short) ((short) i / 5);
      region.put("" + i, pf);
    }
    QueryService qs = CacheUtils.getQueryService();
    String[] queries = {
        "select  count(distinct pos.secId) from /portfolio p, p.positions.values pos where  pos.secId > 'APPL' ",// 10

    };
    Object r[][] = new Object[queries.length][2];

    // Execute Queries without Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        SelectResults sr = (SelectResults) q.execute();
        r[i][0] = sr;
        assertTrue(sr.size() > 0);
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    // Create Indexes
    qs.createIndex("secIdIndex", "pos.secId", "/portfolio p, p.positions.values pos");
    // Execute Queries with Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        r[i][1] = q.execute();
        SelectResults rcw = (SelectResults) r[i][1];
        int indexLimit = queries[i].indexOf("limit");
        int limit = -1;
        boolean limitQuery = indexLimit != -1;
        if (limitQuery) {
          limit = Integer.parseInt(queries[i].substring(indexLimit + 5).trim());
        }
        assertTrue("Result size is " + rcw.size() + " and limit is " + limit,
            !limitQuery || rcw.size() <= limit);
        String colType = rcw.getCollectionType().getSimpleClassName();
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    StructSetOrResultsSet ssOrrs = new StructSetOrResultsSet();

    ssOrrs.CompareQueryResultsWithoutAndWithIndexes(r, queries.length, true, queries);
  }

  public void testLimitWithGroupBy() throws Exception {
    Region region = this.createRegion("portfolio", Portfolio.class);
    for (int i = 1; i < 200; ++i) {
      Portfolio pf = new Portfolio(i);
      pf.shortID = (short) ((short) i / 5);
      region.put("" + i, pf);
    }
    QueryService qs = CacheUtils.getQueryService();
    String queryStr =
        "select pos.secId as a, count( *) as x from /portfolio p, p.positions.values pos group by a  limit 5 ";
    Query q = qs.newQuery(queryStr);

    SelectResults sr = (SelectResults) q.execute();
    assertEquals(5, sr.size());

    queryStr =
        "select pos.secId as a, count( *) as x from /portfolio p, p.positions.values pos group by a  limit 0 ";
    q = qs.newQuery(queryStr);

    sr = (SelectResults) q.execute();
    assertEquals(0, sr.size());

    queryStr =
        "select pos.secId as a, count( *) as x from /portfolio p, p.positions.values pos group by a  order by count(*) limit 5 ";
    q = qs.newQuery(queryStr);
    sr = (SelectResults) q.execute();
    assertEquals(5, sr.size());
    StructSetOrResultsSet ss = new StructSetOrResultsSet();
    ss.compareExternallySortedQueriesWithOrderBy(new String[] {queryStr},
        new Object[][] {{sr, null}});
  }

  @Before
  public void setUp() throws Exception {
    CacheUtils.startCache();
    CacheUtils.getCache();
  }

  @After
  public void tearDown() throws Exception {
    CacheUtils.closeCache();
  }
}
