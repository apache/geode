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
package org.apache.geode.cache.query.internal;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collection;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.CacheUtils;
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.data.Quote;
import org.apache.geode.cache.query.data.Restricted;
import org.apache.geode.cache.query.types.StructType;
import org.apache.geode.test.junit.categories.OQLQueryTest;

@Category({OQLQueryTest.class})
public class ProjectionAttributeJUnitTest {

  String[] queries = {"select distinct p from " + SEPARATOR + "pos p where p.ID > 0 ", // ResultSet
      "select distinct p.status from " + SEPARATOR + "pos p where p.ID > 0 ", // ResultSet
      "select distinct 'a' from " + SEPARATOR + "pos p ", // ResultSet
      "select distinct 1 from " + SEPARATOR + "pos p ", // ResultSet
      "select distinct p.status,p.ID from " + SEPARATOR + "pos p where p.ID > 0 ",
      "select distinct p,p.P1 from " + SEPARATOR + "pos p where p.ID > 0 ",
      "select distinct p,p.P1.SecId from " + SEPARATOR + "pos p where p.ID > 0 ",
      "select distinct portfolio: p ,p.P1.SecId from " + SEPARATOR + "pos p where p.ID > 0 ",
      "select distinct p.status as STATUS, SECID: p.P1.SecId, ID from " + SEPARATOR
          + "pos p where p.ID > 0 ",
      "select distinct p.status as STATUS, SECID: p.P1.SecId, ID from " + SEPARATOR + "pos p ",
      "select distinct 'a',1, p from " + SEPARATOR + "pos p ",};

  String[] miscQueries = {"select distinct * from null ", "select distinct 1 from null ",
      "select distinct 'a',1, p from null ", "select distinct * from UNDEFINED ",
      "select distinct 1 from UNDEFINED", "select distinct 'a',1, p from UNDEFINED",};

  @Test
  public void testMisc() throws Exception {
    QueryService qs = CacheUtils.getQueryService();
    for (String qStr : miscQueries) {
      Query q = qs.newQuery(qStr);
      Object r = q.execute();
      if (!(r instanceof Collection) || ((Collection) r).size() != 0) {
        fail(q.getQueryString());
      }
    }
  }

  @Test
  public void testProjectionAttributes() throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException {
    QueryService qs = CacheUtils.getQueryService();

    for (int i = 0; i < queries.length; ++i) {
      String qStr = queries[i];
      Query q = qs.newQuery(qStr);
      SelectResults r = (SelectResults) q.execute();
      if (i < 4 && r.getCollectionType().getElementType().isStructType()) {
        fail(q.getQueryString());
      }

      if (i > 3) {
        assertTrue(r.getCollectionType().getElementType().isStructType());
        checkNames(r, qStr);
      }
    }

    String qStr = "select distinct * from " + SEPARATOR + "pos p ";
    Query q = qs.newQuery(qStr);
    Object r = q.execute();
    if (r instanceof StructSet) {
      fail(q.getQueryString());
    }
  }

  private void checkNames(SelectResults results, String query) {
    int i1 = query.indexOf(" distinct ");
    int i2 = query.indexOf(" from ");
    if (i1 < 0 || i2 < 0) {
      fail(query);
    }
    String projStr = query.substring(i1 + " distinct ".length(), i2);
    // CacheUtils.log("projStr = "+projStr);
    QCompiler compiler = new QCompiler();
    List projAttrs = compiler.compileProjectionAttributes(projStr);
    StructType stype = (StructType) results.getCollectionType().getElementType();
    String[] names = stype.getFieldNames();
    for (int i = 0; i < names.length; i++) {
      String name = names[i];
      Object[] arr = (Object[]) projAttrs.get(i);
      String nameToMatch = "";
      if (arr[0] != null) {
        nameToMatch = (String) arr[0];
      } else {
        if (arr[1] instanceof CompiledID) {
          nameToMatch = ((CompiledID) arr[1]).getId();
        }
        if (arr[1] instanceof CompiledPath) {
          nameToMatch = ((CompiledPath) arr[1]).getTailID();
        }
        if (arr[1] instanceof CompiledLiteral) {
          nameToMatch = (((CompiledLiteral) arr[1])._obj).toString();
        }
      }
      if (!nameToMatch.equals(name)) {
        fail(query);
      }
    }
  }

  public void tesBug33581_tMultiRegionIndexTest() {
    try {
      Region region1 = CacheUtils.createRegion("Quotes1", Quote.class);
      Region region2 = CacheUtils.createRegion("Quotes2", Quote.class);
      Region region3 = CacheUtils.createRegion("Restricted1", Restricted.class);
      for (int i = 0; i < 10; i++) {
        region1.put(i, new Quote(i));
        region2.put(i, new Quote(i));
        region3.put(i, new Restricted(i));
      }
      QueryService qs = CacheUtils.getQueryService();
      ////////// creating indexes on region Quotes1
      qs.createIndex("Quotes1Region-quoteIdStrIndex", IndexType.PRIMARY_KEY, "q.quoteIdStr",
          SEPARATOR + "Quotes1 q");

      // qs.createIndex("Quotes1Region-cusipIndex1", IndexType.FUNCTIONAL, "q.cusip", "/Quotes1 q,
      // q.restrict r");
      qs.createIndex("Quotes1Region-quoteTypeIndex", IndexType.FUNCTIONAL, "q.quoteType",
          SEPARATOR + "Quotes1 q, q.restrict r");

      qs.createIndex("Quotes1Region-dealerPortfolioIndex", IndexType.FUNCTIONAL,
          "q.dealerPortfolio", SEPARATOR + "Quotes1 q, q.restrict r");

      qs.createIndex("Quotes1Region-channelNameIndex", IndexType.FUNCTIONAL, "q.channelName",
          SEPARATOR + "Quotes1 q, q.restrict r");

      qs.createIndex("Quotes1Region-priceTypeIndex", IndexType.FUNCTIONAL, "q.priceType",
          SEPARATOR + "Quotes1 q, q.restrict r");

      qs.createIndex("Quotes1Region-lowerQtyIndex", IndexType.FUNCTIONAL, "q.lowerQty",
          SEPARATOR + "Quotes1 q, q.restrict r");
      qs.createIndex("Quotes1Region-upperQtyIndex", IndexType.FUNCTIONAL, "q.upperQty",
          SEPARATOR + "Quotes1 q, q.restrict r");
      qs.createIndex("Quotes1Restricted-quoteTypeIndex", IndexType.FUNCTIONAL, "r.quoteType",
          SEPARATOR + "Quotes1 q, q.restrict r");

      qs.createIndex("Quotes1Restricted-minQtyIndex", IndexType.FUNCTIONAL, "r.minQty",
          SEPARATOR + "Quotes1 q, q.restrict r");
      qs.createIndex("Quotes1Restricted-maxQtyIndex", IndexType.FUNCTIONAL, "r.maxQty",
          SEPARATOR + "Quotes1 q, q.restrict r");

      ////////// creating indexes on region Quotes2
      qs.createIndex("Quotes2Region-quoteIdStrIndex", IndexType.PRIMARY_KEY, "q.quoteIdStr",
          SEPARATOR + "Quotes2 q");

      // qs.createIndex("Quotes1Region-cusipIndex2", IndexType.FUNCTIONAL, "q.cusip", "/Quotes2 q,
      // q.restrict r");
      qs.createIndex("Quotes2Region-quoteTypeIndex", IndexType.FUNCTIONAL, "q.quoteType",
          SEPARATOR + "Quotes2 q, q.restrict r");

      qs.createIndex("Quotes2Region-dealerPortfolioIndex", IndexType.FUNCTIONAL,
          "q.dealerPortfolio", SEPARATOR + "Quotes2 q, q.restrict r");

      qs.createIndex("Quotes2Region-channelNameIndex", IndexType.FUNCTIONAL, "q.channelName",
          SEPARATOR + "Quotes2 q, q.restrict r");

      qs.createIndex("Quotes2Region-priceTypeIndex", IndexType.FUNCTIONAL, "q.priceType",
          SEPARATOR + "Quotes2 q, q.restrict r");

      qs.createIndex("Quotes2Region-lowerQtyIndex", IndexType.FUNCTIONAL, "q.lowerQty",
          SEPARATOR + "Quotes2 q, q.restrict r");
      qs.createIndex("Quotes2Region-upperQtyIndex", IndexType.FUNCTIONAL, "q.upperQty",
          SEPARATOR + "Quotes2 q, q.restrict r");
      qs.createIndex("Quotes2Restricted-quoteTypeIndex", IndexType.FUNCTIONAL, "r.quoteType",
          SEPARATOR + "Quotes2 q, q.restrict r");

      qs.createIndex("Quotes2Restricted-minQtyIndex", IndexType.FUNCTIONAL, "r.minQty",
          SEPARATOR + "Quotes2 q, q.restrict r");
      qs.createIndex("Quotes2Restricted-maxQtyIndex", IndexType.FUNCTIONAL, "r.maxQty",
          SEPARATOR + "Quotes2 q, q.restrict r");

      ////////// creating indexes on region Restricted1
      // qs.createIndex("RestrictedRegion-cusip", IndexType.FUNCTIONAL, "r.cusip", "/Restricted1
      ////////// r");

      qs.createIndex("RestrictedRegion-quoteTypeIndex", IndexType.FUNCTIONAL, "r.quoteType",
          SEPARATOR + "Restricted1 r");
      qs.createIndex("RestrictedRegion-minQtyIndex", IndexType.FUNCTIONAL, "r.minQty",
          SEPARATOR + "Restricted1 r");
      qs.createIndex("RestrictedRegion-maxQtyIndex-1", IndexType.FUNCTIONAL, "r.maxQty",
          SEPARATOR + "Restricted1 r");
      Query q = qs.newQuery(
          "SELECT DISTINCT * FROM " + SEPARATOR + "Quotes1 q1, q1.restrict r1, " + SEPARATOR
              + "Quotes2 q2, q2.restrict r2, " + SEPARATOR
              + "Restricted1 r3 WHERE r1.quoteType = r2.quoteType AND r2.quoteType = r3.quoteType AND r3.maxQty > 1050");
      q.execute();
    } catch (Exception e) {
      e.printStackTrace();
      fail("Test failed bcoz of exception " + e);
    }


  }

  @Test
  public void testProjectionAttributesWithIndex() throws QueryException {
    QueryService qs = CacheUtils.getQueryService();
    qs.createIndex("PortFolioID", IndexType.FUNCTIONAL, "ID", SEPARATOR + "pos");
    testProjectionAttributes();
  }



  @Before
  public void setUp() throws Exception {
    CacheUtils.startCache();
    CacheUtils.getCache();
    Region region = CacheUtils.createRegion("pos", Portfolio.class);
    region.put("0", new Portfolio(0));
    region.put("1", new Portfolio(1));
    region.put("2", new Portfolio(2));
    region.put("3", new Portfolio(3));
  }

  @After
  public void tearDown() throws Exception {
    CacheUtils.closeCache();
  }
}
