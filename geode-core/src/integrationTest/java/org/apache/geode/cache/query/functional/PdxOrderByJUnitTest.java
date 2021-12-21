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
package org.apache.geode.cache.query.functional;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.junit.Assert.fail;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.cache.query.CacheUtils;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.Struct;
import org.apache.geode.cache.query.data.PortfolioPdx;
import org.apache.geode.cache.query.internal.index.PartitionedIndex;
import org.apache.geode.cache.query.internal.index.RangeIndex;
import org.apache.geode.cache.query.types.CollectionType;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.test.junit.categories.OQLQueryTest;

@Category({OQLQueryTest.class})
public class PdxOrderByJUnitTest {
  private final String rootRegionName = "root";
  private final String regionName = "PdxTest";
  private final String regName = SEPARATOR + rootRegionName + SEPARATOR + regionName;

  private final String[] queryString = new String[] {
      "SELECT pos.secId FROM " + regName + " p, p.positions.values pos WHERE pos.secId LIKE '%L'", // 0
      "SELECT pos.secId FROM " + regName + " p, p.positions.values pos where pos.secId = 'IBM'", // 1
      "SELECT pos.secId, p.status FROM " + regName
          + " p, p.positions.values pos where pos.secId > 'APPL'", // 2
      "SELECT pos.secId FROM " + regName
          + " p, p.positions.values pos WHERE pos.secId > 'APPL' and pos.secId < 'SUN'", // 3
      "select pos.secId from " + regName
          + " p, p.positions.values pos where pos.secId  IN SET ('YHOO', 'VMW')", // 4
      "select pos.secId from " + regName
          + " p, p.positions.values pos where NOT (pos.secId = 'VMW')", // 5
      "select pos.secId from " + regName
          + " p, p.positions.values pos where NOT (pos.secId IN SET('SUN', 'ORCL')) ", // 6
      "SELECT distinct pos.secId FROM " + regName + " p, p.positions.values pos order by pos.secId", // 7
      "SELECT distinct pos.secId FROM " + regName
          + " p, p.positions.values pos WHERE p.ID > 1 order by pos.secId limit 5",// 58
  };

  private final String[] queryString2 = new String[] {
      "SELECT pos.secIdIndexed FROM " + regName
          + " p, p.positions.values pos WHERE pos.secIdIndexed LIKE '%L'", // 0
      "SELECT pos.secIdIndexed FROM " + regName
          + " p, p.positions.values pos where pos.secIdIndexed = 'IBM'", // 1
      "SELECT pos.secIdIndexed, p.status FROM " + regName
          + " p, p.positions.values pos where pos.secIdIndexed > 'APPL'", // 2
      "SELECT pos.secIdIndexed FROM " + regName
          + " p, p.positions.values pos WHERE pos.secIdIndexed > 'APPL' and pos.secIdIndexed < 'SUN'", // 3
      "select pos.secIdIndexed from " + regName
          + " p, p.positions.values pos where pos.secIdIndexed  IN SET ('YHOO', 'VMW')", // 4
      "select pos.secIdIndexed from " + regName
          + " p, p.positions.values pos where NOT (pos.secIdIndexed = 'VMW')", // 5
      "select pos.secIdIndexed from " + regName
          + " p, p.positions.values pos where NOT (pos.secIdIndexed IN SET('SUN', 'ORCL')) ", // 6
      "SELECT distinct pos.secIdIndexed FROM " + regName
          + " p, p.positions.values pos order by pos.secIdIndexed", // 7
      "SELECT distinct pos.secIdIndexed FROM " + regName
          + " p, p.positions.values pos WHERE p.ID > 1 order by pos.secIdIndexed limit 5",// 8
  };

  @Before
  public void setUp() throws java.lang.Exception {
    CacheUtils.startCache();
  }

  @After
  public void tearDown() throws java.lang.Exception {
    CacheUtils.closeCache();
  }

  @Test
  public void testPartitionRangeIndex() throws Exception {
    final int numberOfEntries = 10;
    Region pr = configurePR();
    // create a local query service
    QueryService localQueryService = null;
    try {
      localQueryService = CacheUtils.getQueryService();
    } catch (Exception e) {
      fail(e.toString());
    }
    // Verify the type of index created

    for (int i = 0; i < numberOfEntries; i++) {
      pr.put("key-" + i, new PortfolioPdx(i));
    }
    localQueryService = CacheUtils.getCache().getQueryService();
    SelectResults[][] rs = new SelectResults[queryString.length][2];

    for (int i = 0; i < queryString.length; i++) {
      try {
        Query query = localQueryService.newQuery(queryString[i]);
        rs[i][0] = (SelectResults) query.execute();
        checkForPdxString(rs[i][0].asList(), queryString[i]);
      } catch (Exception e) {
        fail("Failed executing " + queryString[i]);
      }
    }
    Index index = null;
    try {
      index = localQueryService.createIndex("secIdIndex", "pos.secId",
          regName + " p, p.positions.values pos");
      if (index instanceof PartitionedIndex) {
        for (Object o : ((PartitionedIndex) index).getBucketIndexes()) {
          if (!(o instanceof RangeIndex)) {
            fail("Range Index should have been created instead of " + index.getClass());
          }
        }
      } else {
        fail("Partitioned index expected");
      }
    } catch (Exception ex) {
      fail("Failed to create index." + ex.getMessage());
    }



    for (int i = 0; i < queryString.length; i++) {
      try {
        Query query = localQueryService.newQuery(queryString[i]);

        rs[i][1] = (SelectResults) query.execute();
        checkForPdxString(rs[i][1].asList(), queryString[i]);


      } catch (Exception e) {
        fail("Failed executing " + queryString[i]);
      }
    }


    for (int i = 0; i < queryString.length; i++) {
      try {

        if (i < 7) {
          // Compare local and remote query results.
          if (!compareResultsOfWithAndWithoutIndex(rs[i])) {
            fail("Local and Remote Query Results are not matching for query :" + queryString[i]);
          }
        } else {
          // compare the order of results returned
          compareResultsOrder(rs[i], true);
        }
      } catch (Exception e) {
        fail("Failed executing " + queryString[i]);
      }
    }
  }

  private Region configurePR() {
    AttributesFactory factory = new AttributesFactory();

    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    // factory.setDataPolicy(DataPolicy.PARTITION);

    PartitionAttributes prAttr = paf.setTotalNumBuckets(20).setRedundantCopies(0).create();
    factory.setPartitionAttributes(prAttr);

    return createRegion(regionName, rootRegionName, factory.create());

  }

  public Region createRegion(String name, String rootName, RegionAttributes attrs)
      throws CacheException {
    Region root = getRootRegion(rootName);
    if (root == null) {
      // don't put listeners on root region
      RegionAttributes rootAttrs = attrs;
      AttributesFactory fac = new AttributesFactory(attrs);
      ExpirationAttributes expiration = ExpirationAttributes.DEFAULT;

      // fac.setCacheListener(null);
      fac.setCacheLoader(null);
      fac.setCacheWriter(null);
      fac.setPoolName(null);
      fac.setPartitionAttributes(null);
      fac.setRegionTimeToLive(expiration);
      fac.setEntryTimeToLive(expiration);
      fac.setRegionIdleTimeout(expiration);
      fac.setEntryIdleTimeout(expiration);
      rootAttrs = fac.create();
      root = createRootRegion(rootName, rootAttrs);
    }

    return root.createSubregion(name, attrs);
  }

  public Region getRootRegion(String rootName) {
    return CacheUtils.getRegion(rootName);
  }

  public Region createRootRegion(String rootName, RegionAttributes attrs)
      throws RegionExistsException, TimeoutException {
    return ((GemFireCacheImpl) CacheUtils.getCache()).createRegion(rootName, attrs);
  }

  private void checkForPdxString(List results, String query) {
    for (Object o : results) {
      if (o instanceof Struct) {
        Object o1 = ((Struct) o).getFieldValues()[0];
        Object o2 = ((Struct) o).getFieldValues()[1];
        if (!(o1 instanceof String)) {
          fail("Returned instance of " + o1.getClass() + " instead of String for query: " + query);
        }
        if (!(o2 instanceof String)) {
          fail("Returned instance of " + o2.getClass() + " instead of String for query: " + query);
        }
      } else {
        if (!(o instanceof String)) {
          fail("Returned instance of " + o.getClass() + " instead of String for query: " + query);
        }
      }
    }
  }

  public boolean compareResultsOfWithAndWithoutIndex(SelectResults[] r) {
    boolean ok = true;
    Set set1 = null;
    Set set2 = null;
    Iterator itert1 = null;
    Iterator itert2 = null;
    ObjectType type1, type2;
    // outer: for (int j = 0; j < r.length; j++) {
    CollectionType collType1 = r[0].getCollectionType();
    CollectionType collType2 = r[1].getCollectionType();
    type1 = collType1.getElementType();
    type2 = collType2.getElementType();

    if (r[0].size() == r[1].size()) {
      System.out.println("Both SelectResults are of Same Size i.e.  Size= " + r[1].size());
    } else {
      System.out.println("FAILED4: SelectResults size is different in both the cases. Size1="
          + r[0].size() + " Size2 = " + r[1].size());
      ok = false;

    }
    if (ok) {
      set2 = (r[1].asSet());
      set1 = (r[0].asSet());
      boolean pass = true;
      itert1 = set1.iterator();
      while (itert1.hasNext()) {
        Object p1 = itert1.next();
        itert2 = set2.iterator();

        boolean exactMatch = false;
        while (itert2.hasNext()) {
          Object p2 = itert2.next();
          if (p1 instanceof Struct) {
            Object[] values1 = ((Struct) p1).getFieldValues();
            Object[] values2 = ((Struct) p2).getFieldValues();
            // test.assertIndexDetailsEquals(values1.length, values2.length);
            if (values1.length != values2.length) {
              ok = false;
              break;
            }
            boolean elementEqual = true;
            for (int i = 0; i < values1.length; ++i) {
              elementEqual =
                  elementEqual && ((values1[i] == values2[i]) || values1[i].equals(values2[i]));
            }
            exactMatch = elementEqual;
          } else {
            exactMatch = (p2 == p1) || p2.equals(p1);
          }
          if (exactMatch) {
            break;
          }
        }
        if (!exactMatch) {
          System.out.println(
              "FAILED5: Atleast one element in the pair of SelectResults supposedly identical, is not equal ");
          ok = false;
          break;
        }
      }
    }
    return ok;
  }

  private void compareResultsOrder(SelectResults[] r, boolean isPr) {
    // for (int j = 0; j < r.length; j++) {
    Object[] r1 = (r[0]).toArray();
    Object[] r2 = (r[1]).toArray();
    if (r1.length != r2.length) {
      fail("Size of results not equal: " + r1.length + " vs " + r2.length);
    } else {
      System.out.println("Both ordered SelectResults are of Same Size i.e.  Size= " + r[1].size());
    }
    for (int i = 0, k = 0; i < r1.length && k < r2.length; i++, k++) {
      System.out.println("r1: " + r1[i] + " r2: " + r2[k]);
      if (!r1[i].equals(r2[k])) {
        fail("Order not equal: " + r1[i] + " : " + r2[k] + " isPR: " + isPr);
      }
    }
    // }
  }
}
