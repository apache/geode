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
 * StructMemberAcessJUnitTest.java JUnit based test
 *
 * Created on March 24, 2005, 5:54 PM
 */
package org.apache.geode.cache.query.functional;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.query.CacheUtils;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.data.Address;
import org.apache.geode.cache.query.data.Employee;
import org.apache.geode.cache.query.data.Manager;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.internal.StructSet;
import org.apache.geode.cache.query.types.CollectionType;
import org.apache.geode.cache.query.types.StructType;
import org.apache.geode.test.junit.categories.OQLQueryTest;

@Category({OQLQueryTest.class})
public class StructMemberAccessJUnitTest {

  @Before
  public void setUp() throws java.lang.Exception {
    CacheUtils.startCache();
    Region region = CacheUtils.createRegion("Portfolios", Portfolio.class);
    for (int i = 0; i < 4; i++) {
      region.put("" + i, new Portfolio(i));
    }
  }

  @After
  public void tearDown() throws java.lang.Exception {
    CacheUtils.closeCache();
  }

  @Test
  public void testUnsupportedQueries() throws Exception {
    String[] queries = {
        "SELECT DISTINCT * FROM" + " (SELECT DISTINCT * FROM " + SEPARATOR
            + "Portfolios ptf, positions pos)"
            + " WHERE value.secId = 'IBM'",
        "SELECT DISTINCT * FROM" + " (SELECT DISTINCT * FROM " + SEPARATOR
            + "Portfolios ptf, positions pos) p"
            + " WHERE p.get(1).value.secId = 'IBM'",
        "SELECT DISTINCT * FROM" + " (SELECT DISTINCT * FROM " + SEPARATOR
            + "Portfolios ptf, positions pos) p"
            + " WHERE p[1].value.secId = 'IBM'",
        "SELECT DISTINCT * FROM" + " (SELECT DISTINCT * FROM " + SEPARATOR
            + "Portfolios ptf, positions pos) p"
            + " WHERE p.value.secId = 'IBM'"};
    for (final String query : queries) {
      try {
        Query q = CacheUtils.getQueryService().newQuery(query);
        Object r = q.execute();
        fail(query);
      } catch (Exception e) {
        // e.printStackTrace();
      }
    }
  }

  @Test
  public void testSupportedQueries() throws Exception {
    String[] queries = {
        "SELECT DISTINCT * FROM" + " (SELECT DISTINCT * FROM " + SEPARATOR
            + "Portfolios ptf, positions pos)"
            + " WHERE pos.value.secId = 'IBM'",
        "SELECT DISTINCT * FROM" + " (SELECT DISTINCT * FROM " + SEPARATOR
            + "Portfolios AS ptf, positions AS pos)"
            + " WHERE pos.value.secId = 'IBM'",
        "SELECT DISTINCT * FROM" + " (SELECT DISTINCT * FROM ptf IN " + SEPARATOR
            + "Portfolios, pos IN positions)"
            + " WHERE pos.value.secId = 'IBM'",
        "SELECT DISTINCT * FROM"
            + " (SELECT DISTINCT pos AS myPos FROM " + SEPARATOR + "Portfolios ptf, positions pos)"
            + " WHERE myPos.value.secId = 'IBM'",
        "SELECT DISTINCT * FROM" + " (SELECT DISTINCT * FROM " + SEPARATOR
            + "Portfolios ptf, positions pos) p"
            + " WHERE p.pos.value.secId = 'IBM'",
        "SELECT DISTINCT * FROM" + " (SELECT DISTINCT * FROM " + SEPARATOR
            + "Portfolios ptf, positions pos) p"
            + " WHERE pos.value.secId = 'IBM'",
        "SELECT DISTINCT * FROM" + " (SELECT DISTINCT * FROM " + SEPARATOR
            + "Portfolios, positions) p"
            + " WHERE p.positions.value.secId = 'IBM'",
        "SELECT DISTINCT * FROM" + " (SELECT DISTINCT * FROM " + SEPARATOR
            + "Portfolios, positions)"
            + " WHERE positions.value.secId = 'IBM'",
        "SELECT DISTINCT * FROM" + " (SELECT DISTINCT * FROM " + SEPARATOR
            + "Portfolios ptf, positions pos) p"
            + " WHERE p.get('pos').value.secId = 'IBM'",
        "SELECT DISTINCT name FROM" + " " + SEPARATOR
            + "Portfolios , secIds name where length > 0 ",};
    for (final String query : queries) {
      try {
        Query q = CacheUtils.getQueryService().newQuery(query);
        Object r = q.execute();
      } catch (Exception e) {
        e.printStackTrace();
        fail(query);
      }
    }
  }

  @Test
  public void testResultComposition() throws Exception {
    String[] queries = {"select distinct p from " + SEPARATOR + "Portfolios p where p.ID > 0",
        "select distinct p.getID from " + SEPARATOR + "Portfolios p where p.ID > 0 ",
        "select distinct p.getID as secID from " + SEPARATOR + "Portfolios p where p.ID > 0 "};
    for (int i = 0; i < queries.length; i++) {
      Query q = CacheUtils.getQueryService().newQuery(queries[i]);
      Object o = q.execute();
      if (o instanceof SelectResults) {
        SelectResults sr = (SelectResults) o;
        if (sr instanceof StructSet && i != 2) {
          fail(
              " StructMemberAccess::testResultComposition: Got StrcutSet when expecting ResultSet");
        }
        CollectionType ct = sr.getCollectionType();
        CacheUtils.log("***Elememt Type of Colelction = " + ct.getElementType());
        CacheUtils.log((sr.getCollectionType()).getElementType().getSimpleClassName());
        List ls = sr.asList();
        for (Object l : ls) {
          CacheUtils.log("Object in the resultset = " + l.getClass());
        }
        switch (i) {
          case 0:
            if (ct.getElementType().getSimpleClassName().equals("Portfolio")) {
              assertTrue(true);
            } else {
              System.out.println(
                  "StructMemberAcessJUnitTest::testResultComposition:Colelction Element's class="
                      + ct.getElementType().getSimpleClassName());
              fail();
            }
            break;
          case 1:
            if (ct.getElementType().getSimpleClassName().equals("int")) {
              assertTrue(true);
            } else {
              System.out.println(
                  "StructMemberAcessJUnitTest::testResultComposition:Colelction Element's class="
                      + ct.getElementType().getSimpleClassName());
              fail();
            }
            break;
          case 2:
            if (ct.getElementType().getSimpleClassName().equals("Struct")) {
              assertTrue(true);
            } else {
              System.out.println(
                  "StructMemberAcessJUnitTest::testResultComposition:Colelction Element's class="
                      + ct.getElementType().getSimpleClassName());
              fail();
            }
        }
      }
    }
  }

  public void _BUGtestSubClassQuery() throws Exception {
    Set add1 = new HashSet();
    Set add2 = new HashSet();
    add1.add(new Address("Hp3 9yf", "Apsley"));
    add1.add(new Address("Hp4 9yf", "Apsleyss"));
    add2.add(new Address("Hp3 8DZ", "Hemel"));
    add2.add(new Address("Hp4 8DZ", "Hemel"));
    Region region = CacheUtils.createRegion("employees", Employee.class);
    region.put("1", new Manager("aaa", 27, 270, "QA", 1800, add1, 2701));
    region.put("2", new Manager("bbb", 28, 280, "QA", 1900, add2, 2801));
    String[] queries = {"SELECT DISTINCT e.manager_id FROM " + SEPARATOR + "employees e"};
    for (final String query : queries) {
      Query q = CacheUtils.getQueryService().newQuery(query);
      Object r = q.execute();
      String className =
          (((SelectResults) r).getCollectionType()).getElementType().getSimpleClassName();
      if (!className.equals("Employee")) {
        fail(
            "StructMemberAccessTest::testSubClassQuery:failed .Expected class name Employee. Actualy obtained="
                + className);
      }
    }
  }

  @Test
  public void testBugNumber_32354() {
    String[] queries = {"select distinct * from " + SEPARATOR + "root" + SEPARATOR
        + "portfolios.values, positions.values ",};
    int i = 0;
    try {
      tearDown();
      CacheUtils.startCache();
      Region rootRegion = CacheUtils.createRegion("root", null);
      AttributesFactory attributesFactory = new AttributesFactory();
      attributesFactory.setValueConstraint(Portfolio.class);
      RegionAttributes regionAttributes = attributesFactory.create();
      Region region = rootRegion.createSubregion("portfolios", regionAttributes);
      for (i = 0; i < 4; i++) {
        region.put("" + i, new Portfolio(i));
      }
      for (i = 0; i < queries.length; i++) {
        Query q = CacheUtils.getQueryService().newQuery(queries[i]);
        Object r = q.execute();
        StructType type = ((StructType) ((SelectResults) r).getCollectionType().getElementType());
        String[] fieldNames = type.getFieldNames();
        for (i = 0; i < fieldNames.length; ++i) {
          String name = fieldNames[i];
          if (name.equals(SEPARATOR + "root" + SEPARATOR + "portfolios")
              || name.equals("positions.values")) {
            fail("The field name in struct = " + name);
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(queries[i]);
    }
  }

  @Test
  public void testBugNumber_32355() {
    String[] queries = {
        "select distinct positions.values.toArray[0], positions.values.toArray[0],status from "
            + SEPARATOR + "Portfolios",};
    int i = 0;
    try {
      for (i = 0; i < queries.length; i++) {
        Query q = CacheUtils.getQueryService().newQuery(queries[i]);
        Object r = q.execute();
        StructType type = ((StructType) ((SelectResults) r).getCollectionType().getElementType());
        String[] fieldNames = type.getFieldNames();
        for (i = 0; i < fieldNames.length; ++i) {
          String name = fieldNames[i];
          CacheUtils.log("Struct Field name = " + name);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(queries[i]);
    }
  }
}
