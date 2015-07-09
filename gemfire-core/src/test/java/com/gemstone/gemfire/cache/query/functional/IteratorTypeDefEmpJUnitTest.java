/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * IteratorTypeDefEmpTest_vj.java
 *
 * Created on April 11, 2005, 11:56 AM
 */
/*
 * 
 * @author vikramj
 */
package com.gemstone.gemfire.cache.query.functional;

import static org.junit.Assert.fail;

//import com.gemstone.gemfire.cache.query.internal.StructSet;
import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.Utils;
import com.gemstone.gemfire.cache.query.data.Address;
import com.gemstone.gemfire.cache.query.data.Employee;
import com.gemstone.gemfire.cache.query.data.Manager;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class IteratorTypeDefEmpJUnitTest {

  @Before
  public void setUp() throws java.lang.Exception {
    CacheUtils.startCache();
    Set add1 = new HashSet();
    Set add2 = new HashSet();
    add1.add(new Address("Hp3 9yf", "Apsley"));
    add1.add(new Address("Hp4 9yf", "Apsleyss"));
    add2.add(new Address("Hp3 8DZ", "Hemel"));
    add2.add(new Address("Hp4 8DZ", "Hemel"));
    Region region = CacheUtils.createRegion("employees", Employee.class);
    region.put("1", new Manager("aaa", 27, 270, "QA", 1800, add1, 2701));
    region.put("2", new Manager("bbb", 28, 280, "QA", 1900, add2, 2801));
  }

  @After
  public void tearDown() throws java.lang.Exception {
    CacheUtils.closeCache();
  }

  @Test
  public void testIteratorDef() throws Exception {
    String queries[] = { "IMPORT com.gemstone.gemfire.cache.\"query\".data.Manager;"
        + "SELECT DISTINCT manager_id FROM (set<Manager>)/employees where empId > 0"};
    Query q = null;
    for (int i = 0; i < queries.length; i++) {
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        Object r = q.execute();
        CacheUtils.log(Utils.printResult(r));
      }
      catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
  }
}
