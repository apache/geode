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
 * IteratorTypeDefEmpTest_vj.java
 *
 * Created on April 11, 2005, 11:56 AM
 */
package org.apache.geode.cache.query.functional;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.junit.Assert.fail;

import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.CacheUtils;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.data.Address;
import org.apache.geode.cache.query.data.Employee;
import org.apache.geode.cache.query.data.Manager;
import org.apache.geode.test.junit.categories.OQLQueryTest;

@Category({OQLQueryTest.class})
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
    String[] queries = {"IMPORT org.apache.geode.cache.\"query\".data.Manager;"
        + "SELECT DISTINCT manager_id FROM (set<Manager>)" + SEPARATOR
        + "employees where empId > 0"};
    Query q = null;
    for (int i = 0; i < queries.length; i++) {
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        Object r = q.execute();
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
  }
}
