/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * TypedIteratorJUnitTest.java
 * JUnit based test
 *
 * Created on March 22, 2005, 2:01 PM
 */

package org.apache.geode.cache.query;

import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.test.junit.categories.IntegrationTest;

/**
 *
 */
@Category(IntegrationTest.class)
public class TypedIteratorJUnitTest {
  Region region;
  QueryService qs;
  Cache cache;
  
  @Test
  public void testUntyped() throws QueryException {
    // one untyped iterator is now resolved fine
    Query q = this.qs.newQuery("SELECT DISTINCT * " +
                "FROM /pos " +
                "WHERE ID = 3 ");
    q.execute();
    
    // if there are two untyped iterators, then it's a problem, see bug 32251 and BugTest
    q = this.qs.newQuery("SELECT DISTINCT * FROM /pos, positions WHERE ID = 3");
    try {
      q.execute();
      fail("Expected a TypeMismatchException");
    }
    catch (TypeMismatchException e) {
      // pass
    }
  }
  
  @Test
  public void testTyped() throws QueryException {
    Query q = this.qs.newQuery( // must quote "query" because it is a reserved word
      "IMPORT org.apache.geode.cache.\"query\".data.Portfolio;\n" +       
      "SELECT DISTINCT *\n" +
      "FROM /pos TYPE Portfolio\n" +
      "WHERE ID = 3  ");
    Object r = q.execute();
    CacheUtils.getLogger().fine(Utils.printResult(r));

    
    q = this.qs.newQuery( // must quote "query" because it is a reserved word
      "IMPORT org.apache.geode.cache.\"query\".data.Portfolio;\n" +       
      "SELECT DISTINCT *\n" +
      "FROM /pos ptfo TYPE Portfolio\n" +
      "WHERE ID = 3  ");
    r = q.execute();
    CacheUtils.getLogger().fine(Utils.printResult(r));
  }  
  
  
  @Test
  public void testTypeCasted() throws QueryException {
    Query q = this.qs.newQuery( // must quote "query" because it is a reserved word
      "IMPORT org.apache.geode.cache.\"query\".data.Portfolio;\n" +       
      "SELECT DISTINCT *\n" +
      "FROM (collection<Portfolio>)/pos\n" +
      "WHERE ID = 3  ");
//    org.apache.geode.internal.util.DebuggerSupport.waitForJavaDebugger(this.cache.getLogger());
    Object r = q.execute();
    CacheUtils.getLogger().fine(Utils.printResult(r));
    
    q = this.qs.newQuery( // must quote "query" because it is a reserved word
      "IMPORT org.apache.geode.cache.\"query\".data.Position;\n" +       
      "SELECT DISTINCT *\n" +
      "FROM /pos p, (collection<Position>)p.positions.values\n" +
      "WHERE secId = 'IBM'");
//    org.apache.geode.internal.util.DebuggerSupport.waitForJavaDebugger(this.cache.getLogger());
    r = q.execute();
    CacheUtils.getLogger().fine(Utils.printResult(r));
  }
  
  @Before
  public void setUp() throws Exception {
    CacheUtils.startCache();
    cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
//    attributesFactory.setValueConstraint(Portfolio.class);
    RegionAttributes regionAttributes = attributesFactory.create();
    
    region = cache.createRegion("pos",regionAttributes);
    region.put("0",new Portfolio(0));
    region.put("1",new Portfolio(1));
    region.put("2",new Portfolio(2));
    region.put("3",new Portfolio(3));
    
    qs = cache.getQueryService();
  }
  
  @After
  public void tearDown() throws Exception {
    CacheUtils.closeCache();
  }
  
}
