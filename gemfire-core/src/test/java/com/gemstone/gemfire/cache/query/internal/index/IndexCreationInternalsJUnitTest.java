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
 * IndexCreationInternalsJUnitTest.java
 * JUnit based test
 *
 * Created on February 22, 2005, 11:24 AM
 */

package com.gemstone.gemfire.cache.query.internal.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.cache.query.QueryInvalidException;
import com.gemstone.gemfire.cache.query.internal.CompiledID;
import com.gemstone.gemfire.cache.query.internal.CompiledIteratorDef;
import com.gemstone.gemfire.cache.query.internal.CompiledPath;
import com.gemstone.gemfire.cache.query.internal.CompiledRegion;
import com.gemstone.gemfire.cache.query.internal.QCompiler;
import com.gemstone.gemfire.cache.query.internal.types.TypeUtils;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 *
 * @author ericz
 */
@Category(IntegrationTest.class)
public class IndexCreationInternalsJUnitTest {
  protected String childThreadName1 = "";
  protected String childThreadName2 = "";
  
  @Before
  public void setUp() throws Exception {
    CacheUtils.startCache();
  }
  
  @After
  public void tearDown() throws Exception {
    CacheUtils.closeCache();
  }
  
  
  @Test
  public void testLoneFromClause() throws Exception {
    // compileFromClause returns a List<CompiledIteratorDef>
    QCompiler compiler = new QCompiler();
    List list = compiler.compileFromClause("/pos p, p.positions");
    assertEquals(2, list.size());
    
    CompiledIteratorDef first = (CompiledIteratorDef)list.get(0);
    assertEquals("p", first.getName());
    assertEquals("/pos", ((CompiledRegion)first.getCollectionExpr()).getRegionPath());
    assertEquals(TypeUtils.OBJECT_TYPE, first.getElementType());
    
    CompiledIteratorDef second = (CompiledIteratorDef)list.get(1);
    assertNull(second.getName());
    CompiledPath path = (CompiledPath)second.getCollectionExpr();
    assertEquals("p", ((CompiledID)path.getReceiver()).getId());
    assertEquals("positions", path.getTailID());
    assertEquals(TypeUtils.OBJECT_TYPE, second.getElementType());
  }
  
  @Test
  public void testLoneProjectionAttributes() throws Exception {
    // compileProjectionAttributes returns a List or null.
    // null if '*', or a List<Object[2]>.
    // The two-element Object arrays are:
    // 0: a String, the field name, or null if no identifier provided
    // 1: The CompiledValue for the projection expression.
    
    QCompiler compiler = new QCompiler();
    List list = compiler.compileProjectionAttributes("*");
    assertNull(list);
    
    compiler = new QCompiler();
    list = compiler.compileProjectionAttributes("ID, status");
    assertEquals(2, list.size());
    Object[] firstProj = (Object[])list.get(0);
    assertEquals(2, firstProj.length);
    assertNull(firstProj[0]); // no field name
    CompiledID id1 = (CompiledID)firstProj[1];
    assertEquals("ID", id1.getId());
    Object[] secondProj = (Object[])list.get(1);
    assertEquals(2, secondProj.length);
    assertNull(secondProj[0]); // no field name
    CompiledID id2 = (CompiledID)secondProj[1];
    assertEquals("status", id2.getId());
    
    // test two ways of specifying the field names
    compiler = new QCompiler();
    list = compiler.compileProjectionAttributes("x: ID, y: status");
    assertEquals(2, list.size());
    firstProj = (Object[])list.get(0);
    assertEquals(2, firstProj.length);
    assertEquals("x", firstProj[0]);
    id1 = (CompiledID)firstProj[1];
    assertEquals("ID", id1.getId());
    secondProj = (Object[])list.get(1);
    assertEquals(2, secondProj.length);
    assertEquals("y", secondProj[0]);
    id2 = (CompiledID)secondProj[1];
    assertEquals("status", id2.getId());
    
    // the following is invalid
    try {
      compiler = new QCompiler();
      list = compiler.compileProjectionAttributes("ID x, status y");
      fail("Should have thrown a QueryInvalidException");
    } catch (QueryInvalidException e) {
      // pass
    }
    
    // test three ways of specifying the field names
    compiler = new QCompiler();
    list = compiler.compileProjectionAttributes("ID AS x, status as y");
    assertEquals(2, list.size());
    firstProj = (Object[])list.get(0);
    assertEquals(2, firstProj.length);
    assertEquals("x", firstProj[0]);
    id1 = (CompiledID)firstProj[1];
    assertEquals("ID", id1.getId());
    secondProj = (Object[])list.get(1);
    assertEquals(2, secondProj.length);
    assertEquals("y", secondProj[0]);
    id2 = (CompiledID)secondProj[1];
    assertEquals("status", id2.getId());
  }
  
  @Test
  public void testGenerationOfCanonicalizedIteratorNames() {
     try {
       Region rgn = CacheUtils.createRegion("dummy",null);
       
   final IndexManager imgr = new IndexManager(rgn);
   ((LocalRegion)rgn).setIndexManager(imgr);
   String name = imgr.putCanonicalizedIteratorNameIfAbsent("dummy");
   assertTrue("Error as the iterator name was  expected as index_iter1 , but is actually " +name,name.equals("index_iter1"));
     }catch(Exception e) {
       fail("Exception in running the test "+ e);
     }
   
  }
  
  @Test
  public void testConcurrentGenerationOfCanonicalizedIteratorNames () {
    try {
    Region rgn = CacheUtils.createRegion("dummy",null);
    final IndexManager imgr = new IndexManager(rgn);
    ((LocalRegion)rgn).setIndexManager(imgr);
    
    String name = imgr.putCanonicalizedIteratorNameIfAbsent("dummy");
    assertTrue("Error as the iterator name was  expected as index_iter1 , but is actually " +name,name.equals("index_iter1"));
    
    Thread th1 = new Thread ( new Runnable () {
     public void run() {
      IndexCreationInternalsJUnitTest.this.childThreadName1=  imgr.putCanonicalizedIteratorNameIfAbsent("index_iter1.coll1");
     }
    
    } 
      );
    
    Thread th2 = new Thread ( new Runnable () {
      public void run() {
       IndexCreationInternalsJUnitTest.this.childThreadName2=  imgr.putCanonicalizedIteratorNameIfAbsent("index_iter1.coll1");
      }
     
     } 
       );
    
   
    th1.start();
    th2.start();
    name = imgr.putCanonicalizedIteratorNameIfAbsent("index_iter1.coll1");
    DistributedTestCase.join(th1, 30 * 1000, null);
    DistributedTestCase.join(th2, 30 * 1000, null);
    if( !(name.equals(this.childThreadName1) && name.equals(this.childThreadName2)) ) {
     fail("Canonicalization name generation test failed in concurrent scenario as first name is "+this.childThreadName1 + "and second is "+name + " and third is "+this.childThreadName2);   
    }
    System.out.print(" Canonicalized name = "+ name);
    
    }catch(Exception e) {
      fail("Exception in running the test "+ e);
    }
    
  }
}
