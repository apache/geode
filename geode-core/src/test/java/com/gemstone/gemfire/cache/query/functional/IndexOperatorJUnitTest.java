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
 * CompileIndexOperatorTest.java
 *
 * Created on March 23, 2005, 4:52 PM
 */
package com.gemstone.gemfire.cache.query.functional;

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashMap;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.TypeMismatchException;
import com.gemstone.gemfire.cache.query.Utils;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 *
 */
@Category(IntegrationTest.class)
public class IndexOperatorJUnitTest {
  
  @Before
  public void setUp() throws Exception {
    CacheUtils.startCache();
  }
  
  @After
  public void tearDown() throws Exception {
    CacheUtils.closeCache();
  }
  
  @Test
  public void testWithString() throws Exception {
    String str = "xyz";
    Character c = (Character)runQuery(str, 0);
    if(c.charValue() != 'x')
      fail();
    Character d = (Character)runQuery(str, 2);
    if(d.charValue() != 'z')
      fail();
  }
  
  @Test
  public void testWithArray() throws Exception {
    Object result = null;
    int index = 1;
    String stringArray[] = {"a","b"};
    result = runQuery(stringArray, index);
    if(result == null || !stringArray[index].equals(result))
      fail("failed for String array");
    
    int intArray[] = {1,2};
    result = runQuery(intArray, index);
    if(result == null || intArray[index] != ((Integer)result).intValue())
      fail("failed for int array");
    
    Object objectArray[] = {"a","b"};
    result = runQuery(objectArray, index);
    if(result == null || !objectArray[index].equals(result))
      fail("failed for String array");
    
  }
  
  @Test
  public void testWithList() throws Exception {
    ArrayList list = new ArrayList();
    list.add("aa");
    list.add("bb");
    Object result = null;
    int index = 1;
    result = runQuery(list, index);
    if(result == null || !list.get(index).equals(result))
      fail("failed for List");
  }
  
  @Test
  public void testWithMap() throws Exception {
    
    HashMap map = new HashMap();
    map.put("0",new Integer(11));
    map.put("1",new Integer(12));
    Object result = null;
    Object index = "1";
    result = runQuery(map, index);
    if(result == null || !map.get(index).equals(result))
      fail("failed for Map");
  }
  
  @Test
  public void testWithRegion() throws Exception {
    
    Region region = CacheUtils.createRegion("Portfolio", Portfolio.class);
    for(int i=0;i<5;i++){
      region.put(""+i, new Portfolio(i));
    }
    Object result = null;
    Object index="2";
    result=runQuery(region, index);
    if(result == null || !region.get(index).equals(result))
      fail("failed for Region");
  }
  
  @Test
  public void testIndexOfIndex() throws Exception{
    String array[] = { "abc", "def"};
    Query q = CacheUtils.getQueryService().newQuery("$1[0][0]");
    Object params[] = {array, new Integer(0)};
    Character result = (Character)q.execute(params);
    CacheUtils.log(Utils.printResult(result));
    if(result == null || result.charValue() != 'a')
      fail();
  }
  
  @Test
  public void testWithNULL() throws Exception{
    runQuery(null, 0);
    runQuery(null, null);
    Object objectArray[] = {"a","b"};
    try{
      runQuery(objectArray, null);
      fail();
    }catch(TypeMismatchException e){
    }
    HashMap map = new HashMap();
    map.put("0",new Integer(11));
    map.put("1",new Integer(12));
    Object result = runQuery(map, null);
    if(result != null)
      fail();
  }
  
  @Test
  public void testWithUNDEFINED() throws Exception{
    try{
      runQuery(QueryService.UNDEFINED, 0);
    }catch(TypeMismatchException e){
      fail();
    }
    try{
      runQuery(QueryService.UNDEFINED, QueryService.UNDEFINED);
    }catch(TypeMismatchException e){
      fail();
    }
    Object objectArray[] = {"a","b"};
    try{
      runQuery(objectArray, QueryService.UNDEFINED);
      fail();
    }catch(TypeMismatchException e){
    }
    HashMap map = new HashMap();
    map.put("0",new Integer(11));
    map.put("1",new Integer(12));
    Object result = runQuery(map, QueryService.UNDEFINED);
    if(result != null)
      fail();
  }
  
  @Test
  public void testWithUnsupportedArgs() throws Exception{
    try{
      runQuery("a","a");
      fail();
    }catch(TypeMismatchException e){
    }
    
    try{
      runQuery(new Object(), 0);
      fail();
    }catch(TypeMismatchException e){
    }
    
    try{
      Object objectArray[] = {"a","b"};
      runQuery(objectArray, new Object());
      fail();
    }catch(TypeMismatchException e){
    }
  }
  
  public Object runQuery(Object array, Object index) throws Exception{
    Query q = CacheUtils.getQueryService().newQuery("$1[$2]");
    Object params[] = {array, index};
    Object result = q.execute(params);
    CacheUtils.log(Utils.printResult(result));
    return result;
  }
  
  public Object runQuery(Object array, int index) throws Exception{
    Query q = CacheUtils.getQueryService().newQuery("$1[$2]");
    Object params[] = {array, new Integer(index)};
    Object result = q.execute(params);
    CacheUtils.log(Utils.printResult(result));
    return result;
  }
  
  
}
