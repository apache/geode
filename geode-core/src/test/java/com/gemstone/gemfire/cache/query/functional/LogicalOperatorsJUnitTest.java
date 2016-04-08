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
 * LogicalOperatorsJUnitTest.java
 * JUnit based test
 *
 * Created on March 11, 2005, 12:50 PM
 */

package com.gemstone.gemfire.cache.query.functional;

import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 *
 */
@Category(IntegrationTest.class)
public class LogicalOperatorsJUnitTest {
  
  @Before
  public void setUp() throws java.lang.Exception {
    CacheUtils.startCache();
    Region region = CacheUtils.createRegion("Portfolios", Portfolio.class);
    for(int i=0;i<5;i++){
      region.put(""+i, new Portfolio(i));
    }
  }
  
  @After
  public void tearDown() throws java.lang.Exception {
    CacheUtils.closeCache();
  }
  
  Object validOperands[]={Boolean.TRUE, Boolean.FALSE, null, QueryService.UNDEFINED };
  
  Object invalidOperands[]={ new Integer(0), "a"};
  
  
  @Test
  public void testAND() throws Exception {
    QueryService qs = CacheUtils.getQueryService();
    Object params[] = new Object[2];
    for(int i=0;i<validOperands.length;i++){
      for(int j=0;j<validOperands.length;j++){
        Query query = qs.newQuery("$1 AND $2");
        params[0] = validOperands[i];
        params[1] = validOperands[j];
        Object result = query.execute(params);
        //CacheUtils.log("LogicalTest "+validOperands[i]+" AND "+validOperands[j]+" = "+result+" "+checkResult("AND", result, validOperands[i], validOperands[j]));
        if(!checkResult("AND", result, validOperands[i], validOperands[j]))
          fail(validOperands[i]+" AND "+validOperands[j]+" returns "+result);
      }
    }
    for(int i=0;i<validOperands.length;i++){
      for(int j=0;j<invalidOperands.length;j++){
        Query query = qs.newQuery("$1 AND $2");
        params[0] = validOperands[i];
        params[1] = invalidOperands[j];
        try{
          Object result = query.execute(params);
          fail(validOperands[i]+" AND "+validOperands[j]+" returns "+result);
        }catch(Exception e){
          
        }
      }
    }
  }
  
  @Test
  public void testOR() throws Exception {
    QueryService qs = CacheUtils.getQueryService();
    Object params[] = new Object[2];
    for(int i=0;i<validOperands.length;i++){
      for(int j=0;j<validOperands.length;j++){
        Query query = qs.newQuery("$1 OR $2");
        params[0] = validOperands[i];
        params[1] = validOperands[j];
        Object result = query.execute(params);
        //CacheUtils.log("LogicalTest "+validOperands[i]+" OR "+validOperands[j]+" = "+result+" "+checkResult("OR", result, validOperands[i], validOperands[j]));
        if(!checkResult("OR", result, validOperands[i], validOperands[j]))
          fail(validOperands[i]+" OR "+validOperands[j]+" returns "+result);
      }
    }
    
    for(int i=0;i<validOperands.length;i++){
      for(int j=0;j<invalidOperands.length;j++){
        Query query = qs.newQuery("$1 OR $2");
        params[0] = validOperands[i];
        params[1] = invalidOperands[j];
        try{
          Object result = query.execute(params);
          fail(validOperands[i]+" OR "+validOperands[j]+" returns "+result);
        }catch(Exception e){
          
        }
      }
    }
  }
  
  @Test
  public void testNOT() throws Exception {
    QueryService qs = CacheUtils.getQueryService();
    for(int i=0;i<validOperands.length;i++){
      Query query = qs.newQuery("NOT $"+(i+1));
      Object result = query.execute(validOperands);
      //CacheUtils.log("LogicalTest "+"NOT "+validOperands[i]+" = "+result+" "+checkResult("NOT", result, validOperands[i], null));
      if(!checkResult("NOT", result, validOperands[i], null))
        fail("NOT "+validOperands[i]+" returns "+result);
    }
    
    for(int j=0;j<invalidOperands.length;j++){
      Query query = qs.newQuery("NOT $"+(j+1));
      try{
        Object result = query.execute(invalidOperands);
        fail("NOT "+invalidOperands[j]+" returns "+result);
      }catch(Exception e){
        
      }
    }
  }
  
  private boolean checkResult(String operator, Object result, Object operand1, Object operand2){
    try{
      Object temp;
      if(operand1 == null){
        temp = operand1;
        operand1 = operand2;
        operand2 = temp;
      }
      if(operand1 == null)
        return result.equals(QueryService.UNDEFINED);
      
      if(operator.equalsIgnoreCase("AND")){
        
        if(operand1.equals(Boolean.FALSE))
          return result.equals(Boolean.FALSE);
        
        if(operand2 != null && operand2.equals(Boolean.FALSE))
          return result.equals(Boolean.FALSE);
        
        if(operand1 == QueryService.UNDEFINED || operand2 == QueryService.UNDEFINED)
          return result.equals(QueryService.UNDEFINED);
        
        if(operand2 == null)
          return result.equals(QueryService.UNDEFINED);
        
        return result.equals(Boolean.TRUE);
        
      }else if(operator.equalsIgnoreCase("OR")){
        
        if(operand1.equals(Boolean.TRUE))
          return result.equals(Boolean.TRUE);
        
        if(operand2 != null && operand2.equals(Boolean.TRUE))
          return result.equals(Boolean.TRUE);
        
        if(operand1 == QueryService.UNDEFINED || operand2 == QueryService.UNDEFINED)
          return result == QueryService.UNDEFINED;
        
        if(/*operand1 == null || not possible */ operand2 == null)
          return result == QueryService.UNDEFINED;
        
        return result.equals(Boolean.FALSE);
        
      }else if(operator.equalsIgnoreCase("NOT")){
        if(operand1 instanceof Boolean)
          return ((Boolean)result).booleanValue() != ((Boolean)operand1).booleanValue();
        return result == QueryService.UNDEFINED;
      }
    }catch(Exception e){
    }
    return false;
  }
}
