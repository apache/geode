/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * ComparisonOperatorsJUnitTest.java
 * JUnit based test
 *
 * Created on March 10, 2005, 3:14 PM
 */
package com.gemstone.gemfire.cache.query.functional;



import static org.junit.Assert.fail;

import java.util.Collection;
import java.util.Iterator;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.CacheUtils;
//import com.gemstone.gemfire.cache.query.IndexType;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 *
 * @author vaibhav
 */
@Category(IntegrationTest.class)
public class ComparisonOperatorsJUnitTest  {
  
  public ComparisonOperatorsJUnitTest() {
  }
  
  public String getName() {
    return this.getClass().getSimpleName();
  }
  
  @Before
  public void setUp() throws java.lang.Exception {
    CacheUtils.startCache();
    Region region = CacheUtils.createRegion("Portfolios", Portfolio.class);
    region.put("0",new Portfolio(0));
    region.put("1",new Portfolio(1));
    region.put("2",new Portfolio(2));
    region.put("3",new Portfolio(3));
  }
  
  @After
  public void tearDown() throws java.lang.Exception {
    CacheUtils.closeCache();
  }
  
  String operators[] ={"=","<>","!=","<","<=",">",">="};

  @Test
  public void testCompareWithInt() throws Exception{
    String var = "ID";
    int value = 2;
    QueryService qs = CacheUtils.getQueryService();
    for(int i=0;i<operators.length;i++){
      Query query = qs.newQuery("SELECT DISTINCT * FROM /Portfolios where "+var+operators[i]+value);
      Object result = query.execute();
      if(result instanceof Collection){
        Iterator iter = ((Collection)result).iterator();
        while(iter.hasNext()){
          boolean isPassed = false;
          Portfolio p = (Portfolio)iter.next();
          switch(i){
            case 0 : isPassed = (p.getID() == value); break;
            case 1 : isPassed = (p.getID() != value); break;
            case 2 : isPassed = (p.getID() != value); break;
            case 3 : isPassed = (p.getID() < value); break;
            case 4 : isPassed = (p.getID() <= value); break;
            case 5 : isPassed = (p.getID() > value); break;
            case 6 : isPassed = (p.getID() >= value); break;
          }
          if(!isPassed)
            fail(this.getName()+" failed for operator "+operators[i]);
        }
      }else{
        fail(this.getName()+" failed for operator "+operators[i]);
      }
    }
  }
  
  @Test
  public void testCompareWithString() throws Exception{
    String var = "P1.secId";
    String value = "DELL";
    QueryService qs = CacheUtils.getQueryService();
    for(int i=0;i<operators.length;i++){
      Query query = qs.newQuery("SELECT DISTINCT * FROM /Portfolios where "+var+operators[i]+"'"+value+"'");
      Object result = query.execute();
      if(result instanceof Collection){
        Iterator iter = ((Collection)result).iterator();
        while(iter.hasNext()){
          boolean isPassed = false;
          Portfolio p = (Portfolio)iter.next();
          switch(i){
            case 0 : isPassed = (p.getP1().getSecId().compareTo(value) == 0); break;
            case 1 : isPassed = (p.getP1().getSecId().compareTo(value) != 0); break;
            case 2 : isPassed = (p.getP1().getSecId().compareTo(value) != 0); break;
            case 3 : isPassed = (p.getP1().getSecId().compareTo(value) < 0); break;
            case 4 : isPassed = (p.getP1().getSecId().compareTo(value) <= 0); break;
            case 5 : isPassed = (p.getP1().getSecId().compareTo(value) > 0); break;
            case 6 : isPassed = (p.getP1().getSecId().compareTo(value) >= 0); break;
          }
          if(!isPassed)
            fail(this.getName()+" failed for operator "+operators[i]);
        }
      }else{
        fail(this.getName()+" failed for operator "+operators[i]);
      }
    }
  }
  
  @Test
  public void testCompareWithNULL() throws Exception{
    String var ="P2";
    Object value = null;
    QueryService qs = CacheUtils.getQueryService();
    for(int i=0;i<operators.length;i++){
      Query query = qs.newQuery("SELECT DISTINCT * FROM /Portfolios where "+var+operators[i]+value);
      Object result = query.execute();
      if(result instanceof Collection){
        Iterator iter = ((Collection)result).iterator();
        while(iter.hasNext()){
          boolean isPassed = false;
          Portfolio p = (Portfolio)iter.next();
          switch(i){
            case 0 : isPassed = (p.getP2() == value); break;
            default : isPassed = (p.getP2() != value); break;
          }
          if(!isPassed)
            fail(this.getName()+" failed for operator "+operators[i]);
        }
      }else{
        fail(this.getName()+" failed for operator "+operators[i]);
      }
    }
  }
  
  @Test
  public void testCompareWithUNDEFINED() throws Exception{
    String var = "P2.secId";
    QueryService qs = CacheUtils.getQueryService();
    for(int i=0;i<operators.length;i++){
      // According to docs:
      // To perform equality or inequality comparisons with UNDEFINED, use the
      // IS_DEFINED and IS_UNDEFINED preset query functions instead of these
      // comparison operators.
      if(!operators[i].equals("=") && !operators[i].equals("!=") && !operators[i].equals("<>")) {
        Query query = qs.newQuery("SELECT DISTINCT * FROM /Portfolios where "+var+operators[i]+" UNDEFINED");
        Object result = query.execute();
        if(result instanceof Collection){
          if(((Collection)result).size() != 0)
            fail(this.getName()+" failed for operator "+operators[i]);
        }else{
          fail(this.getName()+" failed for operator "+operators[i]);
        }
      }
    }
  }
}
