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
package com.gemstone.gemfire.cache.query.internal.aggregate;

import java.math.BigDecimal;

import junit.framework.TestCase;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.junit.categories.UnitTest;

import junit.framework.TestCase;

/**
 * 
 *
 */
@Category(UnitTest.class)
public class AggregatorJUnitTest extends TestCase{

  @Test
  public void testCount() throws Exception {
    Count count = new Count();
    count.accumulate(new Integer(5));
    count.accumulate(new Integer(6));
    count.accumulate(null);
    assertEquals(2, ((Number)count.terminate()).intValue());
    
    Count  countPrQ = new Count();
    Count count1 = new Count();
    for(int i = 1; i <=5; ++i) {
      count1.accumulate(100);
    }
    
    Count count2 = new Count();
    for(int i = 1; i <=6; ++i) {
      count2.accumulate(100);
    }
    
    countPrQ.merge(count1);
    countPrQ.merge(count2);
       
    assertEquals(11, ((Number)countPrQ.terminate()).intValue());    
  }
  
  @Test
  public void testCountDistinct() throws Exception {
    CountDistinct count = new CountDistinct();
    count.accumulate(new Integer(5));
    count.accumulate(new Integer(6));
    count.accumulate(new Integer(5));
    count.accumulate(new Integer(6));
    count.accumulate(null);
    count.accumulate(null);
    assertEquals(2, ((Number)count.terminate()).intValue());
    
    CountDistinct cdpr = new CountDistinct();
    
    CountDistinct cd1  = new CountDistinct();
    cd1.accumulate(1);
    cd1.accumulate(2);
    cd1.accumulate(3);
    
    CountDistinct cd2  = new CountDistinct();
    cd1.accumulate(3);
    cd1.accumulate(4);
    cd1.accumulate(5);
    
    cdpr.merge(cd1);
    cdpr.merge(cd2);
    assertEquals(5, ((Number)cdpr.terminate()).intValue());    
  }
  
  @Test
  public void testSum() throws Exception {
    Sum sum = new Sum();
    sum.accumulate(new Integer(5));
    sum.accumulate(new Integer(6));
    sum.accumulate(null);
    assertEquals(11, ((Number)sum.terminate()).intValue());  
    
    Sum sumPR = new Sum();
    
    Sum sum1 = new Sum();
    sum1.accumulate(new Integer(5));
    sum1.accumulate(new Integer(6));
    sum1.accumulate(null);
    
    Sum sum2 = new Sum();
    sum2.accumulate(new Integer(7));
    sum2.accumulate(new Integer(8));
    
    
    sumPR.merge(sum1);
    sumPR.merge(sum2);
    
    assertEquals(26, ((Number)sumPR.terminate()).intValue());  
    
  }
  
  @Test
  public void testSumDistinct() throws Exception {
    SumDistinct sum = new SumDistinct();
    sum.accumulate(new Integer(5));
    sum.accumulate(new Integer(6));
    sum.accumulate(null);
    sum.accumulate(new Integer(5));
    sum.accumulate(new Integer(6));
    assertEquals(11, ((Number)sum.terminate()).intValue());
    
    SumDistinct sdpr = new SumDistinct();
   
    SumDistinct sd1 = new SumDistinct();   
    sd1.accumulate(5);
    sd1.accumulate(6);
    sd1.accumulate(3);
    
    SumDistinct sd2 = new SumDistinct();    
    sd1.accumulate(3);
    sd1.accumulate(7);
    sd1.accumulate(8);
   
    
    sdpr.merge(sd1);
    sdpr.merge(sd2);
   
    assertEquals(29, ((Number)sdpr.terminate()).intValue());
  }
  
  @Test
  public void testAvg() throws Exception {
    Avg avg = new Avg();
    avg.accumulate(new Integer(1));
    avg.accumulate(new Integer(2));
    avg.accumulate(new Integer(3));
    avg.accumulate(new Integer(4));
    avg.accumulate(new Integer(5));
    avg.accumulate(new Integer(6));
    avg.accumulate(new Integer(7));
    avg.accumulate(new Integer(7));
    avg.accumulate(null);
    avg.accumulate(null);
    float expected = (1 + 2+ 3 + 4 + 5 + 6 + 7 +7)/8.0f ;    
    assertEquals(expected, ((Number)avg.terminate()).floatValue());
    
   
    Avg avgQ = new Avg();
    
    Avg abn1 = new Avg();
    abn1.accumulate(new Integer(1));
    abn1.accumulate(new Integer(2));
    abn1.accumulate(new Integer(3));
    abn1.accumulate(new Integer(4));
    
    Avg abn2 = new Avg();
    abn2.accumulate(new Integer(5));
    abn2.accumulate(new Integer(6));
    abn2.accumulate(new Integer(7));
    abn2.accumulate(new Integer(8));
    abn2.accumulate(null);
    abn2.accumulate(null); 
    
    avgQ.merge(abn1);
    avgQ.merge(abn2);
    
    expected = (1+2+3 +4 +5 +6 + 7 + 8)/8.0f ;
    assertEquals(expected, ((Number)avgQ.terminate()).floatValue());    
  }
  
  @Test
  public void testAvgDistinct() throws Exception {
    AvgDistinct avg = new AvgDistinct();
    avg.accumulate(new Integer(1));
    avg.accumulate(new Integer(2));
    avg.accumulate(new Integer(2));
    avg.accumulate(new Integer(3));
    avg.accumulate(new Integer(3));
    avg.accumulate(new Integer(4));
    avg.accumulate(new Integer(5));
    avg.accumulate(new Integer(6));
    avg.accumulate(new Integer(7));
    avg.accumulate(new Integer(7));
    avg.accumulate(new Integer(6));
    avg.accumulate(null);
    avg.accumulate(null);
    float expected = (1 + 2+ 3 + 4 + 5 + 6 + 7)/7.0f ;    
    assertEquals(expected, ((Number)avg.terminate()).floatValue());
    
   
    AvgDistinct adpqn = new AvgDistinct();
    
    AvgDistinct ad1 = new AvgDistinct();
    ad1.accumulate(5);
    ad1.accumulate(6);
    ad1.accumulate(3);
    ad1.accumulate(4);
    
    AvgDistinct ad2 = new AvgDistinct();
    ad2.accumulate(3);
    ad2.accumulate(7);
    ad2.accumulate(8);
    ad2.accumulate(4);
    
    adpqn.merge(ad1);
    adpqn.merge(ad2);
   
    expected = (3+4+5+6+7+8)/6.0f ;
    assertEquals(expected, ((Number)adpqn.terminate()).floatValue());    
  }
  
  @Test
  public void testMaxMin() throws Exception {
    MaxMin max = new MaxMin(true);
    max.accumulate(new Integer(1));
    assertEquals(1,((Integer)max.terminate()).intValue());
    max.accumulate(new Integer(2));
    max.accumulate(null);
    assertEquals(2,((Integer)max.terminate()).intValue());
    
    MaxMin min = new MaxMin(false);
    min.accumulate(new Integer(1));
    assertEquals(1,((Integer)min.terminate()).intValue());
    min.accumulate(new Integer(2));
    min.accumulate(null);
    assertEquals(1,((Integer)min.terminate()).intValue());
  }
  
  @Test 
  public void testDowncast() throws Exception {
   new BigDecimal(1d).longValueExact();
    Sum sum = new Sum();
    sum.accumulate(new Integer(Integer.MAX_VALUE));
    sum.accumulate(new Integer(6));
    Number result = (Number)sum.terminate();
    assertTrue( result instanceof Long);
    assertEquals(Integer.MAX_VALUE + 6l, result.longValue());
    
    sum = new Sum();
    sum.accumulate(new Long(5));
    sum.accumulate(new Integer(6));
    result = (Number)sum.terminate();
    assertTrue( result instanceof Integer);
    assertEquals(11, result.intValue());
    
    sum = new Sum();
    sum.accumulate(Float.MAX_VALUE);
    result = (Number)sum.terminate();
    assertTrue( result instanceof Double);
    assertEquals(Float.MAX_VALUE, result.floatValue());
    
  }
 
}
