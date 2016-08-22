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

import static org.junit.Assert.*;

import java.util.HashSet;
import java.util.Set;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class AggregatorJUnitTest {

  @Test
  public void testCount() throws Exception {
    Count count = new Count();
    count.accumulate(new Integer(5));
    count.accumulate(new Integer(6));
    count.accumulate(null);
    assertEquals(2, ((Number)count.terminate()).intValue());
    
    CountPRQueryNode countPrQ = new CountPRQueryNode();
    countPrQ.accumulate(new Integer(5));
    countPrQ.accumulate(new Integer(6));    
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
    
    CountDistinctPRQueryNode cdpr = new CountDistinctPRQueryNode();
    
    Set<Integer> set1 = new HashSet<Integer>();
    set1.add(1);
    set1.add(2);
    set1.add(3);
    
    Set<Integer> set2 = new HashSet<Integer>();
    set2.add(3);
    set2.add(4);
    set2.add(5);
    
    cdpr.accumulate(set1);
    cdpr.accumulate(set2);
    assertEquals(5, ((Number)cdpr.terminate()).intValue());    
  }
  
  @Test
  public void testSum() throws Exception {
    Sum sum = new Sum();
    sum.accumulate(new Integer(5));
    sum.accumulate(new Integer(6));
    sum.accumulate(null);
    assertEquals(11, ((Number)sum.terminate()).intValue());      
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
    
    SumDistinctPRQueryNode sdpr = new SumDistinctPRQueryNode();
   
    Set<Integer> set1 = new HashSet<Integer>();
    set1.add(5);
    set1.add(6);
    set1.add(3);
    
    Set<Integer> set2 = new HashSet<Integer>();
    set2.add(3);
    set2.add(7);
    set2.add(8);
    
    sdpr.accumulate(set1);
    sdpr.accumulate(set2);
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
    assertEquals(expected, ((Number)avg.terminate()).floatValue(), 0);
    
    AvgBucketNode abn = new AvgBucketNode();
    abn.accumulate(new Integer(1));
    abn.accumulate(new Integer(2));
    abn.accumulate(new Integer(3));
    abn.accumulate(new Integer(4));
    abn.accumulate(new Integer(5));
    abn.accumulate(new Integer(6));
    abn.accumulate(new Integer(7));
    abn.accumulate(new Integer(7));
    abn.accumulate(null);
    abn.accumulate(null); 
    Object[] arr = (Object[]) abn.terminate();
    assertEquals(8, ((Integer)arr[0]).intValue());
    assertEquals(35, ((Number)arr[1]).intValue());
    
    AvgPRQueryNode apqn = new AvgPRQueryNode();
    Object[] val1 = new Object[]{new Integer(7), new Double(43)};
    Object[] val2 = new Object[]{new Integer(5), new Double(273.86)};
    apqn.accumulate(val1);
    apqn.accumulate(val2);
    expected = (43+273.86f)/12.0f ;
    assertEquals(expected, ((Number)apqn.terminate()).floatValue(), 0);
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
    assertEquals(expected, ((Number)avg.terminate()).floatValue(), 0);

    AvgDistinctPRQueryNode adpqn = new AvgDistinctPRQueryNode();
    
    Set<Integer> set1 = new HashSet<Integer>();
    set1.add(5);
    set1.add(6);
    set1.add(3);
    set1.add(4);
    
    Set<Integer> set2 = new HashSet<Integer>();
    set2.add(3);
    set2.add(7);
    set2.add(8);
    set2.add(4);
    
    adpqn.accumulate(set1);
    adpqn.accumulate(set2);
   
    expected = (3+4+5+6+7+8)/6.0f ;
    assertEquals(expected, ((Number)adpqn.terminate()).floatValue(), 0);
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
 
}
