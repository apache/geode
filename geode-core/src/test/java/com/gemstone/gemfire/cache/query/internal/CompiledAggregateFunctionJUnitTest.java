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
package com.gemstone.gemfire.cache.query.internal;

import static org.junit.Assert.*;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.jmock.Mockery;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.query.Aggregator;
import com.gemstone.gemfire.cache.query.internal.aggregate.Avg;
import com.gemstone.gemfire.cache.query.internal.aggregate.AvgBucketNode;
import com.gemstone.gemfire.cache.query.internal.aggregate.AvgDistinct;
import com.gemstone.gemfire.cache.query.internal.aggregate.AvgDistinctPRQueryNode;
import com.gemstone.gemfire.cache.query.internal.aggregate.AvgPRQueryNode;
import com.gemstone.gemfire.cache.query.internal.aggregate.Count;
import com.gemstone.gemfire.cache.query.internal.aggregate.CountDistinct;
import com.gemstone.gemfire.cache.query.internal.aggregate.CountDistinctPRQueryNode;
import com.gemstone.gemfire.cache.query.internal.aggregate.CountPRQueryNode;
import com.gemstone.gemfire.cache.query.internal.aggregate.DistinctAggregator;
import com.gemstone.gemfire.cache.query.internal.aggregate.MaxMin;
import com.gemstone.gemfire.cache.query.internal.aggregate.Sum;
import com.gemstone.gemfire.cache.query.internal.aggregate.SumDistinct;
import com.gemstone.gemfire.cache.query.internal.aggregate.SumDistinctPRQueryNode;
import com.gemstone.gemfire.cache.query.internal.parse.OQLLexerTokenTypes;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class CompiledAggregateFunctionJUnitTest {
  
  private Mockery context;
  private Cache cache;
  private List bucketList;
  
  @Before
  public void setUp() throws Exception {
    context = new Mockery();
    cache = context.mock(Cache.class);
    bucketList = new ArrayList();
    bucketList.add(Integer.valueOf(1));
  }

  @Test
  public void testCount() throws Exception {
    CompiledAggregateFunction caf1 = new CompiledAggregateFunction(null,
        OQLLexerTokenTypes.COUNT);
    ExecutionContext context1 = new ExecutionContext(null,cache);
    assertTrue(caf1.evaluate(context1) instanceof Count);

    CompiledAggregateFunction caf2 = new CompiledAggregateFunction(null,
        OQLLexerTokenTypes.COUNT, true);
    ExecutionContext context2 = new ExecutionContext(null,cache);
    assertTrue(caf2.evaluate(context2) instanceof CountDistinct);

    CompiledAggregateFunction caf3 = new CompiledAggregateFunction(null,
        OQLLexerTokenTypes.COUNT);
    ExecutionContext context3 = new ExecutionContext(null,cache);
    context3.setIsPRQueryNode(true);
    assertTrue(caf3.evaluate(context3) instanceof CountPRQueryNode);

    CompiledAggregateFunction caf4 = new CompiledAggregateFunction(null,
        OQLLexerTokenTypes.COUNT);
    QueryExecutionContext context4 = new QueryExecutionContext(null,cache);
   
    context4.setBucketList(bucketList);
    assertTrue(caf4.evaluate(context4) instanceof Count);

    CompiledAggregateFunction caf5 = new CompiledAggregateFunction(null,
        OQLLexerTokenTypes.COUNT, true);
    ExecutionContext context5 = new ExecutionContext(null,cache);
    context5.setIsPRQueryNode(true);
    assertTrue(caf5.evaluate(context5) instanceof CountDistinctPRQueryNode);

    CompiledAggregateFunction caf6 = new CompiledAggregateFunction(null,
        OQLLexerTokenTypes.COUNT, true);
    QueryExecutionContext context6 = new QueryExecutionContext(null,cache);
    context6.setBucketList(bucketList);
    assertTrue(caf6.evaluate(context6) instanceof DistinctAggregator);
  }

  @Test
  public void testSum() throws Exception {
    CompiledAggregateFunction caf1 = new CompiledAggregateFunction(null,
        OQLLexerTokenTypes.SUM);
    ExecutionContext context1 = new ExecutionContext(null,cache);
    assertTrue(caf1.evaluate(context1) instanceof Sum);

    CompiledAggregateFunction caf2 = new CompiledAggregateFunction(null,
        OQLLexerTokenTypes.SUM, true);
    ExecutionContext context2 = new ExecutionContext(null,cache);
    assertTrue(caf2.evaluate(context2) instanceof SumDistinct);

    CompiledAggregateFunction caf3 = new CompiledAggregateFunction(null,
        OQLLexerTokenTypes.SUM);
    ExecutionContext context3 = new ExecutionContext(null,cache);
    context3.setIsPRQueryNode(true);
    assertTrue(caf3.evaluate(context3) instanceof Sum);

    CompiledAggregateFunction caf4 = new CompiledAggregateFunction(null,
        OQLLexerTokenTypes.SUM);
    QueryExecutionContext context4 = new QueryExecutionContext(null,cache);
    context4.setBucketList(bucketList);
    assertTrue(caf4.evaluate(context4) instanceof Sum);

    CompiledAggregateFunction caf5 = new CompiledAggregateFunction(null,
        OQLLexerTokenTypes.SUM, true);
    ExecutionContext context5 = new ExecutionContext(null,cache);
    context5.setIsPRQueryNode(true);
    assertTrue(caf5.evaluate(context5) instanceof SumDistinctPRQueryNode);

    CompiledAggregateFunction caf6 = new CompiledAggregateFunction(null,
        OQLLexerTokenTypes.SUM, true);
    QueryExecutionContext context6 = new QueryExecutionContext(null,cache);
    context6.setBucketList(bucketList);
    assertTrue(caf6.evaluate(context6) instanceof DistinctAggregator);
  }

  @Test
  public void testAvg() throws Exception {
    CompiledAggregateFunction caf1 = new CompiledAggregateFunction(null,
        OQLLexerTokenTypes.AVG);
    ExecutionContext context1 = new ExecutionContext(null,cache);
    assertTrue(caf1.evaluate(context1) instanceof Avg);

    CompiledAggregateFunction caf2 = new CompiledAggregateFunction(null,
        OQLLexerTokenTypes.AVG, true);
    ExecutionContext context2 = new ExecutionContext(null,cache);
    assertTrue(caf2.evaluate(context2) instanceof AvgDistinct);

    CompiledAggregateFunction caf3 = new CompiledAggregateFunction(null,
        OQLLexerTokenTypes.AVG);
    ExecutionContext context3 = new ExecutionContext(null,cache);
    context3.setIsPRQueryNode(true);
    assertTrue(caf3.evaluate(context3) instanceof AvgPRQueryNode);

    CompiledAggregateFunction caf4 = new CompiledAggregateFunction(null,
        OQLLexerTokenTypes.AVG);
    QueryExecutionContext context4 = new QueryExecutionContext(null,cache);
    context4.setBucketList(this.bucketList);
    assertTrue(caf4.evaluate(context4) instanceof AvgBucketNode);

    CompiledAggregateFunction caf5 = new CompiledAggregateFunction(null,
        OQLLexerTokenTypes.AVG, true);
    ExecutionContext context5 = new ExecutionContext(null,cache);
    context5.setIsPRQueryNode(true);
    assertTrue(caf5.evaluate(context5) instanceof AvgDistinctPRQueryNode);

    CompiledAggregateFunction caf6 = new CompiledAggregateFunction(null,
        OQLLexerTokenTypes.AVG, true);
    QueryExecutionContext context6 = new QueryExecutionContext(null,cache);
    context6.setBucketList(this.bucketList);
    assertTrue(caf6.evaluate(context6) instanceof DistinctAggregator);
  }

  @Test
  public void testMaxMin() throws Exception {
    CompiledAggregateFunction caf1 = new CompiledAggregateFunction(null,
        OQLLexerTokenTypes.MAX);
    ExecutionContext context1 = new ExecutionContext(null,cache);
    Aggregator agg = (Aggregator) caf1.evaluate(context1);
    assertTrue(agg instanceof MaxMin);
    MaxMin maxMin = (MaxMin) agg;
    Class maxMinClass = MaxMin.class;
    Field findMax = maxMinClass.getDeclaredField("findMax");
    findMax.setAccessible(true);
    assertTrue(((Boolean) findMax.get(maxMin)).booleanValue());

    CompiledAggregateFunction caf2 = new CompiledAggregateFunction(null,
        OQLLexerTokenTypes.MIN);
    ExecutionContext context2 = new ExecutionContext(null,cache);
    Aggregator agg1 = (Aggregator) caf2.evaluate(context1);
    assertTrue(agg1 instanceof MaxMin);
    MaxMin maxMin1 = (MaxMin) agg1;
    assertFalse(((Boolean) findMax.get(maxMin1)).booleanValue());
  }
}
