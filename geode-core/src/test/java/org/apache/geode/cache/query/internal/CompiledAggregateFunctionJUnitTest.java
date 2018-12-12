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
package org.apache.geode.cache.query.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.query.Aggregator;
import org.apache.geode.cache.query.internal.aggregate.Avg;
import org.apache.geode.cache.query.internal.aggregate.AvgBucketNode;
import org.apache.geode.cache.query.internal.aggregate.AvgDistinct;
import org.apache.geode.cache.query.internal.aggregate.AvgDistinctPRQueryNode;
import org.apache.geode.cache.query.internal.aggregate.AvgPRQueryNode;
import org.apache.geode.cache.query.internal.aggregate.Count;
import org.apache.geode.cache.query.internal.aggregate.CountDistinct;
import org.apache.geode.cache.query.internal.aggregate.CountDistinctPRQueryNode;
import org.apache.geode.cache.query.internal.aggregate.CountPRQueryNode;
import org.apache.geode.cache.query.internal.aggregate.DistinctAggregator;
import org.apache.geode.cache.query.internal.aggregate.MaxMin;
import org.apache.geode.cache.query.internal.aggregate.Sum;
import org.apache.geode.cache.query.internal.aggregate.SumDistinct;
import org.apache.geode.cache.query.internal.aggregate.SumDistinctPRQueryNode;
import org.apache.geode.cache.query.internal.parse.OQLLexerTokenTypes;
import org.apache.geode.internal.cache.InternalCache;

public class CompiledAggregateFunctionJUnitTest {
  private InternalCache cache;
  private List<Integer> bucketList;

  @Before
  public void setUp() throws Exception {
    cache = mock(InternalCache.class);
    bucketList = Collections.singletonList(1);
  }

  @Test
  public void testCount() throws Exception {
    CompiledAggregateFunction caf1 = new CompiledAggregateFunction(null, OQLLexerTokenTypes.COUNT);
    ExecutionContext context1 = new ExecutionContext(null, cache);
    assertThat(caf1.evaluate(context1)).isInstanceOf(Count.class);

    CompiledAggregateFunction caf2 =
        new CompiledAggregateFunction(null, OQLLexerTokenTypes.COUNT, true);
    ExecutionContext context2 = new ExecutionContext(null, cache);
    assertThat(caf2.evaluate(context2)).isInstanceOf(CountDistinct.class);

    CompiledAggregateFunction caf3 = new CompiledAggregateFunction(null, OQLLexerTokenTypes.COUNT);
    ExecutionContext context3 = new ExecutionContext(null, cache);
    context3.setIsPRQueryNode(true);
    assertThat(caf3.evaluate(context3)).isInstanceOf(CountPRQueryNode.class);

    CompiledAggregateFunction caf4 = new CompiledAggregateFunction(null, OQLLexerTokenTypes.COUNT);
    QueryExecutionContext context4 = new QueryExecutionContext(null, cache);

    context4.setBucketList(bucketList);
    assertThat(caf4.evaluate(context4)).isInstanceOf(Count.class);

    CompiledAggregateFunction caf5 =
        new CompiledAggregateFunction(null, OQLLexerTokenTypes.COUNT, true);
    ExecutionContext context5 = new ExecutionContext(null, cache);
    context5.setIsPRQueryNode(true);
    assertThat(caf5.evaluate(context5)).isInstanceOf(CountDistinctPRQueryNode.class);

    CompiledAggregateFunction caf6 =
        new CompiledAggregateFunction(null, OQLLexerTokenTypes.COUNT, true);
    QueryExecutionContext context6 = new QueryExecutionContext(null, cache);
    context6.setBucketList(bucketList);
    assertThat(caf6.evaluate(context6)).isInstanceOf(DistinctAggregator.class);
  }

  @Test
  public void testSum() throws Exception {
    CompiledAggregateFunction caf1 = new CompiledAggregateFunction(null, OQLLexerTokenTypes.SUM);
    ExecutionContext context1 = new ExecutionContext(null, cache);
    assertThat(caf1.evaluate(context1)).isInstanceOf(Sum.class);

    CompiledAggregateFunction caf2 =
        new CompiledAggregateFunction(null, OQLLexerTokenTypes.SUM, true);
    ExecutionContext context2 = new ExecutionContext(null, cache);
    assertThat(caf2.evaluate(context2)).isInstanceOf(SumDistinct.class);

    CompiledAggregateFunction caf3 = new CompiledAggregateFunction(null, OQLLexerTokenTypes.SUM);
    ExecutionContext context3 = new ExecutionContext(null, cache);
    context3.setIsPRQueryNode(true);
    assertThat(caf3.evaluate(context3)).isInstanceOf(Sum.class);

    CompiledAggregateFunction caf4 = new CompiledAggregateFunction(null, OQLLexerTokenTypes.SUM);
    QueryExecutionContext context4 = new QueryExecutionContext(null, cache);
    context4.setBucketList(bucketList);
    assertThat(caf4.evaluate(context4)).isInstanceOf(Sum.class);

    CompiledAggregateFunction caf5 =
        new CompiledAggregateFunction(null, OQLLexerTokenTypes.SUM, true);
    ExecutionContext context5 = new ExecutionContext(null, cache);
    context5.setIsPRQueryNode(true);
    assertThat(caf5.evaluate(context5)).isInstanceOf(SumDistinctPRQueryNode.class);

    CompiledAggregateFunction caf6 =
        new CompiledAggregateFunction(null, OQLLexerTokenTypes.SUM, true);
    QueryExecutionContext context6 = new QueryExecutionContext(null, cache);
    context6.setBucketList(bucketList);
    assertThat(caf6.evaluate(context6)).isInstanceOf(DistinctAggregator.class);
  }

  @Test
  public void testAvg() throws Exception {
    CompiledAggregateFunction caf1 = new CompiledAggregateFunction(null, OQLLexerTokenTypes.AVG);
    ExecutionContext context1 = new ExecutionContext(null, cache);
    assertThat(caf1.evaluate(context1)).isInstanceOf(Avg.class);

    CompiledAggregateFunction caf2 =
        new CompiledAggregateFunction(null, OQLLexerTokenTypes.AVG, true);
    ExecutionContext context2 = new ExecutionContext(null, cache);
    assertThat(caf2.evaluate(context2)).isInstanceOf(AvgDistinct.class);

    CompiledAggregateFunction caf3 = new CompiledAggregateFunction(null, OQLLexerTokenTypes.AVG);
    ExecutionContext context3 = new ExecutionContext(null, cache);
    context3.setIsPRQueryNode(true);
    assertThat(caf3.evaluate(context3)).isInstanceOf(AvgPRQueryNode.class);

    CompiledAggregateFunction caf4 = new CompiledAggregateFunction(null, OQLLexerTokenTypes.AVG);
    QueryExecutionContext context4 = new QueryExecutionContext(null, cache);
    context4.setBucketList(this.bucketList);
    assertThat(caf4.evaluate(context4)).isInstanceOf(AvgBucketNode.class);

    CompiledAggregateFunction caf5 =
        new CompiledAggregateFunction(null, OQLLexerTokenTypes.AVG, true);
    ExecutionContext context5 = new ExecutionContext(null, cache);
    context5.setIsPRQueryNode(true);
    assertThat(caf5.evaluate(context5)).isInstanceOf(AvgDistinctPRQueryNode.class);

    CompiledAggregateFunction caf6 =
        new CompiledAggregateFunction(null, OQLLexerTokenTypes.AVG, true);
    QueryExecutionContext context6 = new QueryExecutionContext(null, cache);
    context6.setBucketList(this.bucketList);
    assertThat(caf6.evaluate(context6)).isInstanceOf(DistinctAggregator.class);
  }

  @Test
  public void testMaxMin() throws Exception {
    CompiledAggregateFunction caf1 = new CompiledAggregateFunction(null, OQLLexerTokenTypes.MAX);
    ExecutionContext context1 = new ExecutionContext(null, cache);
    Aggregator agg = (Aggregator) caf1.evaluate(context1);
    assertThat(agg).isInstanceOf(MaxMin.class);
    MaxMin maxMin = (MaxMin) agg;
    Class maxMinClass = MaxMin.class;
    Field findMax = maxMinClass.getDeclaredField("findMax");
    findMax.setAccessible(true);
    assertThat(findMax.get(maxMin)).isEqualTo(Boolean.TRUE);

    CompiledAggregateFunction caf2 = new CompiledAggregateFunction(null, OQLLexerTokenTypes.MIN);
    Aggregator agg1 = (Aggregator) caf2.evaluate(context1);
    assertThat(agg1).isInstanceOf(MaxMin.class);
    MaxMin maxMin1 = (MaxMin) agg1;
    assertThat(findMax.get(maxMin1)).isEqualTo(Boolean.FALSE);
  }
}
