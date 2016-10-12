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
package org.apache.geode.cache.query.functional;

public interface GroupByTestInterface {


  public void testConvertibleGroupByQuery_1() throws Exception;

  public void testConvertibleGroupByQuery_refer_column() throws Exception;

  public void testConvertibleGroupByQuery_refer_column_alias_Bug520141() throws Exception;

  public void testAggregateFuncCountStar() throws Exception;

  public void testAggregateFuncCountDistinctStar_1() throws Exception;

  public void testAggregateFuncCountDistinctStar_2() throws Exception;

  public void testAggregateFuncSum() throws Exception;

  public void testAggregateFuncSumDistinct() throws Exception;

  public void testAggregateFuncNoGroupBy() throws Exception;

  public void testAggregateFuncAvg() throws Exception;

  public void testAggregateFuncAvgDistinct() throws Exception;

  public void testAggregateFuncWithOrderBy() throws Exception;

  public void testComplexValueAggregateFuncAvgDistinct() throws Exception;

  public void testAggregateFuncMax() throws Exception;

  public void testSumWithMultiColumnGroupBy() throws Exception;

  public void testAggregateFuncMin() throws Exception;

  public void testCompactRangeIndex() throws Exception;

  public void testDistinctCountWithoutGroupBy() throws Exception;

  public void testLimitWithGroupBy() throws Exception;

}
