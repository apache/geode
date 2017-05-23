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


  void testConvertibleGroupByQuery_1() throws Exception;

  void testConvertibleGroupByQuery_refer_column() throws Exception;

  void testConvertibleGroupByQuery_refer_column_alias_Bug520141() throws Exception;

  void testAggregateFuncCountStar() throws Exception;

  void testAggregateFuncCountDistinctStar_1() throws Exception;

  void testAggregateFuncCountDistinctStar_2() throws Exception;

  void testAggregateFuncSum() throws Exception;

  void testAggregateFuncSumDistinct() throws Exception;

  void testAggregateFuncNoGroupBy() throws Exception;

  void testAggregateFuncAvg() throws Exception;

  void testAggregateFuncAvgDistinct() throws Exception;

  void testAggregateFuncWithOrderBy() throws Exception;

  void testComplexValueAggregateFuncAvgDistinct() throws Exception;

  void testAggregateFuncMax() throws Exception;

  void testSumWithMultiColumnGroupBy() throws Exception;

  void testAggregateFuncMin() throws Exception;

  void testCompactRangeIndex() throws Exception;

  void testDistinctCountWithoutGroupBy() throws Exception;

  void testLimitWithGroupBy() throws Exception;

}
