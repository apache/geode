package com.gemstone.gemfire.cache.query.functional;

public interface GroupByTestInterface {


  public void testConvertibleGroupByQuery_1() throws Exception ;

  public void testConvertibleGroupByQuery_refer_column() throws Exception;

  public void testConvertibleGroupByQuery_refer_column_alias_Bug520141()
      throws Exception;

  public void testAggregateFuncCountStar() throws Exception ;
  public void testAggregateFuncCountDistinctStar_1() throws Exception;

  public void testAggregateFuncCountDistinctStar_2() throws Exception ;
  public void testAggregateFuncSum() throws Exception ;

  public void testAggregateFuncSumDistinct() throws Exception ;
  public void testAggregateFuncNoGroupBy() throws Exception ;

  public void testAggregateFuncAvg() throws Exception ;

  public void testAggregateFuncAvgDistinct() throws Exception ;

  public void testAggregateFuncWithOrderBy() throws Exception ;

  public void testComplexValueAggregateFuncAvgDistinct() throws Exception;

  public void testAggregateFuncMax() throws Exception ;

  public void testSumWithMultiColumnGroupBy() throws Exception ;

  public void testAggregateFuncMin() throws Exception ;
  
  public void testCompactRangeIndex() throws Exception ;
  
  public void testDistinctCountWithoutGroupBy() throws Exception ;
  
  public void testLimitWithGroupBy() throws Exception ;
  
}
