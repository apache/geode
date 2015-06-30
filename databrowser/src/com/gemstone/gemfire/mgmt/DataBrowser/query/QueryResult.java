/*========================================================================= 
 * (c)Copyright 2002-2009, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006 
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.query;

import java.util.Collection;
import java.util.Map;

import com.gemstone.gemfire.cache.query.QueryStatistics;
import com.gemstone.gemfire.mgmt.DataBrowser.query.internal.ResultSetImpl;

/**
 * This class represents the results of a GemFire query execution which also
 * includes, 1. Meta-data about results of query. 2. GemFire Query execution
 * statistics.
 * 
 * @author Hrishi
 **/
public class QueryResult implements ResultSet {

  private ResultSet                        result;  
  private QueryStatistics                  statistics;
  
  public QueryResult(Map<Object, IntrospectionResult> typesInRes,
       Collection<?> res, QueryStatistics stats) {
    result = new ResultSetImpl(typesInRes, res);
    statistics = stats;
  }
  
  public QueryResult(ResultSet res, QueryStatistics stats) {
    result = res;
    statistics = stats;
  }

  public boolean isEmpty() {
    return result.isEmpty();
  }

  /**
   * This method returns the meta-data about the result of executed query.
   * 
   * @return The meta-data of result of the query execution.
   */
  public IntrospectionResult[] getIntrospectionResult() {
    return result.getIntrospectionResult();
  }

  /**
   * This method returns the results of the executed query.
   * 
   * @return The result of query execution.
   */
  public Collection<?> getQueryResult() {
    return this.result.getQueryResult();
  }

  /**
   * This method returns the subset of result of the executed query for a given
   * introspection type.
   * 
   * @param metaInfo
   *          IntrospectionResult type for which we require result
   * @return subset of result of the query execution.
   */
  public Collection<?> getQueryResult(IntrospectionResult metaInfo) {
    return this.result.getQueryResult(metaInfo);
  }

  /**
   * This method returns the statistics of the Query execution.
   * 
   * @return The GemFire query execution statistics.
   */
  public QueryStatistics getQueryStatistics() {
    return this.statistics;
  }

  /**
   * This method returns the value of given field (column) for a given object
   * (tuple).
   **/
  public Object getColumnValue(Object tuple, int index)
      throws ColumnNotFoundException, ColumnValueNotAvailableException {
    return this.result.getColumnValue(tuple, index);
  }
  
  public void close() {
    //this.result.close();    
  }
}
