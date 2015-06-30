/*========================================================================= 
 * (c)Copyright 2002-2009, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006 
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.query;

import java.util.Collection;

public interface ResultSet {

  public boolean isEmpty();
  
  /**
   * This method returns the meta-data about the result of executed query.
   * 
   * @return The meta-data of result of the query execution.
   */
  public IntrospectionResult[] getIntrospectionResult();
  
  /**
   * This method returns the results of the executed query.
   * 
   * @return The result of query execution.
   */
  public Collection<?> getQueryResult();
  
  /**
   * This method returns the subset of result of the executed query for a given
   * introspection type.
   * 
   * @param metaInfo
   *          IntrospectionResult type for which we require result
   * @return subset of result of the query execution.
   */
  public Collection<?> getQueryResult(IntrospectionResult metaInfo);
  
  /**
   * This method returns the statistics of the Query execution.
   * 
   * @return The GemFire query execution statistics.
   */  
  public Object getColumnValue(Object tuple, int index)
  throws ColumnNotFoundException, ColumnValueNotAvailableException; 
  
  public void close();
  
}
