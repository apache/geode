/*========================================================================= 
 * (c)Copyright 2002-2009, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006 
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.connection;

import com.gemstone.gemfire.mgmt.DataBrowser.model.member.GemFireMember;
import com.gemstone.gemfire.mgmt.DataBrowser.query.QueryExecutionException;
import com.gemstone.gemfire.mgmt.DataBrowser.query.QueryResult;
import com.gemstone.gemfire.mgmt.DataBrowser.query.cq.CQException;
import com.gemstone.gemfire.mgmt.DataBrowser.query.cq.CQQuery;

/**
 * This interface is responsible to provide Query execution functionality to the application.
 * 
 * @author Hrishi
 **/
public interface GFQueryExecution {
  
  /**
   * This method executes a specified query on a given GemFire member.
   * @param query The query to be executed.
   * @param member The GemFire member on which we want to execute the query.
   * @return Result of Query execution.
   */
  public QueryResult executeQuery( String query, GemFireMember member) throws QueryExecutionException;
  
  
  /**
   * This method creates a new CQ on a given GemFire member.
   * @param name Name of the query.
   * @param query The query to be created.
   * @param member The GemFire member on which we want to execute the query.
   * @return The reference of the query.
   */
  public CQQuery newCQ( String name, String query, GemFireMember member) throws QueryExecutionException;
  
  
  /**
   * This method retrieves a CQQuery with the required name.
   * 
   * @param name Name of the CQ query to be retrieved.
   * @return CQQuery
   */
  public CQQuery getCQ( String name );
  
  /**
   * This method closes a CQQuery with the required name.
   * 
   * @param name Name of the CQ query to be retrieved.
   * @return CQQuery
   */
  public void closeCQ( String name )throws CQException;
  
  /**
   * This method stops a CQQuery with the required name.
   * 
   * @param name Name of the CQ query to be retrieved.
   * @return CQQuery
   */
  public void stopCQ( String name )throws CQException;  

  /**
   * This method closes the under-laying GemFire connection.
   */
  public void close();

}
