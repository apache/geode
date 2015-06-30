/*=========================================================================
 * (c) Copyright 2002-2009, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200,  Beaverton, OR 97006
 * All Rights Reserved.
 *========================================================================
 */

package com.gemstone.gemfire.mgmt.DataBrowser.query.cq;

import com.gemstone.gemfire.admin.RegionNotFoundException;
import com.gemstone.gemfire.cache.query.CqClosedException;
import com.gemstone.gemfire.cache.query.CqStatistics;
import com.gemstone.gemfire.mgmt.DataBrowser.model.member.GemFireMember;


/**
 * Interface for continuous query. This provides methods for managing a CQ 
 * once it is created through the QueryService. 
 * The methods allow you to retrieve CQ related information, operate on CQ
 * like execute, stop, close and get the state of the CQ.
 *
 * @author      Hrishi
 **/

public interface CQQuery {
  
  public GemFireMember getMember();  

  /**
   * Get the original query string that was specified with CQ.
   * @return String the query string associated with CQ.
   */
  public String getQueryString();

  /**
   * Get the query object generated for this CQs query.
   * @return Query query object for the query string.
   */
  public CQResult getQueryResult();

  /**
   * Get the name of the CQ.
   * @return the name of the CQ.
   */
  public String getName();

  /**
   * Get statistics information for this CQ.
   * @return CqStatistics CQ statistics object.
   */
  public CqStatistics getStatistics();  

  /**
   * Start executing the CQ or if this CQ is stopped earlier, resumes execution of the CQ.
   * The CQ is executed on primary and redundant servers, if CQ execution fails on all the 
   * server then a CqException is thrown.
   * @throws CqClosedException if this CqQuery is closed.
   * @throws RegionNotFoundException if the specified region in the
   *         query string is not found.
   * @throws IllegalStateException if the CqQuery is in the RUNNING state
   *         already.
   * @throws CQException if failed to execute.
   */
  public void execute() throws CQException;

  /**
   *  Stops this CqQuery without releasing resources. Puts the CqQuery into
   *  the STOPPED state. Can be resumed by calling execute or
   *  executeWithInitialResults.
   *  @throws IllegalStateException if the CqQuery is in the STOPPED state
   *          already.
   *  @throws CqClosedException if the CQ is CLOSED.
   */
  public void stop() throws CQException;

  /**
   * Close the CQ and stop execution.  
   * Releases the resources associated with this CqQuery.
   * @throws CqClosedException Further calls on this CqQuery instance except 
   *         for getState() or getName().
   * @throws CQException - if failure during cleanup of CQ resources.
   */ 
  public void close() throws CQException;

  /**
   * This allows to check if the CQ is in running or active.
   * @return boolean true if running, false otherwise
   */
  public boolean isRunning();

  /**
   * This allows to check if the CQ is in stopped.
   * @return boolean true if stopped, false otherwise
   */ 
  public boolean isStopped();

  /**
   * This allows to check if the CQ is closed.
   * @return boolean true if closed, false otherwise
   */
  public boolean isClosed();
  
  /**
   * This allows to check if the CQ is durable.
   * @return boolean true if durable, false otherwise
   * @since 5.5
   */
  public boolean isDurable();

}
