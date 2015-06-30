package com.gemstone.gemfire.mgmt.DataBrowser.controller;

/**
 * This interface provides call back functionality for listening query execution
 * completed result
 * 
 * @author mjha
 **/
public interface IQueryExecutionListener {

  /**
   * to be called when query is successfully executed
   * 
   * @param queryEvent
   *          instance of {@link IQueryExecutedEvent }
   */
  public void queryExecuted(IQueryExecutedEvent queryEvent);

  /**
   * to be called when error occurred while executing query
   * 
   * @param queryEvent
   *          instance of {@link IQueryExecutedEvent }
   * @param ex
   *          exception occurred while executing query
   */
  public void queryFailed(IQueryExecutedEvent queryEvent, Throwable ex);
}
