package com.gemstone.gemfire.mgmt.DataBrowser.controller;

import com.gemstone.gemfire.mgmt.DataBrowser.model.member.GemFireMember;

/**
 * Encapsulating informations required to executed query
 * 
 * @author mjha
 * 
 */
public class QueryConfigurationPrms {

  /**
   * the query string for which query to be performed
   */
  private String                  queryString;

  /**
   * Member to which query has to be sent
   */
  private GemFireMember           member;

  /**
   * Call back object({@link IQueryExecutionListener}) , to which result has to
   * be pass after query
   */
  private IQueryExecutionListener queryExecutionListener;

  public QueryConfigurationPrms() {

  }

  /**
   * 
   * @param strQuery
   *          the query string for which query to be performed
   * @param gfeMbr
   *          to which query has to be sent
   * @param listener
   *          Call back object({@link IQueryExecutionListener}) , to which
   *          result has to be pass after query
   */
  public QueryConfigurationPrms(String strQuery, GemFireMember gfeMbr,
      IQueryExecutionListener listener) {
    this.queryString = strQuery;
    this.member = gfeMbr;
    this.queryExecutionListener = listener;
  }

  public String getQueryString() {
    return queryString;
  }

  public void setQueryString(String strQuery) {
    this.queryString = strQuery;
  }

  public GemFireMember getMember() {
    return member;
  }

  public void setMember(GemFireMember gfeMbr) {
    this.member = gfeMbr;
  }

  public IQueryExecutionListener getQueryExecutionListener() {
    return queryExecutionListener;
  }

  public void setQueryExecutionListener(IQueryExecutionListener listener) {
    this.queryExecutionListener = listener;
  }
  
  // TODO MGH - this works for us. But this is not really good design! :)
  public boolean isCQ(){
    return false;
  }

}
