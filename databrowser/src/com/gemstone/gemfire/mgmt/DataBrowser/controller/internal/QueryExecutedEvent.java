package com.gemstone.gemfire.mgmt.DataBrowser.controller.internal;

import com.gemstone.gemfire.mgmt.DataBrowser.controller.IQueryExecutedEvent;
import com.gemstone.gemfire.mgmt.DataBrowser.model.member.GemFireMember;
import com.gemstone.gemfire.mgmt.DataBrowser.query.QueryResult;

/**
 * Implementation of {@link IQueryExecutedEvent}
 * 
 * @author mjha
 */
public class QueryExecutedEvent implements IQueryExecutedEvent {

  private String        queryString;
  private QueryResult   queryResult;
  private GemFireMember member;

  public QueryExecutedEvent(String strQuery, QueryResult resQuery,
      GemFireMember gfeMbr) {
    this.queryString = strQuery;
    this.queryResult = resQuery;
    this.member = gfeMbr;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * com.gemstone.gemfire.mgmt.DataBrowser.model.IQueryExecutedEvent#getQueryResult
   * ()
   */
  public QueryResult getQueryResult() {
    return queryResult;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * com.gemstone.gemfire.mgmt.DataBrowser.model.IQueryExecutedEvent#getQueryString
   * ()
   */
  public String getQueryString() {
    return queryString;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * com.gemstone.gemfire.mgmt.DataBrowser.model.IQueryExecutedEvent#getMember()
   */
  public GemFireMember getMember() {
    return member;
  }
}
