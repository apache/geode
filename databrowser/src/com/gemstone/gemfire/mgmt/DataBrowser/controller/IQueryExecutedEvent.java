package com.gemstone.gemfire.mgmt.DataBrowser.controller;

import com.gemstone.gemfire.mgmt.DataBrowser.model.member.GemFireMember;
import com.gemstone.gemfire.mgmt.DataBrowser.query.QueryResult;

/**
 * Event for query execution to be sent to all listeners
 * {@link IQueryExecutionListener}
 * 
 * @author mjha
 * 
 */
public interface IQueryExecutedEvent {

	/**
	 * @return the original query string for which query was performed
	 */
	public String getQueryString();

	/**
	 * @return the result of query performed, null in case of error occurred
	 *         while querying
	 */
	public QueryResult getQueryResult();

	/**
	 * @return the member on which query was fired
	 */
	public GemFireMember getMember();
}
