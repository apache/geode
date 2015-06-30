package com.gemstone.gemfire.mgmt.DataBrowser.controller;

import com.gemstone.gemfire.mgmt.DataBrowser.query.cq.CQEventListener;
import com.gemstone.gemfire.mgmt.DataBrowser.query.cq.CQQuery;
import com.gemstone.gemfire.mgmt.DataBrowser.query.cq.TypeListener;

/**
 * This interface provides call back functionality for listening query execution
 * completed result
 * 
 * @author mjha
 **/
public interface ICQueryEventListener extends CQEventListener, TypeListener{

  public void setQuery(CQQuery cQuery);

}
