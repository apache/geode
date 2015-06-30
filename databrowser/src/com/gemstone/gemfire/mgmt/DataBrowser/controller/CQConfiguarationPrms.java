package com.gemstone.gemfire.mgmt.DataBrowser.controller;

import com.gemstone.gemfire.mgmt.DataBrowser.query.cq.CQQuery;

/**
 * 
 * @author mjha
 * 
 */
public class CQConfiguarationPrms extends QueryConfigurationPrms {

  private ICQueryEventListener cQeventListner_;

  private String name_;

  private CQQuery cQuery_;

  public CQConfiguarationPrms(String queryName,
      ICQueryEventListener cQeventListner) {
    name_ = queryName;
    cQeventListner_ = cQeventListner;
  }

  public String getName() {
    return name_;
  }

  @Override
  public boolean isCQ() {
    return true;
  }

  public CQQuery getCQuery() {
    return cQuery_;
  }

  public void setCQuery(CQQuery query) {
    cQuery_ = query;
  }

  public ICQueryEventListener getCQeventListner() {
    return cQeventListner_;
  }
}
