package com.gemstone.gemfire.mgmt.DataBrowser.ui.model;

import com.gemstone.gemfire.mgmt.DataBrowser.query.IntrospectionResult;

public interface ResultsInput {

  public IntrospectionResult getMetaData() ;
  
  public Object getSelectedObject();
}
