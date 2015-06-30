/*========================================================================= 
 * (c)Copyright 2002-2009, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006 
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.query.cq;

import com.gemstone.gemfire.mgmt.DataBrowser.query.IntrospectionResult;

public interface EventData extends Comparable<EventData> {
  
  public long getId();
  
  public Object getKey();
  
  public Object getValue();
  
  public IntrospectionResult getIntrospectionResult();
  
  public void setValue(IntrospectionResult metaInfo, Object value);  

}
