/*========================================================================= 
 * (c)Copyright 2002-2009, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006 
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.query.internal;

import com.gemstone.gemfire.mgmt.DataBrowser.query.ColumnValueNotAvailableException;

/**
 * This interface represents a Column for a given Java type, which can either be
 * a field or a Java Beans TM property of that type.
 * 
 * @author Hrishi
 **/
public interface ObjectColumn {

  public Class getFieldType();

  public String getFieldName();

  public boolean isReadOnly();
  
  public boolean isFinal();
  
  public Class getDeclaringClass();
  
  public Object getValue(Object object)throws ColumnValueNotAvailableException;

}
