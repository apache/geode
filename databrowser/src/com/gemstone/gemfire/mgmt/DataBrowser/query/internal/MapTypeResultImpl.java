/*=========================================================================
 * (c)Copyright 2002-2011, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.query.internal;

import java.util.Collection;
import java.util.Map;

import com.gemstone.gemfire.mgmt.DataBrowser.query.ColumnNotFoundException;
import com.gemstone.gemfire.mgmt.DataBrowser.query.ColumnValueNotAvailableException;
import com.gemstone.gemfire.mgmt.DataBrowser.query.IntrospectionResult;
import com.gemstone.gemfire.mgmt.DataBrowser.query.QueryExecutionException;
import com.gemstone.gemfire.mgmt.DataBrowser.query.QueryUtil;

public class MapTypeResultImpl implements IntrospectionResult {
  private static final int CONST_COLUMN_COUNT = 1;
  public static final String CONST_COLUMN_NAME = "Result";
  private Class<?> type;
  
  public MapTypeResultImpl(Class<?> ty) {
   this.type = ty; 
  }
  
  
  public int getResultType() {
    return MAP_TYPE_RESULT;
  }
  
  public Class<?> getJavaType() {
    return this.type;
  }  

  public Class getColumnClass(int index) {
    return this.type;
  }
  
  public Class getColumnClass(Object tuple, int index)
      throws ColumnNotFoundException {
    return getColumnClass(index);
  }

  public int getColumnCount() {
    return CONST_COLUMN_COUNT;
  }

  public String getColumnName(int index) {
    return CONST_COLUMN_NAME;
  }
  
  public int getColumnIndex(String name) throws ColumnNotFoundException {
    if(CONST_COLUMN_NAME.equals(name))
     return 0;
    
    throw new ColumnNotFoundException("Column not available for name :" + name);
  }

  public Object getColumnValue(Object tuple, int index) throws ColumnValueNotAvailableException {
    if(tuple instanceof Map) {
      Collection temp = null;
      temp = ((Map)tuple).entrySet();   
      
      try {
        return QueryUtil.introspectCollectionObject(temp);
      }
      catch (QueryExecutionException e) {
        throw new ColumnValueNotAvailableException(e);
      }
    }    
    throw new ColumnValueNotAvailableException(" Invalid object type. It should be java.util.Map : "+tuple);    
  }

  public int getColumnType(int index) throws ColumnNotFoundException {
    if(index < CONST_COLUMN_COUNT)
      return MAP_TYPE_COLUMN;
    
    throw new ColumnNotFoundException("Column not available for index :" + index);
  }
  
  public int getColumnType(Object tuple, int index)
      throws ColumnNotFoundException {
    return getColumnType(index);
  }
  
  public String getJavaTypeName() {
    Class<?> javaType = getJavaType();
    
    return javaType != null ? javaType.getName() : null;
  }

  public boolean isCompatible(Object data) {
    //checks for equality by reference not assignable. Using == assuming single 
    //instance of a class
    return data != null && getJavaType() == data.getClass();
  }
}
