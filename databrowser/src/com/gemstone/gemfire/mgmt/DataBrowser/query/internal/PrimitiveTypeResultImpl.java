/*=========================================================================
 * (c)Copyright 2002-2011, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.query.internal;

import com.gemstone.gemfire.mgmt.DataBrowser.query.ColumnNotFoundException;
import com.gemstone.gemfire.mgmt.DataBrowser.query.IntrospectionResult;

public class PrimitiveTypeResultImpl implements IntrospectionResult {
  private static final int CONST_COLUMN_COUNT = 1;
  public static final String CONST_COLUMN_NAME = "Result";
  
  private Class<?> type;
  
  public PrimitiveTypeResultImpl(Class<?> ty) {
   this.type = ty; 
  }
  
  public Class<?> getJavaType() {
    return this.type;
  }
  
  public int getResultType() {
    return PRIMITIVE_TYPE_RESULT;
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

  public Object getColumnValue(Object tuple, int index) {
    return tuple;
  }

  public int getColumnType(int index) throws ColumnNotFoundException {
    if(index < CONST_COLUMN_COUNT)
      return PRIMITIVE_TYPE_COLUMN;
    
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
