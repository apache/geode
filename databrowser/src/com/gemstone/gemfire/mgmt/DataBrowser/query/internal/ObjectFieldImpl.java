/*========================================================================= 
 * (c)Copyright 2002-2009, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006 
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.query.internal;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import com.gemstone.gemfire.mgmt.DataBrowser.query.ColumnValueNotAvailableException;

public class ObjectFieldImpl implements ObjectColumn {
  private Field field;
  private boolean readOnly;
  private boolean Final;
    
  public ObjectFieldImpl(Field fld, boolean ro) {
    super();
    this.field = fld;
    this.readOnly = ro;
    int modifier = this.field.getModifiers();
    this.Final = (this.field.getType().isPrimitive() || Modifier.isFinal(modifier));
  }

  public Class<?> getFieldType() {
    return field.getType();
  }

  public String getFieldName() {
    return field.getName();
  }

  public boolean isReadOnly() {
    return this.readOnly;
  }
  
  public boolean isFinal() {
    return this.Final;
  }
  
  public Class getDeclaringClass() {
    return this.field.getDeclaringClass();
  }
    
  public Object getValue(Object object) throws ColumnValueNotAvailableException {
    try {
      return field.get(object);
    }
    catch (Exception e) {
      throw new ColumnValueNotAvailableException(e);
    }   
  }
  
  @Override
  public int hashCode() {
    return this.field.getName().hashCode();
  }
  
  @Override
  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer.append("ObjectFieldImpl [ declaringClass :"+this.getDeclaringClass()+" name :"+this.getFieldName()+" ]");
    return buffer.toString();
  }
    
  
  @Override
  public boolean equals(Object obj) {
    if(obj instanceof ObjectColumn) {
      ObjectColumn other = (ObjectColumn)obj;  
      return (this.getFieldName().equals(other.getFieldName()));
    }
    return false;
  }
}
