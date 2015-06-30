/*=========================================================================
 * (c)Copyright 2002-2009, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.query.internal;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import com.gemstone.gemfire.mgmt.DataBrowser.query.ColumnValueNotAvailableException;

public class ObjectPropertyImpl implements ObjectColumn {
  protected final String name;
  protected final Method readMethod;
  protected final Method writeMethod;
  protected boolean final_;

  public ObjectPropertyImpl(String nm, Method mthdRead, Method mthdWrite ) {
    name = nm;
    readMethod = mthdRead;
    writeMethod = mthdWrite;
    int modifier = readMethod.getReturnType().getModifiers();
    this.final_ = (readMethod.getReturnType().isPrimitive() || Modifier.isFinal(modifier));
  }

  public String getFieldName() {
    return this.name;
  }

  public Class<?> getFieldType() {
    return readMethod.getReturnType();
  }

  public boolean isReadOnly() {
    return (writeMethod == null);
  }

  public boolean isFinal() {
    return this.final_;
  }

  public Class getDeclaringClass() {
    return this.readMethod.getDeclaringClass();
  }

  public Object getValue(Object object) throws ColumnValueNotAvailableException {
    try {
     return readMethod.invoke(object);
    }
    catch (Exception e) {
      throw new ColumnValueNotAvailableException("Could not read the value of the property "+name + " for input type "+object, e);
    }
  }

  @Override
  public int hashCode() {
    return this.name.hashCode();
  }

  @Override
  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer.append("ObjectPropertyImpl [ declaringClass :"+this.getDeclaringClass()+" name :"+this.getFieldName()+" ]");
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
