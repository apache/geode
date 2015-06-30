/*
 * =========================================================================
 * Copyright (c) 2002-2012 VMware, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. VMware products are covered by
 * more patents listed at http://www.vmware.com/go/patents.
 * All Rights Reserved.
 * ========================================================================
 */
package com.gemstone.gemfire.mgmt.DataBrowser.query.internal;

import java.lang.reflect.Field;

import com.gemstone.gemfire.mgmt.DataBrowser.utils.LogUtil;

/**
 * FieldType enum is made 1 public class since GemFire 6.6.2. 
 * In 6.6 it is:   com.gemstone.gemfire.pdx.internal.FieldType
 * In 6.6.2 it is: com.gemstone.gemfire.pdx.FieldType
 * 
 * This class is used to access the enums defined by FieldType using reflection. 
 * 
 * @author abhishek
 *
 */
public class FieldTypeAccessor {
  
  private Class<?> fieldTypeClass;

  FieldTypeAccessor(Class<?> fieldTypeClass) {
    this.fieldTypeClass = fieldTypeClass;
  }
  
  public Object getField(String name) {
    Object value = null;
    try {
      Field declaredField = fieldTypeClass.getDeclaredField(name);
      //No null check as it'll throw NoSuchFieldException if Field is not found
      value = declaredField.get(fieldTypeClass);
    } catch (SecurityException e) {
      LogUtil.info("Could not access "+name+" in "+fieldTypeClass, e);
      value = null;
    } catch (NoSuchFieldException e) {
      LogUtil.info("Could not find "+name+" in "+fieldTypeClass, e);
      value = null;
    } catch (IllegalArgumentException e) {
      LogUtil.info("Could not find "+name+" in "+fieldTypeClass, e);
      value = null;
    } catch (IllegalAccessException e) {
      LogUtil.info("Could not access "+name+" in "+fieldTypeClass, e);
      value = null;
    }
    
    return value;
  }
  
  public Object getBoolean() {
    return getField("BOOLEAN");
  }
  
  public Object getByte() {
    return getField("BYTE");
  }
  
  public Object getChar() {
    return getField("CHAR");
  }
  
  public Object getShort() {
    return getField("SHORT");
  }
  
  public Object getInt() {
    return getField("INT");
  }
  
  public Object getLong() {
    return getField("LONG");
  }
  
  public Object getFloat() {
    return getField("FLOAT");
  }
  
  public Object getDouble() {
    return getField("DOUBLE");
  }
  
  public Object getDate() {
    return getField("DATE");
  }
  
  public Object getString() {
    return getField("STRING");
  }
  
  public Object getObject() {
    return getField("OBJECT");
  }
  
  public Object getBooleanArray() {
    return getField("BOOLEAN_ARRAY");
  }
  
  public Object getByteArray() {
    return getField("BYTE_ARRAY");
  }
  
  public Object getCharArray() {
    return getField("CHAR_ARRAY");
  }
  
  public Object getShortArray() {
    return getField("SHORT_ARRAY");
  }
  
  public Object getIntArray() {
    return getField("INT_ARRAY");
  }
  
  public Object getLongArray() {
    return getField("LONG_ARRAY");
  }
  
  public Object getFloatArray() {
    return getField("FLOAT_ARRAY");
  }
  
  public Object getDoubleArray() {
    return getField("DOUBLE_ARRAY");
  }
  
  public Object getStringArray() {
    return getField("STRING_ARRAY");
  }
  
  public Object getObjectArray() {
    return getField("OBJECT_ARRAY");
  }
  
  public Object getArrayOfByteArrays() {
    return getField("ARRAY_OF_BYTE_ARRAYS");
  }
  
//  public static void main(String[] args) throws ClassNotFoundException {
////    FieldTypeProxy proxy = new FieldTypeProxy(Class.forName("com.gemstone.gemfire.pdx.internal.FieldType"));
//    FieldTypeProxy proxy = new FieldTypeProxy(Class.forName("com.gemstone.gemfire.pdx.FieldType"));
//    
//    System.out.println(proxy.getBoolean());
//    System.out.println(proxy.getByte());
//  }
  
  @Override
  public String toString() {
    return FieldTypeAccessor.class.getSimpleName()+"["+fieldTypeClass+"]";
  }

}
