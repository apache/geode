/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.codeAnalysis;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import com.gemstone.gemfire.codeAnalysis.decode.CompiledClass;
import com.gemstone.gemfire.codeAnalysis.decode.CompiledField;



public class ClassAndVariables implements Comparable {
  public CompiledClass dclass;
  public boolean hasSerialVersionUID = false;
  public long serialVersionUID;
  public Map<String,CompiledField> variables = new HashMap<String,CompiledField>();
  
  public ClassAndVariables(CompiledClass parsedClass) {
    this.dclass = parsedClass;
    
    String name = dclass.fullyQualifiedName().replace('/', '.');
    try {
      Class realClass = Class.forName(name);
      Field field = realClass.getDeclaredField("serialVersionUID");
      field.setAccessible(true);
      serialVersionUID = field.getLong(null);
      hasSerialVersionUID = true;
    } catch (NoSuchFieldException e) {
      //No serialVersionUID defined
      
    } catch (Throwable e) {  
      System.out.println("Unable to load" + name + ":" + e);
    }
    
  }
  
  public int compareTo(Object other) {
    if ( !(other instanceof ClassAndVariables) ) {
      return -1;
    }
    return dclass.compareTo(((ClassAndVariables)other).dclass);
  }
  
  
  @Override public String toString() {
    return ClassAndVariableDetails.convertForStoring(this);
  }

}