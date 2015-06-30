/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.codeAnalysis;

import com.gemstone.gemfire.codeAnalysis.decode.CompiledClass;
import com.gemstone.gemfire.codeAnalysis.decode.CompiledMethod;

import java.util.HashMap;
import java.util.Map;



public class ClassAndMethods implements Comparable {
  public CompiledClass dclass;
  public Map<String,CompiledMethod> methods = new HashMap<String,CompiledMethod>();
  public short variableCount;
  
  public ClassAndMethods(CompiledClass parsedClass) {
    this.dclass = parsedClass;
  }
  
  public int compareTo(Object other) {
    if ( !(other instanceof ClassAndMethods) ) {
      return -1;
    }
    return dclass.compareTo(((ClassAndMethods)other).dclass);
  }
  
  public int numMethods() {
    return methods.size();
  }
  
  @Override public String toString() {
    return ClassAndMethodDetails.convertForStoring(this);
  }
}