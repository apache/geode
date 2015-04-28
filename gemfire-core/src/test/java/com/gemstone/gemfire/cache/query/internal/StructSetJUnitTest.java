/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * IndexCreationInternalsTest.java
 * JUnit based test
 *
 * Created on February 22, 2005, 11:24 AM
 */
package com.gemstone.gemfire.cache.query.internal;

import java.util.Iterator;

import junit.framework.TestCase;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.internal.types.StructTypeImpl;
import com.gemstone.gemfire.cache.query.internal.types.TypeUtils;
import com.gemstone.gemfire.cache.query.types.ObjectType;
import com.gemstone.junit.UnitTest;

/**
 *
 * @author ericz
 */
@Category(UnitTest.class)
public class StructSetJUnitTest extends TestCase {
  
  public StructSetJUnitTest(String testName) {
    super(testName);
  }
  
  public void testIntersectionAndRetainAll() {
    String names[] = {"p","pos"};
    ObjectType types[] = {TypeUtils.OBJECT_TYPE, TypeUtils.OBJECT_TYPE};
    StructTypeImpl sType = new StructTypeImpl(names, types);
    StructSet set1 = new StructSet(sType);
    Portfolio ptf = new Portfolio(0);
    Iterator pIter = ptf.positions.values().iterator();
    while(pIter.hasNext()){
      Object arr[] = {ptf, pIter.next()};
      set1.addFieldValues(arr);
    }
    
    StructSet set2 = new StructSet(sType);
    pIter = ptf.positions.values().iterator();
    while(pIter.hasNext()){
      Object arr[] = {ptf, pIter.next()};
      set2.addFieldValues(arr);
    }
    
    assertEquals(2, set1.size());
    assertEquals(2, set2.size());
    // tests that retainAll does not modify set1
    assertTrue(!set1.retainAll(set2));
    assertEquals(2, set1.size());
    assertEquals(2, set2.size());
    SelectResults sr = QueryUtils.intersection(set1, set2, null);
    assertEquals(2, sr.size());
  }  
}
