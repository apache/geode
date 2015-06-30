/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.cli;

import java.io.IOException;
import java.util.Set;

import org.junit.experimental.categories.Category;

import junit.framework.TestCase;

import com.gemstone.gemfire.internal.util.Callable;
import com.gemstone.gemfire.management.internal.cli.domain.AbstractImpl;
import com.gemstone.gemfire.management.internal.cli.domain.Impl1;
import com.gemstone.gemfire.management.internal.cli.domain.Impl12;
import com.gemstone.gemfire.management.internal.cli.domain.Interface1;
import com.gemstone.gemfire.management.internal.cli.domain.Interface2;
import com.gemstone.gemfire.management.internal.cli.util.ClasspathScanLoadHelper;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * @author tushark
 */
@Category(UnitTest.class)
public class ClasspathScanLoadHelperJUnitTest  extends TestCase{
  
  private static final String PACKAGE_NAME = "com.gemstone.gemfire.management.internal.cli.domain";
  private static final String WRONG_PACKAGE_NAME = "com.gemstone.gemfire.management.internal.cli.domain1";
  private static final Class<?> INTERFACE1 = Interface1.class;
  private static final Class<?> NO_IMPL_INTERFACE = Callable.class;
  private static final Class<?> INTERFACE2 = Interface2.class;
  private static final Class<?> IMPL1 = Impl1.class;
  private static final Class<?> IMPL2 = Impl12.class;
  private static final Class<?> ABSTRACT_IMPL = AbstractImpl.class;
  

  public void testloadAndGet(){

    try {
      Set<Class<?>> classLoaded = ClasspathScanLoadHelper.loadAndGet(PACKAGE_NAME, INTERFACE1, true);
      assertEquals(2, classLoaded.size());
      assertTrue(classLoaded.contains(IMPL1));
      assertTrue(classLoaded.contains(IMPL2));
      //impl1 and impl12
      
      classLoaded = ClasspathScanLoadHelper.loadAndGet(PACKAGE_NAME, INTERFACE1, false);
      assertEquals(4, classLoaded.size());
      assertTrue(classLoaded.contains(IMPL1));
      assertTrue(classLoaded.contains(IMPL2));
      assertTrue(classLoaded.contains(ABSTRACT_IMPL));
      assertTrue(classLoaded.contains(INTERFACE1));
      
      classLoaded = ClasspathScanLoadHelper.loadAndGet(PACKAGE_NAME, INTERFACE2, false);
      assertEquals(2, classLoaded.size());      
      assertTrue(classLoaded.contains(IMPL2));      
      assertTrue(classLoaded.contains(INTERFACE2));
      
      classLoaded = ClasspathScanLoadHelper.loadAndGet(PACKAGE_NAME, INTERFACE2, true);      
      assertEquals(1, classLoaded.size());      
      assertTrue(classLoaded.contains(IMPL2));
      
      classLoaded = ClasspathScanLoadHelper.loadAndGet(WRONG_PACKAGE_NAME, INTERFACE2, true);
      assertEquals(0, classLoaded.size());      
      
      classLoaded = ClasspathScanLoadHelper.loadAndGet(PACKAGE_NAME, NO_IMPL_INTERFACE, true);
      assertEquals(0, classLoaded.size());
      
      classLoaded = ClasspathScanLoadHelper.loadAndGet(WRONG_PACKAGE_NAME, NO_IMPL_INTERFACE, true);
      assertEquals(0, classLoaded.size());
      
      
    } catch (ClassNotFoundException e) {
      fail("Error loading class" + e);
    } catch (IOException e) {
      fail("Error loading class" + e);
    }   
  }
}
