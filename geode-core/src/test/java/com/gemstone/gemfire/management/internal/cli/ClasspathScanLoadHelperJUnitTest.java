/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
