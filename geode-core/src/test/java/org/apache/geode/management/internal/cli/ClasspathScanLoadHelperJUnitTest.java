/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.management.internal.cli;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import org.apache.geode.internal.util.Versionable;
import org.apache.geode.management.internal.cli.domain.AbstractImpl;
import org.apache.geode.management.internal.cli.domain.Impl1;
import org.apache.geode.management.internal.cli.domain.Impl12;
import org.apache.geode.management.internal.cli.domain.Interface1;
import org.apache.geode.management.internal.cli.domain.Interface2;
import org.apache.geode.management.internal.cli.util.ClasspathScanLoadHelper;

public class ClasspathScanLoadHelperJUnitTest {

  private final String PACKAGE_NAME = "org.apache.geode.management.internal.cli.domain";
  private final String WRONG_PACKAGE_NAME = "org.apache.geode.management.internal.cli.domain1";
  private final Class<?> INTERFACE1 = Interface1.class;
  private final Class<?> NO_IMPL_INTERFACE = Versionable.class;
  private final Class<?> INTERFACE2 = Interface2.class;
  private final Class<?> IMPL1 = Impl1.class;
  private final Class<?> IMPL2 = Impl12.class;
  private final Class<?> ABSTRACT_IMPL = AbstractImpl.class;

  @Test
  public void testLoadAndGet() throws Exception {
    Set<String> package1 = new HashSet<String>() {
      {
        add(PACKAGE_NAME);
      }
    };
    ClasspathScanLoadHelper scanner1 = new ClasspathScanLoadHelper(package1);
    Set<Class<?>> classLoaded = scanner1.scanPackagesForClassesImplementing(INTERFACE1);
    assertEquals(2, classLoaded.size());
    assertTrue(classLoaded.contains(IMPL1));
    assertTrue(classLoaded.contains(IMPL2));

    Set<String> package2 = new HashSet<String>() {
      {
        add(PACKAGE_NAME);
      }
    };
    ClasspathScanLoadHelper scanner2 = new ClasspathScanLoadHelper(package2);
    classLoaded =
        scanner2.scanPackagesForClassesImplementing(INTERFACE2);
    assertEquals(1, classLoaded.size());
    assertTrue(classLoaded.contains(IMPL2));

    Set<String> package3 = new HashSet<String>() {
      {
        add(WRONG_PACKAGE_NAME);
      }
    };
    ClasspathScanLoadHelper scanner3 = new ClasspathScanLoadHelper(package3);
    classLoaded =
        scanner3.scanPackagesForClassesImplementing(INTERFACE2);
    assertEquals(0, classLoaded.size());

    Set<String> package4 = new HashSet<String>() {
      {
        add(PACKAGE_NAME);
      }
    };
    ClasspathScanLoadHelper scanner4 = new ClasspathScanLoadHelper(package4);
    classLoaded =
        scanner4.scanPackagesForClassesImplementing(NO_IMPL_INTERFACE);
    assertEquals(0, classLoaded.size());

    Set<String> package5 = new HashSet<String>() {
      {
        add(WRONG_PACKAGE_NAME);
      }
    };
    ClasspathScanLoadHelper scanner5 = new ClasspathScanLoadHelper(package5);
    classLoaded = scanner5.scanPackagesForClassesImplementing(NO_IMPL_INTERFACE);
    assertEquals(0, classLoaded.size());
  }
}
