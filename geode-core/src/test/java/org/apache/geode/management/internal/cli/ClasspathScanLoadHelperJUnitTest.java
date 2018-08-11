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
    Set<String> packages = new HashSet<String>() {
      {
        add(PACKAGE_NAME);
        add(WRONG_PACKAGE_NAME);
      }
    };
    ClasspathScanLoadHelper scanner = new ClasspathScanLoadHelper(packages);
    Set<Class<?>> classLoaded =
        scanner.scanPackagesForClassesImplementing(INTERFACE1, PACKAGE_NAME);
    assertEquals(2, classLoaded.size());
    assertTrue(classLoaded.contains(IMPL1));
    assertTrue(classLoaded.contains(IMPL2));

    classLoaded =
        scanner.scanPackagesForClassesImplementing(INTERFACE2, PACKAGE_NAME);
    assertEquals(1, classLoaded.size());
    assertTrue(classLoaded.contains(IMPL2));

    classLoaded =
        scanner.scanPackagesForClassesImplementing(INTERFACE2, WRONG_PACKAGE_NAME);
    assertEquals(0, classLoaded.size());

    classLoaded =
        scanner.scanPackagesForClassesImplementing(NO_IMPL_INTERFACE, PACKAGE_NAME);
    assertEquals(0, classLoaded.size());

    classLoaded = scanner.scanPackagesForClassesImplementing(NO_IMPL_INTERFACE, WRONG_PACKAGE_NAME);
    assertEquals(0, classLoaded.size());
  }
}
