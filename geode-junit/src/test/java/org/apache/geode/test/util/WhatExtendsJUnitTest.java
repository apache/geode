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

package org.apache.geode.test.util;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import org.junit.Before;
import org.junit.Test;

public class WhatExtendsJUnitTest {

  private StressNewTestHelper scanner;

  public abstract static class A {
  }

  public static class B extends A {
  }

  public static class C extends B {
  }

  @Before
  public void setup() {
    scanner = new StressNewTestHelper("org.apache.geode");
  }

  @Test
  public void nothingExtendsC() {
    scanner.add(getClassLocation(C.class));
    assertThat(scanner.buildGradleCommand())
        .isEqualTo("repeatUnitTest --tests WhatExtendsJUnitTest$C -PtestCount=1");
  }

  @Test
  public void classAisExtendedByBandC() {
    scanner.add(getClassLocation(A.class));
    assertThat(scanner.buildGradleCommand()).isEqualTo(
        "repeatUnitTest --tests WhatExtendsJUnitTest$B --tests WhatExtendsJUnitTest$C -PtestCount=2");
  }

  @Test
  public void classAisExtendedByBandC_withDuplicatesRemoved() {
    scanner.add(getClassLocation(A.class));
    scanner.add(getClassLocation(A.class));
    assertThat(scanner.buildGradleCommand())
        .isEqualTo(
            "repeatUnitTest --tests WhatExtendsJUnitTest$B --tests WhatExtendsJUnitTest$C -PtestCount=2");
  }

  @Test
  public void usingJavaFileWithSameCategoryAsSubClasses() {
    scanner.add(getClassLocation(A.class, "foo/src/test/java/"));
    assertThat(scanner.buildGradleCommand())
        .isEqualTo(
            "repeatUnitTest --tests WhatExtendsJUnitTest$B --tests WhatExtendsJUnitTest$C -PtestCount=2");
  }

  @Test
  public void usingJavaFileWithDifferentCategoryAsSubClasses() {
    scanner.add(getClassLocation(A.class, "foo/src/integrationTest/java/"));
    scanner.add(getClassLocation(this.getClass(), "foo/src/integrationTest/java/"));
    assertThat(scanner.buildGradleCommand())
        .isEqualTo(
            "repeatUnitTest --tests WhatExtendsJUnitTest$B --tests WhatExtendsJUnitTest$C repeatIntegrationTest --tests WhatExtendsJUnitTest -PtestCount=3");
  }

  @Test
  public void ignoreAcceptanceTestSourcesForNow() {
    scanner.add(getClassLocation(this.getClass(), "foo/src/acceptanceTest/java/"));
    assertThat(scanner.buildGradleCommand()).isEqualTo("-PtestCount=0");
  }

  @Test
  public void ignoreNonGeodeClasses() {
    scanner.add("foo/src/test/java/org/example/Foo.java");
    assertThat(scanner.buildGradleCommand()).isEqualTo("-PtestCount=0");
  }

  private String getClassLocation(Class<?> clazz) {
    String codeSource = clazz.getProtectionDomain().getCodeSource().getLocation().getFile();
    String classFile = clazz.getName().replace(".", "/");

    return codeSource + classFile;
  }

  private String getClassLocation(Class<?> clazz, String fakePrefix) {
    String classFile = clazz.getName().replace(".", "/");

    return fakePrefix + classFile;
  }
}
