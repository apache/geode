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

import org.junit.Test;

public class WhatExtendsJUnitTest {

  public abstract static class A {
  }

  public static class B extends A {
  }

  public static class C extends B {
  }

  @Test
  public void nothingExtendsC() {
    ClassScanner scanner = new ClassScanner("org.apache.geode");
    assertThat(scanner.whatExtends(C.class.getName())).isEmpty();
  }

  @Test
  public void classAisExtendedByBandC() {
    ClassScanner scanner = new ClassScanner("org.apache.geode");
    assertThat(scanner.whatExtends(A.class.getName()))
        .containsExactlyInAnyOrder("WhatExtendsJUnitTest$B", "WhatExtendsJUnitTest$C");
  }

  @Test
  public void usingSimpleNames() {
    ClassScanner scanner = new ClassScanner("org.apache.geode");
    assertThat(scanner.whatExtends(A.class.getSimpleName()))
        .containsExactlyInAnyOrder("WhatExtendsJUnitTest$B", "WhatExtendsJUnitTest$C");
  }

  @Test
  public void usingJavaFile() {
    ClassScanner scanner = new ClassScanner("org.apache.geode");
    assertThat(
        scanner.whatExtends("src/main/org/apache/geode/test/util/WhatExtendsJUnitTest$A.java"))
            .containsExactlyInAnyOrder("WhatExtendsJUnitTest$B", "WhatExtendsJUnitTest$C");
  }
}
