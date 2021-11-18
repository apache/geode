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
package org.apache.geode.test.compiler;

import static java.lang.System.lineSeparator;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class InMemorySourceFileTest {

  @Test
  public void fromClassNameWithNoPackage() {
    InMemorySourceFile sourceFile = InMemorySourceFile.fromClassName("NoPackage");
    assertThat(sourceFile.getName())
        .as("name")
        .isEqualTo("NoPackage");
    assertThat(sourceFile.getCharContent(true))
        .as("content")
        .isEqualTo("public class NoPackage {}");
  }

  @Test
  public void fromClassNameWithPackage() {
    InMemorySourceFile sourceFile = InMemorySourceFile.fromClassName("foo.bar.ClassName");
    assertThat(sourceFile.getName())
        .as("name")
        .isEqualTo("foo.bar.ClassName");
    assertThat(sourceFile.getCharContent(true))
        .as("content")
        .isEqualTo("package foo.bar;" + lineSeparator() + "public class ClassName {}");
  }
}
