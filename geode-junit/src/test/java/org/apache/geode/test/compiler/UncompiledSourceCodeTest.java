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

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.commons.lang.SystemUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.UnitTest;

public class UncompiledSourceCodeTest {
  @Test
  public void fromClassNameWithNoPackage() throws Exception {
    UncompiledSourceCode uncompiledSourceCode = UncompiledSourceCode.fromClassName("NoPackage");
    assertThat(uncompiledSourceCode.simpleClassName).isEqualTo("NoPackage");
    assertThat(uncompiledSourceCode.sourceCode).isEqualTo("public class NoPackage {}");
  }

  @Test
  public void fromClassNameWithPackage() throws Exception {
    UncompiledSourceCode uncompiledSourceCode =
        UncompiledSourceCode.fromClassName("foo.bar.ClassName");
    assertThat(uncompiledSourceCode.simpleClassName).isEqualTo("ClassName");
    assertThat(uncompiledSourceCode.sourceCode)
        .isEqualTo("package foo.bar;" + SystemUtils.LINE_SEPARATOR + "public class ClassName {}");
  }

}
