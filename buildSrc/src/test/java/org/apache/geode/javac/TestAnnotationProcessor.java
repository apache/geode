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

package org.apache.geode.javac;

import org.junit.Test;

public class TestAnnotationProcessor {
  private static final String VALID_CLASS_TEMPLATE = "package org.apache.geode;\n"
      + "import org.junit.runner.RunWith;\n" + "import org.junit.runners.Parameterized;\n"
      + "import org.junit.experimental.categories.Category;\n"
      + "import org.apache.geode.test.junit.categories.UnitTest;\n"
      + "import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;\n"
      + "@Category(UnitTest.class)\n"
      + "@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)\n"
      + "@RunWith(Parameterized.class)\n" + "public class Test {\n" + "}\n";

  private static final String INVALID_CLASS_TEMPLATE = "package org.apache.geode;\n"
      + "import org.junit.runner.RunWith;\n" + "import org.junit.runners.Parameterized;\n"
      + "import org.junit.experimental.categories.Category;\n"
      + "import org.apache.geode.test.junit.categories.UnitTest;\n"
      + "@Category(UnitTest.class)\n"
      + "@RunWith(Parameterized.class)\n" + "public class Test {\n" + "}\n";

  private TestCompiler compiler = new TestCompiler();

  @Test
  public void checkValidAnnotations() {
    String qualifiedClassName = "org.apache.geode.Test";
    compiler.compile(qualifiedClassName, VALID_CLASS_TEMPLATE);
  }

  @Test (expected = CompilerException.class)
  public void checkInvalidAnnotations() {
    String qualifiedClassName = "org.apache.geode.Test";
    compiler.compile(qualifiedClassName, INVALID_CLASS_TEMPLATE);
  }
}
