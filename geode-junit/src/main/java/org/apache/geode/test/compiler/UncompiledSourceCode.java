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

import org.apache.commons.lang3.StringUtils;

public class UncompiledSourceCode {

  private final String simpleClassName;
  private final String sourceCode;

  private UncompiledSourceCode(String simpleClassName, String sourceCode) {
    this.simpleClassName = simpleClassName;
    this.sourceCode = sourceCode;
  }

  static UncompiledSourceCode fromSourceCode(String sourceCode) {
    String simpleClassName = new ClassNameExtractor().extractFromSourceCode(sourceCode);
    return new UncompiledSourceCode(simpleClassName, sourceCode);
  }

  static UncompiledSourceCode fromClassName(String fullyQualifiedClassName) {
    ClassNameWithPackage classNameWithPackage = ClassNameWithPackage.of(fullyQualifiedClassName);
    boolean isPackageSpecified = StringUtils.isNotBlank(classNameWithPackage.packageName);

    StringBuilder sourceCode = new StringBuilder();
    if (isPackageSpecified) {
      sourceCode.append(String.format("package %s;", classNameWithPackage.packageName));
      sourceCode.append(lineSeparator());
    }

    sourceCode.append(String.format("public class %s {}", classNameWithPackage.simpleClassName));

    return new UncompiledSourceCode(classNameWithPackage.simpleClassName, sourceCode.toString());
  }

  public String getSimpleClassName() {
    return simpleClassName;
  }

  public String getSourceCode() {
    return sourceCode;
  }

  private static class ClassNameWithPackage {

    private final String packageName;
    private final String simpleClassName;

    static ClassNameWithPackage of(String fqClassName) {
      int indexOfLastDot = fqClassName.lastIndexOf('.');

      if (indexOfLastDot == -1) {
        return new ClassNameWithPackage("", fqClassName);
      }

      String specifiedPackage = fqClassName.substring(0, indexOfLastDot);
      String simpleClassName = fqClassName.substring(indexOfLastDot + 1);

      return new ClassNameWithPackage(specifiedPackage, simpleClassName);
    }

    private ClassNameWithPackage(String packageName, String simpleClassName) {
      this.packageName = packageName;
      this.simpleClassName = simpleClassName;
    }
  }
}
