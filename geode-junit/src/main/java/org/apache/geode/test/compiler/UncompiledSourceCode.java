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

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.SystemUtils;

public class UncompiledSourceCode {
  public String simpleClassName;
  public String sourceCode;

  private UncompiledSourceCode(String simpleClassName, String sourceCode) {
    this.simpleClassName = simpleClassName;
    this.sourceCode = sourceCode;
  }

  public static UncompiledSourceCode fromSourceCode(String sourceCode) {
    String simpleClassName = new ClassNameExtractor().extractFromSourceCode(sourceCode);
    return new UncompiledSourceCode(simpleClassName, sourceCode);
  }

  public static UncompiledSourceCode fromClassName(String fullyQualifiedClassName) {
    ClassNameWithPackage classNameWithPackage = ClassNameWithPackage.of(fullyQualifiedClassName);
    boolean isPackageSpecified = StringUtils.isNotBlank(classNameWithPackage.packageName);

    StringBuilder sourceCode = new StringBuilder();
    if (isPackageSpecified) {
      sourceCode.append(String.format("package %s;", classNameWithPackage.packageName));
      sourceCode.append(SystemUtils.LINE_SEPARATOR);
    }

    sourceCode.append(String.format("public class %s {}", classNameWithPackage.simpleClassName));

    return new UncompiledSourceCode(classNameWithPackage.simpleClassName, sourceCode.toString());
  }

  private static class ClassNameWithPackage {
    String packageName;
    String simpleClassName;

    static ClassNameWithPackage of(String fqClassName) {
      int indexOfLastDot = fqClassName.lastIndexOf(".");

      if (indexOfLastDot == -1) {
        return new ClassNameWithPackage("", fqClassName);
      } else {
        String specifiedPackage = fqClassName.substring(0, indexOfLastDot);
        String simpleClassName = fqClassName.substring(indexOfLastDot + 1);

        return new ClassNameWithPackage(specifiedPackage, simpleClassName);
      }
    }

    private ClassNameWithPackage(String packageName, String simpleClassName) {
      this.packageName = packageName;
      this.simpleClassName = simpleClassName;
    }
  }
}
