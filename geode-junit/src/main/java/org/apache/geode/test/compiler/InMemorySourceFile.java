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

import java.net.URI;

import javax.tools.SimpleJavaFileObject;

import org.apache.commons.lang3.StringUtils;

public class InMemorySourceFile extends SimpleJavaFileObject {
  private final String name;
  private final String sourceCode;

  public InMemorySourceFile(String name, String sourceCode) {
    super(URI.create("string:///" + name.replace('.', '/') + Kind.SOURCE.extension), Kind.SOURCE);
    this.name = name;
    this.sourceCode = sourceCode;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public CharSequence getCharContent(boolean ignoreEncodingErrors) {
    return sourceCode;
  }

  static InMemorySourceFile fromSourceCode(String sourceCode) {
    String className = new ClassNameExtractor().extractFromSourceCode(sourceCode);
    System.out.println("DHE: extracted class name: " + className);
    return new InMemorySourceFile(className, sourceCode);
  }

  static InMemorySourceFile fromClassName(String className) {
    ClassNameWithPackage classNameWithPackage = ClassNameWithPackage.of(className);
    boolean isPackageSpecified = StringUtils.isNotBlank(classNameWithPackage.packageName);

    StringBuilder sourceCode = new StringBuilder();
    if (isPackageSpecified) {
      sourceCode.append(String.format("package %s;", classNameWithPackage.packageName));
      sourceCode.append(lineSeparator());
    }

    sourceCode.append(String.format("public class %s {}", classNameWithPackage.simpleClassName));

    return new InMemorySourceFile(className, sourceCode.toString());
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
