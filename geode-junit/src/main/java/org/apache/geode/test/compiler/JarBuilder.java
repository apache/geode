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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;


/**
 * This class accepts java source code in the format of .java source files or strings containing the
 * contents of .java source files, and compiles the given source code into a jar file specified by
 * the user.
 *
 * <p>
 * Example of usage:
 *
 * <pre>
`` *
 * &#064;Test
 * public void buildJarUsingStrings() {
 *   JarBuilder jarBuilder = new JarBuilder();
 *   File outputJar = new File("output.jar");
 *
 *   String classInFooBarPackage = &quot;package foo.bar; public class ClassA {int n = 10;}&quot;;
 *   String classInDefaultPackage = &quot;public class ClassB {}&quot;;
 *   jarBuilder.buildJar(outputJar, classInFooBarPackage, classInDefaultPackage);
 * }
 *
 * &#064;Test
 * public void buildJarUsingFiles() {
 *   JarBuilder jarBuilder = new JarBuilder();
 *   File outputJar = new File("output.jar");
 *
 *   // The following two files are expected to contain valid Java source code
 *   File sourceFileOne = new File("ClassA.java");
 *   File sourceFileTwo = new File("ClassB.java");
 *   jarBuilder.buildJar(outputJar, sourceFileOne, sourceFileTwo);
 * }
 *
 * </pre>
 **/
public class JarBuilder {
  private final JavaCompiler javaCompiler;

  public JarBuilder() throws IOException {
    javaCompiler = new JavaCompiler();
  }

  /**
   * Adds the given jarFile to the classpath that will be used for compilation by the buildJar
   * methods.
   */
  public void addToClasspath(File jarFile) {
    javaCompiler.addToClasspath(jarFile);
  }

  /**
   * Builds a jar file containing empty classes with the given classNames.
   */
  public void buildJarFromClassNames(File outputJarFile, String... classNames) throws IOException {
    UncompiledSourceCode[] uncompiledSourceCodes = Arrays.stream(classNames)
        .map(UncompiledSourceCode::fromClassName).toArray(UncompiledSourceCode[]::new);

    List<CompiledSourceCode> compiledSourceCodes = javaCompiler.compile(uncompiledSourceCodes);

    buildJar(outputJarFile, compiledSourceCodes);
  }

  public void buildJar(File outputJarFile, String... sourceFileContents) throws IOException {
    List<CompiledSourceCode> compiledSourceCodes = javaCompiler.compile(sourceFileContents);

    buildJar(outputJarFile, compiledSourceCodes);
  }

  public void buildJar(File outputJarFile, File... sourceFiles) throws IOException {
    List<CompiledSourceCode> compiledSourceCodes = javaCompiler.compile(sourceFiles);

    buildJar(outputJarFile, compiledSourceCodes);
  }

  private void buildJar(File outputJarFile, List<CompiledSourceCode> compiledSourceCodes)
      throws IOException {
    assertThat(outputJarFile).doesNotExist();

    try (FileOutputStream outputStream = new FileOutputStream(outputJarFile)) {
      JarOutputStream jarOutputStream = new JarOutputStream(outputStream);
      for (CompiledSourceCode compiledSource : compiledSourceCodes) {

        String formattedName = compiledSource.className.replace(".", "/");
        if (!formattedName.endsWith(".class")) {
          formattedName = formattedName.concat(".class");
        }

        JarEntry entry = new JarEntry(formattedName);
        entry.setTime(System.currentTimeMillis());
        jarOutputStream.putNextEntry(entry);
        jarOutputStream.write(compiledSource.compiledBytecode);
        jarOutputStream.closeEntry();
      }

      // Add timestamp so that the jar file itself has a new MD5 signature
      JarEntry timestampEntry = new JarEntry("timestamp");
      long now = System.currentTimeMillis();
      timestampEntry.setTime(now);
      jarOutputStream.putNextEntry(timestampEntry);
      jarOutputStream.write(Long.toString(now).getBytes());
      jarOutputStream.closeEntry();

      jarOutputStream.close();
    }
  }
}
