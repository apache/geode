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

import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;


public class JarBuilderTest {
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private JarBuilder jarBuilder;
  private File outputJar;

  @Before
  public void setup() throws IOException {
    jarBuilder = new JarBuilder();
    outputJar = new File(temporaryFolder.getRoot(), "output.jar");
  }

  @Test
  public void jarWithSingleClass() throws Exception {
    File classContents = loadTestResource("AbstractClass.java");
    jarBuilder.buildJar(outputJar, classContents);

    Set<String> jarEntryNames = jarEntryNamesFromFile(outputJar);
    assertThat(jarEntryNames).containsExactlyInAnyOrder(
        "timestamp",
        "org/apache/geode/test/compiler/AbstractClass.class");
  }

  @Test
  public void jarWithTwoDependentClasses() throws Exception {
    File sourceFileOne = loadTestResource("AbstractClass.java");
    File sourceFileTwo = loadTestResource("ConcreteClass.java");

    jarBuilder.buildJar(outputJar, sourceFileOne, sourceFileTwo);

    Set<String> jarEntryNames = jarEntryNamesFromFile(outputJar);

    assertThat(jarEntryNames).containsExactlyInAnyOrder(
        "timestamp",
        "org/apache/geode/test/compiler/AbstractClass.class",
        "org/apache/geode/test/compiler/ConcreteClass.class");
  }

  @Test
  public void jarWithClassInDefaultPackage() throws Exception {
    String classInFooBarPackage = "package foo.bar; public class ClassInFooBarPackage {}";
    String classInDefaultPackage = "public class ClassInDefaultPackage {}";
    jarBuilder.buildJar(outputJar, classInFooBarPackage, classInDefaultPackage);

    Set<String> jarEntryNames = jarEntryNamesFromFile(outputJar);
    assertThat(jarEntryNames).containsExactlyInAnyOrder(
        "timestamp",
        "ClassInDefaultPackage.class",
        "foo/bar/ClassInFooBarPackage.class");
  }


  @Test
  public void jarFromOnlyClassNames() throws Exception {
    String defaultPackageClassName = "DefaultClass";
    String otherPackageClassName = "foo.bar.OtherClass";
    jarBuilder.buildJarFromClassNames(outputJar, defaultPackageClassName, otherPackageClassName);

    Set<String> jarEntryNames = jarEntryNamesFromFile(outputJar);
    assertThat(jarEntryNames).containsExactlyInAnyOrder("DefaultClass.class",
        "foo/bar/OtherClass.class", "timestamp");
  }

  @Test
  public void canLoadClassesFromJar() throws Exception {
    String defaultPackageClassName = "DefaultClass";
    String otherPackageClassName = "foo.bar.OtherClass";
    jarBuilder.buildJarFromClassNames(outputJar, defaultPackageClassName, otherPackageClassName);

    URLClassLoader jarClassLoader = new URLClassLoader(new URL[] {outputJar.toURL()});

    jarClassLoader.loadClass("DefaultClass");
    jarClassLoader.loadClass("foo.bar.OtherClass");
  }

  private Set<String> jarEntryNamesFromFile(File jarFile) throws Exception {
    assertThat(jarFile).exists();

    Enumeration<JarEntry> jarEntries = new JarFile(jarFile).entries();
    return Collections.list(jarEntries).stream().map(JarEntry::getName).collect(toSet());
  }

  private File loadTestResource(String fileName) throws URISyntaxException {
    URL resourceFileURL = this.getClass().getResource(fileName);
    assertThat(resourceFileURL).isNotNull();

    URI resourceUri = resourceFileURL.toURI();
    return new File(resourceUri);
  }
}
