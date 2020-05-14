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
package org.apache.geode;

import static org.apache.geode.test.util.ResourceUtils.createTempFileFromResource;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.RestAPITest;
import org.apache.geode.test.junit.rules.RequiresGeodeHome;

@Category(RestAPITest.class)
public class GeodeDependencyJarIntegrationTest {

  private static final String GEODE_HOME = System.getenv("GEODE_HOME");

  private List<String> expectedClasspathElements;

  @Rule
  public RequiresGeodeHome requiresGeodeHome = new RequiresGeodeHome();

  @Before
  public void loadExpectedClassPath() throws IOException {
    String dependencyClasspath =
        createTempFileFromResource(AssemblyContentsIntegrationTest.class,
            "/dependency_classpath.txt").getAbsolutePath();

    expectedClasspathElements =
        Files.lines(Paths.get(dependencyClasspath)).collect(Collectors.toList());
  }

  @Test
  public void verifyManifestClassPath() throws IOException {
    List<String> currentClasspathElements = getManifestClassPath();
    Files.write(Paths.get("dependency_classpath.txt"), currentClasspathElements);

    assertThat(getManifestClassPath())
        .describedAs("The geode-dependency jar's manifest classpath has changed. Verify "
            + "dependencies and copy geode-assembly/build/integrationTest/dependency_classpath.txt "
            + "to geode-assembly/src/integrationTest/resources/dependency_classpath.txt")
        .containsExactlyInAnyOrderElementsOf(expectedClasspathElements);
  }

  /**
   * Find all of the jars bundled with the project. Key is the name of the jar, value is the path.
   */
  private List<String> getManifestClassPath() throws IOException {
    File geodeHomeDirectory = new File(GEODE_HOME);
    assertThat(geodeHomeDirectory)
        .describedAs(
            "Please set the GEODE_HOME environment variable to the product installation directory.")
        .isDirectory();

    JarFile geodeDependencies =
        new JarFile(new File(geodeHomeDirectory, "lib/geode-dependencies.jar"));

    Manifest geodeDependenciesManifest = geodeDependencies.getManifest();

    String classpath = geodeDependenciesManifest.getMainAttributes().getValue("Class-Path");

    return Arrays.stream(classpath.split(" "))
        .map(entry -> entry.contains("geode")
            ? entry.replaceFirst("\\d+\\.\\d+\\.\\d+(-build\\.\\d+)?", "0.0.0") : entry)
        .collect(Collectors.toList());
  }
}
