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

import static org.junit.Assert.assertTrue;

import org.apache.geode.test.junit.categories.RestAPITest;
import org.apache.geode.util.test.TestUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.stream.Collectors;

@Category({RestAPITest.class})
public class GeodeDependencyJarIntegrationTest {

  private static final String GEODE_HOME = System.getenv("GEODE_HOME");
  private Set<String> expectedClassPath;

  @Before
  public void loadExpectedClassPath() throws IOException {
    String assemblyContent =
        TestUtil.getResourcePath(AssemblyContentsIntegrationTest.class, "/dependency_classpath.txt");

    expectedClassPath = Files.lines(Paths.get(assemblyContent)).collect(Collectors.toSet());
  }

  @Test
  public void verifyManifestClassPath() throws IOException {
    Set<String> currentAssemblyContent = getManifestClassPath();

    Files.write(Paths.get("assembly_content.txt"), currentAssemblyContent);

    Set<String> newAssemblyContent = new TreeSet<>(currentAssemblyContent);
    newAssemblyContent.removeAll(expectedClassPath);
    Set<String> missingAssemblyContent = new TreeSet<>(expectedClassPath);
    missingAssemblyContent.removeAll(currentAssemblyContent);

    String message =
        "The assembly contents have changed. Verify dependencies."
            + "\nWhen fixed, copy geode-assembly/build/integrationTest/assembly_content.txt"
            + "\nto src/integrationTest/resources/assembly_content.txt"
            + "\nRemoved Content\n--------------\n"
            + String.join("\n", missingAssemblyContent) + "\n\nAdded Content\n--------------\n"
            + String.join("\n", newAssemblyContent) + "\n\n";

    assertTrue(message, expectedClassPath.equals(currentAssemblyContent));
  }

  /**
   * Find all of the jars bundled with the project. Key is the name of the jar, value is the path.
   */
  private Set<String> getManifestClassPath() throws IOException {
    File geodeHomeDirectory = new File(GEODE_HOME);
    Path geodeHomePath = Paths.get(GEODE_HOME);

    assertTrue(
        "Please set the GEODE_HOME environment variable to the product installation directory.",
        geodeHomeDirectory.isDirectory());

    JarFile geodeDependencies = new JarFile(new File(geodeHomeDirectory, "lib/geode-dependencies.jar"));

    Manifest geodeDependenciesManifest = geodeDependencies.getManifest();

    String classPath = geodeDependenciesManifest.getMainAttributes().getValue("Class-Path");

    return Arrays.stream(classPath.split(" ")).collect(Collectors.toSet());
  }

}

